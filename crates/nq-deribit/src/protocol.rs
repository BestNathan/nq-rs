use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::json;
use tracing::{debug, info, warn};

use crate::jsonrpc::{IDGenerator, JSONRPCResponse, JSPNRPCRequest};
use crate::request::authentication::AuthRequest;
use crate::request::subscribe::PublicSubscribeRequest;

// ─── JsonRpcCaller ────────────────────────────────────────────────────

/// Trait for making JSON-RPC calls through the Connection eventloop.
/// Abstracts the channel-based call mechanism so Protocol doesn't need
/// to know about Connection internals.
#[async_trait::async_trait]
pub trait JsonRpcCaller: Send {
    /// Send a JSON-RPC request payload and wait for the matching response.
    /// The payload must include an "id" field for correlation.
    async fn call(&mut self, payload: &str, timeout: Duration) -> Result<String>;
}

// ─── OutgoingAction ──────────────────────────────────────────────────

/// Actions returned by [`ProtocolHandler::handle_notification`] that the
/// Connection eventloop should execute (e.g. sending a response).
pub enum OutgoingAction {
    /// Send this payload on the transport.
    Send(String),
    /// Send this payload and register a dummy waiter for its JSON-RPC `id`.
    /// Use this for fire-and-forget requests (e.g. heartbeat `public/test`)
    /// that still expect a response — the response is consumed silently
    /// instead of triggering a "no waiter for response" warning.
    ExpectResponse(String, i64),
}

// ─── ProtocolHandler ─────────────────────────────────────────────────

/// Layer 2: JSON-RPC / Deribit protocol handler.
///
/// Knows about JSON-RPC framing, Deribit request/response format,
/// subscription lifecycle, and heartbeat. Does NOT own a transport —
/// all I/O goes through the [`JsonRpcCaller`] trait passed as a parameter.
pub struct ProtocolHandler {
    token: Arc<RwLock<Option<String>>>,
    channels: Arc<RwLock<HashSet<String>>>,
    broadcast_tx: Option<tokio::sync::broadcast::Sender<String>>,
    heartbeat_interval: u64,
    conn_id: usize,
    client_id: Option<String>,
    client_secret: Option<String>,
}

impl ProtocolHandler {
    pub fn new(
        token: Arc<RwLock<Option<String>>>,
        channels: Arc<RwLock<HashSet<String>>>,
        broadcast_tx: Option<tokio::sync::broadcast::Sender<String>>,
        heartbeat_interval: u64,
        conn_id: usize,
        client_id: Option<String>,
        client_secret: Option<String>,
    ) -> Self {
        Self {
            token,
            channels,
            broadcast_tx,
            heartbeat_interval,
            conn_id,
            client_id,
            client_secret,
        }
    }

    // ── Synchronous setup ───────────────────────────────────────────

    /// Run the full setup sequence using the JsonRpcCaller (channel-based,
    /// no direct transport.recv() calls):
    ///   1. Set heartbeat (liveness probe, 10s timeout, fatal)
    ///   2. Authenticate (10s timeout, non-fatal)
    ///   3. Re-subscribe all tracked channels (60s/batch timeout, fatal)
    pub async fn run_setup(
        &self,
        caller: &mut dyn JsonRpcCaller,
    ) -> Result<()> {
        let conn_id = self.conn_id;

        // ── 1. Set heartbeat (liveness probe) ───────────────────────
        let hb_payload = json!({
            "jsonrpc": "2.0",
            "id": crate::jsonrpc::global_id_generator().next_id(),
            "method": "public/set_heartbeat",
            "params": { "interval": self.heartbeat_interval }
        }).to_string();
        caller.call(&hb_payload, Duration::from_secs(10))
            .await
            .with_context(|| "heartbeat probe failed")?;
        debug!(connection_id = conn_id, "heartbeat set");

        // ── 2. Auth (non-fatal) ─────────────────────────────────────
        if let (Some(client_id), Some(client_secret)) =
            (&self.client_id, &self.client_secret)
        {
            let auth_payload = {
                let mut val = serde_json::to_value(
                    &AuthRequest::credential_auth(client_id, client_secret),
                )
                .context("auth serialize")?;
                if let Some(obj) = val.as_object_mut() {
                    obj.insert("jsonrpc".to_string(), json!("2.0"));
                    obj.insert(
                        "id".to_string(),
                        json!(crate::jsonrpc::global_id_generator().next_id()),
                    );
                }
                val.to_string()
            };
            match caller.call(&auth_payload, Duration::from_secs(10)).await {
                Ok(resp) => {
                    if let Ok(result) = serde_json::from_str::<
                        JSONRPCResponse<crate::request::authentication::AuthResponse>,
                    >(&resp)
                    {
                        if let either::Either::Left(auth_resp) = result.result {
                            *self.token.write().unwrap() =
                                auth_resp.access_token;
                            info!(connection_id = conn_id, "authenticated");
                        }
                    }
                }
                Err(e) => {
                    warn!(connection_id = conn_id, error = ?e, "auth failed (non-fatal)")
                }
            }
        }

        // ── 3. Re-subscribe all tracked channels (fatal) ────────────
        let channel_list: Vec<String> =
            self.channels.read().unwrap().iter().cloned().collect();
        if !channel_list.is_empty() {
            self.resubscribe_via_caller(caller, &channel_list)
                .await
                .context("setup resubscribe failed")?;
        }

        debug!(connection_id = conn_id, "setup complete");
        Ok(())
    }

    /// Synchronous re-subscribe via the JsonRpcCaller (uses the eventloop
    /// channel dispatch instead of direct transport.recv()).
    async fn resubscribe_via_caller(
        &self,
        caller: &mut dyn JsonRpcCaller,
        channel_list: &[String],
    ) -> Result<()> {
        const BATCH_SIZE: usize = 250;
        const BATCH_DELAY_MS: u64 = 200;
        let total = channel_list.len();
        let mut done = 0usize;

        for chunk in channel_list.chunks(BATCH_SIZE) {
            let req = JSPNRPCRequest::<PublicSubscribeRequest>::from(
                PublicSubscribeRequest::new(chunk.to_vec()),
            );
            let sub_payload = serde_json::to_string(&req)?;
            caller
                .call(&sub_payload, Duration::from_secs(60))
                .await?;
            done += chunk.len();
            info!(
                connection_id = self.conn_id,
                progress = %format!("{}/{}", done, total),
                "resubscribed batch"
            );
            if done < total {
                tokio::time::sleep(Duration::from_millis(BATCH_DELAY_MS))
                    .await;
            }
        }
        info!(
            connection_id = self.conn_id,
            "re-subscribed {} channels", total
        );
        Ok(())
    }

    // ── Notification dispatch ──────────────────────────────────────

    /// Handle an incoming JSON-RPC notification (message with "method" but no
    /// "id"). Called by the Connection eventloop from its recv dispatch branch.
    ///
    /// Returns [`OutgoingAction`]s for the eventloop to execute (e.g. sending
    /// a `public/test` response to a heartbeat).
    ///
    /// Unlike the old `handle_message`, this does NOT handle response routing —
    /// that's done directly by the Connection eventloop via `responser_map`.
    pub fn handle_notification(&self, method: &str, text: &str) -> Vec<OutgoingAction> {
        match method {
            "heartbeat" => {
                let id = crate::jsonrpc::global_id_generator().next_id();
                let test_payload = json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": "public/test",
                    "params": {}
                })
                .to_string();
                vec![OutgoingAction::ExpectResponse(test_payload, id)]
            }
            "subscription" => {
                crate::metrics::DERIBIT_METRICS.sub_received.add(1, &[]);
                if let Some(tx) = &self.broadcast_tx {
                    match tx.send(text.to_string()) {
                        Ok(_) => {
                            crate::metrics::DERIBIT_METRICS.sub_enqueued.add(1, &[]);
                        }
                        Err(_) => {
                            crate::metrics::DERIBIT_METRICS.sub_dropped.add(1, &[]);
                        }
                    }
                }
                Vec::new()
            }
            _ => {
                warn!(
                    connection_id = self.conn_id,
                    "unknown notification method: {}", method
                );
                Vec::new()
            }
        }
    }
}

// ─── Unit tests ──────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// A mock JsonRpcCaller that returns pre-queued responses.
    struct MockCaller {
        responses: Mutex<std::collections::VecDeque<Result<String>>>,
    }

    impl MockCaller {
        fn new() -> Self {
            Self {
                responses: Mutex::new(std::collections::VecDeque::new()),
            }
        }

        fn queue_ok(&self, text: &str) {
            self.responses
                .lock()
                .unwrap()
                .push_back(Ok(text.to_string()));
        }

        fn queue_err(&self, err: &str) {
            self.responses
                .lock()
                .unwrap()
                .push_back(Err(anyhow::anyhow!("{}", err)));
        }
    }

    #[async_trait::async_trait]
    impl JsonRpcCaller for MockCaller {
        async fn call(&mut self, _payload: &str, _timeout: Duration) -> Result<String> {
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Err(anyhow::anyhow!("no response queued")))
        }
    }

    fn make_handler() -> ProtocolHandler {
        ProtocolHandler::new(
            Arc::new(RwLock::new(None)),
            Arc::new(RwLock::new(HashSet::new())),
            None,
            30,
            0,
            None,
            None,
        )
    }

    // ── handle_notification tests ──────────────────────────────────

    #[test]
    fn test_handle_heartbeat() {
        let handler = make_handler();
        let text = r#"{"jsonrpc":"2.0","method":"heartbeat","params":{"type":"test_request"}}"#;
        let actions = handler.handle_notification("heartbeat", text);
        assert_eq!(actions.len(), 1);
        let payload = match &actions[0] {
            OutgoingAction::ExpectResponse(payload, _) => payload,
            OutgoingAction::Send(payload) => payload,
        };
        assert!(payload.contains("public/test"));
    }

    #[test]
    fn test_handle_subscription() {
        let handler = make_handler();
        let text = r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"ticker.BTC.agg2","data":{}}}"#;
        let actions = handler.handle_notification("subscription", text);
        assert!(actions.is_empty());
    }

    #[test]
    fn test_handle_unknown_method() {
        let handler = make_handler();
        let text = r#"{"jsonrpc":"2.0","method":"unknown_thing","params":{}}"#;
        let actions = handler.handle_notification("unknown_thing", text);
        assert!(actions.is_empty());
    }

    // ── run_setup tests ────────────────────────────────────────────

    #[tokio::test]
    async fn test_run_setup_heartbeat_only() {
        let handler = make_handler();
        let mut caller = MockCaller::new();
        caller.queue_ok(r#"{"jsonrpc":"2.0","id":1,"result":{"interval":30}}"#);

        let result = handler.run_setup(&mut caller).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_run_setup_heartbeat_fails() {
        let handler = make_handler();
        let mut caller = MockCaller::new();
        caller.queue_err("timeout");

        let result = handler.run_setup(&mut caller).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("heartbeat"));
    }

    #[tokio::test]
    async fn test_run_setup_with_channels() {
        let channels = Arc::new(RwLock::new(HashSet::new()));
        channels.write().unwrap().insert("ticker.BTC-PERP.agg2".into());
        let handler = ProtocolHandler::new(
            Arc::new(RwLock::new(None)),
            channels,
            None,
            30,
            0,
            None,
            None,
        );

        let mut caller = MockCaller::new();
        caller.queue_ok(r#"{"jsonrpc":"2.0","id":1,"result":{"interval":30}}"#);
        caller.queue_ok(r#"{"jsonrpc":"2.0","id":2,"result":["ticker.BTC-PERP.agg2"]}"#);

        let result = handler.run_setup(&mut caller).await;
        assert!(result.is_ok());
    }
}
