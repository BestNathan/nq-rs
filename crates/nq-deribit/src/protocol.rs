use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use tracing::{debug, info, warn};

use crate::errors::DeribitError::RequestTimeout;
use crate::jsonrpc::JSONRPCResponse;
use crate::request::authentication::AuthRequest;
use crate::request::subscribe::PublicSubscribeRequest;
use crate::transport::Transport;

// ─── ProtocolEvent ───────────────────────────────────────────────────

/// Events produced by [`ProtocolHandler::handle_message`] for the upper
/// layer (Connection eventloop) to process.
pub enum ProtocolEvent {
    /// Route this response text to the API caller waiting on `id`.
    RouteResponse(i64, String),
    /// Send this text payload on the transport.
    Send(String),
    /// A server heartbeat notification was received (for metrics/logging).
    HeartbeatDetected,
}

// ─── ProtocolHandler ─────────────────────────────────────────────────

/// Layer 2: JSON-RPC / Deribit protocol handler.
///
/// Knows about JSON-RPC framing, Deribit request/response format,
/// subscription lifecycle, and heartbeat. Does NOT own a transport —
/// all I/O goes through the [`Transport`] trait passed as a parameter.
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

    /// Run the full setup sequence on a freshly connected transport:
    ///   1. Set heartbeat (liveness probe, 10s timeout, fatal)
    ///   2. Authenticate (10s timeout, non-fatal)
    ///   3. Re-subscribe all tracked channels (60s/batch timeout, fatal)
    ///
    /// Reads responses directly from the transport (synchronous ws_call),
    /// avoiding the channel-based routing race that caused the reconnect
    /// death spiral in the original code.
    pub async fn run_setup(
        &self,
        transport: &mut impl Transport,
    ) -> Result<()> {
        let conn_id = self.conn_id;

        // ── 1. Set heartbeat (liveness probe) ───────────────────────
        let hb_id = 900_000 + conn_id as i64;
        let hb_payload = json!({
            "jsonrpc": "2.0",
            "id": hb_id,
            "method": "public/set_heartbeat",
            "params": { "interval": self.heartbeat_interval }
        }).to_string();
        self.ws_call(transport, &hb_payload, hb_id, Duration::from_secs(10))
            .await
            .with_context(|| "heartbeat probe failed")?;
        debug!(connection_id = conn_id, "heartbeat set");

        // ── 2. Auth (non-fatal) ─────────────────────────────────────
        if let (Some(client_id), Some(client_secret)) =
            (&self.client_id, &self.client_secret)
        {
            let auth_id = 800_000 + conn_id as i64;
            let auth_val = {
                let mut val = serde_json::to_value(
                    &AuthRequest::credential_auth(client_id, client_secret),
                )
                .context("auth serialize")?;
                if let Some(obj) = val.as_object_mut() {
                    obj.insert("jsonrpc".to_string(), json!("2.0"));
                    obj.insert("id".to_string(), json!(auth_id));
                }
                val.to_string()
            };
            match self
                .ws_call(transport, &auth_val, auth_id, Duration::from_secs(10))
                .await
            {
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
            self.resubscribe_sync(transport, &channel_list)
                .await
                .context("setup resubscribe failed")?;
        }

        debug!(connection_id = conn_id, "setup complete");
        Ok(())
    }

    /// Synchronous re-subscribe: sends batched subscribe requests directly
    /// on the transport and waits for each response. Returns an error on
    /// transport failure so the caller can trigger reconnection.
    async fn resubscribe_sync(
        &self,
        transport: &mut impl Transport,
        channel_list: &[String],
    ) -> Result<()> {
        const BATCH_SIZE: usize = 250;
        const BATCH_DELAY_MS: u64 = 200;
        let total = channel_list.len();
        let mut base_id = 700_000 + self.conn_id as i64;
        let mut done = 0usize;

        for chunk in channel_list.chunks(BATCH_SIZE) {
            let sub_id = base_id;
            base_id += 1;
            let mut sub_val = serde_json::to_value(
                &PublicSubscribeRequest::new(chunk.to_vec()),
            )?;
            if let Some(obj) = sub_val.as_object_mut() {
                obj.insert("jsonrpc".to_string(), json!("2.0"));
                obj.insert("id".to_string(), json!(sub_id));
            }
            self.ws_call(
                transport,
                &sub_val.to_string(),
                sub_id,
                Duration::from_secs(60),
            )
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

    /// Send a JSON-RPC request on the transport and wait for the matching
    /// response by `id`. Used during synchronous setup (before the main
    /// eventloop starts).
    ///
    /// Returns the raw response text on success. Only checks transport-level
    /// errors — JSON-RPC error responses (which also have an `id`) are returned
    /// as `Ok`, leaving business-level error handling to the caller.
    async fn ws_call(
        &self,
        transport: &mut impl Transport,
        payload: &str,
        request_id: i64,
        timeout_dur: Duration,
    ) -> Result<String> {
        transport
            .send(payload.to_string())
            .await
            .with_context(|| "ws_call send")?;

        let deadline = tokio::time::Instant::now() + timeout_dur;
        loop {
            let remaining = deadline
                .saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(anyhow::Error::from(RequestTimeout));
            }

            let result = tokio::time::timeout(remaining, transport.recv())
                .await
                .map_err(|_| anyhow::Error::from(RequestTimeout))
                .with_context(|| "ws_call timeout")?;

            let text = match result {
                Ok(Some(t)) => t,
                Ok(None) => {
                    return Err(anyhow::anyhow!("ws_call: transport closed"));
                }
                Err(e) => {
                    return Err(anyhow::Error::new(e)
                        .context("ws_call: transport error"));
                }
            };

            let value: Value = serde_json::from_str(&text)
                .with_context(|| "ws_call decode json")?;

            if let Some(id) = value.get("id").and_then(|v| v.as_i64()) {
                if id == request_id {
                    return Ok(text);
                }
            }
            // Ignore non-response messages (subscription data, etc.) during setup
        }
    }

    // ── Message dispatch ────────────────────────────────────────────

    /// Handle one incoming JSON text message from the transport.
    ///
    /// Returns a list of [`ProtocolEvent`]s for the upper layer to process:
    /// - API responses (with "id") → `RouteResponse`
    /// - "heartbeat" method → `HeartbeatDetected` + `Send(public/test)`
    /// - "subscription" method → tracked in metrics, broadcast to pool
    pub fn handle_message(&self, text: &str) -> Vec<ProtocolEvent> {
        let value: Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(_) => return Vec::new(),
        };

        // API response (has "id" field — even errors have an id)
        if let Some(id) = value.get("id").and_then(|v| v.as_i64()) {
            return vec![ProtocolEvent::RouteResponse(id, text.to_string())];
        }

        // Notification message (has "method" field)
        if let Some(method) = value.get("method").and_then(|v| v.as_str()) {
            match method {
                "heartbeat" => {
                    let test_id = 600_000 + self.conn_id as i64;
                    let test_payload = json!({
                        "jsonrpc": "2.0",
                        "id": test_id,
                        "method": "public/test",
                        "params": {}
                    })
                    .to_string();
                    vec![
                        ProtocolEvent::HeartbeatDetected,
                        ProtocolEvent::Send(test_payload),
                    ]
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
                        "unknown method: {}", method
                    );
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        }
    }
}

// ─── Unit tests ──────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{TransportError, Transport as TransportTrait};

    /// A mock transport that uses in-memory channels for testing.
    struct MockTransport {
        /// Messages we feed to the code under test as incoming responses.
        recv_rx: flume::Receiver<String>,
        /// Pre-queued responses returned before reading from the channel.
        responses: std::collections::VecDeque<String>,
        connected: bool,
    }

    impl MockTransport {
        fn new() -> (Self, flume::Sender<String>) {
            let (recv_tx, recv_rx) = flume::bounded::<String>(32);
            (
                Self {
                    recv_rx,
                    responses: std::collections::VecDeque::new(),
                    connected: false,
                },
                recv_tx,
            )
        }

        fn queue_response(&mut self, text: &str) {
            self.responses.push_back(text.to_string());
        }
    }

    #[async_trait::async_trait]
    impl TransportTrait for MockTransport {
        async fn connect(&mut self) -> Result<(), TransportError> {
            self.connected = true;
            Ok(())
        }

        async fn send(&mut self, _text: String) -> Result<(), TransportError> {
            if !self.connected {
                return Err(TransportError::NotConnected);
            }
            Ok(())
        }

        async fn recv(&mut self) -> Result<Option<String>, TransportError> {
            if !self.connected {
                return Err(TransportError::NotConnected);
            }
            match self.responses.pop_front() {
                Some(text) => Ok(Some(text)),
                None => {
                    match self.recv_rx.recv_async().await {
                        Ok(msg) => Ok(Some(msg)),
                        Err(_) => Ok(None),
                    }
                }
            }
        }

        async fn send_ping(&mut self) -> Result<(), TransportError> {
            Ok(())
        }

        async fn close(&mut self) -> Result<(), TransportError> {
            self.connected = false;
            Ok(())
        }

        fn is_connected(&self) -> bool {
            self.connected
        }
    }

    fn make_handler() -> ProtocolHandler {
        ProtocolHandler::new(
            Arc::new(RwLock::new(None)),
            Arc::new(RwLock::new(HashSet::new())),
            None, // no broadcast in tests
            30,
            0,
            None,
            None,
        )
    }

    // ── handle_message tests ───────────────────────────────────────

    /// API response with "id" produces RouteResponse.
    #[test]
    fn test_handle_api_response() {
        let handler = make_handler();
        let text = r#"{"jsonrpc":"2.0","id":42,"result":"ok"}"#;
        let events = handler.handle_message(text);
        assert_eq!(events.len(), 1);
        match &events[0] {
            ProtocolEvent::RouteResponse(id, t) => {
                assert_eq!(*id, 42);
                assert_eq!(t, text);
            }
            _ => panic!("expected RouteResponse"),
        }
    }

    /// Heartbeat notification produces HeartbeatDetected + Send.
    #[test]
    fn test_handle_heartbeat() {
        let handler = make_handler();
        let text = r#"{"jsonrpc":"2.0","method":"heartbeat","params":{"type":"test_request"}}"#;
        let events = handler.handle_message(text);
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], ProtocolEvent::HeartbeatDetected));
        assert!(matches!(events[1], ProtocolEvent::Send(_)));
    }

    /// Unknown method produces warn log, no events.
    #[test]
    fn test_handle_unknown_method() {
        let handler = make_handler();
        let text = r#"{"jsonrpc":"2.0","method":"unknown_thing","params":{}}"#;
        let events = handler.handle_message(text);
        assert!(events.is_empty());
    }

    /// Malformed JSON produces no events (no panic).
    #[test]
    fn test_handle_malformed_json() {
        let handler = make_handler();
        let events = handler.handle_message("not json at all");
        assert!(events.is_empty());
    }

    /// Message with no "id" and no "method" produces no events.
    #[test]
    fn test_handle_no_id_no_method() {
        let handler = make_handler();
        let text = r#"{"jsonrpc":"2.0","params":{}}"#;
        let events = handler.handle_message(text);
        assert!(events.is_empty());
    }

    // ── ws_call test ───────────────────────────────────────────────

    /// ws_call sends the payload, receives matching response.
    #[tokio::test]
    async fn test_ws_call_success() {
        let handler = make_handler();
        let (mut transport, _recv_tx) = MockTransport::new();
        transport.connect().await.unwrap();

        // Queue a matching response
        transport.queue_response(r#"{"jsonrpc":"2.0","id":99,"result":"pong"}"#);

        let result = handler
            .ws_call(&mut transport, r#"{"method":"test"}"#, 99, Duration::from_secs(5))
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap().contains("pong"));
    }

    /// ws_call times out if no matching response arrives.
    #[tokio::test]
    async fn test_ws_call_timeout() {
        let handler = make_handler();
        let (mut transport, _recv_tx) = MockTransport::new();
        transport.connect().await.unwrap();

        // No response queued — will timeout
        let result = handler
            .ws_call(&mut transport, r#"{"method":"test"}"#, 99, Duration::from_millis(10))
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("timeout"));
    }

    // ── run_setup test ─────────────────────────────────────────────

    /// run_setup succeeds with heartbeat + resubscribe for tracked channels.
    #[tokio::test]
    async fn test_run_setup_heartbeat_only() {
        let handler = make_handler();
        let (mut transport, _recv_tx) = MockTransport::new();
        transport.connect().await.unwrap();

        // Queue heartbeat response
        transport.queue_response(
            r#"{"jsonrpc":"2.0","id":900000,"result":{"interval":30}}"#,
        );

        // No channels tracked, so setup should succeed with just heartbeat
        let result = handler.run_setup(&mut transport).await;
        assert!(result.is_ok());
    }

    /// run_setup with tracked channels also does resubscribe.
    #[tokio::test]
    async fn test_run_setup_with_channels() {
        // Create handler with tracked channels
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

        let (mut transport, _recv_tx) = MockTransport::new();
        transport.connect().await.unwrap();

        // Queue heartbeat response
        transport.queue_response(
            r#"{"jsonrpc":"2.0","id":900000,"result":{"interval":30}}"#,
        );
        // Queue subscribe response (1 channel, 1 batch)
        transport.queue_response(
            r#"{"jsonrpc":"2.0","id":700000,"result":["ticker.BTC-PERP.agg2"]}"#,
        );

        let result = handler.run_setup(&mut transport).await;
        assert!(result.is_ok());
    }
}
