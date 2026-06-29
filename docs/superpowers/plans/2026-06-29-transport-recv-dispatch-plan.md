# Transport Recv Dispatch Refactor — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor WebSocket message receive path so `transport.recv()` has a single consumer with oneshot-based response routing and callback-based notification dispatch.

**Architecture:** Transport self-contains ping/pong keepalive. Connection eventloop is the single `recv()` consumer — it routes responses by id→oneshot and dispatches notifications to Protocol's callback. Protocol uses a unified `JsonRpcCaller` trait for all API calls including setup. No more direct `transport.recv()` outside the eventloop.

**Tech Stack:** Rust, tokio, reqwest-websocket, serde_json

---

### Task 1: Transport — internalize ping timer, demote pong timeout

**Files:**
- Modify: `crates/nq-deribit/src/transport.rs`

Moves ping scheduling into `recv()` and makes pong timeout warn-only. Removes `send_ping()` from the trait and `PongTimeout` error variant.

- [ ] **Step 1: Remove `PongTimeout` from `TransportError`**

Edit `crates/nq-deribit/src/transport.rs` lines 12-26, replace the enum:

```rust
#[derive(Error, Debug)]
pub enum TransportError {
    #[error("connection failed: {0}")]
    ConnectFailed(#[source] anyhow::Error),
    #[error("send failed: {0}")]
    SendFailed(#[source] anyhow::Error),
    #[error("receive failed: {0}")]
    RecvFailed(#[source] anyhow::Error),
    #[error("websocket closed: code={code:?} reason={reason}")]
    Closed { code: Option<u16>, reason: String },
    #[error("transport not connected")]
    NotConnected,
}
```

- [ ] **Step 2: Remove `send_ping` from the `Transport` trait**

Edit the trait doc comment for `recv()` and remove the `send_ping` method declaration (currently lines 59-63):

```rust
    /// Receive the next application-level message from the WebSocket.
    ///
    /// Handles WebSocket protocol frames internally:
    /// - Ping → auto-responds with Pong, continues waiting
    /// - Pong → updates liveness timestamp, continues waiting
    /// - Close → returns `Ok(None)` (clean close)
    /// - Text/Binary → returns `Ok(Some(text))`
    ///
    /// Also sends periodic Ping frames internally for keepalive.
    async fn recv(&mut self) -> Result<Option<String>, TransportError>;

    /// Close the WebSocket connection cleanly.
    async fn close(&mut self) -> Result<(), TransportError>;
```

- [ ] **Step 3: Remove `send_ping` impl from `WsTransportImpl`**

Delete the entire `send_ping` method (lines 232-241).

- [ ] **Step 4: Remove `ping_interval()` getter from `WsTransportImpl`**

Delete lines 111-113.

- [ ] **Step 5: Rewrite `recv()` with internal ping timer and demoted pong timeout**

Replace the entire `recv()` method (lines 157-230) with:

```rust
    async fn recv(&mut self) -> Result<Option<String>, TransportError> {
        let ws = self
            .ws
            .as_mut()
            .ok_or(TransportError::NotConnected)?;

        // ── Pong timeout check (warn only) ─────────────────────────
        if let Some(last_pong) = self.last_pong {
            if last_pong.elapsed() > self.pong_timeout {
                warn!(
                    connection_id = self.conn_id,
                    elapsed_ms = last_pong.elapsed().as_millis(),
                    timeout_ms = self.pong_timeout.as_millis(),
                    "pong timeout — server may not support WS ping/pong"
                );
                // Reset to avoid log spam; server uses its own heartbeat
                self.last_pong = Some(Instant::now());
            }
        }

        // ── Read next message with internal ping timer ────────────
        let next_ping = Instant::now() + self.ping_interval;
        loop {
            let message = tokio::select! {
                msg = ws.next() => {
                    match msg {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            return Err(TransportError::RecvFailed(e.into()));
                        }
                        None => {
                            debug!(connection_id = self.conn_id, "ws stream ended");
                            return Ok(None);
                        }
                    }
                }
                _ = tokio::time::sleep_until(next_ping) => {
                    trace!(connection_id = self.conn_id, "sending ping (internal timer)");
                    if let Err(e) = ws.send(Message::Ping(Vec::new())).await {
                        warn!(connection_id = self.conn_id, error = ?e, "failed to send ping");
                    }
                    continue;
                }
            };

            match message {
                Message::Text(t) => {
                    trace!(connection_id = self.conn_id, len = t.len(), "recv text");
                    return Ok(Some(t));
                }
                Message::Binary(b) => {
                    let text = String::from_utf8(b).map_err(|e| {
                        TransportError::RecvFailed(anyhow::anyhow!("invalid utf8: {}", e))
                    })?;
                    trace!(connection_id = self.conn_id, len = text.len(), "recv binary");
                    return Ok(Some(text));
                }
                Message::Ping(data) => {
                    trace!(connection_id = self.conn_id, "recv ping, sending pong");
                    if let Err(e) = ws.send(Message::Pong(data)).await {
                        warn!(connection_id = self.conn_id, error = ?e, "failed to send pong");
                    }
                }
                Message::Pong(_data) => {
                    trace!(connection_id = self.conn_id, "recv pong");
                    self.last_pong = Some(Instant::now());
                }
                Message::Close { code, reason } => {
                    let code_u16: u16 = code.into();
                    debug!(
                        connection_id = self.conn_id,
                        code = code_u16,
                        reason = reason,
                        "recv close frame"
                    );
                    return Ok(None);
                }
            }
        }
    }
```

- [ ] **Step 6: Update unit tests to remove PongTimeout references**

Edit `test_transport_error_display` (line 279) to remove PongTimeout assertion:

```rust
    #[test]
    fn test_transport_error_display() {
        let err = TransportError::NotConnected;
        assert!(err.to_string().contains("not connected"));
    }
```

Edit `test_transport_new` (line 284) to remove `ping_interval()` assertion:

```rust
    #[test]
    fn test_transport_new() {
        let client = reqwest::Client::new();
        let t = WsTransportImpl::new(
            client,
            "wss://example.com/ws".into(),
            Duration::from_secs(15),
            Duration::from_secs(30),
            0,
        );
        assert!(!t.is_connected());
    }
```

- [ ] **Step 7: Run transport unit tests**

Run: `cargo test -p nq-deribit transport`
Expected: all transport tests PASS

- [ ] **Step 8: Commit**

```bash
git add crates/nq-deribit/src/transport.rs
git commit -m "refactor(transport): internalize ping timer, demote pong timeout to warn

- recv() now uses internal tokio::select! over ws.next() and ping sleep
- Pong timeout logs warn! only, no longer returns error
- Removed send_ping() from Transport trait and WsTransportImpl
- Removed PongTimeout from TransportError

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 2: Protocol — add JsonRpcCaller trait, notification handler, remove ws_call

**Files:**
- Modify: `crates/nq-deribit/src/protocol.rs`

Defines `JsonRpcCaller` trait and `OutgoingAction` enum (needed by Connection in Task 3). Removes `ws_call()` and `resubscribe_sync()`. Changes `run_setup()` to use `&dyn JsonRpcCaller`. Adds `handle_notification()` method. Removes old `ProtocolEvent` enum and `handle_message()`.

- [ ] **Step 1: Add `JsonRpcCaller` trait and `OutgoingAction` enum**

Insert after the `use` statements (after the existing `use crate::transport::Transport;` line), before the `ProtocolEvent` section:

```rust
// ─── JsonRpcCaller ────────────────────────────────────────────────────

/// Trait for making JSON-RPC calls through the Connection eventloop.
/// Abstracts the channel-based call mechanism so Protocol doesn't need
/// to know about Connection internals.
#[async_trait]
pub trait JsonRpcCaller: Send + Sync {
    /// Send a JSON-RPC request payload and wait for the matching response.
    /// The payload must include an "id" field for correlation.
    async fn call(&self, payload: &str, timeout: Duration) -> Result<String>;
}

// ─── OutgoingAction ──────────────────────────────────────────────────

/// Actions returned by [`ProtocolHandler::handle_notification`] that the
/// Connection eventloop should execute (e.g. sending a response).
pub enum OutgoingAction {
    /// Send this payload on the transport.
    Send(String),
}
```

- [ ] **Step 2: Remove `ProtocolEvent` enum**

Delete lines 17-26 (the `ProtocolEvent` enum and its doc comment). It's replaced by `OutgoingAction` + direct response routing in Connection.

- [ ] **Step 3: Remove `ws_call()` and `resubscribe_sync()` methods**

Delete lines 202-248 (the entire `ws_call` method and the `resubscribe_sync` method).

- [ ] **Step 4: Rewrite `run_setup()` to use `&dyn JsonRpcCaller`**

Replace the entire `run_setup` method (lines 76-144) and add `resubscribe_via_caller`:

```rust
    // ── Synchronous setup ───────────────────────────────────────────

    /// Run the full setup sequence using the JsonRpcCaller (channel-based,
    /// no direct transport.recv() calls):
    ///   1. Set heartbeat (liveness probe, 10s timeout, fatal)
    ///   2. Authenticate (10s timeout, non-fatal)
    ///   3. Re-subscribe all tracked channels (60s/batch timeout, fatal)
    pub async fn run_setup(
        &self,
        caller: &dyn JsonRpcCaller,
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
        caller: &dyn JsonRpcCaller,
        channel_list: &[String],
    ) -> Result<()> {
        const BATCH_SIZE: usize = 250;
        const BATCH_DELAY_MS: u64 = 200;
        let total = channel_list.len();
        let mut done = 0usize;

        for chunk in channel_list.chunks(BATCH_SIZE) {
            let mut sub_val = serde_json::to_value(
                &PublicSubscribeRequest::new(chunk.to_vec()),
            )?;
            if let Some(obj) = sub_val.as_object_mut() {
                obj.insert("jsonrpc".to_string(), json!("2.0"));
                obj.insert(
                    "id".to_string(),
                    json!(crate::jsonrpc::global_id_generator().next_id()),
                );
            }
            caller
                .call(&sub_val.to_string(), Duration::from_secs(60))
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
```

- [ ] **Step 5: Replace `handle_message()` with `handle_notification()`**

Replace lines 258-311 (the `handle_message` method) with:

```rust
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
                let test_payload = json!({
                    "jsonrpc": "2.0",
                    "id": crate::jsonrpc::global_id_generator().next_id(),
                    "method": "public/test",
                    "params": {}
                })
                .to_string();
                vec![OutgoingAction::Send(test_payload)]
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
```

- [ ] **Step 6: Update imports — remove unused, add needed**

Replace the imports block (lines 1-13):

```rust
use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::json;
use tracing::{debug, info, warn};

use crate::jsonrpc::JSONRPCResponse;
use crate::request::authentication::AuthRequest;
use crate::request::subscribe::PublicSubscribeRequest;
```

Removed: `Value` (no longer needed), `DeribitError::RequestTimeout` (only used in ws_call), `Transport` (no longer passed to run_setup).

- [ ] **Step 7: Rewrite unit tests**

Replace the entire test module (lines 316-543) with:

```rust
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
        async fn call(&self, _payload: &str, _timeout: Duration) -> Result<String> {
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
        match &actions[0] {
            OutgoingAction::Send(payload) => {
                assert!(payload.contains("public/test"));
            }
        }
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
        let caller = MockCaller::new();
        caller.queue_ok(r#"{"jsonrpc":"2.0","id":1,"result":{"interval":30}}"#);

        let result = handler.run_setup(&caller).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_run_setup_heartbeat_fails() {
        let handler = make_handler();
        let caller = MockCaller::new();
        caller.queue_err("timeout");

        let result = handler.run_setup(&caller).await;
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

        let caller = MockCaller::new();
        caller.queue_ok(r#"{"jsonrpc":"2.0","id":1,"result":{"interval":30}}"#);
        caller.queue_ok(r#"{"jsonrpc":"2.0","id":2,"result":["ticker.BTC-PERP.agg2"]}"#);

        let result = handler.run_setup(&caller).await;
        assert!(result.is_ok());
    }
}
```

- [ ] **Step 8: Run protocol unit tests**

Run: `cargo test -p nq-deribit protocol`
Expected: all protocol tests PASS

- [ ] **Step 9: Commit**

```bash
git add crates/nq-deribit/src/protocol.rs
git commit -m "refactor(protocol): remove ws_call, add notification handler

- Remove ws_call() and resubscribe_sync() — no more direct transport.recv()
- run_setup() uses &dyn JsonRpcCaller instead of &mut impl Transport
- Add handle_notification(method, text) -> Vec<OutgoingAction>
- Add JsonRpcCaller trait and OutgoingAction enum
- Remove ProtocolEvent enum and old handle_message()
- Use global ID generator instead of manual id ranges
- Rewrite tests with MockCaller

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 3: Connection — simplify eventloop, add notification dispatch

**Files:**
- Modify: `crates/nq-deribit/src/connection.rs`

Removes the ping timer branch and `message_map` from the eventloop. Changes the recv branch to directly classify & dispatch messages. Adds `JsonRpcCaller` impl for `Connection`. Updates the `run_setup` call.

- [ ] **Step 1: Add import for protocol types**

Add to the existing imports (after line 19):

```rust
use crate::protocol::{JsonRpcCaller, OutgoingAction};
```

- [ ] **Step 2: Remove `use tokio::time::Instant;`**

Delete line 13 (`use tokio::time::Instant;`). It was only used for `next_ping`, which is now handled inside Transport.

- [ ] **Step 3: Replace the inner eventloop select!**

Replace lines 397-502 (the inner loop including variable declarations and select!) with:

```rust
            // ── Main eventloop ───────────────────────────────────────
            let mut responser_map: HashMap<i64, oneshot::Sender<String>> =
                HashMap::new();
            const MAX_MAP_SIZE: usize = 1000;

            loop {
                select! {
                    biased;

                    () = ct.cancelled() => {
                        return Ok(());
                    }

                    // ── API response routing ─────────────────────────
                    Ok((id, responser)) = self.responser_rx.recv_async() => {
                        if responser_map.len() >= MAX_MAP_SIZE {
                            warn!(connection_id = self.id,
                                map_size = responser_map.len(),
                                "responser_map too large, clearing");
                            responser_map.clear();
                        }
                        responser_map.insert(id, responser);
                    }

                    // ── Outgoing API message ─────────────────────────
                    msg = self.message_rx.recv_async() => {
                        let msg = msg.with_context(|| "connection recv message")?;
                        if let Err(e) = transport.send(msg).await {
                            warn!(connection_id = self.id,
                                error = ?e,
                                "transport send error, reconnecting");
                            break;
                        }
                    }

                    // ── Incoming message dispatch ────────────────────
                    result = transport.recv() => {
                        let text = match result {
                            Ok(Some(t)) => t,
                            Ok(None) => {
                                debug!(connection_id = self.id, "transport closed");
                                break;
                            }
                            Err(e) => {
                                warn!(connection_id = self.id,
                                    error = ?e,
                                    "transport recv error, reconnecting");
                                break;
                            }
                        };

                        // Classify: response (has "id") vs notification (has "method")
                        let value: serde_json::Value = match serde_json::from_str(&text) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };

                        if let Some(id) = value.get("id").and_then(|v| v.as_i64()) {
                            // JSON-RPC response → route to waiting caller
                            if let Some(responser) = responser_map.remove(&id) {
                                let _ = responser.send(text);
                            } else {
                                warn!(connection_id = self.id, id, "no waiter for response");
                            }
                        } else if let Some(method) = value.get("method").and_then(|v| v.as_str()) {
                            // JSON-RPC notification → dispatch to Protocol handler
                            let actions = protocol.handle_notification(method, &text);
                            for action in actions {
                                match action {
                                    OutgoingAction::Send(payload) => {
                                        if let Err(e) = transport.send(payload).await {
                                            warn!(connection_id = self.id,
                                                error = ?e,
                                                "transport send (notification response) failed");
                                        }
                                    }
                                }
                            }
                        } else {
                            warn!(connection_id = self.id, "unrecognized message (no id or method)");
                        }
                    }
                }
            }
```

- [ ] **Step 4: Update `run_setup` call to pass `self` as caller**

Edit line 369. Change:

```rust
            match protocol.run_setup(&mut transport).await {
```

To:

```rust
            match protocol.run_setup(self).await {
```

- [ ] **Step 5: Add `JsonRpcCaller` impl for `Connection`**

Insert after the closing `}` of `impl Connection` (after line 507):

```rust
#[async_trait]
impl JsonRpcCaller for Connection {
    async fn call(&self, payload: &str, timeout: Duration) -> Result<String> {
        // Extract id from the JSON-RPC payload for correlation
        let value: serde_json::Value = serde_json::from_str(payload)
            .with_context(|| "JsonRpcCaller: invalid JSON payload")?;
        let id = value
            .get("id")
            .and_then(|v| v.as_i64())
            .with_context(|| "JsonRpcCaller: payload missing 'id'")?;

        let (responser_tx, responser_rx) = oneshot::channel();

        self.message_tx
            .send_async(payload.to_string())
            .await
            .with_context(|| "JsonRpcCaller: send payload")?;

        self.responser_tx
            .send_async((id, responser_tx))
            .await
            .with_context(|| "JsonRpcCaller: register responser")?;

        let resp = tokio::time::timeout(timeout, responser_rx)
            .await
            .map_err(|_| {
                anyhow::Error::from(crate::errors::DeribitError::RequestTimeout)
            })
            .with_context(|| "JsonRpcCaller: timeout")?
            .with_context(|| "JsonRpcCaller: responser dropped")?;

        Ok(resp)
    }
}
```

- [ ] **Step 6: Build check**

Run: `cargo check -p nq-deribit 2>&1`
Expected: COMPILE SUCCESS

- [ ] **Step 7: Commit**

```bash
git add crates/nq-deribit/src/connection.rs
git commit -m "refactor(connection): simplify eventloop, add notification dispatch

- Remove message_map early-arrival buffer
- Remove ping timer branch (moved into Transport)
- Classify incoming messages directly: id→oneshot, method→notification handler
- Add JsonRpcCaller impl for Connection
- run_setup now takes &dyn JsonRpcCaller instead of &mut Transport

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 4: Integration — full test suite and cleanup

**Files:**
- Check: `crates/nq-deribit/src/connection.rs`
- Check: `crates/nq-deribit/src/transport.rs`
- Check: `crates/nq-deribit/src/protocol.rs`

- [ ] **Step 1: Run full nq-deribit test suite**

Run: `cargo test -p nq-deribit 2>&1`
Expected: all tests PASS

- [ ] **Step 2: Check for compiler warnings**

Run: `cargo check -p nq-deribit 2>&1`
Expected: no warnings

Fix any unused import warnings by removing the affected `use` lines.

- [ ] **Step 3: Run full workspace build**

Run: `cargo build 2>&1`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit any cleanup**

```bash
git add -A
git commit -m "chore: cleanup unused imports after recv dispatch refactor

Co-Authored-By: Claude <noreply@anthropic.com>"
```
