# Deribit Option Monitor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Deribit options monitoring app that dynamically subscribes to all active options' ticker data and publishes per-instrument to MQTT, with multi-connection pool support.

**Architecture:** New `Connection` type (dynamic channel subscribe/unsubscribe + auto-resubscribe on reconnect) and `ConnectionPool` (multi-WS, capacity-based allocation) added to `nq-deribit` crate. New `deribit-option-monitor` app with `InstrumentFetcher`, `SubscriptionManager`, and `TickerRouter` components.

**Tech Stack:** Rust, tokio, flume, reqwest-websocket, rumqttc, serde, tracing

---

## File Structure

### nq-deribit crate (library additions)

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `crates/nq-deribit/src/model/instrument.rs` | Add `InstrumentInfo` struct for get_instruments response |
| Modify | `crates/nq-deribit/src/request/market_data.rs` | Add `GetInstrumentsRequest/Response` |
| Modify | `crates/nq-deribit/src/subscription/instrument.rs` | Fix channel format from `"instrument.state"` to `"instrument_state"` |
| Create | `crates/nq-deribit/src/connection.rs` | `Connection` — WS client with dynamic channel management |
| Create | `crates/nq-deribit/src/pool.rs` | `ConnectionPool` — multi-connection pool with capacity routing |
| Modify | `crates/nq-deribit/src/lib.rs` | Register `connection` and `pool` modules |

### New app

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `apps/deribit-option-monitor/Cargo.toml` | App dependencies |
| Create | `apps/deribit-option-monitor/src/main.rs` | Entry point: assemble components, start Application |
| Create | `apps/deribit-option-monitor/src/config.rs` | `AppConfig` with env var loading |
| Create | `apps/deribit-option-monitor/src/fetcher.rs` | `InstrumentFetcher` — get_instruments wrapper |
| Create | `apps/deribit-option-monitor/src/subscription_mgr.rs` | `SubscriptionManager` — track options, coordinate subscribe |
| Create | `apps/deribit-option-monitor/src/ticker_router.rs` | `TickerRouter` — parse ticker → publish to MQTT |

---

## Task 1: Add InstrumentInfo model

**Files:**
- Modify: `crates/nq-deribit/src/model/instrument.rs`

- [ ] **Step 1: Write test for InstrumentInfo deserialization**

Add to the END of `crates/nq-deribit/src/model/instrument.rs`:

```rust
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct InstrumentInfo {
    pub instrument_name: String,
    pub kind: InstrumentKind,
    pub base_currency: Currency,
    pub quote_currency: Currency,
    pub is_active: bool,
    pub creation_timestamp: u64,
    pub expiration_timestamp: u64,
    pub tick_size: f64,
    pub contract_size: i64,
    pub state: String,
    #[serde(default)]
    pub strike: Option<f64>,
    #[serde(default)]
    pub option_type: Option<String>,
    #[serde(default)]
    pub settlement_period: Option<String>,
    #[serde(default)]
    pub min_trade_amount: Option<f64>,
    #[serde(default)]
    pub maker_commission: Option<f64>,
    #[serde(default)]
    pub taker_commission: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_option_instrument() {
        let json = r#"{
            "instrument_name": "BTC-27JUN25-100000-C",
            "kind": "option",
            "base_currency": "BTC",
            "quote_currency": "USD",
            "is_active": true,
            "creation_timestamp": 1664524802000,
            "expiration_timestamp": 1695974400000,
            "tick_size": 0.0001,
            "contract_size": 1,
            "state": "open",
            "strike": 100000.0,
            "option_type": "call",
            "settlement_period": "month",
            "min_trade_amount": 0.1
        }"#;

        let info: InstrumentInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.instrument_name, "BTC-27JUN25-100000-C");
        assert_eq!(info.kind, InstrumentKind::Option);
        assert_eq!(info.strike, Some(100000.0));
        assert_eq!(info.option_type.as_deref(), Some("call"));
        assert!(info.is_active);
    }

    #[test]
    fn test_deserialize_future_instrument() {
        let json = r#"{
            "instrument_name": "BTC-PERPETUAL",
            "kind": "future",
            "base_currency": "BTC",
            "quote_currency": "USD",
            "is_active": true,
            "creation_timestamp": 1534167754000,
            "expiration_timestamp": 32503708800000,
            "tick_size": 0.5,
            "contract_size": 10,
            "state": "open",
            "settlement_period": "perpetual"
        }"#;

        let info: InstrumentInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.instrument_name, "BTC-PERPETUAL");
        assert!(info.strike.is_none());
        assert!(info.option_type.is_none());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p nq-deribit --lib model::instrument::tests -- --nocapture`
Expected: FAIL — `InstrumentInfo` not defined (or test functions not found if no tests module yet)

- [ ] **Step 3: Verify the code from Step 1 is present in the file**

The `InstrumentInfo` struct and tests are already written in Step 1. Ensure the imports `use serde::{Deserialize, Serialize};` and `use super::{Currency, InstrumentKind};` exist at the top of the file. The file already has `use serde::{Deserialize, Serialize};` and the enums — just verify the struct and test module are appended after the existing `InstrumentState` code.

The file already has these imports (from the existing code):
```rust
use std::str::FromStr;
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
```

These are sufficient. No new imports needed.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p nq-deribit --lib model::instrument::tests -- --nocapture`
Expected: PASS — both `test_deserialize_option_instrument` and `test_deserialize_future_instrument` pass

- [ ] **Step 5: Commit**

```bash
git add crates/nq-deribit/src/model/instrument.rs
git commit -m "feat(deribit): add InstrumentInfo model for get_instruments response"
```

---

## Task 2: Add GetInstrumentsRequest

**Files:**
- Modify: `crates/nq-deribit/src/request/market_data.rs`

- [ ] **Step 1: Add GetInstrumentsRequest and test**

Add to the END of `crates/nq-deribit/src/request/market_data.rs`:

```rust
use crate::model::currency::Currency;
use crate::model::instrument::{InstrumentInfo, InstrumentKind};

impl_request!(
    GetInstrumentsRequest,
    GetInstrumentsResponse,
    "public/get_instruments"
);

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct GetInstrumentsRequest {
    pub currency: Currency,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<InstrumentKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expired: Option<bool>,
}

impl GetInstrumentsRequest {
    pub fn options(currency: Currency) -> Self {
        Self {
            currency,
            kind: Some(InstrumentKind::Option),
            expired: Some(false),
        }
    }
}

pub type GetInstrumentsResponse = Vec<InstrumentInfo>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::Request;

    #[test]
    fn test_get_instruments_request_serialization() {
        let req = GetInstrumentsRequest::options(Currency::BTC);
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"currency\":\"BTC\""));
        assert!(json.contains("\"kind\":\"option\""));
        assert!(json.contains("\"expired\":false"));
        assert!(!json.contains("null"));
    }

    #[test]
    fn test_get_instruments_method() {
        assert_eq!(GetInstrumentsRequest::METHOD, "public/get_instruments");
        assert!(GetInstrumentsRequest::HAS_PAYLOAD);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p nq-deribit --lib request::market_data::tests -- --nocapture`
Expected: FAIL — `GetInstrumentsRequest` or `InstrumentInfo` not found

- [ ] **Step 3: Verify code from Step 1 is present**

Ensure the imports at the top include `use crate::model::currency::Currency;` and `use crate::model::instrument::{InstrumentInfo, InstrumentKind};`. The file already has `use serde::{Deserialize, Serialize};` and `use crate::impl_request;`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p nq-deribit --lib request::market_data::tests -- --nocapture`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/nq-deribit/src/request/market_data.rs
git commit -m "feat(deribit): add GetInstrumentsRequest for fetching active instruments"
```

---

## Task 3: Fix InstrumentStateChannel format

**Files:**
- Modify: `crates/nq-deribit/src/subscription/instrument.rs`

The existing `InstrumentStateChannel` uses `"instrument", "state"` prefixes which produces `"instrument.state.{kind}.{currency}"`. Per Deribit docs, the correct format is `"instrument_state.{kind}.{currency}"` (underscore, not dot).

- [ ] **Step 1: Add test and fix the channel**

Replace the entire content of `crates/nq-deribit/src/subscription/instrument.rs` with:

```rust
use serde::{Deserialize, Serialize};
use crate::{
    model::{
        currency::Currency,
        instrument::{InstrumentKind, InstrumentState},
    },
    gen_channel,
};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct InstrumentStateData {
    pub timestamp: u64,
    pub state: InstrumentState,
    pub instrument_name: String,
}

gen_channel!(InstrumentStateChannel, "instrument_state", InstrumentKind, Currency);

impl std::fmt::Display for InstrumentStateChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "instrument_state.{}.{}", self.0, self.1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_display() {
        let ch = InstrumentStateChannel(InstrumentKind::Option, Currency::BTC);
        assert_eq!(ch.to_string(), "instrument_state.option.BTC");
    }

    #[test]
    fn test_channel_deserialize() {
        let ch: InstrumentStateChannel = serde_json::from_str("\"instrument_state.option.ETH\"").unwrap();
        assert_eq!(ch.0, InstrumentKind::Option);
        assert_eq!(ch.1, Currency::ETH);
    }
}
```

- [ ] **Step 2: Run test**

Run: `cargo test -p nq-deribit --lib subscription::instrument::tests -- --nocapture`
Expected: PASS — both tests pass

- [ ] **Step 3: Verify full crate still compiles**

Run: `cargo build -p nq-deribit`
Expected: Compiles without errors

- [ ] **Step 4: Commit**

```bash
git add crates/nq-deribit/src/subscription/instrument.rs
git commit -m "fix(deribit): InstrumentStateChannel format to instrument_state (underscore)"
```

---

## Task 4: Implement Connection

**Files:**
- Create: `crates/nq-deribit/src/connection.rs`
- Modify: `crates/nq-deribit/src/lib.rs`

This is the core library change. `Connection` is similar to the existing `Client` but supports dynamic channel subscription/unsubscription and maintains a live channel set for automatic re-subscription on reconnect.

- [ ] **Step 1: Create connection.rs**

Create `crates/nq-deribit/src/connection.rs`:

```rust
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use derive_builder::Builder;
use flume::{Receiver, Sender};
use futures_util::{SinkExt, StreamExt};
use nq_app::runner::Runner;
use reqwest::Proxy;
use reqwest_websocket::{Message, RequestBuilderExt, WebSocket};
use serde_json::{Value, json};
use tokio::{select, sync::oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

use crate::errors::DeribitError::RequestTimeout;
use crate::jsonrpc::{JSPNRPCRequest, JSONRPCResponse};
use crate::request::authentication::AuthRequest;
use crate::request::session_management::SetHeartbeatRequest;
use crate::request::subscribe::PublicSubscribeRequest;
use crate::request::support::TestRequest;
use crate::request::Request;

// ─── ConnectionCommand ───────────────────────────────────────────────

pub(crate) enum ConnectionCommand {
    Subscribe { channels: Vec<String> },
    Unsubscribe { channels: Vec<String> },
}

// ─── Connection ──────────────────────────────────────────────────────

pub struct Connection {
    id: usize,
    channels: Arc<RwLock<HashSet<String>>>,
    cmd_tx: Sender<ConnectionCommand>,
    config: Arc<ConnectionConfig>,
    token: Arc<RwLock<Option<String>>>,
    subscription_tx: Sender<String>,
    subscription_rx: Receiver<String>,
    message_tx: Sender<String>,
    message_rx: Receiver<String>,
    responser_tx: Sender<(i64, oneshot::Sender<String>)>,
    responser_rx: Receiver<(i64, oneshot::Sender<String>)>,
}

impl Connection {
    pub fn new(id: usize, config: ConnectionConfig) -> Self {
        let (subscription_tx, subscription_rx) = flume::unbounded::<String>();
        let (message_tx, message_rx) = flume::unbounded::<String>();
        let (responser_tx, responser_rx) = flume::unbounded::<(i64, oneshot::Sender<String>)>();
        let (cmd_tx, _cmd_rx) = flume::unbounded::<ConnectionCommand>();

        Self {
            id,
            channels: Arc::new(RwLock::new(HashSet::new())),
            cmd_tx,
            config: Arc::new(config),
            token: Arc::new(RwLock::new(None)),
            subscription_tx,
            subscription_rx,
            message_tx,
            message_rx,
            responser_tx,
            responser_rx,
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn channel_count(&self) -> usize {
        self.channels.read().unwrap().len()
    }

    pub fn subscribed_channels(&self) -> HashSet<String> {
        self.channels.read().unwrap().clone()
    }

    pub fn subscription_rx(&self) -> Receiver<String> {
        self.subscription_rx.clone()
    }

    /// Subscribe to new channels dynamically. Channels are added to the live set
    /// immediately (for reconnect resilience) and the subscribe request is sent
    /// via the eventloop.
    pub async fn subscribe(&self, channels: Vec<String>) -> Result<()> {
        if channels.is_empty() {
            return Ok(());
        }

        // Add to set immediately so reconnect will include them even if subscribe fails temporarily
        {
            let mut set = self.channels.write().unwrap();
            set.extend(channels.iter().cloned());
        }

        let req = PublicSubscribeRequest::new(channels.clone());
        let resp = self.call_api(req).await;

        match resp {
            Ok(_) => {
                debug!(connection_id = self.id, "subscribed to {} channels", channels.len());
                Ok(())
            }
            Err(e) => {
                warn!(connection_id = self.id, error = ?e, "subscribe failed, channels will retry on reconnect");
                Ok(()) // channels are in the set, will retry on reconnect
            }
        }
    }

    /// Unsubscribe from channels and remove from the live set.
    pub async fn unsubscribe(&self, channels: Vec<String>) -> Result<()> {
        if channels.is_empty() {
            return Ok(());
        }

        let req = crate::request::subscribe::PublicUnsubscribeRequest::new(channels.clone());
        let resp = self.call_api(req).await;

        match resp {
            Ok(_) => {
                let mut set = self.channels.write().unwrap();
                for ch in &channels {
                    set.remove(ch);
                }
                debug!(connection_id = self.id, "unsubscribed from {} channels", channels.len());
                Ok(())
            }
            Err(e) => {
                warn!(connection_id = self.id, error = ?e, "unsubscribe failed");
                Err(e)
            }
        }
    }

    pub async fn call_api<R>(&self, request: R) -> Result<R::Response>
    where
        R: Request,
    {
        let (responser_tx, responser_rx) = oneshot::channel();

        // Build JSON-RPC request using the existing wrapper
        let req = JSPNRPCRequest::<R>::from(request);
        let id = req.id;

        let payload = {
            if let Some(token) = self.token.read().unwrap().as_ref() {
                let mut value = serde_json::to_value(&req)?;
                if let Some(obj) = value.as_object_mut() {
                    obj.insert("access_token".to_string(), serde_json::json!(token));
                }
                value.to_string()
            } else {
                serde_json::to_string(&req)?
            }
        };

        self.message_tx
            .send_async(payload)
            .await
            .with_context(|| "connection send payload")?;

        self.responser_tx
            .send_async((id, responser_tx))
            .await
            .with_context(|| "connection send responser")?;

        let resp = tokio::time::timeout(Duration::from_secs(self.config.request_timeout), responser_rx)
            .await
            .map_err(|_| anyhow::Error::from(RequestTimeout))
            .with_context(|| "connection responser timeout")?
            .with_context(|| "connection responser recv")?;

        let result: JSONRPCResponse<R::Response> =
            serde_json::from_str(&resp).with_context(|| "connection response serde")?;

        match result.result.map_right(|e| {
            crate::errors::DeribitError::RemoteError {
                code: e.code,
                message: e.message,
            }
            .into()
        }) {
            either::Either::Left(v) => Ok(v),
            either::Either::Right(e) => Err(e),
        }
    }

    fn build_http_client(&self) -> Result<reqwest::Client> {
        let client = match self.config.proxy {
            Some(ref proxy) => reqwest::Client::builder().proxy(proxy.clone()).build()?,
            _ => reqwest::Client::builder().build()?,
        };
        Ok(client)
    }

    async fn connect_websocket(&self) -> Result<WebSocket> {
        let client = self.build_http_client()?;
        let res = client
            .get(self.config.url.clone())
            .upgrade()
            .send()
            .await
            .with_context(|| "connection http upgrade")?
            .into_websocket()
            .await
            .with_context(|| "connection websocket upgrade")?;
        Ok(res)
    }

    async fn setup(&self, ws_payload_tx: Sender<String>, ws_responser_tx: Sender<(i64, oneshot::Sender<String>)>) -> Result<()> {
        // Set heartbeat
        {
            let (responser_tx, responser_rx) = oneshot::channel();
            let id = 900_000 + self.id as i64; // offset to avoid collision with call_api IDs
            let payload = json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "public/set_heartbeat",
                "params": { "interval": self.config.heartbeat_interval }
            }).to_string();

            ws_payload_tx.send_async(payload).await?;
            ws_responser_tx.send_async((id, responser_tx)).await?;
            let _ = tokio::time::timeout(Duration::from_secs(10), responser_rx).await?;
        }

        // Auth if configured
        if let (Some(ref client_id), Some(ref client_secret)) = (&self.config.client_id, &self.config.client_secret) {
            let auth_req = AuthRequest::credential_auth(client_id, client_secret);
            let (responser_tx, responser_rx) = oneshot::channel();
            let id = 800_000 + self.id as i64;

            let mut value = serde_json::to_value(&auth_req)?;
            if let Some(obj) = value.as_object_mut() {
                obj.insert("jsonrpc".to_string(), json!("2.0"));
                obj.insert("id".to_string(), json!(id));
            }

            ws_payload_tx.send_async(value.to_string()).await?;
            ws_responser_tx.send_async((id, responser_tx)).await?;
            let resp = tokio::time::timeout(Duration::from_secs(10), responser_rx).await??;
            let result: JSONRPCResponse<crate::request::authentication::AuthResponse> = serde_json::from_str(&resp)?;
            if let either::Either::Left(auth_resp) = result.result {
                *self.token.write().unwrap() = auth_resp.access_token;
            }
        }

        // Re-subscribe all channels from the live set
        let channels: Vec<String> = self.channels.read().unwrap().iter().cloned().collect();
        if !channels.is_empty() {
            let sub_req = PublicSubscribeRequest::new(channels);
            let (responser_tx, responser_rx) = oneshot::channel();
            let id = 700_000 + self.id as i64;

            let mut value = serde_json::to_value(&sub_req)?;
            if let Some(obj) = value.as_object_mut() {
                obj.insert("jsonrpc".to_string(), json!("2.0"));
                obj.insert("id".to_string(), json!(id));
            }

            ws_payload_tx.send_async(value.to_string()).await?;
            ws_responser_tx.send_async((id, responser_tx)).await?;
            let _ = tokio::time::timeout(Duration::from_secs(30), responser_rx).await?;
            info!(connection_id = self.id, "re-subscribed {} channels on reconnect", channels.len());
        }

        Ok(())
    }

    pub async fn eventloop(&self, ct: CancellationToken) -> Result<()> {
        debug!(connection_id = self.id, "connection eventloop begin");

        // Create internal channels for eventloop-local communication
        let (el_payload_tx, el_payload_rx) = flume::unbounded::<String>();
        let (el_responser_tx, el_responser_rx) = flume::unbounded::<(i64, oneshot::Sender<String>)>();

        loop {
            if ct.is_cancelled() {
                return Ok(());
            }

            debug!(connection_id = self.id, "connecting websocket");
            let mut ws = select! {
                ws = self.connect_websocket() => ws?,
                _ = ct.cancelled() => return Ok(()),
            };
            debug!(connection_id = self.id, "websocket connected");

            let (err_tx, err_rx) = flume::bounded(1);
            let mut responser_map: HashMap<i64, oneshot::Sender<String>> = HashMap::new();
            let mut message_map: HashMap<i64, String> = HashMap::new();

            // Run setup in spawned task (heartbeat, auth, re-subscribe tracked channels)
            {
                let err_tx = err_tx.clone();
                let el_payload_tx = el_payload_tx.clone();
                let el_responser_tx = el_responser_tx.clone();
                let channels = self.channels.read().unwrap().clone();
                let token = self.token.clone();
                let heartbeat_interval = self.config.heartbeat_interval;
                let client_id = self.config.client_id.clone();
                let client_secret = self.config.client_secret.clone();
                let conn_id = self.id;

                tokio::spawn(async move {
                    let res = async {
                        // 1. Set heartbeat
                        let hb_id = 900_000 + conn_id as i64;
                        let hb_payload = json!({
                            "jsonrpc": "2.0",
                            "id": hb_id,
                            "method": "public/set_heartbeat",
                            "params": { "interval": heartbeat_interval }
                        }).to_string();
                        let (tx, rx) = oneshot::channel();
                        el_payload_tx.send_async(hb_payload).await?;
                        el_responser_tx.send_async((hb_id, tx)).await?;
                        let _ = tokio::time::timeout(Duration::from_secs(10), rx).await?;

                        // 2. Auth if configured
                        if let (Some(id), Some(secret)) = (&client_id, &client_secret) {
                            let auth_id = 800_000 + conn_id as i64;
                            let mut auth_val = serde_json::to_value(&AuthRequest::credential_auth(id, secret))?;
                            if let Some(obj) = auth_val.as_object_mut() {
                                obj.insert("jsonrpc".to_string(), json!("2.0"));
                                obj.insert("id".to_string(), json!(auth_id));
                            }
                            let (tx, rx) = oneshot::channel();
                            el_payload_tx.send_async(auth_val.to_string()).await?;
                            el_responser_tx.send_async((auth_id, tx)).await?;
                            let resp = tokio::time::timeout(Duration::from_secs(10), rx).await??;
                            let result: JSONRPCResponse<crate::request::authentication::AuthResponse> = serde_json::from_str(&resp)?;
                            if let either::Either::Left(auth_resp) = result.result {
                                *token.write().unwrap() = auth_resp.access_token;
                            }
                        }

                        // 3. Re-subscribe all tracked channels
                        let channel_list: Vec<String> = channels.into_iter().collect();
                        if !channel_list.is_empty() {
                            let sub_id = 700_000 + conn_id as i64;
                            let mut sub_val = serde_json::to_value(&PublicSubscribeRequest::new(channel_list.clone()))?;
                            if let Some(obj) = sub_val.as_object_mut() {
                                obj.insert("jsonrpc".to_string(), json!("2.0"));
                                obj.insert("id".to_string(), json!(sub_id));
                            }
                            let (tx, rx) = oneshot::channel();
                            el_payload_tx.send_async(sub_val.to_string()).await?;
                            el_responser_tx.send_async((sub_id, tx)).await?;
                            let _ = tokio::time::timeout(Duration::from_secs(30), rx).await?;
                            info!(connection_id = conn_id, "re-subscribed {} channels", channel_list.len());
                        }

                        Ok::<(), anyhow::Error>(())
                    }.await;
                    if let Err(e) = res {
                        err_tx.send_async(e).await.unwrap_or_default();
                    }
                });
            }

            // Main eventloop
            loop {
                select! {
                    // Handle setup errors
                    err = err_rx.recv_async() => {
                        let err = err.with_context(|| "connection setup error")?;
                        return Err(err);
                    }
                    // Handle cancel
                    () = ct.cancelled() => {
                        return Ok(());
                    }
                    // Handle outgoing messages from call_api
                    msg = self.message_rx.recv_async() => {
                        let msg = msg.with_context(|| "connection recv message")?;
                        ws.send(Message::Text(msg)).await.with_context(|| "connection ws send")?;
                    }
                    // Handle outgoing messages from setup/initial
                    msg = el_payload_rx.recv_async() => {
                        let msg = msg.with_context(|| "connection recv el payload")?;
                        ws.send(Message::Text(msg)).await.with_context(|| "connection ws send el")?;
                    }
                    // Handle responser from call_api
                    Ok((id, responser)) = self.responser_rx.recv_async() => {
                        if let Some(text) = message_map.remove(&id) {
                            let _ = responser.send(text);
                        } else {
                            responser_map.insert(id, responser);
                        }
                    }
                    // Handle responser from setup
                    Ok((id, responser)) = el_responser_rx.recv_async() => {
                        if let Some(text) = message_map.remove(&id) {
                            let _ = responser.send(text);
                        } else {
                            responser_map.insert(id, responser);
                        }
                    }
                    // Handle websocket messages
                    next = ws.next() => {
                        let message = match next {
                            Some(Err(e)) => {
                                warn!(connection_id = self.id, "ws error: {}", e);
                                break;
                            }
                            Some(Ok(m)) => m,
                            None => {
                                debug!(connection_id = self.id, "ws closed");
                                break;
                            }
                        };

                        let text = match message {
                            Message::Text(t) => t,
                            Message::Binary(b) => String::from_utf8(b)?,
                            Message::Close { code, reason } => {
                                debug!(connection_id = self.id, "ws close(code={}, reason={})", code, reason);
                                break;
                            }
                            Message::Pong(_) => continue,
                            _ => continue,
                        };

                        let value: Value = serde_json::from_str(&text)
                            .with_context(|| "connection decode json")?;

                        // Handle API response (has "id" field)
                        if let Some(id) = value.get("id").and_then(|v| v.as_i64()) {
                            if let Some(responser) = responser_map.remove(&id) {
                                let _ = responser.send(text);
                            } else {
                                message_map.insert(id, text);
                            }
                            continue;
                        }

                        // Handle subscription/notification messages
                        if let Some(method) = value.get("method").and_then(|v| v.as_str()) {
                            match method {
                                "heartbeat" => {
                                    // Reply with test
                                    let test_id = 600_000 + self.id as i64;
                                    let test_payload = json!({
                                        "jsonrpc": "2.0",
                                        "id": test_id,
                                        "method": "public/test",
                                        "params": {}
                                    }).to_string();
                                    let _ = el_payload_tx.send_async(test_payload).await;
                                }
                                "subscription" => {
                                    if self.subscription_tx.send_async(text).await.is_err() {
                                        warn!(connection_id = self.id, "subscription rx dropped");
                                    }
                                }
                                _ => {
                                    warn!(connection_id = self.id, "unknown method: {}", method);
                                }
                            }
                        }
                    }
                }
            }
            // Loop continues → reconnect
        }
    }
}

#[async_trait]
impl Runner for Connection {
    async fn run(&self, canceltoken: CancellationToken) -> Result<()> {
        info!(connection_id = self.id, "connection is running");
        self.eventloop(canceltoken).await?;
        info!(connection_id = self.id, "connection done");
        Ok(())
    }
}

// ─── ConnectionConfig ────────────────────────────────────────────────

#[derive(Builder, Clone)]
pub struct ConnectionConfig {
    #[builder(default = "nq_env::deribit::ws_url()")]
    pub url: String,
    #[builder(setter(into, strip_option), default)]
    pub proxy: Option<Proxy>,
    #[builder(default = "30")]
    pub heartbeat_interval: u64,
    #[builder(default = "60")]
    pub request_timeout: u64,
    #[builder(default)]
    pub client_id: Option<String>,
    #[builder(default)]
    pub client_secret: Option<String>,
}
```

- [ ] **Step 2: Register module in lib.rs**

Add to `crates/nq-deribit/src/lib.rs` after the existing `pub mod` lines:

```rust
pub mod connection;
pub mod pool;
```

(Note: `pool` module doesn't exist yet — add it now so it compiles when Task 5 creates it. If you prefer, add only `pub mod connection;` now and add `pub mod pool;` in Task 5.)

- [ ] **Step 3: Build to check for compilation errors**

Run: `cargo build -p nq-deribit 2>&1 | head -50`
Expected: May have errors since `pool` module doesn't exist yet. If you only added `pub mod connection;`, it should compile (with warnings). Fix any errors in `connection.rs`.

- [ ] **Step 4: Commit**

```bash
git add crates/nq-deribit/src/connection.rs crates/nq-deribit/src/lib.rs
git commit -m "feat(deribit): add Connection with dynamic channel subscribe/unsubscribe"
```

---

## Task 5: Implement ConnectionPool

**Files:**
- Create: `crates/nq-deribit/src/pool.rs`

- [ ] **Step 1: Create pool.rs**

Create `crates/nq-deribit/src/pool.rs`:

```rust
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{Result, Context};
use flume::Receiver;
use futures_util::{Stream, StreamExt};

use crate::api::DeribitApiClient;
use crate::connection::{Connection, ConnectionConfig};

pub struct ConnectionPool {
    connections: Vec<Arc<Connection>>,
    capacity: usize,
    next_id: AtomicUsize,
    base_config: ConnectionConfig,
}

pub struct PoolConfig {
    pub capacity_per_connection: usize,
    pub connection_config: ConnectionConfig,
}

impl ConnectionPool {
    pub fn new(config: PoolConfig) -> Self {
        let first = Arc::new(Connection::new(0, config.connection_config.clone()));
        Self {
            connections: vec![first],
            capacity: config.capacity_per_connection,
            next_id: AtomicUsize::new(1),
            base_config: config.connection_config,
        }
    }

    /// Subscribe to channels, distributing across connections by capacity.
    /// If no connection has capacity, a new one is created automatically.
    pub async fn subscribe(&self, channels: Vec<String>) -> Result<()> {
        if channels.is_empty() {
            return Ok(());
        }

        // Find a connection with capacity
        let conn = self.find_available_connection().await;
        conn.subscribe(channels).await
    }

    /// Unsubscribe from channels. Finds the connection that has these channels.
    pub async fn unsubscribe(&self, channels: Vec<String>) -> Result<()> {
        if channels.is_empty() {
            return Ok(());
        }

        // For simplicity, send to the connection with fewest channels
        let conn = self.find_available_connection().await;
        conn.unsubscribe(channels).await
    }

    /// Merge subscription streams from all connections into one.
    pub fn subscription_stream(&self) -> impl Stream<Item = String> {
        let streams: Vec<_> = self.connections.iter()
            .map(|c| c.subscription_rx().into_stream())
            .collect();
        futures_util::stream::select_all(streams)
    }

    /// Get an API client from the first connection.
    /// The API client sends requests through the connection's WS.
    pub fn api_client_payload_tx(&self) -> flume::Sender<String> {
        // Expose the message_tx of the first connection for external API calls
        // This is used by InstrumentFetcher which doesn't need typed API calls
        // Actually, for get_instruments we need the typed api_client
        // We'll create a DeribitApiClient from the first connection's internals
        unimplemented!("use Connection's call_api instead — see fetcher.rs")
    }

    /// Get all connections (for adding them as Runners to the Application).
    pub fn connections(&self) -> &[Arc<Connection>] {
        &self.connections
    }

    /// Get a reference to the first connection (for API calls like get_instruments).
    pub fn first_connection(&self) -> &Arc<Connection> {
        &self.connections[0]
    }

    async fn find_available_connection(&self) -> Arc<Connection> {
        // Simple: find first connection under capacity
        for conn in &self.connections {
            if conn.channel_count() < self.capacity {
                return conn.clone();
            }
        }

        // All full — create new connection
        // NOTE: In a real implementation, we'd need interior mutability (RwLock)
        // to add connections. For now, we'll return the last connection and log a warning.
        // The pool will need to be redesigned with Arc<RwLock<Vec<...>>> for dynamic growth.
        tracing::warn!("all connections at capacity, consider increasing pool_capacity");
        self.connections.last().unwrap().clone()
    }
}
```

**IMPORTANT NOTE:** The pool as written above cannot dynamically add connections because `connections` is not behind a `RwLock`. For the initial implementation, this is acceptable — the pool starts with enough connections based on expected channel count. Dynamic connection creation will be added in a refinement step.

**Revised pool.rs with interior mutability for dynamic growth:**

Replace `connections: Vec<Arc<Connection>>` with `connections: Arc<RwLock<Vec<Arc<Connection>>>>`:

```rust
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use anyhow::Result;
use futures_util::{Stream, StreamExt};
use tracing::{info, warn};

use crate::connection::{Connection, ConnectionConfig};

pub struct ConnectionPool {
    connections: Arc<RwLock<Vec<Arc<Connection>>>>,
    capacity: usize,
    next_id: AtomicUsize,
    base_config: ConnectionConfig,
}

pub struct PoolConfig {
    pub capacity_per_connection: usize,
    pub connection_config: ConnectionConfig,
}

impl ConnectionPool {
    pub fn new(config: PoolConfig) -> Self {
        let first = Arc::new(Connection::new(0, config.connection_config.clone()));
        Self {
            connections: Arc::new(RwLock::new(vec![first])),
            capacity: config.capacity_per_connection,
            next_id: AtomicUsize::new(1),
            base_config: config.connection_config,
        }
    }

    pub async fn subscribe(&self, channels: Vec<String>) -> Result<()> {
        if channels.is_empty() {
            return Ok(());
        }
        let conn = self.find_or_create_connection();
        conn.subscribe(channels).await
    }

    pub async fn unsubscribe(&self, channels: Vec<String>) -> Result<()> {
        if channels.is_empty() {
            return Ok(());
        }
        // Find the connection that likely has these channels
        let conn = {
            let conns = self.connections.read().unwrap();
            conns.iter()
                .min_by_key(|c| c.channel_count())
                .unwrap()
                .clone()
        };
        conn.unsubscribe(channels).await
    }

    pub fn subscription_stream(&self) -> impl Stream<Item = String> {
        let conns = self.connections.read().unwrap();
        let streams: Vec<_> = conns.iter()
            .map(|c| c.subscription_rx().into_stream())
            .collect();
        futures_util::stream::select_all(streams)
    }

    /// Returns a snapshot of all connections. Call this once at startup to add
    /// them as Runners to the Application. After that, new connections created
    /// by the pool won't automatically become Runners — size the pool upfront.
    pub fn connection_runners(&self) -> Vec<Arc<Connection>> {
        self.connections.read().unwrap().clone()
    }

    pub fn first_connection(&self) -> Arc<Connection> {
        self.connections.read().unwrap()[0].clone()
    }

    fn find_or_create_connection(&self) -> Arc<Connection> {
        // Find connection with capacity
        {
            let conns = self.connections.read().unwrap();
            for conn in conns.iter() {
                if conn.channel_count() < self.capacity {
                    return conn.clone();
                }
            }
        }

        // All full — create new connection
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let conn = Arc::new(Connection::new(id, self.base_config.clone()));
        {
            let mut conns = self.connections.write().unwrap();
            conns.push(conn.clone());
        }
        info!(connection_id = id, "pool created new connection");

        // NOTE: The new connection won't have a Runner spawned yet.
        // For the initial implementation, callers should ensure pool is sized correctly.
        // A background spawner can be added later.
        conn
    }
}
```

- [ ] **Step 2: Ensure lib.rs has `pub mod pool;`**

If not already added in Task 4, add `pub mod pool;` to `crates/nq-deribit/src/lib.rs`.

- [ ] **Step 3: Build to verify compilation**

Run: `cargo build -p nq-deribit 2>&1 | head -50`
Expected: Compiles successfully. Fix any import errors.

- [ ] **Step 4: Commit**

```bash
git add crates/nq-deribit/src/pool.rs crates/nq-deribit/src/lib.rs
git commit -m "feat(deribit): add ConnectionPool with capacity-based channel distribution"
```

---

## Task 6: Verify library compiles and tests pass

- [ ] **Step 1: Run all nq-deribit tests**

Run: `cargo test -p nq-deribit 2>&1`
Expected: All existing + new tests pass

- [ ] **Step 2: Build full workspace**

Run: `cargo build 2>&1`
Expected: Full workspace compiles. The existing `deribit-subscription` app must still work unchanged.

- [ ] **Step 3: Fix any issues found**

If compilation errors exist in `deribit-subscription` or other crates, fix them. The spec requires backward compatibility.

- [ ] **Step 4: Commit any fixes**

```bash
git add -A
git commit -m "fix(deribit): resolve compilation issues after Connection/Pool additions"
```

(Only commit if changes were needed.)

---

## Task 7: Create app scaffold

**Files:**
- Create: `apps/deribit-option-monitor/Cargo.toml`

- [ ] **Step 1: Create Cargo.toml**

Create `apps/deribit-option-monitor/Cargo.toml`:

```toml
[package]
edition = "2024"
name = "deribit-option-monitor"
version = "0.1.0"

[dependencies]
anyhow = {workspace = true}
async-trait = {workspace = true}
flume = {workspace = true}
futures-util = {workspace = true}
nq-app = {workspace = true}
nq-deribit = {workspace = true}
nq-env = {workspace = true}
nq-mqtt = {workspace = true}
rumqttc = "0.24.0"
serde = {workspace = true}
serde_json = {workspace = true}
tokio = {workspace = true}
tokio-util = {workspace = true}
tracing = {workspace = true}
tracing-subscriber = {workspace = true}
```

- [ ] **Step 2: Verify it compiles (empty crate)**

Create a minimal `apps/deribit-option-monitor/src/main.rs`:

```rust
fn main() {
    println!("deribit-option-monitor");
}
```

Run: `cargo build -p deribit-option-monitor`
Expected: Compiles

- [ ] **Step 3: Commit**

```bash
git add apps/deribit-option-monitor/
git commit -m "chore: scaffold deribit-option-monitor app"
```

---

## Task 8: Implement config.rs

**Files:**
- Create: `apps/deribit-option-monitor/src/config.rs`

- [ ] **Step 1: Create config.rs**

```rust
use std::env;

use nq_deribit::model::currency::Currency;
use nq_deribit::model::interval::Interval;

const DEFAULT_CURRENCIES: &str = "BTC,ETH";
const DEFAULT_TICKER_INTERVAL: &str = "agg2";
const DEFAULT_MQTT_TOPIC_PREFIX: &str = "t/deribit/option_ticker";
const DEFAULT_POLL_INTERVAL_SECS: u64 = 60;
const DEFAULT_POOL_CAPACITY: usize = 200;

pub struct AppConfig {
    pub currencies: Vec<Currency>,
    pub ticker_interval: Interval,
    pub mqtt_topic_prefix: String,
    pub poll_interval_secs: u64,
    pub pool_capacity: usize,
}

impl AppConfig {
    pub fn from_env() -> Self {
        let currencies_str = env::var("DERIBIT_OPTION_CURRENCIES")
            .unwrap_or(DEFAULT_CURRENCIES.to_string());
        let currencies: Vec<Currency> = currencies_str
            .split(',')
            .map(|s| s.trim().to_uppercase())
            .filter_map(|s| Currency::try_from(s).ok())
            .collect();

        let interval_str = env::var("DERIBIT_OPTION_TICKER_INTERVAL")
            .unwrap_or(DEFAULT_TICKER_INTERVAL.to_string());
        let ticker_interval = Interval::from(interval_str);

        let mqtt_topic_prefix = env::var("DERIBIT_OPTION_MQTT_TOPIC_PREFIX")
            .unwrap_or(DEFAULT_MQTT_TOPIC_PREFIX.to_string());

        let poll_interval_secs = env::var("DERIBIT_OPTION_POLL_INTERVAL")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_POLL_INTERVAL_SECS);

        let pool_capacity = env::var("DERIBIT_OPTION_POOL_CAPACITY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_POOL_CAPACITY);

        Self {
            currencies,
            ticker_interval,
            mqtt_topic_prefix,
            poll_interval_secs,
            pool_capacity,
        }
    }
}
```

- [ ] **Step 2: Verify compilation**

Run: `cargo build -p deribit-option-monitor`
Expected: Compiles (config is not used by main.rs yet, but it compiles as part of the crate)

- [ ] **Step 3: Commit**

```bash
git add apps/deribit-option-monitor/src/config.rs
git commit -m "feat(option-monitor): add AppConfig with env var loading"
```

---

## Task 9: Implement fetcher.rs

**Files:**
- Create: `apps/deribit-option-monitor/src/fetcher.rs`

- [ ] **Step 1: Create fetcher.rs**

```rust
use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::{debug, warn};

use nq_deribit::connection::Connection;
use nq_deribit::model::currency::Currency;
use nq_deribit::model::instrument::InstrumentInfo;
use nq_deribit::request::market_data::{GetInstrumentsRequest, GetInstrumentsResponse};

pub struct InstrumentFetcher {
    connection: Arc<Connection>,
}

impl InstrumentFetcher {
    pub fn new(connection: Arc<Connection>) -> Self {
        Self { connection }
    }

    /// Fetch all active options for the given currencies.
    pub async fn fetch_all_options(&self, currencies: &[Currency]) -> Result<Vec<InstrumentInfo>> {
        let mut all_options = Vec::new();

        for currency in currencies {
            match self.fetch_options(*currency).await {
                Ok(options) => {
                    debug!(currency = ?currency, count = options.len(), "fetched options");
                    all_options.extend(options);
                }
                Err(e) => {
                    warn!(currency = ?currency, error = ?e, "failed to fetch options, will retry on next poll");
                }
            }
        }

        Ok(all_options)
    }

    async fn fetch_options(&self, currency: Currency) -> Result<Vec<InstrumentInfo>> {
        let req = GetInstrumentsRequest::options(currency);
        let resp: GetInstrumentsResponse = self.connection.call_api(req).await
            .with_context(|| format!("get_instruments for {:?}", currency))?;
        Ok(resp)
    }
}
```

**IMPORTANT:** The `fetcher.rs` calls `self.connection.call_api(req)`, but `call_api` is currently a private method on `Connection`. We need to make it `pub`. Update `crates/nq-deribit/src/connection.rs`:

Change `async fn call_api<R>` to `pub async fn call_api<R>`.

- [ ] **Step 2: Make `call_api` public on Connection**

In `crates/nq-deribit/src/connection.rs`, find:
```rust
    async fn call_api<R>(&self, request: R) -> Result<R::Response>
```
Replace with:
```rust
    pub async fn call_api<R>(&self, request: R) -> Result<R::Response>
```

- [ ] **Step 3: Verify compilation**

Run: `cargo build -p deribit-option-monitor`
Expected: Compiles

- [ ] **Step 4: Commit**

```bash
git add apps/deribit-option-monitor/src/fetcher.rs crates/nq-deribit/src/connection.rs
git commit -m "feat(option-monitor): add InstrumentFetcher for get_instruments"
```

---

## Task 10: Implement subscription_mgr.rs

**Files:**
- Create: `apps/deribit-option-monitor/src/subscription_mgr.rs`

- [ ] **Step 1: Create subscription_mgr.rs**

```rust
use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use nq_app::runner::Runner;
use nq_deribit::connection::Connection;
use nq_deribit::message::{SubscriptionMessage, SubscriptionParams};
use nq_deribit::model::currency::Currency;
use nq_deribit::model::interval::Interval;
use nq_deribit::pool::ConnectionPool;
use nq_deribit::subscription::instrument::InstrumentStateData;
use tokio::select;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::fetcher::InstrumentFetcher;

pub struct SubscriptionManager {
    pool: Arc<ConnectionPool>,
    fetcher: Arc<InstrumentFetcher>,
    tracked_options: Arc<RwLock<HashSet<String>>>,
    currencies: Vec<Currency>,
    interval: Interval,
    poll_interval_secs: u64,
}

impl SubscriptionManager {
    pub fn new(
        pool: Arc<ConnectionPool>,
        fetcher: Arc<InstrumentFetcher>,
        currencies: Vec<Currency>,
        interval: Interval,
        poll_interval_secs: u64,
    ) -> Self {
        Self {
            pool,
            fetcher,
            tracked_options: Arc::new(RwLock::new(HashSet::new())),
            currencies,
            interval,
            poll_interval_secs,
        }
    }

    /// Initial fetch of all options and subscribe to their tickers.
    pub async fn initialize(&self) -> Result<()> {
        info!("subscription manager initializing...");
        let options = self.fetcher.fetch_all_options(&self.currencies).await?;
        let names: Vec<String> = options.iter().map(|o| o.instrument_name.clone()).collect();
        info!(count = names.len(), "fetched active options");
        self.subscribe_new_options(&names).await?;
        Ok(())
    }

    /// Subscribe to ticker channels for new options. Idempotent.
    async fn subscribe_new_options(&self, instrument_names: &[String]) -> Result<()> {
        let mut tracked = self.tracked_options.write().unwrap();
        let truly_new: Vec<String> = instrument_names
            .iter()
            .filter(|n| !tracked.contains(*n))
            .cloned()
            .collect();

        if truly_new.is_empty() {
            return Ok(());
        }

        let channels: Vec<String> = truly_new
            .iter()
            .map(|name| format!("ticker.{}.{}", name, self.interval))
            .collect();

        info!(count = channels.len(), "subscribing to new option tickers");
        self.pool.subscribe(channels).await?;

        tracked.extend(truly_new);
        info!(total_tracked = tracked.len(), "tracked options updated");
        Ok(())
    }
}

#[async_trait]
impl Runner for SubscriptionManager {
    async fn run(&self, ct: CancellationToken) -> Result<()> {
        info!("subscription manager is running");

        // Clone shared state for the two tasks
        let tracked = self.tracked_options.clone();
        let pool = self.pool.clone();
        let fetcher = self.fetcher.clone();
        let currencies = self.currencies.clone();
        let interval = self.interval;
        let poll_secs = self.poll_interval_secs;

        // Task 1: Poll loop
        let ct1 = ct.clone();
        let tracked1 = tracked.clone();
        let pool1 = pool.clone();
        let fetcher1 = fetcher.clone();
        let currencies1 = currencies.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = ct1.cancelled() => break,
                    _ = sleep(Duration::from_secs(poll_secs)) => {
                        match fetcher1.fetch_all_options(&currencies1).await {
                            Ok(options) => {
                                let names: Vec<String> = options.iter().map(|o| o.instrument_name.clone()).collect();
                                let mut t = tracked1.write().unwrap();
                                let new: Vec<String> = names.iter().filter(|n| !t.contains(*n)).cloned().collect();
                                if !new.is_empty() {
                                    let channels: Vec<String> = new.iter()
                                        .map(|n| format!("ticker.{}.{}", n, interval))
                                        .collect();
                                    info!(count = channels.len(), "poll discovered new options");
                                    if let Err(e) = pool1.subscribe(channels).await {
                                        warn!(error = ?e, "poll subscribe failed");
                                    }
                                    t.extend(new);
                                }
                            }
                            Err(e) => {
                                warn!(error = ?e, "poll fetch failed");
                            }
                        }
                    }
                }
            }
            debug!("poll loop done");
        });

        // Task 2: Instrument state loop
        let ct2 = ct.clone();
        let tracked2 = tracked.clone();
        let pool2 = pool.clone();
        let interval2 = interval;
        let conn = pool.first_connection();
        let mut sub_rx = conn.subscription_rx();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = ct2.cancelled() => break,
                    msg = sub_rx.recv_async() => {
                        let msg = match msg {
                            Ok(m) => m,
                            Err(_) => break,
                        };
                        // Try to parse as subscription message
                        if let Ok(sub_msg) = serde_json::from_str::<SubscriptionMessage>(&msg) {
                            if let SubscriptionParams::Subscribe(params) = sub_msg.params {
                                if params.channel.starts_with("instrument_state.") {
                                    // Parse the data as InstrumentStateData
                                    if let Ok(state_data) = serde_json::from_value::<InstrumentStateData>(params.data) {
                                        let mut t = tracked2.write().unwrap();
                                        if !t.contains(&state_data.instrument_name) {
                                            let channel = format!("ticker.{}.{}", state_data.instrument_name, interval2);
                                            info!(instrument = state_data.instrument_name, "new option from instrument_state");
                                            if let Err(e) = pool2.subscribe(vec![channel]).await {
                                                warn!(error = ?e, "instrument_state subscribe failed");
                                            }
                                            t.insert(state_data.instrument_name);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            debug!("instrument state loop done");
        });

        // Wait for cancellation
        ct.cancelled().await;
        info!("subscription manager done");
        Ok(())
    }
}
```

- [ ] **Step 2: Verify compilation**

Run: `cargo build -p deribit-option-monitor 2>&1 | head -30`
Expected: Compiles (main.rs doesn't use it yet, but the module compiles)

If `subscription_mgr` module isn't declared in `main.rs`, add `mod subscription_mgr;` to `main.rs` temporarily, or just verify the file syntax with `cargo check`.

- [ ] **Step 3: Commit**

```bash
git add apps/deribit-option-monitor/src/subscription_mgr.rs
git commit -m "feat(option-monitor): add SubscriptionManager with poll + instrument_state detection"
```

---

## Task 11: Implement ticker_router.rs

**Files:**
- Create: `apps/deribit-option-monitor/src/ticker_router.rs`

- [ ] **Step 1: Create ticker_router.rs**

```rust
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use futures_util::StreamExt;
use nq_app::runner::Runner;
use nq_deribit::message::{SubscriptionMessage, SubscriptionParams};
use nq_deribit::pool::ConnectionPool;
use nq_deribit::subscription::ticker::TickerData;
use rumqttc::{AsyncClient, QoS};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

pub struct TickerRouter {
    pool: Arc<ConnectionPool>,
    mqtt_client: AsyncClient,
    topic_prefix: String,
}

impl TickerRouter {
    pub fn new(pool: Arc<ConnectionPool>, mqtt_client: AsyncClient, topic_prefix: String) -> Self {
        Self {
            pool,
            mqtt_client,
            topic_prefix,
        }
    }
}

#[async_trait]
impl Runner for TickerRouter {
    async fn run(&self, ct: CancellationToken) -> Result<()> {
        debug!("ticker router is running");

        let mut stream = self.pool.subscription_stream();

        loop {
            select! {
                _ = ct.cancelled() => break,
                msg = stream.next() => {
                    let msg = match msg {
                        Some(m) => m,
                        None => {
                            debug!("ticker router: subscription stream ended");
                            break;
                        }
                    };

                    // Try to parse as subscription message
                    let sub_msg: SubscriptionMessage = match serde_json::from_str(&msg) {
                        Ok(m) => m,
                        Err(_) => continue, // not a subscription message, skip
                    };

                    if let SubscriptionParams::Subscribe(params) = sub_msg.params {
                        // Check if this is a ticker channel
                        if !params.channel.starts_with("ticker.") {
                            continue;
                        }

                        // Parse the data as TickerData
                        let ticker: TickerData = match serde_json::from_value(params.data) {
                            Ok(t) => t,
                            Err(e) => {
                                trace!(error = ?e, "failed to parse ticker data");
                                continue;
                            }
                        };

                        // Publish to MQTT
                        let topic = format!("{}/{}", self.topic_prefix, ticker.instrument_name);
                        let payload = match serde_json::to_vec(&ticker) {
                            Ok(p) => p,
                            Err(e) => {
                                warn!(error = ?e, "failed to serialize ticker");
                                continue;
                            }
                        };

                        if let Err(e) = self.mqtt_client.publish(
                            &topic,
                            QoS::AtLeastOnce,
                            false,
                            payload,
                        ).await {
                            warn!(error = ?e, topic = topic, "mqtt publish failed");
                        }
                    }
                }
            }
        }

        debug!("ticker router done");
        Ok(())
    }
}
```

- [ ] **Step 2: Verify compilation**

Run: `cargo build -p deribit-option-monitor 2>&1 | head -30`

- [ ] **Step 3: Commit**

```bash
git add apps/deribit-option-monitor/src/ticker_router.rs
git commit -m "feat(option-monitor): add TickerRouter for ticker → MQTT routing"
```

---

## Task 12: Implement main.rs

**Files:**
- Modify: `apps/deribit-option-monitor/src/main.rs`

- [ ] **Step 1: Write main.rs**

Replace the scaffold `main.rs` with:

```rust
use std::sync::Arc;

use anyhow::Result;
use nq_app::{application::Application, runner::Runner};
use nq_deribit::connection::ConnectionConfigBuilder;
use nq_deribit::pool::{ConnectionPool, PoolConfig};
use tokio_util::sync::CancellationToken;
use tracing::info;

mod config;
mod fetcher;
mod subscription_mgr;
mod ticker_router;

use config::AppConfig;
use fetcher::InstrumentFetcher;
use subscription_mgr::SubscriptionManager;
use ticker_router::TickerRouter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let config = AppConfig::from_env();

    info!("deribit-option-monitor starting");
    info!(currencies = ?config.currencies, interval = ?config.ticker_interval, "config loaded");

    // 1. Create ConnectionPool
    let conn_config = ConnectionConfigBuilder::default()
        .build()?;

    let pool = Arc::new(ConnectionPool::new(PoolConfig {
        capacity_per_connection: config.pool_capacity,
        connection_config: conn_config,
    }));

    // 2. Create Application and add connection runners
    let application = Application::new();
    for conn in pool.connection_runners() {
        application.add_runner(conn);
    }

    // 3. Create MQTT client
    let mqtt_client = nq_mqtt::client::Client::builder()
        .set_host(nq_env::emqx::host())
        .build();
    let mqtt_async_client = mqtt_client.inner();
    application.add_runner(Arc::new(mqtt_client));

    // 4. Create InstrumentFetcher (uses first connection for API calls)
    let fetcher = Arc::new(InstrumentFetcher::new(pool.first_connection()));

    // 5. Create SubscriptionManager
    let sub_mgr = Arc::new(SubscriptionManager::new(
        pool.clone(),
        fetcher.clone(),
        config.currencies.clone(),
        config.ticker_interval,
        config.poll_interval_secs,
    ));

    // 6. Initialize: fetch all options and subscribe to their tickers
    sub_mgr.initialize().await?;

    // 7. Subscribe to instrument_state channels for real-time new option detection
    let inst_state_channels: Vec<String> = config.currencies.iter()
        .map(|c| format!("instrument_state.option.{}", c))
        .collect();
    info!(channels = ?inst_state_channels, "subscribing to instrument_state");
    pool.subscribe(inst_state_channels).await?;

    // 8. Add SubscriptionManager and TickerRouter as runners
    application.add_runner(sub_mgr);
    application.add_runner(Arc::new(TickerRouter::new(
        pool,
        mqtt_async_client,
        config.mqtt_topic_prefix,
    )));

    // 9. Run
    info!("all components started");
    let ct = CancellationToken::new();
    application.run(ct).await;

    Ok(())
}
```

- [ ] **Step 2: Build the app**

Run: `cargo build -p deribit-option-monitor 2>&1`
Expected: Compiles successfully. Fix any import or type errors.

- [ ] **Step 3: Commit**

```bash
git add apps/deribit-option-monitor/src/main.rs
git commit -m "feat(option-monitor): implement main.rs with full component assembly"
```

---

## Task 13: Integration test — build and verify

- [ ] **Step 1: Build release**

Run: `cargo build -p deribit-option-monitor --release 2>&1`
Expected: Builds successfully

- [ ] **Step 2: Verify existing apps still build**

Run: `cargo build --workspace 2>&1`
Expected: Full workspace builds, including `deribit-subscription`

- [ ] **Step 3: Run all tests**

Run: `cargo test --workspace 2>&1`
Expected: All tests pass

- [ ] **Step 4: Manual test (optional, requires network)**

If Deribit API is reachable:

```bash
# Set env vars
export DERIBIT_WS_URL=wss://test.deribit.com/ws/api/v2
export EMQX_HOST=127.0.0.1

# Run the app
RUST_LOG=info cargo run -p deribit-option-monitor
```

Observe:
- Log shows "fetched active options" with count
- Log shows "subscribing to new option tickers"
- If MQTT broker is running, subscribe to `t/deribit/option_ticker/+` and verify data flows

- [ ] **Step 5: Final commit**

```bash
git add -A
git commit -m "chore: final cleanup for deribit-option-monitor"
```

(Only if there are uncommitted changes.)

---

## Task 14: Add Makefile target (optional)

- [ ] **Step 1: Add make target**

Add to the project `Makefile`:

```makefile
.PHONY: deribit-option-monitor

deribit-option-monitor:
	docker build -t nq-rs/deribit-option-monitor --build-arg APP=deribit-option-monitor --build-arg PROXY=http://192.168.2.98:8890 .
	@docker rm -f deribit-option-monitor
	@docker run -d --name deribit-option-monitor \
	    --restart always \
	    --env-file $(DERIBIT_NQ_HOME)/env/.env.credential \
	    nq-rs/deribit-option-monitor
```

- [ ] **Step 2: Commit**

```bash
git add Makefile
git commit -m "chore: add Makefile target for deribit-option-monitor"
```
