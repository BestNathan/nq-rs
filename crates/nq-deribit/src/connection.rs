use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use derive_builder::Builder;
use flume::{Receiver, Sender};
use reqwest::Proxy;
use tokio::select;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::jsonrpc::{JSPNRPCRequest, JSONRPCResponse};
use crate::protocol::{JsonRpcCaller, OutgoingAction, ProtocolHandler};
use crate::request::Request;
use crate::transport::{Transport, WsTransportImpl};

/// WebSocket-level ping interval (seconds). Sends a Ping frame at this interval
/// to keep the connection alive and detect dead connections early.
const PING_INTERVAL_SECS: u64 = 15;

// ─── SetupCaller ──────────────────────────────────────────────────────

/// Implements JsonRpcCaller via direct transport access.
/// Used during the setup phase before the main select! eventloop starts.
struct SetupCaller<'a> {
    transport: &'a mut (dyn Transport + 'a),
}

#[async_trait]
impl JsonRpcCaller for SetupCaller<'_> {
    async fn call(&mut self, payload: &str, timeout: Duration) -> Result<String> {
        let id = {
            let value: serde_json::Value = serde_json::from_str(payload)
                .with_context(|| "SetupCaller: invalid JSON")?;
            value
                .get("id")
                .and_then(|v| v.as_i64())
                .with_context(|| "SetupCaller: missing id")?
        };

        self.transport
            .send(payload.to_string())
            .await
            .with_context(|| "SetupCaller: send")?;

        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining =
                deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(anyhow::Error::from(
                    crate::errors::DeribitError::RequestTimeout,
                ));
            }

            let result = tokio::time::timeout(remaining, self.transport.recv())
                .await
                .map_err(|_| {
                    anyhow::Error::from(crate::errors::DeribitError::RequestTimeout)
                })?;

            let text = match result {
                Ok(Some(t)) => t,
                Ok(None) => {
                    return Err(anyhow::anyhow!(
                        "SetupCaller: transport closed"
                    ))
                }
                Err(e) => {
                    return Err(anyhow::Error::new(e)
                        .context("SetupCaller: transport error"))
                }
            };

            let value: serde_json::Value = serde_json::from_str(&text)
                .with_context(|| "SetupCaller: invalid JSON response")?;

            if let Some(resp_id) = value.get("id").and_then(|v| v.as_i64()) {
                if resp_id == id {
                    return Ok(text);
                }
            }
            // Non-matching messages (notifications) are discarded during setup.
            // This is acceptable because setup runs before any subscriptions
            // are active.
        }
    }
}

// ─── Connection ──────────────────────────────────────────────────────

pub struct Connection {
    id: usize,
    channels: Arc<RwLock<HashSet<String>>>,
    config: Arc<ConnectionConfig>,
    token: Arc<RwLock<Option<String>>>,
    subscription_broadcast_tx: std::sync::OnceLock<tokio::sync::broadcast::Sender<String>>,
    /// Shared HTTP client — created once, reused across all reconnects.
    client: reqwest::Client,
    message_tx: Sender<String>,
    message_rx: Receiver<String>,
    responser_tx: Sender<(i64, oneshot::Sender<String>)>,
    responser_rx: Receiver<(i64, oneshot::Sender<String>)>,
}

impl Connection {
    pub fn new(id: usize, config: ConnectionConfig) -> Self {
        // Build the shared HTTP client once — this is the key fix for the
        // reconnection failure: previously a new client was created on every
        // reconnect, wasting resources and triggering proxy rate limits.
        let client = match config.proxy {
            Some(ref proxy) => reqwest::Client::builder()
                .proxy(proxy.clone())
                .build()
                .expect("reqwest::Client::build failed"),
            _ => reqwest::Client::builder()
                .build()
                .expect("reqwest::Client::build failed"),
        };

        let msg_cap = config.message_channel_capacity;
        let (message_tx, message_rx) = flume::bounded::<String>(msg_cap);
        let resp_cap = config.responser_channel_capacity;
        let (responser_tx, responser_rx) =
            flume::bounded::<(i64, oneshot::Sender<String>)>(resp_cap);

        Self {
            id,
            channels: Arc::new(RwLock::new(HashSet::new())),
            config: Arc::new(config),
            token: Arc::new(RwLock::new(None)),
            subscription_broadcast_tx: std::sync::OnceLock::new(),
            client,
            message_tx,
            message_rx,
            responser_tx,
            responser_rx,
        }
    }

    // ── Public API (unchanged signatures) ───────────────────────────

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn channel_count(&self) -> usize {
        self.channels.read().unwrap().len()
    }

    /// Synchronously add channels to the tracked set without making API calls.
    pub fn pre_track_channels(&self, channels: &[String]) {
        let mut set = self.channels.write().unwrap();
        set.extend(channels.iter().cloned());
    }

    pub fn subscribed_channels(&self) -> HashSet<String> {
        self.channels.read().unwrap().clone()
    }

    pub fn set_broadcast_tx(&self, tx: tokio::sync::broadcast::Sender<String>) {
        let _ = self.subscription_broadcast_tx.set(tx);
    }

    /// Subscribe to new channels dynamically.
    pub async fn subscribe(&self, channels: Vec<String>) -> Result<()> {
        if channels.is_empty() {
            return Ok(());
        }
        {
            let mut set = self.channels.write().unwrap();
            set.extend(channels.iter().cloned());
        }

        const BATCH_SIZE: usize = 250;
        const BATCH_DELAY_MS: u64 = 200;
        let total = channels.len();
        let mut done = 0usize;
        for chunk in channels.chunks(BATCH_SIZE) {
            let req = crate::request::subscribe::PublicSubscribeRequest::new(
                chunk.to_vec(),
            );
            match self.call_api(req).await {
                Ok(_) => {
                    done += chunk.len();
                    info!(
                        connection_id = self.id,
                        progress = %format!("{}/{}", done, total),
                        "subscribed batch"
                    );
                }
                Err(e) => {
                    warn!(
                        connection_id = self.id,
                        error = ?e,
                        batch_size = chunk.len(),
                        "subscribe batch failed, remaining will retry on reconnect"
                    );
                    break;
                }
            }
            if done < total {
                tokio::time::sleep(Duration::from_millis(BATCH_DELAY_MS)).await;
            }
        }
        Ok(())
    }

    /// Re-subscribe all channels currently in the tracked set.
    pub async fn resubscribe_all(&self) -> Result<()> {
        let channels: Vec<String> =
            self.channels.read().unwrap().iter().cloned().collect();
        if channels.is_empty() {
            return Ok(());
        }

        const BATCH_SIZE: usize = 250;
        const BATCH_DELAY_MS: u64 = 200;
        let total = channels.len();
        let mut done = 0usize;
        let mut failed = 0;
        for chunk in channels.chunks(BATCH_SIZE) {
            let req = crate::request::subscribe::PublicSubscribeRequest::new(
                chunk.to_vec(),
            );
            match self.call_api(req).await {
                Ok(_) => {
                    done += chunk.len();
                    info!(
                        connection_id = self.id,
                        progress = %format!("{}/{}", done, total),
                        "resubscribed batch"
                    );
                }
                Err(e) => {
                    warn!(
                        connection_id = self.id,
                        error = ?e,
                        "resubscribe batch failed"
                    );
                    failed += chunk.len();
                }
            }
            if done + failed < total {
                tokio::time::sleep(Duration::from_millis(BATCH_DELAY_MS)).await;
            }
        }
        info!(
            connection_id = self.id,
            success = done,
            failed,
            total,
            "resubscribe_all done"
        );
        Ok(())
    }

    /// Unsubscribe from channels and remove from the live set.
    pub async fn unsubscribe(&self, channels: Vec<String>) -> Result<()> {
        if channels.is_empty() {
            return Ok(());
        }

        let req =
            crate::request::subscribe::PublicUnsubscribeRequest::new(channels.clone());
        let resp = self.call_api(req).await;

        match resp {
            Ok(_) => {
                let mut set = self.channels.write().unwrap();
                for ch in &channels {
                    set.remove(ch);
                }
                debug!(
                    connection_id = self.id,
                    "unsubscribed from {} channels",
                    channels.len()
                );
                Ok(())
            }
            Err(e) => {
                warn!(connection_id = self.id, error = ?e, "unsubscribe failed");
                Err(e)
            }
        }
    }

    /// Send a JSON-RPC API call through the channel-based routing system.
    /// Used by `subscribe`, `unsubscribe`, `resubscribe_all` during normal
    /// operation. During reconnection setup, [`ProtocolHandler::run_setup`]
    /// uses direct transport calls instead.
    pub async fn call_api<R>(&self, request: R) -> Result<R::Response>
    where
        R: Request,
    {
        let (responser_tx, responser_rx) = oneshot::channel();

        let req = JSPNRPCRequest::<R>::from(request);
        let id = req.id;

        let payload = {
            if let Some(token) = self.token.read().unwrap().as_ref() {
                let mut value = serde_json::to_value(&req)?;
                if let Some(obj) = value.as_object_mut() {
                    obj.insert(
                        "access_token".to_string(),
                        serde_json::json!(token),
                    );
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

        let resp = tokio::time::timeout(
            Duration::from_secs(self.config.request_timeout),
            responser_rx,
        )
        .await
        .map_err(|_| {
            anyhow::Error::from(crate::errors::DeribitError::RequestTimeout)
        })
        .with_context(|| "connection responser timeout")?
        .with_context(|| "connection responser recv")?;

        let result: JSONRPCResponse<R::Response> =
            serde_json::from_str(&resp)
                .with_context(|| "connection response serde")?;

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

    // ── Eventloop ───────────────────────────────────────────────────

    pub async fn eventloop(&self, ct: CancellationToken) -> Result<()> {
        debug!(connection_id = self.id, "connection eventloop begin");

        // ── Layer 1: Transport — shared reqwest::Client, one per Connection ──
        let ping_interval = Duration::from_secs(
            self.config
                .ping_interval
                .unwrap_or(PING_INTERVAL_SECS),
        );
        let pong_timeout = Duration::from_secs(
            self.config
                .pong_timeout
                .unwrap_or(ping_interval.as_secs() * 2),
        );
        let mut transport = WsTransportImpl::new(
            self.client.clone(),
            self.config.url.clone(),
            ping_interval,
            pong_timeout,
            self.id,
        );

        // ── Layer 2: Protocol ────────────────────────────────────────
        let protocol = ProtocolHandler::new(
            self.token.clone(),
            self.channels.clone(),
            self.subscription_broadcast_tx.get().cloned(),
            self.config.heartbeat_interval,
            self.id,
            self.config.client_id.clone(),
            self.config.client_secret.clone(),
        );

        let mut backoff_secs: u64 = 1;
        let mut setup_backoff_secs: u64 = 5;
        const MAX_BACKOFF_SECS: u64 = 60;
        const MAX_SETUP_BACKOFF_SECS: u64 = 120;

        // Per-connection random seed for jitter.
        let jitter_seed: u64 = (self.id as u64).wrapping_mul(1_000_000);

        loop {
            if ct.is_cancelled() {
                return Ok(());
            }

            // ── Layer 1: Connect ─────────────────────────────────────
            debug!(connection_id = self.id, "connecting via transport");
            loop {
                match transport.connect().await {
                    Ok(()) => {
                        backoff_secs = 1;
                        break;
                    }
                    Err(e) => {
                        let jitter = (backoff_secs as f64 * 0.25) as u64;
                        let jittered = backoff_secs.saturating_sub(jitter)
                            + ((jitter_seed.wrapping_add(backoff_secs))
                                % (jitter * 2 + 1));
                        let delay = jittered.max(1);
                        warn!(
                            connection_id = self.id,
                            error = ?e,
                            backoff_secs,
                            delay,
                            "transport connect failed, retrying"
                        );
                        select! {
                            _ = tokio::time::sleep(Duration::from_secs(delay)) => {}
                            _ = ct.cancelled() => return Ok(()),
                        }
                        backoff_secs =
                            (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                    }
                }
            }
            debug!(connection_id = self.id, "transport connected");

            // ── Layer 2: Synchronous setup ───────────────────────────
            let mut setup_caller = SetupCaller {
                transport: &mut transport,
            };
            match protocol.run_setup(&mut setup_caller).await {
                Ok(()) => {
                    setup_backoff_secs = 5; // reset on success
                }
                Err(e) => {
                    warn!(
                        connection_id = self.id,
                        error = ?e,
                        setup_backoff_secs,
                        "setup failed, reconnecting"
                    );
                    if setup_backoff_secs >= MAX_SETUP_BACKOFF_SECS {
                        tracing::error!(
                            connection_id = self.id,
                            "setup repeatedly failed after max backoff, exiting to trigger pod restart"
                        );
                        std::process::exit(1);
                    }
                    select! {
                        _ = tokio::time::sleep(Duration::from_secs(setup_backoff_secs)) => {}
                        _ = ct.cancelled() => return Ok(()),
                    }
                    setup_backoff_secs =
                        (setup_backoff_secs * 2).min(MAX_SETUP_BACKOFF_SECS);
                    continue;
                }
            }

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
                                    OutgoingAction::ExpectResponse(payload, id) => {
                                        // Register a dummy waiter so the response
                                        // doesn't trigger "no waiter for response"
                                        let (tx, _rx) = oneshot::channel();
                                        responser_map.insert(id, tx);
                                        if let Err(e) = transport.send(payload).await {
                                            warn!(connection_id = self.id,
                                                error = ?e,
                                                "transport send (heartbeat) failed");
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
            // Inner loop break → reconnect
        }
    }
}

#[async_trait]
impl JsonRpcCaller for Connection {
    async fn call(&mut self, payload: &str, timeout: Duration) -> Result<String> {
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

#[async_trait]
impl nq_app::runner::Runner for Connection {
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
    /// Capacity of the outgoing message channel (producer → WS writer).
    #[builder(default = "1000")]
    pub message_channel_capacity: usize,
    /// Capacity of the API response routing channel.
    #[builder(default = "1000")]
    pub responser_channel_capacity: usize,
    /// WebSocket-level ping interval in seconds.
    /// Sends Ping frames at this interval to detect dead connections.
    /// Default: 15 seconds.
    #[builder(default = "Some(15)")]
    pub ping_interval: Option<u64>,
    /// Maximum time to wait for a Pong before declaring the connection dead.
    /// Default: 2 × ping_interval.
    #[builder(default)]
    pub pong_timeout: Option<u64>,
}
