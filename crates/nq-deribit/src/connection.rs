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
use tracing::{debug, info, warn};

use crate::errors::DeribitError::RequestTimeout;
use crate::jsonrpc::{JSPNRPCRequest, JSONRPCResponse};
use crate::request::authentication::AuthRequest;
use crate::request::subscribe::PublicSubscribeRequest;
use crate::request::Request;

// ─── Connection ──────────────────────────────────────────────────────

pub struct Connection {
    id: usize,
    channels: Arc<RwLock<HashSet<String>>>,
    config: Arc<ConnectionConfig>,
    token: Arc<RwLock<Option<String>>>,
    subscription_broadcast_tx: std::sync::OnceLock<tokio::sync::broadcast::Sender<String>>,
    message_tx: Sender<String>,
    message_rx: Receiver<String>,
    responser_tx: Sender<(i64, oneshot::Sender<String>)>,
    responser_rx: Receiver<(i64, oneshot::Sender<String>)>,
}

impl Connection {
    pub fn new(id: usize, config: ConnectionConfig) -> Self {
        // Use bounded channels for outgoing message and API response routing
        let msg_cap = config.message_channel_capacity;
        let (message_tx, message_rx) = flume::bounded::<String>(msg_cap);
        let resp_cap = config.responser_channel_capacity;
        let (responser_tx, responser_rx) = flume::bounded::<(i64, oneshot::Sender<String>)>(resp_cap);

        Self {
            id,
            channels: Arc::new(RwLock::new(HashSet::new())),
            config: Arc::new(config),
            token: Arc::new(RwLock::new(None)),
            subscription_broadcast_tx: std::sync::OnceLock::new(),
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

    /// Synchronously add channels to the tracked set without making API calls.
    /// Used by ConnectionPool to pre-track assignments so the distribution loop
    /// sees accurate counts immediately.
    pub fn pre_track_channels(&self, channels: &[String]) {
        let mut set = self.channels.write().unwrap();
        set.extend(channels.iter().cloned());
    }

    pub fn subscribed_channels(&self) -> HashSet<String> {
        self.channels.read().unwrap().clone()
    }

    /// Set the pool-level broadcast sender. Called once during connection creation.
    /// Subsequent calls are silently ignored (OnceLock).
    pub fn set_broadcast_tx(&self, tx: tokio::sync::broadcast::Sender<String>) {
        let _ = self.subscription_broadcast_tx.set(tx);
    }

    /// Subscribe to new channels dynamically. Channels are added to the live set
    /// immediately (for reconnect resilience) and the subscribe request is sent.
    pub async fn subscribe(&self, channels: Vec<String>) -> Result<()> {
        if channels.is_empty() {
            return Ok(());
        }

        // Add to set immediately so reconnect will include them even if subscribe fails temporarily
        {
            let mut set = self.channels.write().unwrap();
            set.extend(channels.iter().cloned());
        }

        // Batch subscribe in chunks to avoid oversized WS messages.
        // Channels are already in the tracked set, so any that fail here will be
        // picked up by reconnect/resubscribe. Stop on first error to avoid wasting
        // time on a broken connection (each timeout is 60s).
        const BATCH_SIZE: usize = 100;
        let total = channels.len();
        let mut done = 0usize;
        for chunk in channels.chunks(BATCH_SIZE) {
            let req = PublicSubscribeRequest::new(chunk.to_vec());
            match self.call_api(req).await {
                Ok(_) => {
                    done += chunk.len();
                    info!(connection_id = self.id, progress = %format!("{}/{}", done, total), "subscribed batch");
                }
                Err(e) => {
                    warn!(connection_id = self.id, error = ?e, batch_size = chunk.len(),
                        "subscribe batch failed, remaining {} channels will retry on reconnect", total - done);
                    break; // stop trying — WS is down, reconnect will resubscribe
                }
            }
        }

        Ok(())
    }

    /// Re-subscribe all channels currently in the tracked set.
    /// Useful after reconnect when the WS subscription state may be lost.
    pub async fn resubscribe_all(&self) -> Result<()> {
        let channels: Vec<String> = self.channels.read().unwrap().iter().cloned().collect();
        if channels.is_empty() {
            return Ok(());
        }

        const BATCH_SIZE: usize = 100;
        let total = channels.len();
        let mut done = 0usize;
        let mut failed = 0;
        for chunk in channels.chunks(BATCH_SIZE) {
            let req = PublicSubscribeRequest::new(chunk.to_vec());
            match self.call_api(req).await {
                Ok(_) => {
                    done += chunk.len();
                    info!(connection_id = self.id, progress = %format!("{}/{}", done, total), "resubscribed batch");
                }
                Err(e) => {
                    warn!(connection_id = self.id, error = ?e, "resubscribe batch failed");
                    failed += chunk.len();
                }
            }
        }
        info!(connection_id = self.id, success = done, failed, total, "resubscribe_all done");
        Ok(())
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

    pub async fn eventloop(&self, ct: CancellationToken) -> Result<()> {
        debug!(connection_id = self.id, "connection eventloop begin");

        // Bounded channels for per-reconnect eventloop communication
        let (el_payload_tx, el_payload_rx) = flume::unbounded::<String>();
        let (el_responser_tx, el_responser_rx) = flume::unbounded::<(i64, oneshot::Sender<String>)>();

        let mut backoff_secs: u64 = 1;
        const MAX_BACKOFF_SECS: u64 = 60;

        // Per-connection random seed for jitter, to prevent all connections
        // from reconnecting simultaneously (thundering herd).
        let jitter_seed: u64 = (self.id as u64).wrapping_mul(1_000_000);

        loop {
            if ct.is_cancelled() {
                return Ok(());
            }

            debug!(connection_id = self.id, "connecting websocket");
            let mut ws = loop {
                match self.connect_websocket().await {
                    Ok(ws) => {
                        backoff_secs = 1; // reset on success
                        break ws;
                    }
                    Err(e) => {
                        // Add ±25% jitter to spread out reconnect attempts
                        let jitter = (backoff_secs as f64 * 0.25) as u64;
                        let jittered = backoff_secs.saturating_sub(jitter)
                            + ((jitter_seed.wrapping_add(backoff_secs)) % (jitter * 2 + 1));
                        let delay = jittered.max(1);
                        warn!(connection_id = self.id, error = ?e, backoff_secs, delay,
                            "websocket connect failed, retrying after backoff");
                        select! {
                            _ = tokio::time::sleep(Duration::from_secs(delay)) => {}
                            _ = ct.cancelled() => return Ok(()),
                        }
                        backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
                    }
                }
            };
            debug!(connection_id = self.id, "websocket connected");

            let (err_tx, err_rx) = flume::bounded(1);
            let mut responser_map: HashMap<i64, oneshot::Sender<String>> = HashMap::new();
            let mut message_map: HashMap<i64, String> = HashMap::new();
            const MAX_MAP_SIZE: usize = 1000;

            // Setup task: heartbeat, auth, re-subscribe tracked channels
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
                        let _ = tokio::time::timeout(Duration::from_secs(30), rx).await?;

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
                            let resp = tokio::time::timeout(Duration::from_secs(30), rx).await??;
                            let result: JSONRPCResponse<crate::request::authentication::AuthResponse> = serde_json::from_str(&resp)?;
                            if let either::Either::Left(auth_resp) = result.result {
                                *token.write().unwrap() = auth_resp.access_token;
                            }
                        }

                        // 3. Re-subscribe all tracked channels (batched)
                        let channel_list: Vec<String> = channels.into_iter().collect();
                        if !channel_list.is_empty() {
                            const BATCH_SIZE: usize = 100;
                            let mut base_id = 700_000 + conn_id as i64;
                            for chunk in channel_list.chunks(BATCH_SIZE) {
                                let sub_id = base_id;
                                base_id += 1;
                                let mut sub_val = serde_json::to_value(&PublicSubscribeRequest::new(chunk.to_vec()))?;
                                if let Some(obj) = sub_val.as_object_mut() {
                                    obj.insert("jsonrpc".to_string(), json!("2.0"));
                                    obj.insert("id".to_string(), json!(sub_id));
                                }
                                let (tx, rx) = oneshot::channel();
                                el_payload_tx.send_async(sub_val.to_string()).await?;
                                el_responser_tx.send_async((sub_id, tx)).await?;
                                let _ = tokio::time::timeout(Duration::from_secs(30), rx).await?;
                            }
                            info!(connection_id = conn_id, "re-subscribed {} channels", channel_list.len());
                        }

                        Ok::<(), anyhow::Error>(())
                    }.await;
                    if let Err(e) = res {
                        warn!(connection_id = conn_id, error = ?e, "setup task failed, triggering reconnect");
                        // Signal the eventloop to break and reconnect with a fresh WS.
                        // Without this, the connection limps along without heartbeat/auth
                        // and eventually Deribit closes it without recovery.
                        let _ = err_tx.send_async(e).await;
                    }
                });
            }

            // Main eventloop
            loop {
                select! {
                    err = err_rx.recv_async() => {
                        let err = err.with_context(|| "connection setup error")?;
                        warn!(connection_id = self.id, error = ?err, "setup failed, reconnecting");
                        break;
                    }
                    () = ct.cancelled() => {
                        return Ok(());
                    }
                    msg = self.message_rx.recv_async() => {
                        let msg = msg.with_context(|| "connection recv message")?;
                        ws.send(Message::Text(msg)).await.with_context(|| "connection ws send")?;
                    }
                    msg = el_payload_rx.recv_async() => {
                        let msg = msg.with_context(|| "connection recv el payload")?;
                        ws.send(Message::Text(msg)).await.with_context(|| "connection ws send el")?;
                    }
                    Ok((id, responser)) = self.responser_rx.recv_async() => {
                        if let Some(text) = message_map.remove(&id) {
                            let _ = responser.send(text);
                        } else {
                            // Prevent unbounded growth: if map is too large, clear stale entries
                            if responser_map.len() >= MAX_MAP_SIZE {
                                warn!(connection_id = self.id, map_size = responser_map.len(), "responser_map too large, clearing");
                                responser_map.clear();
                            }
                            responser_map.insert(id, responser);
                        }
                    }
                    Ok((id, responser)) = el_responser_rx.recv_async() => {
                        if let Some(text) = message_map.remove(&id) {
                            let _ = responser.send(text);
                        } else {
                            if responser_map.len() >= MAX_MAP_SIZE {
                                warn!(connection_id = self.id, map_size = responser_map.len(), "responser_map too large, clearing");
                                responser_map.clear();
                            }
                            responser_map.insert(id, responser);
                        }
                    }
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
                                // Prevent unbounded growth
                                if message_map.len() >= MAX_MAP_SIZE {
                                    warn!(connection_id = self.id, map_size = message_map.len(), "message_map too large, clearing");
                                    message_map.clear();
                                }
                                message_map.insert(id, text);
                            }
                            continue;
                        }

                        // Handle subscription/notification messages
                        if let Some(method) = value.get("method").and_then(|v| v.as_str()) {
                            match method {
                                "heartbeat" => {
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
                                    crate::metrics::DERIBIT_SUB_RECEIVED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    // Broadcast send is non-blocking: when the ring buffer is full,
                                    // the oldest message is evicted to make room. This never
                                    // blocks the WS reader and prevents OOM.
                                    // SendError only occurs when there are zero receivers,
                                    // which is a transient startup state.
                                    if let Some(tx) = self.subscription_broadcast_tx.get() {
                                        match tx.send(text) {
                                            Ok(_) => {
                                                crate::metrics::DERIBIT_SUB_ENQUEUED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                            }
                                            Err(_) => {
                                                crate::metrics::DERIBIT_SUB_DROPPED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                            }
                                        }
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
    /// Capacity of the outgoing message channel (producer → WS writer).
    #[builder(default = "1000")]
    pub message_channel_capacity: usize,
    /// Capacity of the API response routing channel.
    #[builder(default = "1000")]
    pub responser_channel_capacity: usize,
}
