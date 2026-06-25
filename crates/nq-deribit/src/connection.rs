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

        Self {
            id,
            channels: Arc::new(RwLock::new(HashSet::new())),
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

        // Batch subscribe in chunks to avoid oversized WS messages
        const BATCH_SIZE: usize = 100;
        for chunk in channels.chunks(BATCH_SIZE) {
            let req = PublicSubscribeRequest::new(chunk.to_vec());
            match self.call_api(req).await {
                Ok(_) => {
                    debug!(connection_id = self.id, "subscribed to {} channels", chunk.len());
                }
                Err(e) => {
                    warn!(connection_id = self.id, error = ?e, batch_size = chunk.len(),
                        "subscribe batch failed, channels will retry on reconnect");
                }
            }
        }

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
                        err_tx.send_async(e).await.unwrap_or_default();
                    }
                });
            }

            // Main eventloop
            loop {
                select! {
                    err = err_rx.recv_async() => {
                        let err = err.with_context(|| "connection setup error")?;
                        return Err(err);
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
                            responser_map.insert(id, responser);
                        }
                    }
                    Ok((id, responser)) = el_responser_rx.recv_async() => {
                        if let Some(text) = message_map.remove(&id) {
                            let _ = responser.send(text);
                        } else {
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
