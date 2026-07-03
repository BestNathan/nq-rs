#![allow(deprecated)]

use crate::request::authentication::AuthRequest;
use crate::request::session_management::SetHeartbeatRequest;
use crate::request::subscribe::{PrivateSubscribeRequest, PublicSubscribeRequest};
use crate::request::support::TestRequest;
use crate::{api::DeribitApiClient, sub::DeribitSubscriptionClient};
use anyhow::{Context, Result};
use async_trait::async_trait;
use derive_builder::Builder;
use flume::{Receiver, Sender};
use futures_util::{SinkExt, StreamExt};
use nq_app::runner::Runner;
use reqwest::Proxy;
use reqwest_websocket::{Message, RequestBuilderExt, WebSocket};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::RwLock;
use std::{cell::RefCell, sync::Arc, time::Duration};
use tokio::{select, sync::oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

#[deprecated(
    note = "Use Connection + ConnectionPool instead. See connection.rs and pool.rs for the new multi-connection architecture."
)]
pub struct Client {
    config: Arc<Config>,
    token: Arc<RwLock<Option<String>>>,
    subscription_tx: Sender<String>,
    subscription_rx: Receiver<String>,
    message_tx: Sender<String>,
    message_rx: Receiver<String>,
    responser_tx: Sender<(i64, oneshot::Sender<String>)>,
    responser_rx: Receiver<(i64, oneshot::Sender<String>)>,
}
impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder { config: ConfigBuilder::default().build().unwrap().into() }
    }

    fn build_http_client(&self) -> Result<reqwest::Client> {
        let client = match self.config.proxy {
            Some(ref proxy) => reqwest::Client::builder().proxy(proxy.clone()).build()?,
            _ => reqwest::Client::builder().build()?,
        };

        Ok(client)
    }

    async fn connect_websocket(&self) -> Result<WebSocket> {
        let client =
            self.build_http_client().with_context(|| "deribit client build http client")?;

        let res = client
            .get(self.config.url.clone())
            .upgrade()
            .send()
            .await
            .with_context(|| "deribit client http send upgrade")?
            .into_websocket()
            .await
            .with_context(|| "deribit client websocket upgrade")?;

        Ok(res)
    }

    pub fn subscription_client(&self) -> DeribitSubscriptionClient {
        DeribitSubscriptionClient::new(self.subscription_rx.clone())
    }

    pub fn api_client(&self) -> DeribitApiClient {
        DeribitApiClient::new(
            self.token.clone(),
            self.message_tx.clone(),
            self.responser_tx.clone(),
            Duration::from_secs(self.config.request_timeout),
        )
    }

    fn auth_req(&self) -> Option<AuthRequest> {
        if self.config.client_id.is_some() && self.config.client_secret.is_some() {
            Some(AuthRequest::credential_auth(
                self.config.client_id.as_ref()?,
                self.config.client_secret.as_ref()?,
            ))
        } else {
            None
        }
    }

    fn public_sub_req(&self) -> Option<PublicSubscribeRequest> {
        if self.config.public_subscribe_channels.is_empty() {
            None
        } else {
            Some(PublicSubscribeRequest::new(self.config.public_subscribe_channels.clone()))
        }
    }

    fn private_sub_req(&self) -> Option<PrivateSubscribeRequest> {
        if self.config.private_subscribe_channels.is_empty() {
            None
        } else {
            Some(PrivateSubscribeRequest::new(self.config.private_subscribe_channels.clone()))
        }
    }

    pub async fn eventloop_with_cancel(&self, ct: CancellationToken) -> Result<()> {
        debug!("deribit client eventloop begin");

        loop {
            if ct.is_cancelled() {
                return Ok(());
            }

            debug!("deribit client begin to connect websocket");
            let mut ws = select! {
                ws = self.connect_websocket() => ws.with_context(|| "deribit client connect websocket")?,
                _ = ct.cancelled() => return Ok(())
            };
            debug!("deribit client websocket connected");

            let (err_tx, err_rx) = flume::bounded(1);
            let mut responser_map: HashMap<i64, oneshot::Sender<String>> = HashMap::new();
            let mut message_map: HashMap<i64, String> = HashMap::new();
            const MAX_MAP_SIZE: usize = 1000;

            // setup
            {
                let err_tx = err_tx.clone();
                let apiclient = self.api_client();
                let set_heartbeat_req =
                    SetHeartbeatRequest::with_interval(self.config.heartbeat_interval);
                let public_sub_req = self.public_sub_req();
                let private_sub_req = self.private_sub_req();
                let auth_req = self.auth_req();
                let token = self.token.clone();
                tokio::spawn(async move {
                    let res = async {
                        apiclient
                            .call(set_heartbeat_req)
                            .await
                            .with_context(|| "deribit client set heartbeat")?;

                        if let Some(req) = auth_req {
                            let res =
                                apiclient.call(req).await.with_context(|| "deribit client auth")?;

                            // this token expires_in 31536000(365 days)
                            // currently do not need to handle refresh logic
                            *token.write().unwrap() = res.access_token;
                        }

                        if let Some(req) = public_sub_req {
                            apiclient
                                .call(req)
                                .await
                                .with_context(|| "deribit client public subscribe")?;
                        }

                        if let Some(req) = private_sub_req {
                            apiclient
                                .call(req)
                                .await
                                .with_context(|| "deribit client private subscribe")?;
                        }

                        Ok::<(), anyhow::Error>(())
                    }
                    .await;

                    if let Err(e) = res {
                        err_tx.send_async(e).await.unwrap();
                    }
                });
            }

            loop {
                select! {
                    // handle error
                    err = err_rx.recv_async() => {
                        trace!("deribit client recv error: {:?}", err);
                        let err = err.with_context(|| "deribit client recv err")?;
                        return Err(err);
                    },
                    // handle cancel
                    () = ct.cancelled() => {
                        trace!("deribit client recv cancelling");
                        return Ok(());
                    },
                    // handle message sending
                    msg = self.message_rx.recv_async() => {
                        trace!("deribit client recv tx message");
                        let msg = msg.with_context(|| "deribit client recv message")?;
                        ws.send(Message::Text(msg)).await.with_context(|| "deribit client send message")?;
                    },
                    // handle request responser
                    Ok((id, responser)) = self.responser_rx.recv_async() => {
                        trace!("deribit client recv responser");
                        if let Some(text) = message_map.remove(&id) {
                            if responser.send(text).is_err() {
                                warn!("deribit client missing message responser for id={}", id);
                            }
                        } else {
                            // Prevent unbounded growth
                            if responser_map.len() >= MAX_MAP_SIZE {
                                warn!(map_size = responser_map.len(), "responser_map too large, clearing");
                                responser_map.clear();
                            }
                            responser_map.insert(id, responser);
                        }
                    }
                    // handle websocket message
                    next = ws.next() => {
                        trace!("deribit client recv next");
                        let message = match next {
                            Some(Err(e)) => {
                                warn!("deribit client websocket next message error: {}", e);
                                break;
                            }
                            Some(Ok(m)) => {m}
                            None => {
                                debug!("deribit client websocket no more messages");
                                break;
                            },
                        };

                        trace!("deribit client websocket recv message: {:?}", message);

                        let text = match message {
                            Message::Text(text) => {text}
                            // should never have happened
                            Message::Binary(bs) => {String::from_utf8(bs)?}
                            Message::Close { code, reason } => {
                                debug!("deribit client websocket recv close(code={}, reason={}) message", code, reason);
                                break;
                            }
                            Message::Pong(_) => {
                                debug!("deribit client websocket recv pong");
                                continue;
                            }
                            _ => {
                                debug!("deribit client websocket recv unhandled message: {:?}", message);
                                continue;
                            }
                        };

                        let value: Value = serde_json::from_str(&text).with_context(|| "deribit client websocket decode to value")?;

                        // handle api message
                        if let Some(id) = value.get("id") {
                            let id = match id.as_i64() {
                                Some(id) => {id}
                                None => {
                                    warn!("deribit client websocket recv api message with invalid id={:?}", id);
                                    continue;
                                }
                            };

                            let responser = match responser_map.remove(&id) {
                                Some(r) => {r}
                                None => {
                                    // Prevent unbounded growth
                                    if message_map.len() >= MAX_MAP_SIZE {
                                        warn!(map_size = message_map.len(), "message_map too large, clearing");
                                        message_map.clear();
                                    }
                                    message_map.insert(id, text);
                                    continue;
                                }
                            };

                            if responser.send(text).is_err() {
                                warn!("deribit client missing message responser for id={}", id);
                            }
                            continue;
                        }

                        // handle subscription message
                        if let Some(method) = value.get("method").and_then(Value::as_str) {
                            match method {
                                "heartbeat" => {
                                    {
                                        let apiclient = self.api_client();
                                        tokio::spawn(async move {
                                            apiclient.call(TestRequest::default()).await?;
                                            Ok::<(), anyhow::Error>(())
                                        });
                                    }
                                }
                                "subscription" => {
                                    crate::metrics::DERIBIT_METRICS.sub_received.add(1, &[]);
                                    // Use try_send to avoid blocking WS reader when channel is full
                                    match self.subscription_tx.try_send(text) {
                                        Ok(_) => {
                                            crate::metrics::DERIBIT_METRICS.sub_enqueued.add(1, &[]);
                                        }
                                        Err(flume::TrySendError::Full(_)) => {
                                            crate::metrics::DERIBIT_METRICS.sub_dropped.add(1, &[]);
                                            warn!("subscription channel full, dropping ticker message");
                                        }
                                        Err(flume::TrySendError::Disconnected(_)) => {
                                            crate::metrics::DERIBIT_METRICS.sub_dropped.add(1, &[]);
                                            warn!("subscription rx disconnected");
                                        }
                                    }
                                }
                                _ => {
                                    warn!("deribit client recv unknown method: {}", method);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Runner for Client {
    async fn run(&self, canceltoken: CancellationToken) -> Result<()> {
        info!("deribit client is running...");
        self.eventloop_with_cancel(canceltoken).await?;
        info!("deribit client done.");
        Ok(())
    }
}

#[derive(Builder)]
pub struct Config {
    #[builder(default = "nq_env::deribit::ws_url()")]
    pub url: String,
    #[builder(setter(into, strip_option), default)]
    pub proxy: Option<Proxy>,
    #[builder(default = "30")]
    pub heartbeat_interval: u64,
    #[builder(default = "60")]
    pub request_timeout: u64,
    #[builder(default = "Vec::new()")]
    pub public_subscribe_channels: Vec<String>,
    #[builder(default = "Vec::new()")]
    pub private_subscribe_channels: Vec<String>,
    #[builder(default)]
    pub client_id: Option<String>,
    #[builder(default)]
    pub client_secret: Option<String>,
    /// Capacity of the subscription message channel (Deribit → consumer).
    /// When full, messages are dropped (via try_send) to avoid blocking the WS reader.
    #[builder(default = "50000")]
    pub subscription_channel_capacity: usize,
    /// Capacity of the outgoing message channel (producer → WS writer).
    #[builder(default = "1000")]
    pub message_channel_capacity: usize,
    /// Capacity of the API response routing channel.
    #[builder(default = "1000")]
    pub responser_channel_capacity: usize,
}

pub struct ClientBuilder {
    config: RefCell<Config>,
}

impl ClientBuilder {
    pub fn build(self) -> Result<Client> {
        let cfg = self.config.into_inner();
        let (subscription_tx, subscription_rx) =
            flume::bounded::<String>(cfg.subscription_channel_capacity);
        let (message_tx, message_rx) = flume::bounded::<String>(cfg.message_channel_capacity);
        let (responser_tx, responser_rx) =
            flume::bounded::<(i64, oneshot::Sender<String>)>(cfg.responser_channel_capacity);

        Ok(Client {
            config: Arc::new(cfg),
            token: Arc::new(RwLock::new(None)),
            subscription_rx,
            subscription_tx,
            message_tx,
            message_rx,
            responser_tx,
            responser_rx,
        })
    }

    pub fn config(self, config: Config) -> Self {
        *self.config.borrow_mut() = config;
        self
    }
}

#[cfg(test)]
mod tests {
    use nq_app::runner::Runner;
    use reqwest::Proxy;
    use tokio::select;
    use tokio_util::sync::CancellationToken;
    use tracing::{debug, error};

    use crate::client::Client;
    use crate::client::ConfigBuilder;

    #[tokio::test]
    async fn test_ws_base_client() -> anyhow::Result<()> {
        tracing_subscriber::fmt::try_init().unwrap_or_default();

        let config = ConfigBuilder::default()
            .proxy(Proxy::all("http://192.168.2.98:8890")?)
            .public_subscribe_channels(vec!["markprice.options.btc_usd".into()])
            .build()?;

        let client = Client::builder().config(config).build()?;

        let subscriber = client.subscription_client();

        let tt = tokio_util::task::TaskTracker::new();
        let canceltoken = CancellationToken::new();

        {
            let token = canceltoken.clone();
            tt.spawn(async move {
                loop {
                    select! {
                        () = token.cancelled() => {
                            break;
                        },
                        msg = subscriber.recv() => {
                            match msg.ok() {
                                Some(data) => {
                                    debug!("recv subscription data: {:?}", data);
                                },
                                None => break,
                            }
                        }
                    }
                }

                debug!("subscriber done");
            });
        }

        {
            let token = canceltoken.clone();
            tt.spawn(async move {
                client.run(token).await.map_err(|e| error!("client run error: {:?}", e))
            });
        }

        tt.close();
        tt.wait().await;
        Ok(())
    }
}
