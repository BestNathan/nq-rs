use std::{cell::RefCell, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use flume::{Receiver, Sender};
use futures_util::{SinkExt, StreamExt};
use nq_app::runner::Runner;
use reqwest::Proxy;
use reqwest_websocket::{Message, RequestBuilderExt, WebSocket};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::message::WebsocketMessage;

use super::message::{self, MessageAssembler, SubscriptionMessage};

pub struct Client {
    canceltoken: CancellationToken,
    config: Arc<Config>,
    inner_client: reqwest::Client,
    ma: Arc<message::MessageAssembler>,
    subscription_tx: Sender<message::SubscriptionMessage>,
    subscription_rx: Receiver<message::SubscriptionMessage>,
    message_tx: Sender<Message>,
    message_rx: Receiver<Message>,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder {
            config: Config::default().into(),
        }
    }

    async fn connect(&self) -> Result<WebSocket> {
        let res = self
            .inner_client
            .get(self.config.url.clone())
            .upgrade()
            .send()
            .await?;

        let mut ws = res.into_websocket().await?;

        ws.send(Message::Text(
            self.ma
                .set_heartbeat_message(self.config.heartbeat_interval),
        ))
        .await?;

        Ok(ws)
    }

    pub fn split(&self) -> (MessageWriter, MessageSubscriber) {
        (
            MessageWriter::new(
                self.canceltoken.clone(),
                self.message_tx.clone(),
                self.ma.clone(),
            ),
            MessageSubscriber::new(self.canceltoken.clone(), self.subscription_rx.clone()),
        )
    }
}

#[async_trait]
impl Runner for Client {
    async fn run(&self, canceltoken: CancellationToken) -> Result<()> {
        info!("deribit client is connecting websocket");

        let mut ws = select! {
            ws = self.connect() => ws?,
            _ = canceltoken.cancelled() => return Ok(())
        };

        info!("deribit client is running");

        loop {
            select! {
                () = canceltoken.cancelled() => {
                    debug!("deribit client recv cancelling");
                    break;
                },
                Ok(msg_to_send) = self.message_rx.recv_async() => {
                    let _ = ws.send(msg_to_send).await;
                },
                next = ws.next() => {
                    let message = match next {
                        Some(m) => {m}
                        None => {
                            debug!("deribit client not have next message");
                            break;
                        },
                    }?;

                    let text = match message {
                        Message::Text(text) => {text}
                        _ => {
                            debug!("deribit client recv non text message: {:?}", message);
                            continue;
                        }
                    };

                    match serde_json::from_str::<WebsocketMessage>(&text) {
                        Ok(json) => match json {
                            WebsocketMessage::SubscriptionMessage(submsg) => {
                                if submsg.method == "heartbeat" {
                                    let _ = self.message_tx.send_async(Message::Text(self.ma.test_message())).await;
                                } else {
                                    let _ = self.subscription_tx.send_async(submsg).await;
                                }
                            }
                            WebsocketMessage::ResultMessage(result) => {
                                debug!("deribit client recv result message: {:?}", result)
                            }
                            WebsocketMessage::Other(val) => {
                                debug!("deribit client recv other message: {:?}", val.to_string())
                            }
                        },
                        Err(error) => {
                            warn!(?error, "deribit client parse websocket message fail");
                            return Err(error.into())
                        }
                    };

                }
            }
        }

        // clear
        if !self.canceltoken.is_cancelled() {
            self.canceltoken.cancel();
        }

        info!("deribit client done");

        Ok(())
    }
}

pub struct MessageWriter {
    canceltoken: CancellationToken,
    tx: Sender<Message>,
    ma: Arc<MessageAssembler>,
}

impl MessageWriter {
    fn new(canceltoken: CancellationToken, tx: Sender<Message>, ma: Arc<MessageAssembler>) -> Self {
        Self {
            canceltoken,
            tx,
            ma,
        }
    }

    async fn send(&self, msg: String) -> Result<()> {
        select! {
            res = self.tx.send_async(Message::Text(msg)) =>  Ok(res?),
            _ = self.canceltoken.cancelled() => Ok(())
        }
    }

    pub async fn subscribe(&self, channels: Vec<String>) -> Result<()> {
        self.send(self.ma.subscribe_message(channels)).await?;
        Ok(())
    }
}

pub struct MessageSubscriber {
    canceltoken: CancellationToken,
    rx: Receiver<SubscriptionMessage>,
}

impl MessageSubscriber {
    fn new(canceltoken: CancellationToken, rx: Receiver<SubscriptionMessage>) -> Self {
        Self { canceltoken, rx }
    }

    pub async fn recv(&self) -> Option<SubscriptionMessage> {
        select! {
            _ = self.canceltoken.cancelled() => None,
            msg = self.rx.recv_async() => {
                match msg {
                    Ok(msg) => Some(msg),
                    Err(e) => {
                        warn!(error = ?e, "deribit client subscription recv fail");
                        None
                    }
                }
            },
        }
    }
}

pub struct Config {
    url: String,
    proxy: Option<Proxy>,
    heartbeat_interval: i64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            url: nq_env::deribit::ws_url(),
            proxy: Proxy::all(nq_env::proxy()).ok(),
            heartbeat_interval: 10,
        }
    }
}

pub struct ClientBuilder {
    config: RefCell<Config>,
}

impl ClientBuilder {
    pub fn build(self) -> Result<Client> {
        let client = match self.config.borrow().proxy {
            Some(ref proxy) => reqwest::Client::builder().proxy(proxy.clone()).build()?,
            _ => reqwest::Client::builder().build()?,
        };

        let (subscription_tx, subscription_rx) = flume::unbounded::<message::SubscriptionMessage>();
        let (message_tx, message_rx) = flume::unbounded::<Message>();

        Ok(Client {
            canceltoken: CancellationToken::new(),
            config: Arc::from(self.config.into_inner()),
            inner_client: client,
            ma: MessageAssembler::new().into(),
            subscription_rx,
            subscription_tx,
            message_tx,
            message_rx,
        })
    }

    pub fn set_proxy(self, proxy: Proxy) -> Self {
        self.config.borrow_mut().proxy = Some(proxy);
        self
    }

    pub fn proxy(&self) -> Option<Proxy> {
        self.config.borrow().proxy.clone()
    }
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            config: RefCell::new(Config::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use nq_app::runner::Runner;
    use reqwest::Proxy;
    use tokio::select;
    use tokio_util::sync::CancellationToken;
    use tracing::debug;

    use crate::client::Client;

    #[tokio::test]
    async fn test_ws_base_client() {
        unsafe {
            std::env::set_var("RUST_LOG", "debug");
        }
        tracing_subscriber::fmt::try_init().unwrap_or_default();

        let client = Client::builder()
            .set_proxy(Proxy::all("http://192.168.2.98:8890").unwrap())
            .build()
            .unwrap();

        let (writer, subscriber) = client.split();

        let tt = tokio_util::task::TaskTracker::new();
        let canceltoken = CancellationToken::new();

        {
            let token = canceltoken.clone();
            tt.spawn(async move {
                writer
                    .subscribe(vec!["markprice.options.btc_usd".into()])
                    .await
                    .unwrap();

                loop {
                    select! {
                        () = token.cancelled() => {
                            break;
                        },
                        msg = subscriber.recv() => {
                            match msg {
                                Some(smsg) => {
                                    debug!(method = smsg.method, "recv subscription message")
                                },
                                None => break,
                            }
                        }
                    }
                }
            });
        }

        {
            let token = canceltoken.clone();
            tt.spawn(async move {
                client.run(token).await.unwrap();
            });
        }

        tt.close();
        tt.wait().await;
    }
}
