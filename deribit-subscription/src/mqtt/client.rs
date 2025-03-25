use std::{sync::Arc, time::Duration};

use anyhow::Result;
use application::{runner::Runner, Application};
use async_trait::async_trait;
use futures_util::lock::Mutex;
use rand::{distr::Alphanumeric, rng, Rng};
use rumqttc::{AsyncClient, EventLoop, MqttOptions};
use serde_json::json;
use tokio::{select, signal, sync::broadcast, time::sleep};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, info, trace, warn, Level};
use tracing_subscriber::{field::debug, FmtSubscriber};

fn random_string(length: usize) -> String {
    rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

pub struct Client {
    inner: AsyncClient,
    canceltoken: CancellationToken,
}

impl Client {
    pub fn new(client: AsyncClient, eventloop: EventLoop) -> Self {
        let canceltoken = CancellationToken::new();

        {
            let canceltoken = canceltoken.clone();
            let mut eventloop = eventloop;
            tokio::spawn(async move {
                loop {
                    select! {
                        _ = canceltoken.cancelled() => return,
                        res = eventloop.poll() => {
                            match res {
                                Ok(notification) => {
                                    debug!("mqtt client poll notification: {:?}", notification)
                                },
                                Err(err) => {
                                    warn!(error = ?err, "mqtt client eventloop poll fail")
                                }
                            }
                        }
                    }
                }
            });
        }

        Self {
            inner: client,
            canceltoken,
        }
    }

    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    pub fn inner(&self) -> AsyncClient {
        self.inner.clone()
    }
}

#[async_trait]
impl Runner for Client {
    async fn run(&self, canceltoken: CancellationToken) -> Result<()> {
        canceltoken.cancelled().await;

        if !self.canceltoken.is_cancelled() {
            self.canceltoken.cancel();
        }

        Ok(())
    }
}

pub struct Config {
    id: Option<String>,
    host: String,
    port: u16,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            id: None,
            host: "127.0.0.1".to_owned(),
            port: 1883,
        }
    }
}

pub struct ClientBuilder {
    config: Config,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            config: Config::default(),
        }
    }
}

impl ClientBuilder {
    pub fn build(self) -> Client {
        let mut option = MqttOptions::new(
            self.config
                .id
                .unwrap_or(format!("nq-rs/{}", random_string(10))),
            self.config.host,
            self.config.port,
        );

        option.set_max_packet_size(1024 * 1024, 1024 * 1024);

        let (client, eventloop) = AsyncClient::new(option, 10);

        Client::new(client, eventloop)
    }

    pub fn set_host(mut self, host: String) -> Self {
        self.config.host = host;
        self
    }

    pub fn set_port(mut self, port: u16) -> Self {
        self.config.port = port;
        self
    }

    pub fn set_id(mut self, id: String) -> Self {
        self.config.id = Some(id);
        self
    }
}

#[tokio::test]
async fn test_base_client() {
    std::env::set_var("RUST_LOG", "debug");
    tracing_subscriber::fmt::try_init().unwrap_or_default();

    let canceltoken = CancellationToken::new();
    let application = Application::new();

    let client = Client::builder()
        .set_host("192.168.2.106".to_string())
        .set_port(1883)
        .build();

    let ac = client.inner();

    {
        let token = canceltoken.clone();
        tokio::spawn(async move {
            ac.publish(
                "hello/world",
                rumqttc::QoS::AtLeastOnce,
                true,
                json!({
                    "key": "hello",
                    "value": "world",
                })
                .to_string(),
            )
            .await
            .unwrap();

            sleep(Duration::from_secs(3)).await;

            token.cancel();
        });
    }

    application.add_runner(Arc::new(client));

    application.run(canceltoken).await;
}
