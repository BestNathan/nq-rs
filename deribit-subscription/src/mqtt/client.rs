use std::time::Duration;

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
    eventloop: EventLoop,
}

impl Client {
    pub fn new(client: AsyncClient, eventloop: EventLoop) -> Self {
        Self {
            inner: client,
            eventloop: eventloop,
        }
    }

    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    pub fn inner(&self) -> AsyncClient {
        self.inner.clone()
    }

    pub async fn run(&mut self, cancel: CancellationToken) {
        loop {
            let _ = tokio::select! {
                _ = cancel.cancelled() => {
                    debug!("mqtt client stopped due to canceling");
                    break;
                },
                res = self.eventloop.poll() => {
                    match res {
                        Ok(notification) => {
                            debug!("mqtt client poll notification: {:?}", notification)
                        },
                        Err(err) => {
                            warn!("mqtt eventloop recv err: {:?}", err)
                        }
                    }
                }
            };
        }
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
    // a builder for `FmtSubscriber`.
    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(Level::TRACE)
        // completes the builder.
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let mut client = Client::builder()
        .set_host("192.168.2.106".to_string())
        .set_port(1883)
        .build();

    let canceltoken = CancellationToken::new();

    client
        .inner()
        .try_publish(
            "hello/world",
            rumqttc::QoS::AtLeastOnce,
            true,
            json!({
                "key": "hello",
                "value": "world",
            })
            .to_string(),
        )
        .unwrap();

    {
        let token = canceltoken.clone();
        tokio::spawn(async move {
            sleep(Duration::from_secs(3)).await;
            token.cancel();
        });
    }

    let tt = TaskTracker::new();

    {
        let token = canceltoken.clone();
        tt.spawn(async move { client.run(token).await });
    }

    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("application quit with signaling");
            canceltoken.cancel();
        },
        _ = canceltoken.cancelled() => {
            info!("application quit with canceling");
        },
    }

    tt.close();
    tt.wait().await;

    info!("application done!")
}
