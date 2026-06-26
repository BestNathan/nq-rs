use std::{env, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use nq_app::{application::Application, runner::Runner};
use nq_deribit::client::ConfigBuilder;
use rumqttc::{AsyncClient, QoS};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{info, trace, warn};

const SUBSCRIPTION: &str = include_str!("../resources/subscription.txt");
const DERIBIT_SUBSCRIPTION_TOPIC: &str = "t/deribit/subscription";
const DERIBIT_SUBSCRIPTION_TOPIC_ENV: &str = "DERIBIT_SUBSCRIPTION_TOPIC";
const DERIBIT_API_CLIENT_ID_ENV: &str = "DERIBIT_API_CLIENT_ID";
const DERIBIT_API_CLIENT_SECRET_ENV: &str = "DERIBIT_API_CLIENT_SECRET";

fn deribit_subscription_topic() -> String {
    env::var(DERIBIT_SUBSCRIPTION_TOPIC_ENV).unwrap_or(DERIBIT_SUBSCRIPTION_TOPIC.to_string())
}

struct App {
    deribit_subscriber: nq_deribit::sub::DeribitSubscriptionClient,
    mqtt_async_client: AsyncClient,
}

#[async_trait]
impl Runner for App {
    async fn run(&self, canceltoken: CancellationToken) -> Result<()> {
        let topic = deribit_subscription_topic();

        info!("app is running...");
        info!("");

        loop {
            select! {
                _ = canceltoken.cancelled() => break,
                msg = self.deribit_subscriber.recv() => {
                    match msg {
                        Ok(data) => {
                            trace!("recv subscription data: {:?}", data);

                            if let Err(e) = self.mqtt_async_client.publish(
                                topic.clone(),
                                QoS::AtLeastOnce,
                                true,
                                data,
                            ).await {
                                nq_deribit::metrics::MQTT_PUBLISH_FAILED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                warn!(error = ?e, "mqtt publish failed");
                            } else {
                                nq_deribit::metrics::MQTT_PUBLISHED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        },
                        Err(_) => {
                            info!("no more subscription messages");
                            break;
                        }
                    }
                },
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!(
        "deribit subscriptions: {}",
        SUBSCRIPTION.replace("\n", ", ")
    );
    info!("");

    if SUBSCRIPTION.len() == 0 {
        warn!("no deribit subscriptions");
        return Ok(());
    }

    let application = Application::new();

    let deribit_config = ConfigBuilder::default()
        .public_subscribe_channels(SUBSCRIPTION.split("\n").map(|v| v.to_string()).collect())
        .client_id(env::var(DERIBIT_API_CLIENT_ID_ENV).ok())
        .client_secret(env::var(DERIBIT_API_CLIENT_SECRET_ENV).ok())
        .build()?;
    let deribit_client = nq_deribit::client::Client::builder()
        .config(deribit_config)
        .build()?;
    let deribit_subscriber = deribit_client.subscription_client();

    application.add_runner(Arc::new(deribit_client));

    let mqtt_client = nq_mqtt::client::Client::builder()
        .set_host(nq_env::emqx::host())
        .build();
    let mqtt_async_client = mqtt_client.inner();

    application.add_runner(Arc::new(mqtt_client));

    let app = App {
        deribit_subscriber,
        mqtt_async_client,
    };

    application.add_runner(Arc::new(app));

    let canceltoken = CancellationToken::new();
    application.run(canceltoken).await;

    Ok(())
}
