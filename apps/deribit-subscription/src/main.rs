use std::{env, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use nq_app::{application::Application, runner::Runner};
use nq_deribit::message::SubscriptionParams;
use rumqttc::{AsyncClient, QoS};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{info, trace, warn};

const SUBSCRIPTION: &str = include_str!("../resources/subscription.txt");
const DERIBIT_SUBSCRIPTION_TOPIC: &str = "t/deribit/subscription";

fn deribit_subscription_topic() -> String {
    env::var("DERIBIT_SUBSCRIPTION_TOPIC").unwrap_or(DERIBIT_SUBSCRIPTION_TOPIC.to_string())
}

struct App {
    deribit_writer: nq_deribit::client::MessageWriter,
    deribit_subscriber: nq_deribit::client::MessageSubscriber,
    mqtt_async_client: AsyncClient,
}

#[async_trait]
impl Runner for App {
    async fn run(&self, canceltoken: CancellationToken) -> Result<()> {
        let topic = deribit_subscription_topic();

        info!("app is running...");
        info!("");

        info!(
            "deribit subscriptions: {}",
            SUBSCRIPTION.replace("\n", ", ")
        );
        info!("");

        if SUBSCRIPTION.len() == 0 {
            warn!("no deribit subscriptions");
            return Ok(());
        }

        select! {
            _ = canceltoken.cancelled() => {},
            _ = async {
                self.deribit_writer.subscribe(SUBSCRIPTION.split("\n").map(|v| v.to_string()).collect()).await.unwrap();
            } => {  }
        };

        loop {
            select! {
                _ = canceltoken.cancelled() => break,
                msg = self.deribit_subscriber.recv() => {
                    match msg {
                        Some(ref smsg) => {
                            if let SubscriptionParams::Subscribe(p) = &smsg.params {
                                trace!("recv subscription message from channel: {:}", p.channel);

                                let payload = serde_json::to_string(&smsg).unwrap();

                                self.mqtt_async_client.publish(
                                    topic.clone(),
                                    QoS::AtLeastOnce,
                                    true,
                                    payload,
                                ).await?;

                            };
                        },
                        None => {
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

    let application = Application::new();

    let deribit_client = nq_deribit::client::Client::builder().build()?;
    let (deribit_writer, deribit_subscriber) = deribit_client.split();
    application.add_runner(Arc::new(deribit_client));

    let mqtt_client = nq_mqtt::client::Client::builder()
        .set_host(nq_env::emqx::host())
        .build();
    let mqtt_async_client = mqtt_client.inner();

    application.add_runner(Arc::new(mqtt_client));

    let app = App {
        deribit_writer,
        deribit_subscriber,
        mqtt_async_client,
    };

    application.add_runner(Arc::new(app));

    let canceltoken = CancellationToken::new();
    application.run(canceltoken).await;

    Ok(())
}
