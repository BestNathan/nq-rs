use std::sync::Arc;

use anyhow::Result;
use application::{runner::Runner, Application};
use async_trait::async_trait;
use deribit::message::SubscriptionParams;
use rumqttc::{AsyncClient, QoS};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::info;

mod deribit;
mod env;
mod mqtt;

const SUBSCRIPTION: &str = include_str!("../subscription.txt");
const EMQX_TOPIC: &str = "t/deribit/subscription";

struct App {
    deribit_writer: deribit::client::MessageWriter,
    deribit_subscriber: deribit::client::MessageSubscriber,
    mqtt_async_client: AsyncClient,
}

#[async_trait]
impl Runner for App {
    async fn run(&self, canceltoken: CancellationToken) -> Result<()> {
        info!("app is running...");
        info!("");

        info!(
            "deribit subscriptions: {}",
            SUBSCRIPTION.replace("\n", ", ")
        );
        info!("");
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
                                info!("recv subscription message from channel: {:}", p.channel);

                                let payload = serde_json::to_string(&smsg).unwrap();

                                self.mqtt_async_client.publish(
                                    EMQX_TOPIC.to_string(),
                                    QoS::AtLeastOnce,
                                    true,
                                    payload,
                                ).await?;

                            };
                        },
                        None => {
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

    let deribit_client = deribit::client::Client::builder().build()?;
    let (deribit_writer, deribit_subscriber) = deribit_client.split();
    application.add_runner(Arc::new(deribit_client));

    let mqtt_client = mqtt::client::Client::builder()
        .set_host(env::emqx_host())
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
