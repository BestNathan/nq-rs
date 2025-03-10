use anyhow::Result;
use deribit::message::SubscriptionParams;
use rumqttc::{AsyncClient, QoS};
use tokio::{select, signal};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
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

impl App {
    async fn run(&self, canceltoken: CancellationToken) {
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
                _ = canceltoken.cancelled() => {break;},
                msg = self.deribit_subscriber.recv() => {
                    match msg {
                        Some(ref smsg) => {
                            if let SubscriptionParams::Subscribe(p) = &smsg.params {
                                info!("recv subscription message from channel: {:}", p.channel);

                                let payload = serde_json::to_string(&smsg).unwrap();

                                self.mqtt_async_client.publish(
                                    EMQX_TOPIC.to_owned(),
                                    QoS::AtLeastOnce,
                                    true,
                                    payload,
                                ).await.unwrap();
                            };
                        },
                        None => {
                            break;
                        }
                    }
                },
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let canceltoken = CancellationToken::new();
    let tt = TaskTracker::new();

    let mut deribit_client = deribit::client::Client::builder().build()?;
    let (deribit_writer, deribit_subscriber) = deribit_client.split();

    {
        let canceltoken = canceltoken.clone();
        tt.spawn(async move { deribit_client.run(canceltoken).await });
    }

    let mut mqtt_client = mqtt::client::Client::builder()
        .set_host(env::emqx_host())
        .build();
    let mqtt_async_client = mqtt_client.inner();

    {
        let canceltoken = canceltoken.clone();
        tt.spawn(async move { mqtt_client.run(canceltoken).await });
    }

    {
        let canceltoken = canceltoken.clone();
        let app = App {
            deribit_writer,
            deribit_subscriber,
            mqtt_async_client,
        };

        select! {
            _ = signal::ctrl_c() => {
                info!("recv terminated signal...")
            }
            _ = tt.spawn(async move {
                app.run(canceltoken).await;
                info!("app done");
            }) => {}
        }
    }

    tt.close();
    canceltoken.cancel();

    info!("waiting for all task done");
    tt.wait().await;

    Ok(())
}
