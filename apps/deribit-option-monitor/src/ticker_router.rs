use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use futures_util::StreamExt;
use nq_app::runner::Runner;
use nq_deribit::message::{SubscriptionMessage, SubscriptionParams};
use nq_deribit::pool::ConnectionPool;
use nq_deribit::subscription::ticker::TickerData;
use rumqttc::{AsyncClient, QoS};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

pub struct TickerRouter {
    pool: Arc<ConnectionPool>,
    mqtt_client: AsyncClient,
    topic_prefix: String,
}

impl TickerRouter {
    pub fn new(pool: Arc<ConnectionPool>, mqtt_client: AsyncClient, topic_prefix: String) -> Self {
        Self {
            pool,
            mqtt_client,
            topic_prefix,
        }
    }
}

#[async_trait]
impl Runner for TickerRouter {
    async fn run(&self, ct: CancellationToken) -> Result<()> {
        debug!("ticker router is running");

        let mut stream = self.pool.subscription_stream();

        loop {
            select! {
                _ = ct.cancelled() => break,
                msg = stream.next() => {
                    let msg = match msg {
                        Some(m) => m,
                        None => {
                            debug!("ticker router: subscription stream ended");
                            break;
                        }
                    };

                    let sub_msg: SubscriptionMessage = match serde_json::from_str(&msg) {
                        Ok(m) => m,
                        Err(_) => continue,
                    };

                    if let SubscriptionParams::Subscribe(params) = sub_msg.params {
                        if !params.channel.starts_with("ticker.") {
                            continue;
                        }

                        let ticker: TickerData = match serde_json::from_value(params.data) {
                            Ok(t) => t,
                            Err(e) => {
                                trace!(error = ?e, "failed to parse ticker data");
                                continue;
                            }
                        };

                        let topic = format!("{}/{}", self.topic_prefix, ticker.instrument_name);
                        let payload = match serde_json::to_vec(&ticker) {
                            Ok(p) => p,
                            Err(e) => {
                                warn!(error = ?e, "failed to serialize ticker");
                                continue;
                            }
                        };

                        if let Err(e) = self.mqtt_client.publish(
                            &topic,
                            QoS::AtLeastOnce,
                            false,
                            payload,
                        ).await {
                            warn!(error = ?e, topic = topic, "mqtt publish failed");
                        }
                    }
                }
            }
        }

        debug!("ticker router done");
        Ok(())
    }
}
