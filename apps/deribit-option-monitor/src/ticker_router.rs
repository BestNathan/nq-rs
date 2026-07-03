use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use nq_app::runner::Runner;
use nq_deribit::message::{SubscriptionMessage, SubscriptionParams};

use nq_deribit::pool::ConnectionPool;
use nq_deribit::subscription::ticker::TickerData;
use nq_observability::metrics::KeyValue;
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
        Self { pool, mqtt_client, topic_prefix }
    }
}

#[async_trait]
impl Runner for TickerRouter {
    async fn run(&self, ct: CancellationToken) -> Result<()> {
        debug!("ticker router is running");

        let mut rx = self.pool.subscribe_to_broadcast();

        loop {
            select! {
                _ = ct.cancelled() => break,
                result = rx.recv() => {
                    let msg = match result {
                        Ok(m) => m,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            debug!("ticker router: broadcast channel closed");
                            break;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            nq_deribit::metrics::DERIBIT_METRICS.broadcast_lagged.add(n, &[]);
                            warn!(skipped = n, "ticker router: lagged behind broadcast");
                            continue;
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

                        let topic_label = topic.clone();

                        // Use timeout to prevent TickerRouter from blocking forever
                        // when the MQTT client is disconnected. Without this, the router
                        // stalls, the subscription channel fills, and OOM follows.
                        match tokio::time::timeout(
                            std::time::Duration::from_secs(5),
                            self.mqtt_client.publish(
                                &topic,
                                QoS::AtLeastOnce,
                                false,
                                payload,
                            )
                        ).await {
                            Ok(Ok(())) => {
                                nq_deribit::metrics::DERIBIT_METRICS.mqtt_published.add(
                                    1,
                                    &[KeyValue::new("mqtt_topic", topic_label)],
                                );
                            }
                            Ok(Err(e)) => {
                                nq_deribit::metrics::DERIBIT_METRICS.mqtt_publish_failed.add(
                                    1,
                                    &[
                                        KeyValue::new("mqtt_topic", topic_label),
                                        KeyValue::new("error", "error"),
                                    ],
                                );
                                warn!(error = ?e, topic = topic, "mqtt publish failed");
                            }
                            Err(_timeout) => {
                                nq_deribit::metrics::DERIBIT_METRICS.mqtt_publish_failed.add(
                                    1,
                                    &[
                                        KeyValue::new("mqtt_topic", topic_label),
                                        KeyValue::new("error", "timeout"),
                                    ],
                                );
                                // Don't warn on every timeout — would flood logs at ~150 msg/s
                            }
                        }
                    }
                }
            }
        }

        debug!("ticker router done");
        Ok(())
    }
}
