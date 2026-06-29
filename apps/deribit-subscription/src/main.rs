use std::{env, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use nq_app::{application::Application, runner::Runner};
use nq_deribit::connection::ConnectionConfigBuilder;
use nq_deribit::pool::{ConnectionPool, PoolConfig};
use rumqttc::{AsyncClient, QoS};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{info, trace, warn};

const SUBSCRIPTION: &str = include_str!("../resources/subscription.txt");
const DERIBIT_SUBSCRIPTION_TOPIC: &str = "t/deribit/subscription";
const DERIBIT_SUBSCRIPTION_TOPIC_ENV: &str = "DERIBIT_SUBSCRIPTION_TOPIC";
const DERIBIT_SUBSCRIPTION_CHANNELS_ENV: &str = "DERIBIT_SUBSCRIPTION_CHANNELS";
const DERIBIT_SUBSCRIPTION_CHANNELS_FILE_ENV: &str = "DERIBIT_SUBSCRIPTION_CHANNELS_FILE";
const DERIBIT_API_CLIENT_ID_ENV: &str = "DERIBIT_API_CLIENT_ID";
const DERIBIT_API_CLIENT_SECRET_ENV: &str = "DERIBIT_API_CLIENT_SECRET";
const DRY_RUN_ENV: &str = "DRY_RUN";

fn deribit_subscription_topic() -> String {
    env::var(DERIBIT_SUBSCRIPTION_TOPIC_ENV)
        .unwrap_or(DERIBIT_SUBSCRIPTION_TOPIC.to_string())
}

fn is_dry_run() -> bool {
    env::var(DRY_RUN_ENV)
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false)
}

/// Resolve subscription channels using priority:
/// 1. DERIBIT_SUBSCRIPTION_CHANNELS_FILE (file path, one per line)
/// 2. DERIBIT_SUBSCRIPTION_CHANNELS (comma-separated)
/// 3. Default: resources/subscription.txt
fn resolve_channels() -> Vec<String> {
    if let Ok(path) = env::var(DERIBIT_SUBSCRIPTION_CHANNELS_FILE_ENV) {
        match std::fs::read_to_string(&path) {
            Ok(content) => {
                let channels: Vec<String> = content
                    .lines()
                    .map(|v| v.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                if !channels.is_empty() {
                    info!(path = %path, count = channels.len(), "channels from file");
                    return channels;
                }
                warn!(path = %path, "channels file is empty, falling back");
            }
            Err(e) => {
                warn!(path = %path, error = ?e, "failed to read channels file, falling back");
            }
        }
    }

    if let Ok(list) = env::var(DERIBIT_SUBSCRIPTION_CHANNELS_ENV) {
        let channels: Vec<String> = list
            .split(',')
            .map(|v| v.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if !channels.is_empty() {
            info!(count = channels.len(), "channels from env var");
            return channels;
        }
    }

    let channels: Vec<String> = SUBSCRIPTION
        .lines()
        .map(|v| v.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    info!(count = channels.len(), "channels from default subscription.txt");
    channels
}

/// Application runner that forwards Deribit subscription data to MQTT
/// (or logs to stdout in DRY_RUN mode).
struct App {
    mqtt_async_client: Option<AsyncClient>,
    pool: Arc<ConnectionPool>,
    dry_run: bool,
    topic: String,
}

#[async_trait]
impl Runner for App {
    async fn run(&self, canceltoken: CancellationToken) -> Result<()> {
        let topic = self.topic.clone();
        let mut sub_rx = self.pool.subscribe_to_broadcast();

        if self.dry_run {
            info!("app is running (DRY_RUN), logging to stdout");
        } else {
            info!("app is running, forwarding to MQTT topic: {}", topic);
        }

        loop {
            select! {
                _ = canceltoken.cancelled() => break,
                result = sub_rx.recv() => {
                    let data = match result {
                        Ok(m) => m,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            info!("broadcast channel closed");
                            break;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!(skipped = n, "broadcast lagged, skipping messages");
                            continue;
                        }
                    };

                    if self.dry_run {
                        // Log a preview (first 200 chars) at INFO level
                        let preview: String = data.chars().take(200).collect();
                        info!(
                            "[dry-run] would publish to {}: {}",
                            topic,
                            if data.len() > 200 {
                                format!("{}…", preview)
                            } else {
                                preview
                            }
                        );
                    } else if let Some(ref mqtt) = self.mqtt_async_client {
                        trace!("recv subscription data: {:?}", data);
                        if let Err(e) = mqtt.publish(
                            topic.clone(),
                            QoS::AtLeastOnce,
                            true,
                            data,
                        ).await {
                            nq_deribit::metrics::DERIBIT_METRICS.mqtt_publish_failed.add(1, &[]);
                            warn!(error = ?e, "mqtt publish failed");
                        } else {
                            nq_deribit::metrics::DERIBIT_METRICS.mqtt_published.add(1, &[]);
                        }
                    }
                },
            }
        }

        info!("app done");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let channels = resolve_channels();

    if channels.is_empty() {
        warn!("no deribit subscriptions configured");
        return Ok(());
    }

    let dry_run = is_dry_run();
    if dry_run {
        info!("DRY_RUN mode: MQTT publish disabled, logging subscriptions to stdout");
    }

    info!(
        "deribit subscriptions ({} channels): {}",
        channels.len(),
        channels.join(", ")
    );

    // ── Build ConnectionConfig with optional auth ──────────────────
    let conn_config = ConnectionConfigBuilder::default()
        .client_id(env::var(DERIBIT_API_CLIENT_ID_ENV).ok())
        .client_secret(env::var(DERIBIT_API_CLIENT_SECRET_ENV).ok())
        .build()?;

    // ── Create ConnectionPool ──────────────────────────────────────
    let pool = Arc::new(ConnectionPool::new(PoolConfig {
        capacity_per_connection: 2000,
        connection_config: conn_config,
    }));

    let ct = pool.cancel_token();

    // Spawn connection eventloops — must be running before subscribe
    for conn in pool.connection_runners() {
        let ct = ct.clone();
        tokio::spawn(async move {
            let _ = conn.run(ct).await;
        });
    }

    // Subscribe to all resolved channels
    info!(count = channels.len(), "subscribing to channels");
    pool.subscribe(channels).await?;

    // ── Resolve subscription topic (computed once at startup) ──────
    let topic = deribit_subscription_topic();

    // ── Create MQTT client (or skip in DRY_RUN) ────────────────────
    let (mqtt_client, mqtt_async_client) = if dry_run {
        (None, None)
    } else {
        let mqtt_client = nq_mqtt::client::Client::builder()
            .set_host(nq_env::emqx::host())
            .build();
        let inner = mqtt_client.inner();
        (Some(mqtt_client), Some(inner))
    };

    // ── Run application ────────────────────────────────────────────
    let application = Application::new();

    // Register MQTT runner (if not DRY_RUN)
    if let Some(mqtt) = mqtt_client {
        application.add_runner(Arc::new(mqtt));
    }

    application.add_runner(Arc::new(App {
        mqtt_async_client,
        pool,
        dry_run,
        topic,
    }));

    let canceltoken = ct;
    application.run(canceltoken).await;

    Ok(())
}
