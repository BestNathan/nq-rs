use std::{env, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use nq_app::{application::Application, runner::Runner};
use nq_deribit::{
    connection::ConnectionConfigBuilder,
    metrics::DERIBIT_METRICS,
    pool::{ConnectionPool, PoolConfig},
};
use nq_observability::metrics::KeyValue;
use tokio::select;
use tokio::sync::broadcast::error::RecvError;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const SUBSCRIPTION: &str = include_str!("../resources/subscription.txt");
const DEFAULT_TOPIC: &str = "t/deribit/subscription";

const ENV_TOPIC: &str = "DERIBIT_SUBSCRIPTION_TOPIC";
const ENV_CHANNELS: &str = "DERIBIT_SUBSCRIPTION_CHANNELS";
const ENV_CHANNELS_FILE: &str = "DERIBIT_SUBSCRIPTION_CHANNELS_FILE";
const ENV_CLIENT_ID: &str = "DERIBIT_API_CLIENT_ID";
const ENV_CLIENT_SECRET: &str = "DERIBIT_API_CLIENT_SECRET";
const ENV_DRY_RUN: &str = "DRY_RUN";

/// Known Deribit channel prefixes for early validation.
const VALID_CHANNEL_PREFIXES: &[&str] = &[
    "markprice.",
    "deribit_price_index.",
    "deribit_volatility_index.",
    "deribit_price_statistics.",
    "trades.",
    "book.",
    "ticker.",
    "quote.",
    "perpetual.",
    "instruments.",
    "user.",
    "announcements.",
];

// ── Configuration ──────────────────────────────────────────────────────

fn resolve_topic() -> Arc<str> {
    env::var(ENV_TOPIC).unwrap_or_else(|_| DEFAULT_TOPIC.to_string()).into()
}

fn is_dry_run() -> bool {
    env::var(ENV_DRY_RUN).map(|v| v == "true" || v == "1").unwrap_or(false)
}

/// Parse one-channel-per-line text, trimming whitespace and skipping blanks.
fn channels_from_text(content: &str) -> Vec<String> {
    content.lines().map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
}

fn is_valid_channel(ch: &str) -> bool {
    VALID_CHANNEL_PREFIXES.iter().any(|p| ch.starts_with(p))
}

/// Resolve subscription channels using priority:
/// 1. `DERIBIT_SUBSCRIPTION_CHANNELS_FILE` (file path, one per line)
/// 2. `DERIBIT_SUBSCRIPTION_CHANNELS` (comma-separated)
/// 3. Default: embedded `resources/subscription.txt`
fn resolve_channels() -> Vec<String> {
    let from_file = || -> Option<Vec<String>> {
        let path = env::var(ENV_CHANNELS_FILE).ok()?;
        match std::fs::read_to_string(&path) {
            Ok(content) => {
                let chs = channels_from_text(&content);
                if chs.is_empty() {
                    warn!(path = %path, "channels file is empty, falling back");
                    None
                } else {
                    info!(path = %path, count = chs.len(), "channels from file");
                    Some(chs)
                }
            }
            Err(e) => {
                warn!(path = %path, error = ?e, "failed to read channels file, falling back");
                None
            }
        }
    };

    let from_env = || -> Option<Vec<String>> {
        let list = env::var(ENV_CHANNELS).ok()?;
        let chs: Vec<String> =
            list.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
        if chs.is_empty() {
            None
        } else {
            info!(count = chs.len(), "channels from env var");
            Some(chs)
        }
    };

    let from_default = || {
        let chs = channels_from_text(SUBSCRIPTION);
        info!(count = chs.len(), "channels from default subscription.txt");
        chs
    };

    from_file().or_else(from_env).unwrap_or_else(from_default)
}

/// Log warnings for channels with unknown prefixes — these may fail at
/// subscribe time on the Deribit side.
fn validate_channels(channels: &[String]) {
    let invalid: Vec<&str> =
        channels.iter().filter(|c| !is_valid_channel(c)).map(String::as_str).collect();
    if !invalid.is_empty() {
        warn!(
            count = invalid.len(),
            channels = ?invalid,
            "channels with unknown prefixes may fail to subscribe"
        );
    }
}

// ── Output destination ─────────────────────────────────────────────────

/// Where subscription data is forwarded to.
enum Output {
    /// Forward to MQTT via nq-mqtt.
    Mqtt { client: Arc<nq_mqtt::client::Client> },
    /// Log previews to stdout (dry-run mode).
    Stdout,
}

// ── Application runner ─────────────────────────────────────────────────

struct App {
    pool: Arc<ConnectionPool>,
    output: Output,
    topic: Arc<str>,
}

#[async_trait]
impl Runner for App {
    async fn run(&self, canceltoken: CancellationToken) -> Result<()> {
        let mut sub_rx = self.pool.subscribe_to_broadcast();

        match &self.output {
            Output::Stdout => info!("app running (DRY_RUN), logging to stdout"),
            Output::Mqtt { .. } => {
                info!("app running, forwarding to MQTT topic: {}", self.topic)
            }
        }

        loop {
            select! {
                _ = canceltoken.cancelled() => break,
                result = sub_rx.recv() => {
                    let data = match result {
                        Ok(m) => m,
                        Err(RecvError::Closed) => {
                            info!("broadcast channel closed");
                            break;
                        }
                        Err(RecvError::Lagged(n)) => {
                            DERIBIT_METRICS.broadcast_lagged.add(n, &[]);
                            warn!(skipped = n, "broadcast lagged, skipping messages");
                            continue;
                        }
                    };

                    let mqtt_attrs = &[KeyValue::new("mqtt_topic", self.topic.as_ref().to_string())];

                    match &self.output {
                        Output::Mqtt { client } => {
                            if let Err(e) = client.publish(&self.topic, data).await {
                                DERIBIT_METRICS.mqtt_publish_failed.add(1, mqtt_attrs);
                                warn!(error = ?e, "mqtt publish failed");
                            } else {
                                DERIBIT_METRICS.mqtt_published.add(1, mqtt_attrs);
                            }
                        }
                        Output::Stdout => {
                            let preview: String = data.chars().take(200).collect();
                            info!(
                                "[dry-run] {}: {}",
                                self.topic,
                                if data.len() > 200 {
                                    format!("{}…", preview)
                                } else {
                                    preview
                                }
                            );
                        }
                    }
                },
            }
        }

        info!("app done");
        Ok(())
    }
}

// ── Main ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let _guard = nq_observability::init_telemetry(
        nq_observability::TelemetryConfig::new("deribit-subscription")
            .with_version(env!("CARGO_PKG_VERSION")),
    )?;
    nq_observability::spawn_tokio_metrics();

    let channels = resolve_channels();
    if channels.is_empty() {
        warn!("no deribit subscriptions configured");
        return Ok(());
    }

    validate_channels(&channels);

    let dry_run = is_dry_run();
    if dry_run {
        info!("DRY_RUN mode: MQTT publish disabled, logging subscriptions to stdout");
    }

    info!("deribit subscriptions ({} channels): {}", channels.len(), channels.join(", "));

    // ── Build ConnectionConfig with optional auth ──────────────────────
    let conn_config = ConnectionConfigBuilder::default()
        .client_id(env::var(ENV_CLIENT_ID).ok())
        .client_secret(env::var(ENV_CLIENT_SECRET).ok())
        .build()?;

    // ── Create ConnectionPool (auto-spawns all connection eventloops) ──
    let pool = Arc::new(ConnectionPool::new(PoolConfig {
        capacity_per_connection: 2000,
        connection_config: conn_config,
    }));

    // Subscribe to all resolved channels
    info!(count = channels.len(), "subscribing to channels");
    pool.subscribe(channels).await?;

    // ── Build output ───────────────────────────────────────────────────
    let topic = resolve_topic();

    let (output, mqtt_runner): (Output, Option<Arc<dyn Runner>>) = if dry_run {
        (Output::Stdout, None)
    } else {
        let mqtt =
            Arc::new(nq_mqtt::client::Client::builder().set_host(nq_env::emqx::host()).build());
        let runner: Arc<dyn Runner> = mqtt.clone();
        (Output::Mqtt { client: mqtt }, Some(runner))
    };

    // ── Run application ────────────────────────────────────────────────
    let application = Application::new();

    if let Some(runner) = mqtt_runner {
        application.add_runner(runner);
    }

    let canceltoken = pool.cancel_token();

    application.add_runner(Arc::new(App { pool, output, topic }));

    application.run(canceltoken).await;

    Ok(())
}
