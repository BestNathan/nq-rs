use std::sync::Arc;

use anyhow::Result;
use nq_app::application::Application;
use tracing::info;

mod api;
mod bridge_handle;
mod bridge_runner;
mod config;
mod metrics;
mod template;

fn resolve_config_path() -> String {
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--config"
            && let Some(path) = args.get(i + 1)
        {
            return path.clone();
        }
        i += 1;
    }
    std::env::var("BRIDGE_CONFIG_PATH").unwrap_or_else(|_| "bridges.yaml".to_string())
}

fn resolve_api_port() -> u16 {
    std::env::var("BRIDGE_API_PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(7896)
}

#[tokio::main]
async fn main() -> Result<()> {
    let _guard = nq_observability::init_telemetry(
        nq_observability::TelemetryConfig::new("mqtt-http-bridge")
            .with_version(env!("CARGO_PKG_VERSION")),
    )?;
    nq_observability::spawn_tokio_metrics();

    let config_path = resolve_config_path();
    let api_port = resolve_api_port();

    info!(config_path = config_path, api_port = api_port, "starting");

    // Load initial configs from YAML file
    let initial_configs = config::load_from_file(&config_path)?;

    if initial_configs.is_empty() {
        info!("no initial bridge configs — starting with empty config set");
    } else {
        info!(count = initial_configs.len(), "loaded initial configs");
    }

    // Build HTTP client (shared across all handles)
    let http_client = reqwest::Client::builder()
        .build()
        .map_err(|e| anyhow::anyhow!("failed to create HTTP client: {e}"))?;

    // Channel for API → BridgeRunner commands
    let (command_tx, command_rx) = flume::bounded::<bridge_runner::Command>(64);

    // Build runners
    let bridge =
        Arc::new(bridge_runner::BridgeRunner::new(initial_configs, command_rx, http_client)?);

    let api_server = Arc::new(api::ApiServer::new(api_port, command_tx));

    // Run
    let application = Application::new();
    application.add_runner(bridge);
    application.add_runner(api_server);

    let canceltoken = tokio_util::sync::CancellationToken::new();
    application.run(canceltoken).await;

    Ok(())
}
