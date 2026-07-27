use anyhow::Result;

mod config;
mod template;
mod bridge_handle;
mod bridge_runner;
mod api;

#[tokio::main]
async fn main() -> Result<()> {
    let _guard = nq_observability::init_telemetry(
        nq_observability::TelemetryConfig::new("mqtt-http-bridge")
            .with_version(env!("CARGO_PKG_VERSION")),
    )?;
    nq_observability::spawn_tokio_metrics();
    tracing::info!("mqtt-http-bridge starting");
    Ok(())
}
