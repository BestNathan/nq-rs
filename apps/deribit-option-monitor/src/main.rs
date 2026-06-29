use std::sync::Arc;

use anyhow::Result;
use nq_app::{application::Application, runner::Runner};
use nq_deribit::connection::ConnectionConfigBuilder;
use nq_deribit::pool::{ConnectionPool, PoolConfig};
use tracing::info;

mod config;
mod fetcher;
mod subscription_mgr;
mod ticker_router;

use config::AppConfig;
use fetcher::InstrumentFetcher;
use subscription_mgr::SubscriptionManager;
use ticker_router::TickerRouter;

#[tokio::main]
async fn main() -> Result<()> {
    let _guard = nq_observability::init_telemetry("deribit-option-monitor")?;
    nq_observability::spawn_tokio_metrics(std::time::Duration::from_secs(15));

    let config = AppConfig::from_env();

    info!("deribit-option-monitor starting");
    info!(currencies = ?config.currencies, interval = ?config.ticker_interval, "config loaded");

    // 1. Create ConnectionPool
    let conn_config = ConnectionConfigBuilder::default()
        .build()?;

    let pool = Arc::new(ConnectionPool::new(PoolConfig {
        capacity_per_connection: config.pool_capacity,
        connection_config: conn_config,
    }));

    let ct = pool.cancel_token();

    // 2. Spawn connection eventloops FIRST — they must be running before any API calls
    for conn in pool.connection_runners() {
        let ct = ct.clone();
        tokio::spawn(async move {
            let _ = conn.run(ct).await;
        });
    }

    // 3. Create MQTT client (spawns its own eventloop internally)
    let mqtt_client = nq_mqtt::client::Client::builder()
        .set_host(nq_env::emqx::host())
        .build();
    let mqtt_async_client = mqtt_client.inner();

    // 4. Create InstrumentFetcher (uses independent HTTP client, not WebSocket)
    let http_client = reqwest::Client::builder()
        .build()
        .expect("create HTTP client for InstrumentFetcher");
    let fetcher = Arc::new(InstrumentFetcher::new(http_client, config.rest_base_url.clone()));

    // 5. Create SubscriptionManager
    let sub_mgr = Arc::new(SubscriptionManager::new(
        pool.clone(),
        fetcher.clone(),
        config.currencies.clone(),
        config.ticker_interval,
        config.poll_interval_secs,
    ));

    // 6. Initialize: fetch all options and subscribe to their tickers
    //    (connections are now running, so call_api will work)
    sub_mgr.initialize().await?;

    // 7. Subscribe to instrument_state channels
    let inst_state_channels: Vec<String> = config.currencies.iter()
        .map(|c| format!("instrument_state.option.{}", c))
        .collect();
    info!(channels = ?inst_state_channels, "subscribing to instrument_state");
    pool.subscribe(inst_state_channels).await?;

    // 8. Run SubscriptionManager and TickerRouter
    let application = Application::new();
    application.add_runner(sub_mgr);
    application.add_runner(Arc::new(TickerRouter::new(
        pool,
        mqtt_async_client,
        config.mqtt_topic_prefix,
    )));

    info!("all components started");
    application.run(ct).await;

    Ok(())
}
