use std::sync::Arc;

use anyhow::Result;
use nq_app::application::Application;
use nq_deribit::connection::ConnectionConfigBuilder;
use nq_deribit::pool::{ConnectionPool, PoolConfig};
use tokio_util::sync::CancellationToken;
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
    tracing_subscriber::fmt::init();

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

    // 2. Create Application and add connection runners
    let application = Application::new();
    for conn in pool.connection_runners() {
        application.add_runner(conn);
    }

    // 3. Create MQTT client
    let mqtt_client = nq_mqtt::client::Client::builder()
        .set_host(nq_env::emqx::host())
        .build();
    let mqtt_async_client = mqtt_client.inner();
    application.add_runner(Arc::new(mqtt_client));

    // 4. Create InstrumentFetcher
    let fetcher = Arc::new(InstrumentFetcher::new(pool.first_connection()));

    // 5. Create SubscriptionManager
    let sub_mgr = Arc::new(SubscriptionManager::new(
        pool.clone(),
        fetcher.clone(),
        config.currencies.clone(),
        config.ticker_interval,
        config.poll_interval_secs,
    ));

    // 6. Initialize: fetch all options and subscribe to their tickers
    sub_mgr.initialize().await?;

    // 7. Subscribe to instrument_state channels
    let inst_state_channels: Vec<String> = config.currencies.iter()
        .map(|c| format!("instrument_state.option.{}", c))
        .collect();
    info!(channels = ?inst_state_channels, "subscribing to instrument_state");
    pool.subscribe(inst_state_channels).await?;

    // 8. Add SubscriptionManager and TickerRouter as runners
    application.add_runner(sub_mgr);
    application.add_runner(Arc::new(TickerRouter::new(
        pool,
        mqtt_async_client,
        config.mqtt_topic_prefix,
    )));

    // 9. Run
    info!("all components started");
    let ct = CancellationToken::new();
    application.run(ct).await;

    Ok(())
}
