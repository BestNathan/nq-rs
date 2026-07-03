use std::sync::Arc;

use anyhow::Result;
use nq_app::application::Application;
use nq_app::runner::Runner;
use nq_deribit::connection::ConnectionConfigBuilder;
use nq_deribit::pool::{ConnectionPool, PoolConfig};
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let channels: Vec<String> =
        vec!["trades.future.BTC.agg2".to_string(), "trades.option.BTC.agg2".to_string()];

    let conn_config = ConnectionConfigBuilder::default().build()?;
    let pool = Arc::new(ConnectionPool::new(PoolConfig {
        capacity_per_connection: 200,
        connection_config: conn_config,
    }));

    let ct = pool.cancel_token();

    // Spawn connection eventloops
    for conn in pool.connection_runners() {
        let ct = ct.clone();
        tokio::spawn(async move {
            let _ = conn.run(ct).await;
        });
    }

    // Subscribe
    pool.subscribe(channels).await?;

    // Receive subscription messages
    let mut sub_rx = pool.subscribe_to_broadcast();
    tokio::spawn(async move {
        loop {
            match sub_rx.recv().await {
                Ok(msg) => info!("{}", msg),
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(skipped = n, "broadcast lagged");
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    let app = Application::new();
    app.run(CancellationToken::new()).await;
    Ok(())
}
