//! Integration test: verify pool.subscribe() distributes real Deribit option
//! tickers across multiple connections, each ≤ capacity_per_connection.
//!
//! This test connects to Deribit through the proxy, fetches active options,
//! subscribes to all tickers, and verifies distribution across connections.
//!
//! Run with:
//!   cargo test -p nq-deribit --test pool_subscribe_integration -- --nocapture --test-threads=1

use std::sync::Arc;
use std::time::Duration;

use nq_app::runner::Runner;
use nq_deribit::connection::{Connection, ConnectionConfigBuilder};
use nq_deribit::model::currency::Currency;
use nq_deribit::pool::{ConnectionPool, PoolConfig};
use nq_deribit::request::market_data::GetInstrumentsRequest;
use reqwest::Proxy;
use tokio::time::timeout;

fn test_pool() -> anyhow::Result<ConnectionPool> {
    let conn_config = ConnectionConfigBuilder::default()
        .proxy(Proxy::all("http://192.168.2.98:7892")?)
        .request_timeout(30)
        .subscription_channel_capacity(50000)
        .build()?;

    Ok(ConnectionPool::new(PoolConfig {
        capacity_per_connection: 200,
        connection_config: conn_config,
    }))
}

/// Spawn eventloops for all connections in the pool. Returns handles that
/// must be kept alive for the duration of the test.
fn spawn_eventloops(pool: &ConnectionPool) -> Vec<tokio::task::JoinHandle<()>> {
    let ct = pool.cancel_token();
    pool.connection_runners()
        .iter()
        .map(|conn| {
            let conn = conn.clone();
            let ct = ct.clone();
            tokio::spawn(async move {
                let _ = conn.run(ct).await;
            })
        })
        .collect()
}

/// Fetch all active option instruments for the given currencies.
async fn fetch_options(
    conn: Arc<Connection>,
    currencies: &[Currency],
) -> anyhow::Result<Vec<String>> {
    let mut names = Vec::new();
    for &currency in currencies {
        let req = GetInstrumentsRequest::options(currency);
        let resp = conn.call_api(req).await?;
        names.extend(resp.into_iter().map(|info| info.instrument_name));
    }
    Ok(names)
}

#[tokio::test]
async fn test_pool_subscribe_all_options() -> anyhow::Result<()> {
    tracing_subscriber::fmt::try_init().ok();

    let pool = test_pool()?;
    let currencies = vec![Currency::BTC, Currency::ETH];

    // Spawn eventloops FIRST — API calls need a live WebSocket
    let _handles = spawn_eventloops(&pool);

    // Give the first connection time to establish WebSocket
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Fetch all active options
    let conn = pool.first_connection();
    let option_names = timeout(Duration::from_secs(30), fetch_options(conn, &currencies)).await??;
    println!(
        "Fetched {} options for {:?}",
        option_names.len(),
        currencies
    );
    assert!(
        option_names.len() > 100,
        "expected >100 options, got {}",
        option_names.len()
    );

    // Build ticker channels: ticker.{name}.agg2
    let ticker_channels: Vec<String> = option_names
        .iter()
        .map(|name| format!("ticker.{}.agg2", name))
        .collect();

    // Subscribe all tickers via pool — this is the code under test
    let start = tokio::time::Instant::now();
    pool.subscribe(ticker_channels.clone()).await?;
    let elapsed = start.elapsed();
    println!(
        "Subscribe completed in {:.1}s for {} channels",
        elapsed.as_secs_f64(),
        ticker_channels.len()
    );

    // ─── Verify distribution ───────────────────────────────────────
    let conns = pool.connection_runners();
    let total_tracked: usize = conns.iter().map(|c| c.channel_count()).sum();
    println!(
        "Connections: {}, total tracked channels: {}",
        conns.len(),
        total_tracked
    );

    for conn in &conns {
        let count = conn.channel_count();
        println!("  conn {}: {} channels", conn.id(), count);
        assert!(
            count <= 200,
            "connection {} has {} channels, exceeding capacity 200",
            conn.id(),
            count
        );
    }

    // All tickers should be tracked
    assert_eq!(
        total_tracked, ticker_channels.len(),
        "total tracked {} != subscribed {}",
        total_tracked,
        ticker_channels.len()
    );

    // Should have used multiple connections (1680 / 200 = 9)
    let expected_conns = (ticker_channels.len() + 199) / 200; // ceil division
    println!(
        "Expected ~{} connections, got {}",
        expected_conns,
        conns.len()
    );
    assert!(
        conns.len() >= 2,
        "expected >=2 connections for {} channels with cap=200, got {}",
        ticker_channels.len(),
        conns.len()
    );

    // ─── Verify data flows ─────────────────────────────────────────
    // Read from subscription stream for a few seconds to confirm tickers arrive
    let mut stream = pool.subscription_stream();
    let mut msg_count = 0usize;
    let read_deadline = tokio::time::Instant::now() + Duration::from_secs(10);

    while tokio::time::Instant::now() < read_deadline {
        match timeout(Duration::from_secs(2), futures_util::StreamExt::next(&mut stream)).await {
            Ok(Some(_msg)) => {
                msg_count += 1;
                if msg_count >= 10 {
                    break; // Enough to prove data flows
                }
            }
            Ok(None) => break,
            Err(_) => break, // timeout
        }
    }

    println!(
        "Received {} subscription messages in ~10s window",
        msg_count
    );
    assert!(
        msg_count > 0,
        "expected at least 1 subscription message, got 0 — data flow broken"
    );

    // Cleanup
    pool.cancel_token().cancel();
    Ok(())
}
