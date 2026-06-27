use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use nq_app::runner::Runner;
use nq_deribit::message::{SubscriptionMessage, SubscriptionParams};
use nq_deribit::model::currency::Currency;
use nq_deribit::model::interval::Interval;
use nq_deribit::pool::ConnectionPool;
use nq_deribit::subscription::instrument::InstrumentStateData;
use tokio::select;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::fetcher::InstrumentFetcher;

pub struct SubscriptionManager {
    pool: Arc<ConnectionPool>,
    fetcher: Arc<InstrumentFetcher>,
    tracked_options: Arc<RwLock<HashSet<String>>>,
    currencies: Vec<Currency>,
    interval: Interval,
    poll_interval_secs: u64,
}

impl SubscriptionManager {
    pub fn new(
        pool: Arc<ConnectionPool>,
        fetcher: Arc<InstrumentFetcher>,
        currencies: Vec<Currency>,
        interval: Interval,
        poll_interval_secs: u64,
    ) -> Self {
        Self {
            pool,
            fetcher,
            tracked_options: Arc::new(RwLock::new(HashSet::new())),
            currencies,
            interval,
            poll_interval_secs,
        }
    }

    pub async fn initialize(&self) -> Result<()> {
        info!("subscription manager initializing...");
        let options = self.fetcher.fetch_all_options(&self.currencies).await?;
        let names: Vec<String> = options.iter().map(|o| o.instrument_name.clone()).collect();
        info!(count = names.len(), "fetched active options");
        self.subscribe_new_options(&names).await?;
        Ok(())
    }

    async fn subscribe_new_options(&self, instrument_names: &[String]) -> Result<()> {
        let mut tracked = self.tracked_options.write().unwrap();
        let truly_new: Vec<String> = instrument_names
            .iter()
            .filter(|n| !tracked.contains(*n))
            .cloned()
            .collect();

        if truly_new.is_empty() {
            return Ok(());
        }

        let channels: Vec<String> = truly_new
            .iter()
            .map(|name| format!("ticker.{}.{}", name, self.interval))
            .collect();

        info!(count = channels.len(), "subscribing to new option tickers");
        self.pool.subscribe(channels).await?;

        tracked.extend(truly_new);
        info!(total_tracked = tracked.len(), "tracked options updated");
        Ok(())
    }
}

#[async_trait]
impl Runner for SubscriptionManager {
    async fn run(&self, ct: CancellationToken) -> Result<()> {
        info!("subscription manager is running");

        let tracked = self.tracked_options.clone();
        let pool = self.pool.clone();
        let fetcher = self.fetcher.clone();
        let currencies = self.currencies.clone();
        let interval = self.interval;
        let poll_secs = self.poll_interval_secs;

        // Task 1: Poll loop
        let ct1 = ct.clone();
        let tracked1 = tracked.clone();
        let pool1 = pool.clone();
        let fetcher1 = fetcher.clone();
        let currencies1 = currencies.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = ct1.cancelled() => break,
                    _ = sleep(Duration::from_secs(poll_secs)) => {
                        // Poll for all active options and compute bidirectional diff
                        match fetcher1.fetch_all_options(&currencies1).await {
                            Ok(options) => {
                                let active_names: HashSet<String> = options
                                    .iter()
                                    .map(|o| o.instrument_name.clone())
                                    .collect();

                                // Compute additions and removals under a single write lock
                                let (new_opts, expired_opts): (Vec<String>, Vec<String>) = {
                                    let mut t = tracked1.write().unwrap();
                                    let new: Vec<String> = active_names
                                        .iter()
                                        .filter(|n| !t.contains(*n))
                                        .cloned()
                                        .collect();
                                    let expired: Vec<String> = t
                                        .iter()
                                        .filter(|n| !active_names.contains(*n))
                                        .cloned()
                                        .collect();
                                    // Update tracked set: add new, remove expired
                                    t.extend(new.iter().cloned());
                                    for e in &expired {
                                        t.remove(e);
                                    }
                                    (new, expired)
                                };

                                // Subscribe to new options
                                if !new_opts.is_empty() {
                                    let channels: Vec<String> = new_opts.iter()
                                        .map(|n| format!("ticker.{}.{}", n, interval))
                                        .collect();
                                    info!(count = channels.len(), total = active_names.len(), "poll discovered new options");
                                    if let Err(e) = pool1.subscribe(channels).await {
                                        warn!(error = ?e, "poll subscribe failed");
                                    }
                                }

                                // Unsubscribe from expired options
                                if !expired_opts.is_empty() {
                                    let channels: Vec<String> = expired_opts.iter()
                                        .map(|n| format!("ticker.{}.{}", n, interval))
                                        .collect();
                                    info!(count = channels.len(), remaining = active_names.len(), "poll removing expired options");
                                    if let Err(e) = pool1.unsubscribe(channels).await {
                                        warn!(error = ?e, "poll unsubscribe failed");
                                    }
                                    // Clean up empty connections to free resources
                                    pool1.cleanup_empty_connections();
                                }

                                if new_opts.is_empty() && expired_opts.is_empty() {
                                    debug!(total = active_names.len(), "poll: no changes");
                                }
                            }
                            Err(e) => {
                                warn!(error = ?e, "poll fetch failed");
                            }
                        }
                    }
                }
            }
            debug!("poll loop done");
        });

        // Task 2: Instrument state loop
        let ct2 = ct.clone();
        let tracked2 = tracked.clone();
        let pool2 = pool.clone();
        let interval2 = interval;
        let mut sub_rx = pool.subscribe_to_broadcast();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = ct2.cancelled() => break,
                    result = sub_rx.recv() => {
                        let msg = match result {
                            Ok(m) => m,
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                debug!("instrument state: broadcast channel closed");
                                break;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                warn!(skipped = n, "instrument state: lagged behind broadcast");
                                continue;
                            }
                        };
                        if let Ok(sub_msg) = serde_json::from_str::<SubscriptionMessage>(&msg) {
                            if let SubscriptionParams::Subscribe(params) = sub_msg.params {
                                if params.channel.starts_with("instrument_state.") {
                                    if let Ok(state_data) = serde_json::from_value::<InstrumentStateData>(params.data) {
                                        // Use write lock directly to avoid TOCTOU race
                                        let should_subscribe = {
                                            let mut t = tracked2.write().unwrap();
                                            t.insert(state_data.instrument_name.clone())
                                        };
                                        if should_subscribe {
                                            let channel = format!("ticker.{}.{}", state_data.instrument_name, interval2);
                                            info!(instrument = state_data.instrument_name, "new option from instrument_state");
                                            if let Err(e) = pool2.subscribe(vec![channel]).await {
                                                warn!(error = ?e, "instrument_state subscribe failed");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            debug!("instrument state loop done");
        });

        // Task 3: Periodic metrics logging (every 60 seconds)
        let ct3 = ct.clone();
        let tracked3 = tracked.clone();
        let pool3 = pool.clone();
        let mut prev_snapshot = nq_deribit::metrics::MetricsSnapshot::read();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = ct3.cancelled() => break,
                    _ = sleep(Duration::from_secs(60)) => {
                        let snapshot = nq_deribit::metrics::MetricsSnapshot::read();
                        let rates = snapshot.rates_since(&prev_snapshot, 60.0);
                        prev_snapshot = snapshot;

                        let t_count = tracked3.read().unwrap().len();
                        let conn_count = pool3.connection_count();
                        let conns = pool3.connection_runners();
                        let channel_counts: Vec<usize> = conns.iter().map(|c| c.channel_count()).collect();

                        // Read memory usage (cross-platform)
                        let memory_kb = read_memory_kb();

                        info!(
                            tracked_options = t_count,
                            connections = conn_count,
                            channel_counts = ?channel_counts,
                            memory_kb = memory_kb,
                            // Cumulative counters
                            deribit_received = snapshot.deribit_sub_received,
                            deribit_enqueued = snapshot.deribit_sub_enqueued,
                            deribit_dropped = snapshot.deribit_sub_dropped,
                            mqtt_published = snapshot.mqtt_published,
                            mqtt_failed = snapshot.mqtt_publish_failed,
                            // Per-second rates (over last 60s window)
                            deribit_recv_per_sec = format_rate(rates.deribit_sub_received_per_sec),
                            deribit_enq_per_sec = format_rate(rates.deribit_sub_enqueued_per_sec),
                            deribit_drop_per_sec = format_rate(rates.deribit_sub_dropped_per_sec),
                            mqtt_pub_per_sec = format_rate(rates.mqtt_published_per_sec),
                            "periodic metrics (1m)"
                        );
                    }
                }
            }
            debug!("metrics loop done");
        });

        ct.cancelled().await;
        info!("subscription manager done");
        Ok(())
    }
}

// ─── Helper functions ─────────────────────────────────────────────

/// Read current process memory usage in KB. Cross-platform:
/// - Linux: reads VmRSS from /proc/self/status
/// - macOS: uses `task_info` via libc
fn read_memory_kb() -> u64 {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|content| {
                content
                    .lines()
                    .find(|line| line.starts_with("VmRSS:"))
                    .and_then(|line| line.split_whitespace().nth(1))
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .unwrap_or(0)
    }

    #[cfg(target_os = "macos")]
    {
        // Use `ps` to get RSS on macOS (no libc dependency needed)
        use std::process::Command;
        let pid = std::process::id();
        Command::new("ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .output()
            .ok()
            .and_then(|out| String::from_utf8(out.stdout).ok())
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        0 // Unsupported platform
    }
}

/// Format a f64 rate to a compact string with 1 decimal place.
fn format_rate(rate: f64) -> String {
    format!("{:.1}", rate)
}
