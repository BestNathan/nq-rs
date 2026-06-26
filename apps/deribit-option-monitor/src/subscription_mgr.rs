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
        let conn = pool.first_connection();
        let sub_rx = conn.subscription_rx();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = ct2.cancelled() => break,
                    msg = sub_rx.recv_async() => {
                        let msg = match msg {
                            Ok(m) => m,
                            Err(_) => break,
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

        // Task 3: Periodic metrics logging
        let ct3 = ct.clone();
        let tracked3 = tracked.clone();
        let pool3 = pool.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = ct3.cancelled() => break,
                    _ = sleep(Duration::from_secs(300)) => {
                        let t_count = tracked3.read().unwrap().len();
                        let conn_count = pool3.connection_count();
                        let conns = pool3.connection_runners();
                        let channel_counts: Vec<usize> = conns.iter().map(|c| c.channel_count()).collect();
                        info!(
                            tracked_options = t_count,
                            connections = conn_count,
                            channel_counts = ?channel_counts,
                            "periodic metrics"
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
