//! Tokio runtime metrics exported via OTel.
//!
//! Spawn a background task that periodically collects [`RuntimeMetrics`]
//! from the Tokio runtime and records them as OTel instruments.
//!
//! Requires `--cfg tokio_unstable` (set in `.cargo/config.toml`).
//! All fields below come from `tokio-metrics` 0.5 `RuntimeMetrics`.

use std::time::Duration;

use opentelemetry::metrics::Meter;
use tokio_metrics::RuntimeMonitor;

use crate::metrics::meter;

/// Spawn a background task that records Tokio runtime metrics at the given
/// interval. Returns a [`tokio::task::JoinHandle`] that resolves when the
/// task exits (which is never, unless the runtime shuts down).
///
/// **Stable fields** (always available):
/// - `tokio.active_tasks` (gauge) — number of currently live tasks
/// - `tokio.global_queue_depth` (gauge) — tasks waiting in the global queue
/// - `tokio.workers` (gauge) — number of worker threads
///
/// **Unstable fields** (require `tokio_unstable` cfg):
/// - `tokio.total_busy_secs` (counter) — cumulative worker busy time
/// - `tokio.total_polls` (counter) — cumulative task polls
/// - `tokio.total_steals` (counter) — tasks stolen between workers
/// - `tokio.total_overflows` (counter) — local queue overflow events
/// - `tokio.total_noops` (counter) — false-positive wakeups
/// - `tokio.total_local_queue_depth` (gauge) — current tasks in local queues
/// - `tokio.budget_yields` (counter) — forced yields from exhausted budgets
/// - `tokio.io_ready_events` (counter) — I/O driver ready events
/// - `tokio.mean_poll_us` (histogram) — per-poll duration (EWMA, µs)
/// - `tokio.blocking_queue_depth` (gauge) — tasks waiting in blocking pool
/// - `tokio.blocking_threads` (gauge) — threads in blocking pool (active / idle)
pub fn spawn_tokio_metrics(interval: Duration) -> tokio::task::JoinHandle<()> {
    let handle = tokio::runtime::Handle::current();
    let monitor = RuntimeMonitor::new(&handle);
    let m = meter("tokio");

    record_metrics(monitor, m, interval)
}

fn record_metrics(
    monitor: RuntimeMonitor,
    m: Meter,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    // ── Stable gauges ────────────────────────────────────────────────
    let active_tasks = m
        .u64_gauge("tokio.active_tasks")
        .with_description("Number of currently alive tasks")
        .build();
    let global_queue_depth = m
        .u64_gauge("tokio.global_queue_depth")
        .with_description("Tasks scheduled in the runtime's global queue")
        .build();
    let workers = m
        .u64_gauge("tokio.workers")
        .with_description("Number of worker threads")
        .build();

    // ── Counters (cumulative, split into separate instruments) ───────
    let busy_secs = m
        .f64_counter("tokio.total_busy_secs")
        .with_description("Cumulative busy duration across all workers (seconds)")
        .build();
    let total_polls = m
        .u64_counter("tokio.total_polls")
        .with_description("Cumulative task polls across all workers")
        .build();
    let total_steals = m
        .u64_counter("tokio.total_steals")
        .with_description("Cumulative tasks stolen between workers")
        .build();
    let total_overflows = m
        .u64_counter("tokio.total_overflows")
        .with_description("Cumulative local-queue overflow events")
        .build();
    let total_noops = m
        .u64_counter("tokio.total_noops")
        .with_description("Cumulative false-positive unpark events")
        .build();
    let budget_yields = m
        .u64_counter("tokio.budget_yields")
        .with_description("Cumulative forced yields from exhausted budgets")
        .build();
    let io_events = m
        .u64_counter("tokio.io_ready_events")
        .with_description("Cumulative I/O driver ready events")
        .build();

    // ── Gauges ───────────────────────────────────────────────────────
    let local_queue_depth = m
        .u64_gauge("tokio.total_local_queue_depth")
        .with_description("Current total tasks in all workers' local queues")
        .build();
    let blocking_queue_depth = m
        .u64_gauge("tokio.blocking_queue_depth")
        .with_description("Tasks waiting in the blocking threadpool")
        .build();
    let blocking_threads = m
        .u64_gauge("tokio.blocking_threads")
        .with_description("Active threads in blocking pool")
        .build();
    let idle_blocking_threads = m
        .u64_gauge("tokio.idle_blocking_threads")
        .with_description("Idle threads in blocking pool")
        .build();

    // ── Histogram ────────────────────────────────────────────────────
    let mean_poll_us = m
        .f64_histogram("tokio.mean_poll_us")
        .with_description("Exponentially-weighted moving average poll duration (µs)")
        .build();

    tokio::spawn(async move {
        for metrics in monitor.intervals() {
            // Stable
            active_tasks.record(metrics.live_tasks_count as u64, &[]);
            global_queue_depth.record(metrics.global_queue_depth as u64, &[]);
            workers.record(metrics.workers_count as u64, &[]);

            // Counters
            busy_secs.add(metrics.total_busy_duration.as_secs_f64(), &[]);
            total_polls.add(metrics.total_polls_count, &[]);
            total_steals.add(metrics.total_steal_count, &[]);
            total_overflows.add(metrics.total_overflow_count, &[]);
            total_noops.add(metrics.total_noop_count, &[]);
            budget_yields.add(metrics.budget_forced_yield_count, &[]);
            io_events.add(metrics.io_driver_ready_count, &[]);

            // Gauges
            local_queue_depth.record(metrics.total_local_queue_depth as u64, &[]);
            blocking_queue_depth.record(metrics.blocking_queue_depth as u64, &[]);
            blocking_threads.record(metrics.blocking_threads_count as u64, &[]);
            idle_blocking_threads.record(metrics.idle_blocking_threads_count as u64, &[]);

            // Histogram
            mean_poll_us.record(metrics.mean_poll_duration.as_micros() as f64, &[]);

            tokio::time::sleep(interval).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_spawn_tokio_metrics_compiles() {
        let handle = spawn_tokio_metrics(Duration::from_secs(15));
        handle.abort();
    }
}
