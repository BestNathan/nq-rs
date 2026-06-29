//! Tokio runtime metrics exported via OTel.
//!
//! Spawn a background task that periodically collects [`RuntimeMetrics`]
//! from the Tokio runtime and records them as OTel instruments.
//!
//! Uses stable `tokio-metrics` fields only (no `tokio_unstable` cfg required):
//! `live_tasks_count`, `global_queue_depth`, `workers_count`.

use std::time::Duration;

use opentelemetry::metrics::Meter;
use tokio_metrics::RuntimeMonitor;

use crate::metrics::meter;

/// Spawn a background task that records Tokio runtime metrics at the given
/// interval. Returns a [`tokio::task::JoinHandle`] that resolves when the
/// task exits (which is never, unless the runtime shuts down).
///
/// Collects:
/// - `tokio.active_tasks` (gauge) — number of currently live tasks
/// - `tokio.global_queue_depth` (gauge) — tasks waiting in the global queue
/// - `tokio.workers` (gauge) — number of worker threads
pub fn spawn_tokio_metrics(interval: Duration) -> tokio::task::JoinHandle<()> {
    let handle = tokio::runtime::Handle::current();
    let monitor = RuntimeMonitor::new(&handle);
    let m = meter("tokio");

    record_metrics(monitor, m, interval)
}

/// Shared implementation: takes a `RuntimeMonitor` and `Meter`, loops forever
/// collecting metrics at `interval` cadence and recording them to OTel.
fn record_metrics(
    monitor: RuntimeMonitor,
    m: Meter,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    let active_tasks = m
        .u64_gauge("tokio.active_tasks")
        .with_description("Number of currently alive tasks in the Tokio runtime")
        .build();
    let global_queue_depth = m
        .u64_gauge("tokio.global_queue_depth")
        .with_description("Tasks currently scheduled in the runtime's global queue")
        .build();
    let workers = m
        .u64_gauge("tokio.workers")
        .with_description("Number of worker threads in the Tokio runtime")
        .build();

    tokio::spawn(async move {
        for metrics in monitor.intervals() {
            active_tasks.record(metrics.live_tasks_count as u64, &[]);
            global_queue_depth.record(metrics.global_queue_depth as u64, &[]);
            workers.record(metrics.workers_count as u64, &[]);
            tokio::time::sleep(interval).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_spawn_tokio_metrics_compiles() {
        // Verify the public API compiles and spawn_tokio_metrics returns a JoinHandle.
        let handle = spawn_tokio_metrics(Duration::from_secs(15));
        handle.abort(); // clean up immediately
    }
}
