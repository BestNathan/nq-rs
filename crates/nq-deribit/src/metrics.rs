//! Global metrics counters for the Deribit data pipeline.
//!
//! These are `AtomicU64` statics that are incremented at key points in the
//! data flow: Deribit WS → subscription channel → MQTT publish.
//!
//! Counters are cheap (lock-free) and safe to increment from any task.

use std::sync::atomic::{AtomicU64, Ordering};

// ─── Deribit-side counters (incremented in connection.rs) ──────────

/// Total subscription messages received from Deribit WebSocket.
pub static DERIBIT_SUB_RECEIVED: AtomicU64 = AtomicU64::new(0);

/// Subscription messages successfully enqueued into the subscription channel.
pub static DERIBIT_SUB_ENQUEUED: AtomicU64 = AtomicU64::new(0);

/// Subscription messages dropped because the subscription channel was full.
pub static DERIBIT_SUB_DROPPED: AtomicU64 = AtomicU64::new(0);

// ─── MQTT-side counters (incremented in ticker_router.rs) ─────────

/// Ticker messages successfully published to MQTT/EMQX.
pub static MQTT_PUBLISHED: AtomicU64 = AtomicU64::new(0);

/// MQTT publish attempts that failed.
pub static MQTT_PUBLISH_FAILED: AtomicU64 = AtomicU64::new(0);

// ─── Convenience functions ─────────────────────────────────────────

/// Snapshot of all counters at a point in time.
#[derive(Debug, Clone, Copy, Default)]
pub struct MetricsSnapshot {
    pub deribit_sub_received: u64,
    pub deribit_sub_enqueued: u64,
    pub deribit_sub_dropped: u64,
    pub mqtt_published: u64,
    pub mqtt_publish_failed: u64,
}

impl MetricsSnapshot {
    pub fn read() -> Self {
        Self {
            deribit_sub_received: DERIBIT_SUB_RECEIVED.load(Ordering::Relaxed),
            deribit_sub_enqueued: DERIBIT_SUB_ENQUEUED.load(Ordering::Relaxed),
            deribit_sub_dropped: DERIBIT_SUB_DROPPED.load(Ordering::Relaxed),
            mqtt_published: MQTT_PUBLISHED.load(Ordering::Relaxed),
            mqtt_publish_failed: MQTT_PUBLISH_FAILED.load(Ordering::Relaxed),
        }
    }

    /// Return per-second rates computed against a previous snapshot and elapsed seconds.
    pub fn rates_since(&self, prev: &MetricsSnapshot, elapsed_secs: f64) -> MetricsRates {
        let dt = if elapsed_secs > 0.0 { elapsed_secs } else { 1.0 };
        MetricsRates {
            deribit_sub_received_per_sec: (self.deribit_sub_received.saturating_sub(prev.deribit_sub_received)) as f64 / dt,
            deribit_sub_enqueued_per_sec: (self.deribit_sub_enqueued.saturating_sub(prev.deribit_sub_enqueued)) as f64 / dt,
            deribit_sub_dropped_per_sec: (self.deribit_sub_dropped.saturating_sub(prev.deribit_sub_dropped)) as f64 / dt,
            mqtt_published_per_sec: (self.mqtt_published.saturating_sub(prev.mqtt_published)) as f64 / dt,
            mqtt_publish_failed_per_sec: (self.mqtt_publish_failed.saturating_sub(prev.mqtt_publish_failed)) as f64 / dt,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MetricsRates {
    pub deribit_sub_received_per_sec: f64,
    pub deribit_sub_enqueued_per_sec: f64,
    pub deribit_sub_dropped_per_sec: f64,
    pub mqtt_published_per_sec: f64,
    pub mqtt_publish_failed_per_sec: f64,
}
