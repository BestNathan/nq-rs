//! OpenTelemetry metrics counters for the Deribit data pipeline.
//!
//! Uses OTel Counters (exported via OTLP to Prometheus) instead of
//! the previous `AtomicU64` statics. Access via the global `DERIBIT_METRICS` lazy.

use nq_observability::metrics::{Counter, meter};
use once_cell::sync::Lazy;

/// Extract the channel group from a Deribit subscription notification JSON.
///
/// A subscription message looks like:
/// ```json
/// {"jsonrpc":"2.0","method":"subscription","params":{"channel":"ticker.BTC-...","data":{...}}}
/// ```
///
/// Returns the prefix of `params.channel` before the first `.` (e.g. `"ticker"`),
/// or `"unknown"` if the channel field cannot be found.
///
/// This is a zero-allocation string scan — it does NOT parse the full JSON.
pub fn extract_channel_group(text: &str) -> &str {
    const KEY: &str = "\"channel\":\"";
    if let Some(pos) = text.find(KEY) {
        let start = pos + KEY.len();
        let remaining = &text[start..];
        if let Some(end) = remaining.find('"') {
            let channel = &remaining[..end];
            if let Some(dot) = channel.find('.') {
                return &channel[..dot];
            }
            return channel;
        }
    }
    "unknown"
}

/// Global Deribit pipeline metrics. Initialized lazily on first access.
/// Requires `nq_observability::init_telemetry()` to have been called first
/// (sets the global MeterProvider).
pub static DERIBIT_METRICS: Lazy<DeribitMetrics> = Lazy::new(|| {
    let m = meter("nq-deribit");
    DeribitMetrics {
        sub_received: m
            .u64_counter("nq_deribit_sub_received")
            .with_description("Total subscription messages received from Deribit WebSocket")
            .build(),
        sub_enqueued: m
            .u64_counter("nq_deribit_sub_enqueued")
            .with_description("Subscription messages successfully enqueued to broadcast channel")
            .build(),
        sub_dropped: m
            .u64_counter("nq_deribit_sub_dropped")
            .with_description("Subscription messages dropped (broadcast channel full)")
            .build(),
        mqtt_published: m
            .u64_counter("nq_mqtt_published")
            .with_description("Messages successfully published to MQTT/EMQX")
            .build(),
        mqtt_publish_failed: m
            .u64_counter("nq_mqtt_publish_failed")
            .with_description("MQTT publish attempts that failed")
            .build(),
        broadcast_lagged: m
            .u64_counter("nq_deribit_broadcast_lagged")
            .with_description("Messages skipped due to slow consumer (broadcast channel lagged)")
            .build(),
    }
});

pub struct DeribitMetrics {
    pub sub_received: Counter<u64>,
    pub sub_enqueued: Counter<u64>,
    pub sub_dropped: Counter<u64>,
    pub mqtt_published: Counter<u64>,
    pub mqtt_publish_failed: Counter<u64>,
    pub broadcast_lagged: Counter<u64>,
}
