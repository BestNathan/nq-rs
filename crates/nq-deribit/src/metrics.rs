//! OpenTelemetry metrics counters for the Deribit data pipeline.
//!
//! Uses OTel Counters (exported via OTLP to Prometheus) instead of
//! the previous `AtomicU64` statics. Access via the global `DERIBIT_METRICS` lazy.

use nq_observability::metrics::{meter, Counter};
use once_cell::sync::Lazy;

/// Global Deribit pipeline metrics. Initialized lazily on first access.
/// Requires `nq_observability::init_telemetry()` to have been called first
/// (sets the global MeterProvider).
pub static DERIBIT_METRICS: Lazy<DeribitMetrics> = Lazy::new(|| {
    let m = meter("nq-deribit");
    DeribitMetrics {
        sub_received: m
            .u64_counter("deribit.sub.received")
            .with_description("Total subscription messages received from Deribit WebSocket")
            .build(),
        sub_enqueued: m
            .u64_counter("deribit.sub.enqueued")
            .with_description("Subscription messages successfully enqueued to broadcast channel")
            .build(),
        sub_dropped: m
            .u64_counter("deribit.sub.dropped")
            .with_description("Subscription messages dropped (broadcast channel full)")
            .build(),
        mqtt_published: m
            .u64_counter("mqtt.published")
            .with_description("Messages successfully published to MQTT/EMQX")
            .build(),
        mqtt_publish_failed: m
            .u64_counter("mqtt.publish.failed")
            .with_description("MQTT publish attempts that failed")
            .build(),
    }
});

pub struct DeribitMetrics {
    pub sub_received: Counter<u64>,
    pub sub_enqueued: Counter<u64>,
    pub sub_dropped: Counter<u64>,
    pub mqtt_published: Counter<u64>,
    pub mqtt_publish_failed: Counter<u64>,
}
