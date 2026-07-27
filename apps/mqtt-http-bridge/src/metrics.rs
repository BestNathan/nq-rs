use nq_observability::metrics::{Counter, Histogram, meter};
use std::sync::LazyLock;

pub static BRIDGE_METRICS: LazyLock<BridgeMetrics> = LazyLock::new(|| {
    let m = meter("mqtt-http-bridge");
    BridgeMetrics {
        messages_received: m
            .u64_counter("mqtt_http_messages_received")
            .with_description("MQTT messages received by the bridge")
            .build(),
        messages_forwarded: m
            .u64_counter("mqtt_http_messages_forwarded")
            .with_description("Messages successfully forwarded via HTTP")
            .build(),
        batches_sent: m
            .u64_counter("mqtt_http_batches_sent")
            .with_description("Batch HTTP requests completed")
            .build(),
        failures: m
            .u64_counter("mqtt_http_failures")
            .with_description("HTTP dispatch failures")
            .build(),
        batch_size: m
            .f64_histogram("mqtt_http_batch_size")
            .with_description("Histogram of actual batch sizes at dispatch time")
            .build(),
        latency_ms: m
            .f64_histogram("mqtt_http_latency_ms")
            .with_description("Histogram of HTTP request duration in milliseconds")
            .build(),
    }
});

pub struct BridgeMetrics {
    pub messages_received: Counter<u64>,
    pub messages_forwarded: Counter<u64>,
    pub batches_sent: Counter<u64>,
    pub failures: Counter<u64>,
    pub batch_size: Histogram<f64>,
    pub latency_ms: Histogram<f64>,
}
