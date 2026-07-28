//! Bridge metrics — dual-registry: OTel (OTLP push) + Prometheus (pull + stdout).
//!
//! Every counter/histogram is recorded to both registries so Grafana (OTel)
//! and the /metrics endpoint + stdout log (Prometheus) stay in sync.

use nq_observability::metrics::{Counter as OtelCounter, Histogram as OtelHistogram, meter};
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry, TextEncoder,
};
use std::sync::LazyLock;
use tracing::info;

// ── Prometheus registry (for /metrics endpoint and stdout reporting) ──────

static PROM_REGISTRY: LazyLock<Registry> = LazyLock::new(|| {
    Registry::new_custom(Some("mqtt_http_bridge".into()), None).expect("create prometheus registry")
});

pub fn prometheus_metrics_text() -> String {
    let mut buf = Vec::new();
    let encoder = TextEncoder::new();
    let families = PROM_REGISTRY.gather();
    let _ = encoder.encode(&families, &mut buf);
    String::from_utf8_lossy(&buf).to_string()
}

/// Print a one-line summary of all metrics to stdout (called every 60s).
pub fn report_metrics() {
    let families = PROM_REGISTRY.gather();
    for mf in &families {
        for m in mf.get_metric() {
            let labels: Vec<String> = m
                .get_label()
                .iter()
                .map(|l| format!("{}=\"{}\"", l.get_name(), l.get_value()))
                .collect();
            let label_str =
                if labels.is_empty() { String::new() } else { format!("{{{}}}", labels.join(",")) };

            let val = if mf.get_field_type() == prometheus::proto::MetricType::COUNTER {
                format!("{}", m.get_counter().get_value() as u64)
            } else if mf.get_field_type() == prometheus::proto::MetricType::HISTOGRAM {
                format!(
                    "count={},sum={:.0}",
                    m.get_histogram().get_sample_count(),
                    m.get_histogram().get_sample_sum()
                )
            } else {
                "?".into()
            };

            info!(metric = mf.get_name(), labels = label_str, value = val, "bridge metrics");
        }
    }
}

// ── Metric definitions (dual-registry) ────────────────────────────────────

fn int_counter(name: &str, help: &str) -> IntCounterVec {
    IntCounterVec::new(Opts::new(name, help), &["config_id"]).expect("create prom counter")
}

fn histogram(name: &str, help: &str) -> HistogramVec {
    HistogramVec::new(HistogramOpts::new(name, help), &["config_id"])
        .expect("create prom histogram")
}

/// Helper: record on both OTel counter (no config label) and Prometheus counter (with config_id).
fn record_counter(prom: &IntCounterVec, config_id: &str, value: u64, otel: &OtelCounter<u64>) {
    otel.add(value, &[]);
    prom.with_label_values(&[config_id]).inc_by(value);
}

/// Helper: record on both OTel histogram (no config label) and Prometheus histogram (with config_id).
fn record_histogram(prom: &HistogramVec, config_id: &str, value: f64, otel: &OtelHistogram<f64>) {
    otel.record(value, &[]);
    prom.with_label_values(&[config_id]).observe(value);
}

pub struct BridgeMetrics {
    // Prometheus instruments
    pub prom_mqtt_received: IntCounterVec,
    pub prom_messages_processed: IntCounterVec,
    pub prom_http_requests: IntCounterVec,
    pub prom_http_success: IntCounterVec,
    pub prom_http_failures: IntCounterVec,
    pub prom_batch_size: HistogramVec,
    pub prom_http_latency_ms: HistogramVec,

    // OTel instruments
    pub otel_mqtt_received: OtelCounter<u64>,
    pub otel_messages_processed: OtelCounter<u64>,
    pub otel_http_requests: OtelCounter<u64>,
    pub otel_http_success: OtelCounter<u64>,
    pub otel_http_failures: OtelCounter<u64>,
    pub otel_batch_size: OtelHistogram<f64>,
    pub otel_http_latency_ms: OtelHistogram<f64>,
}

pub static BRIDGE_METRICS: LazyLock<BridgeMetrics> = LazyLock::new(|| {
    let m = meter("mqtt-http-bridge");

    let pm_rx = int_counter("bridge_mqtt_received_total", "MQTT messages received");
    let pm_done =
        int_counter("bridge_messages_processed_total", "Messages successfully forwarded to HTTP");
    let pm_req = int_counter("bridge_http_requests_total", "HTTP requests sent");
    let pm_ok = int_counter("bridge_http_success_total", "Successful HTTP responses (2xx)");
    let pm_fail =
        int_counter("bridge_http_failures_total", "Failed HTTP responses (4xx/5xx/timeout)");
    let pm_bs = histogram("bridge_batch_size", "Batch sizes at dispatch");
    let pm_lat = histogram("bridge_http_latency_ms", "HTTP request latency in ms");

    for var in [&pm_rx, &pm_done, &pm_req, &pm_ok, &pm_fail] {
        let _ = PROM_REGISTRY.register(Box::new(var.clone()));
    }
    let _ = PROM_REGISTRY.register(Box::new(pm_bs.clone()));
    let _ = PROM_REGISTRY.register(Box::new(pm_lat.clone()));

    BridgeMetrics {
        prom_mqtt_received: pm_rx,
        prom_messages_processed: pm_done,
        prom_http_requests: pm_req,
        prom_http_success: pm_ok,
        prom_http_failures: pm_fail,
        prom_batch_size: pm_bs,
        prom_http_latency_ms: pm_lat,
        otel_mqtt_received: m
            .u64_counter("bridge_mqtt_received_total")
            .with_description("MQTT messages received")
            .build(),
        otel_messages_processed: m
            .u64_counter("bridge_messages_processed_total")
            .with_description("Messages successfully forwarded to HTTP")
            .build(),
        otel_http_requests: m
            .u64_counter("bridge_http_requests_total")
            .with_description("HTTP requests sent")
            .build(),
        otel_http_success: m
            .u64_counter("bridge_http_success_total")
            .with_description("Successful HTTP responses (2xx)")
            .build(),
        otel_http_failures: m
            .u64_counter("bridge_http_failures_total")
            .with_description("Failed HTTP responses (4xx/5xx/timeout)")
            .build(),
        otel_batch_size: m
            .f64_histogram("bridge_batch_size")
            .with_description("Batch sizes at dispatch")
            .build(),
        otel_http_latency_ms: m
            .f64_histogram("bridge_http_latency_ms")
            .with_description("HTTP request latency in ms")
            .build(),
    }
});

// ── Convenience recording methods ─────────────────────────────────────────

impl BridgeMetrics {
    pub fn record_received(&self, config_id: &str) {
        record_counter(&self.prom_mqtt_received, config_id, 1, &self.otel_mqtt_received);
    }

    pub fn record_http_success(&self, config_id: &str, batch_size: u64, latency_ms: f64) {
        record_counter(&self.prom_http_requests, config_id, 1, &self.otel_http_requests);
        record_counter(&self.prom_http_success, config_id, 1, &self.otel_http_success);
        record_counter(
            &self.prom_messages_processed,
            config_id,
            batch_size,
            &self.otel_messages_processed,
        );
        record_histogram(
            &self.prom_batch_size,
            config_id,
            batch_size as f64,
            &self.otel_batch_size,
        );
        record_histogram(
            &self.prom_http_latency_ms,
            config_id,
            latency_ms,
            &self.otel_http_latency_ms,
        );
    }

    pub fn record_http_failure(&self, config_id: &str, batch_size: u64) {
        record_counter(&self.prom_http_requests, config_id, 1, &self.otel_http_requests);
        record_counter(&self.prom_http_failures, config_id, 1, &self.otel_http_failures);
        // Failed messages are still "processed" in the sense that we attempted
        record_counter(
            &self.prom_messages_processed,
            config_id,
            batch_size,
            &self.otel_messages_processed,
        );
    }
}
