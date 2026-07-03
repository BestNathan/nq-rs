use std::env;
use std::time::Duration;

use anyhow::Result;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::{LogExporter, MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

fn otlp_endpoint() -> String {
    env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://otel-collector.observability:4317".into())
}

/// Initialize OpenTelemetry: traces, metrics, logs — all exported via OTLP gRPC.
///
/// - Log level is controlled by `RUST_LOG` env var (defaults to `info`).
///   The same filter applies to console output AND OTel export — internal
///   TRACE/DEBUG noise from tonic/h2/hyper is never sent to the collector.
/// - Metrics export interval: `OTEL_METRICS_EXPORT_INTERVAL_SECS` (default 60s).
///
/// The returned `OTelGuard` must be kept alive for the lifetime of the process.
pub fn init_telemetry(service_name: &str) -> Result<OTelGuard> {
    let endpoint = otlp_endpoint();

    // ── Metrics ──────────────────────────────────────────────────────
    let metric_exporter = MetricExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .with_timeout(Duration::from_secs(10))
        .build()?;

    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name(service_name.to_string())
                .build(),
        )
        .build();

    global::set_meter_provider(meter_provider.clone());

    // ── Traces ───────────────────────────────────────────────────────
    let span_exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .with_timeout(Duration::from_secs(10))
        .build()?;

    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name(service_name.to_string())
                .build(),
        )
        .build();

    let tracer = tracer_provider.tracer(service_name.to_string());
    global::set_tracer_provider(tracer_provider.clone());

    // ── Logs (via tracing bridge) ────────────────────────────────────
    let log_exporter = LogExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .with_timeout(Duration::from_secs(10))
        .build()?;

    let logger_provider = SdkLoggerProvider::builder()
        .with_batch_exporter(log_exporter)
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name(service_name.to_string())
                .build(),
        )
        .build();

    // ── tracing subscriber ───────────────────────────────────────────
    // Global filter from RUST_LOG (default info). Applied to ALL layers
    // so internal TRACE/DEBUG from tonic/h2/opentelemetry_sdk stays out
    // of both console and the OTel collector.
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    let otel_trace_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let otel_log_layer =
        opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(&logger_provider);

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(otel_trace_layer)
        .with(otel_log_layer)
        .with(filter)
        .try_init()
        .ok();

    Ok(OTelGuard {
        _meter_provider: meter_provider,
        _tracer_provider: tracer_provider,
        _logger_provider: logger_provider,
    })
}

pub struct OTelGuard {
    _meter_provider: SdkMeterProvider,
    _tracer_provider: SdkTracerProvider,
    _logger_provider: SdkLoggerProvider,
}

impl Drop for OTelGuard {
    fn drop(&mut self) {
        let _ = self._meter_provider.force_flush();
        let _ = self._tracer_provider.force_flush();
        let _ = self._logger_provider.force_flush();
    }
}
