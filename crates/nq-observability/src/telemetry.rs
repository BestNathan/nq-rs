use std::env;
use std::time::Duration;

use anyhow::Result;
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::{LogExporter, MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

fn otlp_endpoint() -> String {
    env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://otel-collector.observability:4317".into())
}

/// Configuration for OpenTelemetry initialization.
pub struct TelemetryConfig {
    /// Service name — sets `service.name` resource attribute (required).
    pub service_name: String,
    /// Service version — sets `service.version` resource attribute.
    /// Falls back to `OTEL_SERVICE_VERSION` env var, then `"unknown"`.
    pub service_version: Option<String>,
}

impl TelemetryConfig {
    pub fn new(service_name: impl Into<String>) -> Self {
        Self { service_name: service_name.into(), service_version: None }
    }

    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.service_version = Some(version.into());
        self
    }
}

/// Read `OTEL_RESOURCE_ATTRIBUTES` env var and parse into `KeyValue` pairs.
///
/// Format: `key1=value1,key2=value2,...`
fn otel_resource_attributes() -> Vec<KeyValue> {
    env::var("OTEL_RESOURCE_ATTRIBUTES")
        .ok()
        .into_iter()
        .flat_map(|s| {
            s.split(',')
                .filter_map(|pair| {
                    let (k, v) = pair.split_once('=')?;
                    Some(KeyValue::new(k.trim().to_string(), v.trim().to_string()))
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Build the OTel [`Resource`] with standard service attributes.
///
/// Attributes set:
/// - `service.name` — from config (required)
/// - `service.namespace` — `"nq"`
/// - `service.version` — from config, or `OTEL_SERVICE_VERSION` env var, or `"unknown"`
/// - `deployment.environment` — from `DEPLOY_ENV` env var, default `"development"`
/// - Any additional attributes from `OTEL_RESOURCE_ATTRIBUTES` env var
fn build_resource(config: &TelemetryConfig) -> Resource {
    let service_version = config
        .service_version
        .clone()
        .or_else(|| env::var("OTEL_SERVICE_VERSION").ok())
        .unwrap_or_else(|| "unknown".into());

    let deploy_env = env::var("DEPLOY_ENV").unwrap_or_else(|_| "development".into());

    let mut resource =
        Resource::builder().with_service_name(config.service_name.clone()).with_attributes([
            KeyValue::new("service.namespace", "nq"),
            KeyValue::new("service.version", service_version),
            KeyValue::new("deployment.environment", deploy_env),
        ]);

    for attr in otel_resource_attributes() {
        resource = resource.with_attributes([attr]);
    }

    resource.build()
}

/// Initialize OpenTelemetry: traces, metrics, logs — all exported via OTLP gRPC.
///
/// The returned `OTelGuard` must be kept alive for the lifetime of the process.
///
/// # Resource attributes
///
/// | Attribute | Source |
/// |---|---|
/// | `service.name` | `config.service_name` (required) |
/// | `service.namespace` | `"nq"` (hardcoded) |
/// | `service.version` | `config.service_version` → `OTEL_SERVICE_VERSION` → `"unknown"` |
/// | `deployment.environment` | `DEPLOY_ENV` env var, default `"development"` |
/// | (custom) | `OTEL_RESOURCE_ATTRIBUTES` env var (`key=value,...`) |
///
/// # Env vars
///
/// - `OTEL_EXPORTER_OTLP_ENDPOINT` — OTLP collector address (default: `http://otel-collector.observability:4317`)
/// - `RUST_LOG` — log level filter (default: `info`)
pub fn init_telemetry(config: TelemetryConfig) -> Result<OTelGuard> {
    let endpoint = otlp_endpoint();
    let resource = build_resource(&config);

    // ── Metrics ──────────────────────────────────────────────────────
    let metric_exporter = MetricExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .with_timeout(Duration::from_secs(10))
        .build()?;

    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(resource.clone())
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
        .with_resource(resource.clone())
        .build();

    let tracer = tracer_provider.tracer(config.service_name.clone());
    global::set_tracer_provider(tracer_provider.clone());

    // ── Logs (via tracing bridge) ────────────────────────────────────
    let log_exporter = LogExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .with_timeout(Duration::from_secs(10))
        .build()?;

    let logger_provider = SdkLoggerProvider::builder()
        .with_batch_exporter(log_exporter)
        .with_resource(resource)
        .build();

    // ── tracing subscriber ───────────────────────────────────────────
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
