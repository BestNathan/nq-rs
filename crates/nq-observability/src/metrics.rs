use opentelemetry::global;
use opentelemetry::metrics::Meter;

/// Get a named Meter from the global MeterProvider.
///
/// Panics if `init_telemetry()` hasn't been called yet (no global MeterProvider).
pub fn meter(scope: &'static str) -> Meter {
    global::meter(scope)
}

// Re-export commonly used OTel metrics types so downstream crates don't
// need to add opentelemetry as a direct dependency.
pub use opentelemetry::KeyValue;
pub use opentelemetry::metrics::{Counter, Histogram, UpDownCounter};
