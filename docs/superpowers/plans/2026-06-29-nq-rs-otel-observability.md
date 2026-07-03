# nq-rs OpenTelemetry Observability — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add full-stack OpenTelemetry observability: new `nq-observability` crate, migrate `AtomicU64` to OTel Counters, bridge `tracing` to OTel, provide Grafana dashboard.

**Architecture:** New `nq-observability` crate wraps OTel SDK (OTLP gRPC exporter). `nq-deribit` replaces `AtomicU64` counters with OTel instruments. Both apps call `init_telemetry()` at startup. Dashboard JSON generated for manual Grafana import.

**Tech Stack:** opentelemetry, opentelemetry_sdk, opentelemetry-otlp (tonic+grpc), opentelemetry-appender-tracing, tracing-opentelemetry, once_cell

---

### Task 1: Create nq-observability crate — Cargo.toml

**Files:**
- Create: `crates/nq-observability/Cargo.toml`

- [ ] **Step 1: Create Cargo.toml**

```toml
[package]
edition = "2024"
name = "nq-observability"
version = "0.1.0"

[dependencies]
anyhow = {workspace = true}
once_cell = "1.20"
opentelemetry = "0.28"
opentelemetry_sdk = { version = "0.28", features = ["rt-tokio"] }
opentelemetry-otlp = { version = "0.28", features = ["tonic", "metrics", "logs", "traces"] }
opentelemetry-appender-tracing = "0.28"
tokio = {workspace = true}
tracing = {workspace = true}
tracing-opentelemetry = "0.29"
tracing-subscriber = {workspace = true}
```

- [ ] **Step 2: Verify it parses**

```bash
cargo metadata --format-version 1 > /dev/null 2>&1 && echo "OK"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add crates/nq-observability/Cargo.toml
git commit -m "feat(observability): create nq-observability crate skeleton"
```

---

### Task 2: Create nq-observability — metrics.rs (tool function)

**Files:**
- Create: `crates/nq-observability/src/metrics.rs`

- [ ] **Step 1: Write metrics.rs**

```rust
use opentelemetry::global;
use opentelemetry::metrics::Meter;

/// Get a named Meter from the global MeterProvider.
///
/// Panics if `init_telemetry()` hasn't been called yet (no global MeterProvider).
pub fn meter(scope: &str) -> Meter {
    global::meter(scope)
}

// Re-export commonly used OTel metrics types so downstream crates don't
// need to add opentelemetry as a direct dependency.
pub use opentelemetry::metrics::{Counter, Histogram, UpDownCounter};
```

- [ ] **Step 2: Commit**

```bash
git add crates/nq-observability/src/metrics.rs
git commit -m "feat(observability): add meter() helper and OTel type re-exports"
```

---

### Task 3: Create nq-observability — telemetry.rs + lib.rs

**Files:**
- Create: `crates/nq-observability/src/telemetry.rs`
- Create: `crates/nq-observability/src/lib.rs`

- [ ] **Step 1: Write telemetry.rs**

```rust
use std::env;
use std::time::Duration;

use anyhow::Result;
use opentelemetry::global;
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
/// The returned `OTelGuard` must be kept alive for the lifetime of the process.
/// On drop it flushes and shuts down all providers.
pub fn init_telemetry(service_name: &str) -> Result<OTelGuard> {
    let endpoint = otlp_endpoint();
    let rt = opentelemetry_sdk::runtime::Tokio;

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
        .with_batch_exporter(span_exporter, rt.clone())
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
        .with_batch_exporter(log_exporter, rt.clone())
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name(service_name.to_string())
                .build(),
        )
        .build();

    // ── tracing subscriber: fmt layer + OTel layers ──────────────────
    let otel_trace_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let otel_log_layer =
        opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(
            &logger_provider,
        );

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(otel_trace_layer)
        .with(otel_log_layer)
        .try_init()
        .ok(); // ignore if already initialized (tests may call multiple times)

    Ok(OTelGuard {
        _meter_provider: meter_provider,
        _tracer_provider: tracer_provider,
        _logger_provider: logger_provider,
    })
}

/// Guard that flushes and shuts down OTel providers on drop.
pub struct OTelGuard {
    _meter_provider: SdkMeterProvider,
    _tracer_provider: SdkTracerProvider,
    _logger_provider: SdkLoggerProvider,
}

impl Drop for OTelGuard {
    fn drop(&mut self) {
        // Force-flush everything before shutdown.
        let _ = self._meter_provider.force_flush();
        let _ = self._tracer_provider.force_flush();
        let _ = self._logger_provider.force_flush();
        // Shutdowns happen on drop of each provider struct.
    }
}
```

- [ ] **Step 2: Write lib.rs**

```rust
pub mod metrics;
pub mod telemetry;

pub use telemetry::{init_telemetry, OTelGuard};
```

- [ ] **Step 3: Verify compilation**

```bash
cargo check -p nq-observability 2>&1
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add crates/nq-observability/src/
git commit -m "feat(observability): OTel SDK init with OTLP gRPC exporter"
```

---

### Task 4: Register nq-observability in workspace + add dep to nq-deribit

**Files:**
- Modify: `Cargo.toml` (workspace)
- Modify: `crates/nq-deribit/Cargo.toml`

- [ ] **Step 1: Add workspace dependency**

In `Cargo.toml`, add after line 18 (`nq-mqtt = ...`):

```toml
nq-observability = { path = "./crates/nq-observability" }
```

- [ ] **Step 2: Add dep to nq-deribit Cargo.toml**

Under `[dependencies]` in `crates/nq-deribit/Cargo.toml`, add:

```toml
nq-observability = {workspace = true}
once_cell = "1.20"
```

- [ ] **Step 3: Verify workspace metadata**

```bash
cargo metadata --format-version 1 > /dev/null 2>&1 && echo "OK"
```

Expected: `OK`

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml crates/nq-deribit/Cargo.toml
git commit -m "chore: register nq-observability in workspace, add to nq-deribit"
```

---

### Task 5: Migrate nq-deribit metrics.rs — AtomicU64 → OTel Counters

**Files:**
- Modify: `crates/nq-deribit/src/metrics.rs`

- [ ] **Step 1: Replace the entire file**

Replace the current `metrics.rs` content with:

```rust
//! OpenTelemetry metrics counters for the Deribit data pipeline.
//!
//! Uses OTel Counters (exported via OTLP to Prometheus) instead of
//! the previous `AtomicU64` statics. Access via the global `DERIBIT_METRICS` lazy.

use nq_observability::{meter, Counter};
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
```

Remove: `AtomicU64` statics, `Ordering` imports, `MetricsSnapshot` struct, `MetricsRates` struct, `read()` method, `rates_since()` method.

- [ ] **Step 2: Verify compilation**

```bash
cargo check -p nq-deribit 2>&1
```

Expected: errors about `DERIBIT_SUB_RECEIVED` / `MQTT_PUBLISHED` / `MetricsSnapshot` in other files (to be fixed in Tasks 6-8).

- [ ] **Step 3: Commit**

```bash
git add crates/nq-deribit/src/metrics.rs
git commit -m "refactor(nq-deribit): migrate AtomicU64 metrics to OTel Counters

Replace AtomicU64 statics with OTel Counter instruments via
nq-observability. Remove MetricsSnapshot/MetricsRates (Prometheus
handles rate computation). Call sites will be updated next.
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 6: Update counter call sites — protocol.rs + connection.rs

**Files:**
- Modify: `crates/nq-deribit/src/protocol.rs`
- Modify: `crates/nq-deribit/src/connection.rs` (if any counter calls)

- [ ] **Step 1: Update protocol.rs counter calls**

Three calls to update in `protocol.rs` lines 287-297.

Old:
```rust
crate::metrics::DERIBIT_SUB_RECEIVED
    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
```

New:
```rust
crate::metrics::DERIBIT_METRICS.sub_received.add(1, &[]);
```

Old:
```rust
crate::metrics::DERIBIT_SUB_ENQUEUED
    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
```

New:
```rust
crate::metrics::DERIBIT_METRICS.sub_enqueued.add(1, &[]);
```

Old:
```rust
crate::metrics::DERIBIT_SUB_DROPPED
    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
```

New:
```rust
crate::metrics::DERIBIT_METRICS.sub_dropped.add(1, &[]);
```

- [ ] **Step 2: Verify compilation**

```bash
cargo check -p nq-deribit 2>&1
```

Expected: errors only from external callers (ticker_router.rs in option-monitor, subscription_mgr.rs).

- [ ] **Step 3: Commit**

```bash
git add crates/nq-deribit/src/protocol.rs
git commit -m "refactor(nq-deribit): update counter calls in protocol.rs to OTel"
```

---

### Task 7: Update counter call sites — option-monitor's ticker_router.rs + subscription_mgr.rs

**Files:**
- Modify: `apps/deribit-option-monitor/src/ticker_router.rs`
- Modify: `apps/deribit-option-monitor/src/subscription_mgr.rs`

- [ ] **Step 1: Update ticker_router.rs MQTT counter calls**

Three calls at lines 94, 97, 101. Replace:

Old:
```rust
metrics::MQTT_PUBLISHED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
```

New:
```rust
nq_deribit::metrics::DERIBIT_METRICS.mqtt_published.add(1, &[]);
```

Old:
```rust
metrics::MQTT_PUBLISH_FAILED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
```

New:
```rust
nq_deribit::metrics::DERIBIT_METRICS.mqtt_publish_failed.add(1, &[]);
```

- [ ] **Step 2: Update subscription_mgr.rs — remove MetricsSnapshot usage**

Replace the periodic metrics logging block (lines 220-264) with a simplified version that keeps business info but drops counter/rate fields:

```rust
        // Task 3: Periodic status logging (every 60 seconds)
        let ct3 = ct.clone();
        let tracked3 = tracked.clone();
        let pool3 = pool.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = ct3.cancelled() => break,
                    _ = sleep(Duration::from_secs(60)) => {
                        let t_count = tracked3.read().unwrap().len();
                        let conn_count = pool3.connection_count();
                        let conns = pool3.connection_runners();
                        let channel_counts: Vec<usize> = conns.iter().map(|c| c.channel_count()).collect();
                        let memory_kb = read_memory_kb();

                        info!(
                            tracked_options = t_count,
                            connections = conn_count,
                            channel_counts = ?channel_counts,
                            memory_kb = memory_kb,
                            "periodic status (1m)"
                        );
                    }
                }
            }
            debug!("status loop done");
        });
```

Also remove the `use nq_deribit::metrics::MetricsSnapshot` import (if present).
Remove the `format_rate` helper function (line 313) — it was only used for the now-removed rate logging.

- [ ] **Step 3: Verify compilation**

```bash
cargo check -p deribit-option-monitor 2>&1
```

Expected: no errors (or only about missing `init_telemetry`).

- [ ] **Step 4: Commit**

```bash
git add apps/deribit-option-monitor/src/ticker_router.rs apps/deribit-option-monitor/src/subscription_mgr.rs
git commit -m "refactor(option-monitor): migrate counter calls and metrics logging to OTel

- Replace MQTT_* AtomicU64 calls with DERIBIT_METRICS OTel counters
- Simplify periodic status log: remove MetricsSnapshot/rates (now in Prometheus)
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 8: Add init_telemetry to apps

**Files:**
- Modify: `apps/deribit-subscription/Cargo.toml`
- Modify: `apps/deribit-subscription/src/main.rs`
- Modify: `apps/deribit-option-monitor/Cargo.toml`
- Modify: `apps/deribit-option-monitor/src/main.rs`

- [ ] **Step 1: Add nq-observability dep to both apps' Cargo.toml**

In `apps/deribit-subscription/Cargo.toml` and `apps/deribit-option-monitor/Cargo.toml`, under `[dependencies]`, add:

```toml
nq-observability = {workspace = true}
```

- [ ] **Step 2: Add init_telemetry to deribit-subscription main.rs**

In `apps/deribit-subscription/src/main.rs`, after `tracing_subscriber::fmt::init();`:

Old:
```rust
    tracing_subscriber::fmt::init();
```

New:
```rust
    let _guard = nq_observability::init_telemetry("deribit-subscription")?;
```

(Remove `tracing_subscriber::fmt::init();` — `init_telemetry` already sets up the subscriber.)

- [ ] **Step 3: Add init_telemetry to deribit-option-monitor main.rs**

In `apps/deribit-option-monitor/src/main.rs`, at the start of `main()`:

```rust
#[tokio::main]
async fn main() -> Result<()> {
    let _guard = nq_observability::init_telemetry("deribit-option-monitor")?;
    // ... rest of main
```

If there's `tracing_subscriber::fmt::init();` already, remove it.

- [ ] **Step 4: Verify both apps compile**

```bash
cargo check -p deribit-subscription -p deribit-option-monitor 2>&1
```

Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add apps/deribit-subscription/Cargo.toml apps/deribit-subscription/src/main.rs apps/deribit-option-monitor/Cargo.toml apps/deribit-option-monitor/src/main.rs
git commit -m "feat: add OTel init_telemetry to deribit apps

Replace tracing_subscriber::fmt::init with nq_observability::init_telemetry.
Tracing events now flow to OTLP → Loki/Tempo, metrics to Prometheus.
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 9: Generate Grafana dashboard JSON

**Files:**
- Create: `deploy/observability/dashboards/nq-deribit.json`

- [ ] **Step 1: Write the dashboard JSON**

```json
{
  "title": "nq-deribit",
  "tags": ["deribit", "nq-rs"],
  "refresh": "10s",
  "time": { "from": "now-15m", "to": "now" },
  "panels": [
    {
      "id": 1,
      "title": "Deribit Sub Received / sec",
      "type": "timeseries",
      "gridPos": { "x": 0, "y": 0, "w": 12, "h": 8 },
      "targets": [
        {
          "datasource": { "type": "prometheus", "uid": "prometheus" },
          "expr": "rate(deribit_sub_received_total{service_name=~\"deribit-.*\"}[1m])",
          "legendFormat": "{{service_name}}"
        }
      ],
      "fieldConfig": {
        "defaults": { "unit": "cps", "custom": { "drawStyle": "line" } }
      }
    },
    {
      "id": 2,
      "title": "Deribit Sub Enqueued vs Dropped / sec",
      "type": "timeseries",
      "gridPos": { "x": 12, "y": 0, "w": 12, "h": 8 },
      "targets": [
        {
          "datasource": { "type": "prometheus", "uid": "prometheus" },
          "expr": "rate(deribit_sub_enqueued_total{service_name=~\"deribit-.*\"}[1m])",
          "legendFormat": "{{service_name}} - enqueued"
        },
        {
          "datasource": { "type": "prometheus", "uid": "prometheus" },
          "expr": "rate(deribit_sub_dropped_total{service_name=~\"deribit-.*\"}[1m])",
          "legendFormat": "{{service_name}} - dropped"
        }
      ],
      "fieldConfig": {
        "defaults": { "unit": "cps", "custom": { "drawStyle": "line" } }
      }
    },
    {
      "id": 3,
      "title": "MQTT Published vs Failed / sec",
      "type": "timeseries",
      "gridPos": { "x": 0, "y": 8, "w": 12, "h": 8 },
      "targets": [
        {
          "datasource": { "type": "prometheus", "uid": "prometheus" },
          "expr": "rate(mqtt_published_total{service_name=~\"deribit-.*\"}[1m])",
          "legendFormat": "{{service_name}} - published"
        },
        {
          "datasource": { "type": "prometheus", "uid": "prometheus" },
          "expr": "rate(mqtt_publish_failed_total{service_name=~\"deribit-.*\"}[1m])",
          "legendFormat": "{{service_name}} - failed"
        }
      ],
      "fieldConfig": {
        "defaults": { "unit": "cps", "custom": { "drawStyle": "line" } }
      }
    },
    {
      "id": 4,
      "title": "Application Logs",
      "type": "logs",
      "gridPos": { "x": 12, "y": 8, "w": 12, "h": 8 },
      "targets": [
        {
          "datasource": { "type": "loki", "uid": "loki" },
          "expr": "{service_name=~\"deribit-.*\"}"
        }
      ]
    },
    {
      "id": 5,
      "title": "Traces",
      "type": "traces",
      "gridPos": { "x": 0, "y": 16, "w": 24, "h": 8 },
      "targets": [
        {
          "datasource": { "type": "tempo", "uid": "tempo" },
          "queryType": "traceql",
          "query": "{ .service.name =~ \"deribit-.*\" }"
        }
      ]
    }
  ],
  "schemaVersion": 39
}
```

- [ ] **Step 2: Commit**

```bash
mkdir -p deploy/observability/dashboards
git add deploy/observability/dashboards/nq-deribit.json
git commit -m "feat(observability): add Grafana dashboard for nq-deribit"
```

---

### Task 10: Full build + test + self-review

- [ ] **Step 1: Build entire workspace**

```bash
cargo build --workspace 2>&1 | tail -5
```

Expected: `Finished` with no errors.

- [ ] **Step 2: Run nq-deribit unit tests**

```bash
cargo test -p nq-deribit --lib -- --skip test_ws_base_client 2>&1 | grep "test result"
```

Expected: `test result: ok. 38 passed; 0 failed; ...`

- [ ] **Step 3: Verify no remaining AtomicU64 or old metrics references**

```bash
grep -rn "AtomicU64\|MetricsSnapshot\|MetricsRates\|DERIBIT_SUB_RECEIVED\|DERIBIT_SUB_ENQUEUED\|DERIBIT_SUB_DROPPED\|MQTT_PUBLISHED\|MQTT_PUBLISH_FAILED" crates/ apps/ --include="*.rs" 2>/dev/null | grep -v target | grep -v ".git/"
```

Expected: no output (all old references replaced).

- [ ] **Step 4: Verify git status clean**

```bash
git status
git log --oneline -12
```

All changes committed.
