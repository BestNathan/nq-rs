# nq-rs OpenTelemetry Observability

**Date:** 2026-06-29
**Status:** Approved

## Overview

Add full-stack OpenTelemetry observability to the nq-rs project using the existing cluster observability stack (OTel Collector → Prometheus/Loki/Tempo → Grafana). Extract infrastructure into a new `nq-observability` crate, migrate existing `AtomicU64` counters to OTel instruments, bridge `tracing` to OTel, and provide a Grafana dashboard.

## Cluster Context

Existing `observability` namespace deployed in cluster:

| Component | Port | Role |
|-----------|------|------|
| OTel Collector | 4317(gRPC), 4318(HTTP), 8889(Prometheus exporter) | OTLP ingestion hub |
| Prometheus | 9090 | Metrics storage (scrapes Collector:8889) |
| Loki | 3100 | Log aggregation |
| Tempo | 3200, 4317 | Distributed tracing |
| Grafana | 3000 (NodePort 31149) | Unified visualization |

Data pipeline: `App → OTLP gRPC (4317) → Collector → Prometheus/Loki/Tempo → Grafana`

## Architecture

```
┌─ App (deribit-subscription / option-monitor) ───┐
│  nq_observability::init_telemetry("service")     │
│  ├─ tracing → OTel LoggerProvider (logs)         │
│  ├─ tracing → OTel TracerProvider (traces)       │
│  └─ OTel MeterProvider → Counters (metrics)      │
│                    │                              │
│            OTLP gRPC (collector:4317)             │
└────────────────────┼──────────────────────────────┘
                     ▼
┌─ observability namespace ────────────────────────┐
│  OTel Collector → Prometheus / Loki / Tempo      │
│  Grafana → unified dashboards                     │
└──────────────────────────────────────────────────┘
```

## Crate Separation

### nq-observability (new)

Infrastructure only — OTel SDK setup, no business metrics.

**Files:**
- `crates/nq-observability/Cargo.toml`
- `crates/nq-observability/src/lib.rs` — `init_telemetry(service_name) -> OTelGuard`
- `crates/nq-observability/src/metrics.rs` — `fn meter(scope) -> Meter`, re-export OTel types
- `crates/nq-observability/src/telemetry.rs` — LoggerProvider, TracerProvider, MeterProvider init

**Dependencies:** `opentelemetry`, `opentelemetry_sdk`, `opentelemetry-otlp` (tonic, gRPC), `opentelemetry-appender-tracing`, `tracing-opentelemetry`, `tracing-subscriber`, `tokio`

**Public API:**
```rust
pub fn init_telemetry(service_name: &str) -> Result<OTelGuard>
pub fn meter(scope: &str) -> opentelemetry::metrics::Meter
pub use opentelemetry::metrics::{Counter, Histogram, UpDownCounter, Meter};
pub struct OTelGuard; // drop → flush + shutdown
```

### nq-deribit (modified)

Business metrics stay here — replace `AtomicU64` with OTel counters.

**Files:**
- `crates/nq-deribit/Cargo.toml` — add `nq-observability` dep
- `crates/nq-deribit/src/metrics.rs` — replace `pub static AtomicU64` with `Lazy<DeribitMetrics>` using OTel Counters

**Migrated counters:**
| Old (AtomicU64) | New (OTel Counter) |
|---|---|
| `DERIBIT_SUB_RECEIVED` | `deribit.sub.received` |
| `DERIBIT_SUB_ENQUEUED` | `deribit.sub.enqueued` |
| `DERIBIT_SUB_DROPPED` | `deribit.sub.dropped` |
| `MQTT_PUBLISHED` | `mqtt.published` |
| `MQTT_PUBLISH_FAILED` | `mqtt.publish.failed` |

- Remove `MetricsSnapshot` and `MetricsRates` structs (no longer needed — Prometheus handles rate computation).

### App changes

Each app calls `init_telemetry()` at startup:

**Files:**
- `apps/deribit-subscription/src/main.rs` — add `let _guard = nq_observability::init_telemetry("deribit-subscription")?;`
- `apps/deribit-subscription/Cargo.toml` — add `nq-observability` dep
- `apps/deribit-option-monitor/src/main.rs` — add `let _guard = nq_observability::init_telemetry("deribit-option-monitor")?;`
- `apps/deribit-option-monitor/Cargo.toml` — add `nq-observability` dep

## Telemetry Configuration

Environment variables (all optional, with defaults):

| Variable | Default | Description |
|----------|---------|-------------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://otel-collector.observability:4317` | OTLP gRPC endpoint |
| `RUST_LOG` | `info` | Log level (tracing native, unchanged) |

Service name is set programmatically by `init_telemetry(service_name)`.

## Grafana Dashboard

Delivered as a standalone JSON file, **not** auto-managed by ArgoCD. Manually import into Grafana.

**File:** `deploy/observability/dashboards/nq-deribit.json`

**Panels:**

| Panel | Data Source | Query |
|-------|-------------|-------|
| Deribit Sub Received Rate | Prometheus | `rate(deribit_sub_received_total[1m])` |
| Deribit Sub Enqueued Rate | Prometheus | `rate(deribit_sub_enqueued_total[1m])` |
| Deribit Sub Dropped Rate | Prometheus | `rate(deribit_sub_dropped_total[1m])` |
| MQTT Published Rate | Prometheus | `rate(mqtt_published_total[1m])` |
| MQTT Publish Failed Rate | Prometheus | `rate(mqtt_publish_failed_total[1m])` |
| Logs | Loki | `{service_name=~"deribit-.*"}` |
| Traces | Tempo | (linked from logs via TraceID) |

## Files Changed

| File | Change |
|------|--------|
| `crates/nq-observability/Cargo.toml` | Create |
| `crates/nq-observability/src/lib.rs` | Create |
| `crates/nq-observability/src/metrics.rs` | Create |
| `crates/nq-observability/src/telemetry.rs` | Create |
| `crates/nq-deribit/Cargo.toml` | Add dep |
| `crates/nq-deribit/src/metrics.rs` | AtomicU64 → OTel Counters |
| `crates/nq-deribit/src/connection.rs` | Update counter calls |
| `crates/nq-deribit/src/protocol.rs` | Update counter calls |
| `apps/deribit-subscription/Cargo.toml` | Add dep |
| `apps/deribit-subscription/src/main.rs` | Add init_telemetry |
| `apps/deribit-option-monitor/Cargo.toml` | Add dep |
| `apps/deribit-option-monitor/src/main.rs` | Add init_telemetry |
| `deploy/observability/dashboards/nq-deribit.json` | Create |

## Testing

- `cargo build --workspace` — compilation
- `cargo test -p nq-deribit` — verify counter migration doesn't break tests (pool, protocol tests use metrics)
- Manual: deploy to cluster, verify metrics appear in Prometheus, logs in Loki, traces in Tempo

## Scope Notes

- Dashboard JSON is generated for manual import, not ArgoCD-managed
- `MetricsSnapshot` / `MetricsRates` are removed (replaced by Prometheus rate queries)
- Existing `tracing` macros (`info!`, `warn!`, `debug!`, `trace!`) unchanged — bridge handles conversion
- No K8s deployment file changes needed (default OTLP endpoint matches cluster setup)
