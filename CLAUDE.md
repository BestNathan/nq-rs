# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Structure

```
nq-rs/                          # Rust workspace (resolver = "2")
├── crates/
│   ├── nq-deribit/             # Deribit WS client: connection pool, JSON-RPC, subscriptions, transport, protocol
│   ├── nq-app/                 # Application runner framework (spawn/graceful-shutdown/TaskTracker)
│   ├── nq-mqtt/                # MQTT client wrapper (rumqttc)
│   ├── nq-env/                 # Environment config helpers (Deribit, EMQX)
│   ├── nq-observability/       # OpenTelemetry init, tracing-subscriber, tokio metrics
│   └── nq-macro/               # proc-macro: `#[derive(ChannelSerialize)]`
├── apps/
│   ├── deribit-subscription/   # Bin: subscribe to Deribit channels → forward to MQTT
│   └── deribit-option-monitor/ # Bin: poll options, subscribe tickers, route to MQTT
├── fluvio/                     # Fluvio streaming (SDF, smartmodules, connectors)
├── deploy/                     # Kubernetes/ArgoCD deployment manifests
├── Dockerfile                  # Multi-stage musl build (cargo-chef → release → alpine)
└── Makefile                    # Docker build+run shortcuts for deribit-* services
```

**Architecture layers** (nq-deribit, bottom-up):
1. `transport` — raw WebSocket (reqwest-websocket), no protocol knowledge
2. `protocol` — JSON-RPC framing, heartbeat, auth setup, subscription lifecycle
3. `connection` — per-endpoint eventloop: `select!` over send/recv/responser channels
4. `pool` — ConnectionPool: round-robin dispatch, broadcast fan-out, dynamic subscribe/unsubscribe

**Key patterns**: `#[async_trait]` on Transport/JsonRpcCaller/Runner traits; `flume` channels for internal messaging; `tokio::sync::broadcast` for subscription fan-out; `ConnectionPool::subscribe_to_broadcast()` replaces the deprecated `DeribitSubscriptionClient`.

## Build / Lint / Test

```bash
# Build all workspace members
cargo build --workspace

# Build a specific app (musl target for Docker parity)
cargo build --release --target x86_64-unknown-linux-musl --bin deribit-option-monitor

# Run all tests
cargo test --workspace

# Run tests for a specific crate
cargo test -p nq-deribit

# Run clippy (lint groups: all + pedantic + nursery + cargo via workspace.lints)
cargo clippy --workspace --no-deps

# Auto-fix clippy warnings where supported
cargo clippy --fix --workspace --no-deps

# Format code (max_width=100, StdExternalCrate import ordering)
cargo fmt --all

# Check formatting without modifying
cargo fmt --check
```

## Code Quality Gates (CI-equivalent)

All checks MUST pass before committing:

```bash
cargo check --workspace          # -D warnings: all warnings = errors
cargo clippy --workspace --no-deps
cargo fmt --check
cargo test --workspace
```

### Lint Configuration

| File | Purpose |
|------|---------|
| `.cargo/config.toml` | `-D warnings` (hard gate) |
| `clippy.toml` | `cognitive-complexity-threshold=15`, `too-many-arguments-threshold=4`, no unwrap/expect in tests |
| `rustfmt.toml` | `max_width=100`, `reorder_imports=true` |
| `Cargo.toml` → `[workspace.lints]` | 30+ deny/warn rules inherited by all crates |

### Key deny rules (no escape)

- `unsafe_code`, `unwrap_used`, `expect_used`, `panic`, `indexing_slicing`
- `float_arithmetic`, `todo`, `unimplemented`, `dbg_macro`, `print_stdout`, `print_stderr`
- `cast_possible_truncation`, `cast_possible_wrap`, `cast_precision_loss`
- `clone_on_ref_ptr`, `implicit_clone`, `redundant_clone`

`#[allow(clippy::*)]` is **forbidden** — fix the root cause instead. Other `#[allow]` (e.g. `dead_code`, `deprecated`) is permitted when justified.

### Function argument limit

`too-many-arguments-threshold = 4`. When a function needs ≥5 parameters, group related fields into a config struct (see `ProtocolConfig`, `SubscriptionConfig` as examples).
