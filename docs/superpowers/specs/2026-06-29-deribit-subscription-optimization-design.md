# Deribit Subscription App Optimization

**Date:** 2026-06-29
**Status:** Approved

## Overview

Complete the migration of `deribit-subscription` app from the deprecated `Client` to the new `ConnectionPool`/`Connection` architecture, implement flexible channel configuration, clean up dependencies, update examples, and add local testing support.

## Channel Configuration

Channel sources, in priority order (highest wins, each fully replaces the default):

1. `DERIBIT_SUBSCRIPTION_CHANNELS_FILE` — path to a file, one channel per line
2. `DERIBIT_SUBSCRIPTION_CHANNELS` — comma-separated channel list
3. Default: `resources/subscription.txt` embedded via `include_str!` (14 channels)

None of the env vars are additive — setting one replaces the default entirely.

## deribit-subscription Changes

### main.rs

- Extract channels using the priority chain above
- Add `DRY_RUN` env var: when `true`, skip MQTT client creation and publishing, log each subscription message at `info!` level instead
- Keep `ConnectionPool` architecture (already migrated in working tree)

### Cargo.toml

Remove 5 unused dependencies left over from the old `Client` architecture:

- `flume` — old subscription channel, replaced by `tokio::broadcast`
- `futures-util` — old WebSocket stream handling, now internal to nq-deribit
- `reqwest-websocket` — old direct WebSocket usage, now internal
- `tokio-tungstenite` — old WebSocket backend, now internal
- `rand` — unused

## Example Updates

### examples/subscription.rs

Migrate from deprecated `Client` to `ConnectionPool`:
- Use `ConnectionConfigBuilder` + `ConnectionPool` + `PoolConfig`
- Subscribe via `pool.subscribe(channels)`
- Receive via `pool.subscribe_to_broadcast()`
- Use raw channel strings (matching the production app pattern)

### examples/subscription_with_auth.rs

Same migration as above, plus:
- Set `client_id` and `client_secret` on `ConnectionConfigBuilder`
- Demonstrate authenticated subscription flow

## nq-deribit Crate Cleanup

- `sub.rs`: Add `#[deprecated]` on `DeribitSubscriptionClient` (only used by deprecated `Client`)
- `api.rs`: Already marked deprecated ✓
- `client.rs`: Already marked deprecated ✓

## Testing

### Compilation

```bash
cargo build -p deribit-subscription
cargo test -p nq-deribit
```

### Local End-to-End (Dry Run)

```bash
DRY_RUN=true \
DERIBIT_SUBSCRIPTION_CHANNELS="ticker.BTC-PERP.agg2" \
  cargo run -p deribit-subscription
```

Expected behavior:
1. Connect to Deribit WebSocket
2. Subscribe to specified channels
3. Log received subscription messages at INFO level instead of publishing to MQTT

### Local End-to-End (Full)

```bash
DERIBIT_SUBSCRIPTION_CHANNELS="ticker.BTC-PERP.agg2" \
  cargo run -p deribit-subscription
```

Requires EMQX accessible at `EMQX_HOST`. Publishes subscription data to `DERIBIT_SUBSCRIPTION_TOPIC` (default: `t/deribit/subscription`).

## Files Changed

| File | Change |
|------|--------|
| `apps/deribit-subscription/src/main.rs` | Channel config, DRY_RUN mode |
| `apps/deribit-subscription/Cargo.toml` | Remove 5 unused deps |
| `crates/nq-deribit/examples/subscription.rs` | Migrate to ConnectionPool |
| `crates/nq-deribit/examples/subscription_with_auth.rs` | Migrate to ConnectionPool |
| `crates/nq-deribit/src/sub.rs` | Add deprecated tag |
