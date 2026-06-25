# Deribit Option Monitor — Design Spec

**Date**: 2026-06-25
**Branch**: `feat/deribit`
**Status**: Approved

## Overview

A new application (`deribit-option-monitor`) that monitors Deribit options in real-time:

1. On startup, fetches all currently active options via `public/get_instruments`
2. Subscribes to `ticker.{instrument_name}.agg2` for each option
3. Monitors for new option creation via `instrument_state.option.{currency}` subscription + periodic polling fallback
4. When a new option is discovered, dynamically subscribes to its ticker
5. Publishes ticker data to MQTT, one topic per instrument: `t/deribit/option_ticker/{instrument_name}`

Supports potentially hundreds of simultaneous options (BTC + ETH) via a multi-connection pool.

## Configuration

| Parameter | Env Var | Default | Description |
|-----------|---------|---------|-------------|
| `currencies` | `DERIBIT_OPTION_CURRENCIES` | `BTC,ETH` | Comma-separated currency list |
| `ticker_interval` | `DERIBIT_OPTION_TICKER_INTERVAL` | `agg2` | Ticker aggregation interval (`100ms`, `agg2`, `raw`) |
| `mqtt_topic_prefix` | `DERIBIT_OPTION_MQTT_TOPIC_PREFIX` | `t/deribit/option_ticker` | MQTT topic prefix |
| `poll_interval_secs` | `DERIBIT_OPTION_POLL_INTERVAL` | `60` | Fallback polling interval in seconds |
| `pool_capacity` | `DERIBIT_OPTION_POOL_CAPACITY` | `200` | Max channels per WS connection |

Standard env vars also apply: `EMQX_HOST` (MQTT broker), `DERIBIT_WS_URL` (WS endpoint), `DERIBIT_API_CLIENT_ID`/`DERIBIT_API_CLIENT_SECRET` (optional auth).

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   deribit-option-monitor                 │
│                                                         │
│  ┌─────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │ Instrument  │  │  Subscription│  │    Ticker     │  │
│  │  Fetcher    │  │   Manager    │  │   Router      │  │
│  │             │  │              │  │               │  │
│  │ - startup   │  │ - tracks     │  │ - parses      │  │
│  │   get_inst  │  │   option set │  │   ticker msgs │  │
│  │ - periodic  │  │ - triggers   │  │ - publishes   │  │
│  │   poll      │  │   subscribe  │  │   per-instr   │  │
│  └──────┬──────┘  └──────┬───────┘  └───────┬───────┘  │
│         │                │                   │          │
│         │         ┌──────▼───────────────────▼───────┐  │
│         │         │       ConnectionPool              │  │
│         │         │                                   │  │
│         │         │  ┌──────────┐  ┌──────────┐       │  │
│         └─────────┤  │Connection│  │Connection│ ...   │  │
│       (inst_state)│  │ (WS #1)  │  │ (WS #2)  │       │  │
│                   │  │ ch 0..N  │  │ ch N+1.. │       │  │
│                   │  └──────────┘  └──────────┘       │  │
│                   └───────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
                    ┌───────────┐
                    │   MQTT    │
                    │  Broker   │
                    └───────────┘
```

### Components

1. **ConnectionPool** (nq-deribit crate): Manages multiple WS connections, distributes channels by capacity, auto-creates new connections when existing ones are full.

2. **Connection** (nq-deribit crate): Evolved version of existing `Client` with dynamic channel management. Maintains a live `HashSet<String>` of subscribed channels; reconnect automatically re-subscribes all channels from this set.

3. **InstrumentFetcher** (app): Calls `public/get_instruments(kind=option, expired=false)` for each configured currency. Returns `Vec<Instrument>`.

4. **SubscriptionManager** (app): Orchestrates the lifecycle:
   - On startup: calls `InstrumentFetcher` → subscribes ticker for all returned options
   - Subscribes to `instrument_state.option.{currency}` on pool
   - Runs poll loop (every `poll_interval_secs`) as fallback
   - Processes `instrument_state` messages to detect new options
   - Tracks all known options in a `HashSet<String>` to avoid duplicate subscriptions

5. **TickerRouter** (app): Consumes from pool's merged subscription stream. For each message, attempts to parse as `Ticker`. If successful, publishes to MQTT topic `{prefix}/{instrument_name}`. Non-ticker messages (instrument_state, etc.) are silently skipped.

## nq-deribit Crate Changes

### New: `Connection` (`crates/nq-deribit/src/connection.rs`)

```rust
pub struct Connection {
    id: usize,
    channels: Arc<RwLock<HashSet<String>>>,
    cmd_tx: Sender<ConnectionCommand>,
    subscription_rx: Receiver<String>,
    config: Arc<Config>,
    token: Arc<RwLock<Option<String>>>,
    message_tx: Sender<String>,
    message_rx: Receiver<String>,
    responser_tx: Sender<(i64, oneshot::Sender<String>)>,
    responser_rx: Receiver<(i64, oneshot::Sender<String>)>,
}

enum ConnectionCommand {
    Subscribe { channels: Vec<String> },
    Unsubscribe { channels: Vec<String> },
}
```

Key behaviors:
- `subscribe(channels)` — sends command to eventloop → calls `public/subscribe` via api_client → adds to `channels` set
- `unsubscribe(channels)` — sends command → calls `public/unsubscribe` → removes from `channels` set
- On reconnect — reads from `channels` set → re-subscribes all
- `channel_count()` — returns current subscribed channel count (used by Pool)
- Implements `Runner` (same eventloop pattern as existing `Client`)

### New: `ConnectionPool` (`crates/nq-deribit/src/pool.rs`)

```rust
pub struct ConnectionPool {
    connections: Vec<Arc<Connection>>,
    capacity: usize,
    next_index: AtomicUsize,
    base_config: Config,
}

impl ConnectionPool {
    pub fn new(config: PoolConfig) -> Self;
    pub async fn subscribe(&self, channels: Vec<String>) -> Result<()>;
    pub async fn unsubscribe(&self, channels: Vec<String>) -> Result<()>;
    pub fn subscription_stream(&self) -> impl Stream<Item = String>;
    pub fn api_client(&self) -> DeribitApiClient;
    pub fn connections(&self) -> &[Arc<Connection>];
}
```

Allocation strategy: Round-robin with capacity check. When subscribing, find the first connection with `channel_count < capacity`. If none available, create a new `Connection` and add it to the pool.

### New: `GetInstrumentsRequest` (`crates/nq-deribit/src/request/market_data.rs`)

```rust
impl_request!(GetInstrumentsRequest, GetInstrumentsResponse, "public/get_instruments");

pub struct GetInstrumentsRequest {
    pub currency: Currency,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<InstrumentKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expired: Option<bool>,
}

pub type GetInstrumentsResponse = Vec<InstrumentInfo>;
```

### New: `InstrumentInfo` model (`crates/nq-deribit/src/model/instrument.rs`)

Fields needed (subset of Deribit's full instrument response):
- `instrument_name: String`
- `kind: InstrumentKind`
- `base_currency: Currency`
- `is_active: bool`
- `expiration_timestamp: u64`
- `strike: Option<f64>` (options only)
- `option_type: Option<String>` (options only: "call"/"put")
- `state: String` (book state)

### New: `InstrumentStateChannel` (`crates/nq-deribit/src/subscription/instrument_state.rs`)

```rust
gen_channel!(InstrumentStateChannel, "instrument_state", InstrumentKind, Currency);
// Display impl: "instrument_state.{kind}.{currency}"
```

### New: `TickerChannel` (`crates/nq-deribit/src/subscription/ticker.rs`)

```rust
gen_channel!(TickerChannel, "ticker", String, Interval);
// Display impl: "ticker.{instrument_name}.{interval}"
```

The `TickerSubscription` wrapper for parsing incoming messages:
```rust
#[derive(Deserialize)]
struct TickerSubscription {
    method: String,        // "subscription"
    params: TickerParams,
}
#[derive(Deserialize)]
struct TickerParams {
    channel: String,       // "ticker.{inst}.agg2"
    data: Ticker,          // reuses existing model::ticker::Ticker
}
```

### Backward Compatibility

The existing `Client` and `ConfigBuilder` are **not modified**. `Connection` and `ConnectionPool` are additive. The existing `deribit-subscription` app continues to work unchanged.

## App Structure

```
apps/deribit-option-monitor/
├── Cargo.toml
├── src/
│   ├── main.rs              # Entry point: assemble components, start Application
│   ├── config.rs            # AppConfig with env var loading
│   ├── fetcher.rs           # InstrumentFetcher
│   ├── subscription_mgr.rs  # SubscriptionManager (Runner)
│   └── ticker_router.rs     # TickerRouter (Runner)
```

### SubscriptionManager as Runner

Runs two concurrent tasks (spawned via `tokio::spawn` inside `run()`):
1. **Poll loop** — every `poll_interval_secs`, calls `get_instruments` and subscribes new options
2. **Instrument state loop** — clones the pool's `subscription_rx`, filters messages where `params.channel` starts with `"instrument_state."`, extracts `data.instrument_name` from the parsed JSON, checks against `tracked_options`, and calls `subscribe_new_options` for any untracked names

Both loops share `tracked_options: Arc<RwLock<HashSet<String>>>` to prevent duplicate subscriptions. The `subscribe_new_options` method is idempotent — calling with already-tracked names is a no-op.

### TickerRouter as Runner

Consumes from `pool.subscription_stream()`, attempts `serde_json::from_str::<TickerSubscription>(msg)`. On success, publishes JSON to `mqtt_client.publish("{prefix}/{instrument_name}")`. On parse failure, silently skips (message is likely instrument_state or heartbeat).

## Error Handling

| Scenario | Behavior |
|----------|----------|
| WS disconnect | Connection auto-reconnects, re-subscribes from `channels` set |
| `get_instruments` failure | Log warning + wait for next poll retry |
| `public/subscribe` failure for a channel | Log warning + skip that channel, continue with others |
| Pool capacity exceeded | Auto-create new Connection |
| MQTT publish failure | Log warning + continue processing |
| Ticker parse failure | Silently skip (non-ticker message) |

## Testing

| Type | Scope |
|------|-------|
| Unit | `GetInstrumentsRequest` serialization, `TickerChannel`/`InstrumentStateChannel` display, Ticker parsing, SubscriptionManager dedup logic |
| Integration | Connect to Deribit testnet (`test.deribit.com`), fetch options, subscribe ticker, verify data received |
| Manual | Start app, check log for subscribed option count, subscribe MQTT topic, verify data flow |
