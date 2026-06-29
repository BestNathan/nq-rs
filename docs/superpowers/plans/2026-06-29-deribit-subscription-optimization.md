# Deribit Subscription App Optimization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete migration to ConnectionPool, add flexible channel configuration, enable DRY_RUN local testing, clean up dependencies, and update examples.

**Architecture:** The app already uses `ConnectionPool` (new arch). This plan finishes the migration: removes old `Client` dependencies from Cargo.toml, adds env-driven channel configuration (file > comma-list > default), adds DRY_RUN mode for testing without EMQX, updates two examples, and marks `DeribitSubscriptionClient` deprecated.

**Tech Stack:** Rust 2024 edition, tokio, nq-deribit (ConnectionPool), nq-mqtt, rumqttc

---

### Task 1: Clean deribit-subscription Cargo.toml

**Files:**
- Modify: `apps/deribit-subscription/Cargo.toml`

Remove 5 dependencies that were only needed by the deprecated `Client` architecture.

- [ ] **Step 1: Remove unused dependencies from Cargo.toml**

Replace the `[dependencies]` section. Current:

```toml
[dependencies]
anyhow = {workspace = true}
async-trait = {workspace = true}
flume = {workspace = true}
futures-util = {workspace = true}
nq-app = {workspace = true}
nq-deribit = {workspace = true}
nq-env = {workspace = true}
nq-mqtt = {workspace = true}
rand = "0.9.0"
reqwest = {version = "0.12", default-features = false, features = ["rustls-tls", "json"]}
reqwest-websocket = "0.4.4"
rumqttc = "0.24.0"
serde = {workspace = true}
serde_json = {workspace = true}
tokio = {workspace = true}
tokio-tungstenite = "0.26.1"
tokio-util = {workspace = true}
tracing = {workspace = true}
tracing-subscriber = {workspace = true}
```

Change to:

```toml
[dependencies]
anyhow = {workspace = true}
async-trait = {workspace = true}
nq-app = {workspace = true}
nq-deribit = {workspace = true}
nq-env = {workspace = true}
nq-mqtt = {workspace = true}
rumqttc = "0.24.0"
serde = {workspace = true}
serde_json = {workspace = true}
tokio = {workspace = true}
tokio-util = {workspace = true}
tracing = {workspace = true}
tracing-subscriber = {workspace = true}
```

- [ ] **Step 2: Verify Cargo.toml parses**

```bash
cargo metadata -p deribit-subscription --format-version 1 > /dev/null 2>&1 && echo "OK"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add apps/deribit-subscription/Cargo.toml
git commit -m "chore: remove unused deps from deribit-subscription

Remove flume, futures-util, reqwest-websocket, tokio-tungstenite, rand —
all only needed by the deprecated Client architecture which has been
replaced by ConnectionPool.
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 2: Mark DeribitSubscriptionClient deprecated

**Files:**
- Modify: `crates/nq-deribit/src/sub.rs`

- [ ] **Step 1: Add deprecated attribute to the struct**

Current `sub.rs`:

```rust
use flume::{Receiver, RecvError};
use futures_util::Stream;

#[derive(Clone)]
pub struct DeribitSubscriptionClient {
```

Change to:

```rust
use flume::{Receiver, RecvError};
use futures_util::Stream;

#[deprecated(note = "Use ConnectionPool::subscribe_to_broadcast() instead. DeribitSubscriptionClient is only used by the deprecated Client.")]
#[derive(Clone)]
pub struct DeribitSubscriptionClient {
```

- [ ] **Step 2: Verify it compiles (allow_deprecated should already be on client.rs)**

```bash
cargo check -p nq-deribit 2>&1 | tail -5
```

Expected: no errors (warnings about deprecated usage from client.rs are fine).

- [ ] **Step 3: Commit**

```bash
git add crates/nq-deribit/src/sub.rs
git commit -m "chore: deprecate DeribitSubscriptionClient

Only used by the deprecated Client; users should migrate to
ConnectionPool::subscribe_to_broadcast().
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 3: Update deribit-subscription main.rs — channel config + DRY_RUN

**Files:**
- Modify: `apps/deribit-subscription/src/main.rs`

This is the core task. Replace the current `main()` with channel configuration logic and DRY_RUN support.

- [ ] **Step 1: Add new env var constants and channel resolution logic**

Replace the current constants and `deribit_subscription_topic()` function (lines 13-23) with the expanded set:

```rust
const SUBSCRIPTION: &str = include_str!("../resources/subscription.txt");
const DERIBIT_SUBSCRIPTION_TOPIC: &str = "t/deribit/subscription";
const DERIBIT_SUBSCRIPTION_TOPIC_ENV: &str = "DERIBIT_SUBSCRIPTION_TOPIC";
const DERIBIT_SUBSCRIPTION_CHANNELS_ENV: &str = "DERIBIT_SUBSCRIPTION_CHANNELS";
const DERIBIT_SUBSCRIPTION_CHANNELS_FILE_ENV: &str = "DERIBIT_SUBSCRIPTION_CHANNELS_FILE";
const DERIBIT_API_CLIENT_ID_ENV: &str = "DERIBIT_API_CLIENT_ID";
const DERIBIT_API_CLIENT_SECRET_ENV: &str = "DERIBIT_API_CLIENT_SECRET";
const DRY_RUN_ENV: &str = "DRY_RUN";

fn deribit_subscription_topic() -> String {
    env::var(DERIBIT_SUBSCRIPTION_TOPIC_ENV)
        .unwrap_or(DERIBIT_SUBSCRIPTION_TOPIC.to_string())
}

fn is_dry_run() -> bool {
    env::var(DRY_RUN_ENV)
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false)
}

/// Resolve subscription channels using priority:
/// 1. DERIBIT_SUBSCRIPTION_CHANNELS_FILE (file path, one per line)
/// 2. DERIBIT_SUBSCRIPTION_CHANNELS (comma-separated)
/// 3. Default: resources/subscription.txt
fn resolve_channels() -> Vec<String> {
    if let Ok(path) = env::var(DERIBIT_SUBSCRIPTION_CHANNELS_FILE_ENV) {
        match std::fs::read_to_string(&path) {
            Ok(content) => {
                let channels: Vec<String> = content
                    .lines()
                    .map(|v| v.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                if !channels.is_empty() {
                    info!(path = %path, count = channels.len(), "channels from file");
                    return channels;
                }
                warn!(path = %path, "channels file is empty, falling back");
            }
            Err(e) => {
                warn!(path = %path, error = ?e, "failed to read channels file, falling back");
            }
        }
    }

    if let Ok(list) = env::var(DERIBIT_SUBSCRIPTION_CHANNELS_ENV) {
        let channels: Vec<String> = list
            .split(',')
            .map(|v| v.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if !channels.is_empty() {
            info!(count = channels.len(), "channels from env var");
            return channels;
        }
    }

    let channels: Vec<String> = SUBSCRIPTION
        .lines()
        .map(|v| v.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    info!(count = channels.len(), "channels from default subscription.txt");
    channels
}
```

- [ ] **Step 2: Rewrite the `main()` function to use `resolve_channels()` and DRY_RUN**

Replace the existing `main()` function (lines 79-143) with:

```rust
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let channels = resolve_channels();

    if channels.is_empty() {
        warn!("no deribit subscriptions configured");
        return Ok(());
    }

    let dry_run = is_dry_run();
    if dry_run {
        info!("DRY_RUN mode: MQTT publish disabled, logging subscriptions to stdout");
    }

    info!(
        "deribit subscriptions ({} channels): {}",
        channels.len(),
        channels.join(", ")
    );

    // ── Build ConnectionConfig with optional auth ──────────────────
    let conn_config = ConnectionConfigBuilder::default()
        .client_id(env::var(DERIBIT_API_CLIENT_ID_ENV).ok())
        .client_secret(env::var(DERIBIT_API_CLIENT_SECRET_ENV).ok())
        .build()?;

    // ── Create ConnectionPool ──────────────────────────────────────
    let pool = Arc::new(ConnectionPool::new(PoolConfig {
        capacity_per_connection: 2000,
        connection_config: conn_config,
    }));

    let ct = pool.cancel_token();

    // Spawn connection eventloops — must be running before subscribe
    for conn in pool.connection_runners() {
        let ct = ct.clone();
        tokio::spawn(async move {
            let _ = conn.run(ct).await;
        });
    }

    // Subscribe to all resolved channels
    info!(count = channels.len(), "subscribing to channels");
    pool.subscribe(channels).await?;

    // ── Create MQTT client (or skip in DRY_RUN) ────────────────────
    let (mqtt_client, mqtt_async_client) = if dry_run {
        (None, None)
    } else {
        let mqtt_client = nq_mqtt::client::Client::builder()
            .set_host(nq_env::emqx::host())
            .build();
        let inner = mqtt_client.inner();
        (Some(mqtt_client), Some(inner))
    };

    // ── Run application ────────────────────────────────────────────
    let mut application = Application::new();

    // Register MQTT runner (if not DRY_RUN)
    if let Some(mqtt) = mqtt_client {
        application.add_runner(Arc::new(mqtt));
    }

    application.add_runner(Arc::new(App {
        mqtt_async_client,
        pool,
        dry_run,
    }));

    let canceltoken = CancellationToken::new();
    application.run(canceltoken).await;

    Ok(())
}
```

- [ ] **Step 3: Update the `App` struct and its `Runner` impl for DRY_RUN**

Replace the `App` struct definition and `Runner` impl (lines 25-76) with:

```rust
/// Application runner that forwards Deribit subscription data to MQTT
/// (or logs to stdout in DRY_RUN mode).
struct App {
    mqtt_async_client: Option<AsyncClient>,
    pool: Arc<ConnectionPool>,
    dry_run: bool,
}

#[async_trait]
impl Runner for App {
    async fn run(&self, canceltoken: CancellationToken) -> Result<()> {
        let topic = deribit_subscription_topic();
        let mut sub_rx = self.pool.subscribe_to_broadcast();

        if self.dry_run {
            info!("app is running (DRY_RUN), logging to stdout");
        } else {
            info!("app is running, forwarding to MQTT topic: {}", topic);
        }

        loop {
            select! {
                _ = canceltoken.cancelled() => break,
                result = sub_rx.recv() => {
                    let data = match result {
                        Ok(m) => m,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            info!("broadcast channel closed");
                            break;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!(skipped = n, "broadcast lagged, skipping messages");
                            continue;
                        }
                    };

                    if self.dry_run {
                        // Log a preview (first 200 chars) at INFO level
                        let preview: String = data.chars().take(200).collect();
                        info!(
                            "[dry-run] would publish to {}: {}",
                            topic,
                            if data.len() > 200 {
                                format!("{}…", preview)
                            } else {
                                preview
                            }
                        );
                    } else if let Some(ref mqtt) = self.mqtt_async_client {
                        trace!("recv subscription data: {:?}", data);
                        if let Err(e) = mqtt.publish(
                            topic.clone(),
                            QoS::AtLeastOnce,
                            true,
                            data,
                        ).await {
                            nq_deribit::metrics::MQTT_PUBLISH_FAILED
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            warn!(error = ?e, "mqtt publish failed");
                        } else {
                            nq_deribit::metrics::MQTT_PUBLISHED
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                },
            }
        }

        info!("app done");
        Ok(())
    }
}
```

- [ ] **Step 4: Verify compilation**

```bash
cargo check -p deribit-subscription 2>&1
```

Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add apps/deribit-subscription/src/main.rs
git commit -m "feat: channel config from env, DRY_RUN mode for local testing

- DERIBIT_SUBSCRIPTION_CHANNELS_FILE: file path, one channel per line
- DERIBIT_SUBSCRIPTION_CHANNELS: comma-separated channel list
- Default: resources/subscription.txt (14 channels)
- DRY_RUN=true: logs subscriptions to stdout, skips MQTT entirely
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 4: Update examples/subscription.rs

**Files:**
- Modify: `crates/nq-deribit/examples/subscription.rs`

Rewrite using `ConnectionPool` instead of deprecated `Client`, with raw channel strings.

- [ ] **Step 1: Replace the entire file**

```rust
use std::sync::Arc;

use anyhow::Result;
use nq_app::application::Application;
use nq_deribit::connection::ConnectionConfigBuilder;
use nq_deribit::pool::{ConnectionPool, PoolConfig};
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let channels: Vec<String> = vec![
        "trades.future.BTC.agg2".to_string(),
        "trades.option.BTC.agg2".to_string(),
    ];

    let conn_config = ConnectionConfigBuilder::default().build()?;
    let pool = Arc::new(ConnectionPool::new(PoolConfig {
        capacity_per_connection: 200,
        connection_config: conn_config,
    }));

    let ct = pool.cancel_token();

    // Spawn connection eventloops
    for conn in pool.connection_runners() {
        let ct = ct.clone();
        tokio::spawn(async move {
            let _ = conn.run(ct).await;
        });
    }

    // Subscribe
    pool.subscribe(channels).await?;

    // Receive subscription messages
    let mut sub_rx = pool.subscribe_to_broadcast();
    tokio::spawn(async move {
        loop {
            match sub_rx.recv().await {
                Ok(msg) => info!("{}", msg),
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(skipped = n, "broadcast lagged");
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    let app = Application::new();
    app.run(CancellationToken::new()).await;
    Ok(())
}
```

- [ ] **Step 2: Verify compilation**

```bash
cargo check --example subscription -p nq-deribit 2>&1
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add crates/nq-deribit/examples/subscription.rs
git commit -m "refactor(examples): migrate subscription example to ConnectionPool

Replace deprecated Client with ConnectionPool + raw channel strings.
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 5: Update examples/subscription_with_auth.rs

**Files:**
- Modify: `crates/nq-deribit/examples/subscription_with_auth.rs`

Same migration, plus client_id/client_secret for authenticated subscription.

- [ ] **Step 1: Replace the entire file**

```rust
use std::env;
use std::sync::Arc;

use anyhow::Result;
use nq_app::application::Application;
use nq_deribit::connection::ConnectionConfigBuilder;
use nq_deribit::pool::{ConnectionPool, PoolConfig};
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let channels: Vec<String> = vec![
        "user.orders.BTC-PERPETUAL.raw".to_string(),
        "user.trades.BTC-PERPETUAL.raw".to_string(),
    ];

    let conn_config = ConnectionConfigBuilder::default()
        .client_id(env::var("DERIBIT_API_CLIENT_ID").ok())
        .client_secret(env::var("DERIBIT_API_CLIENT_SECRET").ok())
        .build()?;
    let pool = Arc::new(ConnectionPool::new(PoolConfig {
        capacity_per_connection: 200,
        connection_config: conn_config,
    }));

    let ct = pool.cancel_token();

    // Spawn connection eventloops
    for conn in pool.connection_runners() {
        let ct = ct.clone();
        tokio::spawn(async move {
            let _ = conn.run(ct).await;
        });
    }

    // Subscribe (private channels require auth)
    pool.subscribe(channels).await?;

    // Receive subscription messages
    let mut sub_rx = pool.subscribe_to_broadcast();
    tokio::spawn(async move {
        loop {
            match sub_rx.recv().await {
                Ok(msg) => info!("{}", msg),
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(skipped = n, "broadcast lagged");
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    let app = Application::new();
    app.run(CancellationToken::new()).await;
    Ok(())
}
```

- [ ] **Step 2: Verify compilation**

```bash
cargo check --example subscription_with_auth -p nq-deribit 2>&1
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add crates/nq-deribit/examples/subscription_with_auth.rs
git commit -m "refactor(examples): migrate auth subscription example to ConnectionPool

Replace deprecated Client with ConnectionPool + auth credentials.
Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 6: Full compilation check & unit tests

- [ ] **Step 1: Build the subscription app**

```bash
cargo build -p deribit-subscription 2>&1
```

Expected: `Finished` with no errors.

- [ ] **Step 2: Run nq-deribit unit tests**

```bash
cargo test -p nq-deribit 2>&1
```

Expected: all tests pass (including `protocol.rs` and `transport.rs` tests).

- [ ] **Step 3: Run subscription app in DRY_RUN mode against testnet**

```bash
# Run for ~15 seconds then kill. On macOS, use perl for timeout.
# The app will run until killed; look for the key log lines.
DRY_RUN=true \
DERIBIT_WS_URL="wss://test.deribit.com/ws/api/v2" \
DERIBIT_SUBSCRIPTION_CHANNELS="ticker.BTC-PERP.agg2" \
  cargo run -p deribit-subscription 2>&1 &
PID=$!
sleep 15
kill $PID 2>/dev/null
wait $PID 2>/dev/null
```

Expected output should include:
- `DRY_RUN mode: MQTT publish disabled`
- `subscribing to channels, count=1`
- `transport connected`
- `heartbeat set`
- `subscribed batch, progress=1/1`
- `[dry-run] would publish to t/deribit/subscription: {"method":"subscription"...`

- [ ] **Step 4: Commit all test results (if any changes)**

```bash
# No changes expected unless test reveals issues
git status
```

---

### Task 7: Final verification

- [ ] **Step 1: Check all changes are committed**

```bash
git status
git log --oneline -6
```

Expected: 5-6 commits on top of the base, no uncommitted changes.

- [ ] **Step 2: Full workspace check**

```bash
cargo check --workspace 2>&1 | tail -5
```

Expected: `Finished` with no errors across entire workspace.
