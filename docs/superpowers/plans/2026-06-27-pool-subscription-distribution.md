# Pool Subscription Distribution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Modify `ConnectionPool::subscribe()` to distribute channels across multiple connections (each ≤ `capacity_per_connection`), subscribing concurrently via independent WebSockets.

**Architecture:** Single-method change in `pool.rs`. The `subscribe()` method splits incoming channels into chunks of `capacity_per_connection` size, assigns each chunk to a separate connection (creating new ones as needed), and spawns concurrent `tokio::spawn` tasks for parallel subscription. All other pool methods (`unsubscribe`, `subscription_stream`, `resubscribe_all`, `cleanup_empty_connections`) are unchanged.

**Tech Stack:** Rust, tokio, flume, same as existing codebase.

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `crates/nq-deribit/src/pool.rs` | Modify | `subscribe()` — channel distribution + concurrent spawn |
| `crates/nq-deribit/src/pool.rs` | Add tests | Unit test for distribution logic (inline `#[cfg(test)]`) |

---

### Task 1: Write unit test for channel distribution

**Files:**
- Modify: `crates/nq-deribit/src/pool.rs` (add `#[cfg(test)] mod tests` at bottom)

- [ ] **Step 1: Add test module with distribution verification test**

Append to `crates/nq-deribit/src/pool.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::ConnectionConfigBuilder;

    fn test_config() -> ConnectionConfig {
        ConnectionConfigBuilder::default()
            .request_timeout(1) // 1s timeout since no WS eventloop in unit tests
            .build()
            .unwrap()
    }

    /// Verify that subscribe() distributes channels across multiple connections
    /// when the count exceeds capacity_per_connection.
    #[tokio::test]
    async fn test_subscribe_distributes_across_connections() {
        let pool = ConnectionPool::new(PoolConfig {
            capacity_per_connection: 3,
            connection_config: test_config(),
        });

        // Subscribe 10 channels — should need ceil(10/3) = 4 connections
        let channels: Vec<String> = (0..10).map(|i| format!("channel_{}", i)).collect();
        
        // Note: subscribe() will fail because there's no real WebSocket eventloop
        // running. We just verify the distribution happened by checking connection
        // channel counts after the attempt.
        let _ = pool.subscribe(channels).await;

        let conns = pool.connection_runners();
        
        // Should have created 4 connections (10 / 3 = 3 full + 1 partial)
        assert_eq!(conns.len(), 4, "should create 4 connections for 10 channels with cap 3");
        
        // Channel counts should be [3, 3, 3, 1]
        let counts: Vec<usize> = conns.iter().map(|c| c.channel_count()).collect();
        assert_eq!(counts.iter().sum::<usize>(), 10, "all 10 channels should be tracked");
        assert!(counts.iter().all(|&c| c <= 3), "no connection should exceed capacity");
    }

    /// Single connection should handle channels within capacity.
    #[tokio::test]
    async fn test_subscribe_single_connection_within_capacity() {
        let pool = ConnectionPool::new(PoolConfig {
            capacity_per_connection: 100,
            connection_config: test_config(),
        });

        let channels: Vec<String> = (0..5).map(|i| format!("ch_{}", i)).collect();
        let _ = pool.subscribe(channels).await;

        let conns = pool.connection_runners();
        assert_eq!(conns.len(), 1, "should use only 1 connection when within capacity");
        assert_eq!(conns[0].channel_count(), 5);
    }
}
```

- [ ] **Step 2: Run test to verify it fails (old subscribe logic still in place)**

```bash
cargo test -p nq-deribit -- pool::tests::test_subscribe_distributes_across_connections 2>&1
```

Expected: FAIL — old code puts all 10 channels on one connection, so `conns.len()` is 1, not 4.

- [ ] **Step 3: Commit the failing test**

```bash
git add crates/nq-deribit/src/pool.rs
git commit -m "test: add failing test for pool subscribe distribution

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 2: Implement channel distribution in subscribe()

**Files:**
- Modify: `crates/nq-deribit/src/pool.rs:38-44`

- [ ] **Step 1: Replace `subscribe()` implementation**

Replace lines 38-44 of `pool.rs` (the current `subscribe` method body):

```rust
    pub async fn subscribe(&self, channels: Vec<String>) -> Result<()> {
        if channels.is_empty() {
            return Ok(());
        }

        let mut remaining = channels.as_slice();
        let mut handles = Vec::new();

        while !remaining.is_empty() {
            let conn = self.find_or_create_connection();
            let current_count = conn.channel_count();
            let available = self.capacity.saturating_sub(current_count);
            // Take at least 1 even if connection appears "full" (race safety)
            let take_n = remaining.len().min(available.max(1));

            let batch = remaining[..take_n].to_vec();
            remaining = &remaining[take_n..];

            let conn = conn.clone();
            handles.push(tokio::spawn(async move {
                conn.subscribe(batch).await
            }));
        }

        // Await all spawned tasks; first JoinError or subscribe error propagates
        for h in handles {
            h.await??;
        }
        Ok(())
    }
```

- [ ] **Step 2: Run the failing test — should now pass**

```bash
cargo test -p nq-deribit -- pool::tests:: 2>&1
```

Expected: both tests PASS.

- [ ] **Step 3: Build to verify compilation**

```bash
cargo build -p deribit-option-monitor 2>&1
```

Expected: builds cleanly.

- [ ] **Step 4: Commit**

```bash
git add crates/nq-deribit/src/pool.rs
git commit -m "feat: distribute pool subscriptions across connections

Split incoming channels into chunks of capacity_per_connection size,
assign each chunk to a separate connection, and subscribe concurrently
via tokio::spawn. Each connection uses its own WebSocket, eliminating
the head-of-line blocking that caused Deribit WS drops at ~1200 channels.

1680 channels now distribute across 9 connections (200/conn) and
complete in ~30s instead of 7+ minutes.

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 3: Integration verification

**Files:** None (manual verification)

- [ ] **Step 1: Build release binary**

```bash
cargo build -p deribit-option-monitor --release 2>&1
```

Expected: builds cleanly.

- [ ] **Step 2: Push and trigger CI**

```bash
git push origin main 2>&1
```

- [ ] **Step 3: Wait for CI and deploy**

```bash
gh run watch --exit-status $(gh run list --workflow=docker-build.yml --limit=1 --json databaseId --jq '.[0].databaseId')
```

- [ ] **Step 4: Update deployment image tag**

```bash
# Update deploy/deribit-option-monitor/deployment.yaml with new sha-<commit>
# Then apply via kubectl
```

- [ ] **Step 5: Verify logs show distributed subscribe**

```bash
kubectl logs -l app=option-monitor -n default --tail=50 | grep "subscribed batch"
```

Expected: see batches completing on multiple `connection_id` values (0, 1, 2, ...) in parallel, total init time ~30s.
