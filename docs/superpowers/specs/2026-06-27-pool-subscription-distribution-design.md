# Pool Subscription Distribution Redesign

## Problem

`ConnectionPool::subscribe()` sends ALL channels to a single connection. That connection
issues sequential batch API calls over one WebSocket. Deribit drops the WS after ~1200
channels, causing remaining batches to timeout (60s each). Total init time: ~7 minutes.

## Design

### Core change: `pool.subscribe()` distributes channels across connections

```
subscribe([1680 channels])
  → split into chunks of ≤ capacity_per_connection (200)
  → chunk 1 (187) → connection 0 (existing, 13 already there)
  → chunk 2 (200) → connection 1 (new)
  → chunk 3 (200) → connection 2 (new)
  → ...
  → chunk 9 (93)  → connection 8 (new)

  Each connection subscribes concurrently via tokio::spawn.
  Each connection has ≤ 2 batches (≤ 200 / 100 batch_size).
  All complete in ~30s (slowest connection, not sum).
```

### Algorithm

```rust
pub async fn subscribe(&self, channels: Vec<String>) -> Result<()> {
    let mut remaining = channels.as_slice();
    let mut handles = Vec::new();

    while !remaining.is_empty() {
        let conn = self.find_or_create_connection();
        let available = self.capacity.saturating_sub(conn.channel_count());
        let take_n = remaining.len().min(available.max(1)); // at least 1

        let batch = remaining[..take_n].to_vec();
        remaining = &remaining[take_n..];

        let conn = conn.clone();
        handles.push(tokio::spawn(async move {
            conn.subscribe(batch).await
        }));
    }

    // Await all — first error propagates (other tasks keep running)
    for h in handles {
        h.await??;
    }
    Ok(())
}
```

### Key properties

- **Distribution**: channels split by `capacity_per_connection` (200), fill-first strategy.
  If connection 0 has 13 existing channels, it gets up to 187 new ones.
- **Concurrency**: each connection subscribes via its own WebSocket. No head-of-line
  blocking between connections.
- **Graceful degradation**: if one connection's subscribe fails, the error propagates.
  Channels are already in the tracked set, so reconnect will retry.
- **No wasted time**: `break` on first batch error (already implemented) means a failing
  connection stops immediately.

### Unchanged interfaces

| Method | Behavior |
|--------|----------|
| `unsubscribe(channels)` | Already checks each connection, removes from matching one |
| `subscription_stream()` | Already merges all connections via `select_all` |
| `resubscribe_all()` | Already iterates all connections |
| `cleanup_empty_connections()` | Already removes connections with 0 channels |
| `find_or_create_connection()` | Unchanged — finds first under-capacity or creates new |

### Before/After

| Metric | Before | After |
|--------|--------|-------|
| 1680 channels distribution | 1 conn (1680) + 1 conn (2) | 9 conns, each ≤ 200 |
| Init time | ~7 min (12 batches + 5 timeouts) | ~30s (9 conns × ≤2 batches parallel) |
| Subscribe parallelism | Serial (single WS) | Concurrent (9 WS) |
| Memory per connection | 50K channel buffer | 50K channel buffer (same) |
| Total WS connections | 2 | 9 |

### Risk: more WS connections

9 connections × 50K subscription channel buffer = more memory potential.
Mitigation: the subscription channel is bounded and uses `try_send` (drops on full).
In steady state, the TickerRouter keeps up, so channels don't fill.
