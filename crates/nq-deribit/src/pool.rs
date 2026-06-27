use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use anyhow::Result;
use nq_app::runner::Runner;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::connection::{Connection, ConnectionConfig};

pub struct ConnectionPool {
    connections: Arc<RwLock<Vec<Arc<Connection>>>>,
    capacity: usize,
    next_id: AtomicUsize,
    base_config: ConnectionConfig,
    cancel_token: CancellationToken,
    broadcast_tx: tokio::sync::broadcast::Sender<String>,
    /// Serializes subscribe() calls to prevent concurrent distribution races.
    subscribe_mutex: tokio::sync::Mutex<()>,
}

pub struct PoolConfig {
    pub capacity_per_connection: usize,
    pub connection_config: ConnectionConfig,
}

impl ConnectionPool {
    pub fn new(config: PoolConfig) -> Self {
        let cancel_token = CancellationToken::new();
        let (broadcast_tx, _) = tokio::sync::broadcast::channel(50000);
        let first = Arc::new(Connection::new(0, config.connection_config.clone()));
        first.set_broadcast_tx(broadcast_tx.clone());
        Self {
            connections: Arc::new(RwLock::new(vec![first])),
            capacity: config.capacity_per_connection,
            next_id: AtomicUsize::new(1),
            base_config: config.connection_config,
            cancel_token,
            broadcast_tx,
            subscribe_mutex: tokio::sync::Mutex::new(()),
        }
    }

    /// Subscribe channels across connections, respecting capacity_per_connection.
    /// Uses a mutex to prevent concurrent subscribe() calls from racing on
    /// connection allocation, and a local planned-counts map to guarantee that
    /// no single connection exceeds capacity regardless of task scheduling.
    pub async fn subscribe(&self, channels: Vec<String>) -> Result<()> {
        if channels.is_empty() {
            return Ok(());
        }

        // Serialize subscribe calls: concurrent calls racing on channel_count()
        // can cause the first-fit loop to overload a single connection.
        let _guard = self.subscribe_mutex.lock().await;

        // Phase 1: Distribute channels across connections deterministically
        // using a local planned-counts map so the allocation is independent of
        // async task scheduling and HashSet state visibility.
        let mut planned: HashMap<usize, usize> = HashMap::new(); // connection_id → planned_count
        let mut assignments: Vec<(Arc<Connection>, Vec<String>)> = Vec::new();
        let mut remaining = channels.as_slice();

        while !remaining.is_empty() {
            // Find or create a connection with spare planned capacity
            let conn = {
                let conns = self.connections.read().unwrap();
                let found = conns.iter().find(|c| {
                    let planned = planned.get(&c.id()).copied().unwrap_or(0);
                    let actual = c.channel_count();
                    // Use the larger of planned and actual to avoid overallocation
                    let effective = planned.max(actual);
                    effective < self.capacity
                });
                match found {
                    Some(c) => c.clone(),
                    None => {
                        drop(conns);
                        self.create_connection()
                    }
                }
            };

            let conn_id = conn.id();
            let planned_count = planned.get(&conn_id).copied().unwrap_or(0);
            let actual_count = conn.channel_count();
            let effective = planned_count.max(actual_count);
            let available = self.capacity.saturating_sub(effective);

            if available == 0 {
                // This shouldn't happen since we checked effective < capacity,
                // but handle defensively: create a new connection instead.
                warn!(
                    connection_id = conn_id,
                    planned = planned_count,
                    actual = actual_count,
                    capacity = self.capacity,
                    "unexpected: connection has no spare capacity despite check; creating new"
                );
                drop(conn);
                let conn = self.create_connection();
                let conn_id = conn.id();
                let available = self.capacity;
                let take_n = remaining.len().min(available);
                let batch = remaining[..take_n].to_vec();
                remaining = &remaining[take_n..];
                *planned.entry(conn_id).or_insert(0) += take_n;
                conn.pre_track_channels(&batch);
                assignments.push((conn, batch));
                continue;
            }

            let take_n = remaining.len().min(available);
            let batch = remaining[..take_n].to_vec();
            remaining = &remaining[take_n..];

            // Track planned count locally BEFORE pre-tracking, so the next
            // iteration's find() sees the planned allocation regardless of
            // whether the HashSet write is visible yet.
            *planned.entry(conn_id).or_insert(0) += take_n;

            // Pre-track synchronously for reconnect resilience
            conn.pre_track_channels(&batch);

            assignments.push((conn, batch));
        }

        info!(
            total_channels = channels.len(),
            connections_used = planned.len(),
            distribution = ?planned.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>(),
            "pool subscribe distribution planned"
        );

        // Phase 2: Spawn subscribe tasks for all assignments
        let mut handles = Vec::new();
        for (conn, batch) in assignments {
            handles.push(tokio::spawn(async move {
                conn.subscribe(batch).await
            }));
        }

        // Phase 3: Await all spawned tasks; first error propagates
        for h in handles {
            h.await??;
        }
        Ok(())
    }

    /// Re-subscribe all tracked channels on all connections.
    /// Call this periodically to recover from WS reconnects.
    pub async fn resubscribe_all(&self) -> Result<()> {
        let conns = self.connections.read().unwrap().clone();
        for conn in &conns {
            conn.resubscribe_all().await?;
        }
        Ok(())
    }

    pub async fn unsubscribe(&self, channels: Vec<String>) -> Result<()> {
        if channels.is_empty() {
            return Ok(());
        }
        // Route unsubscribe to the connection(s) that actually have these channels
        let conns = self.connections.read().unwrap().clone();
        for channel in &channels {
            for conn in &conns {
                if conn.subscribed_channels().contains(channel) {
                    conn.unsubscribe(vec![channel.clone()]).await?;
                    break; // Found the connection, move to next channel
                }
            }
        }
        Ok(())
    }

    /// Subscribe to the pool-level broadcast channel for subscription messages.
    /// Each caller gets its own receiver and sees all messages independently.
    pub fn subscribe_to_broadcast(&self) -> tokio::sync::broadcast::Receiver<String> {
        self.broadcast_tx.subscribe()
    }

    /// Returns a snapshot of all connections. Call once at startup if you want
    /// to manage Runner lifecycle externally. If you don't call this, the pool
    /// auto-spawns eventloops for all connections.
    pub fn connection_runners(&self) -> Vec<Arc<Connection>> {
        self.connections.read().unwrap().clone()
    }

    pub fn first_connection(&self) -> Arc<Connection> {
        self.connections.read().unwrap()[0].clone()
    }

    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    /// Remove empty connections from the pool (those with zero channels).
    /// Keeps at least one connection alive.
    pub fn cleanup_empty_connections(&self) {
        let mut conns = self.connections.write().unwrap();
        if conns.len() <= 1 {
            return;
        }
        let before = conns.len();
        conns.retain(|c| c.channel_count() > 0);
        let removed = before - conns.len();
        if removed > 0 {
            tracing::info!(removed, remaining = conns.len(), "cleaned up empty connections");
        }
    }

    pub fn connection_count(&self) -> usize {
        self.connections.read().unwrap().len()
    }

    fn create_connection(&self) -> Arc<Connection> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let conn = Arc::new(Connection::new(id, self.base_config.clone()));
        conn.set_broadcast_tx(self.broadcast_tx.clone());
        {
            let mut conns = self.connections.write().unwrap();
            conns.push(conn.clone());
        }
        info!(connection_id = id, "pool created and spawning new connection");

        // Auto-spawn eventloop for the new connection
        let ct = self.cancel_token.clone();
        let conn_clone = conn.clone();
        tokio::spawn(async move {
            let _ = conn_clone.run(ct).await;
        });

        conn
    }
}

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

    fn channels(n: usize) -> Vec<String> {
        channels_from(0, n)
    }

    fn channels_from(start: usize, count: usize) -> Vec<String> {
        (start..start + count).map(|i| format!("ch_{}", i)).collect()
    }

    // ─── Basic distribution ────────────────────────────────────────

    /// Empty channel list should return immediately, no connections created.
    #[tokio::test]
    async fn test_subscribe_empty() {
        let pool = ConnectionPool::new(PoolConfig {
            capacity_per_connection: 100,
            connection_config: test_config(),
        });

        pool.subscribe(Vec::new()).await.unwrap();

        let conns = pool.connection_runners();
        assert_eq!(conns.len(), 1, "empty subscribe should not create connections");
        assert_eq!(conns[0].channel_count(), 0);
    }

    /// Single connection handles channels within capacity.
    #[tokio::test]
    async fn test_subscribe_within_capacity() {
        let pool = ConnectionPool::new(PoolConfig {
            capacity_per_connection: 100,
            connection_config: test_config(),
        });

        let _ = pool.subscribe(channels(50)).await;

        let conns = pool.connection_runners();
        assert_eq!(conns.len(), 1);
        assert_eq!(conns[0].channel_count(), 50);
    }

    /// Exactly at capacity: N channels, cap=N → 1 connection.
    #[tokio::test]
    async fn test_subscribe_exact_capacity() {
        let pool = ConnectionPool::new(PoolConfig {
            capacity_per_connection: 200,
            connection_config: test_config(),
        });

        let _ = pool.subscribe(channels(200)).await;

        let conns = pool.connection_runners();
        assert_eq!(conns.len(), 1, "exact capacity should fit in 1 connection");
        assert_eq!(conns[0].channel_count(), 200);
    }

    /// Capacity + 1 → spills into second connection.
    #[tokio::test]
    async fn test_subscribe_capacity_plus_one() {
        let pool = ConnectionPool::new(PoolConfig {
            capacity_per_connection: 200,
            connection_config: test_config(),
        });

        let _ = pool.subscribe(channels(201)).await;

        let conns = pool.connection_runners();
        assert_eq!(conns.len(), 2, "201 channels with cap=200 needs 2 connections");
        let total: usize = conns.iter().map(|c| c.channel_count()).sum();
        assert_eq!(total, 201);
        assert!(conns.iter().all(|c| c.channel_count() <= 200));
    }

    /// Small capacity distribution: 10 channels, cap=3 → 4 connections [3,3,3,1].
    #[tokio::test]
    async fn test_subscribe_small_capacity_distribution() {
        let pool = ConnectionPool::new(PoolConfig {
            capacity_per_connection: 3,
            connection_config: test_config(),
        });

        let _ = pool.subscribe(channels(10)).await;

        let conns = pool.connection_runners();
        assert_eq!(conns.len(), 4, "ceil(10/3) = 4");
        let counts: Vec<usize> = conns.iter().map(|c| c.channel_count()).collect();
        assert_eq!(counts.iter().sum::<usize>(), 10);
        // First 3 connections at capacity, last with remainder
        assert_eq!(counts[0], 3);
        assert_eq!(counts[1], 3);
        assert_eq!(counts[2], 3);
        assert_eq!(counts[3], 1);
    }

    // ─── Production-scale distribution ─────────────────────────────

    /// Simulate real workload: 1680 channels, cap=200 → 9 connections.
    #[tokio::test]
    async fn test_subscribe_production_scale() {
        let pool = ConnectionPool::new(PoolConfig {
            capacity_per_connection: 200,
            connection_config: test_config(),
        });

        let _ = pool.subscribe(channels(1680)).await;

        let conns = pool.connection_runners();
        assert_eq!(conns.len(), 9, "ceil(1680/200) = 9");
        let total: usize = conns.iter().map(|c| c.channel_count()).sum();
        assert_eq!(total, 1680, "all channels must be tracked");
        // Each connection ≤ capacity
        assert!(
            conns.iter().all(|c| c.channel_count() <= 200),
            "no connection should exceed capacity"
        );
        // First 8 connections should be at exactly 200 (full), last at 80
        for i in 0..8 {
            assert_eq!(conns[i].channel_count(), 200, "connection {} should be full", i);
        }
        assert_eq!(conns[8].channel_count(), 80, "last connection gets remainder");
    }

    // ─── Pre-existing channels ─────────────────────────────────────

    /// Connection with existing channels gets fewer new ones.
    #[tokio::test]
    async fn test_subscribe_with_existing_channels() {
        let pool = ConnectionPool::new(PoolConfig {
            capacity_per_connection: 200,
            connection_config: test_config(),
        });

        // Seed connection 0 with 100 channels (ch_0..ch_99)
        let _ = pool.subscribe(channels_from(0, 100)).await;

        // Now add 150 more (ch_100..ch_249) — connection 0 can take 100 more, rest goes to conn 1
        let _ = pool.subscribe(channels_from(100, 150)).await;

        let conns = pool.connection_runners();
        assert_eq!(conns.len(), 2, "100 + 150 = 250, needs 2 connections with cap=200");
        assert_eq!(conns[0].channel_count(), 200); // first conn filled to capacity
        assert_eq!(conns[1].channel_count(), 50);  // rest on new connection
    }

    /// Multiple rounds of subscribe correctly fill connections.
    #[tokio::test]
    async fn test_subscribe_multiple_rounds() {
        let pool = ConnectionPool::new(PoolConfig {
            capacity_per_connection: 200,
            connection_config: test_config(),
        });

        let _ = pool.subscribe(channels_from(0, 150)).await;    // conn0: 150
        let _ = pool.subscribe(channels_from(150, 100)).await;  // conn0: +50=200, conn1: 50
        let _ = pool.subscribe(channels_from(250, 300)).await;  // conn1: +150=200, conn2: 150

        let conns = pool.connection_runners();
        assert_eq!(conns.len(), 3, "150+100+300=550, ceil(550/200)=3");
        let total: usize = conns.iter().map(|c| c.channel_count()).sum();
        assert_eq!(total, 550);
        assert!(conns.iter().all(|c| c.channel_count() <= 200));
    }

    // ─── Concurrency correctness ───────────────────────────────────

    /// The planned HashMap prevents duplicate assignment even when
    /// spawned tasks haven't updated channel_count yet.
    /// We verify by checking that each channel appears exactly once
    /// across all connections (no duplicates, no missing).
    #[tokio::test]
    async fn test_subscribe_no_duplicate_channels() {
        let pool = ConnectionPool::new(PoolConfig {
            capacity_per_connection: 200,
            connection_config: test_config(),
        });

        // Use unique identifiable channels
        let chs: Vec<String> = (0..500).map(|i| format!("ticker.OPT-{}.agg2", i)).collect();
        let _ = pool.subscribe(chs).await;

        let conns = pool.connection_runners();
        let total: usize = conns.iter().map(|c| c.channel_count()).sum();
        assert_eq!(total, 500);

        // Collect all subscribed channels across connections, verify no duplicates
        let mut all: std::collections::HashSet<String> = std::collections::HashSet::new();
        for conn in &conns {
            for ch in conn.subscribed_channels() {
                assert!(all.insert(ch.clone()), "duplicate channel: {}", ch);
            }
        }
        assert_eq!(all.len(), 500);
    }

    /// Planned assignments should be deterministic: same input → same distribution.
    #[tokio::test]
    async fn test_subscribe_deterministic_distribution() {
        for _ in 0..5 {
            let pool = ConnectionPool::new(PoolConfig {
                capacity_per_connection: 200,
                connection_config: test_config(),
            });

            let _ = pool.subscribe(channels(500)).await;

            let conns = pool.connection_runners();
            assert_eq!(conns.len(), 3, "ceil(500/200) = 3");
            assert_eq!(conns[0].channel_count(), 200);
            assert_eq!(conns[1].channel_count(), 200);
            assert_eq!(conns[2].channel_count(), 100);
        }
    }

    // ─── Connection ID continuity ──────────────────────────────────

    /// Connection IDs should be sequential and start from 0.
    #[tokio::test]
    async fn test_subscribe_connection_ids() {
        let pool = ConnectionPool::new(PoolConfig {
            capacity_per_connection: 200,
            connection_config: test_config(),
        });

        let _ = pool.subscribe(channels(500)).await;

        let conns = pool.connection_runners();
        assert_eq!(conns[0].id(), 0);
        assert_eq!(conns[1].id(), 1);
        assert_eq!(conns[2].id(), 2);
    }
}
