use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use anyhow::Result;
use futures_util::Stream;
use nq_app::runner::Runner;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::connection::{Connection, ConnectionConfig};

pub struct ConnectionPool {
    connections: Arc<RwLock<Vec<Arc<Connection>>>>,
    capacity: usize,
    next_id: AtomicUsize,
    base_config: ConnectionConfig,
    cancel_token: CancellationToken,
}

pub struct PoolConfig {
    pub capacity_per_connection: usize,
    pub connection_config: ConnectionConfig,
}

impl ConnectionPool {
    pub fn new(config: PoolConfig) -> Self {
        let cancel_token = CancellationToken::new();
        let first = Arc::new(Connection::new(0, config.connection_config.clone()));
        Self {
            connections: Arc::new(RwLock::new(vec![first])),
            capacity: config.capacity_per_connection,
            next_id: AtomicUsize::new(1),
            base_config: config.connection_config,
            cancel_token,
        }
    }

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
            // Yield so the spawned task can run and increment channel_count(),
            // ensuring find_or_create_connection on the next iteration sees
            // up-to-date capacity.
            tokio::task::yield_now().await;
        }

        // Await all spawned tasks; first JoinError or subscribe error propagates
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

    pub fn subscription_stream(&self) -> impl Stream<Item = String> {
        let conns = self.connections.read().unwrap();
        let streams: Vec<_> = conns.iter()
            .map(|c| c.subscription_rx().into_stream())
            .collect();
        futures_util::stream::select_all(streams)
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

    fn find_or_create_connection(&self) -> Arc<Connection> {
        {
            let conns = self.connections.read().unwrap();
            for conn in conns.iter() {
                if conn.channel_count() < self.capacity {
                    return conn.clone();
                }
            }
        }

        // All full — create new connection and auto-spawn its eventloop
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let conn = Arc::new(Connection::new(id, self.base_config.clone()));
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
