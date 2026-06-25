use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use anyhow::Result;
use futures_util::Stream;
use tracing::info;

use crate::connection::{Connection, ConnectionConfig};

pub struct ConnectionPool {
    connections: Arc<RwLock<Vec<Arc<Connection>>>>,
    capacity: usize,
    next_id: AtomicUsize,
    base_config: ConnectionConfig,
}

pub struct PoolConfig {
    pub capacity_per_connection: usize,
    pub connection_config: ConnectionConfig,
}

impl ConnectionPool {
    pub fn new(config: PoolConfig) -> Self {
        let first = Arc::new(Connection::new(0, config.connection_config.clone()));
        Self {
            connections: Arc::new(RwLock::new(vec![first])),
            capacity: config.capacity_per_connection,
            next_id: AtomicUsize::new(1),
            base_config: config.connection_config,
        }
    }

    pub async fn subscribe(&self, channels: Vec<String>) -> Result<()> {
        if channels.is_empty() {
            return Ok(());
        }
        let conn = self.find_or_create_connection();
        conn.subscribe(channels).await
    }

    pub async fn unsubscribe(&self, channels: Vec<String>) -> Result<()> {
        if channels.is_empty() {
            return Ok(());
        }
        let conn = {
            let conns = self.connections.read().unwrap();
            conns.iter()
                .min_by_key(|c| c.channel_count())
                .unwrap()
                .clone()
        };
        conn.unsubscribe(channels).await
    }

    pub fn subscription_stream(&self) -> impl Stream<Item = String> {
        let conns = self.connections.read().unwrap();
        let streams: Vec<_> = conns.iter()
            .map(|c| c.subscription_rx().into_stream())
            .collect();
        futures_util::stream::select_all(streams)
    }

    /// Returns a snapshot of all connections. Call once at startup to add
    /// them as Runners to the Application.
    pub fn connection_runners(&self) -> Vec<Arc<Connection>> {
        self.connections.read().unwrap().clone()
    }

    pub fn first_connection(&self) -> Arc<Connection> {
        self.connections.read().unwrap()[0].clone()
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

        // All full — create new connection
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let conn = Arc::new(Connection::new(id, self.base_config.clone()));
        {
            let mut conns = self.connections.write().unwrap();
            conns.push(conn.clone());
        }
        info!(connection_id = id, "pool created new connection");

        // NOTE: The new connection won't have a Runner spawned yet.
        // Callers should ensure pool is sized correctly upfront.
        conn
    }
}
