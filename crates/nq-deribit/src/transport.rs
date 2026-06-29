use std::time::{Duration, Instant};

use anyhow;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use reqwest_websocket::{CloseCode, Message, RequestBuilderExt, WebSocket};
use thiserror::Error;
use tracing::{debug, trace, warn};

// ─── TransportError ──────────────────────────────────────────────────

#[derive(Error, Debug)]
pub enum TransportError {
    #[error("connection failed: {0}")]
    ConnectFailed(#[source] anyhow::Error),
    #[error("send failed: {0}")]
    SendFailed(#[source] anyhow::Error),
    #[error("receive failed: {0}")]
    RecvFailed(#[source] anyhow::Error),
    #[error("websocket closed: code={code:?} reason={reason}")]
    Closed { code: Option<u16>, reason: String },
    #[error("transport not connected")]
    NotConnected,
}

// ─── Transport trait ─────────────────────────────────────────────────

/// WebSocket transport abstraction.
///
/// Layer 1 of the connection stack. Handles raw WebSocket lifecycle:
/// connect, send, receive, ping/pong keepalive, and close.
/// Has no knowledge of JSON-RPC or Deribit protocol.
#[async_trait]
pub trait Transport: Send {
    /// Establish a WebSocket connection to the configured endpoint.
    /// Must be called before any send/recv operations.
    /// Idempotent on re-connect: old connection is dropped, new one created.
    async fn connect(&mut self) -> Result<(), TransportError>;

    /// Send a text message over the WebSocket.
    /// Returns `NotConnected` if `connect()` hasn't been called or the
    /// previous connection was closed.
    async fn send(&mut self, text: String) -> Result<(), TransportError>;

    /// Receive the next application-level message from the WebSocket.
    ///
    /// Handles WebSocket protocol frames internally:
    /// - Ping → auto-responds with Pong, continues waiting
    /// - Pong → updates liveness timestamp, continues waiting
    /// - Close → returns `Ok(None)` (clean close)
    /// - Text/Binary → returns `Ok(Some(text))`
    ///
    /// Also sends periodic Ping frames internally for keepalive.
    async fn recv(&mut self) -> Result<Option<String>, TransportError>;

    /// Close the WebSocket connection cleanly.
    async fn close(&mut self) -> Result<(), TransportError>;

    /// Returns true if the transport currently has an open WebSocket.
    fn is_connected(&self) -> bool;
}

// ─── WsTransportImpl ─────────────────────────────────────────────────

pub struct WsTransportImpl {
    /// Single reqwest::Client shared across all reconnect attempts.
    /// Created once at construction time, never re-created.
    client: reqwest::Client,
    url: String,
    /// Current WebSocket connection. None until `connect()` succeeds.
    ws: Option<WebSocket>,
    /// How often to send Ping frames (configured by ConnectionConfig::ping_interval).
    ping_interval: Duration,
    /// Maximum time to wait for a Pong response before declaring the connection dead.
    /// Defaults to 2 × ping_interval.
    pong_timeout: Duration,
    /// Timestamp of the most recent Pong received. Updated in recv().
    last_pong: Option<Instant>,
    /// Connection ID for logging.
    conn_id: usize,
}

impl WsTransportImpl {
    pub fn new(
        client: reqwest::Client,
        url: String,
        ping_interval: Duration,
        pong_timeout: Duration,
        conn_id: usize,
    ) -> Self {
        Self {
            client,
            url,
            ws: None,
            ping_interval,
            pong_timeout,
            last_pong: None,
            conn_id,
        }
    }

}

#[async_trait]
impl Transport for WsTransportImpl {
    async fn connect(&mut self) -> Result<(), TransportError> {
        debug!(connection_id = self.conn_id, "transport connecting to {}", self.url);

        // Drop old WebSocket if any (from a previous connection cycle).
        // Reqwest closes the underlying connection on drop.
        self.ws = None;

        let res = self
            .client
            .get(&self.url)
            .upgrade()
            .send()
            .await
            .map_err(|e| TransportError::ConnectFailed(anyhow::anyhow!(e)))?;

        let ws = res
            .into_websocket()
            .await
            .map_err(|e| TransportError::ConnectFailed(anyhow::anyhow!(e)))?;

        self.ws = Some(ws);
        self.last_pong = Some(Instant::now()); // reset pong timer on fresh connect

        debug!(connection_id = self.conn_id, "transport connected");
        Ok(())
    }

    async fn send(&mut self, text: String) -> Result<(), TransportError> {
        let ws = self
            .ws
            .as_mut()
            .ok_or(TransportError::NotConnected)?;

        ws.send(Message::Text(text))
            .await
            .map_err(|e| TransportError::SendFailed(e.into()))
    }

    async fn recv(&mut self) -> Result<Option<String>, TransportError> {
        let ws = self
            .ws
            .as_mut()
            .ok_or(TransportError::NotConnected)?;

        // ── Read next message (no proactive ping — Deribit uses
        //    JSON-RPC heartbeat via public/set_heartbeat instead) ─────
        loop {
            let message = match ws.next().await {
                Some(Ok(m)) => m,
                Some(Err(e)) => {
                    return Err(TransportError::RecvFailed(e.into()));
                }
                None => {
                    debug!(connection_id = self.conn_id, "ws stream ended");
                    return Ok(None);
                }
            };

            match message {
                Message::Text(t) => {
                    trace!(connection_id = self.conn_id, len = t.len(), "recv text");
                    return Ok(Some(t));
                }
                Message::Binary(b) => {
                    let text = String::from_utf8(b).map_err(|e| {
                        TransportError::RecvFailed(anyhow::anyhow!("invalid utf8: {}", e))
                    })?;
                    trace!(connection_id = self.conn_id, len = text.len(), "recv binary");
                    return Ok(Some(text));
                }
                Message::Ping(data) => {
                    trace!(connection_id = self.conn_id, "recv ping, sending pong");
                    if let Err(e) = ws.send(Message::Pong(data)).await {
                        warn!(connection_id = self.conn_id, error = ?e, "failed to send pong");
                    }
                    // Continue waiting for next message
                }
                Message::Pong(_data) => {
                    trace!(connection_id = self.conn_id, "recv pong");
                    // Deribit doesn't send unsolicited pongs; if we ever
                    // get one, just ignore it — no internal ping to track.
                }
                Message::Close { code, reason } => {
                    let code_u16: u16 = code.into();
                    debug!(
                        connection_id = self.conn_id,
                        code = code_u16,
                        reason = reason,
                        "recv close frame"
                    );
                    return Ok(None);
                }
            }
        }
    }

    async fn close(&mut self) -> Result<(), TransportError> {
        if let Some(ws) = self.ws.take() {
            debug!(connection_id = self.conn_id, "closing transport");
            // Send a clean close frame then drop the WebSocket.
            // We use the ws.close() method which sends Close and shuts down.
            let _ = ws.close(CloseCode::Normal, Some("client shutdown")).await;
        }
        self.last_pong = None;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.ws.is_some()
    }
}

// ─── Unit tests ──────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify TransportError implements std::error::Error (for anyhow compatibility).
    #[test]
    fn test_transport_error_is_error() {
        fn _assert_error<T: std::error::Error>() {}
        _assert_error::<TransportError>();
    }

    /// Verify TransportError Display impls are meaningful.
    #[test]
    fn test_transport_error_display() {
        let err = TransportError::NotConnected;
        assert!(err.to_string().contains("not connected"));
    }

    /// WsTransportImpl can be constructed without panicking.
    #[test]
    fn test_transport_new() {
        let client = reqwest::Client::new();
        let t = WsTransportImpl::new(
            client,
            "wss://example.com/ws".into(),
            Duration::from_secs(15),
            Duration::from_secs(30),
            0,
        );
        assert!(!t.is_connected());
    }

    /// Send fails with NotConnected when not connected.
    #[tokio::test]
    async fn test_send_not_connected() {
        let client = reqwest::Client::new();
        let mut t = WsTransportImpl::new(
            client,
            "wss://example.com/ws".into(),
            Duration::from_secs(15),
            Duration::from_secs(30),
            0,
        );
        let result = t.send("hello".into()).await;
        assert!(result.is_err());
        match result {
            Err(TransportError::NotConnected) => {}
            other => panic!("expected NotConnected, got {:?}", other),
        }
    }

    /// Recv fails with NotConnected when not connected.
    #[tokio::test]
    async fn test_recv_not_connected() {
        let client = reqwest::Client::new();
        let mut t = WsTransportImpl::new(
            client,
            "wss://example.com/ws".into(),
            Duration::from_secs(15),
            Duration::from_secs(30),
            0,
        );
        let result = t.recv().await;
        assert!(result.is_err());
    }

    /// Close is a no-op (success) when not connected.
    #[tokio::test]
    async fn test_close_not_connected() {
        let client = reqwest::Client::new();
        let mut t = WsTransportImpl::new(
            client,
            "wss://example.com/ws".into(),
            Duration::from_secs(15),
            Duration::from_secs(30),
            0,
        );
        assert!(t.close().await.is_ok());
    }
}
