#![allow(dead_code)]

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use axum::{Extension, Json, Router, extract::Path, http::StatusCode, routing::get};
use flume::Sender;
use nq_app::runner::Runner;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::bridge_runner::Command;
use crate::config::BridgeConfig;

pub struct ApiServer {
    port: u16,
    command_tx: Sender<Command>,
}

impl ApiServer {
    pub fn new(port: u16, command_tx: Sender<Command>) -> Self {
        Self { port, command_tx }
    }
}

#[async_trait]
impl Runner for ApiServer {
    fn name(&self) -> &'static str {
        "mqtt-http-bridge-api"
    }

    async fn run(&self, canceltoken: CancellationToken) -> Result<()> {
        let state = Arc::new(self.command_tx.clone());

        let app = Router::new()
            .route("/health", get(health))
            .route("/bridges", get(list_bridges).post(create_bridge))
            .route("/bridges/{id}", get(get_bridge).put(update_bridge).delete(delete_bridge))
            .layer(Extension(state));

        let addr = format!("0.0.0.0:{}", self.port);
        let listener = tokio::net::TcpListener::bind(&addr).await?;
        info!("api server listening on {addr}");

        select! {
            _ = canceltoken.cancelled() => {
                info!("api server shutting down");
            }
            result = axum::serve(listener, app) => {
                result?;
            }
        }

        Ok(())
    }
}

// ── Handlers ──────────────────────────────────────────────────

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

async fn list_bridges(
    Extension(tx): Extension<Arc<Sender<Command>>>,
) -> Result<Json<Vec<BridgeConfig>>, StatusCode> {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    tx.send_async(Command::List(reply_tx)).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let configs = reply_rx.await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(configs))
}

async fn get_bridge(
    Extension(tx): Extension<Arc<Sender<Command>>>,
    Path(id): Path<String>,
) -> Result<Json<BridgeConfig>, StatusCode> {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    tx.send_async(Command::Get(id, reply_tx))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    match reply_rx.await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)? {
        Some(config) => Ok(Json(config)),
        None => Err(StatusCode::NOT_FOUND),
    }
}

#[allow(clippy::type_complexity)]
async fn create_bridge(
    Extension(tx): Extension<Arc<Sender<Command>>>,
    Json(config): Json<BridgeConfig>,
) -> Result<(StatusCode, Json<BridgeConfig>), (StatusCode, Json<serde_json::Value>)> {
    // Validate
    if let Err(errors) = config.validate() {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({"error": "validation failed", "details": errors})),
        ));
    }

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    tx.send_async(Command::Add(config, reply_tx)).await.map_err(|_| {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "internal"})))
    })?;

    let response = reply_rx.await.map_err(|_| {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "internal"})))
    })?;
    match response {
        Ok(config) => Ok((StatusCode::CREATED, Json(config))),
        Err(e) => Err((StatusCode::CONFLICT, Json(serde_json::json!({"error": e.to_string()})))),
    }
}

#[allow(clippy::type_complexity)]
async fn update_bridge(
    Extension(tx): Extension<Arc<Sender<Command>>>,
    Path(id): Path<String>,
    Json(config): Json<BridgeConfig>,
) -> Result<(StatusCode, Json<BridgeConfig>), (StatusCode, Json<serde_json::Value>)> {
    if let Err(errors) = config.validate() {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({"error": "validation failed", "details": errors})),
        ));
    }

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    tx.send_async(Command::Update(id, config, reply_tx)).await.map_err(|_| {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "internal"})))
    })?;

    let response = reply_rx.await.map_err(|_| {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "internal"})))
    })?;
    match response {
        Ok(config) => Ok((StatusCode::OK, Json(config))),
        Err(e) => Err((StatusCode::NOT_FOUND, Json(serde_json::json!({"error": e.to_string()})))),
    }
}

#[allow(clippy::type_complexity)]
async fn delete_bridge(
    Extension(tx): Extension<Arc<Sender<Command>>>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<serde_json::Value>)> {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    tx.send_async(Command::Remove(id, reply_tx)).await.map_err(|_| {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "internal"})))
    })?;

    let response = reply_rx.await.map_err(|_| {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "internal"})))
    })?;
    match response {
        Ok(()) => Ok(StatusCode::NO_CONTENT),
        Err(e) => Err((StatusCode::NOT_FOUND, Json(serde_json::json!({"error": e.to_string()})))),
    }
}
