use thiserror::Error;

#[derive(Error, Debug)]
pub enum DeribitError {
    #[error("Deribit remote error {{code: {code}, message: {message}}}")]
    RemoteError { code: i64, message: String },
    #[error("The background servo pulling message exited")]
    ServoExited,
    #[error("Unknown currency {0}")]
    UnknownCurrency(String),
    #[error("Unknown asset kind {0}")]
    UnknownAssetKind(String),
    #[error("Websocket disconnected")]
    WebsocketDisconnected,
    #[error("Request timed out")]
    RequestTimeout,
}
