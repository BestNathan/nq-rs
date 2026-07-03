#![allow(deprecated)]

use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context as _, Result};
use flume::Sender;
use serde_json::{Value, json};
use tokio::{sync::oneshot, time::timeout};
use tracing::debug;

use crate::errors::DeribitError::RequestTimeout;
use crate::{
    errors::DeribitError,
    jsonrpc::{JSONRPCResponse, JSPNRPCRequest},
    request::Request,
};

#[deprecated(
    note = "DeribitApiClient is internal to the legacy Client. Use Connection::call_api for channel-based API calls or ProtocolHandler for direct transport calls."
)]
pub struct DeribitApiClient {
    token: Arc<RwLock<Option<String>>>,
    payload_tx: Sender<String>,
    responser_tx: Sender<(i64, oneshot::Sender<String>)>,
    timeout: Duration,
}

impl DeribitApiClient {
    pub(crate) fn new(
        token: Arc<RwLock<Option<String>>>,
        payload_tx: Sender<String>,
        responser_tx: Sender<(i64, oneshot::Sender<String>)>,
        timeout: Duration,
    ) -> Self {
        Self { token, payload_tx, responser_tx, timeout }
    }

    pub async fn call_raw<R>(&self, request: R) -> Result<JSONRPCResponse<R::Response>>
    where
        R: Request,
    {
        let (responser_tx, responser_rx) = oneshot::channel();
        let req = JSPNRPCRequest::<R>::from(request);
        let payload = {
            if let Some(token) = self.token.read().unwrap().as_ref() {
                let mut value: Value = serde_json::to_value(&req)?;

                if let Some(m) = value.as_object_mut() {
                    m.insert("access_token".to_string(), json!(token));
                }

                value.to_string()
            } else {
                serde_json::to_string(&req)?
            }
        };

        debug!(
            "deribit api client send request(id={},method={}) with payload: {payload}",
            req.id, req.method
        );

        self.payload_tx
            .send_async(payload)
            .await
            .with_context(|| "deribit api client send payload")?;

        self.responser_tx
            .send_async((req.id, responser_tx))
            .await
            .with_context(|| "deribit responser tx send async")?;

        let resp = timeout(self.timeout, responser_rx)
            .await
            .map_err(|_| anyhow::Error::from(RequestTimeout))
            .with_context(|| "deribit responser timeout")?
            .with_context(|| "deribit responser recv")?;

        let result: JSONRPCResponse<R::Response> =
            serde_json::from_str(&resp).with_context(|| "deribit response serde json")?;

        debug!(
            "deribit api client recv response(id={}) with result: {:?}",
            result.id, result.result
        );
        Ok(result)
    }

    pub async fn call<R>(&self, request: R) -> Result<R::Response>
    where
        R: Request,
    {
        let resp = self.call_raw(request).await.with_context(|| "deribit api client call raw")?;

        match resp
            .result
            .map_right(|e| DeribitError::RemoteError { code: e.code, message: e.message }.into())
        {
            either::Either::Left(v) => Ok(v),
            either::Either::Right(e) => Err(e),
        }
    }
}
