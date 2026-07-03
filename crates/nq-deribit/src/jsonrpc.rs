use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicI64, Ordering},
};

use anyhow::{Context, Error, Result, anyhow};
use either::Either;
use reqwest_websocket::Message;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::request::Request;

#[derive(Deserialize, Serialize, Clone, Debug, Copy)]
pub enum JSONRPCVersion {
    #[serde(rename = "2.0")]
    V2,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct JSPNRPCRequest<T: Request> {
    pub jsonrpc: JSONRPCVersion,
    pub id: i64,
    pub method: String,
    #[serde(skip_serializing_if = "crate::request::Request::no_payload")]
    pub params: T,
}

impl<T: Request> From<T> for JSPNRPCRequest<T> {
    fn from(value: T) -> Self {
        Self {
            jsonrpc: JSONRPCVersion::V2,
            id: global_id_generator().next_id(),
            method: T::METHOD.to_string(),
            params: value,
        }
    }
}

impl<T: Request> TryFrom<JSPNRPCRequest<T>> for Message {
    type Error = Error;

    fn try_from(value: JSPNRPCRequest<T>) -> Result<Self> {
        Ok(Message::Text(serde_json::to_string(&value)?))
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct JSONRPCError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct JSONRPCResponse<T>
where
    T: Serialize + for<'a> Deserialize<'a>,
{
    pub jsonrpc: JSONRPCVersion,
    pub id: i64,
    pub testnet: bool,

    #[serde(alias = "error", with = "either::serde_untagged")]
    pub result: Either<T, JSONRPCError>,
    pub us_in: u64,
    pub us_out: u64,
    pub us_diff: u64,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum JSONRPCSubscriptionMethod {
    Subscription,
    Heartbeat,
}

#[derive(Deserialize, Debug)]
pub struct JSONRPCSubscription<C, D>
where
    C: for<'a> Deserialize<'a>,
    D: for<'a> Deserialize<'a>,
{
    pub jsonrpc: JSONRPCVersion,
    pub method: JSONRPCSubscriptionMethod,
    #[serde(with = "either::serde_untagged")]
    pub params: Either<JSONRPCSubscriptionParam<C, D>, HeartbeatParam>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum HeartbeatType {
    Heartbeat,
    TestRequest,
}

#[derive(Deserialize, Debug)]
pub struct HeartbeatParam {
    pub r#type: HeartbeatType,
}

#[derive(Deserialize, Debug)]
pub struct JSONRPCSubscriptionParam<C, D> {
    pub channel: C,
    pub data: D,
}

impl<C, D> TryFrom<Message> for JSONRPCSubscription<C, D>
where
    C: for<'a> Deserialize<'a>,
    D: for<'a> Deserialize<'a>,
{
    type Error = Error;

    fn try_from(value: Message) -> Result<Self, Self::Error> {
        match value {
            Message::Text(txt) => Ok(serde_json::from_str(&txt)?),
            Message::Binary(b) => Ok(serde_json::from_slice(&b)?),
            _ => Err(anyhow!("unsupported message: {:?}", value)),
        }
    }
}

static DEFAULT_ID_GENERATOR: OnceLock<DefaultIDGenerator> = OnceLock::new();

pub fn global_id_generator() -> &'static impl IDGenerator {
    DEFAULT_ID_GENERATOR.get_or_init(|| DefaultIDGenerator::new(0))
}

pub trait IDGenerator: Sync + Send {
    fn next_id(&self) -> i64;
}

pub struct DefaultIDGenerator {
    counter: Arc<AtomicI64>,
}

impl DefaultIDGenerator {
    pub fn new(initial_value: i64) -> Self {
        Self { counter: Arc::new(AtomicI64::new(initial_value)) }
    }
}

impl IDGenerator for DefaultIDGenerator {
    fn next_id(&self) -> i64 {
        self.counter.fetch_add(1, Ordering::SeqCst) + 1
    }
}

// ─── Validation ──────────────────────────────────────────────────────

/// Validate that a JSON-RPC 2.0 request payload has the required fields:
/// `jsonrpc`, `method`, `id`. Rejects responses and notifications.
///
/// This catches protocol errors like missing `method` or `params` wrapper
/// that occur when JSON is assembled manually instead of using
/// [`JSPNRPCRequest`].
pub fn validate_jsonrpc_request(payload: &str) -> Result<()> {
    let val: Value = serde_json::from_str(payload).context("invalid JSON")?;

    let obj = val.as_object().context("JSON-RPC request must be an object")?;

    let version = obj.get("jsonrpc").and_then(|v| v.as_str()).context("missing 'jsonrpc' field")?;
    anyhow::ensure!(version == "2.0", "jsonrpc version must be '2.0', got '{version}'");

    // Must have a method (reject if it's a response with "result"/"error")
    anyhow::ensure!(
        obj.contains_key("method"),
        "missing 'method' field — did you forget to wrap params in a JSPNRPCRequest?"
    );

    // Must have an id (notifications lack id, but we only send requests)
    anyhow::ensure!(
        obj.contains_key("id"),
        "missing 'id' field — JSON-RPC requests require an id for correlation"
    );

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    // ── validate_jsonrpc_request tests ─────────────────────────────

    #[test]
    fn test_validate_correct_request() {
        // A proper JSPNRPCRequest-serialized payload
        let payload = r#"{"jsonrpc":"2.0","id":1,"method":"public/auth","params":{"grant_type":"client_credentials","client_id":"x","client_secret":"y"}}"#;
        assert!(validate_jsonrpc_request(payload).is_ok());
    }

    #[test]
    fn test_validate_missing_method() {
        // The bug we had: manually assembled without "method" field
        let payload = r#"{"grant_type":"client_credentials","client_id":"x","client_secret":"y","jsonrpc":"2.0","id":1}"#;
        let err = validate_jsonrpc_request(payload).unwrap_err();
        assert!(err.to_string().contains("method"), "expected 'method' error, got: {err}");
    }

    #[test]
    fn test_validate_missing_jsonrpc() {
        let payload = r#"{"id":1,"method":"public/test","params":{}}"#;
        assert!(validate_jsonrpc_request(payload).is_err());
    }

    #[test]
    fn test_validate_missing_id() {
        let payload = r#"{"jsonrpc":"2.0","method":"public/test","params":{}}"#;
        assert!(validate_jsonrpc_request(payload).is_err());
    }

    #[test]
    fn test_validate_not_json() {
        assert!(validate_jsonrpc_request("not json").is_err());
    }

    #[test]
    fn test_validate_not_object() {
        assert!(validate_jsonrpc_request("[]").is_err());
    }

    // ── existing tests ─────────────────────────────────────────────
    use serde_json::Value;

    use super::JSONRPCResponse;

    const ERRSTR: &str = r#"{
    "jsonrpc": "2.0",
    "id": 8163,
    "error": {
        "code": 11050,
        "message": "bad_request"
    },
    "testnet": false,
    "usIn": 1535037392434763,
    "usOut": 1535037392448119,
    "usDiff": 13356
}"#;

    const SUCSTR: &str = r#"{
    "jsonrpc": "2.0",
    "id": 5239,
    "testnet": false,
    "result": [
        {
            "coin_type": "BITCOIN",
            "currency": "BTC",
            "currency_long": "Bitcoin",
            "fee_precision": 4,
            "min_confirmations": 1,
            "min_withdrawal_fee": 0.0001,
            "withdrawal_fee": 0.0001,
            "withdrawal_priorities": [
                {
                    "value": 0.15,
                    "name": "very_low"
                },
                {
                    "value": 1.5,
                    "name": "very_high"
                }
            ]
        }
    ],
    "usIn": 1535043730126248,
    "usOut": 1535043730126250,
    "usDiff": 2
}"#;

    #[test]
    fn test() {
        let res: JSONRPCResponse<Value> = serde_json::from_str(SUCSTR).unwrap();
        println!("success res: {:?}", res);

        let res: JSONRPCResponse<String> = serde_json::from_str(ERRSTR).unwrap();
        println!("error res: {:?}", res);
    }
}
