use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WebsocketMessage {
    SubscriptionMessage(SubscriptionMessage),
    ResultMessage(ResultMessage),
    Other(Value),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubscriptionMessage {
    jsonrpc: String,
    pub method: String,
    pub params: SubscriptionParams,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SubscriptionParams {
    Subscribe(SubscribeParams),
    Other(Value),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubscribeParams {
    pub channel: String,
    pub data: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResultMessage {
    jsonrpc: String,
    id: u64,
    result: Value,
    #[serde(rename = "usIn")]
    us_in: u64,
    #[serde(rename = "usOut")]
    us_out: u64,
    #[serde(rename = "usDiff")]
    us_diff: u64,
    testnet: bool,
}

pub struct MessageAssembler {
    counter: Arc<Mutex<i64>>,
}

impl MessageAssembler {
    pub fn new() -> Self {
        MessageAssembler {
            counter: Mutex::new(0).into(),
        }
    }

    fn id(&self) -> i64 {
        let mut v = self.counter.lock().unwrap();
        *v += 1;
        *v
    }

    pub fn subscribe_message(&self, channels: Vec<String>) -> String {
        json!({
          "jsonrpc" : "2.0",
          "id" : self.id(),
          "method" : "public/subscribe",
          "params" : {
            "channels" : channels
          }
        })
        .to_string()
    }

    pub fn set_heartbeat_message(&self, i: i64) -> String {
        json!({
          "jsonrpc" : "2.0",
          "id" : self.id(),
          "method" : "public/set_heartbeat",
          "params" : {
            "interval" : i,
          }
        })
        .to_string()
    }

    pub fn test_message(&self) -> String {
        json!({
          "jsonrpc" : "2.0",
          "id" : self.id(),
          "method" : "public/test",
          "params" : {

          }
        })
        .to_string()
    }
}
