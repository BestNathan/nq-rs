use serde::{Deserialize, Serialize};

use crate::impl_request;

impl_request!(
    GetOrderBookRequest,
    GetOrderBookResponse,
    "public/get_order_book",
    false
);

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct GetOrderBookRequest {
    instrument_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    depth: Option<u64>,
}

impl GetOrderBookRequest {
    pub fn new(instrument_name: &str) -> Self {
        Self {
            instrument_name: instrument_name.to_string(),
            ..Default::default()
        }
    }
    pub fn with_depth(instrument_name: &str, depth: u64) -> Self {
        Self {
            instrument_name: instrument_name.to_string(),
            depth: Some(depth),
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct GetOrderBookResponse(Vec<()>);
