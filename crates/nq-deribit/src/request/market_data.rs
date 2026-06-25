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

use crate::model::currency::Currency;
use crate::model::instrument::{InstrumentInfo, InstrumentKind};

impl_request!(
    GetInstrumentsRequest,
    GetInstrumentsResponse,
    "public/get_instruments"
);

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct GetInstrumentsRequest {
    pub currency: Currency,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<InstrumentKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expired: Option<bool>,
}

impl GetInstrumentsRequest {
    pub fn options(currency: Currency) -> Self {
        Self {
            currency,
            kind: Some(InstrumentKind::Option),
            expired: Some(false),
        }
    }
}

pub type GetInstrumentsResponse = Vec<InstrumentInfo>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::Request;

    #[test]
    fn test_get_instruments_request_serialization() {
        let req = GetInstrumentsRequest::options(Currency::BTC);
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"currency\":\"BTC\""));
        assert!(json.contains("\"kind\":\"option\""));
        assert!(json.contains("\"expired\":false"));
        assert!(!json.contains("null"));
    }

    #[test]
    fn test_get_instruments_method() {
        assert_eq!(GetInstrumentsRequest::METHOD, "public/get_instruments");
        assert!(GetInstrumentsRequest::HAS_PAYLOAD);
    }
}
