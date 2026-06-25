use std::str::FromStr;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use crate::model::currency::Currency;

#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InstrumentKind {
    Future,
    Option,
    Spot,
    FutureCombo,
    OptionCombo,
    Any,
}

impl std::fmt::Display for InstrumentKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", String::from(*self))
    }
}

impl TryFrom<String> for InstrumentKind {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "future" => Ok(InstrumentKind::Future),
            "option" => Ok(InstrumentKind::Option),
            "spot" => Ok(InstrumentKind::Spot),
            "future_combo" => Ok(InstrumentKind::FutureCombo),
            "option_combo" => Ok(InstrumentKind::OptionCombo),
            "any" => Ok(InstrumentKind::Any),
            _ => Err(anyhow!("unsupported instrument kind: {}", value)),
        }
    }
}

impl FromStr for InstrumentKind {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        InstrumentKind::try_from(s.to_string())
    }
}

impl From<InstrumentKind> for String {
    fn from(value: InstrumentKind) -> Self {
        match value {
            InstrumentKind::Future => "future".to_string(),
            InstrumentKind::Option => "option".to_string(),
            InstrumentKind::Spot => "spot".to_string(),
            InstrumentKind::FutureCombo => "future_combo".to_string(),
            InstrumentKind::OptionCombo => "option_combo".to_string(),
            InstrumentKind::Any => "any".to_string(),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub enum InstrumentState {
    Created,
    Started,
    Settled,
    Closed,
    Deactivated,
    Terminated,
}

impl std::fmt::Display for InstrumentState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", String::from(*self))
    }
}

impl TryFrom<String> for InstrumentState {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "created" => Ok(InstrumentState::Created),
            "started" => Ok(InstrumentState::Started),
            "settled" => Ok(InstrumentState::Settled),
            "closed" => Ok(InstrumentState::Closed),
            "deactivated" => Ok(InstrumentState::Deactivated),
            "terminated" => Ok(InstrumentState::Terminated),
            _ => Err(anyhow!("unsupported instrument state: {}", value)),
        }
    }
}

impl FromStr for InstrumentState {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        InstrumentState::try_from(s.to_string())
    }
}

impl From<InstrumentState> for String {
    fn from(value: InstrumentState) -> Self {
        match value {
            InstrumentState::Created => "created".to_string(),
            InstrumentState::Started => "started".to_string(),
            InstrumentState::Settled => "settled".to_string(),
            InstrumentState::Closed => "closed".to_string(),
            InstrumentState::Deactivated => "deactivated".to_string(),
            InstrumentState::Terminated => "terminated".to_string(),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct InstrumentInfo {
    pub instrument_name: String,
    pub kind: InstrumentKind,
    pub base_currency: Currency,
    pub quote_currency: Currency,
    pub is_active: bool,
    pub creation_timestamp: u64,
    pub expiration_timestamp: u64,
    pub tick_size: f64,
    pub contract_size: f64,
    pub state: String,
    #[serde(default)]
    pub strike: Option<f64>,
    #[serde(default)]
    pub option_type: Option<String>,
    #[serde(default)]
    pub settlement_period: Option<String>,
    #[serde(default)]
    pub min_trade_amount: Option<f64>,
    #[serde(default)]
    pub maker_commission: Option<f64>,
    #[serde(default)]
    pub taker_commission: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_option_instrument() {
        let json = r#"{
            "instrument_name": "BTC-27JUN25-100000-C",
            "kind": "option",
            "base_currency": "BTC",
            "quote_currency": "USDC",
            "is_active": true,
            "creation_timestamp": 1664524802000,
            "expiration_timestamp": 1695974400000,
            "tick_size": 0.0001,
            "contract_size": 1,
            "state": "open",
            "strike": 100000.0,
            "option_type": "call",
            "settlement_period": "month",
            "min_trade_amount": 0.1
        }"#;

        let info: InstrumentInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.instrument_name, "BTC-27JUN25-100000-C");
        assert_eq!(info.kind, InstrumentKind::Option);
        assert_eq!(info.strike, Some(100000.0));
        assert_eq!(info.option_type.as_deref(), Some("call"));
        assert!(info.is_active);
    }

    #[test]
    fn test_deserialize_future_instrument() {
        let json = r#"{
            "instrument_name": "BTC-PERPETUAL",
            "kind": "future",
            "base_currency": "BTC",
            "quote_currency": "USDC",
            "is_active": true,
            "creation_timestamp": 1534167754000,
            "expiration_timestamp": 32503708800000,
            "tick_size": 0.5,
            "contract_size": 10,
            "state": "open",
            "settlement_period": "perpetual"
        }"#;

        let info: InstrumentInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.instrument_name, "BTC-PERPETUAL");
        assert!(info.strike.is_none());
        assert!(info.option_type.is_none());
    }
}
