use std::{fmt::Display, str::FromStr};

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
#[serde(rename_all = "UPPERCASE")]
pub enum Currency {
    BTC,
    ETH,
    USDC,
    USDT,
    EURR,
    #[serde(rename = "any")]
    Any,
}

impl Display for Currency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", String::from(*self))
    }
}

impl TryFrom<String> for Currency {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "BTC" => Ok(Currency::BTC),
            "ETH" => Ok(Currency::ETH),
            "EURR" => Ok(Currency::EURR),
            "USDT" => Ok(Currency::USDT),
            "USDC" => Ok(Currency::USDC),
            "any" => Ok(Currency::Any),
            _ => Err(anyhow!("invalid value: {}", value)),
        }
    }
}

impl From<Currency> for String {
    fn from(value: Currency) -> Self {
        match value {
            Currency::BTC => "BTC".to_string(),
            Currency::ETH => "ETH".to_string(),
            Currency::EURR => "EURR".to_string(),
            Currency::USDT => "USDT".to_string(),
            Currency::USDC => "USDC".to_string(),
            Currency::Any => "any".to_string(),
        }
    }
}

impl FromStr for Currency {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, anyhow::Error> {
        Currency::try_from(s.to_string())
    }
}
