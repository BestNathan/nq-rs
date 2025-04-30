use std::{fmt::Display, str::FromStr};

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Buy,
    Sell,
}

impl Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", String::from(*self))
    }
}

impl From<Direction> for String {
    fn from(value: Direction) -> Self {
        match value {
            Direction::Buy => "buy".to_string(),
            Direction::Sell => "sell".to_string(),
        }
    }
}

impl FromStr for Direction {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Direction::try_from(s.to_string())
    }
}

impl TryFrom<String> for Direction {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "buy" => Ok(Direction::Buy),
            "sell" => Ok(Direction::Sell),
            _ => Err(anyhow!("invalid direction value: {}", value)),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
#[serde(into = "u8", try_from = "u8")]
pub enum TickDirection {
    /// 0 = Plus Tick (价格上涨)
    Plus,
    /// 1 = Zero-Plus Tick (价格不变但前一次是上涨)
    ZeroPlus,
    /// 2 = Minus Tick (价格下跌)
    Minus,
    /// 3 = Zero-Minus Tick (价格不变但前一次是下跌)
    ZeroMinus,
}

impl TryFrom<u8> for TickDirection {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Plus),
            1 => Ok(Self::ZeroPlus),
            2 => Ok(Self::Minus),
            3 => Ok(Self::ZeroMinus),
            _ => Err(anyhow!("unsupported tick direction value: {}", value)),
        }
    }
}

impl From<TickDirection> for u8 {
    fn from(val: TickDirection) -> Self {
        match val {
            TickDirection::Plus => 0,
            TickDirection::ZeroPlus => 1,
            TickDirection::Minus => 2,
            TickDirection::ZeroMinus => 3,
        }
    }
}
