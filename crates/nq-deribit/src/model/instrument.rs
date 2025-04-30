use std::str::FromStr;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
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
