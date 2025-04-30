use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    Limit,
    Market,
    Liquidation,
    StopLimit,
    StopMarket,
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum OrderState {
    Open,
    Filled,
    Rejected,
    Cancelled,
    Untriggered,
    Archive,
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum OrderTrigger {
    IndexPrice,
    MarkPrice,
    LastPrice,
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    GoodTilCancelled,
    GoodTilDay,
    FillOrKill,
    ImmediateOrCancel,
}
