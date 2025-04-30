use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
pub enum LiquidationType {
    #[serde(rename = "M")]
    Maker,
    #[serde(rename = "T")]
    Taker,
    #[serde(rename = "MT")]
    MakerTaker,
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
pub enum LiquidityType {
    #[serde(rename = "M")]
    Maker,
    #[serde(rename = "T")]
    Taker,
}
