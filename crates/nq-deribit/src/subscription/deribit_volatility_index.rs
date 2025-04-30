use serde::{Deserialize, Serialize};

use crate::{model::index::IndexName, gen_channel};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DeribitVolatilityIndexData {
    pub index_name: String,
    pub timestamp: u64,
    pub volatility: f64,
}

gen_channel!(
    DeribitVolatilityIndexChannel,
    "deribit_volatility_index",
    IndexName
);

impl std::fmt::Display for DeribitVolatilityIndexChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "deribit_volatility_index.{}", self.0)
    }
}
