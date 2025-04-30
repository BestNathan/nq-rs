use serde::{Deserialize, Serialize};

use crate::{model::index::IndexName, gen_channel};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DeribitPriceRankingData {
    pub enabled: bool,
    pub identifier: String,
    pub original_price: f64,
    pub price: f64,
    pub timestamp: u64,
    pub weight: f64,
}

gen_channel!(
    DeribitPriceRankingChannel,
    "deribit_price_ranking",
    IndexName
);

impl std::fmt::Display for DeribitPriceRankingChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "deribit_price_ranking.{}", self.0)
    }
}
