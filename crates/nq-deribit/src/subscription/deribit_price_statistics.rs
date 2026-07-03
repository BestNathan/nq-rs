use serde::{Deserialize, Serialize};

use crate::{gen_channel, model::index::IndexName};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DeribitPriceStatisticsData {
    pub enabled: bool,
    pub identifier: String,
    pub original_price: f64,
    pub price: f64,
    pub timestamp: u64,
    pub weight: f64,
}

gen_channel!(DeribitPriceStatisticsChannel, "deribit_price_statistics", IndexName);

impl std::fmt::Display for DeribitPriceStatisticsChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "deribit_price_statistics.{}", self.0)
    }
}
