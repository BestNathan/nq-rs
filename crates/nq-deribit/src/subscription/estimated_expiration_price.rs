use serde::{Deserialize, Serialize};

use crate::{model::index::IndexName, gen_channel};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct EstimatedExpirationPriceData {
    pub is_estimated: bool,
    pub price: f64,
    pub left_ticks: Option<f64>,
    pub seconds: f64,
    pub total_ticks: Option<f64>,
}

gen_channel!(
    EstimatedExpirationPriceChannel,
    "estimated_expiration_price",
    IndexName
);

impl std::fmt::Display for EstimatedExpirationPriceChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "estimated_expiration_price.{}", self.0)
    }
}
