use serde::{Deserialize, Serialize};

use crate::gen_channel;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct QuoteData {
    pub best_ask_amount: f64,
    pub best_ask_price: f64,
    pub best_bid_amount: f64,
    pub best_bid_price: f64,
    pub instrument_name: String,
    pub timestamp: u64,
}

gen_channel!(QuoteChannel, "quote", String);

impl std::fmt::Display for QuoteChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "quote.{}", self.0)
    }
}
