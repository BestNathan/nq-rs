use serde::{Deserialize, Serialize};

use crate::{gen_channel, model::index::IndexName};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MarkPriceOptionsData {
    pub instrument_name: String,
    pub iv: f64,
    pub mark_price: f64,
    pub timestamp: u64,
}

gen_channel!(MarkPriceOptionsChannel, "markprice", "options", IndexName);

impl std::fmt::Display for MarkPriceOptionsChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "markprice.options.{}", self.0)
    }
}
