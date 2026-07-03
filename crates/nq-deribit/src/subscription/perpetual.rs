use serde::{Deserialize, Serialize};

use crate::{gen_channel, model::interval::Interval};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct PerpetualData {
    pub index_price: f64,
    pub interest: f64,
    pub timestamp: u64,
}

gen_channel!(PerpetualChannel, "perpetual", String, Interval);

impl std::fmt::Display for PerpetualChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "perpetual.{}.{}", self.0, self.1)
    }
}
