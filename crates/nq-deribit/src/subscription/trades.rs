use serde::{Deserialize, Serialize};

use crate::model::currency::Currency;
use crate::model::instrument::InstrumentKind;
use crate::model::interval::Interval;
use crate::gen_channel;

use crate::model::{direction::Direction, liquidation::LiquidationType};

/// Attention: if this is used along with UserTrades,
/// please put this after UserTrades otherwise all UserTrades
/// will be deserialize to Trades since the Trades is a subset of UserTrades
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TradesData {
    pub amount: f64,
    pub block_rfq_id: Option<i64>,
    pub block_trade_id: Option<String>,
    pub combo_id: Option<String>,
    pub combo_trade_id: Option<i64>,
    pub contracts: Option<i64>,
    pub direction: Direction,
    pub index_price: f64,
    pub instrument_name: String,
    pub iv: Option<f64>,
    pub liquidation: Option<LiquidationType>,
    pub mark_price: f64,
    pub price: f64,
    pub tick_direction: u64,
    pub timestamp: u64,
    pub trade_id: String,
    pub trade_seq: u64,
}

gen_channel!(TradesByInstrumentChannel, "trades", String, Interval);
gen_channel!(
    TradesByKindChannel,
    "trades",
    InstrumentKind,
    Currency,
    Interval
);

impl std::fmt::Display for TradesByInstrumentChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "trades.{}.{}", self.0, self.1)
    }
}

impl std::fmt::Display for TradesByKindChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "trades.{}.{}.{}", self.0, self.1, self.2)
    }
}
