use crate::{
    model::{
        currency::Currency, direction::Direction, instrument::InstrumentKind, interval::Interval,
    },
    gen_channel,
};

use serde::{Deserialize, Serialize};

use super::{user_orders::UserOrdersData, user_trades::UserTradesData};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct UserChangesData {
    pub trades: Vec<UserTradesData>,
    pub positions: Vec<UserPositionsData>,
    pub orders: Vec<UserOrdersData>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct UserPositionsData {
    pub average_price: f64,
    pub average_price_usd: Option<f64>,
    pub delta: f64,
    pub direction: Direction,
    pub estimated_liquidation_price: Option<f64>,
    pub floating_profit_loss: f64,
    pub floating_profit_loss_usd: Option<f64>,
    pub index_price: f64,
    pub initial_margin: f64,
    pub instrument_name: String,
    pub kind: InstrumentKind,
    pub leverage: f64,
    pub maintenance_margin: f64,
    pub mark_price: f64,
    pub open_orders_margin: f64,
    pub realized_funding: Option<f64>,
    pub realized_profit_loss: f64,
    pub settlement_price: f64,
    pub size: f64,
    pub size_currency: Option<f64>,
    pub total_profit_loss: f64,
}

gen_channel!(UserChangesByInstrument, "user", "changes", String, Interval);
gen_channel!(
    UserChangesByKind,
    "user",
    "changes",
    InstrumentKind,
    Currency,
    Interval
);

impl std::fmt::Display for UserChangesByInstrument {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "user.changes.{}.{}", self.0, self.1)
    }
}

impl std::fmt::Display for UserChangesByKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "user.changes.{}.{}.{}", self.0, self.1, self.2)
    }
}
