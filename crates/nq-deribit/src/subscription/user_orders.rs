use serde::{Deserialize, Serialize};

use crate::{
    model::{
        currency::Currency,
        direction::Direction,
        instrument::InstrumentKind,
        interval::Interval,
        order::{OrderState, OrderTrigger, OrderType, TimeInForce},
    },
    gen_channel,
};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct UserOrdersData {
    pub advanced: Option<String>,
    pub amount: f64,
    pub api: bool,
    pub average_price: f64,
    pub commission: f64,
    pub creation_timestamp: u64,
    pub direction: Direction,
    pub filled_amount: f64,
    pub implv: Option<f64>,
    pub instrument_name: String,
    pub is_liquidation: bool,
    pub label: String,
    pub last_update_timestamp: u64,
    pub max_show: f64,
    pub order_id: String,
    pub order_state: OrderState,
    pub order_type: OrderType,
    pub post_only: bool,
    pub price: f64,
    pub profit_loss: f64,
    pub reduce_only: bool,
    pub stop_price: Option<f64>,
    pub time_in_force: TimeInForce,
    pub trigger: Option<OrderTrigger>,
    pub triggered: Option<bool>,
    pub usd: Option<f64>,
    pub replaced: bool, // TODO: Remove the Option when necessary
    pub web: bool,
}

gen_channel!(
    UserOrdersByInstrumentChannel,
    "user",
    "orders",
    String,
    Interval
);
gen_channel!(
    UserOrdersByKindChannel,
    "user",
    "orders",
    InstrumentKind,
    Currency,
    Interval
);

impl std::fmt::Display for UserOrdersByInstrumentChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "user.orders.{}.{}", self.0, self.1)
    }
}

impl std::fmt::Display for UserOrdersByKindChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "user.orders.{}.{}.{}", self.0, self.1, self.2)
    }
}
