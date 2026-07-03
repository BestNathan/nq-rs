use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{Error, Unexpected},
};
use serde_json::Value;

use crate::model::{
    currency::Currency,
    direction::{Direction, TickDirection},
    liquidation::{LiquidationType, LiquidityType},
    order::{OrderState, OrderType},
};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct UserTradesData {
    // Unique (per currency) trade identifier
    pub trade_id: String,
    // Direction of the "tick" (0 = Plus Tick, 1 = Zero-Plus Tick, 2 = Minus Tick, 3 = Zero-Minus Tick).
    pub tick_direction: TickDirection,
    // Currency, i.e "BTC", "ETH", "USDC"
    pub fee_currency: Currency,
    // true if user order was created with API
    pub api: bool,
    // Advanced type of user order: "usd" or "implv" (only for options; omitted if not applicable)
    pub advanced: Option<String>,
    // Id of the user order (maker or taker), i.e. subscriber's order id that took part in the trade
    pub order_id: String,
    // Describes what was role of users order: "M" when it was maker order, "T" when it was taker order
    pub liquidity: LiquidityType,
    // true if user order is post-only
    pub post_only: bool,
    // Direction: buy, or sell
    pub direction: Direction,
    // Trade size in contract units (optional, may be absent in historical trades)
    pub contracts: i64,
    // true if user order is MMP
    pub mmp: bool,
    // User's fee in units of the specified fee_currency
    pub fee: f64,
    // QuoteID of the user order (optional, present only for orders placed with private/mass_quote)
    pub quote_id: Option<String>,
    // Index Price at the moment of trade
    pub index_price: f64,
    // User defined label (presented only when previously set for order by user)
    pub label: Option<String>,
    // Block trade id - when trade was part of a block trade
    pub block_trade_id: Option<String>,
    // Price in base currency
    pub price: f64,
    // Optional field containing combo instrument name if the trade is a combo trade
    pub combo_id: Option<String>,
    // Always null
    pub matching_id: Option<String>,
    // Order type: "limit, "market", or "liquidation"
    pub order_type: OrderType,
    // Profit and loss in base currency.
    pub profit_loss: f64,
    // The timestamp of the trade (milliseconds since the UNIX epoch)
    pub timestamp: u64,
    // Option implied volatility for the price (Option only)
    pub iv: Option<f64>,
    // Order state: "open", "filled", "rejected", "cancelled", "untriggered" or "archive" (if order was archived)
    pub state: OrderState,
    // Underlying price for implied volatility calculations (Options only)
    pub underlying_price: Option<f64>,
    // ID of the Block RFQ quote - when trade was part of the Block RFQ
    pub block_rfq_quote_id: Option<i64>,
    // QuoteSet of the user order (optional, present only for orders placed with private/mass_quote)
    pub quote_set_id: Option<String>,
    // Mark Price at the moment of trade
    pub mark_price: Option<f64>,
    // ID of the Block RFQ - when trade was part of the Block RFQ
    pub block_rfq_id: Option<i64>,
    // Optional field containing combo trade identifier if the trade is a combo trade
    pub combo_trade_id: Option<i64>,
    // true if user order is reduce-only
    pub reduce_only: bool,
    // Trade amount. For perpetual and inverse futures the amount is in USD units. For options and linear futures and it is the underlying base currency coin.
    pub amount: f64,
    // Optional field (only for trades caused by liquidation): "M" when maker side of trade was under liquidation, "T" when taker side was under liquidation, "MT" when both sides of trade were under liquidation
    pub liquidation: Option<LiquidationType>,
    // The sequence number of the trade within instrument
    pub trade_seq: i64,
    // true if user order is marked by the platform as a risk reducing order (can apply only to orders placed by PM users)
    pub risk_reducing: bool,
    // Unique instrument identifier
    pub instrument_name: String,
    // Optional field containing leg trades if trade is a combo trade (present when querying for only combo trades and in combo_trades events)
    pub legs: Option<Vec<Value>>,
}

#[derive(Debug, Clone)]
pub enum UserTradesChannel {
    ByInstrument { instrument_name: String, interval: String },
    ByKind { kind: String, currency: String, interval: String },
}

impl<'de> Deserialize<'de> for UserTradesChannel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <&str as Deserialize<'de>>::deserialize(deserializer)?;
        let segments: Vec<_> = s.split(".").collect();
        match segments.as_slice() {
            ["user", "trades", instrument_name, interval] => Ok(UserTradesChannel::ByInstrument {
                instrument_name: instrument_name.to_string(),
                interval: interval.to_string(),
            }),
            ["user", "trades", kind, currency, interval] => Ok(UserTradesChannel::ByKind {
                kind: kind.to_string(),
                currency: currency.to_string(),
                interval: interval.to_string(),
            }),
            _ => Err(D::Error::invalid_value(
                Unexpected::Str(s),
                &"user.trades.{instrument_name}.{interval} or trades.{kind}.{currency}.{interval}",
            )),
        }
    }
}
impl Serialize for UserTradesChannel {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl std::fmt::Display for UserTradesChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UserTradesChannel::ByInstrument { instrument_name, interval } => {
                write!(f, "user.trades.{}.{}", instrument_name, interval)
            }
            UserTradesChannel::ByKind { kind, currency, interval } => {
                write!(f, "user.trades.{}.{}.{}", kind, currency, interval)
            }
        }
    }
}
