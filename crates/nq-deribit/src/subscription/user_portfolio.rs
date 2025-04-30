use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{model::currency::Currency, gen_channel};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct UserPortfolioData {
    // Options profit and Loss
    pub options_pl: f64,
    // The sum of position deltas without positions that will expire during closest expiration
    pub projected_delta_total: f64,
    // Map of options' thetas per index
    pub options_theta_map: HashMap<String, f64>,
    // Optional (only for users using cross margin). The account's total margin balance in all cross collateral currencies, expressed in USD
    pub total_margin_balance_usd: Option<f64>,
    // Optional (only for users using cross margin). The account's total delta total in all cross collateral currencies, expressed in USD
    pub total_delta_total_usd: Option<f64>,
    // The account's available to withdrawal funds
    pub available_withdrawal_funds: f64,
    // Map of Estimated Liquidation Ratio per index, it is returned only for users with segregated_sm margin model. Multiplying it by future position's market price returns its estimated liquidation price.
    pub estimated_liquidation_ratio_map: HashMap<String, f64>,
    // Options session realized profit and Loss
    pub options_session_rpl: f64,
    // Futures session realized profit and Loss
    pub futures_session_rpl: f64,
    // Profit and loss
    pub total_pl: f64,
    // The account's balance reserved in other orders
    pub additional_reserve: f64,
    // Options session unrealized profit and Loss
    pub options_session_upl: f64,
    // When true cross collateral is enabled for user
    pub cross_collateral_enabled: bool,
    // Map of position sum's per index
    pub delta_total_map: HashMap<String, f64>,
    // Options value
    pub options_value: f64,
    // Map of options' vegas per index
    pub options_vega_map: HashMap<String, f64>,
    // The maintenance margin. When cross collateral is enabled, this aggregated value is calculated by converting the sum of each cross collateral currency's value to the given currency, using each cross collateral currency's index.
    pub maintenance_margin: f64,
    // Futures session unrealized profit and Loss
    pub futures_session_upl: f64,
    // When true portfolio margining is enabled for user
    pub portfolio_margining_enabled: bool,
    // Futures profit and Loss
    pub futures_pl: f64,
    // Map of options' gammas per index
    pub options_gamma_map: HashMap<String, f64>,
    // The selected currency
    pub currency: Currency,
    // Options summary delta
    pub options_delta: f64,
    // The account's initial margin. When cross collateral is enabled, this aggregated value is calculated by converting the sum of each cross collateral currency's value to the given currency, using each cross collateral currency's index.
    pub initial_margin: f64,
    // 	Projected maintenance margin. When cross collateral is enabled, this aggregated value is calculated by converting the sum of each cross collateral currency's value to the given currency, using each cross collateral currency's index.
    pub projected_maintenance_margin: f64,
    // The account's available funds. When cross collateral is enabled, this aggregated value is calculated by converting the sum of each cross collateral currency's value to the given currency, using each cross collateral currency's index.
    pub available_funds: f64,
    // The account's current equity
    pub equity: f64,
    // Name of user's currently enabled margin model
    pub margin_model: String,
    // The account's balance
    pub balance: f64,
    // Session unrealized profit and loss
    pub session_upl: f64,
    // The account's margin balance. When cross collateral is enabled, this aggregated value is calculated by converting the sum of each cross collateral currency's value to the given currency, using each cross collateral currency's index.
    pub margin_balance: f64,
    // Options summary theta
    pub options_theta: f64,
    // Optional (only for users using cross margin). The account's total initial margin in all cross collateral currencies, expressed in USD
    pub total_initial_margin_usd: Option<f64>,
    // [DEPRECATED] Estimated Liquidation Ratio is returned only for users with segregated_sm margin model. Multiplying it by future position's market price returns its estimated liquidation price. Use estimated_liquidation_ratio_map instead.
    pub estimated_liquidation_ratio: f64,
    // Session realized profit and loss
    pub session_rpl: f64,
    // The account's fee balance (it can be used to pay for fees)
    pub fee_balance: f64,
    // Optional (only for users using cross margin). The account's total maintenance margin in all cross collateral currencies, expressed in USD
    pub total_maintenance_margin_usd: Option<f64>,
    // Options summary vega
    pub options_vega: f64,
    // Projected initial margin. When cross collateral is enabled, this aggregated value is calculated by converting the sum of each cross collateral currency's value to the given currency, using each cross collateral currency's index.
    pub projected_initial_margin: f64,
    // Options summary gamma
    pub options_gamma: f64,
    // Optional (only for users using cross margin). The account's total equity in all cross collateral currencies, expressed in USD
    pub total_equity_usd: f64,
    // The sum of position deltas
    pub delta_total: f64,
}

gen_channel!(UserPortfolioChannel, "user", "portfolio", Currency);

impl std::fmt::Display for UserPortfolioChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "user.portfolio.{}", self.0)
    }
}
