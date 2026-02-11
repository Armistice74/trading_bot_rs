// stop.rs
// Description: Handles stop-loss behaviors, including per-trade fixed/trailing stops.

// Imports

use crate::config::Config;
use rust_decimal::prelude::*;
use tracing::info;

// Enums

#[derive(Clone, Debug)]
pub enum StopType {
    Fixed,
    Trailing,
}

// Functions

/// Evaluates if a fixed or trailing stop-loss should trigger for a trade.
/// Returns Some(StopType) if triggered, None otherwise.
pub async fn check_stop_loss(
    trade_timestamp: &str,
    avg_cost_basis: Decimal,
    close_price: Decimal,
    highest_price_since_buy: Decimal,
    config: &Config,
) -> Option<StopType> {
    let buy_time = crate::parse_est_timestamp(trade_timestamp);
    let minutes_since_buy = buy_time
        .map(|bt| Decimal::from((crate::get_current_time() - bt).num_minutes()))
        .unwrap_or(Decimal::ZERO);

    // Fixed Stop-Loss
    if config.bot_operation.fixed_stop_loss.value {
        let period = Decimal::from(config.stop_loss_behavior.fixed_stop_tightening_period.value);
        let tightening_factor = (minutes_since_buy / period).min(Decimal::ONE);
        let fixed_stop_percentage = config.stop_loss_behavior.fixed_stop_initial.value
            - (config.stop_loss_behavior.fixed_stop_initial.value
                - config.stop_loss_behavior.fixed_stop_min.value)
                * tightening_factor;
        let fixed_stop_threshold = avg_cost_basis * (Decimal::ONE - fixed_stop_percentage / Decimal::from(100));
        if close_price <= fixed_stop_threshold {
            info!(
                "Fixed stop-loss triggered: close_price={:.6} <= threshold={:.6}",
                close_price, fixed_stop_threshold
            );
            return Some(StopType::Fixed);
        }
    }

    // Trailing Stop-Loss
    if config.bot_operation.trailing_stop_loss.value {
        let trailing_stop_price = highest_price_since_buy
            * (Decimal::ONE - config.stop_loss_behavior.trailing_stop_percentage.value / Decimal::from(100));
        if close_price <= trailing_stop_price {
            info!(
                "Trailing stop-loss triggered: close_price={:.6} <= threshold={:.6}",
                close_price, trailing_stop_price
            );
            return Some(StopType::Trailing);
        }
    }

    None
}