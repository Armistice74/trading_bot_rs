// indicators.rs
// description: This module contains functions for calculating technical indicators.

use crate::config::Config;
use crate::statemanager::StateManager;
use crate::get_current_time;
use ndarray::Array1;
use rust_decimal::prelude::*;
use std::sync::Arc;
use tracing::{error, info};
// ============================================================================
// Technical Indicators and Market Checks
// ============================================================================
pub mod indicators {
    use super::*;
    pub async fn calculate_sma(
        pair: &str,
        sma_period: i32,
        state_manager: Arc<StateManager>,
    ) -> Option<Decimal> {
        let since = get_current_time()
            .checked_sub_signed(chrono::Duration::minutes(sma_period as i64))
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_default();
        match state_manager
            .get_market_data_history(pair.to_string(), since, 1000)
            .await
        {
            Ok(history) => {
                if history.len() < 5 {
                    return None;
                }
                let prices: Vec<f64> = history
                    .iter()
                    .map(|item| item.close_price.to_f64().unwrap_or(0.0))
                    .collect();
                let arr = Array1::from_vec(prices);
                let mean = arr.mean().unwrap_or(0.0);
                Decimal::from_f64(mean)
            }
            Err(e) => {
                error!("Error calculating SMA for {}: {}", pair, e);
                None
            }
        }
    }
    pub async fn calculate_atr(
        pair: &str,
        atr_period: i32,
        state_manager: Arc<StateManager>,
    ) -> Decimal {
        match state_manager
            .get_market_data_history(pair.to_string(), "".to_string(), (atr_period + 1) as i64)
            .await
        {
            Ok(history) => {
                if history.len() < (atr_period + 1) as usize {
                    return Decimal::from_str("0.0001").unwrap();
                }
                let highs: Vec<f64> = history
                    .iter()
                    .map(|item| item.high_price.to_f64().unwrap_or(0.0))
                    .collect();
                let lows: Vec<f64> = history
                    .iter()
                    .map(|item| item.low_price.to_f64().unwrap_or(0.0))
                    .collect();
                let closes: Vec<f64> = history
                    .iter()
                    .map(|item| item.close_price.to_f64().unwrap_or(0.0))
                    .collect();
                let trs: Vec<f64> = (1..highs.len())
                    .map(|i| {
                        let high_low = highs[i] - lows[i];
                        let high_prev_close = (highs[i] - closes[i - 1]).abs();
                        let low_prev_close = (lows[i] - closes[i - 1]).abs();
                        high_low.max(high_prev_close).max(low_prev_close)
                    })
                    .collect();
                let arr = Array1::from_vec(trs);
                Decimal::from_f64(arr.mean().unwrap_or(0.0001)).unwrap_or(Decimal::from_str("0.0001").unwrap())
            }
            Err(e) => {
                error!("Error calculating ATR for {}: {}", pair, e);
                Decimal::from_str("0.0001").unwrap()
            }
        }
    }
    pub async fn calculate_roc(
        pair: &str,
        roc_period: i32,
        current_price: Decimal,
        state_manager: Arc<StateManager>,
    ) -> Option<Decimal> {
        let since = get_current_time()
            .checked_sub_signed(chrono::Duration::minutes(roc_period as i64))
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_default();
        match state_manager
            .get_market_data_history(pair.to_string(), since, 1000)
            .await
        {
            Ok(history) => {
                if history.len() < 5 {
                    return None;
                }
                let past_price = history
                    .first()
                    .map(|item| item.close_price)
                    .unwrap_or(Decimal::ZERO);
                if past_price > Decimal::ZERO {
                    Some((current_price - past_price) / past_price * Decimal::from(100))
                } else {
                    Some(Decimal::ZERO)
                }
            }
            Err(e) => {
                error!("Error calculating ROC for {}: {}", pair, e);
                None
            }
        }
    }
    pub async fn market_condition_check(
        pair: &str,
        close_price: Decimal,
        config: &Config,
        state_manager: Arc<StateManager>,
    ) -> (bool, Vec<String>) {
        if !config.market_checks.conditional_buying.value {
            info!(
                "Market condition checks bypassed for {} (conditional_buying=false)",
                pair
            );
            return (true, vec![]);
        }
        let mut failed_checks = vec![];
        let min_data_records = 5;
        let sma_short = calculate_sma(
            pair,
            config.market_checks.sma_short_period.value,
            state_manager.clone(),
        )
        .await;
        let sma_long = calculate_sma(
            pair,
            config.market_checks.sma_long_period.value,
            state_manager.clone(),
        )
        .await;
        if sma_short.is_none() || sma_long.is_none() {
            failed_checks.push(format!(
                "insufficient data for SMA (<{} records)",
                min_data_records
            ));
            return (false, failed_checks);
        }
        let is_uptrend = sma_short.unwrap() > sma_long.unwrap();
        if !is_uptrend {
            failed_checks.push("downtrend (SMA short <= long)".to_string());
        }
        let roc = calculate_roc(
            pair,
            config.market_checks.roc_period.value,
            close_price,
            state_manager.clone(),
        )
        .await;
        if roc.is_none() {
            failed_checks.push(format!(
                "insufficient data for ROC (<{} records)",
                min_data_records
            ));
            return (false, failed_checks);
        }
        let roc = roc.unwrap();
        let roc_threshold = config.market_checks.roc_down_threshold.value;
        let is_momentum_ok = roc > roc_threshold;
        if !is_momentum_ok {
            failed_checks.push(format!(
                "downtrend (ROC={:.2}% < {:.2}%)",
                roc.to_f64().unwrap_or(0.0), roc_threshold.to_f64().unwrap_or(0.0)
            ));
        }
        let current_atr = calculate_atr(
            pair,
            config.market_checks.atr_period.value,
            state_manager.clone(),
        )
        .await;
        let history = state_manager
            .get_market_data_history(
                pair.to_string(),
                "".to_string(),
                config.market_checks.atr_hist_period.value as i64,
            )
            .await
            .unwrap_or_default();
        if history.len() < min_data_records {
            failed_checks.push(format!(
                "insufficient data for ATR history (<{} records)",
                min_data_records
            ));
            return (false, failed_checks);
        }
        let highs: Vec<f64> = history
            .iter()
            .map(|item| item.high_price.to_f64().unwrap_or(0.0))
            .collect();
        let lows: Vec<f64> = history
            .iter()
            .map(|item| item.low_price.to_f64().unwrap_or(0.0))
            .collect();
        let closes: Vec<f64> = history
            .iter()
            .map(|item| item.close_price.to_f64().unwrap_or(0.0))
            .collect();
        let hist_trs: Vec<f64> = (1..highs.len())
            .map(|i| {
                let high_low = highs[i] - lows[i];
                let high_prev_close = (highs[i] - closes[i - 1]).abs();
                let low_prev_close = (lows[i] - closes[i - 1]).abs();
                high_low.max(high_prev_close).max(low_prev_close)
            })
            .collect();
        let hist_atr = Array1::from_vec(hist_trs).mean().unwrap_or(0.0001);
        let hist_atr_dec = Decimal::from_f64(hist_atr).unwrap_or(Decimal::from_str("0.0001").unwrap());
        let atr_vol_multiplier = config.market_checks.atr_vol_multiplier.value;
        let is_vol_spike = current_atr > (hist_atr_dec * atr_vol_multiplier);
        if is_vol_spike {
            failed_checks.push(format!(
                "vol spike (ATR={:.4} > {:.2}x hist avg={:.4})",
                current_atr.to_f64().unwrap_or(0.0), atr_vol_multiplier.to_f64().unwrap_or(0.0), hist_atr_dec.to_f64().unwrap_or(0.0)
            ));
        }
        let good_conditions = (is_uptrend || is_momentum_ok) && !is_vol_spike;
        (good_conditions, failed_checks)
    }
}