// config.rs
// description:

// ============================================================================
// IMPORTS
// ============================================================================

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::read_to_string;
use toml;
use rust_decimal::{Decimal, prelude::*};

// ============================================================================
//
// ============================================================================

#[derive(Clone, Default, Deserialize, Debug)]
#[allow(dead_code)]
pub struct Config {
    pub api_keys: ApiKeys,
    pub portfolio: Portfolio,
    pub trading_logic: TradingLogic,
    pub stop_loss_behavior: StopLossBehavior,
    pub database: Database,
    pub market_checks: MarketChecks,
    pub bot_operation: BotOperation,
    pub price_logic: PriceLogic,
    pub delays: Delays,
    pub symbol_info: HashMap<String, SymbolInfo>,
}

#[allow(dead_code)]
impl Config {
    pub fn new() -> Self {
        Config {
            api_keys: ApiKeys::default(),
            portfolio: Portfolio::default(),
            trading_logic: TradingLogic::default(),
            stop_loss_behavior: StopLossBehavior::default(),
            database: Database::default(),
            market_checks: MarketChecks::default(),
            bot_operation: BotOperation::default(),
            price_logic: PriceLogic::default(),
            delays: Delays::default(),
            symbol_info: HashMap::new(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.api_keys.key.value.is_empty() {
            return Err(anyhow!("API key cannot be empty"));
        }
        if self.api_keys.secret.value.is_empty() {
            return Err(anyhow!("API secret cannot be empty"));
        }
        if self.portfolio.api_pairs.value.is_empty() {
            return Err(anyhow!("Portfolio API pairs cannot be empty"));
        }
        if self.portfolio.ws_pairs.value.is_empty() {
            return Err(anyhow!("Portfolio WebSocket pairs cannot be empty"));
        }
        if self.portfolio.api_pairs.value.len() != self.portfolio.ws_pairs.value.len() {
            return Err(anyhow!(
                "API pairs and WebSocket pairs must have the same length"
            ));
        }
        if self.portfolio.initial_investment.value < Decimal::ZERO {
            return Err(anyhow!("Initial investment must be non-negative"));
        }
        if self.trading_logic.dip_percentage.value <= Decimal::ZERO {
            return Err(anyhow!("Dip percentage must be positive"));
        }
        if self.trading_logic.profit_target.value < Decimal::ZERO {
            return Err(anyhow!("Profit target must be non-negative"));
        }
        if self.trading_logic.period_minutes.value <= 0 {
            return Err(anyhow!("Period minutes must be positive"));
        }
        if self.trading_logic.data_fetch_period.value <= 0 {
            return Err(anyhow!("Data fetch period must be positive"));
        }
        if self.trading_logic.trade_percentage.value <= Decimal::ZERO
            || self.trading_logic.trade_percentage.value > Decimal::from(100) {
            return Err(anyhow!("Trade percentage must be between 0 and 100"));
        }
        if self.trading_logic.loop_delay_seconds.value <= Decimal::ZERO {
            return Err(anyhow!("Loop delay seconds must be positive"));
        }
        if self.trading_logic.profit_decay_period.value <= 0 {
            return Err(anyhow!("Profit decay period must be positive"));
        }
        if self.trading_logic.take_profit_min.value < Decimal::ZERO {
            return Err(anyhow!("Take profit min must be non-negative"));
        }
        if self.trading_logic.min_notional.value <= Decimal::ZERO {
            return Err(anyhow!("Min notional must be positive"));
        }
        if self.trading_logic.default_fee_rate.value < Decimal::ZERO {
            return Err(anyhow!("Default fee rate must be non-negative"));
        }
        if self.trading_logic.liquidity_threshold.value <= Decimal::ZERO {
            return Err(anyhow!("Liquidity threshold must be positive"));
        }
        if self.trading_logic.price_modifier.value < Decimal::ZERO {
            return Err(anyhow!("Price modifier must be non-negative"));
        }
        if self.trading_logic.buy_order_timeout.value <= 0 {
            return Err(anyhow!("Buy order timeout must be positive"));
        }
        if self.trading_logic.sell_order_timeout.value <= 0 {
            return Err(anyhow!("Sell order timeout must be positive"));
        }
        if self.delays.monitor_partial_delay.value <= Decimal::ZERO {
            return Err(anyhow!("Monitor partial delay must be positive"));
        }
        if self.stop_loss_behavior.fixed_stop_initial.value < Decimal::ZERO {
            return Err(anyhow!("Fixed stop initial must be non-negative"));
        }
        if self.stop_loss_behavior.fixed_stop_min.value < Decimal::ZERO {
            return Err(anyhow!("Fixed stop min must be non-negative"));
        }
        if self.stop_loss_behavior.fixed_stop_tightening_period.value <= 0 {
            return Err(anyhow!("Fixed stop tightening period must be positive"));
        }
        if self.stop_loss_behavior.trailing_stop_percentage.value < Decimal::ZERO {
            return Err(anyhow!("Trailing stop percentage must be non-negative"));
        }
        if self.stop_loss_behavior.pause_after_stop_minutes.value < Decimal::ZERO {
            return Err(anyhow!("Pause after stop minutes must be non-negative"));
        }
        if self.database.host.value.is_empty() {
            return Err(anyhow!("Database host cannot be empty"));
        }
        if self.database.port.value <= 0 {
            return Err(anyhow!("Database port must be positive"));
        }
        if self.database.database.value.is_empty() {
            return Err(anyhow!("Database name cannot be empty"));
        }
        if self.database.user.value.is_empty() {
            return Err(anyhow!("Database user cannot be empty"));
        }
        if self.database.password.value.is_empty() {
            return Err(anyhow!("Database password cannot be empty"));
        }
        if self.database.max_connections.value <= 0 {
            return Err(anyhow!("Max connections must be positive"));
        }
        if self.database.cache_size.value <= 0 {
            return Err(anyhow!("Cache size must be positive"));
        }
        if self.database.max_retries.value <= 0 {
            return Err(anyhow!("Database max retries must be positive"));
        }
        if self.market_checks.sma_short_period.value <= 0 {
            return Err(anyhow!("SMA short period must be positive"));
        }
        if self.market_checks.sma_long_period.value <= 0 {
            return Err(anyhow!("SMA long period must be positive"));
        }
        if self.market_checks.atr_period.value <= 0 {
            return Err(anyhow!("ATR period must be positive"));
        }
        if self.market_checks.atr_hist_period.value <= 0 {
            return Err(anyhow!("ATR history period must be positive"));
        }
        if self.market_checks.atr_vol_multiplier.value < Decimal::ONE
            || self.market_checks.atr_vol_multiplier.value > Decimal::from_str("2.5").unwrap() {
            return Err(anyhow!(
                "ATR volatility multiplier must be between 1.0 and 2.5"
            ));
        }
        if self.market_checks.roc_period.value <= 0 {
            return Err(anyhow!("ROC period must be positive"));
        }
        if self.market_checks.roc_down_threshold.value > Decimal::ZERO
            || self.market_checks.roc_down_threshold.value < Decimal::from(-10) {
            return Err(anyhow!("ROC down threshold must be between -10.0 and 0.0"));
        }
        if self.delays.poll_interval.value <= Decimal::ZERO {
            return Err(anyhow!("Poll interval must be positive"));
        }
        for (api_pair, ws_pair) in self
            .portfolio
            .api_pairs
            .value
            .iter()
            .zip(self.portfolio.ws_pairs.value.iter())
        {
            if !self.symbol_info.contains_key(api_pair) {
                return Err(anyhow!("Symbol info missing for API pair: {}", api_pair));
            }
            let info = self.symbol_info.get(api_pair).unwrap();
            if info.quantity_precision < 0 {
                return Err(anyhow!(
                    "Quantity precision for {} must be non-negative",
                    api_pair
                ));
            }
            if info.price_precision < 0 {
                return Err(anyhow!(
                    "Price precision for {} must be non-negative",
                    api_pair
                ));
            }
            if info.lot_size.min_qty <= Decimal::ZERO {
                return Err(anyhow!("Min quantity for {} must be positive", api_pair));
            }
            if info.lot_size.max_qty <= info.lot_size.min_qty {
                return Err(anyhow!(
                    "Max quantity for {} must be greater than min quantity",
                    api_pair
                ));
            }
            if info.lot_size.step_size <= Decimal::ZERO {
                return Err(anyhow!("Step size for {} must be positive", api_pair));
            }
            // Require balance_key to be present and non-empty for all pairs
            match &info.balance_key {
                Some(balance_key) => {
                    if balance_key.trim().is_empty() {
                        return Err(anyhow!("Balance key for {} cannot be empty", api_pair));
                    }
                }
                None => {
                    return Err(anyhow!(
                        "Balance key (balanceKey) is required for pair {} in symbol_info",
                        api_pair
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Default, Deserialize, Debug)]
#[allow(dead_code)]
pub struct ValueWrapper<T> {
    pub value: T,
    #[serde(default)]
    pub description: String,
}

#[derive(Clone, Default, Deserialize, Debug)]
#[allow(dead_code)]
pub struct ApiKeys {
    pub key: ValueWrapper<String>,
    pub secret: ValueWrapper<String>,
}

#[derive(Clone, Default, Deserialize, Debug)]
#[allow(dead_code)]
pub struct Portfolio {
    pub api_pairs: ValueWrapper<Vec<String>>,
    pub ws_pairs: ValueWrapper<Vec<String>>,
    pub initial_investment: ValueWrapper<Decimal>,
}

#[derive(Clone, Default, Deserialize, Debug)]
#[allow(dead_code)]
pub struct TradingLogic {
    pub dip_percentage: ValueWrapper<Decimal>,
    pub profit_target: ValueWrapper<Decimal>,
    pub period_minutes: ValueWrapper<i32>,
    pub data_fetch_period: ValueWrapper<i32>,
    pub trade_percentage: ValueWrapper<Decimal>,
    pub loop_delay_seconds: ValueWrapper<Decimal>,
    pub profit_decay_period: ValueWrapper<i32>,
    pub take_profit_min: ValueWrapper<Decimal>,
    pub min_notional: ValueWrapper<Decimal>,
    pub sell_safety_cap: ValueWrapper<Decimal>,
    pub default_fee_rate: ValueWrapper<Decimal>,
    pub liquidity_threshold: ValueWrapper<Decimal>,
    pub price_modifier: ValueWrapper<Decimal>,
    pub buy_order_timeout: ValueWrapper<i32>,
    pub sell_order_timeout: ValueWrapper<i32>,
}

#[derive(Clone, Default, Deserialize, Debug)]
#[allow(dead_code)]
pub struct StopLossBehavior {
    pub fixed_stop_initial: ValueWrapper<Decimal>,
    pub fixed_stop_min: ValueWrapper<Decimal>,
    pub fixed_stop_tightening_period: ValueWrapper<i32>,
    pub trailing_stop_percentage: ValueWrapper<Decimal>,
    pub pause_after_stop_minutes: ValueWrapper<Decimal>,
}

#[derive(Clone, Default, Deserialize, Debug)]
#[allow(dead_code)]
pub struct Database {
    pub host: ValueWrapper<String>,
    pub port: ValueWrapper<i32>,
    pub database: ValueWrapper<String>,
    pub user: ValueWrapper<String>,
    pub password: ValueWrapper<String>,
    pub max_connections: ValueWrapper<i32>,
    pub cache_size: ValueWrapper<i32>,
    pub max_retries: ValueWrapper<i32>,
}

#[derive(Clone, Default, Deserialize, Debug)]
#[allow(dead_code)]
pub struct MarketChecks {
    pub conditional_buying: ValueWrapper<bool>,
    pub sma_short_period: ValueWrapper<i32>,
    pub sma_long_period: ValueWrapper<i32>,
    pub atr_period: ValueWrapper<i32>,
    pub atr_hist_period: ValueWrapper<i32>,
    pub atr_vol_multiplier: ValueWrapper<Decimal>,
    pub roc_period: ValueWrapper<i32>,
    pub roc_down_threshold: ValueWrapper<Decimal>,
}

#[derive(Clone, Default, Deserialize, Debug)]
#[allow(dead_code)]
pub struct BotOperation {
    pub fixed_stop_loss: ValueWrapper<bool>,
    pub trailing_stop_loss: ValueWrapper<bool>,
    pub buy_post_only: ValueWrapper<bool>,
    pub sell_post_only: ValueWrapper<bool>,
}

#[derive(Clone, Default, Deserialize, Debug)]
#[allow(dead_code)]
pub struct PriceLogic {
    pub buy_price: ValueWrapper<String>,
    pub sell_price: ValueWrapper<String>,
}

#[derive(Clone, Default, Deserialize, Debug)]
#[allow(dead_code)]
pub struct SymbolInfo {
    #[serde(rename = "quantityPrecision")]
    pub quantity_precision: i32,
    #[serde(rename = "pricePrecision")]
    pub price_precision: i32,
    #[serde(rename = "lotSize")]
    pub lot_size: LotSize,
    #[serde(rename = "balanceKey")]
    pub balance_key: Option<String>,
}

#[derive(Clone, Default, Deserialize, Debug)]
#[allow(dead_code)]
pub struct LotSize {
    #[serde(rename = "minQty")]
    pub min_qty: Decimal,
    #[serde(rename = "maxQty")]
    pub max_qty: Decimal,
    #[serde(rename = "stepSize")]
    pub step_size: Decimal,
}

#[derive(Clone, Default, Deserialize, Debug)]
#[allow(dead_code)]
pub struct Delays {
    pub monitor_loop_delay: ValueWrapper<Decimal>,
    pub monitor_pair_delay: ValueWrapper<Decimal>,
    pub monitor_partial_delay: ValueWrapper<Decimal>,
    pub poll_interval: ValueWrapper<Decimal>,
}

pub fn load_config() -> Result<Config> {
    let toml_str = read_to_string("config.toml").context("Failed to read config.toml")?;
    let config: Config = toml::from_str(&toml_str).context("Failed to parse config.toml")?;
    config
        .validate()
        .map_err(|e| anyhow::anyhow!("Config validation failed: {}", e))?;
    Ok(config)
}