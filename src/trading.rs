// trading.rs
// description:

// Imports

use crate::api::KrakenClient;
use crate::config::{Config, SymbolInfo};
use crate::statemanager::{
    BuyMessage, MarketData, PriceData, SellMessage,StateManager, Trade,};
use crate::stop::{check_stop_loss, StopType};
use crate::{get_current_time, parse_est_timestamp};

use crate::monitor::monitor_order;
use anyhow::{anyhow, Result};
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Instant};
use tokio::sync::oneshot;
use tokio::time::Duration;
use tracing::{error, info, warn};
use evalexpr::{eval_float_with_context, HashMapContext, Value};
use evalexpr::ContextWithMutableVariables;
use crate::indicators::indicators::market_condition_check;
use crate::statemanager::OrderComplete;
use tokio::sync::mpsc;
use tokio::sync::broadcast;
use crate::utils::report_log;
use std::sync::atomic::{AtomicU64, Ordering};

// Trading Logic

pub mod trading_logic {
    use super::*;
    use std::f64::consts::E;
    pub fn get_balance_key_for_pair(pair: &str, config: &Config) -> String {
        if let Some(symbol_info) = config.symbol_info.get(pair) {
            if let Some(balance_key) = &symbol_info.balance_key {
                return balance_key.clone();
            }
        }
        let extracted_key = pair
            .trim_end_matches("USD")
            .trim_end_matches('Z')
            .to_string();
        warn!("No balance key configured for {} in config, using extracted key: {} -> {} (add balanceKey to config for proper mapping)",
              pair, pair, extracted_key);
        extracted_key
    }
    pub async fn fetch_and_store_market_data(
        pair: &str,
        client: &KrakenClient,
        config: &Config,
        state_manager: Arc<StateManager>,
    ) -> Result<(), anyhow::Error> {
        let (mut market_data, _recent_timestamp) =
            state_manager.get_market_data(pair.to_string()).await?;
        let timestamp_secs = parse_est_timestamp(&market_data.timestamp)
            .map(|dt| (chrono::Local::now() - dt).num_seconds())
            .unwrap_or(i64::MAX);
        if timestamp_secs <= config.trading_logic.data_fetch_period.value as i64 {
            let _open_p = market_data.open_price;
            let _high_p = market_data.high_price;
            let _low_p = market_data.low_price;
            let _close_p = market_data.close_price;
            let _vol_p = market_data.volume;
            let _bid_p = market_data.bid_price;
            let _ask_p = market_data.ask_price;
            if timestamp_secs <= config.trading_logic.data_fetch_period.value as i64 {
                info!(
                    "Using WebSocket OHLC data for {}, timestamp: {}",
                    pair, market_data.timestamp
                );
                state_manager.store_market_data(market_data.clone()).await?;
                return Ok(());
            } else {
                info!(
                    "WebSocket data for {} too old ({}s > {}s), fetching from REST API",
                    pair, timestamp_secs, config.trading_logic.data_fetch_period.value
                );
            }
        }
        let ohlc_result = client
            .fetch_market_data(pair, 1)
            .await;
        let order_book_result = client.fetch_order_book(pair, 10).await;
        market_data.timestamp = get_current_time().format("%Y-%m-%d %H:%M:%S").to_string();
        let mut has_ohlc = false;
        let mut fallback_close = Decimal::ZERO;
        if let Ok(data) = ohlc_result {
            let ohlc = data
                .as_array()
                .ok_or_else(|| anyhow!("Invalid OHLC data for {}", pair))?;
            if ohlc.is_empty() {
                error!("Empty OHLC data for {}", pair);
            } else {
                let latest = ohlc
                    .last()
                    .ok_or_else(|| anyhow!("Empty OHLC data for {}", pair))?;
                let open = latest[1]
                    .as_str()
                    .and_then(|s| Decimal::from_str(s).ok())
                    .unwrap_or(Decimal::ZERO);
                let high = latest[2]
                    .as_str()
                    .and_then(|s| Decimal::from_str(s).ok())
                    .unwrap_or(Decimal::ZERO);
                let low = latest[3]
                    .as_str()
                    .and_then(|s| Decimal::from_str(s).ok())
                    .unwrap_or(Decimal::ZERO);
                let close = latest[4]
                    .as_str()
                    .and_then(|s| Decimal::from_str(s).ok())
                    .unwrap_or(Decimal::ZERO);
                let volume = latest[5]
                    .as_str()
                    .and_then(|s| Decimal::from_str(s).ok())
                    .unwrap_or(Decimal::ZERO);
                if close > Decimal::ZERO && (high > low || volume == Decimal::ZERO) {
                    market_data.open_price = open;
                    market_data.high_price = high;
                    market_data.low_price = low;
                    market_data.close_price = close;
                    market_data.volume = volume;
                    market_data.highest_price_period = high;
                    has_ohlc = true;
                    fallback_close = close;
                } else {
                    error!("Invalid OHLC values for {}: open={}, high={}, low={}, close={}, volume={}",
                        pair, open, high, low, close, volume
                    );
                }
            }
        } else {
            error!(
                "Failed to fetch OHLC data for {}: {:?}",
                pair,
                ohlc_result.unwrap_err()
            );
        }
        let mut has_order_book = false;
        if let Ok(order_book) = order_book_result {
            let empty_vec = vec![];
            let bids = order_book["bids"].as_array().unwrap_or(&empty_vec);
            let asks = order_book["asks"].as_array().unwrap_or(&empty_vec);
            let bid_price = bids
                .get(0)
                .and_then(|b| b[0].as_str())
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(Decimal::ZERO);
            let ask_price = asks
                .get(0)
                .and_then(|a| a[0].as_str())
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(Decimal::ZERO);
            let bid_depth: Decimal = bids
                .iter()
                .filter_map(|b| b[1].as_str().and_then(|s| Decimal::from_str(s).ok()))
                .sum();
            let ask_depth: Decimal = asks
                .iter()
                .filter_map(|a| a[1].as_str().and_then(|s| Decimal::from_str(s).ok()))
                .sum();
            let liquidity = (bid_depth * bid_price + ask_depth * ask_price).min(Decimal::from_str("1000000.0").unwrap());
            if bid_price > Decimal::ZERO
                && ask_price > Decimal::ZERO
                && bid_depth > Decimal::ZERO
                && ask_depth > Decimal::ZERO
                && liquidity > Decimal::ZERO
            {
                market_data.bid_price = bid_price;
                market_data.ask_price = ask_price;
                market_data.bid_depth = bid_depth;
                market_data.ask_depth = ask_depth;
                market_data.liquidity = liquidity;
                has_order_book = true;
                let close_price = if fallback_close > Decimal::ZERO {
                    fallback_close
                } else if bid_price > Decimal::ZERO && ask_price > Decimal::ZERO {
                    (bid_price + ask_price) / Decimal::from(2)
                } else {
                    Decimal::ZERO
                };
                if close_price > Decimal::ZERO {
                    market_data.close_price = close_price;
                    has_ohlc = true;
                    market_data.high_price = market_data.high_price.max(close_price);
                    market_data.low_price = market_data.low_price.min(close_price);
                }
                let price_data = PriceData::new(close_price, bid_price, ask_price);
                if let Err(e) = state_manager
                    .update_price(pair.to_string(), price_data)
                    .await
                {
                    error!("Failed to update price data with bid/ask for {}: {}", pair, e);
                }
            } else {
                error!("Invalid order book values for {}: bid_price={}, ask_price={}, bid_depth={}, ask_depth={}, liquidity={}", pair, bid_price, ask_price, bid_depth, ask_depth, liquidity);
            }
        } else {
            error!(
                "Failed to fetch order book data for {}: {:?}",
                pair,
                order_book_result.unwrap_err()
            );
        }
        if has_ohlc && has_order_book {
            state_manager.store_market_data(market_data.clone()).await?;
            Ok(())
        } else {
            Err(anyhow!(
                "Incomplete market data for {}: has_ohlc={}, has_order_book={}",
                pair,
                has_ohlc,
                has_order_book
            ))
        }
    }
    pub async fn adjust_quantity(
        pair: &str,
        qty: Decimal,
        price: Decimal,
        symbol_info: &SymbolInfo,
        min_notional_config: Decimal,
    ) -> Option<Decimal> {
        let lot_size = &symbol_info.lot_size;
        let min_qty = lot_size.min_qty;
        let max_qty = lot_size.max_qty;
        let step_size = lot_size.step_size;
        let quantity_precision = symbol_info.quantity_precision as u32;
        let min_notional = min_notional_config;
        info!("Adjust quantity for {}: input_qty={:.8}, price={:.8}, min_qty={:.8}, step_size={:.8}, quantity_precision={}, min_notional={:.8}",
            pair, qty.to_f64().unwrap_or(0.0), price.to_f64().unwrap_or(0.0), min_qty.to_f64().unwrap_or(0.0), step_size.to_f64().unwrap_or(0.0), quantity_precision, min_notional.to_f64().unwrap_or(0.0));
        let adjusted_qty = (qty / step_size).floor() * step_size;
        let adjusted_qty = adjusted_qty.max(min_qty).min(max_qty);
        let adjusted_qty = adjusted_qty.round_dp(quantity_precision);
        let trade_value = adjusted_qty * price;
        info!(
            "Adjusted qty={:.8}, trade_value={:.8}",
            adjusted_qty.to_f64().unwrap_or(0.0), trade_value.to_f64().unwrap_or(0.0)
        );
        if trade_value < min_notional {
            warn!(
                "Trade value {:.2} USD for {} below min_notional {:.2} USD",
                trade_value.to_f64().unwrap_or(0.0), pair, min_notional.to_f64().unwrap_or(0.0)
            );
            return None;
        }
        Some(adjusted_qty)
    }
    pub async fn buy_logic(
        pair: &str,
        state_manager: Arc<StateManager>,
        client: &KrakenClient,
        config: &Config,
        completion_rx: &mut mpsc::UnboundedReceiver<OrderComplete>,
        shutdown_tx: broadcast::Sender<()>,
    ) -> Result<(bool, Decimal), anyhow::Error> {
        info!("Entering buy_logic for {}", pair);
        let pause_minutes = config.stop_loss_behavior.pause_after_stop_minutes.value;
        if pause_minutes > Decimal::ZERO {
            let latest_stop_ts = state_manager.get_latest_stop_loss_timestamp(pair.to_string()).await?;
            if let Some(ts) = latest_stop_ts {
                if let Some(parsed_ts) = parse_est_timestamp(&ts) {
                    let minutes_since = (get_current_time() - parsed_ts).num_minutes();
                    if minutes_since < pause_minutes.to_i64().unwrap_or(0) {
                        info!("Buy paused for {} due to stop-loss at {}", pair, ts);
                        let position = state_manager.get_position(pair.to_string()).await?;
                        return Ok((false, position.usd_balance));
                    }
                } else {
                    warn!("Failed to parse stop-loss timestamp {} for {}", ts, pair);
                }
            }
        }
        let position = state_manager.get_position(pair.to_string()).await?;
        let price_data = state_manager.get_price(pair.to_string()).await?;
        let close_price = price_data.close_price;
        if close_price == Decimal::ZERO {
            warn!("No price data for {}, skipping buy logic", pair);
            return Ok((false, position.usd_balance));
        }
        let (good_conditions, failed_checks) =
            market_condition_check(pair, close_price, config, state_manager.clone()).await;
        if !good_conditions {
            info!(
                "Buy skipped for {}: failed market conditions - {}",
                pair,
                failed_checks.join(", ")
            );
            return Ok((false, position.usd_balance));
        }
        let symbol_info = config
            .symbol_info
            .get(pair)
            .ok_or_else(|| anyhow!("Symbol {} not found in config", pair))?;
        let trade_percentage = config.trading_logic.trade_percentage.value;
        let usd_available = state_manager.get_usd_balance_locked().await?;
        let trade_usd = usd_available * (trade_percentage / Decimal::from(100));
        if trade_usd <= config.trading_logic.min_notional.value {
            info!(
                "Buy skipped for {}: trade_usd={:.2} <= min_notional={:.2}",
                pair, trade_usd.to_f64().unwrap_or(0.0), config.trading_logic.min_notional.value.to_f64().unwrap_or(0.0)
            );
            return Ok((false, position.usd_balance));
        }
        let highest_price = get_highest_price_period(
            client,
            pair,
            config.trading_logic.period_minutes.value,
            close_price,
            state_manager.clone(),
        )
        .await;
        let buy_threshold =
            highest_price * (Decimal::ONE - config.trading_logic.dip_percentage.value / Decimal::from(100));
        info!(
            "Buy logic for {}: close_price={:.6}, highest_price={:.6}, buy_threshold={:.6}",
            pair, close_price.to_f64().unwrap_or(0.0), highest_price.to_f64().unwrap_or(0.0), buy_threshold.to_f64().unwrap_or(0.0)
        );
        if close_price >= buy_threshold {
            info!(
                "Buy skipped for {}: close_price={:.6} >= buy_threshold={:.6}",
                pair, close_price.to_f64().unwrap_or(0.0), buy_threshold.to_f64().unwrap_or(0.0)
            );
            return Ok((false, position.usd_balance));
        }
        let amount = adjust_quantity(
            pair,
            trade_usd / close_price,
            close_price,
            symbol_info,
            config.trading_logic.min_notional.value,
        )
        .await;
        if amount.is_none() {
            warn!("Buy skipped for {}: adjusted amount is None", pair);
            return Ok((false, position.usd_balance));
        }
        let amount = amount.unwrap();
        let result = execute_buy_trade(
            client,
            pair,
            amount,
            "dip",
            Decimal::ZERO,
            symbol_info,
            close_price,
            config,
            state_manager.clone(),
            shutdown_tx.clone(),
        ).await?;
        let triggered = result.0.is_some();
        if triggered {
            // Await completion signal
            if let Some(sig) = completion_rx.recv().await {
                match sig {
                    OrderComplete::Success(_) => info!("Buy order for {} completed successfully", pair),
                    OrderComplete::Error(e) => error!("Buy order for {} failed: {}", pair, e),
                    OrderComplete::Shutdown => return Err(anyhow!("Shutdown during buy for {}", pair)),
                    _ => {}
                }
            } else {
                error!("Completion channel closed during buy await for {}", pair);
            }
        }
        Ok((
            triggered,
            position.usd_balance - (result.5 * result.1 + result.7),
        ))
    }
    pub async fn sell_logic(
        pair: &str,
        state_manager: Arc<StateManager>,
        client: &KrakenClient,
        config: &Config,
        completion_rx: &mut mpsc::UnboundedReceiver<OrderComplete>,
        shutdown_tx: broadcast::Sender<()>,
    ) -> Result<bool, anyhow::Error> {
        info!("Entering sell_logic for {}", pair);
        let mut open_trades = state_manager.get_open_trades(pair.to_string()).await?;
        info!("Found {} open trades for {}", open_trades.len(), pair);
        if open_trades.is_empty() {
            info!("Sell skipped for {}: no open trades", pair);
            return Ok(false);
        }
        let price_data = state_manager.get_price(pair.to_string()).await?;
        let close_price = price_data.close_price;
        if close_price == Decimal::ZERO {
            warn!("No price data for {}, skipping sell logic", pair);
            return Ok(false);
        }
        let symbol_info = config
            .symbol_info
            .get(pair)
            .ok_or_else(|| anyhow!("Symbol {} not found in config", pair))?;
        let mut recent_sells: HashMap<String, (String, Instant)> = HashMap::new();
        let now = Instant::now();
        recent_sells.retain(|_, (_, add_time)| now.duration_since(*add_time) < Duration::from_secs(60));
        // Explicitly sort by timestamp ASC (though DB should already order)
        open_trades.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        let mut open_only_trades: Vec<_> = open_trades.iter().filter(|t| t.open == 1).cloned().collect();
        open_only_trades.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        let mut triggered_count = 0;
        for trade in open_only_trades {
            // Refetch fresh open_trades for each evaluation
            let fresh_open_trades = state_manager.get_open_trades(pair.to_string()).await?;
            match evaluate_trade(
                &trade,
                pair,
                close_price,
                config,
                state_manager.clone(),
                client,
                symbol_info,
                &fresh_open_trades,  // Pass fresh trades
            )
            .await
            {
                Ok(Some((trade_id, trade_ids_to_sell, amount, reason, avg_cost_basis, total_buy_qty))) => {
                    let mut skip_race = false;
                    for buy_id in &trade_ids_to_sell {
                        if let Some((orig_buy, add_time)) = recent_sells.get(buy_id) {
                            if now.duration_since(*add_time) < Duration::from_secs(60) {
                                warn!("skipped_race: {} (recent sell on {})", buy_id, orig_buy);
                                skip_race = true;
                                break;
                            }
                        }
                    }
                    if skip_race {
                        continue;
                    }
                    info!(
                        "Processing sell for {}, trade_id={}, amount={:.4}, reason={}",
                        pair, trade_id, amount.to_f64().unwrap_or(0.0), reason
                    );
                    let result = execute_sell_trade(
                        client.clone(),
                        pair.to_string(),
                        amount,
                        reason.clone(),
                        avg_cost_basis,
                        symbol_info.clone(),
                        close_price,
                        config.clone(),
                        state_manager.clone(),
                        trade_ids_to_sell.clone(),
                        total_buy_qty,
                        shutdown_tx.clone(),
                    )
                    .await;
                    match result {
                        Ok(Some(_)) => {
                            triggered_count += 1;
                            if let Some(first_buy) = trade_ids_to_sell.first() {
                                recent_sells.insert(first_buy.clone(), (trade_id.clone(), Instant::now()));
                            }
                        }
                        Ok(None) => {
                            warn!("Sell order failed for {}", pair);
                        }
                        Err(e) => {
                            error!("Sell execution failed for {}: {}", pair, e);
                        }
                    }
                }
                Ok(None) => {
                    info!("Sell skipped for {}: no trades triggered", pair);
                }
                Err(e) => error!("Trade evaluation failed: {}", e),
            }
        }
        // After all placements, await completions to allow overlap
        let mut triggered = triggered_count > 0;
        for _ in 0..triggered_count {
            if let Some(sig) = completion_rx.recv().await {
                match sig {
                    OrderComplete::Success(_) => info!("Sell order for {} completed successfully", pair),
                    OrderComplete::Error(e) => error!("Sell order for {} failed: {}", pair, e),
                    OrderComplete::Shutdown => return Err(anyhow!("Shutdown during sell for {}", pair)),
                    _ => {}
                }
            } else {
                error!("Completion channel closed during sell await for {}", pair);
                triggered = false;
            }
        }
        Ok(triggered)
    }
    pub async fn evaluate_trade(
        trade: &Trade,
        pair: &str,
        close_price: Decimal,
        config: &Config,
        state_manager: Arc<StateManager>,
        _client: &KrakenClient,
        symbol_info: &SymbolInfo,
        open_trades: &Vec<Trade>,  // Updated: Pass fresh open_trades
    ) -> Result<Option<(String, Vec<String>, Decimal, String, Decimal, Decimal)>, anyhow::Error> {
        let trade_id = trade.trade_id.clone();
        let remaining_qty = trade.remaining_amount;
        if trade.open == 0 || remaining_qty <= Decimal::new(1, 8) {
            return Ok(None);
        }
        let buy_time = parse_est_timestamp(&trade.timestamp);
        let avg_cost_basis = trade.avg_cost_basis;
        let minutes_since_buy = buy_time
            .map(|bt| (get_current_time() - bt).num_minutes() as f64)
            .unwrap_or(0.0);
        let decay_constant = if config.trading_logic.profit_target.value
            > config.trading_logic.take_profit_min.value
        {
            config.trading_logic.profit_decay_period.value as f64
                / (config.trading_logic.profit_target.value.to_f64().unwrap_or(0.0)
                    / config.trading_logic.take_profit_min.value.to_f64().unwrap_or(0.0))
                    .ln()
        } else {
            120.0
        };
        let decay_rate = minutes_since_buy / decay_constant;
        let current_profit_target = config
            .trading_logic
            .take_profit_min
            .value
            .max(config.trading_logic.profit_target.value * Decimal::from_f64(E.powf(-decay_rate)).unwrap_or(Decimal::ZERO));
        let profit_threshold =
            avg_cost_basis * (Decimal::ONE + current_profit_target / Decimal::from(100));
        let history = state_manager
            .get_market_data_history(pair.to_string(), trade.timestamp.clone(), 1000)
            .await?;
        let highest_price_since_buy = history
            .iter()
            .map(|item| item.high_price)
            .fold(close_price, Decimal::max);
        let stop_type = check_stop_loss(
            &trade.timestamp,
            avg_cost_basis,
            close_price,
            highest_price_since_buy,
            config,
        ).await;
        let mut reason = None;
        if let Some(st) = stop_type {
            reason = Some(match st {
                StopType::Fixed => "fixed_stop".to_string(),
                StopType::Trailing => "trailing_stop".to_string(),
            });
        } else if close_price >= profit_threshold {
            reason = Some("profit".to_string());
        }
        if reason.is_none() {
            info!("Sell skipped for trade_id={} on {}: no profit/stop threshold met (close_price={:.6}, profit_threshold={:.6}, highest_since_buy={:.6})",
                trade_id, pair, close_price.to_f64().unwrap_or(0.0), profit_threshold.to_f64().unwrap_or(0.0), highest_price_since_buy.to_f64().unwrap_or(0.0));
            return Ok(None);
        }
        let reason = reason.unwrap();
        info!("Evaluated sell trigger for trade_id={} on {}: reason={}, close_price={:.6}, profit_threshold={:.6}, highest_since_buy={:.6}",
            trade_id, pair, reason, close_price.to_f64().unwrap_or(0.0), profit_threshold.to_f64().unwrap_or(0.0), highest_price_since_buy.to_f64().unwrap_or(0.0));
        // Use passed fresh open_trades instead of refetching
        let partial_trades: Vec<_> = open_trades
            .iter()
            .filter(|t| {
                t.partial_open == 1 && t.trade_type == "buy" && t.timestamp < trade.timestamp
            })
            .collect();
        let mut total_amount = Decimal::ZERO;
        let mut trade_ids_to_sell = vec![];
        let mut weighted_buy_price = Decimal::ZERO;
        let mut total_weight = Decimal::ZERO;
        for partial_trade in partial_trades {
            let partial_qty = partial_trade.remaining_amount;
            if partial_qty > Decimal::ZERO {
                total_amount += partial_qty;
                trade_ids_to_sell.push(partial_trade.trade_id.clone());
                let partial_price = partial_trade.avg_cost_basis;
                weighted_buy_price += partial_price * partial_qty;
                total_weight += partial_qty;
            }
        }
        if remaining_qty > Decimal::ZERO {
            total_amount += remaining_qty;
            trade_ids_to_sell.push(trade_id.clone());
            weighted_buy_price += avg_cost_basis * remaining_qty;
            total_weight += remaining_qty;
        }
        if total_weight <= Decimal::ZERO {
            return Ok(None);
        }
        let avg_cost_basis = weighted_buy_price / total_weight;
        let amount = adjust_quantity(pair, total_amount, close_price, symbol_info, Decimal::ZERO).await;
        if amount.is_none() {
            return Ok(None);
        }
        let amount = amount.unwrap();
        Ok(Some((
            trade_id,
            trade_ids_to_sell,
            amount,
            reason,
            avg_cost_basis,
            total_weight,
        )))
    }
    pub async fn execute_buy_trade(
        client: &KrakenClient,
        pair: &str,
        amount: Decimal,
        reason: &str,
        avg_cost_basis: Decimal,
        symbol_info: &SymbolInfo,
        close_price: Decimal,
        config: &Config,
        state_manager: Arc<StateManager>,
        shutdown_tx: broadcast::Sender<()>,
    ) -> Result<
        (
            Option<String>,
            Decimal,
            Decimal,
            Decimal,
            String,
            Decimal,
            Option<String>,
            Decimal,
            Decimal,
            Decimal,
        ),
        anyhow::Error,
    > {
        let base_currency = pair.split("USD").next().unwrap_or("");
        let timeout = Decimal::from(config.trading_logic.buy_order_timeout.value);
        let post_only = config.bot_operation.buy_post_only.value;
        info!(
            "Executing buy_trade for {}, amount={:.4}, reason={}",
            pair, amount.to_f64().unwrap_or(0.0), reason
        );
        let usd_position = state_manager.get_position("USD".to_string()).await?;
        let usd_available = state_manager.get_usd_balance_locked().await?;
        let mut context = HashMapContext::new();
        context.set_value("close_price".to_string(), Value::Float(close_price.to_f64().unwrap_or(0.0))).unwrap();
        context.set_value("price_modifier".to_string(), Value::Float(config.trading_logic.price_modifier.value.to_f64().unwrap_or(0.0))).unwrap();
        let formula = &config.price_logic.buy_price.value;
        let limit_price_res = eval_float_with_context(formula, &context);
        let limit_price = match limit_price_res {
            Ok(price) => Decimal::from_f64(price).unwrap_or(Decimal::ZERO),
            Err(e) => {
                error!("Failed to evaluate buy price formula for {}: {}. Using default.", pair, e);
                close_price * (Decimal::ONE - config.trading_logic.price_modifier.value)
            }
        };
        let price_precision = symbol_info.price_precision as u32;
        let limit_price = limit_price.round_dp(price_precision);
        let trade_value = amount * limit_price;
        let estimated_fee = trade_value * config.trading_logic.default_fee_rate.value;
        if trade_value + estimated_fee > usd_available {
            warn!(
                "Insufficient USD balance for buy {}: required={:.2}, available={:.2}",
                pair,
                (trade_value + estimated_fee).to_f64().unwrap_or(0.0),
                usd_available.to_f64().unwrap_or(0.0)
            );
            return Ok((
                None,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
                "".to_string(),
                Decimal::ZERO,
                None,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
            ));
        }
        info!(
            "Executing buy for {}: amount={:.8} {}, limit_price={:.8}",
            pair, amount.to_f64().unwrap_or(0.0), base_currency, limit_price.to_f64().unwrap_or(0.0)
        );
        let (tx, rx) = oneshot::channel();
        state_manager
            .execute_buy_tx
            .send(BuyMessage::Execute {
                pair: pair.to_string(),
                amount,
                limit_price,
                post_only,
                timeout: timeout.to_f64().unwrap_or(0.0),
                symbol: pair.to_string(),
                reason: reason.to_string(),
                reply: tx,
            })
            .await?;
        let order_id_opt = rx.await?;
        if let Some(order_id) = order_id_opt {
            info!("Triggered buy for order {} on {}", order_id, pair);
            let order_id_clone = order_id.clone();
            let client_clone = client.clone();
            let state_manager_clone = state_manager.clone();
            let config_clone = config.clone();
            let pair_clone = pair.to_string();
            let shutdown_tx_clone = shutdown_tx.clone();
            if state_manager.is_monitoring(order_id_clone.clone()).await.unwrap_or(true) {
                info!("Skipping monitor spawn for already monitoring order {}", order_id_clone);
                return Ok((Some(order_id.clone()), Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, base_currency.to_string(), Decimal::ZERO, None, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO));
            }
            tokio::spawn(async move {
                let shutdown_rx = shutdown_tx_clone.subscribe();
                match monitor_order(
                    &client_clone,
                    vec![pair_clone.clone()],
                    state_manager_clone.clone(),
                    &config_clone,
                    order_id_clone,
                    avg_cost_basis,
                    vec![],  // or buy_trade_ids_clone for sell
                    Decimal::ZERO,  // or total_buy_qty_clone for sell
                    shutdown_rx,
                ).await {
                    Ok((filled, _, _, _, _)) => {
                        let _ = state_manager_clone.send_completion(pair_clone, OrderComplete::Success(filled)).await;
                    }
                    Err(e) => {
                        let _ = state_manager_clone.send_completion(pair_clone, OrderComplete::Error(Arc::new(e))).await;
                    }
                }
            });
            Ok((
                Some(order_id),
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
                base_currency.to_string(),
                Decimal::ZERO,
                None,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
            ))
        } else {
            warn!("ExecuteBuy Actor returned no order_id for {}", pair);
            Ok((
                None,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
                "".to_string(),
                Decimal::ZERO,
                None,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
            ))
        }
    }
    pub async fn execute_sell_trade(
        client: KrakenClient,
        pair: String,
        amount: Decimal,
        reason: String,
        avg_cost_basis: Decimal,
        symbol_info: SymbolInfo,
        close_price: Decimal,
        config: Config,
        state_manager: Arc<StateManager>,
        buy_trade_ids: Vec<String>,
        total_buy_qty: Decimal,
        shutdown_tx: broadcast::Sender<()>,
    ) -> Result<
        Option<(
            String,
            Decimal,
            Decimal,
            Decimal,
            String,
            Decimal,
            Option<String>,
            Decimal,
            Decimal,
            Vec<String>,
            Decimal,
        )>,
        anyhow::Error,
    > {
        let base_currency = pair.split("USD").next().unwrap_or("");
        info!(
            "Starting execute_sell_trade for {}, amount={:.4}, reason={}",
            pair, amount.to_f64().unwrap_or(0.0), reason
        );
        let amount = adjust_quantity(&pair, amount, close_price, &symbol_info, Decimal::ZERO).await;
        if amount.is_none() {
            warn!("Adjusted sell amount for {} is invalid", pair);
            return Ok(None);
        }
        let amount = amount.unwrap();
        if amount <= Decimal::ZERO {
            warn!("Sell amount <=0 for {}", pair);
            return Ok(None);
        }
        let mut context = HashMapContext::new();
        context.set_value("close_price".to_string(), Value::Float(close_price.to_f64().unwrap_or(0.0))).unwrap();
        context.set_value("price_modifier".to_string(), Value::Float(config.trading_logic.price_modifier.value.to_f64().unwrap_or(0.0))).unwrap();
        let formula = &config.price_logic.sell_price.value;
        let limit_price_res = eval_float_with_context(formula, &context);
        let limit_price = match limit_price_res {
            Ok(price) => Decimal::from_f64(price).unwrap_or(Decimal::ZERO),
            Err(e) => {
                error!("Failed to evaluate sell price formula for {}: {}. Using default.", pair, e);
                close_price * (Decimal::ONE - config.trading_logic.price_modifier.value)
            }
        };
        let price_precision = symbol_info.price_precision as u32;
        let limit_price = limit_price.round_dp(price_precision);
        let mut order_id: Option<String> = None;
        let post_only = config.bot_operation.sell_post_only.value;
        let timeout = config.trading_logic.sell_order_timeout.value as f64;
        info!("Executing sell for {}: amount={:.8} {}, limit_price={:.8}, post_only={}", pair, amount.to_f64().unwrap_or(0.0), base_currency, limit_price.to_f64().unwrap_or(0.0), post_only);
        info!("Decision close_price for sell on {}: {:.6}", pair, close_price.to_f64().unwrap_or(0.0));
        let (tx, rx) = oneshot::channel();
        state_manager
            .execute_sell_tx
            .send(SellMessage::Execute {
                pair: pair.clone(),
                amount,
                limit_price,
                post_only,
                timeout,
                symbol: pair.to_string(),
                reason: reason.clone(),
                buy_trade_ids: buy_trade_ids.clone(),
                total_buy_qty,
                reply: tx,
            })
            .await?;
        // In execute_sell_trade, replace the spawn block with:
        let order_id_opt = rx.await?;
        if let Some(order_id) = order_id_opt {
            info!("Triggered sell actor for order {} on {}", order_id, pair);
            let order_id_clone = order_id.clone();
            let client_clone = client.clone();
            let state_manager_clone = state_manager.clone();
            let config_clone = config.clone();
            let pair_clone = pair.clone();
            let buy_trade_ids_clone = buy_trade_ids.clone();
            let total_buy_qty_clone = total_buy_qty;
            let shutdown_tx_clone = shutdown_tx.clone();
            if state_manager.is_monitoring(order_id_clone.clone()).await.unwrap_or(true) {
                info!("Skipping monitor spawn for already monitoring order {}", order_id_clone);
                return Ok(Some((order_id.clone(), Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, base_currency.to_string(), Decimal::ZERO, None, Decimal::ZERO, Decimal::ZERO, vec![], total_buy_qty)));
            }
            tokio::spawn(async move {
                let shutdown_rx = shutdown_tx_clone.subscribe();
                match monitor_order(
                    &client_clone,
                    vec![pair_clone.clone()],
                    state_manager_clone.clone(),
                    &config_clone,
                    order_id_clone,
                    avg_cost_basis,
                    buy_trade_ids_clone,
                    total_buy_qty_clone,
                    shutdown_rx,
                ).await {
                    Ok((filled, _, _, _, _)) => {
                        let _ = state_manager_clone.send_completion(pair_clone, OrderComplete::Success(filled)).await;
                    }
                    Err(e) => {
                        let _ = state_manager_clone.send_completion(pair_clone, OrderComplete::Error(Arc::new(e))).await;
                    }
                }
            });
            Ok(Some((
                order_id,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
                base_currency.to_string(),
                Decimal::ZERO,
                None,
                Decimal::ZERO,
                Decimal::ZERO,
                vec![],
                total_buy_qty,
            )))
        } else {
            warn!("ExecuteSell Actor returned no order_id for {}", pair);
            Ok(None)
        }
    }
    pub async fn get_highest_price_period(
        client: &KrakenClient,
        pair: &str,
        period_minutes: i32,
        current_price: Decimal,
        state_manager: Arc<StateManager>,
    ) -> Decimal {
        let since = get_current_time()
            .checked_sub_signed(chrono::Duration::minutes(period_minutes as i64))
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_default();
        match state_manager
            .get_market_data_history(pair.to_string(), since, 1000)
            .await
        {
            Ok(history) => {
                if history.is_empty() {
                    warn!(
                        "No market data history for {}, fetching from REST API",
                        pair
                    );
                    let symbol = pair;
                    match client.fetch_ticker(&symbol).await {
                        Ok(ticker) => {
                            let high_price = ticker
                                .get("h")
                                .and_then(|v| v[1].as_str().and_then(|s| Decimal::from_str(s).ok()))
                                .unwrap_or(current_price);
                            let market_data = MarketData {
                                pair: pair.to_string(),
                                timestamp: get_current_time()
                                    .format("%Y-%m-%d %H:%M:%S")
                                    .to_string(),
                                high_price: high_price,
                                low_price: ticker
                                    .get("l")
                                    .and_then(|v| v[1].as_str().and_then(|s| Decimal::from_str(s).ok()))
                                    .unwrap_or(Decimal::ZERO),
                                close_price: ticker
                                    .get("c")
                                    .and_then(|v| v[0].as_str().and_then(|s| Decimal::from_str(s).ok()))
                                    .unwrap_or(Decimal::ZERO),
                                open_price: ticker
                                    .get("o")
                                    .and_then(|v| v.as_str().and_then(|s| Decimal::from_str(s).ok()))
                                    .unwrap_or(Decimal::ZERO),
                                highest_price_period: high_price,
                                volume: ticker
                                    .get("v")
                                    .and_then(|v| v[1].as_str().and_then(|s| Decimal::from_str(s).ok()))
                                    .unwrap_or(Decimal::ZERO),
                                bid_price: ticker
                                    .get("b")
                                    .and_then(|v| v[0].as_str().and_then(|s| Decimal::from_str(s).ok()))
                                    .unwrap_or(Decimal::ZERO),
                                ask_price: ticker
                                    .get("a")
                                    .and_then(|v| v[0].as_str().and_then(|s| Decimal::from_str(s).ok()))
                                    .unwrap_or(Decimal::ZERO),
                                bid_depth: Decimal::ZERO,
                                ask_depth: Decimal::ZERO,
                                liquidity: Decimal::ZERO,
                            };
                            state_manager
                                .store_market_data(market_data)
                                .await
                                .unwrap_or_else(|e| error!("Failed to store market data: {}", e));
                            high_price
                        }
                        Err(e) => {
                            error!(
                                "Error fetching ticker for {}: {}, using current price: {}",
                                pair, e, current_price
                            );
                            current_price
                        }
                    }
                } else {
                    let recent_highs = history
                        .iter()
                        .map(|item| item.high_price)
                        .collect::<Vec<_>>();
                    let max_high = recent_highs.iter().fold(current_price, |a, &b| a.max(b));
                    max_high
                }
            }
            Err(e) => {
                error!(
                    "Error querying market data history for {}: {}, using current price: {}",
                    pair, e, current_price
                );
                current_price
            }
        }
    }
    pub async fn update_remaining_quantities(
        pair: &str,
        net_qty: Decimal,
        trade_ids_to_sell: Vec<String>,
        state_manager: Arc<StateManager>,
        config: &Config,
        base_currency: &str,
    ) -> Result<(), anyhow::Error> {
        let mut remaining_sell_qty = net_qty;
        let min_notional = config.trading_logic.min_notional.value;
        let open_trades = state_manager.get_open_trades(pair.to_string()).await?;
        let price_data = state_manager.get_price(pair.to_string()).await?;
        let close_price = price_data.close_price;
        let mut updates = Vec::new();
        for trade_id in trade_ids_to_sell.iter() {
            if remaining_sell_qty <= Decimal::ZERO {
                break;
            }
            let trade = open_trades
                .iter()
                .find(|t| t.trade_id == *trade_id)
                .ok_or_else(|| anyhow!("Trade {} not found for {}", trade_id, pair))?;
            let current_qty = trade.remaining_amount;
            let qty_to_reduce = current_qty.min(remaining_sell_qty);
            let new_remaining_qty = (current_qty - qty_to_reduce).max(Decimal::ZERO);
            let new_open =
                if new_remaining_qty <= Decimal::new(1, 8) || new_remaining_qty * close_price < min_notional {
                    0
                } else {
                    1
                };
            let new_partial_open =
                if new_remaining_qty > Decimal::new(1, 8) && new_remaining_qty * close_price < min_notional {
                    1
                } else {
                    0
                };
            info!("DB: Updated buy trade {}: remaining={}, open={}, partial_open={}", trade_id, new_remaining_qty.to_f64().unwrap_or(0.0), new_open, new_partial_open);
            let mut trade_data = trade.clone();
            trade_data.remaining_amount = new_remaining_qty;
            trade_data.open = new_open;
            trade_data.partial_open = new_partial_open;
            updates.push((trade_id.clone(), trade_data, new_remaining_qty));
            info!("Reduced {} by {:.8}: new_rem={:.8}, flags={}/{}", trade_id, qty_to_reduce.to_f64().unwrap_or(0.0), new_remaining_qty.to_f64().unwrap_or(0.0), new_open, new_partial_open);
            remaining_sell_qty -= qty_to_reduce;
        }
        for (trade_id, trade_data, new_remaining_qty) in updates {
            state_manager.update_trades(trade_data.clone()).await?;
            if new_remaining_qty <= Decimal::new(1, 8) {
                state_manager
                    .remove_trade_id(pair.to_string(), trade_id.clone())
                    .await?;
            }
        }
        Ok(())
    }
    pub async fn global_trade_sweep(
        state_manager: Arc<StateManager>,
        client: &KrakenClient,
        config: &Config,
        shutdown_tx: broadcast::Sender<()>,
    ) -> Result<()> {
        let pairs = config.portfolio.api_pairs.value.clone();
        let ws_enabled = true;
        let sweep_interval = Duration::from_secs(60);
        let linger_threshold = Duration::from_secs(30);
        let mut shutdown_rx = shutdown_tx.subscribe();
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Shutdown received in global trade sweep");
                    break;
                }
                _ = tokio::time::sleep(sweep_interval) => {}
            }
            if !state_manager.running() { break; }
            let mut swept_orders = 0;
            for pair in &pairs {
                if let Ok(open_orders) = state_manager.get_open_orders(pair.clone()).await {
                    if open_orders.is_empty() {
                        continue;
                    }
                    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64();
                    for (order_id, info) in open_orders {
                        if now - info.start_time <= linger_threshold.as_secs_f64() && ws_enabled {
                            continue;
                        }
                        info!(target: "trade", "Global sweep: Monitoring lingering order {} on {} (age={:.1}s)", order_id, pair, now - info.start_time);

                        let mut buy_trade_ids = vec![];
                        let mut total_buy_qty = Decimal::ZERO;
                        let open_orders = state_manager.get_open_orders(pair.clone()).await?;
                        if let Some(order_info) = open_orders.get(&order_id) {
                            buy_trade_ids = order_info.buy_trade_ids.clone();
                            total_buy_qty = order_info.total_buy_qty;
                            info!("Sweep: Fetched {} buy_trade_ids and total_buy_qty {} for lingering order {}", buy_trade_ids.len(), total_buy_qty, order_id);
                        } else {
                            warn!("Sweep: No OrderInfo for lingering order {}, skipping fallback reduction", order_id);
                        }

                        let client_clone = client.clone();
                        let state_manager_clone = state_manager.clone();
                        let config_clone = config.clone();
                        let pair_clone = pair.clone();
                        let shutdown_tx_clone = shutdown_tx.clone();
                        let order_id_clone = order_id.clone();
                        if state_manager.is_monitoring(order_id_clone.clone()).await.unwrap_or(true) {
                            info!("Skipping monitor spawn for already monitoring order {}", order_id_clone);
                            continue;
                        }
                        tokio::spawn(async move {
                            let shutdown_rx = shutdown_tx_clone.subscribe();
                            match monitor_order(
                                &client_clone,
                                vec![pair_clone.clone()],
                                state_manager_clone.clone(),
                                &config_clone,
                                order_id_clone,
                                info.avg_cost_basis,
                                buy_trade_ids,
                                total_buy_qty,
                                shutdown_rx,
                            ).await {
                                Ok((filled, _, _, _, _)) => {
                                    let _ = state_manager_clone.send_completion(pair_clone, OrderComplete::Success(filled)).await;
                                }
                                Err(e) => {
                                    let _ = state_manager_clone.send_completion(pair_clone, OrderComplete::Error(Arc::new(e))).await;
                                }
                            }
                        });
                        swept_orders += 1;
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
            if swept_orders > 0 {
                info!(target: "trade", "Global sweep completed (WS=true): monitored {} lingering orders across {} pairs", swept_orders, pairs.len());
            } else if ws_enabled {
                info!(target: "trade", "Global sweep: no lingering orders >{}s (WS handling)", linger_threshold.as_secs());
            }
        }
        Ok(())
    }
}