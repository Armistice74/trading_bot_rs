// processing.rs
// description:
// This module handles post-trade processing, including aggregating trade data from fills, updating positions and USD balances, calculating profits, fees, slippage, and prorated costs for sells, as well as recording events and ensuring data consistency.

// Imports

use crate::api::KrakenClient;
use crate::config::Config;
use crate::statemanager::{Position, StateManager, Trade};
use crate::{get_current_time, record_trade_count};
use anyhow::{anyhow, Result};
use rust_decimal::prelude::*;
use serde_json::json;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use tracing::{error, info, warn};
use crate::fetch::fetch_trades;

// Trade Processing Logic

pub mod processing {
    use super::*;
    pub async fn process_trade_data(
        client: &KrakenClient,
        pair: &str,
        trade_type: &str,
        order_id: &str,
        _start_time: f64,
        state_manager: Arc<StateManager>,
        reason: &str,
        config: &Config,
        average_buy_price: Decimal,
        buy_trade_id: Option<String>,
        pre_fetched_trades: Option<Vec<Trade>>,
        total_buy_qty: Option<Decimal>,
    ) -> Result<(Decimal, Decimal, Decimal, Vec<String>, Vec<Trade>), anyhow::Error> {
        let mut executed_qty_total = Decimal::ZERO;
        let mut fees_usd_total = Decimal::ZERO;
        let mut avg_price_total = Decimal::ZERO;
        let mut trade_ids = vec![];
        let mut trade_data_list = vec![];
        let base_currency = pair.split("USD").next().unwrap_or("");
        let symbol = pair;
        let order_trades = if let Some(trades) = pre_fetched_trades {
            trades
        } else {
            fetch_trades(&client, &symbol, order_id, client.startup_time(), state_manager.clone(), config, None).await?
        };
        let order_book = client.fetch_order_book(&symbol, 10).await.unwrap_or(json!({
            "bids": [], "asks": []
        }));
        let filled = !order_trades.is_empty();
        for trade in order_trades {
            let trade_id = trade.trade_id.clone();
            let qty = trade.amount;
            let price = trade.execution_price;
            if qty == Decimal::ZERO || price == Decimal::ZERO {
                warn!(
                    "Skipping trade {} due to invalid data: qty={}, price={}",
                    trade_id, qty, price
                );
                continue;
            }
            let fees = trade.fees;
            let fees_usd = fees.round_dp(8);
            let empty_vec: Vec<serde_json::Value> = vec![];
            let bids = order_book["bids"].as_array().unwrap_or(&empty_vec);
            let asks = order_book["asks"].as_array().unwrap_or(&empty_vec);
            let best_bid = bids.get(0).and_then(|b| b[0].as_str()).and_then(|s| Decimal::from_str(s).ok()).unwrap_or(Decimal::ZERO);
            let best_ask = asks.get(0).and_then(|a| a[0].as_str()).and_then(|s| Decimal::from_str(s).ok()).unwrap_or(Decimal::ZERO);
            let slippage = if price > Decimal::ZERO {
                let ref_price = if trade_type == "buy" {
                    best_ask
                } else {
                    best_bid
                };
                if ref_price > Decimal::ZERO {
                    ((price - ref_price) / price * Decimal::from(100)).abs()
                } else {
                    Decimal::ZERO
                }
            } else {
                Decimal::ZERO
            };
            let mut trade_data = trade.clone();
            trade_data.reason = reason.to_string();
            if trade_type == "sell" {
                trade_data.avg_cost_basis = average_buy_price;
            } else {
                trade_data.avg_cost_basis = price;
            }
            trade_data.slippage = slippage.round_dp(2);
            trade_data.profit = -fees_usd;
            trade_data.profit_percentage = Decimal::ZERO;
            if trade_type == "sell" {
                let sell_value = (price * qty - fees_usd).round_dp(8);
                let buy_value = (trade_data.avg_cost_basis * qty).round_dp(8);
                let pl = (sell_value - buy_value).round_dp(8);
                trade_data.profit = pl.round_dp(6);
                trade_data.profit_percentage = if buy_value > Decimal::ZERO {
                    (pl / buy_value * Decimal::from(100)).round_dp(2)
                } else {
                    Decimal::ZERO
                };
                trade_data.remaining_amount = Decimal::ZERO;
                trade_data.open = 0;
                trade_data.partial_open = 0;
            } else {
                let notional = qty * price;
                trade_data.profit_percentage = if notional > Decimal::ZERO {
                    (trade_data.profit / notional * Decimal::from(100)).round_dp(2)
                } else {
                    Decimal::ZERO
                };
                trade_data.remaining_amount = qty;
                trade_data.open = if qty * price >= config.trading_logic.min_notional.value { 1 } else { 0 };
                trade_data.partial_open = if qty * price < config.trading_logic.min_notional.value { 1 } else { 0 };
            }
            trade_data.fee_percentage = if qty * price > Decimal::ZERO {
                (fees_usd / (qty * price) * Decimal::from(100)).round_dp(2)
            } else {
                Decimal::ZERO
            };
            if let Err(e) = state_manager.update_trades(trade_data.clone()).await {
                error!(
                    "Failed to update trade {} for {} with additional fields: {}",
                    trade_id, pair, e
                );
                let mut file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("log/error_log.txt")
                    .map_err(|e| anyhow!("Failed to open error log: {}", e))?;
                writeln!(
                    file,
                    "[{}] Failed to update trade {} for {} with additional fields: {}",
                    chrono::Local::now().to_string(),
                    trade_id,
                    pair,
                    e
                )
                .map_err(|e| anyhow!("Failed to write to error log: {}", e))?;
            }
            info!("Trade {} for {} successfully processed", trade_id, pair);
            record_trade_count(pair, trade_type, &trade_data);
            state_manager
                .add_trade_id(pair.to_string(), trade_id.clone(), order_id.to_string())
                .await?;
            executed_qty_total += qty;
            fees_usd_total += fees_usd;
            if qty > Decimal::ZERO {
                avg_price_total = (avg_price_total * (executed_qty_total - qty) + price * qty)
                    / executed_qty_total;
            }
            trade_ids.push(trade_id.clone());
            trade_data_list.push(trade_data);
        }
        if executed_qty_total == Decimal::ZERO && filled {
            warn!(
                "No quantity executed for order {} in {} but trades non-empty, partial/edge case handled",
                order_id, pair
            );
        } else if executed_qty_total == Decimal::ZERO {
            warn!(
                "No quantity executed for order {} in {}, skipping USD position update",
                order_id, pair
            );
        }
        Ok((
            executed_qty_total,
            fees_usd_total,
            avg_price_total,
            trade_ids,
            trade_data_list,
        ))
    }
    pub async fn post_process_trade(
        trade: &mut Trade,
        pair: &str,
        _close_price: Decimal,
        state_manager: &Arc<StateManager>,
        config: &Config,
    ) -> Result<()> {
        let amount = trade.amount;
        let price = trade.execution_price;
        let fees = trade.fees;
        let notional = amount * price;
        let fees_usd = fees.round_dp(8);
        trade.fee_percentage = if notional > Decimal::ZERO {
            (fees_usd / notional * Decimal::from(100)).round_dp(2)
        } else {
            Decimal::ZERO
        };
        let slippage = Decimal::ZERO;
        trade.slippage = slippage.round_dp(2);
        let base_currency = pair.split("USD").next().unwrap_or("");
        let existing_position = state_manager
            .get_position(pair.to_string())
            .await
            .unwrap_or_else(|_| Position::new(pair.to_string()));
        let new_position = if trade.trade_type == "sell" {
            let avg_buy_pos = compute_weighted_basis_from_trades(
                pair,
                None,
                state_manager.clone(),
            ).await;
            let profit_usd = ((price * amount - fees_usd) - (avg_buy_pos * amount)).round_dp(8);
            trade.profit = profit_usd.round_dp(6);
            trade.profit_percentage = if avg_buy_pos * amount > Decimal::ZERO {
                (profit_usd / (avg_buy_pos * amount) * Decimal::from(100)).round_dp(2)
            } else {
                Decimal::ZERO
            };
            trade.avg_cost_basis = avg_buy_pos;
            trade.remaining_amount = Decimal::ZERO;
            trade.open = 0;
            trade.partial_open = 0;
            let pl = ((price * amount - fees_usd) - (avg_buy_pos * amount)).round_dp(8);
            let pl_rounded = pl.round_dp(8);
            let total_pl = existing_position.total_pl + pl_rounded;
            let fees_rounded = fees_usd.round_dp(8);
            let total_fees = existing_position.total_fees + fees_rounded;
            let remaining_qty_total = (existing_position.total_quantity - amount).max(Decimal::ZERO);
            if remaining_qty_total < Decimal::ZERO {
                error!("Negative remaining quantity detected for {}: {}", pair, remaining_qty_total);
            }
            Position {
                pair: pair.to_string(),
                total_usd: existing_position.total_usd - (avg_buy_pos * amount),
                total_quantity: remaining_qty_total,
                usd_balance: Decimal::ZERO,
                last_updated: get_current_time().format("%Y-%m-%d %H:%M:%S").to_string(),
                total_fees,
                total_pl,
                highest_price_since_buy: if remaining_qty_total > Decimal::ZERO {
                    existing_position.highest_price_since_buy
                } else {
                    Decimal::ZERO
                },
                last_synced: chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
            }
        } else {
            trade.profit = -fees_usd;
            trade.profit_percentage = if notional > Decimal::ZERO {
                (trade.profit / notional * Decimal::from(100)).round_dp(2)
            } else {
                Decimal::ZERO
            };
            let avg_buy = price;
            trade.avg_cost_basis = avg_buy;
            let min_notional = config.trading_logic.min_notional.value;
            trade.remaining_amount = amount;
            if notional >= min_notional {
                trade.open = 1;
                trade.partial_open = 0;
            } else {
                trade.open = 0;
                trade.partial_open = 1;
            }
            let new_total_quantity = existing_position.total_quantity + amount;
            let new_total_usd = existing_position.total_usd + (amount * price);
            Position {
                pair: pair.to_string(),
                total_usd: new_total_usd,
                total_quantity: new_total_quantity,
                usd_balance: Decimal::ZERO,
                last_updated: get_current_time().format("%Y-%m-%d %H:%M:%S").to_string(),
                total_fees: existing_position.total_fees,
                total_pl: existing_position.total_pl - fees_usd,
                highest_price_since_buy: existing_position.highest_price_since_buy.max(price),
                last_synced: chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
            }
        };
        if let Err(e) = state_manager
            .update_positions(pair.to_string(), new_position, trade.trade_type == "buy")
            .await
        {
            error!(
                "Failed to update position in post_process_trade for {}: {}",
                pair, e
            );
        }
        let usd_delta = if trade.trade_type == "buy" {
            -(amount * price + fees_usd)
        } else {
            amount * price - fees_usd
        };
        if let Err(e) = state_manager.update_usd_balance_delta(usd_delta).await {
            error!("Failed to update USD balance delta in post_process_trade for {}: {}", pair, e);
        } else {
            info!("Updated USD balance by {:.2} in post_process_trade for {}", usd_delta, pair);
        }
        Ok(())
    }

    pub async fn compute_weighted_basis_from_trades(
        pair: &str,
        buy_trade_ids: Option<Vec<String>>,
        state_manager: Arc<StateManager>,
    ) -> Decimal {
        let open_trades = state_manager.get_open_trades(pair.to_string()).await.unwrap_or(vec![]);
        let filtered_trades: Vec<Trade> = if let Some(ids) = buy_trade_ids {
            open_trades.into_iter().filter(|t| ids.contains(&t.trade_id)).collect()
        } else {
            open_trades.into_iter().filter(|t| t.trade_type == "buy").collect()
        };

        let mut total_weighted_cost = Decimal::ZERO;
        let mut total_remaining = Decimal::ZERO;

        for trade in filtered_trades {
            let remaining = trade.remaining_amount;
            if remaining > Decimal::ZERO {
                total_weighted_cost += trade.avg_cost_basis * remaining;
                total_remaining += remaining;
            }
        }

        if total_remaining > Decimal::ZERO {
            total_weighted_cost / total_remaining
        } else {
            Decimal::ZERO
        }
    }
}