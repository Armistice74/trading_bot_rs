// monitor.rs
// Description: Handles order monitoring logic, including polling for status, trade fetches, and processing with dedup avoidance.

// Imports

use crate::api::KrakenClient;
use crate::config::Config;
use crate::fetch::fetch_trades;
use crate::statemanager::StateManager;
use crate::trading::trading_logic::update_remaining_quantities;
use crate::statemanager::TradeManagerMessage;
use tokio::sync::oneshot;
use anyhow::Result;
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{error, info, warn};
use rust_decimal::prelude::*;
use crate::processing::processing::process_trade_data;
use crate::statemanager::OrderComplete;
use anyhow::anyhow;
use tokio::sync::broadcast::Receiver as BroadcastReceiver;
use tokio::time::Instant;
use crate::statemanager::OrderStatus;
use crate::utils::report_log;
use std::sync::atomic::{AtomicU64, Ordering};

// Monitor Function

pub async fn monitor_order(
    client: &KrakenClient,
    pairs: Vec<String>,
    state_manager: Arc<StateManager>,
    config: &Config,
    report_path: Arc<String>,
    cancels_counter: Arc<AtomicU64>,
    target_order_id: String,
    average_buy_price: Decimal,
    mut buy_trade_ids: Vec<String>,
    mut total_buy_qty: Decimal,
    mut shutdown_rx: BroadcastReceiver<()>,
) -> Result<(bool, Decimal, Decimal, Decimal, Vec<String>), anyhow::Error> {
    let (tx_check, rx_check) = oneshot::channel();
    if state_manager.trade_manager_tx.send(TradeManagerMessage::IsMonitoring {
        order_id: target_order_id.clone(),
        reply: tx_check,
    }).await.is_err() || rx_check.await.ok().unwrap_or(false) {
        info!("Order {} already under monitoring, skipping duplicate spawn", target_order_id);
        return Err(anyhow!("Duplicate monitor attempt"));
    }

    let (tx_set, rx_set) = oneshot::channel();
    if state_manager.trade_manager_tx.send(TradeManagerMessage::SetMonitoring {
        order_id: target_order_id.clone(),
        monitoring: true,
        reply: tx_set,
    }).await.is_err() {
        return Err(anyhow!("Failed to set monitoring flag"));
    }
    rx_set.await.ok();

    let mut executed_qty_total = Decimal::ZERO;
    let mut fees_usd_total = Decimal::ZERO;
    let mut avg_price_total = Decimal::ZERO;
    let mut trade_ids = vec![];
    let mut filled = false;
    let mut rate_limit_backoff = Duration::from_secs(1);
    let monitor_loop_delay = Duration::from_secs_f64(config.delays.monitor_loop_delay.value.to_f64().unwrap_or(0.0));
    let monitor_partial_delay = Duration::from_secs_f64(config.delays.monitor_partial_delay.value.to_f64().unwrap_or(0.0));
    let pair_delay = Duration::from_secs_f64(config.delays.monitor_pair_delay.value.to_f64().unwrap_or(0.0));
    let pair = pairs.first().cloned().unwrap_or_default();  // Assume single pair for signal
    let near_timeout_threshold = 0.8;  // 80% of timeout for "near"

    info!("Starting order monitoring for {}: loop_delay=30s (WS fallback), pair_delay={:.1}s, partial_delay=10s", 
        target_order_id, config.delays.monitor_pair_delay.value.to_f64().unwrap_or(0.0));

    // Add 1s sleep post-signal before check
    tokio::time::sleep(Duration::from_secs(1)).await;

    while state_manager.running() {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Shutdown received in monitor for order {}", target_order_id);
                let _ = state_manager.send_completion(pair.clone(), OrderComplete::Error(Arc::new(anyhow!("Shutdown")))).await;
                clear_monitoring_flag(&state_manager, &target_order_id).await;
                return Err(anyhow!("Shutdown in monitor"));
            }
            _ = tokio::time::sleep(monitor_loop_delay) => {}  // Proceed to loop body
        }

        for (i, pair_item) in pairs.iter().enumerate() {
            if !state_manager.running() {
                break;
            }
            if i > 0 {
                tokio::time::sleep(pair_delay).await;
            }
            let symbol = pair_item;
            let tracked_orders = state_manager.get_open_orders(pair_item.to_string()).await?;
            if !tracked_orders.iter().any(|(id, _)| id == &target_order_id) {
                info!("Target order {} no longer tracked, exiting monitor", target_order_id);
                let _ = state_manager.send_completion(pair.clone(), OrderComplete::Success(false)).await;
                clear_monitoring_flag(&state_manager, &target_order_id).await;
                return Ok((false, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, vec![]));
            }
            let current_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs_f64();
            let price_data = state_manager.get_price(pair_item.to_string()).await?;
            let close_price = price_data.close_price;
            let order_book = client.fetch_order_book(&symbol, 10).await.unwrap_or(json!({
                "bids": [], "asks": []
            }));
            let best_bid = order_book["bids"]
                .as_array()
                .unwrap_or(&vec![])
                .get(0)
                .and_then(|b| b[0].as_str())
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(Decimal::ZERO);
            let best_ask = order_book["asks"]
                .as_array()
                .unwrap_or(&vec![])
                .get(0)
                .and_then(|a| a[0].as_str())
                .and_then(|s| Decimal::from_str(s).ok())
                .unwrap_or(Decimal::ZERO);
            for (order_id, info) in tracked_orders.iter() {
                if *order_id != target_order_id {
                    continue;
                }
                let timeout = info.timeout;
                let trade_type = &info.trade_type;
                let order_age = current_time - info.start_time;
                let symbol = &info.symbol;

                let should_check = if close_price != Decimal::ZERO {
                    if trade_type == "buy"
                        && best_ask > Decimal::ZERO
                        && close_price
                            > best_ask * (Decimal::ONE + config.trading_logic.price_modifier.value)
                    {
                        true
                    } else if trade_type == "sell"
                        && best_bid > Decimal::ZERO
                        && close_price
                            < best_bid * (Decimal::ONE - config.trading_logic.price_modifier.value)
                    {
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };

                // Query WS status first
                let ws_status = get_order_status(&state_manager, order_id).await;

                let mut status = ws_status.as_ref().map(|s| s.status.clone()).unwrap_or("unknown".to_string());
                let mut vol_exec = ws_status.as_ref().map(|s| s.vol_exec).unwrap_or(Decimal::ZERO);
                let mut vol = ws_status.as_ref().map(|s| s.vol).unwrap_or(Decimal::ZERO);

                // One-time vol validate on first partial
                let mut validated_partial = false;
                if status == "partial" && vol_exec > Decimal::ZERO {
                    // Assume GetOrderVol added; here query info.vol or from OrderInfo
                    if vol_exec < info.vol { // Assuming OrderInfo has vol field; add if needed
                        validated_partial = true;
                    } else {
                        warn!("Partial status but vol_exec >= vol for {}, assuming mismatch", order_id);
                    }
                }

                // If WS provides status, log and skip API if terminal or partial confirmed
                if let Some(ws_stat) = ws_status.as_ref() {
                    if is_terminal(&ws_stat.status) || (ws_stat.status == "partial" && ws_stat.vol_exec > Decimal::ZERO) {
                        info!("WS status for order {}: {} vol_exec vol_exec={:.8} (vol={:.8}), skipping API poll if terminal", order_id, ws_stat.status, ws_stat.vol_exec.to_f64().unwrap_or(0.0), ws_stat.vol.to_f64().unwrap_or(0.0));
                    }
                }

                if !is_terminal(&status) && vol_exec == Decimal::ZERO {
                    // Fallback to API if no WS terminal/partial
                    let order_details = match client.fetch_order(symbol, order_id).await {
                        Ok(details) => {
                            info!(
                                "Order {} for {}: raw response: {:?}",
                                order_id, pair_item, details
                            );
                            details
                        }
                        Err(e) => {
                            if e.to_string().contains("Rate limit exceeded") {
                                warn!(
                                    "Rate limit exceeded for order {} in {}, backing off for {:?}",
                                    order_id, pair_item, rate_limit_backoff
                                );
                                tokio::time::sleep(rate_limit_backoff).await;
                                rate_limit_backoff =
                                    (rate_limit_backoff * 2).min(Duration::from_secs(60));
                                continue;
                            }
                            if e.to_string().contains("OrderNotFound") {
                                info!("Order {} for {} not found, removing", order_id, pair_item);
                                state_manager
                                    .remove_open_order(pair_item.to_string(), order_id.clone())
                                    .await?;
                                continue;
                            }
                            warn!("Failed to fetch order {} for {}: {}", order_id, pair_item, e);
                            continue;
                        }
                    };

                    let api_status = order_details
                        .get("status")
                        .and_then(|s| s.as_str())
                        .unwrap_or("open");
                    let api_vol_exec = order_details
                        .get("vol_exec")
                        .and_then(|v| v.as_str().and_then(|s| Decimal::from_str(s).ok()))
                        .unwrap_or(Decimal::ZERO);
                    let api_vol = order_details
                        .get("vol")
                        .and_then(|v| v.as_str().and_then(|s| Decimal::from_str(s).ok()))
                        .unwrap_or(Decimal::ZERO);

                    status = api_status.to_string();
                    vol_exec = api_vol_exec;
                    vol = api_vol;
                }

                let mut trades = vec![];
                let mut skip_fetch = false;

                if let Some(ws_stat) = ws_status.as_ref() {
                    let is_term = is_terminal(&ws_stat.status);
                    if ws_stat.vol_exec == Decimal::ZERO && (order_age <= timeout * near_timeout_threshold || is_term) {
                        skip_fetch = true;
                        info!("Skipping fetch_trades for {}: WS confirms no fills (terminal={}), age={:.1}s <= {:.1}s (80% threshold)", order_id, is_term, order_age, timeout * near_timeout_threshold);
                    }
                }

                if !skip_fetch {
                    trades = match fetch_trades(&client, symbol, order_id, client.startup_time(), state_manager.clone(), config, None).await {
                        Ok(t) => t,
                        Err(e) => {
                            warn!(
                                "Failed to fetch trades for order {} for {}: {}",
                                order_id, pair_item, e
                            );
                            vec![]
                        }
                    };

                    info!(
                        "Order {} for {}: fetch_trades response: {:?}",
                        order_id, pair_item, trades
                    );
                    for trade in &trades {
                        let trade_id = &trade.trade_id;
                        info!(target: "trade", "Fetched trade ID {} for order {} on {}", trade_id, order_id, pair_item);
                    }
                }

                if is_terminal(&status) {
                    info!("Order {} for {} {}, processing and exiting monitor", order_id, pair_item, status);
                }

                if is_terminal(&status) || status == "partial" || should_check || order_age > timeout {
                    let is_partial = status == "partial";

                    if is_partial || !is_terminal(&status) {
                        info!("Order {} for {}: potential partial/incomplete (status={}), entering accumulation", 
                            order_id, pair_item, status);
                        let mut iteration = 0;
                        let max_iterations = 10;
                        let mut sleep_duration = monitor_partial_delay;
                        let overall_start = Instant::now();
                        let overall_timeout = Duration::from_secs(60);
                        let mut stagnant_count = 0;
                        let stagnant_threshold = 3;
                        let mut prev_trades_len = trades.len();

                        loop {
                            if iteration >= max_iterations || overall_start.elapsed() > overall_timeout {
                                warn!("Accumulation exceeded limits for {} (iter={}, time={:?}), escalating to cancel", order_id, iteration, overall_start.elapsed());
                                escalate_to_cancel(client, symbol, order_id, pair_item, state_manager.clone()).await;
                                break;
                            }

                            tokio::select! {
                                _ = shutdown_rx.recv() => {
                                    info!("Shutdown in partial loop for order {}", order_id);
                                    let _ = state_manager.send_completion(pair.clone(), OrderComplete::Error(Arc::new(anyhow!("Shutdown")))).await;
                                    clear_monitoring_flag(&state_manager, &target_order_id).await;
                                    return Err(anyhow!("Shutdown in partial loop"));
                                }
                                _ = tokio::time::sleep(sleep_duration) => {}
                            }

                            // Re-query WS status
                            let ws_status = get_order_status(&state_manager, order_id).await;
                            status = ws_status.as_ref().map(|s| s.status.clone()).unwrap_or(status);
                            vol_exec = ws_status.as_ref().map(|s| s.vol_exec).unwrap_or(vol_exec);
                            vol = ws_status.as_ref().map(|s| s.vol).unwrap_or(vol);

                            if is_terminal(&status) {
                                info!("Order {} now terminal during accumulation (status={}), proceeding to process", order_id, status);
                                break;
                            }

                            let partial_trades = match fetch_trades(&client, symbol, order_id, client.startup_time(), state_manager.clone(), config, None).await {
                                Ok(t) => t,
                                Err(e) => {
                                    warn!("Failed to fetch during accumulation for order {}: {}", order_id, e);
                                    sleep_duration = (sleep_duration * 2).min(Duration::from_secs(10));
                                    iteration += 1;
                                    continue;
                                }
                            };
                            trades.extend(partial_trades.clone());
                            info!("Order {}: accumulation fetched {} trades (total now {}, vol_exec={:.8})", 
                                order_id, partial_trades.len(), trades.len(), vol_exec.to_f64().unwrap_or(0.0));

                            if trades.len() == prev_trades_len {
                                stagnant_count += 1;
                                if stagnant_count >= stagnant_threshold {
                                    warn!("Stagnant accumulation for {} (no new trades {}x), early DB query/escalate", order_id, stagnant_count);
                                    let db_trades = state_manager.get_trades_for_order(order_id.clone(), pair_item.clone(), client.startup_time()).await.unwrap_or(vec![]);
                                    trades.extend(db_trades.iter().filter(|t| !trades.contains(t)).cloned().collect::<Vec<_>>());
                                    if trades.len() == prev_trades_len {
                                        escalate_to_cancel(client, symbol, order_id, pair_item, state_manager.clone()).await;
                                        // NEW: Ensure process if !empty after escalate
                                        if !trades.is_empty() {
                                            info!(
                                                "Order {} for {}: processing accumulated trades after escalate, status={}, trades_len={}",
                                                order_id,
                                                pair_item,
                                                status,
                                                trades.len()
                                            );
                                            let (qty, fees, avg_price, new_trade_ids, _trade_data_list) =
                                                process_trade_data(
                                                    client,
                                                    pair_item,
                                                    trade_type,
                                                    order_id,
                                                    info.start_time,
                                                    state_manager.clone(),
                                                    info.reason.as_str(),
                                                    config,
                                                    average_buy_price,
                                                    None,
                                                    Some(trades.clone()),
                                                    Some(total_buy_qty),
                                                )
                                                .await?;
                                            executed_qty_total += qty;
                                            fees_usd_total += fees;
                                            if qty > Decimal::ZERO {
                                                avg_price_total = (avg_price_total * (executed_qty_total - qty) + avg_price * qty)
                                                    / executed_qty_total;
                                            }
                                            trade_ids.extend(new_trade_ids);
                                            filled = !trades.is_empty();

                                            if trade_type == "sell" && qty > Decimal::ZERO {
                                                info!("Sell fill for order {}: actual avg_price={:.6}, fees={:.2}; compare to decision close_price from evaluate_trade logs",
                                                    order_id, avg_price.to_f64().unwrap_or(0.0), fees.to_f64().unwrap_or(0.0));
                                                let base_currency = pair_item.split("USD").next().unwrap_or("");
                                                let mut trade_ids_to_sell = buy_trade_ids.clone();
                                                if buy_trade_ids.is_empty() {
                                                    warn!("Sell monitoring: buy_trade_ids empty, skipping fallback to avoid incorrect reductions");
                                                    // UPDATED: No fallback to get_open_trades; just skip to prevent mismatches
                                                } else {
                                                    if let Err(e) = update_remaining_quantities(
                                                        pair_item,
                                                        qty,
                                                        trade_ids_to_sell,
                                                        state_manager.clone(),
                                                        config,
                                                        base_currency,
                                                    )
                                                    .await
                                                    {
                                                        error!("Failed to update remaining quantities after sell order {} for {}: {}", order_id, pair_item, e);
                                                    }
                                                }
                                            }

                                            state_manager
                                                .remove_open_order(pair_item.to_string(), order_id.clone())
                                                .await?;
                                            let _ = state_manager.send_completion(pair.clone(), OrderComplete::Success(filled)).await;
                                            clear_monitoring_flag(&state_manager, &target_order_id).await;
                                            return Ok((
                                                filled,
                                                executed_qty_total,
                                                fees_usd_total,
                                                avg_price_total,
                                                trade_ids,
                                            ));
                                        }
                                        break;
                                    }
                                }
                            } else {
                                stagnant_count = 0;
                            }
                            prev_trades_len = trades.len();

                            sleep_duration = (sleep_duration * 2).min(Duration::from_secs(10));
                            iteration += 1;
                        }
                    }

                    // NEW: If buy_trade_ids empty (e.g., sweep), fetch from OrderInfo
                    if buy_trade_ids.is_empty() {
                        let (tx, rx) = oneshot::channel();
                        state_manager.trade_manager_tx
                            .send(TradeManagerMessage::GetOpenOrders {
                                pair: pair_item.to_string(),
                                reply: tx,
                            })
                            .await?;
                        if let Ok(orders) = rx.await {
                            if let Some(order_info) = orders.get(order_id.as_str()) {
                                buy_trade_ids = order_info.buy_trade_ids.clone();
                                total_buy_qty = order_info.total_buy_qty;
                                info!("Fetched buy_trade_ids ({:?}) and total_buy_qty ({}) for {} from OrderInfo", buy_trade_ids, total_buy_qty, order_id);
                            } else {
                                warn!("No OrderInfo for {} to fetch buy_trade_ids", order_id);
                            }
                        }
                    }

                    if !is_terminal(&status) && status != "partial" {
                        warn!("Order {} incomplete after accumulation (status={}), but keeping monitor active for WS updates", order_id, status);
                        continue;
                    }

                    if is_terminal(&status) && trades.is_empty() {
                        info!("Order {} for {}: status={} but 0 trades, final sleep 10s + re-fetch", order_id, pair_item, status);
                        tokio::time::sleep(Duration::from_secs(10)).await;
                        let retry_trades = match fetch_trades(&client, symbol, order_id, client.startup_time(), state_manager.clone(), config, None).await {
                            Ok(t) => t,
                            Err(e) => {
                                warn!("Failed final re-fetch for {}: {}", order_id, e);
                                vec![]
                            }
                        };
                        trades.extend(retry_trades.clone());
                        info!("Final re-fetch added {} trades (total now {})", retry_trades.len(), trades.len());
                    }

                    if is_terminal(&status) || status == "partial" || !trades.is_empty() {
                        if trades.is_empty() && (is_terminal(&status) || status == "partial") {
                            let db_trades = match state_manager.get_trades_for_order(order_id.clone(), pair_item.clone(), client.startup_time()).await {
                                Ok(t) => t,
                                Err(e) => {
                                    warn!("Failed DB fallback for order {} on {}: {}", order_id, pair_item, e);
                                    vec![]
                                }
                            };
                            if !db_trades.is_empty() {
                                trades = db_trades;
                                info!("Fallback to DB trades for order {} on {}: {} trades (WS pre-inserted)", order_id, pair_item, trades.len());
                            }
                        }

                        info!(
                            "Order {} for {}: processing accumulated trades, status={}, trades_len={}",
                            order_id,
                            pair_item,
                            status,
                            trades.len()
                        );
                        let (qty, fees, avg_price, new_trade_ids, _trade_data_list) =
                            process_trade_data(
                                client,
                                pair_item,
                                trade_type,
                                order_id,
                                info.start_time,
                                state_manager.clone(),
                                info.reason.as_str(),
                                config,
                                average_buy_price,
                                None,
                                Some(trades.clone()),
                                Some(total_buy_qty),
                            )
                            .await?;
                        executed_qty_total += qty;
                        fees_usd_total += fees;
                        if qty > Decimal::ZERO {
                            avg_price_total = (avg_price_total * (executed_qty_total - qty) + avg_price * qty)
                                / executed_qty_total;
                        }
                        trade_ids.extend(new_trade_ids);
                        filled = !trades.is_empty();

                        if trade_type == "sell" && qty > Decimal::ZERO {
                            info!("Sell fill for order {}: actual avg_price={:.6}, fees={:.2}; compare to decision close_price from evaluate_trade logs",
                                order_id, avg_price.to_f64().unwrap_or(0.0), fees.to_f64().unwrap_or(0.0));
                            let base_currency = pair_item.split("USD").next().unwrap_or("");
                            let mut trade_ids_to_sell = buy_trade_ids.clone();
                            if buy_trade_ids.is_empty() {
                                warn!("Sell monitoring: buy_trade_ids empty, skipping fallback to avoid incorrect reductions");
                                // UPDATED: No fallback to get_open_trades; just skip to prevent mismatches
                            } else {
                                if let Err(e) = update_remaining_quantities(
                                    pair_item,
                                    qty,
                                    trade_ids_to_sell,
                                    state_manager.clone(),
                                    config,
                                    base_currency,
                                )
                                .await
                                {
                                    error!("Failed to update remaining quantities after sell order {} for {}: {}", order_id, pair_item, e);
                                }
                            }
                        }

                        state_manager
                            .remove_open_order(pair_item.to_string(), order_id.clone())
                            .await?;
                        let _ = state_manager.send_completion(pair.clone(), OrderComplete::Success(filled)).await;
                        clear_monitoring_flag(&state_manager, &target_order_id).await;
                        return Ok((
                            filled,
                            executed_qty_total,
                            fees_usd_total,
                            avg_price_total,
                            trade_ids,
                        ));
                    }

                    if should_check || order_age > timeout {
                        info!("Order {} for {} timed out (age={:.2}s) or price condition met, attempting cancellation", order_id, pair_item, order_age);
                        let final_trades = match fetch_trades(&client, symbol, order_id, client.startup_time(), state_manager.clone(), config, Some(1)).await {
                            Ok(t) => t,
                            Err(e) => {
                                warn!("Failed final fetch on timeout for {}: {}", order_id, e);
                                vec![]
                            }
                        };
                        trades.extend(final_trades.clone());

                        match client.cancel_order(symbol, order_id).await {
                            Ok(_) => {
                                info!("Order {} for {} canceled successfully", order_id, pair_item);
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to cancel order {} for {}: {}",
                                    order_id, pair_item, e
                                );
                            }
                        }

                        state_manager
                            .remove_open_order(pair_item.to_string(), order_id.clone())
                            .await?;

                        if !trades.is_empty() {
                            let (qty, fees, avg_price, new_trade_ids, _trade_data_list) =
                                process_trade_data(
                                    client,
                                    pair_item,
                                    trade_type,
                                    order_id,
                                    info.start_time,
                                    state_manager.clone(),
                                    info.reason.as_str(),
                                    config,
                                    average_buy_price,
                                    None,
                                    Some(trades.clone()),
                                    Some(total_buy_qty),
                                )
                                .await?;
                            executed_qty_total += qty;
                            fees_usd_total += fees;
                            if qty > Decimal::ZERO {
                                avg_price_total = (avg_price_total
                                    * (executed_qty_total - qty)
                                    + avg_price * qty)
                                    / executed_qty_total;
                            }
                            trade_ids.extend(new_trade_ids);
                            filled = !trades.is_empty();

                            if trade_type == "sell" && qty > Decimal::ZERO {
                                info!("Sell fill for order {}: actual avg_price={:.6}, fees={:.2}; compare to decision close_price from evaluate_trade logs",
                                    order_id, avg_price.to_f64().unwrap_or(0.0), fees.to_f64().unwrap_or(0.0));
                                let base_currency = pair_item.split("USD").next().unwrap_or("");
                                let mut trade_ids_to_sell = buy_trade_ids.clone();
                                if buy_trade_ids.is_empty() {
                                    warn!("Sell monitoring on cancel: buy_trade_ids empty, skipping fallback to avoid incorrect reductions");
                                } else {
                                    if let Err(e) = update_remaining_quantities(
                                        pair_item,
                                        qty,
                                        trade_ids_to_sell,
                                        state_manager.clone(),
                                        config,
                                        base_currency,
                                    )
                                    .await
                                    {
                                        error!("Failed to update remaining quantities after canceled sell order {} for {}: {}", order_id, pair_item, e);
                                    }
                                }
                            }

                            let _ = state_manager.send_completion(pair.clone(), OrderComplete::Success(filled)).await;
                            clear_monitoring_flag(&state_manager, &target_order_id).await;
                            return Ok((
                                filled,
                                executed_qty_total,
                                fees_usd_total,
                                avg_price_total,
                                trade_ids,
                            ));
                        }

                        let _ = state_manager.send_completion(pair.clone(), OrderComplete::Error(Arc::new(anyhow!("Timeout with no fills")))).await;
                        clear_monitoring_flag(&state_manager, &target_order_id).await;
                        return Ok((
                            filled,
                            executed_qty_total,
                            fees_usd_total,
                            avg_price_total,
                            trade_ids,
                        ));
                    }
                }
            }
        }
    }
    let _ = state_manager.send_completion(pair.clone(), OrderComplete::Error(Arc::new(anyhow!("Shutdown")))).await;
    clear_monitoring_flag(&state_manager, &target_order_id).await;
    Ok((
        filled,
        executed_qty_total,
        fees_usd_total,
        avg_price_total,
        trade_ids,
    ))
}

async fn clear_monitoring_flag(state_manager: &Arc<StateManager>, order_id: &str) {
    let (tx_clear, rx_clear) = oneshot::channel();
    if state_manager.trade_manager_tx.send(TradeManagerMessage::SetMonitoring {
        order_id: order_id.to_string(),
        monitoring: false,
        reply: tx_clear,
    }).await.is_ok() {
        rx_clear.await.ok(); // Ignore
    }
}

async fn get_order_status(state_manager: &Arc<StateManager>, order_id: &str) -> Option<OrderStatus> {
    let (tx, rx) = oneshot::channel();
    if state_manager.trade_manager_tx.send(TradeManagerMessage::GetOrderStatus {
        order_id: order_id.to_string(),
        reply: tx,
    }).await.is_err() {
        None
    } else {
        rx.await.ok().flatten()
    }
}

fn is_terminal(status: &str) -> bool {
    ["closed", "canceled", "filled", "expired"].contains(&status)
}

async fn escalate_to_cancel(client: &KrakenClient, symbol: &str, order_id: &str, pair_item: &str, state_manager: Arc<StateManager>) {
    match client.cancel_order(symbol, order_id).await {
        Ok(_) => info!("Escalated cancel for order {} on {}", order_id, pair_item),
        Err(e) => warn!("Failed escalated cancel for {}: {}", order_id, e),
    }
    state_manager.remove_open_order(pair_item.to_string(), order_id.to_string()).await.ok();
    // Signal error or partial success as appropriate
}