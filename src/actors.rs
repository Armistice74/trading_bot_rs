// actors.rs
// description: Actor functions for positions, prices, market data, trades, execution, and database.

// ============================================================================
// Imports
// ============================================================================

use crate::api::KrakenClient;
use crate::config::Config;
use crate::db::get_trades_for_order;
use crate::db::{
    check_trade_exists, create_pool, get_all_positions, get_market_data_history, get_open_trades,
    get_trade_fees, insert_trade, store_market_data, update_position, update_trade, get_total_pl_for_pair, get_total_pl_for_crypto,
};
use crate::statemanager::{
    BuyMessage, CacheEntry, DatabaseMessage, MarketData, MarketDataMessage, OrderInfo, Position,
    PositionsMessage, PriceData, PriceMessage, SellMessage, TradeManagerMessage,
};
use crate::statemanager::StateManager;
use crate::statemanager::OrderStatus;
use crate::config::load_config;
use crate::trading::trading_logic::get_balance_key_for_pair;
use crate::db::get_latest_stop_loss_timestamp;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use chrono::Local;
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use tokio::sync::mpsc;
use tracing::{error, info};
use anyhow::{anyhow, Result};
use rust_decimal::prelude::*;

pub async fn positions_actor(client: Arc<KrakenClient>, mut rx: mpsc::Receiver<PositionsMessage>) {
    let mut state = HashMap::new();
    let mut balance_cache: HashMap<String, (Decimal, Instant)> = HashMap::new();
    while let Some(msg) = rx.recv().await {
        match msg {
            PositionsMessage::GetPosition { pair, reply } => {
                let pos = state
                    .get(&pair)
                    .cloned()
                    .unwrap_or_else(|| Position::new(pair.clone()));
                let _ = reply.send(pos);
            }
            PositionsMessage::UpdatePosition {
                pair,
                position,
                reply,
            } => {
                info!(
                    "PositionsActor: Updating position for {} with usd_balance: {}",
                    pair, position.usd_balance.round_dp(4)
                );
                if let Some(old_pos) = state.get(&pair) {
                    if old_pos.total_quantity != position.total_quantity {
                        balance_cache.remove(&pair);
                        info!("PositionsActor: Invalidated balance cache for {} (qty changed)", pair);
                    }
                }
                state.insert(pair.clone(), position);
                let _ = reply.send(());
                info!("PositionsActor: Updated position for {}", pair);
            }
            PositionsMessage::GetAllPositions { reply } => {
                let _ = reply.send(state.clone());
            }
            PositionsMessage::SyncUsdBalance { balance, reply } => {
                info!(
                    "PositionsActor: Received request to sync USD balance to {}",
                    balance.round_dp(4)
                );
                let usd_pos = state
                    .entry("USD".to_string())
                    .or_insert_with(|| Position::new("USD".to_string()));
                let old_balance = usd_pos.usd_balance;
                usd_pos.usd_balance = balance;
                usd_pos.last_updated = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
                let _ = reply.send(());
                info!(
                    "PositionsActor: Synced USD balance from {} to {}",
                    old_balance.round_dp(4), balance.round_dp(4)
                );
            }
            PositionsMessage::GetCachedBalance {
                pair,
                ttl,
                reply,
            } => {
                let now = Instant::now();
                let cached = balance_cache.get(&pair).cloned();
                let available = if let Some((avail, ts)) = cached {
                    if now.duration_since(ts) > ttl {
                        info!("PositionsActor: Balance cache stale for {}, refetching", pair);
                        match fetch_balance_for_pair(&client, &pair).await {
                            Ok(new_avail) => {
                                balance_cache.insert(pair.clone(), (new_avail, now));
                                info!("PositionsActor: Cached fresh balance {} for {}", new_avail.round_dp(4), pair);
                                new_avail
                            }
                            Err(e) => {
                                error!("PositionsActor: Failed to fetch balance for {}: {}, using cached {}", pair, e, avail.round_dp(4));
                                avail  // Fallback to stale
                            }
                        }
                    } else {
                        avail
                    }
                } else {
                    // No cache: fetch
                    info!("PositionsActor: No cache for {}, fetching balance", pair);
                    match fetch_balance_for_pair(&client, &pair).await {
                        Ok(new_avail) => {
                            balance_cache.insert(pair.clone(), (new_avail, now));
                            info!("PositionsActor: Cached new balance {} for {}", new_avail.round_dp(4), pair);
                            new_avail
                        }
                        Err(e) => {
                            error!("PositionsActor: Failed to fetch balance for {}: {}", pair, e);
                            Decimal::ZERO
                        }
                    }
                };
                let _ = reply.send(if available > Decimal::ZERO { Some(available) } else { None });
            }
            PositionsMessage::Shutdown { reply } => {
                let _ = reply.send(());
                info!("PositionsActor: Shutdown completed");
                break;
            }
        }
    }
}

async fn fetch_balance_for_pair(client: &Arc<KrakenClient>, pair: &str) -> Result<Decimal, anyhow::Error> {
    let config = load_config()?;
    let key = get_balance_key_for_pair(pair, &config);
    let balance = client.fetch_balance().await?;
    balance.get(&key)
        .and_then(|v| v.as_str())
        .and_then(|s| Decimal::from_str(s).ok())
        .ok_or_else(|| anyhow!("No balance for key '{}' (pair '{}')", key, pair))
}

pub async fn price_actor(pair: String, mut rx: mpsc::Receiver<PriceMessage>) {
    let mut price_data = PriceData::new(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO);
    while let Some(msg) = rx.recv().await {
        match msg {
            PriceMessage::GetPrice { reply, .. } => {
                let _ = reply.send(price_data.clone());
            }
            PriceMessage::UpdatePrice {
                price_data: new_data,
                reply,
                ..
            } => {
                price_data = new_data;
                let _ = reply.send(());
            }
            PriceMessage::Shutdown { reply } => {
                let _ = reply.send(());
                info!("PriceActor[{}]: Shutdown completed", pair);
                break;
            }
        }
    }
}

pub async fn market_data_actor(pair: String, mut rx: mpsc::Receiver<MarketDataMessage>) {
    let mut market_data = MarketData::new(pair.clone());
    let mut timestamp = 0.0;
    while let Some(msg) = rx.recv().await {
        match msg {
            MarketDataMessage::GetMarketData { reply, .. } => {
                let _ = reply.send((market_data.clone(), timestamp));
            }
            MarketDataMessage::UpdateMarketData {
                market_data: new_data,
                timestamp: new_timestamp,
                reply,
                ..
            } => {
                market_data = new_data;
                timestamp = new_timestamp;
                let _ = reply.send(());
            }
            MarketDataMessage::UpdateOhlc { ohlc, reply } => {
                market_data.open_price = ohlc.open;
                market_data.high_price = ohlc.high;
                market_data.low_price = ohlc.low;
                market_data.close_price = ohlc.close;
                market_data.volume = ohlc.volume;
                market_data.highest_price_period = ohlc.high;
                market_data.timestamp = ohlc.timestamp;
                timestamp = Local::now().timestamp() as f64;
                let _ = reply.send(());
            }
            MarketDataMessage::Shutdown { reply } => {
                let _ = reply.send(());
                info!("MarketDataActor[{}]: Shutdown completed", pair);
                break;
            }
        }
    }
}

pub async fn trade_manager_actor(mut rx: mpsc::Receiver<TradeManagerMessage>) {
    let mut open_orders: HashMap<String, HashMap<String, OrderInfo>> = HashMap::new();
    let mut trade_ids: HashMap<String, HashSet<String>> = HashMap::new();
    let mut db_cache: Vec<CacheEntry> = Vec::new();
    // NEW: Cache for closed order reasons: Vec<(order_id, reason, added_timestamp)>
    let mut closed_reasons: Vec<(String, String, f64)> = Vec::new();
    // NEW: Status from openOrders WS: HashMap<order_id, OrderStatus>
    let mut order_statuses: HashMap<String, OrderStatus> = HashMap::new();
    let mut monitoring: HashMap<String, bool> = HashMap::new();
    const REASON_CACHE_SIZE: usize = 1000;
    const REASON_TTL_SECS: f64 = 3600.0; // 1 hour

    while let Some(msg) = rx.recv().await {
        match msg {
            TradeManagerMessage::UpdateOpenOrder {
                pair,
                order_id,
                order_info,
                reply,
            } => {
                open_orders
                    .entry(pair.clone())
                    .or_insert_with(HashMap::new)
                    .insert(order_id.clone(), order_info);
                let _ = reply.send(());
                info!(
                    "TradeManager: Updated open order for pair {}, order_id {}",
                    pair, order_id
                );
            }
            TradeManagerMessage::RemoveOpenOrder {
                pair,
                order_id,
                reply,
            } => {
                if let Some(orders) = open_orders.get(&pair) {
                    if let Some(order_info) = orders.get(&order_id) {
                        // NEW: Cache reason for closed order
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs_f64();
                        closed_reasons.push((order_id.clone(), order_info.reason.clone(), now));
                        // Trim cache: Remove old (TTL) and limit size
                        closed_reasons.retain(|(_, _, ts)| now - ts < REASON_TTL_SECS);
                        if closed_reasons.len() > REASON_CACHE_SIZE {
                            closed_reasons.drain(0..closed_reasons.len() - REASON_CACHE_SIZE);
                        }
                        info!("TradeManager: Cached closed reason '{}' for order {}", order_info.reason, order_id);
                    }
                }
                if let Some(orders) = open_orders.get_mut(&pair) {
                    orders.remove(&order_id);
                    monitoring.remove(&order_id);  // Add here
                    if orders.is_empty() {
                        open_orders.remove(&pair);
                    }
                }
                // NEW: Also remove from order_statuses on explicit remove
                order_statuses.remove(&order_id);
                let _ = reply.send(());
                info!(
                    "TradeManager: Removed open order for pair {}, order_id {}",
                    pair, order_id
                );
            }
            TradeManagerMessage::GetOpenOrders { pair, reply } => {
                let orders = open_orders.get(&pair).cloned().unwrap_or_else(HashMap::new);
                let _ = reply.send(orders);
            }
            TradeManagerMessage::AddTradeId {
                pair,
                trade_id,
                order_id,
                reply,
            } => {
                trade_ids
                    .entry(pair.clone())
                    .or_insert_with(HashSet::new)
                    .insert(trade_id.clone());
                if let Some(orders) = open_orders.get_mut(&pair) {
                    if let Some(order) = orders.get_mut(&order_id) {
                        order.trade_ids.push(trade_id.clone());
                    }
                }
                let _ = reply.send(());
                info!(
                    "TradeManager: Added trade_id {} for pair {}, order_id {}",
                    trade_id, pair, order_id
                );
            }
            TradeManagerMessage::RemoveTradeId {
                pair,
                trade_id,
                reply,
            } => {
                if let Some(trades) = trade_ids.get_mut(&pair) {
                    trades.remove(&trade_id);
                    if trades.is_empty() {
                        trade_ids.remove(&pair);
                    }
                }
                let _ = reply.send(true);
                info!(
                    "TradeManager: Removed trade_id {} for pair {}",
                    trade_id, pair
                );
            }
            TradeManagerMessage::HasTradeId {
                pair,
                trade_id,
                reply,
            } => {
                let exists = trade_ids
                    .get(&pair)
                    .map_or(false, |trades| trades.contains(&trade_id));
                let _ = reply.send(exists);
                info!(
                    "TradeManager: Checked trade_id {} for pair {}, exists: {}",
                    trade_id, pair, exists
                );
            }
            // NEW: Handler for GetOrderReason
            TradeManagerMessage::GetOrderReason {
                pair,
                order_id,
                reply,
            } => {
                // Check open orders first
                let reason = if let Some(orders) = open_orders.get(&pair) {
                    orders.get(&order_id).map(|oi| oi.reason.clone())
                } else {
                    None
                };
                // Fallback to closed cache if not in open
                let cached_reason = if reason.is_none() {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs_f64();
                    closed_reasons.iter()
                        .find(|(oid, _, ts)| oid == &order_id && now - *ts < REASON_TTL_SECS)
                        .map(|(_, r, _)| r.clone())
                } else {
                    None
                };
                let final_reason = reason.or(cached_reason);
                let _ = reply.send(final_reason.clone());
                if final_reason.is_none() {
                    info!("TradeManager: No reason found for order {} in pair {}", order_id, pair);
                }
            }
            TradeManagerMessage::UpdateOrderStatus {
                order_id,
                pair: ws_pair,
                status,
                vol_exec,
                reply,
            } => {
                let pair = if let Some(p) = ws_pair {
                    p
                } else {
                    open_orders.iter()
                        .find_map(|(pr, orders)| {
                            orders.keys().any(|oid| oid == &order_id).then_some(pr.clone())
                        })
                        .unwrap_or_else(|| "UNKNOWN".to_string())
                };
                let vol = if let Some(orders) = open_orders.get(&pair) {
                    orders.get(&order_id).map(|oi| oi.amount).unwrap_or(Decimal::ZERO)
                } else {
                    Decimal::ZERO
                };
                let new_status = OrderStatus {
                    order_id: order_id.clone(),
                    pair: pair.clone(),
                    status: status.clone(),
                    vol_exec,
                    vol,
                };
                order_statuses.insert(order_id.clone(), new_status);

                // If closed/canceled, remove from open_orders and cache reason
                if status == "closed" || status == "canceled" || status == "filled" {
                    if let Some(orders) = open_orders.get_mut(&pair) {
                        if let Some(order_info) = orders.get(&order_id) {
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs_f64();
                            closed_reasons.push((order_id.clone(), order_info.reason.clone(), now));
                            closed_reasons.retain(|(_, _, ts)| now - *ts < REASON_TTL_SECS);
                            if closed_reasons.len() > REASON_CACHE_SIZE {
                                closed_reasons.drain(0..closed_reasons.len() - REASON_CACHE_SIZE);
                            }
                            info!("TradeManager: WS-closed order {}: cached reason '{}'", order_id, order_info.reason);
                        }
                        orders.remove(&order_id);
                        if orders.is_empty() {
                            open_orders.remove(&pair);
                        }
                    }
                }
                let _ = reply.send(());
                info!(
                    "TradeManager: Updated status for order {} ({}): {} vol_exec={}",
                    order_id, pair, status, vol_exec.round_dp(8)
                );
            }
            // NEW: Handler for GetOrderStatus
            TradeManagerMessage::GetOrderStatus {
                order_id,
                reply,
            } => {
                let status = order_statuses.get(&order_id).cloned();
                let status_for_send = status.clone();
                let _ = reply.send(status_for_send);
                if status.is_none() {
                    info!("TradeManager: No status found for order {}", order_id);
                }
            }
            TradeManagerMessage::CacheCancellation {
                pair,
                order_id,
                symbol,
                reply,
            } => {
                if db_cache.len() >= 10000 {
                    db_cache.remove(0);
                }
                db_cache.push(CacheEntry::CancelOrder {
                    pair: pair.clone(),
                    order_id: order_id.clone(),
                    symbol,
                });
                let _ = reply.send(());
                info!(
                    "TradeManager: Cached cancellation for pair {}, order_id {}",
                    pair, order_id
                );
            }
            TradeManagerMessage::IsMonitoring { order_id, reply } => {
                let _ = reply.send(monitoring.contains_key(&order_id));
            }
            TradeManagerMessage::SetMonitoring { order_id, monitoring: mon, reply } => {
                if mon {
                    monitoring.insert(order_id, true);
                } else {
                    monitoring.remove(&order_id);
                }
                let _ = reply.send(());
            }
            TradeManagerMessage::Shutdown { reply } => {
                let _ = reply.send(db_cache.clone());
                info!(
                    "TradeManager: Shutdown, returned cache of size {}",
                    db_cache.len()
                );
                db_cache.clear();
                break;
            }
        }
    }
}

pub async fn execute_buy_actor(
    client: Arc<KrakenClient>,
    state_manager: Arc<StateManager>,
    mut rx: mpsc::Receiver<BuyMessage>,
) {
    while let Some(msg) = rx.recv().await {
        match msg {
            BuyMessage::Execute {
                pair,
                amount,
                limit_price,
                post_only,
                timeout,
                symbol,
                reason,
                reply,
            } => {
                let order_result = client.as_ref()
                    .create_order(&pair, "buy", amount, limit_price, post_only)
                    .await;

                match order_result {
                    Ok(order_id) => {
                        // Store order info with timeout for monitoring
                        let order_info = OrderInfo::new(
                            amount,
                            SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs_f64(),
                            timeout,
                            reason.clone(),
                            "buy".to_string(),
                            symbol.clone(),
                            vec![],
                            Decimal::ZERO,
                        );

                        if let Err(e) = state_manager
                            .update_open_order(pair.clone(), order_id.clone(), order_info)
                            .await
                        {
                            error!(
                                "ExecuteBuy[{}]: Failed to store order info for {}: {}",
                                pair, order_id, e
                            );
                        }

                        info!(
                            "ExecuteBuy[{}]: Order placed, order_id: {}, reason: {}",
                            pair, order_id, reason
                        );
                        let _ = reply.send(Some(order_id));
                    }
                    Err(e) => {
                        info!("ExecuteBuy[{}]: Failed to execute buy: {}", pair, e);
                        let _ = reply.send(None);
                    }
                }
            }
            BuyMessage::Shutdown { reply } => {
                let _ = reply.send(());
                info!("ExecuteBuy: Shutdown completed");
                break;
            }
        }
    }
}

pub async fn execute_sell_actor(
    client: Arc<KrakenClient>,
    state_manager: Arc<StateManager>,
    mut rx: mpsc::Receiver<SellMessage>,
) {
    while let Some(msg) = rx.recv().await {
        match msg {
            SellMessage::Execute {
                pair,
                amount,
                limit_price,
                post_only,
                timeout,
                symbol,
                reason,
                buy_trade_ids,
                total_buy_qty,
                reply,
            } => {
                let order_result = client.as_ref()
                    .create_order(&pair, "sell", amount, limit_price, post_only)
                    .await;

                match order_result {
                    Ok(order_id) => {
                        // Store order info with timeout for monitoring
                        let order_info = OrderInfo::new(
                            amount,
                            SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs_f64(),
                            timeout,
                            reason.clone(),
                            "sell".to_string(),
                            symbol.clone(),
                            buy_trade_ids,
                            total_buy_qty,
                        );

                        if let Err(e) = state_manager
                            .update_open_order(pair.clone(), order_id.clone(), order_info)
                            .await
                        {
                            error!(
                                "ExecuteSell[{}]: Failed to store order info for {}: {}",
                                pair, order_id, e
                            );
                        }

                        info!(
                            "ExecuteSell[{}]: Order placed, order_id: {}, reason: {}",
                            pair, order_id, reason
                        );
                        let _ = reply.send(Some(order_id));
                    }
                    Err(e) => {
                        info!("ExecuteSell[{}]: Failed to execute sell: {}", pair, e);
                        let _ = reply.send(None);
                    }
                }
            }
            SellMessage::Shutdown { reply } => {
                let _ = reply.send(());
                info!("ExecuteSell: Shutdown completed");
                break;
            }
        }
    }
}

pub async fn database_actor(config: Config, mut rx: mpsc::Receiver<DatabaseMessage>) {
    let max_retries = config.database.max_retries.value as u32;
    let pool = match create_pool(&config).await {
        Ok(pool) => pool,
        Err(e) => {
            error!("DatabaseActor: Failed to create connection pool: {}", e);
            return;
        }
    };
    let mut cache = Vec::new();
    let mut last_retry = std::time::Instant::now();
    let retry_duration = Duration::from_secs(10);
    const FLUSH_THRESHOLD: usize = 1000;  // NEW: Flush if >1000 on UpdateTrades

    loop {
        let timeout_result = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;

        match timeout_result {
            Ok(Some(msg)) => {
                let mut new_cache = Vec::new();
                let mut client = match pool.get().await {
                    Ok(client) => client,
                    Err(e) => {
                        error!("DatabaseActor: Failed to get client from pool: {}", e);
                        continue;
                    }
                };

                match msg {
                    DatabaseMessage::InitializeTables { pairs, reply } => {
                        let result = async {
                            let transaction = client.transaction().await?;
                            transaction
                                .execute(
                                    "CREATE TABLE IF NOT EXISTS positions (
                                        pair TEXT PRIMARY KEY,
                                        total_usd NUMERIC(38,18),
                                        total_quantity NUMERIC(38,18),
                                        usd_balance NUMERIC(38,18),
                                        last_updated TEXT,
                                        total_fees NUMERIC(38,18),
                                        total_pl NUMERIC(38,18),
                                        highest_price_since_buy NUMERIC(38,18),
                                        last_synced TEXT
                                    )",
                                    &[],
                                )
                                .await?;
                            transaction
                                .execute(
                                    "CREATE TABLE IF NOT EXISTS trades (
                                        timestamp TEXT,
                                        pair TEXT,
                                        trade_id TEXT PRIMARY KEY,
                                        order_id TEXT,
                                        type TEXT,
                                        amount NUMERIC(38,18),
                                        execution_price NUMERIC(38,18),
                                        fees NUMERIC(38,18),
                                        fee_percentage NUMERIC(38,18),
                                        profit NUMERIC(38,18),
                                        profit_percentage NUMERIC(38,18),
                                        reason TEXT,
                                        avg_cost_basis NUMERIC(38,18),
                                        slippage NUMERIC(38,18),
                                        remaining_amount NUMERIC(38,18),
                                        open INTEGER,
                                        partial_open INTEGER
                                    )",
                                    &[],
                                )
                                .await?;
                            for pair in pairs.clone() {
                                transaction
                                    .execute(
                                        &format!(
                                            "CREATE TABLE IF NOT EXISTS {}_market_data (
                                            timestamp TEXT,
                                            open_price NUMERIC(38,18),
                                            high_price NUMERIC(38,18),
                                            low_price NUMERIC(38,18),
                                            highest_price_period NUMERIC(38,18),
                                            volume NUMERIC(38,18),
                                            bid_price NUMERIC(38,18),
                                            ask_price NUMERIC(38,18),
                                            bid_depth NUMERIC(38,18),
                                            ask_depth NUMERIC(38,18),
                                            liquidity NUMERIC(38,18),
                                            close_price NUMERIC(38,18)
                                        )",
                                            pair.to_lowercase()
                                        ),
                                        &[],
                                    )
                                    .await?;
                            }
                            transaction.commit().await?;
                            Ok::<(), tokio_postgres::Error>(())
                        }
                        .await;

                        if let Err(e) = result {
                            error!("DatabaseActor: Failed to initialize tables: {}", e);
                        }
                        let _ = reply.send(());
                        info!("DatabaseActor: Initialized tables for pairs: {:?}", pairs);
                    }
                    DatabaseMessage::UpdatePositions {
                        pair,
                        position,
                        is_buy,
                        reply,
                    } => {
                        if let Err(e) = update_position(&mut client, &pair, &position, is_buy).await
                        {
                            error!("DatabaseActor: Failed to update positions for {}: {}. Position: {:?}", pair, e, position);
                            new_cache.push(CacheEntry::UpdatePositions {
                                pair,
                                position,
                                is_buy,
                            });
                        }
                        let _ = reply.send(());
                    }
                    DatabaseMessage::UpdateTrades { trade_data, reply } => {  // Note: mut to allow potential re-use, but clone for safety
                        let mut retry_count = 0;
                        let mut success = false;
                        while retry_count < max_retries {
                            match update_trade(&mut client, &trade_data).await {
                                Ok(_) => {
                                    success = true;
                                    break;
                                }
                                Err(e) => {
                                    error!(
                                        "DatabaseActor: Failed to update trades for {} (attempt {}/{}): {}. Trade: {:?}",
                                        trade_data.trade_id, retry_count + 1, max_retries, e, trade_data
                                    );
                                    retry_count += 1;
                                    if retry_count < max_retries {
                                        let backoff = Duration::from_secs(2u64.pow(retry_count));
                                        tokio::time::sleep(backoff).await;
                                    }
                                }
                            }
                        }
                        if !success {
                            new_cache.push(CacheEntry::UpdateTrades { trade_data: trade_data.clone() });
                        }
                        let _ = reply.send(());

                        // NEW: Flush cache if >1000 on UpdateTrades
                        if cache.len() > FLUSH_THRESHOLD {
                            let batch_size = 100;
                            for chunk in cache.chunks_mut(batch_size) {
                                let mut retry_client = match pool.get().await {
                                    Ok(c) => c,
                                    Err(e) => {
                                        error!("DatabaseActor: Failed pool get for flush: {}", e);
                                        break;
                                    }
                                };
                                for entry in chunk.iter_mut() {
                                    match entry {
                                        CacheEntry::UpdatePositions { pair, position, is_buy } => {
                                            if let Err(e) = update_position(&mut retry_client, pair, position, *is_buy).await {
                                                error!("Flush failed UpdatePositions for {}: {}", pair, e);
                                            }
                                        }
                                        CacheEntry::UpdateTrades { trade_data } => {
                                            if let Err(e) = update_trade(&mut retry_client, trade_data).await {
                                                error!("Flush failed UpdateTrades for {}: {}", trade_data.trade_id, e);
                                            }
                                        }
                                        CacheEntry::StoreMarketData { market_data } => {
                                            if let Err(e) = store_market_data(&mut retry_client, market_data).await {
                                                error!("Flush failed StoreMarketData for {}: {}", market_data.pair, e);
                                            }
                                        }
                                        CacheEntry::CancelOrder { .. } => {
                                            // Skip CancelOrder in flush (handled elsewhere)
                                        }
                                    }
                                }
                            }
                            cache.clear();
                            info!("DatabaseActor: Flushed cache batch on UpdateTrades (was >{})", FLUSH_THRESHOLD);
                        }
                    }
                    DatabaseMessage::GetTradesForOrder {
                        order_id,
                        pair,
                        startup_time,
                        reply,
                    } => {
                        let result = get_trades_for_order(&client, &order_id, &pair, startup_time).await;
                        let _ = reply.send(result.unwrap_or_else(|e| {
                            error!(
                                "DatabaseActor: Failed to get trades for order_id {} on pair {}: {}",
                                order_id, pair, e
                            );
                            vec![]
                        }));
                    }
                    DatabaseMessage::StoreMarketData { market_data, reply } => {
                        if let Err(e) = store_market_data(&mut client, &market_data).await {
                            error!("DatabaseActor: Failed to store market data for {}: {}. Market data: {:?}", market_data.pair, e, market_data);
                            new_cache.push(CacheEntry::StoreMarketData { market_data });
                        }
                        let _ = reply.send(());
                    }
                    DatabaseMessage::GetAllPositions { reply } => {
                        let result = get_all_positions(&client).await;
                        let _ = reply.send(result.unwrap_or_else(|e| {
                            error!("DatabaseActor: Failed to get all positions: {}", e);
                            vec![]
                        }));
                    }
                    DatabaseMessage::GetOpenTrades { pair, reply } => {
                        let result = get_open_trades(&client, &pair).await;
                        let _ = reply.send(result.unwrap_or_else(|e| {
                            error!(
                                "DatabaseActor: Failed to get open trades for {}: {}",
                                pair, e
                            );
                            vec![]
                        }));
                    }
                    DatabaseMessage::GetMarketDataHistory {
                        pair,
                        since,
                        limit,
                        reply,
                    } => {
                        let result = get_market_data_history(&client, &pair, &since, limit).await;
                        let _ = reply.send(result.unwrap_or_else(|e| {
                            error!(
                                "DatabaseActor: Failed to get market data history for {}: {}",
                                pair, e
                            );
                            vec![]
                        }));
                    }
                    DatabaseMessage::GetTradeFees {
                        pair,
                        trade_id,
                        reply,
                    } => {
                        let result = get_trade_fees(&client, &pair, &trade_id).await;
                        let _ = reply.send(result.unwrap_or_else(|e| {
                            error!(
                                "DatabaseActor: Failed to get trade fees for {} trade_id {}: {}",
                                pair, trade_id, e
                            );
                            None
                        }));
                    }
                    DatabaseMessage::CheckTradeExists {
                        pair,
                        trade_id,
                        reply,
                    } => {
                        let result = check_trade_exists(&client, &pair, &trade_id).await;
                        let _ = reply.send(result.unwrap_or(false));
                    }
                    DatabaseMessage::InsertTrade { trade_data, reply } => {
                        if let Err(e) = insert_trade(&mut client, &trade_data).await {
                            error!(
                                "DatabaseActor: Failed to insert trade_id {} for pair {}: {}. Trade: {:?}",
                                trade_data.trade_id, trade_data.pair, e, trade_data
                            );
                            new_cache.push(CacheEntry::UpdateTrades { trade_data });
                        }
                        let _ = reply.send(());
                    }
                    DatabaseMessage::GetLatestStopLossTimestamp { pair, reply } => {
                        let result = get_latest_stop_loss_timestamp(&client, &pair).await;
                        let _ = reply.send(result.unwrap_or(None));
                    }
                    DatabaseMessage::GetTotalPlForPair { pair, reply } => {
                        let result = get_total_pl_for_pair(&client, &pair).await;
                        let _ = reply.send(result.unwrap_or(Decimal::ZERO));
                    }
                    DatabaseMessage::GetTotalPlForCrypto { reply } => {
                        let result = get_total_pl_for_crypto(&client).await;
                        let _ = reply.send(result.unwrap_or(Decimal::ZERO));
                    }

                    DatabaseMessage::Shutdown {
                        reply,
                        trade_manager_cache,
                    } => {
                        let mut combined_cache = cache;
                        combined_cache.extend(trade_manager_cache);
                        let mut new_cache = Vec::new();
                        let mut failed_entries = Vec::new();

                        for cached_entry in combined_cache {
                            let cached_entry_clone = cached_entry.clone();
                            match cached_entry {
                                CacheEntry::UpdatePositions {
                                    pair,
                                    position,
                                    is_buy,
                                } => {
                                    if let Err(e) =
                                        update_position(&mut client, &pair, &position, is_buy).await
                                    {
                                        error!("DatabaseActor: Failed to flush cached update_positions for {}: {}. Position: {:?}", pair, e, position);
                                        new_cache.push(cached_entry_clone.clone());
                                        failed_entries.push(cached_entry_clone);
                                    }
                                }
                                CacheEntry::UpdateTrades { trade_data } => {
                                    if let Err(e) = update_trade(&mut client, &trade_data).await {
                                        error!("DatabaseActor: Failed to flush cached update_trades for {}: {}. Trade: {:?}", trade_data.trade_id, e, trade_data);
                                        new_cache.push(cached_entry_clone.clone());
                                        failed_entries.push(cached_entry_clone);
                                    }
                                }
                                CacheEntry::StoreMarketData { market_data } => {
                                    if let Err(e) =
                                        store_market_data(&mut client, &market_data).await
                                    {
                                        error!("DatabaseActor: Failed to flush cached store_market_data for {}: {}. Market data: {:?}", market_data.pair, e, market_data);
                                        new_cache.push(cached_entry_clone.clone());
                                        failed_entries.push(cached_entry_clone);
                                    }
                                }
                                CacheEntry::CancelOrder { pair, order_id, .. } => {
                                    info!("DatabaseActor: CancelOrder entry for pair {}, order_id {} skipped during shutdown; handled in trading logic", pair, order_id);
                                }
                            }
                        }

                        if !failed_entries.is_empty() {
                            let failed_log = format!(
                                "Failed cache entries during shutdown: {:?}",
                                failed_entries
                            );
                            if let Ok(mut file) = OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open("log/error_log.txt")
                            {
                                let _ = file.write_all(failed_log.as_bytes());
                            }
                            error!(
                                "DatabaseActor: Logged {} failed cache entries during shutdown",
                                failed_entries.len()
                            );
                        }
                        let _ = reply.send(());
                        info!("DatabaseActor: Shutdown completed");
                        return;
                    }
                }

                cache.extend(new_cache);
                if cache.len() >= config.database.cache_size.value as usize {
                    error!("DatabaseActor: Cache size limit reached: {}", cache.len());
                    cache.drain(
                        0..cache
                            .len()
                            .saturating_sub(config.database.cache_size.value as usize / 2),
                    );
                }
            }
            Ok(None) => break,
            Err(_) => {
                if last_retry.elapsed() >= retry_duration {
                    last_retry = std::time::Instant::now();
                    if !cache.is_empty() {
                        let mut retry_client = match pool.get().await {
                            Ok(client) => client,
                            Err(e) => {
                                error!(
                                    "DatabaseActor: Failed to get client from pool for retry: {}",
                                    e
                                );
                                continue;
                            }
                        };
                        let mut new_cache = Vec::new();
                        for cached_entry in cache {
                            let cached_entry_clone = cached_entry.clone();
                            match cached_entry {
                                CacheEntry::UpdatePositions {
                                    pair,
                                    position,
                                    is_buy,
                                } => {
                                    if let Err(e) =
                                        update_position(&mut retry_client, &pair, &position, is_buy)
                                            .await
                                    {
                                        error!("DatabaseActor: Failed to retry cached update_positions for {}: {}. Position: {:?}", pair, e, position);
                                        new_cache.push(cached_entry_clone);
                                    }
                                }
                                CacheEntry::UpdateTrades { trade_data } => {
                                    if let Err(e) =
                                        update_trade(&mut retry_client, &trade_data).await
                                    {
                                        error!(
                                            "DatabaseActor: Failed to retry cached update_trades for {}: {}. Trade: {:?}",
                                            trade_data.trade_id, e, trade_data
                                        );
                                        new_cache.push(cached_entry_clone);
                                    }
                                }
                                CacheEntry::StoreMarketData { market_data } => {
                                    if let Err(e) =
                                        store_market_data(&mut retry_client, &market_data).await
                                    {
                                        error!("DatabaseActor: Failed to retry cached store_market_data for {}: {}. Market data: {:?}", market_data.pair, e, market_data);
                                        new_cache.push(cached_entry_clone);
                                    }
                                }
                                CacheEntry::CancelOrder { pair, order_id, .. } => {
                                    info!("DatabaseActor: CancelOrder entry for pair {}, order_id {} skipped during retry; handled in trading logic", pair, order_id);
                                }
                            }
                        }
                        cache = new_cache;
                        if cache.len() >= config.database.cache_size.value as usize {
                            error!(
                                "DatabaseActor: Cache size limit reached after retry: {}",
                                cache.len()
                            );
                            cache.drain(
                                0..cache
                                    .len()
                                    .saturating_sub(config.database.cache_size.value as usize / 2),
                            );
                        }
                        info!("DatabaseActor: Retried cache, new size: {}", cache.len());
                    }
                }
            }
        }
    }
}