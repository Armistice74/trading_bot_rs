// statemanager.rs
// Description: Coordinates state management using actors for positions, prices, market data, trades/orders, buy/sell execution, and database ops.

// IMPORTS

use crate::actors::{
    database_actor, execute_buy_actor, execute_sell_actor, market_data_actor, positions_actor,
    price_actor, trade_manager_actor,
};
use crate::api::KrakenClient;
use crate::config::Config;
use anyhow::{anyhow, Result};
use std::time::Duration;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{atomic::{AtomicBool, Ordering}, Arc};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};
use crate::config::load_config;
use rust_decimal::prelude::*;
use crate::processing::processing::post_process_trade;
use tokio::sync::Mutex;

// STRUCTS AND ENUMS

pub struct StateManager {
    pub positions_tx: mpsc::Sender<PositionsMessage>,
    pub price_txs: HashMap<String, mpsc::Sender<PriceMessage>>,
    pub market_data_txs: HashMap<String, mpsc::Sender<MarketDataMessage>>,
    pub trade_manager_tx: mpsc::Sender<TradeManagerMessage>,
    pub execute_buy_tx: mpsc::Sender<BuyMessage>,
    pub execute_sell_tx: mpsc::Sender<SellMessage>,
    pub database_tx: mpsc::Sender<DatabaseMessage>,
    running: Arc<AtomicBool>,
    completion_senders: HashMap<String, mpsc::UnboundedSender<OrderComplete>>,
    completion_receivers: Arc<Mutex<HashMap<String, mpsc::UnboundedReceiver<OrderComplete>>>>,
    usd_balance: Arc<Mutex<Decimal>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Position {
    pub pair: String,
    pub total_usd: Decimal,
    pub total_quantity: Decimal,
    pub usd_balance: Decimal,
    pub last_updated: String,
    pub total_fees: Decimal,
    pub total_pl: Decimal,
    pub highest_price_since_buy: Decimal,
    pub last_synced: String,
}

impl Position {
    pub fn new(pair: String) -> Self {
        Position {
            pair,
            total_usd: Decimal::ZERO,
            total_quantity: Decimal::ZERO,
            usd_balance: Decimal::ZERO,
            last_updated: String::new(),
            total_fees: Decimal::ZERO,
            total_pl: Decimal::ZERO,
            highest_price_since_buy: Decimal::ZERO,
            last_synced: String::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PriceData {
    pub close_price: Decimal,
    pub bid_price: Decimal,
    pub ask_price: Decimal,
}

impl PriceData {
    pub fn new(close_price: Decimal, bid_price: Decimal, ask_price: Decimal) -> Self {
        PriceData {
            close_price,
            bid_price,
            ask_price,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderInfo {
    pub amount: Decimal,
    pub start_time: f64,
    pub timeout: f64,
    pub reason: String,
    pub trade_type: String,
    pub symbol: String,
    pub trade_ids: Vec<String>,
    pub avg_cost_basis: Decimal,
    pub vol: Decimal,
    pub buy_trade_ids: Vec<String>,
    pub total_buy_qty: Decimal,
}

impl OrderInfo {
    pub fn new(
        amount: Decimal,
        start_time: f64,
        timeout: f64,
        reason: String,
        trade_type: String,
        symbol: String,
        buy_trade_ids: Vec<String>,
        total_buy_qty: Decimal,
    ) -> Self {
        OrderInfo {
            amount,
            start_time,
            timeout,
            reason,
            trade_type,
            symbol,
            trade_ids: Vec::new(),
            avg_cost_basis: Decimal::ZERO,
            vol: amount,
            buy_trade_ids,
            total_buy_qty,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MarketData {
    pub timestamp: String,
    pub pair: String,
    pub open_price: Decimal,
    pub high_price: Decimal,
    pub low_price: Decimal,
    pub highest_price_period: Decimal,
    pub volume: Decimal,
    pub bid_price: Decimal,
    pub ask_price: Decimal,
    pub bid_depth: Decimal,
    pub ask_depth: Decimal,
    pub liquidity: Decimal,
    pub close_price: Decimal,
}

impl MarketData {
    pub fn new(pair: String) -> Self {
        MarketData {
            timestamp: String::new(),
            pair,
            open_price: Decimal::ZERO,
            high_price: Decimal::ZERO,
            low_price: Decimal::ZERO,
            highest_price_period: Decimal::ZERO,
            volume: Decimal::ZERO,
            bid_price: Decimal::ZERO,
            ask_price: Decimal::ZERO,
            bid_depth: Decimal::ZERO,
            ask_depth: Decimal::ZERO,
            liquidity: Decimal::ZERO,
            close_price: Decimal::ZERO,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Trade {
    pub timestamp: String,
    pub pair: String,
    pub trade_id: String,
    pub order_id: String,
    pub trade_type: String,
    pub amount: Decimal,
    pub execution_price: Decimal,
    pub fees: Decimal,
    pub fee_percentage: Decimal,
    pub profit: Decimal,
    pub profit_percentage: Decimal,
    pub reason: String,
    pub avg_cost_basis: Decimal,
    pub slippage: Decimal,
    pub remaining_amount: Decimal,
    pub open: i32,
    pub partial_open: i32,
}

impl Trade {
    pub fn new(pair: String, trade_id: String) -> Self {
        Trade {
            timestamp: String::new(),
            pair,
            trade_id,
            order_id: String::new(),
            trade_type: String::new(),
            amount: Decimal::ZERO,
            execution_price: Decimal::ZERO,
            fees: Decimal::ZERO,
            fee_percentage: Decimal::ZERO,
            profit: Decimal::ZERO,
            profit_percentage: Decimal::ZERO,
            reason: String::new(),
            avg_cost_basis: Decimal::ZERO,
            slippage: Decimal::ZERO,
            remaining_amount: Decimal::ZERO,
            open: 0,
            partial_open: 0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum CacheEntry {
    UpdatePositions {
        pair: String,
        position: Position,
        is_buy: bool,
    },
    UpdateTrades {
        trade_data: Trade,
    },
    StoreMarketData {
        market_data: MarketData,
    },
    CancelOrder {
        pair: String,
        order_id: String,
        symbol: String,
    },
}

#[derive(Debug)]
pub enum PositionsMessage {
    GetPosition {
        pair: String,
        reply: oneshot::Sender<Position>,
    },
    UpdatePosition {
        pair: String,
        position: Position,
        reply: oneshot::Sender<()>,
    },
    GetAllPositions {
        reply: oneshot::Sender<HashMap<String, Position>>,
    },
    SyncUsdBalance {
        balance: Decimal,
        reply: oneshot::Sender<()>,
    },
    GetCachedBalance {
        pair: String,
        ttl: Duration,
        reply: oneshot::Sender<Option<Decimal>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

#[derive(Debug)]
pub enum PriceMessage {
    GetPrice {
        pair: String,
        reply: oneshot::Sender<PriceData>,
    },
    UpdatePrice {
        pair: String,
        price_data: PriceData,
        reply: oneshot::Sender<()>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

#[derive(Debug)]
pub enum MarketDataMessage {
    GetMarketData {
        pair: String,
        reply: oneshot::Sender<(MarketData, f64)>,
    },
    UpdateMarketData {
        pair: String,
        market_data: MarketData,
        timestamp: f64,
        reply: oneshot::Sender<()>,
    },
    UpdateOhlc {
        ohlc: OhlcUpdate,
        reply: oneshot::Sender<()>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

#[derive(Debug)]
pub enum TradeManagerMessage {
    UpdateOpenOrder {
        pair: String,
        order_id: String,
        order_info: OrderInfo,
        reply: oneshot::Sender<()>,
    },
    RemoveOpenOrder {
        pair: String,
        order_id: String,
        reply: oneshot::Sender<()>,
    },
    GetOpenOrders {
        pair: String,
        reply: oneshot::Sender<HashMap<String, OrderInfo>>,
    },
    AddTradeId {
        pair: String,
        trade_id: String,
        order_id: String,
        reply: oneshot::Sender<()>,
    },
    RemoveTradeId {
        pair: String,
        trade_id: String,
        reply: oneshot::Sender<bool>,
    },
    HasTradeId {
        pair: String,
        trade_id: String,
        reply: oneshot::Sender<bool>,
    },
    CacheCancellation {
        pair: String,
        order_id: String,
        symbol: String,
        reply: oneshot::Sender<()>,
    },
    GetOrderReason {
        pair: String,
        order_id: String,
        reply: oneshot::Sender<Option<String>>,
    },
    UpdateOrderStatus {
        order_id: String,
        pair: Option<String>,
        status: String,
        vol_exec: Decimal,
        reply: oneshot::Sender<()>,
    },
    GetOrderStatus {
        order_id: String,
        reply: oneshot::Sender<Option<OrderStatus>>,
    },
    Shutdown {
        reply: oneshot::Sender<Vec<CacheEntry>>,
    },
    IsMonitoring {
        order_id: String,
        reply: oneshot::Sender<bool>,
    },
    SetMonitoring {
        order_id: String,
        monitoring: bool,
        reply: oneshot::Sender<()>,
    },
}

#[derive(Debug)]
pub enum BuyMessage {
    Execute {
        pair: String,
        amount: Decimal,
        limit_price: Decimal,
        post_only: bool,
        timeout: f64,
        symbol: String,
        reason: String,
        reply: oneshot::Sender<Option<String>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

#[derive(Debug)]
pub enum SellMessage {
    Execute {
        pair: String,
        amount: Decimal,
        limit_price: Decimal,
        post_only: bool,
        timeout: f64,
        symbol: String,
        reason: String,
        buy_trade_ids: Vec<String>,
        total_buy_qty: Decimal,
        reply: oneshot::Sender<Option<String>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

#[derive(Debug)]
pub enum DatabaseMessage {
    InitializeTables {
        pairs: Vec<String>,
        reply: oneshot::Sender<()>,
    },
    UpdatePositions {
        pair: String,
        position: Position,
        is_buy: bool,
        reply: oneshot::Sender<()>,
    },
    UpdateTrades {
        trade_data: Trade,
        reply: oneshot::Sender<()>,
    },
    StoreMarketData {
        market_data: MarketData,
        reply: oneshot::Sender<()>,
    },
    GetAllPositions {
        reply: oneshot::Sender<Vec<Position>>,
    },
    GetOpenTrades {
        pair: String,
        reply: oneshot::Sender<Vec<Trade>>,
    },
    GetMarketDataHistory {
        pair: String,
        since: String,
        limit: i64,
        reply: oneshot::Sender<Vec<MarketDataHistory>>,
    },
    GetTradeFees {
        pair: String,
        trade_id: String,
        reply: oneshot::Sender<Option<Decimal>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
        trade_manager_cache: Vec<CacheEntry>,
    },
    CheckTradeExists {
        pair: String,
        trade_id: String,
        reply: oneshot::Sender<bool>,
    },
    InsertTrade {
        trade_data: Trade,
        reply: oneshot::Sender<()>,
    },
    GetTradesForOrder {
        order_id: String,
        pair: String,
        startup_time: u64,
        reply: oneshot::Sender<Vec<Trade>>,
    },
    GetLatestStopLossTimestamp {
        pair: String,
        reply: oneshot::Sender<Option<String>>,
    },
    GetTotalPlForPair {
    pair: String,
    reply: oneshot::Sender<Decimal>,
    },
    GetTotalPlForCrypto {
        reply: oneshot::Sender<Decimal>,
    },
}

#[derive(Debug, Clone)]
pub enum OrderComplete {
    Success(bool),
    Error(Arc<anyhow::Error>),
    Shutdown,
    DelayElapsed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OhlcUpdate {
    pub pair: String,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub timestamp: String,
}

#[derive(Debug, Clone)]
pub struct PriceUpdate {
    pub pair: String,
    pub bid: Decimal,
    pub ask: Decimal,
    pub close_price: Decimal,
    pub timestamp: String,
}

#[derive(Debug, Clone)]
pub struct OrderStatus {
    pub order_id: String,
    pub pair: String,
    pub status: String,
    pub vol_exec: Decimal,
    pub vol: Decimal,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarketDataHistory {
    pub high_price: Decimal,
    pub low_price: Decimal,
    pub close_price: Decimal,
}

impl MarketDataHistory {
    pub fn new(high_price: Decimal, low_price: Decimal, close_price: Decimal) -> Self {
        MarketDataHistory {
            high_price,
            low_price,
            close_price,
        }
    }
}

// IMPLEMENTATION

impl StateManager {
    pub async fn new(config: &Config, client: KrakenClient) -> Result<Arc<Self>> {
        let client_arc = Arc::new(client);
        let (positions_tx, positions_rx) = mpsc::channel(1000);
        let (trade_manager_tx, trade_manager_rx) = mpsc::channel(1000);
        let (execute_buy_tx, execute_buy_rx) = mpsc::channel(1000);
        let (execute_sell_tx, execute_sell_rx) = mpsc::channel(1000);
        let (database_tx, database_rx) = mpsc::channel(1000);

        tokio::spawn(positions_actor(client_arc.clone(), positions_rx));
        tokio::spawn(trade_manager_actor(trade_manager_rx));
        tokio::spawn(database_actor(config.clone(), database_rx));

        let mut price_txs = HashMap::new();
        let mut market_data_txs = HashMap::new();
        let mut completion_senders = HashMap::new();
        let completion_receivers = Arc::new(Mutex::new(HashMap::new()));
        for pair in &config.portfolio.api_pairs.value {
            let (price_tx, price_rx) = mpsc::channel(1000);
            let (market_data_tx, market_data_rx) = mpsc::channel(1000);
            let (completion_tx, completion_rx) = mpsc::unbounded_channel::<OrderComplete>();

            price_txs.insert(pair.clone(), price_tx);
            market_data_txs.insert(pair.clone(), market_data_tx);
            completion_senders.insert(pair.clone(), completion_tx);
            completion_receivers.lock().await.insert(pair.clone(), completion_rx);

            tokio::spawn(price_actor(pair.clone(), price_rx));
            tokio::spawn(market_data_actor(pair.clone(), market_data_rx));
        }

        let state_manager = StateManager {
            positions_tx,
            price_txs,
            market_data_txs,
            trade_manager_tx,
            execute_buy_tx: execute_buy_tx.clone(),
            execute_sell_tx: execute_sell_tx.clone(),
            database_tx,
            running: Arc::new(AtomicBool::new(true)),
            completion_senders,
            completion_receivers,
            usd_balance: Arc::new(Mutex::new(Decimal::ZERO)),
        };

        let state_manager_arc = Arc::new(state_manager);

        tokio::spawn(execute_buy_actor(
            client_arc.clone(),
            state_manager_arc.clone(),
            execute_buy_rx,
        ));
        tokio::spawn(execute_sell_actor(
            client_arc.clone(),
            state_manager_arc.clone(),
            execute_sell_rx,
        ));

        Ok(state_manager_arc)
    }

    pub fn running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);

        let (tx, rx) = oneshot::channel();
        self.positions_tx
            .send(PositionsMessage::Shutdown { reply: tx })
            .await?;
        rx.await?;

        let (tx, rx) = oneshot::channel();
        self.trade_manager_tx
            .send(TradeManagerMessage::Shutdown { reply: tx })
            .await?;
        let cache = rx.await?;

        let (tx, rx) = oneshot::channel();
        self.execute_buy_tx
            .send(BuyMessage::Shutdown { reply: tx })
            .await?;
        rx.await?;

        let (tx, rx) = oneshot::channel();
        self.execute_sell_tx
            .send(SellMessage::Shutdown { reply: tx })
            .await?;
        rx.await?;

        let (tx, rx) = oneshot::channel();
        self.database_tx
            .send(DatabaseMessage::Shutdown {
                reply: tx,
                trade_manager_cache: cache,
            })
            .await?;
        rx.await?;

        for (_, price_tx) in &self.price_txs {
            let (tx, rx) = oneshot::channel();
            price_tx.send(PriceMessage::Shutdown { reply: tx }).await?;
            rx.await?;
        }

        for (_, market_data_tx) in &self.market_data_txs {
            let (tx, rx) = oneshot::channel();
            market_data_tx
                .send(MarketDataMessage::Shutdown { reply: tx })
                .await?;
            rx.await?;
        }

        for (pair, tx) in &self.completion_senders {
            if let Err(e) = tx.send(OrderComplete::Shutdown) {
                warn!("Failed to send shutdown to completion channel for {}: {}", pair, e);
            }
        }
        info!("Sent shutdown to {} completion channels", self.completion_senders.len());

        Ok(())
    }

    pub async fn get_completion_rx(&self, pair: &str) -> Option<mpsc::UnboundedReceiver<OrderComplete>> {
        let mut receivers = self.completion_receivers.lock().await;
        receivers.remove(pair)
    }

    pub async fn get_cached_balance(&self, pair: String, ttl: Duration) -> Result<Option<Decimal>> {
        let (tx, rx) = oneshot::channel();
        self.positions_tx
            .send(PositionsMessage::GetCachedBalance { pair, ttl, reply: tx })
            .await?;
        Ok(rx.await?)
    }

    pub async fn get_position(&self, pair: String) -> Result<Position> {
        let (tx, rx) = oneshot::channel();
        self.positions_tx
            .send(PositionsMessage::GetPosition { pair, reply: tx })
            .await?;
        Ok(rx.await?)
    }

    async fn send_position_update(
        &self,
        pair: String,
        position: Position,
        is_buy: bool,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.positions_tx
            .send(PositionsMessage::UpdatePosition {
                pair: pair.clone(),
                position: position.clone(),
                reply: tx,
            })
            .await?;
        rx.await?;

        let (tx, rx) = oneshot::channel();
        self.database_tx
            .send(DatabaseMessage::UpdatePositions {
                pair,
                position,
                is_buy,
                reply: tx,
            })
            .await?;
        rx.await?;

        Ok(())
    }

    pub async fn update_positions(
        &self,
        pair: String,
        mut position: Position,
        is_buy: bool,
    ) -> Result<()> {
        self.send_position_update(pair.clone(), position.clone(), is_buy).await?;

        if pair != "USD" {
            let usd_total_pl = self.get_total_pl_for_crypto().await?;
            let mut usd_position = self.get_position("USD".to_string()).await?;
            usd_position.total_pl = usd_total_pl;
            self.send_position_update("USD".to_string(), usd_position, false).await?;
        }

        Ok(())
    }

    pub async fn get_trades_for_order(
        &self,
        order_id: String,
        pair: String,
        startup_time: u64,
    ) -> Result<Vec<Trade>> {
        let (tx, rx) = oneshot::channel();
        self.database_tx
            .send(DatabaseMessage::GetTradesForOrder {
                order_id,
                pair,
                startup_time,
                reply: tx,
            })
            .await?;
        Ok(rx.await?)
    }

    pub async fn sync_usd_balance(&self, balance: Decimal) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.positions_tx
            .send(PositionsMessage::SyncUsdBalance { balance, reply: tx })
            .await?;
        rx.await?;

        let usd_position = Position {
            pair: "USD".to_string(),
            usd_balance: balance,
            last_updated: chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
            last_synced: chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
            ..Position::new("USD".to_string())
        };
        self.update_positions("USD".to_string(), usd_position, false)
            .await?;

        let mut locked_balance = self.usd_balance.lock().await;
        *locked_balance = balance;

        Ok(())
    }

    pub async fn get_price(&self, pair: String) -> Result<PriceData> {
        if let Some(price_tx) = self.price_txs.get(&pair) {
            let (tx, rx) = oneshot::channel();
            price_tx
                .send(PriceMessage::GetPrice { pair, reply: tx })
                .await?;
            Ok(rx.await?)
        } else {
            Err(anyhow!("Price actor not found for pair: {}", pair))
        }
    }

    pub async fn update_price(&self, pair: String, price_data: PriceData) -> Result<()> {
        if let Some(price_tx) = self.price_txs.get(&pair) {
            let (tx, rx) = oneshot::channel();
            price_tx
                .send(PriceMessage::UpdatePrice {
                    pair,
                    price_data,
                    reply: tx,
                })
                .await?;
            rx.await?;
        }
        Ok(())
    }

    pub async fn get_market_data(&self, pair: String) -> Result<(MarketData, f64)> {
        if let Some(market_data_tx) = self.market_data_txs.get(&pair) {
            let (tx, rx) = oneshot::channel();
            market_data_tx
                .send(MarketDataMessage::GetMarketData { pair, reply: tx })
                .await?;
            Ok(rx.await?)
        } else {
            Err(anyhow!("Market data actor not found for pair: {}", pair))
        }
    }

    pub async fn store_market_data(&self, market_data: MarketData) -> Result<()> {
        if let Some(market_data_tx) = self.market_data_txs.get(&market_data.pair) {
            let timestamp = chrono::Local::now().timestamp() as f64;
            let (tx, rx) = oneshot::channel();
            market_data_tx
                .send(MarketDataMessage::UpdateMarketData {
                    pair: market_data.pair.clone(),
                    market_data: market_data.clone(),
                    timestamp,
                    reply: tx,
                })
                .await?;
            rx.await?;
        }

        let (tx, rx) = oneshot::channel();
        self.database_tx
            .send(DatabaseMessage::StoreMarketData {
                market_data,
                reply: tx,
            })
            .await?;
        rx.await?;

        Ok(())
    }

    pub async fn initialize_tables(&self, pairs: Vec<String>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.database_tx
            .send(DatabaseMessage::InitializeTables { pairs, reply: tx })
            .await?;
        rx.await?;
        Ok(())
    }

    pub async fn get_open_trades(&self, pair: String) -> Result<Vec<Trade>> {
        let (tx, rx) = oneshot::channel();
        self.database_tx
            .send(DatabaseMessage::GetOpenTrades { pair, reply: tx })
            .await?;
        Ok(rx.await?)
    }

    pub async fn update_trades(self: &Arc<Self>, mut trade_data: Trade) -> Result<()> {
        if trade_data.reason.is_empty() {
            let (tx, rx) = oneshot::channel();
            self.trade_manager_tx
                .send(TradeManagerMessage::GetOrderReason {
                    pair: trade_data.pair.clone(),
                    order_id: trade_data.order_id.clone(),
                    reply: tx,
                })
                .await?;
            if let Ok(Some(reason)) = rx.await {
                trade_data.reason = reason;
                info!("Fetched reason '{}' for trade {} via TradeManager", trade_data.reason, trade_data.trade_id);
            } else {
                warn!("reason_unset: no order match for {} in {}", trade_data.order_id, trade_data.trade_id);
            }
        }

        let exists = self.check_trade_exists_in_db(trade_data.pair.clone(), trade_data.trade_id.clone()).await?;

        if !exists {
            let config = load_config()?;
            let pair = trade_data.pair.clone();
            let price_data = self.get_price(pair.clone()).await?;
            post_process_trade(&mut trade_data, &pair, price_data.close_price, self, &config).await?;
        }

        let (tx, rx) = oneshot::channel();
        self.database_tx
            .send(DatabaseMessage::UpdateTrades {
                trade_data: trade_data.clone(),
                reply: tx,
            })
            .await?;
        rx.await?;

        let total_pl = self.get_total_pl_for_pair(trade_data.pair.clone()).await?;
        let mut position = self.get_position(trade_data.pair.clone()).await?;
        position.total_pl = total_pl;
        self.update_positions(trade_data.pair.clone(), position, false).await?;

        Ok(())
    }

    pub async fn get_market_data_history(
        &self,
        pair: String,
        since: String,
        limit: i64,
    ) -> Result<Vec<MarketDataHistory>> {
        let (tx, rx) = oneshot::channel();
        self.database_tx
            .send(DatabaseMessage::GetMarketDataHistory {
                pair,
                since,
                limit,
                reply: tx,
            })
            .await?;
        Ok(rx.await?)
    }

    pub async fn get_trade_fees(&self, pair: String, trade_id: String) -> Result<Option<Decimal>> {
        let (tx, rx) = oneshot::channel();
        self.database_tx
            .send(DatabaseMessage::GetTradeFees {
                pair,
                trade_id,
                reply: tx,
            })
            .await?;
        Ok(rx.await?)
    }

    pub async fn update_open_order(
        &self,
        pair: String,
        order_id: String,
        order_info: OrderInfo,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.trade_manager_tx
            .send(TradeManagerMessage::UpdateOpenOrder {
                pair,
                order_id,
                order_info,
                reply: tx,
            })
            .await?;
        rx.await?;
        Ok(())
    }

    pub async fn remove_open_order(&self, pair: String, order_id: String) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.trade_manager_tx
            .send(TradeManagerMessage::RemoveOpenOrder {
                pair,
                order_id,
                reply: tx,
            })
            .await?;
        rx.await?;
        Ok(())
    }

    pub async fn get_open_orders(&self, pair: String) -> Result<HashMap<String, OrderInfo>> {
        let (tx, rx) = oneshot::channel();
        self.trade_manager_tx
            .send(TradeManagerMessage::GetOpenOrders { pair, reply: tx })
            .await?;
        Ok(rx.await?)
    }

    pub async fn add_trade_id(
        &self,
        pair: String,
        trade_id: String,
        order_id: String,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.trade_manager_tx
            .send(TradeManagerMessage::AddTradeId {
                pair,
                trade_id,
                order_id,
                reply: tx,
            })
            .await?;
        rx.await?;
        Ok(())
    }

    pub async fn remove_trade_id(&self, pair: String, trade_id: String) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.trade_manager_tx
            .send(TradeManagerMessage::RemoveTradeId {
                pair,
                trade_id,
                reply: tx,
            })
            .await?;
        Ok(rx.await?)
    }

    pub async fn has_trade_id(&self, pair: String, trade_id: String) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.trade_manager_tx
            .send(TradeManagerMessage::HasTradeId {
                pair,
                trade_id,
                reply: tx,
            })
            .await?;
        Ok(rx.await?)
    }

    pub async fn check_trade_exists_in_db(&self, pair: String, trade_id: String) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.database_tx
            .send(DatabaseMessage::CheckTradeExists {
                pair: pair.clone(),
                trade_id: trade_id.clone(),
                reply: tx,
            })
            .await?;
        let exists = rx.await?;
        info!(
            "Checked trade_id {} for pair {} in DB, exists: {}",
            trade_id, pair, exists
        );
        Ok(exists)
    }

    pub async fn cache_cancellation(
        &self,
        pair: String,
        order_id: String,
        symbol: String,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.trade_manager_tx
            .send(TradeManagerMessage::CacheCancellation {
                pair,
                order_id,
                symbol,
                reply: tx,
            })
            .await?;
        rx.await?;
        Ok(())
    }

    pub async fn send_completion(&self, pair: String, result: OrderComplete) -> Result<()> {
        if let Some(tx) = self.completion_senders.get(&pair) {
            if tx.send(result).is_err() {
                return Err(anyhow!("Failed to send completion for pair: {} (receiver dropped)", pair));
            }
            Ok(())
        } else {
            Err(anyhow!("No completion channel for pair: {}", pair))
        }
    }

    pub async fn get_all_positions(&self) -> Result<HashMap<String, Position>> {
        let (tx, rx) = oneshot::channel();
        self.positions_tx.send(PositionsMessage::GetAllPositions { reply: tx }).await?;
        Ok(rx.await?)
    }

    pub async fn get_latest_stop_loss_timestamp(&self, pair: String) -> Result<Option<String>> {
        let (tx, rx) = oneshot::channel();
        self.database_tx
            .send(DatabaseMessage::GetLatestStopLossTimestamp { pair, reply: tx })
            .await?;
        Ok(rx.await?)
    }

    pub async fn get_total_pl_for_pair(&self, pair: String) -> Result<Decimal> {
        let (tx, rx) = oneshot::channel();
        self.database_tx
            .send(DatabaseMessage::GetTotalPlForPair { pair, reply: tx })
            .await?;
        Ok(rx.await?)
    }

    pub async fn get_total_pl_for_crypto(&self) -> Result<Decimal> {
        let (tx, rx) = oneshot::channel();
        self.database_tx
            .send(DatabaseMessage::GetTotalPlForCrypto { reply: tx })
            .await?;
        Ok(rx.await?)
    }

    pub async fn is_monitoring(&self, order_id: String) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.trade_manager_tx
            .send(TradeManagerMessage::IsMonitoring { order_id, reply: tx })
            .await?;
        Ok(rx.await?)
    }

    pub async fn set_monitoring(&self, order_id: String, monitoring: bool) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.trade_manager_tx
            .send(TradeManagerMessage::SetMonitoring { order_id, monitoring, reply: tx })
            .await?;
        rx.await?;
        Ok(())
    }

    pub async fn update_usd_balance_delta(&self, delta: Decimal) -> Result<()> {
        let mut locked_balance = self.usd_balance.lock().await;
        *locked_balance += delta;

        let mut usd_position = self.get_position("USD".to_string()).await?;
        usd_position.usd_balance += delta;
        usd_position.last_updated = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        self.update_positions("USD".to_string(), usd_position, false).await?;
        Ok(())
    }

    pub async fn get_usd_balance_locked(&self) -> Result<Decimal> {
        let locked_balance = self.usd_balance.lock().await;
        Ok(*locked_balance)
    }
}