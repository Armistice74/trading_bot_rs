// dal.rs - Data Access Layer

use crate::config::Config;
use serde::{Deserialize, Serialize};
use std::error::Error;
use deadpool_postgres::{Pool, Runtime};
use tokio_postgres::NoTls;
use tracing::{info, error};
use tokio::sync::oneshot;
use reqwest::Client;
use serde_json::{json, Value};
use sha2::{Sha256, Sha512, Digest};
use hmac::{Hmac, Mac};
use base64::{engine::general_purpose, Engine};
use url::Url;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use anyhow::anyhow;
use futures_util::sink::SinkExt as _;
use futures_util::stream::StreamExt as _;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use crate::{record_api_latency, record_error, map_ws_pair};

// Data structures from data.rs
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Position {
    pub pair: String,
    pub total_usd: f64,
    pub total_quantity: f64,
    pub average_buy_price: f64,
    pub usd_balance: f64,
    pub last_updated: String,
    pub buy_fees: f64,
    pub total_fees: f64,
    pub total_pl: f64,
    pub highest_price_since_buy: f64,
    pub last_synced: String,
}

impl Position {
    pub fn new(pair: String) -> Self {
        Position {
            pair,
            total_usd: 0.0,
            total_quantity: 0.0,
            average_buy_price: 0.0,
            usd_balance: 0.0,
            last_updated: String::new(),
            buy_fees: 0.0,
            total_fees: 0.0,
            total_pl: 0.0,
            highest_price_since_buy: 0.0,
            last_synced: String::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PriceData {
    pub close_price: f64,
    pub bid_price: f64,
    pub ask_price: f64,
}

impl PriceData {
    pub fn new(close_price: f64, bid_price: f64, ask_price: f64) -> Self {
        PriceData {
            close_price,
            bid_price,
            ask_price,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct OrderInfo {
    pub amount: f64,
    pub start_time: f64,
    pub timeout: f64,
    pub reason: String,
    pub trade_type: String,
    pub symbol: String,
    pub trade_ids: Vec<String>,
}

#[allow(dead_code)]
impl OrderInfo {
    pub fn new(amount: f64, start_time: f64, timeout: f64, reason: String, trade_type: String, symbol: String) -> Self {
        OrderInfo {
            amount,
            start_time,
            timeout,
            reason,
            trade_type,
            symbol,
            trade_ids: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MarketData {
    pub timestamp: String,
    pub pair: String,
    pub open_price: String,
    pub high_price: String,
    pub low_price: String,
    pub highest_price_period: String,
    pub volume: String,
    pub bid_price: String,
    pub ask_price: String,
    pub bid_depth: String,
    pub ask_depth: String,
    pub liquidity: String,
    pub close_price: String,
}

impl MarketData {
    pub fn new(pair: String) -> Self {
        MarketData {
            timestamp: String::new(),
            pair,
            open_price: String::new(),
            high_price: String::new(),
            low_price: String::new(),
            highest_price_period: String::new(),
            volume: String::new(),
            bid_price: String::new(),
            ask_price: String::new(),
            bid_depth: String::new(),
            ask_depth: String::new(),
            liquidity: String::new(),
            close_price: String::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[allow(dead_code)]
pub struct Trade {
    pub timestamp: String,
    pub pair: String,
    pub trade_id: String,
    pub order_id: String,
    pub trade_type: String,
    pub amount: String,
    pub price: String,
    pub fees: String,
    pub fee_percentage: String,
    pub profit: String,
    pub profit_percentage: String,
    pub reason: String,
    pub average_buy_price: String,
    pub slippage: String,
    pub remaining_amount: String,
    pub open: i32,
    pub partial_open: i32,
}

#[allow(dead_code)]
impl Trade {
    pub fn new(pair: String, trade_id: String) -> Self {
        Trade {
            timestamp: String::new(),
            pair,
            trade_id,
            order_id: String::new(),
            trade_type: String::new(),
            amount: String::new(),
            price: String::new(),
            fees: String::new(),
            fee_percentage: String::new(),
            profit: String::new(),
            profit_percentage: String::new(),
            reason: String::new(),
            average_buy_price: String::new(),
            slippage: String::new(),
            remaining_amount: String::new(),
            open: 0,
            partial_open: 0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct MarketDataHistory {
    pub high_price: String,
    pub low_price: String,
    pub close_price: String,
}

#[allow(dead_code)]
impl MarketDataHistory {
    pub fn new(high_price: String, low_price: String, close_price: String) -> Self {
        MarketDataHistory {
            high_price,
            low_price,
            close_price,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum CacheEntry {
    UpdatePositions { pair: String, position: Position, is_buy: bool },
    UpdateTrades { trade_data: Trade },
    StoreMarketData { market_data: MarketData },
    CancelOrder { pair: String, order_id: String, symbol: String },
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum PositionsMessage {
    GetPosition { pair: String, reply: oneshot::Sender<Position> },
    UpdatePosition { pair: String, position: Position, reply: oneshot::Sender<()> },
    GetAllPositions { reply: oneshot::Sender<std::collections::HashMap<String, Position>> },
    SyncUsdBalance { balance: f64, reply: oneshot::Sender<()> },
    Shutdown { reply: oneshot::Sender<()> },
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum PriceMessage {
    GetPrice { pair: String, reply: oneshot::Sender<PriceData> },
    UpdatePrice { pair: String, price_data: PriceData, reply: oneshot::Sender<()> },
    Shutdown { reply: oneshot::Sender<()> },
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum MarketDataMessage {
    GetMarketData { pair: String, reply: oneshot::Sender<(MarketData, f64)> },
    UpdateMarketData { pair: String, market_data: MarketData, timestamp: f64, reply: oneshot::Sender<()> },
    Shutdown { reply: oneshot::Sender<()> },
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum TradeManagerMessage {
    UpdateOpenOrder { pair: String, order_id: String, order_info: OrderInfo, reply: oneshot::Sender<()> },
    RemoveOpenOrder { pair: String, order_id: String, reply: oneshot::Sender<()> },
    GetOpenOrders { pair: String, reply: oneshot::Sender<std::collections::HashMap<String, OrderInfo>> },
    AddTradeId { pair: String, trade_id: String, order_id: String, reply: oneshot::Sender<()> },
    RemoveTradeId { pair: String, trade_id: String, reply: oneshot::Sender<bool> },
    HasTradeId { pair: String, trade_id: String, reply: oneshot::Sender<bool> },
    CacheCancellation { pair: String, order_id: String, symbol: String, reply: oneshot::Sender<()> },
    Shutdown { reply: oneshot::Sender<Vec<CacheEntry>> },
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum BuyMessage {
    Execute { pair: String, amount: f64, limit_price: f64, post_only: bool, timeout: f64, symbol: String, reason: String, reply: oneshot::Sender<Option<String>> },
    Shutdown { reply: oneshot::Sender<()> },
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum SellMessage {
    Execute { pair: String, amount: f64, limit_price: f64, post_only: bool, timeout: f64, symbol: String, reason: String, reply: oneshot::Sender<Option<String>> },
    Shutdown { reply: oneshot::Sender<()> },
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum DatabaseMessage {
    InitializeTables { pairs: Vec<String>, reply: oneshot::Sender<()> },
    UpdatePositions { pair: String, position: Position, is_buy: bool, reply: oneshot::Sender<()> },
    UpdateTrades { trade_data: Trade, reply: oneshot::Sender<()> },
    StoreMarketData { market_data: MarketData, reply: oneshot::Sender<()> },
    GetAllPositions { reply: oneshot::Sender<Vec<Position>> },
    GetOpenTrades { pair: String, reply: oneshot::Sender<Vec<Trade>> },
    GetMarketDataHistory { pair: String, since: String, limit: i64, reply: oneshot::Sender<Vec<MarketDataHistory>> },
    GetTradeFees { pair: String, trade_id: String, reply: oneshot::Sender<Option<String>> },
    Shutdown { reply: oneshot::Sender<()>, trade_manager_cache: Vec<CacheEntry> },
}

#[derive(Clone)]
pub struct KrakenClient {
    pub api_key: String,
    pub api_secret: String,
    pub base_url: String,
    client: Client,
}

impl KrakenClient {
    pub fn new(api_key: String, api_secret: String) -> Self {
        // Trim any whitespace and remove any potential quotes from the API credentials
        let api_key = api_key.trim().trim_matches('"').trim_matches('\'').to_string();
        let api_secret = api_secret.trim().trim_matches('"').trim_matches('\'').to_string();
        
        info!("Initializing KrakenClient with API key: {}...", &api_key[..std::cmp::min(8, api_key.len())]);
        info!("API secret length: {} characters", api_secret.len());
        
        // Validate that we have actual content after trimming
        if api_key.is_empty() {
            error!("API key is empty after trimming");
        }
        if api_secret.is_empty() {
            error!("API secret is empty after trimming");
        }
        
        KrakenClient {
            api_key,
            api_secret,
            base_url: "https://api.kraken.com".to_string(),
            client: Client::new(),
        }
    }

    async fn sign_request(&self, path: &str, nonce: u64, data: &str) -> Result<String, anyhow::Error> {
        // Decode the base64 secret with better error handling
        let secret = general_purpose::STANDARD.decode(&self.api_secret)
            .map_err(|e| anyhow!("Failed to decode API secret: {}. Check if the secret is valid base64.", e))?;
        
        let message = format!("{}{}", nonce, data);
        
        let mut sha256 = Sha256::new();
        sha256.update(message.as_bytes());
        let hash_digest = sha256.finalize();

        // Use HMAC-SHA512 instead of HMAC-SHA256 as required by Kraken
        let mut hmac = Hmac::<Sha512>::new_from_slice(&secret)
            .map_err(|e| anyhow!("Failed to create HMAC: {}", e))?;
        hmac.update(path.as_bytes());
        hmac.update(&hash_digest);
        
        let signature = general_purpose::STANDARD.encode(hmac.finalize().into_bytes());
        
        // Temporary logging for debugging - remove after verification
        info!("DEBUG sign_request: path={}, nonce={}, data={}", path, nonce, data);
        info!("DEBUG sign_request: computed signature={}", signature);
        
        Ok(signature)
    }

    pub async fn fetch_time(&self) -> Result<u64, anyhow::Error> {
        let response = self.client.get(format!("{}/0/public/Time", self.base_url))
            .send()
            .await
            .map_err(|e| anyhow!("Failed to fetch time: {}", e))?;
        let json: Value = response.json().await.map_err(|e| anyhow!("Failed to parse time response: {}", e))?;
        let unixtime = json["result"]["unixtime"]
            .as_u64()
            .ok_or_else(|| anyhow!("No unixtime in response"))?;
        Ok(unixtime * 1000) // Convert to milliseconds
    }

    pub async fn fetch_balance(&self) -> Result<Value, anyhow::Error> {
        // Get server time first to ensure proper nonce
        let server_time = self.fetch_time().await?;
        let nonce = server_time + 1000; // Add 1 second buffer
        
        let params = format!("nonce={}", nonce);
        let path = "/0/private/Balance";
        
        info!("Attempting balance fetch with nonce: {}", nonce);
        
        let signature = self.sign_request(path, nonce, &params).await?;
        let url = Url::parse(&format!("{}/0/private/Balance", self.base_url))?;

        let response = self.api_call_with_retry(
            || async {
                let response = self.client
                    .post(url.clone())
                    .header("API-Key", &self.api_key)
                    .header("API-Sign", &signature)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .header("User-Agent", "Rust Trading Bot/1.0")
                    .body(params.clone())
                    .send()
                    .await
                    .map_err(|e| anyhow!("Failed to send request: {}", e))?;
                
                let status = response.status();
                let response_text = response.text().await
                    .map_err(|e| anyhow!("Failed to read response: {}", e))?;
                
                info!("API Response Status: {}, Body: {}", status, response_text);
                
                let json: Value = serde_json::from_str(&response_text)
                    .map_err(|e| anyhow!("Failed to parse JSON response: {}", e))?;
                
                Ok(json)
            },
            3, // Reduce retries for initial testing
            "fetch_balance",
        ).await?;

        if let Some(error) = response.get("error").and_then(|e| e.as_array()) {
            if !error.is_empty() {
                return Err(anyhow!("Kraken API error: {:?}", error));
            }
        }
        Ok(response["result"].clone())
    }

    async fn api_call_with_retry<T, F, Fut>(
        &self,
        func: F,
        max_retries: u32,
        log_prefix: &str,
    ) -> Result<T, anyhow::Error>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, anyhow::Error>>,
    {
        let mut retries = 0;
        loop {
            let start = std::time::Instant::now();
            match func().await {
                Ok(result) => {
                    info!("API call {} succeeded after {} retries in {:.2}s", log_prefix, retries, start.elapsed().as_secs_f64());
                    return Ok(result);
                }
                Err(e) => {
                    record_error(log_prefix);
                    if retries >= max_retries {
                        return Err(e);
                    }
                    let backoff = 2u64.pow(retries) as f64 * 5.0;
                    if e.to_string().contains("InvalidNonce") {
                        let server_time = self.fetch_time().await.unwrap_or(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64);
                        info!("{}: Synced server time: {}", log_prefix, server_time);
                    } else if e.to_string().contains("RateLimitExceeded") {
                        let wait_time = 10.0;
                        info!("{}: Rate limit exceeded, waiting {:.2}s", log_prefix, wait_time);
                        tokio::time::sleep(Duration::from_secs_f64(wait_time)).await;
                    } else {
                        info!("{}: Error {}, retrying in {:.2}s", log_prefix, e, backoff);
                        tokio::time::sleep(Duration::from_secs_f64(backoff)).await;
                    }
                    retries += 1;
                }
            }
        }
    }

    pub async fn create_order(&self, pair: &str, order_type: &str, amount: f64, limit_price: f64, post_only: bool) -> Result<String, anyhow::Error> {
        // Get server time for proper nonce
        let server_time = self.fetch_time().await?;
        let nonce = server_time + 1000; // Add buffer

        let params = format!(
            "nonce={}&pair={}&type={}&ordertype=limit&volume={}&price={}&oflags={}",
            nonce,
            pair.replace("/", ""),
            order_type,
            amount,
            limit_price,
            if post_only { "post" } else { "" }
        );

        let path = "/0/private/AddOrder";
        let signature = self.sign_request(path, nonce, &params).await?;

        let url = Url::parse(&format!("{}/0/private/AddOrder", self.base_url)).map_err(|e| anyhow!("Failed to parse URL: {}", e))?;
        let response = self.api_call_with_retry(
            || async {
                self.client
                    .post(url.clone())
                    .header("API-Key", &self.api_key)
                    .header("API-Sign", &signature)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .header("User-Agent", "Rust Trading Bot/1.0")
                    .body(params.clone())
                    .send()
                    .await
                    .map_err(|e| anyhow!("Failed to send request: {}", e))?
                    .json::<Value>()
                    .await
                    .map_err(|e| anyhow!("Failed to parse JSON response: {}", e))
            },
            7,
            "create_order",
        ).await?;

        if let Some(error) = response.get("error").and_then(|e| e.as_array()) {
            if !error.is_empty() {
                return Err(anyhow!("Kraken API error: {:?}", error));
            }
        }

        let order_id = response["result"]["txid"][0]
            .as_str()
            .ok_or_else(|| anyhow!("No order ID in response"))?
            .to_string();
        info!(target: "trade", "Created order for {}: order_id={}, type={}, amount={:.8}, price={:.8}", pair, order_id, order_type, amount, limit_price);
        Ok(order_id)
    }

    pub async fn fetch_order(&self, symbol: &str, order_id: &str) -> Result<Value, anyhow::Error> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let params = format!("nonce={}&txid={}", nonce, order_id);
        let path = "/0/private/QueryOrders";
        let signature = self.sign_request(path, nonce, &params).await?;
        let url = Url::parse(&format!("{}/0/private/QueryOrders", self.base_url))?;

        let response = self.api_call_with_retry(
            || async {
                self.client
                    .post(url.clone())
                    .header("API-Key", &self.api_key)
                    .header("API-Sign", &signature)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .body(params.clone())
                    .send()
                    .await
                    .map_err(|e| anyhow!("Failed to send request: {}", e))?
                    .json::<Value>()
                    .await
                    .map_err(|e| anyhow!("Failed to parse JSON response: {}", e))
            },
            7,
            "fetch_order",
        ).await?;

        if let Some(error) = response.get("error").and_then(|e| e.as_array()) {
            if !error.is_empty() {
                return Err(anyhow!("Kraken API error: {:?}", error));
            }
        }
        Ok(response["result"][order_id].clone())
    }

    pub async fn fetch_trades(&self, symbol: &str, order_id: &str) -> Result<Vec<Value>, anyhow::Error> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let params = format!("nonce={}&txid={}", nonce, order_id);
        let path = "/0/private/Trades";
        let signature = self.sign_request(path, nonce, &params).await?;
        let url = Url::parse(&format!("{}/0/private/Trades", self.base_url))?;

        let response = self.api_call_with_retry(
            || async {
                self.client
                    .post(url.clone())
                    .header("API-Key", &self.api_key)
                    .header("API-Sign", &signature)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .body(params.clone())
                    .send()
                    .await
                    .map_err(|e| anyhow!("Failed to send request: {}", e))?
                    .json::<Value>()
                    .await
                    .map_err(|e| anyhow!("Failed to parse JSON response: {}", e))
            },
            7,
            "fetch_trades",
        ).await?;

        if let Some(error) = response.get("error").and_then(|e| e.as_array()) {
            if !error.is_empty() {
                return Err(anyhow!("Kraken API error: {:?}", error));
            }
        }
        let trades = response["result"]["trades"]
            .as_object()
            .map(|obj| obj.values().cloned().collect())
            .unwrap_or_default();
        Ok(trades)
    }

    pub async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<(), anyhow::Error> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let params = format!("nonce={}&txid={}", nonce, order_id);
        let path = "/0/private/CancelOrder";
        let signature = self.sign_request(path, nonce, &params).await?;
        let url = Url::parse(&format!("{}/0/private/CancelOrder", self.base_url))?;

        let response = self.api_call_with_retry(
            || async {
                self.client
                    .post(url.clone())
                    .header("API-Key", &self.api_key)
                    .header("API-Sign", &signature)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .body(params.clone())
                    .send()
                    .await
                    .map_err(|e| anyhow!("Failed to send request: {}", e))?
                    .json::<Value>()
                    .await
                    .map_err(|e| anyhow!("Failed to parse JSON response: {}", e))
            },
            7,
            "cancel_order",
        ).await?;

        if let Some(error) = response.get("error").and_then(|e| e.as_array()) {
            if !error.is_empty() {
                return Err(anyhow!("Kraken API error: {:?}", error));
            }
        }
        info!(target: "trade", "Canceled order {} for {}", order_id, symbol);
        Ok(())
    }

    pub async fn fetch_ticker(&self, symbol: &str) -> Result<Value, anyhow::Error> {
        let params = vec![("pair", symbol.replace("/", ""))];
        let url = Url::parse_with_params(&format!("{}/0/public/Ticker", self.base_url), params)?;
        let response = self.api_call_with_retry(
            || async {
                self.client
                    .get(url.clone())
                    .send()
                    .await
                    .map_err(|e| anyhow!("Failed to send request: {}", e))?
                    .json::<Value>()
                    .await
                    .map_err(|e| anyhow!("Failed to parse JSON response: {}", e))
            },
            7,
            "fetch_ticker",
        ).await?;

        if let Some(error) = response.get("error").and_then(|e| e.as_array()) {
            if !error.is_empty() {
                return Err(anyhow!("Kraken API error: {:?}", error));
            }
        }
        Ok(response["result"][symbol.replace("/", "")].clone())
    }

    pub async fn fetch_market_data(&self, pair: &str, interval: i32) -> Result<Value, anyhow::Error> {
        let params = vec![("pair", pair.replace("/", "")), ("interval", interval.to_string())];
        let url = Url::parse_with_params(&format!("{}/0/public/OHLC", self.base_url), params)?;
        let response = self.api_call_with_retry(
            || async {
                self.client
                    .get(url.clone())
                    .send()
                    .await
                    .map_err(|e| anyhow!("Failed to send request: {}", e))?
                    .json::<Value>()
                    .await
                    .map_err(|e| anyhow!("Failed to parse JSON response: {}", e))
            },
            7,
            "fetch_market_data",
        ).await?;

        if let Some(error) = response.get("error").and_then(|e| e.as_array()) {
            if !error.is_empty() {
                return Err(anyhow!("Kraken API error: {:?}", error));
            }
        }
        let ohlc = response["result"][pair.replace("/", "")].as_array().ok_or_else(|| anyhow!("Invalid OHLC data for {}", pair))?;
        if ohlc.is_empty() {
            return Err(anyhow!("Empty OHLC data for {}", pair));
        }
        Ok(response["result"][pair.replace("/", "")].clone())
    }

    pub async fn fetch_order_book(&self, pair: &str, count: i32) -> Result<Value, anyhow::Error> {
        let params = vec![("pair", pair.replace("/", "")), ("count", count.to_string())];
        let url = Url::parse_with_params(&format!("{}/0/public/Depth", self.base_url), params)?;
        let response = self.api_call_with_retry(
            || async {
                self.client
                    .get(url.clone())
                    .send()
                    .await
                    .map_err(|e| anyhow!("Failed to send request: {}", e))?
                    .json::<Value>()
                    .await
                    .map_err(|e| anyhow!("Failed to parse JSON response: {}", e))
            },
            7,
            "fetch_order_book",
        ).await?;

        if let Some(error) = response.get("error").and_then(|e| e.as_array()) {
            if !error.is_empty() {
                return Err(anyhow!("Kraken API error: {:?}", error));
            }
        }
        let order_book = response["result"][pair.replace("/", "")].clone();
        if order_book["asks"].as_array().map_or(true, |a| a.is_empty()) || order_book["bids"].as_array().map_or(true, |b| b.is_empty()) {
            return Err(anyhow!("Empty order book data for {}", pair));
        }
        Ok(order_book)
    }
}

#[derive(Debug, Clone)]
pub struct PriceUpdate {
    pub pair: String,
    pub bid: f64,
    pub ask: f64,
    pub close_price: f64,
    pub timestamp: String,
}

// Database functions from data.rs
pub async fn create_pool(config: &Config) -> Result<Pool, Box<dyn Error>> {
    let db_config = &config.database;
    let mut cfg = deadpool_postgres::Config::new();
    cfg.host = Some(db_config.host.value.clone());
    cfg.port = Some(db_config.port.value as u16);
    cfg.dbname = Some(db_config.database.value.clone());
    cfg.user = Some(db_config.user.value.clone());
    cfg.password = Some(db_config.password.value.clone());
    cfg.pool = Some(deadpool::managed::PoolConfig {
        max_size: db_config.max_connections.value as usize,
        ..Default::default()
    });
    
    let max_retries = 5;
    for attempt in 0..max_retries {
        match cfg.create_pool(Some(Runtime::Tokio1), NoTls) {
            Ok(pool) => return Ok(pool),
            Err(e) => {
                error!("Failed to create connection pool on attempt {}/{}: {}", attempt + 1, max_retries, e);
                if attempt < max_retries - 1 {
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt as u32))).await;
                } else {
                    return Err(format!("Failed to create connection pool after {} retries: {}", max_retries, e).into());
                }
            }
        }
    }
    Err("Unreachable: max retries exceeded".into())
}

pub async fn init_db(config: &Config) -> Result<(), Box<dyn Error>> {
    let pool = create_pool(config).await?;
    let mut client = pool.get().await.map_err(|e| format!("Failed to get client from pool: {}", e))?;

    let transaction = client.transaction().await?;
    transaction
        .execute(
            "CREATE TABLE IF NOT EXISTS positions (
                pair TEXT PRIMARY KEY,
                total_usd DOUBLE PRECISION,
                total_quantity DOUBLE PRECISION,
                average_buy_price DOUBLE PRECISION,
                usd_balance DOUBLE PRECISION,
                last_updated TEXT,
                buy_fees DOUBLE PRECISION,
                total_fees DOUBLE PRECISION,
                total_pl DOUBLE PRECISION,
                highest_price_since_buy DOUBLE PRECISION,
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
                amount TEXT,
                price TEXT,
                fees TEXT,
                fee_percentage TEXT,
                profit TEXT,
                profit_percentage TEXT,
                reason TEXT,
                average_buy_price TEXT,
                slippage TEXT,
                remaining_amount TEXT,
                open INTEGER,
                partial_open INTEGER
            )",
            &[],
        )
        .await?;
    transaction
        .execute(
            "CREATE INDEX IF NOT EXISTS idx_trades_pair_timestamp ON trades (pair, timestamp)",
            &[],
        )
        .await?;

    for pair in &config.portfolio.pairs.value {
        let table_name = format!("{}_market_data", crate::map_ws_pair(pair).to_lowercase().replace("/", "_"));
        transaction
            .execute(
                &format!(
                    "CREATE TABLE IF NOT EXISTS {} (
                        timestamp TEXT,
                        open_price TEXT,
                        high_price TEXT,
                        low_price TEXT,
                        highest_price_period TEXT,
                        volume TEXT,
                        bid_price TEXT,
                        ask_price TEXT,
                        bid_depth TEXT,
                        ask_depth TEXT,
                        liquidity TEXT,
                        close_price TEXT
                    )",
                    table_name
                ),
                &[],
            )
            .await?;
        transaction
            .execute(
                &format!("CREATE INDEX IF NOT EXISTS idx_{}_timestamp ON {} (timestamp)", table_name, table_name),
                &[],
            )
            .await?;
        info!("Created table and index {} for pair {}", table_name, pair);
    }

    transaction.commit().await?;
    info!("Database tables and indexes initialized successfully");
    Ok(())
}

pub async fn update_position(client: &mut deadpool_postgres::Client, pair: &str, position: &Position, is_buy: bool) -> Result<(), Box<dyn Error>> {
    let max_retries = 3;
    for attempt in 0..max_retries {
        let transaction = client.transaction().await.map_err(|e| format!("Transaction error: {}", e))?;
        let query = if pair == "USD" {
            "UPDATE positions SET usd_balance = $1, last_updated = $2 WHERE pair = 'USD'"
        } else if is_buy {
            "INSERT INTO positions (pair, total_usd, total_quantity, average_buy_price, usd_balance, last_updated, buy_fees, total_fees, total_pl, highest_price_since_buy, last_synced) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT (pair) DO UPDATE SET total_usd = EXCLUDED.total_usd, total_quantity = EXCLUDED.total_quantity, average_buy_price = EXCLUDED.average_buy_price, usd_balance = EXCLUDED.usd_balance, last_updated = EXCLUDED.last_updated, buy_fees = EXCLUDED.buy_fees, total_fees = EXCLUDED.total_fees, total_pl = EXCLUDED.total_pl, highest_price_since_buy = EXCLUDED.highest_price_since_buy, last_synced = EXCLUDED.last_synced"
        } else {
            "INSERT INTO positions (pair, total_usd, total_quantity, average_buy_price, usd_balance, last_updated, buy_fees, total_fees, total_pl, highest_price_since_buy, last_synced) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT (pair) DO UPDATE SET total_usd = EXCLUDED.total_usd, total_quantity = EXCLUDED.total_quantity, average_buy_price = EXCLUDED.average_buy_price, usd_balance = EXCLUDED.usd_balance, last_updated = EXCLUDED.last_updated, buy_fees = EXCLUDED.buy_fees, total_fees = EXCLUDED.total_fees, total_pl = EXCLUDED.total_pl, highest_price_since_buy = EXCLUDED.highest_price_since_buy, last_synced = EXCLUDED.last_synced"
        };
        let result = if pair == "USD" {
            transaction.execute(query, &[&position.usd_balance, &position.last_updated]).await
        } else {
            transaction.execute(
                query,
                &[
                    &pair,
                    &position.total_usd,
                    &position.total_quantity,
                    &position.average_buy_price,
                    &position.usd_balance,
                    &position.last_updated,
                    &position.buy_fees,
                    &position.total_fees,
                    &position.total_pl,
                    &position.highest_price_since_buy,
                    &position.last_synced,
                ],
            ).await
        };
        match result {
            Ok(rows_affected) => {
                if pair == "USD" && rows_affected == 0 {
                    transaction.execute(
                        "INSERT INTO positions (pair, total_usd, total_quantity, average_buy_price, usd_balance, last_updated, buy_fees, total_fees, total_pl, highest_price_since_buy, last_synced) VALUES ($1, 0.0, 0.0, 0.0, $2, $3, 0.0, 0.0, 0.0, 0.0, $4)",
                        &[&pair, &position.usd_balance, &position.last_updated, &position.last_synced],
                    ).await?;
                }
                transaction.commit().await?;
                info!("DB: Updated/Inserted position for {}, rows_affected: {}", pair, rows_affected);
                return Ok(());
            }
            Err(e) => {
                error!("DB: Failed to update position for {} on attempt {}/{}: {}", pair, attempt + 1, max_retries, e);
                transaction.rollback().await?;
                if attempt < max_retries - 1 {
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt as u32))).await;
                } else {
                    return Err(format!("Failed to update position for {} after {} retries: {}", pair, max_retries, e).into());
                }
            }
        }
    }
    Err("Unreachable: max retries exceeded".into())
}

pub async fn update_trade(client: &mut deadpool_postgres::Client, trade_data: &Trade) -> Result<(), Box<dyn Error>> {
    let max_retries = 3;
    for attempt in 0..max_retries {
        let transaction = client.transaction().await.map_err(|e| format!("Transaction error: {}", e))?;
        let result = transaction.execute(
            "INSERT INTO trades (timestamp, pair, trade_id, order_id, type, amount, price, fees, fee_percentage, profit, profit_percentage, reason, average_buy_price, slippage, remaining_amount, open, partial_open) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) ON CONFLICT (trade_id) DO UPDATE SET timestamp = EXCLUDED.timestamp, pair = EXCLUDED.pair, order_id = EXCLUDED.order_id, type = EXCLUDED.type, amount = EXCLUDED.amount, price = EXCLUDED.price, fees = EXCLUDED.fees, fee_percentage = EXCLUDED.fee_percentage, profit = EXCLUDED.profit, profit_percentage = EXCLUDED.profit_percentage, reason = EXCLUDED.reason, average_buy_price = EXCLUDED.average_buy_price, slippage = EXCLUDED.slippage, remaining_amount = EXCLUDED.remaining_amount, open = EXCLUDED.open, partial_open = EXCLUDED.partial_open",
            &[
                &trade_data.timestamp,
                &trade_data.pair,
                &trade_data.trade_id,
                &trade_data.order_id,
                &trade_data.trade_type,
                &trade_data.amount,
                &trade_data.price,
                &trade_data.fees,
                &trade_data.fee_percentage,
                &trade_data.profit,
                &trade_data.profit_percentage,
                &trade_data.reason,
                &trade_data.average_buy_price,
                &trade_data.slippage,
                &trade_data.remaining_amount,
                &trade_data.open,
                &trade_data.partial_open,
            ],
        ).await;
        match result {
            Ok(rows_affected) => {
                transaction.commit().await?;
                info!("DB: Inserted/Updated trade for {}, trade_id: {}, rows_affected: {}", trade_data.pair, trade_data.trade_id, rows_affected);
                return Ok(());
            }
            Err(e) => {
                error!("DB: Failed to update trade for {} on attempt {}/{}: {}", trade_data.trade_id, attempt + 1, max_retries, e);
                transaction.rollback().await?;
                if attempt < max_retries - 1 {
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt as u32))).await;
                } else {
                    return Err(format!("Failed to update trade for {} after {} retries: {}", trade_data.trade_id, max_retries, e).into());
                }
            }
        }
    }
    Err("Unreachable: max retries exceeded".into())
}

pub async fn store_market_data(client: &mut deadpool_postgres::Client, market_data: &MarketData) -> Result<(), Box<dyn Error>> {
    let max_retries = 3;
    for attempt in 0..max_retries {
        let transaction = client.transaction().await map_err(|e| format!("Transaction error: {}", e))?;
        let result = transaction.execute(
            &format!(
                "INSERT INTO {}_market_data (timestamp, open_price, high_price, low_price, highest_price_period, volume, bid_price, ask_price, bid_depth, ask_depth, liquidity, close_price) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) ON CONFLICT DO NOTHING",
                market_data.pair.to_lowercase().replace("/", "_")
            ),
            &[
                &market_data.timestamp,
                &market_data.open_price,
                &market_data.high_price,
                &market_data.low_price,
                &market_data.highest_price_period,
                &market_data.volume,
                &market_data.bid_price,
                &market_data.ask_price,
                &market_data.bid_depth,
                &market_data.ask_depth,
                &market_data.liquidity,
                &market_data.close_price,
            ],
        ).await;
        match result {
            Ok(rows_affected) => {
                transaction.commit().await?;
                info!("DB: Stored market data for {}, timestamp: {}, rows_affected: {}", market_data.pair, market_data.timestamp, rows_affected);
                return Ok(());
            }
            Err(e) => {
                error!("DB: Failed to store market data for {} on attempt {}/{}: {}", market_data.pair, attempt + 1, max_retries, e);
                transaction.rollback().await?;
                if attempt < max_retries - 1 {
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt as u32))).await;
                } else {
                    return Err(format!("Failed to store market data for {} after {} retries: {}", market_data.pair, max_retries, e).into());
                }
            }
        }
    }
    Err("Unreachable: max retries exceeded".into())
}

pub async fn get_all_positions(client: &deadpool_postgres::Client) -> Result<Vec<Position>, Box<dyn Error>> {
    let max_retries = 3;
    for attempt in 0..max_retries {
        match client.query("SELECT * FROM positions", &[]).await {
            Ok(rows) => {
                let positions = rows.into_iter().map(|row| Position {
                    pair: row.get("pair"),
                    total_usd: row.get("total_usd"),
                    total_quantity: row.get("total_quantity"),
                    average_buy_price: row.get("average_buy_price"),
                    usd_balance: row.get("usd_balance"),
                    last_updated: row.get("last_updated"),
                    buy_fees: row.get("buy_fees"),
                    total_fees: row.get("total_fees"),
                    total_pl: row.get("total_pl"),
                    highest_price_since_buy: row.get("highest_price_since_buy"),
                    last_synced: row.get("last_synced"),
                }).collect::<Vec<_>>();
                info!("DB: Fetched {} positions", positions.len());
                return Ok(positions);
            }
            Err(e) => {
                error!("DB: Failed to fetch all positions on attempt {}/{}: {}", attempt + 1, max_retries, e);
                if attempt < max_retries - 1 {
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt as u32))).await;
                } else {
                    return Err(format!("Failed to fetch all positions after {} retries: {}", max_retries, e).into());
                }
            }
        }
    }
    Err("Unreachable: max retries exceeded".into())
}

pub async fn get_open_trades(client: &deadpool_postgres::Client, pair: &str) -> Result<Vec<Trade>, Box<dyn Error>> {
    let max_retries = 3;
    for attempt in 0..max_retries {
        match client.query(
            "SELECT * FROM trades WHERE pair = $1 AND (open = 1 OR partial_open = 1) ORDER BY timestamp",
            &[&pair],
        ).await {
            Ok(rows) => {
                let trades = rows.into_iter().map(|row| Trade {
                    timestamp: row.get("timestamp"),
                    pair: row.get("pair"),
                    trade_id: row.get("trade_id"),
                    order_id: row.get("order_id"),
                    trade_type: row.get("type"),
                    amount: row.get("amount"),
                    price: row.get("price"),
                    fees: row.get("fees"),
                    fee_percentage: row.get("fee_percentage"),
                    profit: row.get("profit"),
                    profit_percentage: row.get("profit_percentage"),
                    reason: row.get("reason"),
                    average_buy_price: row.get("average_buy_price"),
                    slippage: row.get("slippage"),
                    remaining_amount: row.get("remaining_amount"),
                    open: row.get("open"),
                    partial_open: row.get("partial_open"),
                }).collect::<Vec<_>>();
                info!("DB: Fetched {} open trades for {}", trades.len(), pair);
                return Ok(trades);
            }
            Err(e) => {
                error!("DB: Failed to fetch open trades for {} on attempt {}/{}: {}", pair, attempt + 1, max_retries, e);
                if attempt < max_retries - 1 {
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt as u32))).await;
                } else {
                    return Err(format!("Failed to fetch open trades for {} after {} retries: {}", pair, max_retries, e).into());
                }
            }
        }
    }
    Err("Unreachable: max retries exceeded".into())
}

pub async fn get_market_data_history(client: &deadpool_postgres::Client, pair: &str, since: &str, limit: i64) -> Result<Vec<MarketDataHistory>, Box<dyn Error>> {
    let max_retries = 3;
    for attempt in 0..max_retries {
        match client.query(
            &format!(
                "SELECT high_price, low_price, close_price FROM {}_market_data WHERE timestamp >= $1 ORDER BY timestamp DESC LIMIT $2",
                pair.to_lowercase().replace("/", "_")
            ),
            &[&since, &limit],
        ).await {
            Ok(rows) => {
                let history = rows.into_iter().map(|row| MarketDataHistory {
                    high_price: row.get("high_price"),
                    low_price: row.get("low_price"),
                    close_price: row.get("close_price"),
                }).collect::<Vec<_>>();
                info!("DB: Fetched {} market data history records for {} since {}", history.len(), pair, since);
                return Ok(history);
            }
            Err(e) => {
                error!("DB: Failed to fetch market data history for {} on attempt {}/{}: {}", pair, attempt + 1, max_retries, e);
                if attempt < max_retries - 1 {
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt as u32))).await;
                } else {
                    return Err(format!("Failed to fetch market data history for {} after {} retries: {}", pair, max_retries, e).into());
                }
            }
        }
    }
    Err("Unreachable: max retries exceeded".into())
}

pub async fn get_trade_fees(client: &deadpool_postgres::Client, pair: &str, trade_id: &str) -> Result<Option<String>, Box<dyn Error>> {
    let max_retries = 3;
    for attempt in 0..max_retries {
        match client.query_opt(
            "SELECT fees FROM trades WHERE pair = $1 AND trade_id = $2",
            &[&pair, &trade_id],
        ).await {
            Ok(row) => {
                info!("DB: Fetched fees for {} trade_id {}", pair, trade_id);
                return Ok(row.map(|row| row.get("fees")));
            }
            Err(e) => {
                error!("DB: Failed to fetch trade fees for {} trade_id {} on attempt {}/{}: {}", pair, trade_id, attempt + 1, max_retries, e);
                if attempt < max_retries - 1 {
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt as u32))).await;
                } else {
                    return Err(format!("Failed to fetch trade fees for {} trade_id {} after {} retries: {}", pair, trade_id, max_retries, e).into());
                }
            }
        }
    }
    Err("Unreachable: max retries exceeded".into())
}

pub async fn watch_kraken_prices(
    pairs: Vec<String>,
    tx: tokio::sync::mpsc::Sender<PriceUpdate>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ws_url = "wss://ws.kraken.com";
    let max_retries = 5;
    let mut retry_count = 0;

    loop {
        info!("Connecting to Kraken WebSocket at {}", ws_url);
        let (ws_stream, _) = match connect_async(ws_url).await {
            Ok(stream) => stream,
            Err(e) => {
                error!("WebSocket connection error: {}", e);
                record_error("websocket_connect");
                retry_count += 1;
                if retry_count >= max_retries {
                    return Err("Max reconnect attempts reached".into());
                }
                let backoff = 2u64.pow(retry_count as u32);
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                continue;
            }
        };

        let (mut write, mut read) = ws_stream.split();
        let mapped_pairs: Vec<String> = pairs.iter().map(|p| map_ws_pair(p)).collect();
        let sub_msg = json!({
            "event": "subscribe",
            "pair": mapped_pairs,
            "subscription": {"name": "ticker"}
        });
        let sub_str = sub_msg.to_string();
        info!("Subscribing to tickers: {}", sub_str);
        if write.send(Message::Text(sub_str.into())).await.is_err() {
            error!("Failed to send subscription");
            record_error("websocket_subscribe");
            retry_count += 1;
            if retry_count >= max_retries {
                return Err("Failed to send subscription after max retries".into());
            }
            tokio::time::sleep(Duration::from_secs(2u64.pow(retry_count as u32))).await;
            continue;
        }

        let mut subscribed = false;
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    let parsed: Value = match serde_json::from_str(&text) {
                        Ok(v) => v,
                        Err(e) => {
                            error!("Failed to parse WebSocket message: {}", e);
                            record_error("websocket_parse");
                            continue;
                        }
                    };

                    if let Some(event) = parsed.get("event") {
                        if event == "subscriptionStatus" {
                            if parsed.get("status").and_then(|s| s.as_str()) == Some("subscribed") {
                                info!("Subscription confirmed: {:?}", parsed);
                                subscribed = true;
                                break;
                            } else {
                                error!("Subscription failed: {:?}", parsed);
                                record_error("websocket_subscription");
                                retry_count += 1;
                                if retry_count >= max_retries {
                                    return Err("Failed to confirm subscription after max retries".into());
                                }
                                tokio::time::sleep(Duration::from_secs(2u64.pow(retry_count as u32))).await;
                                break;
                            }
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    error!("WebSocket closed by server before subscription");
                    record_error("websocket_close");
                    break;
                }
                Err(e) => {
                    error!("WebSocket error: {}", e);
                    record_error("websocket_error");
                    break;
                }
                _ => {}
            }
        }

        if !subscribed {
            retry_count += 1;
            if retry_count >= max_retries {
                return Err("Failed to confirm subscription after max retries".into());
            }
            continue;
        }

        retry_count = 0;

        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    let parsed: Value = match serde_json::from_str(&text) {
                        Ok(v) => v,
                        Err(e) => {
                            error!("Failed to parse WebSocket message: {}", e);
                            record_error("websocket_parse");
                            continue;
                        }
                    };

                    if parsed.is_array() {
                        let arr = parsed.as_array().unwrap();
                        if arr.len() >= 4 && arr[2] == "ticker" {
                            let data = &arr[1];
                            let pair = arr[3].as_str().unwrap_or("").to_string();
                            let bid_str = data["b"][0].as_str().unwrap_or("0.0");
                            let ask_str = data["a"][0].as_str().unwrap_or("0.0");
                            let close_str = data["c"][0].as_str().unwrap_or("0.0");

                            let update = PriceUpdate {
                                pair: pair.clone(),
                                bid: bid_str.parse().unwrap_or(0.0),
                                ask: ask_str.parse().unwrap_or(0.0),
                                close_price: close_str.parse().unwrap_or(0.0),
                                timestamp: chrono::Local::now().to_string(),
                            };

                            if tx.send(update).await.is_err() {
                                error!("Failed to send price update");
                                record_error("price_update_send");
                                break;
                            }
                            info!(target: "trade", "Price update for {}: bid={}, ask={}, close={}", pair, bid_str, ask_str, close_str);
                        }
                    } else if let Some(event) = parsed.get("event") {
                        if event == "subscriptionStatus" {
                            info!("Subscription status: {:?}", parsed);
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    info!("WebSocket closed by server");
                    record_error("websocket_close");
                    break;
                }
                Err(e) => {
                    error!("WebSocket error: {}", e);
                    record_error("websocket_error");
                    break;
                }
                _ => {}
            }
        }

        retry_count += 1;
        if retry_count >= max_retries {
            return Err("Max reconnect attempts reached".into());
        }
        let backoff = 2u64.pow(retry_count as u32);
        info!("Reconnecting after {} seconds", backoff);
        tokio::time::sleep(Duration::from_secs(backoff)).await;
    }
}