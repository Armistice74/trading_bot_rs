// api.rs
// Description: Handles interactions with the Kraken REST API, including authentication, order creation, and market data retrieval.

// ============================================================================
// IMPORTS
// ============================================================================

use anyhow::{anyhow, Result};
use base64::engine::general_purpose;
use base64::Engine;
use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::{Client, Url};
use serde_json::Value;
use sha2::{Digest, Sha256, Sha512};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::sync::Arc;
use tokio::time::sleep;
use tracing::{error, info};
use rust_decimal::Decimal;

// ============================================================================
// STRUCT DEFINITION
// ============================================================================

#[derive(Clone)]
pub struct WsToken {
    pub token: String,
    pub expires: u64, // Unix timestamp in seconds
}

#[derive(Clone)]
pub struct KrakenClient {
    api_key: String,
    api_secret: String,
    base_url: String,
    client: Client,
    startup_time: u64,
    ws_token: Option<WsToken>,
    semaphore: Arc<tokio::sync::Semaphore>,  // NEW: Rate limiter (10 concurrent)
}

// ============================================================================
// PRIVATE HELPERS
// ============================================================================

impl KrakenClient {
    pub fn startup_time(&self) -> u64 {
        self.startup_time
    }

    pub fn ws_token(&self) -> &Option<WsToken> {
        &self.ws_token
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn http_client(&self) -> &Client {
        &self.client
    }

    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    pub async fn sign_request(
        &self,
        path: &str,
        nonce: u64,
        data: &str,
    ) -> Result<String, anyhow::Error> {
        let secret = general_purpose::STANDARD
            .decode(&self.api_secret)
            .map_err(|e| {
                anyhow!(
                    "Failed to decode API secret: {}. Check if the secret is valid base64.",
                    e
                )
            })?;

        let message = format!("{}{}", nonce, data);

        let mut sha256 = Sha256::new();
        sha256.update(message.as_bytes());
        let hash_digest = sha256.finalize();

        let mut hmac = Hmac::<Sha512>::new_from_slice(&secret)
            .map_err(|e| anyhow!("Failed to create HMAC: {}", e))?;
        hmac.update(path.as_bytes());
        hmac.update(&hash_digest);

        let signature = general_purpose::STANDARD.encode(hmac.finalize().into_bytes());

        Ok(signature)
    }

    pub async fn api_call_with_retry<T, F, Fut>(
        &self,
        func: F,
        max_retries: u32,  // Default lowered to 3 in calls
        log_prefix: &str,
    ) -> Result<T, anyhow::Error>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, anyhow::Error>>,
    {
        let mut retries = 0;
        let mut permit = self.semaphore.acquire().await.map_err(|e| anyhow!("Semaphore acquire failed: {}", e))?;  // NEW: Acquire permit
        loop {
            match func().await {
                Ok(result) => {
                    return Ok(result);
                }
                Err(e) => {
                    crate::record_error(log_prefix);
                    if retries >= max_retries {
                        return Err(e);
                    }
                    let backoff = 5u64 << retries;
                    if e.to_string().contains("InvalidNonce") {
                        let server_time = self.fetch_time().await.unwrap_or(
                            SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64,
                        );
                        info!("{}: Synced server time: {}", log_prefix, server_time);
                    } else if e.to_string().contains("EAPI:Rate limit exceeded") {  // NEW: Specific backoff for rate limit
                        let wait_time = 10u64 + backoff;  // Fixed 10s + exp
                        info!(
                            "{}: Rate limit exceeded, waiting {}s",
                            log_prefix, wait_time
                        );
                        drop(permit);  // Release permit during wait
                        tokio::time::sleep(tokio::time::Duration::from_secs(wait_time)).await;
                        permit = self.semaphore.acquire().await.map_err(|e| anyhow!("Semaphore re-acquire failed: {}", e))?;  // Re-acquire
                    } else {
                        info!("{}: Error {}, retrying in {}s", log_prefix, e, backoff);
                        drop(permit);  // Release during wait
                        tokio::time::sleep(tokio::time::Duration::from_secs(backoff)).await;
                        permit = self.semaphore.acquire().await.map_err(|e| anyhow!("Semaphore re-acquire failed: {}", e))?;  // Re-acquire
                    }
                    retries += 1;
                }
            }
        }
    }
}

// ============================================================================
// PUBLIC METHODS
// ============================================================================

impl KrakenClient {
    pub fn new(api_key: String, api_secret: String) -> Self {
        let startup_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        info!(
            "Initializing KrakenClient with API key: {}..., startup_time: {}",
            &api_key[..std::cmp::min(8, api_key.len())],
            startup_time
        );
        info!("API secret length: {} characters", api_secret.len());

        if api_key.trim().is_empty() {
            error!("API key is empty after trimming");
        }
        if api_secret.trim().is_empty() {
            error!("API secret is empty after trimming");
        }

        KrakenClient {
            api_key: api_key
                .trim()
                .trim_matches('"')
                .trim_matches('\'')
                .to_string(),
            api_secret: api_secret
                .trim()
                .trim_matches('"')
                .trim_matches('\'')
                .to_string(),
            base_url: "https://api.kraken.com".to_string(),
            client: Client::new(),
            startup_time,
            ws_token: None,
            semaphore: Arc::new(tokio::sync::Semaphore::new(5)),  // TIGHTENED: Limit to 5 concurrent calls
        }
    }

    pub async fn init_ws_token(&mut self) -> Result<()> {
        match self.fetch_ws_token().await {
            Ok(token) => {
                self.ws_token = Some(token);
                info!("Initialized WS token, expires at {}", self.ws_token.as_ref().unwrap().expires);
                Ok(())
            }
            Err(e) => {
                error!("Failed to fetch initial WS token: {}", e);
                // Note: Caller should handle disabling WS trades if needed
                Err(e)
            }
        }
    }

    pub async fn fetch_ws_token(&self) -> Result<WsToken, anyhow::Error> {
        let server_time = self.fetch_time().await?;
        let nonce = server_time + 1000;
        let params = format!("nonce={}", nonce);
        let path = "/0/private/GetWebSocketsToken";

        let signature = self.sign_request(path, nonce, &params).await?;
        let url = Url::parse(&format!("{}/0/private/GetWebSocketsToken", self.base_url))?;

        let response: Value = self
            .api_call_with_retry(
                || async {
                    let response = self
                        .client
                        .post(url.clone())
                        .header("API-Key", &self.api_key)
                        .header("API-Sign", &signature)
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .header("User-Agent", "Rust Trading Bot/1.0")
                        .body(params.clone())
                        .send()
                        .await
                        .map_err(|e| anyhow!("Failed to send WS token request: {}", e))?;

                    let status = response.status();
                    let response_text = response
                        .text()
                        .await
                        .map_err(|e| anyhow!("Failed to read WS token response: {}", e))?;

                    info!("WS Token API Response Status: {}, Body: {}", status, response_text);

                    let json: Value = serde_json::from_str(&response_text)
                        .map_err(|e| anyhow!("Failed to parse WS token JSON response: {}", e))?;

                    Ok(json)
                },
                3,
                "fetch_ws_token",
            )
            .await?;

        if let Some(error) = response.get("error").and_then(|e| e.as_array()) {
            if !error.is_empty() {
                let err_msg = format!("Kraken API error fetching WS token: {:?}", error);
                error!("{}", err_msg);
                return Err(anyhow!(err_msg));
            }
        }

        let result = response["result"].clone();
        let token = result["token"]
            .as_str()
            .ok_or_else(|| anyhow!("No token in WS token response"))?
            .to_string();
        let expires = result["expires"]
            .as_u64()
            .ok_or_else(|| anyhow!("No expires in WS token response"))?;

        info!("Fetched WS token, expires at {}", expires);

        Ok(WsToken { token, expires })
    }

    pub async fn refresh_ws_token(&mut self) -> Result<()> {
        let now = Utc::now().timestamp() as u64;
        if let Some(token) = &self.ws_token {
            if token.expires > now + 3600 {
                info!("WS token still valid until {}", token.expires);
                return Ok(());
            }
        }

        let mut retries = 0;
        while retries < 5 {
            match self.fetch_ws_token().await {
                Ok(new_token) => {
                    self.ws_token = Some(new_token);
                    info!("Refreshed WS token, expires at {}", self.ws_token.as_ref().unwrap().expires);
                    return Ok(());
                }
                Err(e) => {
                    error!("WS token refresh failed: {}, attempt {}/5", e, retries + 1);
                    if retries < 4 {
                        let backoff = 2u64.pow(retries);
                        sleep(Duration::from_secs(backoff)).await;
                    }
                    retries += 1;
                }
            }
        }

        let err_msg = "Failed to refresh WS token after 5 attempts - check API permissions (e.g., Query Trades)";
        error!("{}", err_msg);
        Err(anyhow!(err_msg))
    }

    pub async fn fetch_time(&self) -> Result<u64, anyhow::Error> {
        let response = self
            .client
            .get(format!("{}/0/public/Time", self.base_url))
            .send()
            .await
            .map_err(|e| anyhow!("Failed to fetch time: {}", e))?;
        let json: Value = response
            .json()
            .await
            .map_err(|e| anyhow!("Failed to parse time response: {}", e))?;
        let unixtime = json["result"]["unixtime"]
            .as_u64()
            .ok_or_else(|| anyhow!("No unixtime in response"))?;
        Ok(unixtime * 1000) // Convert to milliseconds
    }

    pub async fn fetch_balance(&self) -> Result<Value, anyhow::Error> {
        let server_time = self.fetch_time().await?;
        let nonce = server_time + 1000;

        let params = format!("nonce={}", nonce);
        let path = "/0/private/Balance";

        info!("Attempting balance fetch with nonce: {}", nonce);

        let signature = self.sign_request(path, nonce, &params).await?;
        let url = Url::parse(&format!("{}/0/private/Balance", self.base_url))?;

        let response = self
            .api_call_with_retry(
                || async {
                    let response = self
                        .client
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
                    let response_text = response
                        .text()
                        .await
                        .map_err(|e| anyhow!("Failed to read response: {}", e))?;

                    info!("API Response Status: {}, Body: {}", status, response_text);

                    let json: Value = serde_json::from_str(&response_text)
                        .map_err(|e| anyhow!("Failed to parse JSON response: {}", e))?;

                    Ok(json)
                },
                3,
                "fetch_balance",
            )
            .await?;

        if let Some(error) = response.get("error").and_then(|e| e.as_array()) {
            if !error.is_empty() {
                return Err(anyhow!("Kraken API error: {:?}", error));
            }
        }
        Ok(response["result"].clone())
    }

    pub async fn create_order(
        &self,
        pair: &str,
        order_type: &str,
        amount: Decimal,
        limit_price: Decimal,
        post_only: bool,
    ) -> Result<String, anyhow::Error> {
        let server_time = self.fetch_time().await?;
        let nonce = server_time + 1000;
        let params = if post_only {
            format!(
                "nonce={}&pair={}&type={}&ordertype=limit&volume={}&price={}&oflags=post",
                nonce, pair, order_type, amount, limit_price
            )
        } else {
            format!(
                "nonce={}&pair={}&type={}&ordertype=limit&volume={}&price={}",
                nonce, pair, order_type, amount, limit_price
            )
        };
        let path = "/0/private/AddOrder";
        let signature = self.sign_request(path, nonce, &params).await?;
        let url = Url::parse(&format!("{}/0/private/AddOrder", self.base_url))
            .map_err(|e| anyhow!("Failed to parse URL: {}", e))?;
        let response = self
            .api_call_with_retry(
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
            )
            .await?;
        if let Some(error) = response.get("error").and_then(|e| e.as_array()) {
            if !error.is_empty() {
                return Err(anyhow!("Kraken API error: {:?}", error));
            }
        }
        let order_id = response["result"]["txid"][0]
            .as_str()
            .ok_or_else(|| anyhow!("No order ID in response"))?
            .to_string();
        info!(target: "trade", "Created order for {}: order_id={}, type={}, amount={}, price={}", pair, order_id, order_type, amount, limit_price);
        Ok(order_id)
    }

    pub async fn fetch_order(&self, _pair: &str, order_id: &str) -> Result<Value, anyhow::Error> {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let params = format!("nonce={}&txid={}", nonce, order_id);
        let path = "/0/private/QueryOrders";
        let signature = self.sign_request(path, nonce, &params).await?;
        let url = Url::parse(&format!("{}/0/private/QueryOrders", self.base_url))?;
        let response = self
            .api_call_with_retry(
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
                "fetch_order",
            )
            .await?;
        if let Some(error) = response.get("error").and_then(|e| e.as_array()) {
            if !error.is_empty() {
                return Err(anyhow!("Kraken API error: {:?}", error));
            }
        }
        Ok(response["result"][order_id].clone())
    }

    pub async fn cancel_order(&self, pair: &str, order_id: &str) -> Result<(), anyhow::Error> {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let params = format!("nonce={}&txid={}", nonce, order_id);
        let path = "/0/private/CancelOrder";
        let signature = self.sign_request(path, nonce, &params).await?;
        let url = Url::parse(&format!("{}/0/private/CancelOrder", self.base_url))?;
        let response = self
            .api_call_with_retry(
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
                "cancel_order",
            )
            .await?;
        if let Some(error) = response.get("error").and_then(|e| e.as_array()) {
            if !error.is_empty() {
                let error_msg = error[0].as_str().unwrap_or("");
                if error_msg.contains("EOrder:Unknown order") {
                    info!(target: "trade", "Order {} for {} already closed/filled", order_id, pair);
                    return Ok(());
                }
                return Err(anyhow!("Kraken API error: {:?}", error));
            }
        }
        info!(target: "trade", "Canceled order {} for {}", order_id, pair);
        Ok(())
    }

    pub async fn fetch_ticker(&self, pair: &str) -> Result<Value, anyhow::Error> {
        let params = vec![("pair", pair.to_string())];
        let url = Url::parse_with_params(&format!("{}/0/public/Ticker", self.base_url), params)?;
        let response = self
            .api_call_with_retry(
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
            )
            .await?;
        if let Some(error) = response.get("error").and_then(|e| e.as_array()) {
            if !error.is_empty() {
                return Err(anyhow!("Kraken API error: {:?}", error));
            }
        }
        Ok(response["result"][pair].clone())
    }

    pub async fn fetch_market_data(
        &self,
        pair: &str,
        interval: i32,
    ) -> Result<Value, anyhow::Error> {
        let params = vec![
            ("pair", pair.to_string()),
            ("interval", interval.to_string()),
        ];
        let url = Url::parse_with_params(&format!("{}/0/public/OHLC", self.base_url), params)?;
        let response = self
            .api_call_with_retry(
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
            )
            .await?;
        if let Some(error) = response.get("error").and_then(|e| e.as_array()) {
            if !error.is_empty() {
                return Err(anyhow!("Kraken API error: {:?}", error));
            }
        }
        let ohlc = response["result"][pair]
            .as_array()
            .ok_or_else(|| anyhow!("Invalid OHLC data for {}", pair))?;
        if ohlc.is_empty() {
            return Err(anyhow!("Empty OHLC data for {}", pair));
        }
        Ok(response["result"][pair].clone())
    }

    pub async fn fetch_order_book(&self, pair: &str, count: i32) -> Result<Value, anyhow::Error> {
        let params = vec![("pair", pair.to_string()), ("count", count.to_string())];
        let url = Url::parse_with_params(&format!("{}/0/public/Depth", self.base_url), params)?;
        let response = self
            .api_call_with_retry(
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
            )
            .await?;
        if let Some(error) = response.get("error").and_then(|e| e.as_array()) {
            if !error.is_empty() {
                return Err(anyhow!("Kraken API error: {:?}", error));
            }
        }
        let order_book = response["result"][pair].clone();
        if order_book["asks"].as_array().map_or(true, |a| a.is_empty())
            || order_book["bids"].as_array().map_or(true, |b| b.is_empty())
        {
            return Err(anyhow!("Empty order book data for {}", pair));
        }
        Ok(order_book)
    }
}