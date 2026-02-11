// fetch.rs
// Description: Neutral layer for trade fetching, decoupling WS/API sources.

use crate::utils::{get_current_time, get_eastern_tz, parse_unix_timestamp};
use chrono::Utc;
use crate::api::KrakenClient;
use crate::statemanager::{StateManager, Trade};
use anyhow::{anyhow, Result};
use reqwest::Url;
use serde_json::Value;
use std::sync::Arc;
use tracing::{error, info, warn};
use crate::config::Config;
use tokio::time::Duration;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

// Internal API fetch with QueryOrders/TradesHistory fallback (server-side filtering where possible).
pub async fn fetch_trades_via_api(
    client: &KrakenClient,
    pair: &str,
    order_id: &str,
    startup_time: u64,
    config: &Config,  // NEW: Added config param for delay
    max_retries_override: Option<usize>,  // NEW: Optional override for max_retries (e.g., 1 for minimal)
) -> Result<Vec<(Value, String)>> {
if order_id.is_empty() {
    return Ok(vec![]);
}
let max_retries = max_retries_override.unwrap_or(3);  // NEW: Default 3, allow override
let retry_delay_secs = config.delays.monitor_partial_delay.value;  // NEW: Use 10.0 from config
let mut trades = vec![];
let mut offset = 0;
let page_size = 50;

let canonical = pair.to_string();

let order_open_time = if !order_id.is_empty() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let order_params = format!("nonce={}&txid={}&trades=true&consolidate_taker=false", nonce, order_id);  // Updated: Added consolidate_taker=false
    let order_path = "/0/private/QueryOrders";
    let order_signature = client.sign_request(&order_path, nonce, &order_params).await?;
    let order_url = Url::parse(&format!("{}/0/private/QueryOrders", client.base_url()))?;
    let order_response: Value = client
        .api_call_with_retry(
            || async {
                let response = client
                    .http_client()
                    .post(order_url.clone())
                    .header("API-Key", client.api_key())
                    .header("API-Sign", &order_signature)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .header("User-Agent", "Rust Trading Bot/1.0")
                    .body(order_params.clone())
                    .send()
                    .await
                    .map_err(|e| anyhow!("Failed to send QueryOrders request: {}", e))?;
                let response_text = response
                    .text()
                    .await
                    .map_err(|e| anyhow!("Failed to read QueryOrders response: {}", e))?;
                serde_json::from_str(&response_text).map_err(|e| {
                    anyhow!("Failed to parse QueryOrders JSON response: {}", e)
                })
            },
            7,
            "query_orders_for_trades",
        )
        .await?;
    if let Some(error) = order_response.get("error").and_then(|e| e.as_array()) {
        if !error.is_empty() {
            warn!("QueryOrders for order {} failed: {:?}", order_id, error);
        }
    }
    order_response["result"][order_id]
        .get("opentm")
        .and_then(|t| t.as_f64())
        .map(|t| (t * 1000.0).floor() as u64)
        .unwrap_or(0)
} else {
    startup_time
};

if order_open_time < startup_time && !order_id.is_empty() {
    info!(
        "Order {} for {} opened at {} < startup {}, skipping",
        order_id, pair, order_open_time, startup_time
    );
    return Ok(vec![]);
}

let mut trade_ids = vec![];
    if !order_id.is_empty() {
        let mut attempt = 0;
        while attempt < max_retries {
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let order_params = format!("nonce={}&txid={}&trades=true&consolidate_taker=false", nonce, order_id);  // Updated: Added consolidate_taker=false
            let order_path = "/0/private/QueryOrders";
            let order_signature = client.sign_request(&order_path, nonce, &order_params).await?;
            let order_url = Url::parse(&format!("{}/0/private/QueryOrders", client.base_url()))?;
            let order_response: Value = client
                .api_call_with_retry(
                    || async {
                        let response = client
                            .http_client()
                            .post(order_url.clone())
                            .header("API-Key", client.api_key())
                            .header("API-Sign", &order_signature)
                            .header("Content-Type", "application/x-www-form-urlencoded")
                            .header("User-Agent", "Rust Trading Bot/1.0")
                            .body(order_params.clone())
                            .send()
                            .await
                            .map_err(|e| anyhow!("Failed to send QueryOrders request: {}", e))?;
                        let response_text = response
                            .text()
                            .await
                            .map_err(|e| anyhow!("Failed to read QueryOrders response: {}", e))?;
                        serde_json::from_str(&response_text).map_err(|e| {
                            anyhow!("Failed to parse QueryOrders JSON response: {}", e)
                        })
                    },
                    7,
                    "query_orders_for_trades",
                )
                .await?;
            if let Some(error) = order_response.get("error").and_then(|e| e.as_array()) {
                if !error.is_empty() {
                    warn!(
                        "QueryOrders attempt {}/{} for order {} failed: {:?}",
                        attempt + 1, max_retries, order_id, error
                    );
                    if attempt < max_retries - 1 {
                        tokio::time::sleep(Duration::from_secs_f64(retry_delay_secs.to_f64().unwrap_or(10.0))).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(anyhow!(
                        "Kraken API QueryOrders error after {} retries: {:?}",
                        max_retries, error
                    ));
                }
            }
            trade_ids = order_response["result"][order_id]
                .get("trades")
                .and_then(|t| t.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_default();
            info!(target: "trade", "Fetched {} trade IDs for order {} on {}, attempt {}/{}", trade_ids.len(), order_id, pair, attempt + 1, max_retries);
            if !trade_ids.is_empty() {
                break;  // Success with trades: No retry needed
            }
            if attempt < max_retries - 1 {
                tokio::time::sleep(Duration::from_secs_f64(retry_delay_secs.to_f64().unwrap_or(10.0))).await;  // Fixed delay
            }
            attempt += 1;
        }
    }

    if !trade_ids.is_empty() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let trade_ids_joined = trade_ids.join(",");
        let trade_params = format!("nonce={}&txid={}", nonce, trade_ids_joined);
        let trade_path = "/0/private/QueryTrades";
        let trade_signature = client.sign_request(&trade_path, nonce, &trade_params).await?;
        let trade_url = Url::parse(&format!("{}/0/private/QueryTrades", client.base_url()))?;
        let trade_response: Value = client
            .api_call_with_retry(
                || async {
                    let response = client
                        .http_client()
                        .post(trade_url.clone())
                        .header("API-Key", client.api_key())
                        .header("API-Sign", &trade_signature)
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .header("User-Agent", "Rust Trading Bot/1.0")
                        .body(trade_params.clone())
                        .send()
                        .await
                        .map_err(|e| anyhow!("Failed to send QueryTrades request: {}", e))?;
                    let response_text = response
                        .text()
                        .await
                        .map_err(|e| anyhow!("Failed to read QueryTrades response: {}", e))?;
                    serde_json::from_str(&response_text).map_err(|e| {
                        anyhow!("Failed to parse QueryTrades JSON response: {}", e)
                    })
                },
                7,
                "fetch_trades",
            )
            .await?;
        if let Some(error) = trade_response.get("error").and_then(|e| e.as_array()) {
            if !error.is_empty() {
                return Err(anyhow!("Kraken API QueryTrades error: {:?}", error));
            }
        }
        trades = trade_response["result"]
            .as_object()
            .map(|obj| {
                obj.iter()
                    .map(|(txid, trade)| {
                        let mut trade_clone = trade.clone();
                        if let Some(obj_mut) = trade_clone.as_object_mut() {
                            obj_mut.insert("txid".to_string(), Value::String(txid.clone()));
                        }
                        let trade_pair = trade_clone.get("pair").and_then(|p| p.as_str()).unwrap_or(&canonical).to_string();
                        if trade_pair != canonical {
                            info!("Trade {} pair mismatch in QueryTrades: history={}, input={}", txid, trade_pair, canonical);
                        }
                        (trade_clone, trade_pair)
                    })
                    .collect()
            })
            .unwrap_or_default();
    }

    if trades.is_empty() || order_id.is_empty() {
        loop {
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let start_ms = now_ms.saturating_sub(86400000); // Always start from now-24h
            let history_params = format!(
                "nonce={}&trades=true&ofs={}&start={startup_time}&end={}",
                nonce, offset, start_ms,
            );
            let history_path = "/0/private/TradesHistory";
            let history_signature = client.sign_request(&history_path, nonce, &history_params).await?;
            let history_url = Url::parse(&format!("{}/0/private/TradesHistory", client.base_url()))?;
            let history_response: Value = client
                .api_call_with_retry(
                    || async {
                        let response = client
                            .http_client()
                            .post(history_url.clone())
                            .header("API-Key", client.api_key())
                            .header("API-Sign", &history_signature)
                            .header("Content-Type", "application/x-www-form-urlencoded")
                            .header("User-Agent", "Rust Trading Bot/1.0")
                            .body(history_params.clone())
                            .send()
                            .await
                            .map_err(|e| anyhow!("Failed to send TradesHistory request: {}", e))?;
                        let response_text = response
                            .text()
                            .await
                            .map_err(|e| anyhow!("Failed to read TradesHistory response: {}", e))?;
                        serde_json::from_str(&response_text).map_err(|e| {
                            anyhow!("Failed to parse TradesHistory JSON response: {}", e)
                        })
                    },
                    7,
                    "fetch_trades_history",
                )
                .await?;
            if let Some(error) = history_response.get("error").and_then(|e| e.as_array()) {
                if !error.is_empty() {
                    return Err(anyhow!("Kraken API TradesHistory error: {:?}", error));
                }
            }
            let new_trades = history_response["result"]["trades"]
                .as_object()
                .map(|obj| {
                    obj.iter()
                        .filter_map(|(txid, trade)| {
                            let mut trade_clone = trade.clone();
                            if let Some(obj_mut) = trade_clone.as_object_mut() {
                                obj_mut.insert("txid".to_string(), Value::String(txid.clone()));
                            }
                            let trade_pair = trade_clone.get("pair").and_then(|p| p.as_str()).unwrap_or(&canonical).to_string();
                            let trade_ordertxid = trade_clone.get("ordertxid").and_then(|o| o.as_str()).unwrap_or("");
                            if !order_id.is_empty() && trade_ordertxid != order_id {
                                return None;
                            }
                            if trade_pair != canonical {
                                info!("Trade {} pair mismatch: history={}, input={}", txid, trade_pair, canonical);
                            }
                            let trade_time_f64 = trade_clone.get("time").and_then(|t| t.as_f64()).unwrap_or(0.0);
                            let trade_time_ms = (trade_time_f64 * 1000.0).floor() as u64;
                            let now_ms = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64;
                            let cutoff_24h = now_ms.saturating_sub(86400000);
                            if trade_time_ms >= startup_time {
                                if trade_time_ms < cutoff_24h {
                                    info!("Backfill skipped: trade {} at {} >24h pre-bot (cutoff={})", txid, trade_time_ms, cutoff_24h);
                                    return None;
                                }
                                if !order_id.is_empty() {
                                    let trade_ordertxid = trade_clone.get("ordertxid").and_then(|o| o.as_str()).unwrap_or("");
                                    if trade_ordertxid != order_id {
                                        return None;
                                    }
                                }
                                Some((trade_clone, trade_pair))
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<(Value, String)>>()
                })
                .unwrap_or_default();
            let new_trades_len = new_trades.len();
            let new_trades_empty = new_trades.is_empty();
            trades.extend(new_trades);
            let count = history_response["result"]["count"].as_u64().unwrap_or(0);
            info!(target: "trade", "Fetched {} new trades for {} on {}, offset {}, total count {}", new_trades_len, order_id, pair, offset, count);
            if (trades.len() as u64) >= count || new_trades_empty {
                break;
            }
            offset += page_size;
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }

    info!(target: "trade", "Fetched {} trades via API for order {} on {}", trades.len(), order_id, pair);
    Ok(trades)
}

pub async fn fetch_trades(
    client: &KrakenClient,
    pair: &str,
    order_id: &str,
    startup_time: u64,
    state_manager: Arc<StateManager>,
    config: &Config,  // NEW: Added config param
    max_retries_override: Option<usize>,  // NEW: Pass override to via_api
) -> Result<Vec<Trade>> {
    info!("Fetching trades via API fallback for {} (order_id: {})", pair, order_id);

    let raw_trades_with_pairs = fetch_trades_via_api(client, pair, order_id, startup_time, config, max_retries_override).await?;

    let now_secs = Utc::now().timestamp() as u64;
    let cutoff_24h_secs = now_secs.saturating_sub(86400);

    let mut new_trades = vec![];

    for (trade_obj, trade_pair) in raw_trades_with_pairs {
        let time_f64 = trade_obj.get("time").and_then(|t| t.as_f64()).ok_or_else(|| {
            warn!("Skipping trade without time: {:?}", trade_obj);
            anyhow!("No time in trade")
        })?;
        let time_str = time_f64.to_string();
        let time_secs = time_f64.floor() as u64;

        if time_secs < startup_time || time_secs < cutoff_24h_secs {
            info!("Skipping old trade at {} for {}", time_str, trade_pair);
            continue;
        }

        let raw_id = trade_obj.get("txid").and_then(|id| id.as_str()).ok_or_else(|| anyhow!("No txid in trade"))?.to_string();
        let formatted_id = if raw_id.len() == 17 {
            format!("{}-{}-{}", &raw_id[0..6], &raw_id[6..11], &raw_id[11..17])
        } else {
            raw_id.clone()
        };

        let timestamp = parse_unix_timestamp(&time_str)
            .map_or_else(
                || get_current_time().format("%Y-%m-%d %H:%M:%S").to_string(),
                |dt| dt.with_timezone(&get_eastern_tz()).format("%Y-%m-%d %H:%M:%S").to_string()
            );

        let side = trade_obj.get("type").and_then(|t| t.as_str()).unwrap_or("unknown").to_string();

        let trade = Trade {
            timestamp,
            pair: trade_pair.clone(),
            trade_id: formatted_id.clone(),
            order_id: trade_obj.get("ordertxid").and_then(|o| o.as_str()).unwrap_or("").to_string(),
            trade_type: side.clone(),
            amount: Decimal::from_str_exact(trade_obj.get("vol").and_then(|v| v.as_str()).unwrap_or("0")).unwrap_or(Decimal::ZERO),
            execution_price: Decimal::from_str_exact(trade_obj.get("price").and_then(|p| p.as_str()).unwrap_or("0")).unwrap_or(Decimal::ZERO),
            fees: Decimal::from_str_exact(trade_obj.get("fee").and_then(|f| f.as_str()).unwrap_or("0")).unwrap_or(Decimal::ZERO),
            fee_percentage: Decimal::ZERO,
            profit: Decimal::ZERO,
            profit_percentage: Decimal::ZERO,
            reason: "".to_string(),
            avg_cost_basis: Decimal::ZERO,
            slippage: Decimal::ZERO,
            remaining_amount: Decimal::ZERO,
            open: 0,
            partial_open: 0,
        };

        if !state_manager.check_trade_exists_in_db(trade.pair.clone(), formatted_id.clone()).await? {
            if let Err(e) = state_manager.update_trades(trade.clone()).await {
                error!("Failed to update trade {}: {}", formatted_id, e);
                continue;
            }
            let trade_pair = trade.pair.clone();
            new_trades.push(trade);
            info!(target: "trade", "Inserted new trade {} for {}", formatted_id, trade_pair);
        } else {
            info!(target: "trade", "Trade {} already exists for {}", formatted_id, trade.pair);
        }
    }

    info!("Processed {} new trades for {}", new_trades.len(), pair);
    Ok(new_trades)
}