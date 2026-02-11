// websocket.rs
// Description: Handles WebSocket connections to Kraken for real-time OHLC, price updates, and private executions/open_orders using v2.

// IMPORTS

use crate::statemanager::TradeManagerMessage;
use crate::record_trade_count;
use crate::config::load_config;
use crate::record_error;
use crate::api::KrakenClient;
use crate::statemanager::{OhlcUpdate, PriceUpdate, StateManager, Trade};
use anyhow::{anyhow, Result};
use chrono::{Local, Utc};
use futures_util::{SinkExt, stream::StreamExt};
use serde_json::{json, Value};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn, trace};
use chrono::DateTime;
use rust_decimal::prelude::*;
use crate::trading::trading_logic::update_remaining_quantities;
use std::collections::HashMap;

// WEBSOCKET FUNCTIONS

pub async fn watch_kraken_prices(
    pairs: Vec<String>, 
    tx: mpsc::Sender<PriceUpdate>,
    ohlc_tx: mpsc::Sender<OhlcUpdate>,
    state_manager: Arc<StateManager>,
    client: KrakenClient,
    ready_tx: mpsc::Sender<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ws_url = "wss://ws.kraken.com/v2";
    let max_retries = 5;
    let mut retry_count: u32 = 0;
    let config = load_config()?;
    let mut ready_sent = false;

    loop {
        info!("Connecting to public Kraken WebSocket v2 at {}", ws_url);
        let (ws_stream, _) = match connect_async(ws_url).await {
            Ok(stream) => stream,
            Err(e) => {
                error!("Public WebSocket connection error: {}", e);
                record_error("websocket_connect_public");
                retry_count += 1;
                if retry_count >= max_retries as u32 {
                    return Err("Max reconnect attempts reached for public WS".into());
                }
                let backoff = std::cmp::min(2u64.pow(retry_count as u32), 30);
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                continue;
            }
        };
        let (mut write, mut read) = ws_stream.split();
        let mapped_pairs: Vec<String> = config.portfolio.ws_pairs.value.clone(); // Use slashed directly

        // OHLC sub
        let ohlc_sub = json!({
            "method": "subscribe",
            "params": {
                "channel": "ohlc",
                "interval": 1,
                "snapshot": true,
                "symbol": mapped_pairs.clone()
            }
        });
        let ohlc_str = ohlc_sub.to_string();
        info!("Subscribing to public OHLC v2: {}", ohlc_str);
        if write.send(Message::Text(ohlc_str)).await.is_err() {
            error!("Failed to send public OHLC subscription");
            record_error("websocket_subscribe_public");
            retry_count += 1;
            if retry_count >= max_retries as u32 {
                return Err("Failed to send public OHLC sub after max retries".into());
            }
            let backoff = std::cmp::min(2u64.pow(retry_count as u32), 30);
            tokio::time::sleep(Duration::from_secs(backoff)).await;
            continue;
        }

        // Ticker sub
        let ticker_sub = json!({
            "method": "subscribe",
            "params": {
                "channel": "ticker",
                "snapshot": true,
                "symbol": mapped_pairs.clone()
            }
        });
        let ticker_str = ticker_sub.to_string();
        info!("Subscribing to public ticker v2: {}", ticker_str);
        if write.send(Message::Text(ticker_str)).await.is_err() {
            error!("Failed to send public ticker subscription");
            record_error("websocket_subscribe_ticker");
            retry_count += 1;
            if retry_count >= max_retries as u32 {
                return Err("Failed to send public ticker sub after max retries".into());
            }
            let backoff = std::cmp::min(2u64.pow(retry_count as u32), 30);
            tokio::time::sleep(Duration::from_secs(backoff)).await;
            continue;
        }

        // Wait for OHLC and ticker confirmation with increased polling
        let mut ohlc_subscribed = false;
        let mut ticker_subscribed = false;
        let mut poll_count = 0;
        while poll_count < 5 && (!ohlc_subscribed || !ticker_subscribed) {
            if let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        trace!("Public WS raw message: {}", text);
                        let parsed: Value = match serde_json::from_str(&text) {
                            Ok(v) => v,
                            Err(e) => {
                                error!("Failed to parse public WS msg: {}", e);
                                record_error("websocket_parse_public");
                                break;
                            }
                        };
                        if parsed.get("method") == Some(&Value::String("subscribe".to_string())) && parsed.get("success") == Some(&Value::Bool(true)) {
                            if let Some(channel) = parsed.get("result").and_then(|r| r.get("channel")).and_then(|c| c.as_str()) {
                                if channel == "ohlc" {
                                    info!("Public OHLC sub confirmed v2");
                                    ohlc_subscribed = true;
                                } else if channel == "ticker" {
                                    info!("Public ticker sub confirmed v2");
                                    ticker_subscribed = true;
                                } else {
                                    error!("Public sub failed: {:?}", parsed);
                                    record_error("websocket_sub_public");
                                    let mut file = OpenOptions::new().create(true).append(true).open("log/error_log.txt").unwrap_or_else(|_| panic!("Failed to open error log"));
                                    writeln!(file, "[{}] Public sub failed: {:?}", chrono::Local::now().to_string(), parsed).unwrap_or(());
                                }
                            }
                            if ohlc_subscribed && ticker_subscribed && !ready_sent {
                                let _ = ready_tx.send(()).await;
                                ready_sent = true;
                            }
                        }
                    }
                    Ok(Message::Close(Some(close_frame))) => {
                        info!("Closed with code: {}, reason: {}", close_frame.code, close_frame.reason);
                        error!("Public WS closed by server");
                        record_error("websocket_close_public");
                        break;
                    }
                    Ok(Message::Close(None)) => {
                        info!("Closed without code/reason");
                        error!("Public WS closed by server without frame");
                        record_error("websocket_close_public");
                        break;
                    }
                    Err(e) => {
                        error!("Public WS error before sub: {}", e);
                        record_error("websocket_error_public");
                        break;
                    }
                    _ => {}
                }
            }
            poll_count += 1;
            if poll_count < 5 && (!ohlc_subscribed || !ticker_subscribed) {
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
        if !ohlc_subscribed || !ticker_subscribed {
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open("log/error_log.txt")
                .map_err(|e| anyhow!("Failed to open error log: {}", e))?;
            writeln!(
                file,
                "[{}] Public subs not fully confirmed after {} polls (ohlc: {}, ticker: {})",
                chrono::Local::now().to_string(),
                poll_count,
                ohlc_subscribed,
                ticker_subscribed
            )
            .map_err(|e| anyhow!("Failed to write to error log: {}", e))?;
            warn!("Public subs incomplete after {} polls; continuing but expect gaps", poll_count);
        }

        // Send initial ping after subs
        let initial_ping = json!({
            "method": "ping",
            "req_id": 0
        });
        if write.send(Message::Text(initial_ping.to_string())).await.is_err() {
            error!("Failed to send initial ping");
            continue;
        }
        info!("Sent initial ping after public subs");

        retry_count = 0;
        let mut ping_id: u64 = 0;
        let mut ping_interval = tokio::time::interval(Duration::from_secs(20)); // Adjusted to 20s
        let mut last_activity = tokio::time::Instant::now();
        let inactivity_timeout = Duration::from_secs(60); // Break if no messages for 60s
        let mut sub_retry_counts: HashMap<String, u32> = HashMap::new(); // Track retries per channel

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    ping_id += 1;
                    let ping = json!({
                        "method": "ping",
                        "req_id": ping_id
                    });
                    if write.send(Message::Text(ping.to_string())).await.is_err() {
                        error!("Failed to send ping");
                        break;
                    }
                }
                msg_opt = read.next() => {
                    last_activity = tokio::time::Instant::now(); // Reset on any activity
                    match msg_opt {
                        Some(Ok(Message::Text(text))) => {
                            trace!("Public WS raw message: {}", text);
                            let parsed: Value = match serde_json::from_str(&text) {
                                Ok(v) => v,
                                Err(e) => {
                                    error!("Failed to parse public WS msg: {}", e);
                                    record_error("websocket_parse_public");
                                    continue;
                                }
                            };

                            if parsed.get("method") == Some(&Value::String("pong".to_string())) {
                                if let Some(id) = parsed.get("req_id").and_then(|r| r.as_u64()) {
                                    if id == ping_id {
                                        continue;
                                    } else {
                                        warn!("Pong req_id mismatch: got {}, expected {}", id, ping_id);
                                    }
                                } else {
                                    warn!("Pong without req_id");
                                }
                                continue;
                            }

                            if parsed.get("channel") == Some(&Value::String("heartbeat".to_string())) {
                                // info!("Received heartbeat"); silenced unless needed for debugging.
                                continue;
                            }

                            if parsed.get("success") == Some(&Value::Bool(false)) && parsed.get("method") == Some(&Value::String("subscribe".to_string())) {
                                if let Some(error) = parsed.get("error").and_then(|e| e.as_str()) {
                                    error!("Public WS sub error: {}", error);
                                }
                                if let Some(channel) = parsed.get("result").and_then(|r| r.get("channel")).and_then(|c| c.as_str()) {
                                    let retry_key = channel.to_string();
                                    let retries = sub_retry_counts.entry(retry_key.clone()).or_insert(0);
                                    *retries += 1;
                                    if *retries < 3 {
                                        info!("Retrying sub for channel {} after 5s (attempt {}/3)", channel, *retries);
                                        tokio::time::sleep(Duration::from_secs(5)).await;
                                        let sub_json = if channel == "ohlc" {
                                            ohlc_sub.clone()
                                        } else if channel == "ticker" {
                                            ticker_sub.clone()
                                        } else {
                                            continue;
                                        };
                                        if write.send(Message::Text(sub_json.to_string())).await.is_err() {
                                            error!("Failed to resend sub for {}", channel);
                                            break;
                                        }
                                    } else {
                                        warn!("Max sub retries for {}; triggering reconnect", channel);
                                        sub_retry_counts.remove(&retry_key);
                                        break;
                                    }
                                }
                                continue;
                            }

                            if parsed.get("channel") == Some(&Value::String("ohlc".to_string())) && parsed.get("type") == Some(&Value::String("update".to_string())) {
                                if let Some(data_array) = parsed.get("data").and_then(|d| d.as_array()) {
                                    for data in data_array {
                                        let time_str = data.get("interval_begin").and_then(|t| t.as_str()).unwrap_or("");
                                        if time_str.is_empty() {
                                            warn!("Skipping OHLC update with empty time field for data: {:?}", data);
                                            continue;
                                        }
                                        let timestamp_utc = match DateTime::parse_from_rfc3339(time_str) {
                                            Ok(dt) => dt.with_timezone(&Utc),
                                            Err(e) => {
                                                warn!("Failed to parse OHLC time '{}': {}", time_str, e);
                                                continue;
                                            }
                                        };
                                        let timestamp = timestamp_utc.with_timezone(&Local).format("%Y-%m-%d %H:%M:%S").to_string();

                                        let ws_pair = data.get("symbol").and_then(|s| s.as_str()).unwrap_or("").to_string();
                                        let api_pair = config
                                            .portfolio
                                            .ws_pairs
                                            .value
                                            .iter()
                                            .zip(config.portfolio.api_pairs.value.iter())
                                            .find(|(ws, _)| *ws == &ws_pair)
                                            .map(|(_, api)| api.clone())
                                            .unwrap_or(ws_pair.clone());

                                        let open_str = data.get("open").map(|o| o.to_string()).unwrap_or("0".to_string());
                                        let open = Decimal::from_str(&open_str).unwrap_or(Decimal::ZERO);
                                        let high_str = data.get("high").map(|h| h.to_string()).unwrap_or("0".to_string());
                                        let high = Decimal::from_str(&high_str).unwrap_or(Decimal::ZERO);
                                        let low_str = data.get("low").map(|l| l.to_string()).unwrap_or("0".to_string());
                                        let low = Decimal::from_str(&low_str).unwrap_or(Decimal::ZERO);
                                        let close_str = data.get("close").map(|c| c.to_string()).unwrap_or("0".to_string());
                                        let close = Decimal::from_str(&close_str).unwrap_or(Decimal::ZERO);
                                        let volume_str = data.get("volume").map(|v| v.to_string()).unwrap_or("0".to_string());
                                        let volume = Decimal::from_str(&volume_str).unwrap_or(Decimal::ZERO);

                                        let is_valid_ohlc = close > Decimal::ZERO && high >= low && high >= open && low <= open && volume >= Decimal::ZERO;
                                        if !is_valid_ohlc {
                                            warn!("Invalid OHLC for {}: open={}, high={}, low={}, close={}, vol={}", api_pair, open.to_f64().unwrap_or(0.0), high.to_f64().unwrap_or(0.0), low.to_f64().unwrap_or(0.0), close.to_f64().unwrap_or(0.0), volume.to_f64().unwrap_or(0.0));
                                            let mut file = OpenOptions::new().create(true).append(true).open("log/error_log.txt").unwrap_or_else(|_| panic!("Failed to open error log"));
                                            writeln!(file, "[{}] Invalid OHLC for {}: open={}, high={}, low={}, close={}, vol={}", chrono::Local::now().to_string(), api_pair, open.to_f64().unwrap_or(0.0), high.to_f64().unwrap_or(0.0), low.to_f64().unwrap_or(0.0), close.to_f64().unwrap_or(0.0), volume.to_f64().unwrap_or(0.0)).unwrap_or(());
                                        }

                                        let update = PriceUpdate {
                                            pair: api_pair.clone(),
                                            bid: Decimal::ZERO,
                                            ask: Decimal::ZERO,
                                            close_price: close,
                                            timestamp: timestamp.clone(),
                                        };
                                        let ohlc_update = OhlcUpdate {
                                            pair: api_pair.clone(),
                                            open,
                                            high,
                                            low,
                                            close,
                                            volume,
                                            timestamp,
                                        };
                                        if tx.send(update).await.is_err() {
                                            error!("Failed to send public PriceUpdate for {}", api_pair);
                                            record_error("price_update_send_public");
                                            break;
                                        }
                                        if ohlc_tx.send(ohlc_update).await.is_err() {
                                            error!("Failed to send public OhlcUpdate for {}", api_pair);
                                            record_error("ohlc_update_send_public");
                                            break;
                                        }
                                    }
                                }
                            } else if parsed.get("channel") == Some(&Value::String("ticker".to_string())) && parsed.get("type") == Some(&Value::String("update".to_string())) {
                                if let Some(data) = parsed.get("data").and_then(|d| d.as_object()) {
                                    let ws_pair = data.get("symbol").and_then(|s| s.as_str()).unwrap_or("").to_string();
                                    let api_pair = config
                                        .portfolio
                                        .ws_pairs
                                        .value
                                        .iter()
                                        .zip(config.portfolio.api_pairs.value.iter())
                                        .find(|(ws, _)| *ws == &ws_pair)
                                        .map(|(_, api)| api.clone())
                                        .unwrap_or(ws_pair.clone());

                                    let timestamp_utc = Utc::now();
                                    let timestamp = timestamp_utc.with_timezone(&Local).format("%Y-%m-%d %H:%M:%S").to_string();

                                    let close_str = data.get("close").and_then(|c| c.get("price")).map(|p| p.to_string()).unwrap_or("0".to_string());
                                    let close = if close_str.contains('e') || close_str.contains('E') {
                                        Decimal::from_scientific(&close_str).unwrap_or(Decimal::ZERO)
                                    } else {
                                        Decimal::from_str_exact(&close_str).unwrap_or(Decimal::ZERO)
                                    };
                                    let bid_str = data.get("bid").map(|b| b.to_string()).unwrap_or("0".to_string());
                                    let bid = if bid_str.contains('e') || bid_str.contains('E') {
                                        Decimal::from_scientific(&bid_str).unwrap_or(Decimal::ZERO)
                                    } else {
                                        Decimal::from_str_exact(&bid_str).unwrap_or(Decimal::ZERO)
                                    };
                                    let ask_str = data.get("ask").map(|a| a.to_string()).unwrap_or("0".to_string());
                                    let ask = if ask_str.contains('e') || ask_str.contains('E') {
                                        Decimal::from_scientific(&ask_str).unwrap_or(Decimal::ZERO)
                                    } else {
                                        Decimal::from_str_exact(&ask_str).unwrap_or(Decimal::ZERO)
                                    };

                                    let is_valid_ticker = close > Decimal::ZERO && bid > Decimal::ZERO && ask > Decimal::ZERO && ask >= bid;
                                    if !is_valid_ticker {
                                        warn!("Invalid ticker for {}: close={}, bid={}, ask={}", api_pair, close, bid, ask);
                                        let mut file = OpenOptions::new().create(true).append(true).open("log/error_log.txt").unwrap_or_else(|_| panic!("Failed to open error log"));
                                        writeln!(file, "[{}] Invalid ticker for {}: close={}, bid={}, ask={}", chrono::Local::now().to_string(), api_pair, close, bid, ask).unwrap_or(());
                                    }

                                    let update = PriceUpdate {
                                        pair: api_pair.clone(),
                                        bid,
                                        ask,
                                        close_price: close,
                                        timestamp,
                                    };
                                    if tx.send(update).await.is_err() {
                                        error!("Failed to send public PriceUpdate (ticker) for {}", api_pair);
                                        record_error("price_update_send_ticker");
                                        break;
                                    }
                                }
                            } else if parsed.get("success") == Some(&Value::Bool(false)) {
                                if let Some(error) = parsed.get("error").and_then(|e| e.as_str()) {
                                    error!("Public WS error: {}", error);
                                }
                            }
                        }
                        Some(Ok(Message::Close(Some(close_frame)))) => {
                            info!("Closed with code: {}, reason: {}", close_frame.code, close_frame.reason);
                            error!("Public WS closed by server");
                            record_error("websocket_close_public");
                            break;
                        }
                        Some(Ok(Message::Close(None))) => {
                            info!("Closed without code/reason");
                            error!("Public WS closed by server without frame");
                            record_error("websocket_close_public");
                            break;
                        }
                        Some(Err(e)) => {
                            error!("Public WS error: {}", e);
                            record_error("websocket_error_public");
                            break;
                        }
                        None => {
                            warn!("Transient None from read.next(); waiting 5s before potential break");
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            if last_activity.elapsed() > inactivity_timeout {
                                info!("Inactivity timeout exceeded; breaking for reconnect");
                                break;
                            }
                            continue; // Don't break immediately
                        }
                        _ => {}
                    }
                }
                _ = tokio::time::sleep(inactivity_timeout) => {
                    if last_activity.elapsed() > inactivity_timeout {
                        info!("Inactivity timeout; breaking for reconnect");
                        break;
                    }
                }
            }
        }
        retry_count += 1;
        if retry_count >= max_retries as u32 {
            return Err("Max reconnect attempts reached for public WS".into());
        }
        let backoff = std::cmp::min(2u64.pow(retry_count as u32), 30);
        info!("Reconnecting public WS after {}s", backoff);
        tokio::time::sleep(Duration::from_secs(backoff)).await;
    }
}

pub async fn watch_kraken_private(
    state_manager: Arc<StateManager>,
    mut client: KrakenClient,
    ready_tx: mpsc::Sender<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ws_url = "wss://ws-auth.kraken.com/v2";
    let max_retries = 5;
    let mut retry_count: u32 = 0;
    let config = load_config()?;
    let mut ready_sent = false;

    // Initial token refresh
    if let Err(e) = client.refresh_ws_token().await {
        error!("Failed initial private WS token: {}", e);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("log/error_log.txt")
            .map_err(|e| anyhow!("Failed to open error log: {}", e))?;
        writeln!(
            file,
            "[{}] Private token refresh failed: {}",
            chrono::Local::now().to_string(),
            e
        )
        .map_err(|e| anyhow!("Failed to write to error log: {}", e))?;
    }

    loop {
        info!("Connecting to private Kraken WebSocket v2 at {}", ws_url);
        let (ws_stream, _) = match connect_async(ws_url).await {
            Ok(stream) => stream,
            Err(e) => {
                error!("Private WebSocket connection error: {}", e);
                record_error("websocket_connect_private");
                retry_count += 1;
                if retry_count >= max_retries as u32 {
                    return Err("Max reconnect attempts reached for private WS".into());
                }
                let backoff = std::cmp::min(2u64.pow(retry_count as u32), 30);
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                continue;
            }
        };
        let (mut write, mut read) = ws_stream.split();

        if let Err(e) = client.refresh_ws_token().await {
            error!("Token refresh failed before private subs: {}", e);
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open("log/error_log.txt")
                .map_err(|e| anyhow!("Failed to open error log: {}", e))?;
            writeln!(
                file,
                "[{}] Token refresh failed before private subs: {}",
                chrono::Local::now().to_string(),
                e
            )
            .map_err(|e| anyhow!("Failed to write to error log: {}", e))?;
            retry_count += 1;
            if retry_count >= max_retries as u32 {
                return Err("Failed token refresh after max retries".into());
            }
            let backoff = std::cmp::min(2u64.pow(retry_count as u32), 30);
            tokio::time::sleep(Duration::from_secs(backoff)).await;
            continue;
        }

        let token = match client.ws_token() {
            Some(t) => t.token.clone(),
            None => {
                error!("No token for private subs");
                retry_count += 1;
                if retry_count >= max_retries as u32 {
                    return Err("No token after refresh".into());
                }
                let backoff = std::cmp::min(2u64.pow(retry_count as u32), 30);
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                continue;
            }
        };

        // executions sub (combines ownTrades and openOrders)
        let executions_sub = json!({
            "method": "subscribe",
            "params": {
                "channel": "executions",
                "snap_orders": true,
                "snap_trades": true,
                "token": token.clone()
            }
        });
        let executions_str = executions_sub.to_string();
        info!("Subscribing to private executions v2: {}", executions_str);
        if write.send(Message::Text(executions_str)).await.is_err() {
            error!("Failed to send executions sub");
            record_error("websocket_executions_sub_private");
            retry_count += 1;
            if retry_count >= max_retries as u32 {
                return Err("Failed to send executions sub after max retries".into());
            }
            let backoff = std::cmp::min(2u64.pow(retry_count as u32), 30);
            tokio::time::sleep(Duration::from_secs(backoff)).await;
            continue;
        }

        let mut executions_subscribed = false;
        let mut poll_count = 0;
        while poll_count < 5 && !executions_subscribed {
            if let Some(msg) = read.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        trace!("Private WS raw message: {}", text);
                        let parsed: Value = match serde_json::from_str(&text) {
                            Ok(v) => v,
                            Err(e) => {
                                error!("Failed to parse private WS msg during poll: {}", e);
                                record_error("websocket_parse_private");
                                break;
                            }
                        };
                        if parsed.get("method") == Some(&Value::String("subscribe".to_string())) && parsed.get("success") == Some(&Value::Bool(true)) {
                            if parsed.get("result").and_then(|r| r.get("channel")).and_then(|c| c.as_str()) == Some("executions") {
                                info!("Private executions sub confirmed v2");
                                executions_subscribed = true;
                                if !ready_sent {
                                    let _ = ready_tx.send(()).await;
                                    ready_sent = true;
                                }
                            } else {
                                error!("Private executions sub failed: {:?}", parsed);
                                record_error("websocket_executions_status_private");
                                let mut file = OpenOptions::new()
                                    .create(true)
                                    .append(true)
                                    .open("log/error_log.txt")
                                    .map_err(|e| anyhow!("Failed to open error log: {}", e))?;
                                writeln!(
                                    file,
                                    "[{}] Private executions sub failed: {:?}",
                                    chrono::Local::now().to_string(),
                                    parsed
                                )
                                .map_err(|e| anyhow!("Failed to write to error log: {}", e))?;
                            }
                        }
                    }
                    Ok(Message::Close(Some(close_frame))) => {
                        info!("Closed with code: {}, reason: {}", close_frame.code, close_frame.reason);
                        error!("Private WS closed by server");
                        record_error("websocket_close_private");
                        break;
                    }
                    Ok(Message::Close(None)) => {
                        info!("Closed without code/reason");
                        error!("Private WS closed by server without frame");
                        record_error("websocket_close_private");
                        break;
                    }
                    Err(e) => {
                        error!("Private WS error before subs: {}", e);
                        record_error("websocket_error_private");
                        break;
                    }
                    _ => {}
                }
            }
            poll_count += 1;
            if poll_count < 5 && !executions_subscribed {
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
        if !executions_subscribed {
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open("log/error_log.txt")
                .map_err(|e| anyhow!("Failed to open error log: {}", e))?;
            writeln!(
                file,
                "[{}] Private subs not fully confirmed after {} polls (executions: {})",
                chrono::Local::now().to_string(),
                poll_count,
                executions_subscribed
            )
            .map_err(|e| anyhow!("Failed to write to error log: {}", e))?;
            warn!("Private subs incomplete after {} polls; continuing but expect gaps", poll_count);
        }

        // Send initial ping after subs
        let initial_ping = json!({
            "method": "ping",
            "req_id": 0
        });
        if write.send(Message::Text(initial_ping.to_string())).await.is_err() {
            error!("Failed to send initial ping");
            continue;
        }
        info!("Sent initial ping after private sub");

        retry_count = 0;
        let mut ping_id: u64 = 0;
        let mut ping_interval = tokio::time::interval(Duration::from_secs(20)); // Adjusted to 20s
        let mut last_activity = tokio::time::Instant::now();
        let inactivity_timeout = Duration::from_secs(60); // Break if no messages for 60s

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    ping_id += 1;
                    let ping = json!({
                        "method": "ping",
                        "req_id": ping_id
                    });
                    if write.send(Message::Text(ping.to_string())).await.is_err() {
                        error!("Failed to send ping");
                        break;
                    }
                }
                msg_opt = read.next() => {
                    last_activity = tokio::time::Instant::now(); // Reset on any activity
                    match msg_opt {
                        Some(Ok(Message::Text(text))) => {
                            trace!("Private WS raw message: {}", text);
                            let parsed: Value = match serde_json::from_str(&text) {
                                Ok(v) => v,
                                Err(e) => {
                                    error!("Failed to parse private WS msg: {}", e);
                                    record_error("websocket_parse_private");
                                    continue;
                                }
                            };

                            if parsed.get("method") == Some(&Value::String("pong".to_string())) {
                                if let Some(id) = parsed.get("req_id").and_then(|r| r.as_u64()) {
                                    if id == ping_id {
                                        continue;
                                    } else {
                                        warn!("Pong req_id mismatch: got {}, expected {}", id, ping_id);
                                    }
                                } else {
                                    warn!("Pong without req_id");
                                }
                                continue;
                            }

                            if parsed.get("success") == Some(&Value::Bool(false)) {
                                if let Some(error) = parsed.get("error").and_then(|e| e.as_str()) {
                                    error!("Private WS error: {}", error);
                                    if error.contains("token") {
                                        error!("Token expiry detected; refreshing...");
                                        if let Err(e) = client.refresh_ws_token().await {
                                            error!("Private token refresh failed: {}", e);
                                        }
                                        let new_token = client.ws_token().as_ref().map(|t| t.token.clone());
                                        if let Some(t) = new_token {
                                            let resub = json!({
                                                "method": "subscribe",
                                                "params": {
                                                    "channel": "executions",
                                                    "snapshot": true,
                                                    "token": t
                                                }
                                            });
                                            let _ = write.send(Message::Text(resub.to_string())).await;
                                            info!("Resubscribed private executions after token refresh");
                                        }
                                    }
                                }
                                continue;
                            }

                            if parsed.get("channel") == Some(&Value::String("executions".to_string())) && parsed.get("type") == Some(&Value::String("update".to_string())) {
                                if let Some(data_array) = parsed.get("data").and_then(|d| d.as_array()) {
                                    for item in data_array {
                                        let exec_type = item.get("exec_type").and_then(|et| et.as_str()).unwrap_or("");
                                        let timestamp_str = item.get("timestamp").and_then(|t| t.as_str()).unwrap_or("");
                                        if timestamp_str.is_empty() {
                                            warn!("Skipping execution update with empty timestamp for item: {:?}", item);
                                            continue;
                                        }
                                        let timestamp_utc = match DateTime::parse_from_rfc3339(timestamp_str) {
                                            Ok(dt) => dt.with_timezone(&Utc),
                                            Err(e) => {
                                                warn!("Failed to parse execution timestamp '{}': {}", timestamp_str, e);
                                                continue;
                                            }
                                        };
                                        let timestamp = timestamp_utc.with_timezone(&Local).format("%Y-%m-%d %H:%M:%S").to_string();

                                        let ws_pair = item.get("symbol").and_then(|p| p.as_str()).unwrap_or("");
                                        let api_pair = config
                                            .portfolio
                                            .ws_pairs
                                            .value
                                            .iter()
                                            .zip(config.portfolio.api_pairs.value.iter())
                                            .find(|(ws, _)| *ws == ws_pair)
                                            .map(|(_, api)| api.clone())
                                            .unwrap_or(ws_pair.to_string());

                                        let order_id = item.get("order_id").and_then(|o| o.as_str()).unwrap_or("").to_string();

                                        if exec_type == "trade" {
                                            // Treat as trade (fill)
                                            let side = item.get("side").and_then(|s| s.as_str()).unwrap_or("unknown").to_string();
                                            let amount_str = item.get("last_qty").map(|a| a.to_string()).unwrap_or("0".to_string());
                                            let amount = Decimal::from_str_exact(&amount_str).unwrap_or(Decimal::ZERO);
                                            let price_str = item.get("last_price").map(|p| p.to_string()).unwrap_or("0".to_string());
                                            let execution_price = if price_str.contains('e') || price_str.contains('E') {
                                                Decimal::from_scientific(&price_str).unwrap_or(Decimal::ZERO)
                                            } else {
                                                Decimal::from_str_exact(&price_str).unwrap_or(Decimal::ZERO)
                                            };
                                            let fees = if let Some(fees_arr) = item.get("fees").and_then(|f| f.as_array()) {
                                                fees_arr.iter().fold(Decimal::ZERO, |acc, fee_obj| {
                                                    let qty_str = fee_obj.get("qty").map(|q| q.to_string()).unwrap_or("0".to_string());
                                                    acc + Decimal::from_str_exact(&qty_str).unwrap_or(Decimal::ZERO)
                                                })
                                            } else {
                                                Decimal::ZERO
                                            };

                                            let mut reason = "".to_string();
                                            if !order_id.is_empty() {
                                                let (resp_tx, resp_rx) = oneshot::channel();
                                                state_manager.trade_manager_tx
                                                    .send(TradeManagerMessage::GetOrderReason {
                                                        pair: api_pair.clone(),
                                                        order_id: order_id.clone(),
                                                        reply: resp_tx,
                                                    })
                                                    .await?;
                                                if let Ok(Some(r)) = resp_rx.await {
                                                    reason = r;
                                                } else {
                                                    warn!("reason_unset: no order match for {} in {}", order_id, api_pair);
                                                }
                                            }

                                            let now_secs = Utc::now().timestamp() as u64;
                                            let startup_secs = client.startup_time();
                                            let cutoff_24h_secs = now_secs.saturating_sub(86400);
                                            let time_secs = timestamp_utc.timestamp() as u64;

                                            if time_secs < startup_secs {
                                                info!("Private WS trade filtered: time={} < startup={}", time_secs, startup_secs);
                                                continue;
                                            }
                                            if time_secs < cutoff_24h_secs {
                                                continue;
                                            }

                                            let trade_id = item.get("exec_id").and_then(|id| id.as_str()).unwrap_or(&order_id).to_string(); // Use exec_id or order_id

                                            let trade = Trade {
                                                timestamp,
                                                pair: api_pair.clone(),
                                                trade_id: trade_id.clone(),
                                                order_id: order_id.clone(),
                                                trade_type: side.clone(),
                                                amount,
                                                execution_price,
                                                fees,
                                                fee_percentage: Decimal::ZERO,
                                                profit: Decimal::ZERO,
                                                profit_percentage: Decimal::ZERO,
                                                reason,
                                                avg_cost_basis: Decimal::ZERO,
                                                slippage: Decimal::ZERO,
                                                remaining_amount: Decimal::ZERO,
                                                open: 0,
                                                partial_open: 0,
                                            };
                                            if let Err(e) = state_manager.update_trades(trade.clone()).await {
                                                error!("Failed to update private WS trade {}: {}", trade_id, e);
                                            }
                                            record_trade_count(&api_pair, &side, &trade);
                                            info!(target: "trade", "Private WS trade captured: {} for {}", trade_id, api_pair);

                                            // NEW: After update_trades, fetch OrderInfo and call update_remaining_quantities for sells
                                            if side == "sell" {
                                                let config = load_config()?;
                                                let (tx, rx) = oneshot::channel();
                                                state_manager.trade_manager_tx
                                                    .send(TradeManagerMessage::GetOpenOrders {
                                                        pair: api_pair.clone(),
                                                        reply: tx,
                                                    })
                                                    .await?;
                                                if let Ok(orders) = rx.await {
                                                    if let Some(order_info) = orders.get(&order_id) {
                                                        let buy_trade_ids = order_info.buy_trade_ids.clone();
                                                        let total_buy_qty = order_info.total_buy_qty;
                                                        let filled_qty = amount;
                                                        let base_currency = api_pair.split("USD").next().unwrap_or("");
                                                        if let Err(e) = update_remaining_quantities(
                                                            &api_pair,
                                                            filled_qty,
                                                            buy_trade_ids,
                                                            state_manager.clone(),
                                                            &config,
                                                            base_currency,
                                                        ).await {
                                                            error!("Failed to update remaining quantities after WS sell trade {}: {}", trade_id, e);
                                                        }
                                                    } else {
                                                        warn!("No OrderInfo found for {} in WS sell trade processing", order_id);
                                                    }
                                                }
                                            }

                                            // Also update status for the fill
                                            let status = item.get("order_status").and_then(|s| s.as_str()).unwrap_or("").to_string();
                                            let vol_exec_str = item.get("cum_qty").map(|v| v.to_string()).unwrap_or("0".to_string());
                                            let vol_exec = Decimal::from_str_exact(&vol_exec_str).unwrap_or(Decimal::ZERO);
                                            let pair_opt = Some(api_pair.clone());

                                            let (resp_tx, resp_rx) = oneshot::channel();
                                            if state_manager.trade_manager_tx.send(TradeManagerMessage::UpdateOrderStatus {
                                                order_id: order_id.clone(),
                                                pair: pair_opt,
                                                status,
                                                vol_exec,
                                                reply: resp_tx,
                                            }).await.is_ok() {
                                                let _ = resp_rx.await;
                                                info!(target: "trade", "Updated order status via private WS after trade: {} (vol_exec: {})", order_id, vol_exec.to_f64().unwrap_or(0.0));
                                            } else {
                                                error!("Failed to send UpdateOrderStatus for {}", order_id);
                                            }
                                        } else {
                                            // Treat as order status update
                                            let status = item.get("order_status").and_then(|s| s.as_str()).unwrap_or("").to_string();
                                            let vol_exec_str = item.get("cum_qty").map(|v| v.to_string()).unwrap_or("0".to_string());
                                            let vol_exec = Decimal::from_str_exact(&vol_exec_str).unwrap_or(Decimal::ZERO);
                                            let pair_opt = Some(api_pair.clone());

                                            let (resp_tx, resp_rx) = oneshot::channel();
                                            if state_manager.trade_manager_tx.send(TradeManagerMessage::UpdateOrderStatus {
                                                order_id: order_id.clone(),
                                                pair: pair_opt,
                                                status,
                                                vol_exec,
                                                reply: resp_tx,
                                            }).await.is_ok() {
                                                let _ = resp_rx.await;
                                                info!(target: "trade", "Updated order status via private WS: {} (vol_exec: {})", order_id, vol_exec.to_f64().unwrap_or(0.0));
                                            } else {
                                                error!("Failed to send UpdateOrderStatus for {}", order_id);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Some(Ok(Message::Close(Some(close_frame)))) => {
                            info!("Closed with code: {}, reason: {}", close_frame.code, close_frame.reason);
                            error!("Private WS closed by server");
                            record_error("websocket_close_private");
                            break;
                        }
                        Some(Ok(Message::Close(None))) => {
                            info!("Closed without code/reason");
                            error!("Private WS closed by server without frame");
                            record_error("websocket_close_private");
                            break;
                        }
                        Some(Err(e)) => {
                            error!("Private WS error: {}", e);
                            record_error("websocket_error_private");
                            break;
                        }
                        None => {
                            warn!("Transient None from read.next(); waiting 5s before potential break");
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            if last_activity.elapsed() > inactivity_timeout {
                                info!("Inactivity timeout exceeded; breaking for reconnect");
                                break;
                            }
                            continue; // Don't break immediately
                        }
                        _ => {}
                    }
                }
                _ = tokio::time::sleep(inactivity_timeout) => {
                    if last_activity.elapsed() > inactivity_timeout {
                        info!("Inactivity timeout; breaking for reconnect");
                        break;
                    }
                }
            }
        }
        retry_count += 1;
        if retry_count >= max_retries as u32 {
            return Err("Max reconnect attempts reached for private WS".into());
        }
        let backoff = std::cmp::min(2u64.pow(retry_count as u32), 30);
        info!("Reconnecting private WS after {}s", backoff);
        tokio::time::sleep(Duration::from_secs(backoff)).await;
    }
}