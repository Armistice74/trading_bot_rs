// main.rs
// Description: Entry point of the trading bot. Handles initialization, task spawning, and shutdown.

// IMPORTS AND MODS

use anyhow::{anyhow, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use chrono::{DateTime, Local, NaiveDateTime, TimeZone};
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::{mpsc, broadcast};
use tracing::{error, info, Level};
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::fmt::time::ChronoLocal;
use tracing_subscriber::prelude::*;
use tracing::warn;
use tokio::time::timeout;
use rust_decimal::{Decimal, prelude::*};
use crate::statemanager::OrderComplete;
use crate::db::{create_pool, export_trades_to_csv, export_positions_to_csv};
use crate::utils::report_log;

mod actors;
mod api;
mod config;
mod db;
mod fetch;
mod statemanager;
mod stop;
mod trading;
mod websocket;
mod utils;
mod monitor;
mod indicators;
mod processing;

use api::KrakenClient;
use config::{load_config, Config};
use db::init_db;
use statemanager::{
    MarketDataMessage, OhlcUpdate, Position, PriceData, PriceUpdate, StateManager, Trade,
};
use trading::trading_logic;

// UTILITY FUNCTIONS

pub async fn process_price_updates(
    state_manager: Arc<StateManager>,
    mut rx: mpsc::Receiver<PriceUpdate>,
) {
    while let Some(update) = rx.recv().await {
        let price_data = PriceData {
            close_price: update.close_price,
            bid_price: update.bid,
            ask_price: update.ask,
        };
        if let Err(e) = state_manager
            .update_price(update.pair.clone(), price_data)
            .await
        {
            error!("Failed to update price for {}: {}", update.pair, e);
            record_error("price_update");
        } else {
            info!("Processed price update for {}: {:?}", update.pair, update);
        }
    }
}

pub async fn process_ohlc_updates(
    state_manager: Arc<StateManager>,
    mut rx: mpsc::Receiver<OhlcUpdate>,
) {
    while let Some(update) = rx.recv().await {
        let pair = update.pair.clone();
        if let Some(md_tx) = state_manager.market_data_txs.get(&pair) {
            let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
            if md_tx
                .send(MarketDataMessage::UpdateOhlc {
                    ohlc: update,
                    reply: reply_tx,
                })
                .await
                .is_err()
            {
                error!("Failed to send UpdateOhlc for {}", pair);
            } else {
                if reply_rx.await.is_err() {
                    error!("Failed to receive reply for UpdateOhlc for pair {}", pair);
                }
            }
        } else {
            error!("No market_data_tx for pair {}", pair);
        }
    }
}

pub fn get_current_time() -> DateTime<Local> {
    Local::now()
}

pub fn parse_est_timestamp(timestamp: &str) -> Option<DateTime<Local>> {
    if timestamp.is_empty() {
        return None;
    }
    let formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.fZ",
        "%Y-%m-%dT%H:%M:%S%.f%:z",
    ];
    for format in formats.iter() {
        if let Ok(naive_dt) = NaiveDateTime::parse_from_str(timestamp, format) {
            return Local.from_local_datetime(&naive_dt).single();
        }
        if let Ok(dt) = DateTime::parse_from_str(timestamp, format) {
            return Some(dt.with_timezone(&Local));
        }
    }
    error!("Failed to parse timestamp '{}'", timestamp);
    None
}

pub fn map_ws_pair(api_pair: &str, config: &Config) -> String {
    for (api, ws) in config
        .portfolio
        .api_pairs
        .value
        .iter()
        .zip(config.portfolio.ws_pairs.value.iter())
    {
        if api_pair == api {
            return ws.to_string();
        }
    }
    api_pair.to_string()
}

pub fn init_metrics() {
    info!("Metrics logging initialized with tracing");
}

pub fn record_api_latency(endpoint: &str, duration: f64) {
    info!("API latency for {}: {} seconds", endpoint, duration);
}

pub fn record_trade_count(pair: &str, trade_type: &str, trade: &Trade) {
    info!(
        target: "trade",
        "Trade for {}: type={}, trade_id={}, amount={}, price={}, fees={}, reason={}",
        pair, trade_type, trade.trade_id, trade.amount, trade.execution_price, trade.fees, trade.reason
    );
}

pub fn record_error(error_type: &str) {
    info!(
        "Error occurred: {} at {}",
        error_type,
        chrono::Local::now().to_string()
    );
}

// ============================================================================
// STARTUP FUNCTIONS
// ============================================================================

async fn validate_api_credentials(config: &Config) -> Result<(String, String)> {
    let api_key = config.api_keys.key.value.clone();
    let api_secret = config.api_keys.secret.value.clone();

    let api_key = api_key
        .trim()
        .trim_matches('"')
        .trim_matches('\'')
        .to_string();
    let api_secret = api_secret
        .trim()
        .trim_matches('"')
        .trim_matches('\'')
        .to_string();

    if api_key.is_empty() {
        return Err(anyhow!("API key cannot be empty after cleaning"));
    }
    if api_secret.is_empty() {
        return Err(anyhow!("API secret cannot be empty after cleaning"));
    }

    if STANDARD.decode(&api_secret).is_err() {
        return Err(anyhow!(
            "API secret appears to be invalid base64. Make sure you copied it exactly from Kraken without quotes."
        ));
    }

    info!("Kraken API keys loaded and validated");
    info!(
        "API Key: {}...",
        &api_key[..std::cmp::min(8, api_key.len())]
    );
    info!("API Secret length: {} characters", api_secret.len());

    Ok((api_key, api_secret))
}

async fn initialize_kraken_client(api_key: String, api_secret: String) -> Result<KrakenClient> {
    let kraken_client = KrakenClient::new(api_key, api_secret);
    info!("Kraken REST client initialized");

    info!("Testing Kraken API connection...");
    match kraken_client.fetch_time().await {
        Ok(server_time) => {
            info!("Kraken server time: {}", server_time);
        }
        Err(e) => {
            error!("Failed to connect to Kraken API: {}", e);
            return Err(anyhow!("Kraken API connection test failed: {}", e));
        }
    }

    Ok(kraken_client)
}

async fn validate_kraken_authentication(kraken_client: &KrakenClient) -> Result<Decimal> {
    info!("Validating Kraken API authentication...");
    let usd_balance = match kraken_client.fetch_balance().await {
        Ok(balance) => {
            info!("Successfully authenticated with Kraken API");
            info!("DEBUG: Full balance response: {:#?}", balance);
            let zusd_value = balance.get("ZUSD");
            info!("Raw ZUSD value: {:?}", zusd_value);
            let usd_bal = zusd_value
                .and_then(|v| Decimal::from_str(v.as_str().unwrap_or("0")).ok())
                .unwrap_or(Decimal::ZERO);
            info!("Account USD balance fetched: {:.4}", usd_bal);
            if usd_bal < Decimal::ZERO {
                error!("Invalid USD balance received: {:.4}", usd_bal);
                return Err(anyhow!("Invalid USD balance from Kraken API"));
            }
            info!("Validated USD balance: {:.4}", usd_bal);
            usd_bal
        }
        Err(e) => {
            error!("Failed to authenticate with Kraken API: {}", e);
            error!("Please check your API key and secret, and ensure they have the correct permissions");
            error!("Required permissions: Query Funds, Query Open Orders/Trades, Create & Modify Orders");
            return Err(e.into());
        }
    };

    Ok(usd_balance)
}

async fn initialize_database(config: &Config) -> Result<()> {
    init_db(config)
        .await
        .map_err(|e| anyhow!("Failed to initialize database: {}", e))?;
    info!("Database initialized successfully");
    Ok(())
}

async fn initialize_state_manager(
    config: &Config,
    kraken_client: KrakenClient,
    usd_balance: Decimal,
) -> Result<Arc<StateManager>> {
    let state_manager = StateManager::new(config, kraken_client).await?;
    state_manager
        .initialize_tables(config.portfolio.api_pairs.value.clone())
        .await?;

    for pair in config
        .portfolio
        .api_pairs
        .value
        .iter()
        .chain(std::iter::once(&"USD".to_string()))
    {
        let mut position = Position {
            pair: pair.to_string(),
            last_updated: get_current_time().format("%Y-%m-%d %H:%M:%S").to_string(),
            last_synced: get_current_time().format("%Y-%m-%d %H:%M:%S").to_string(),
            ..Position::new(pair.to_string())
        };

        if pair == "USD" {
            position.usd_balance = usd_balance;
            info!(
                "Creating USD position with balance: {:.4}",
                position.usd_balance
            );
        }

        info!(
            "About to update position for {}: usd_balance={:.4}, total_usd={:.4}",
            pair, position.usd_balance, position.total_usd
        );

        let max_retries = 3;
        let mut retry_count = 0;
        loop {
            match state_manager
                .update_positions(pair.to_string(), position.clone(), pair != "USD")
                .await
            {
                Ok(_) => {
                    info!(
                        "Successfully updated position for {} after {} retries",
                        pair, retry_count
                    );
                    break;
                }
                Err(e) => {
                    retry_count += 1;
                    if retry_count >= max_retries {
                        error!("Failed to update position for {} after {} retries: {}. Position data: {:?}", 
                            pair, max_retries, e, position);
                        return Err(anyhow!("Failed to initialize position for {}: {}", pair, e));
                    }
                    error!(
                        "Failed to update position for {} (attempt {}/{}): {}. Retrying...",
                        pair, retry_count, max_retries, e
                    );
                    tokio::time::sleep(Duration::from_secs(2u64.pow((retry_count - 1) as u32)))
                        .await;
                }
            }
        }
    }

    info!(
        "About to sync USD balance: {:.4}",
        usd_balance
    );
    let max_retries = 3;
    let mut retry_count = 0;
    loop {
        match state_manager.sync_usd_balance(usd_balance).await {
            Ok(_) => {
                info!(
                    "Successfully synced USD balance: {:.4} after {} retries",
                    usd_balance, retry_count
                );
                break;
            }
            Err(e) => {
                retry_count += 1;
                if retry_count >= max_retries {
                    error!(
                        "Failed to sync USD balance after {} retries: {}",
                        max_retries, e
                    );
                    return Err(anyhow!("Failed to sync USD balance: {}", e));
                }
                error!(
                    "Failed to sync USD balance (attempt {}/{}): {}. Retrying...",
                    retry_count, max_retries, e
                );
                tokio::time::sleep(Duration::from_secs(2u64.pow((retry_count - 1) as u32))).await;
            }
        }
    }

    info!(
        "StateManager initialized with {} positions, USD balance synced to database: {:.4}",
        config.portfolio.api_pairs.value.len() + 1,
        usd_balance
    );

    Ok(state_manager)
}

async fn setup_websocket_channels(
    config: &Config,
) -> Result<(
    mpsc::Sender<PriceUpdate>,
    mpsc::Receiver<PriceUpdate>,
    mpsc::Sender<OhlcUpdate>,
    mpsc::Receiver<OhlcUpdate>,
)> {
    let (price_tx, price_rx) = mpsc::channel::<PriceUpdate>(1000);
    let (ohlc_tx, ohlc_rx) = mpsc::channel::<OhlcUpdate>(1000);

    info!(
        "WebSocket channels created for {} pairs",
        config.portfolio.api_pairs.value.len()
    );

    Ok((price_tx, price_rx, ohlc_tx, ohlc_rx))
}

// MAIN FUNCTION

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let log_dir = "log";
    fs::create_dir_all(log_dir)?;
    let timer = ChronoLocal::new("%Y-%m-%d %H:%M:%S %Z".to_string());
    let cmd_log = tracing_subscriber::fmt::layer()
        .with_writer(|| -> Box<dyn std::io::Write> {
            Box::new(
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("log/cmd_log.txt")
                    .expect("Failed to open cmd_log.txt"),
            )
        })
        .with_file(true)
        .with_timer(timer.clone())
        .with_filter(tracing_subscriber::filter::LevelFilter::from_level(
            Level::INFO,
        ));
    let error_log = tracing_subscriber::fmt::layer()
        .with_writer(|| -> Box<dyn std::io::Write> {
            Box::new(
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("log/error_log.txt")
                    .expect("Failed to open error_log.txt"),
            )
        })
        .with_file(true)
        .with_timer(timer.clone())
        .with_filter(tracing_subscriber::filter::LevelFilter::from_level(
            Level::ERROR,
        ));
    let trade_log = tracing_subscriber::fmt::layer()
        .with_writer(|| -> Box<dyn std::io::Write> {
            Box::new(
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("log/trades_log.txt")
                    .expect("Failed to open trades_log.txt"),
            )
        })
        .with_file(true)
        .with_timer(timer)
        .with_filter(filter_fn(|metadata| {
            metadata.fields().field("trade").is_some()
        }));
    tracing_subscriber::registry()
        .with(cmd_log)
        .with(error_log)
        .with(trade_log)
        .with(tracing_subscriber::fmt::layer().with_filter(
            tracing_subscriber::filter::LevelFilter::from_level(Level::INFO),
        ))
        .init();
    init_metrics();
    info!("Starting trading bot");

    let config = load_config()?;
    let pool = create_pool(&config).await?;
    config.validate()?;
    info!(
        "Loaded configuration: {:?}",
        config.portfolio.api_pairs.value
    );

    let (api_key, api_secret) = validate_api_credentials(&config).await?;
    let mut kraken_client = initialize_kraken_client(api_key, api_secret).await?;
    if let Err(e) = kraken_client.init_ws_token().await {
        error!("Failed to init WS token: {}", e);
    }
    let usd_balance = validate_kraken_authentication(&kraken_client).await?;
    fs::create_dir_all("report")?;
    let start_time = Local::now();
    let report_filename = format!("trade_report_{}.txt", start_time.format("%Y-%m-%d_%H-%M-%S"));
    let report_path = format!("report/{}", report_filename);
    let report_path_arc = Arc::new(report_path.clone());

    // Startup header + snapshot
    report_log(&report_path, &format!("BOT STARTED at {}", start_time.format("%Y-%m-%d %H:%M:%S")))?;
    report_log(&report_path, "Initial Kraken balances:")?;
    if let Ok(balances) = kraken_client.fetch_balance().await {
        for (asset, qty) in balances.iter() {
            report_log(&report_path, &format!("  {}: {}", asset, qty))?;
        }
    }
    report_log(&report_path, "Initial open orders:")?;
    if let Ok(orders) = kraken_client.fetch_open_orders().await {
        if orders.is_empty() {
            report_log(&report_path, "  None")?;
        } else {
            for (txid, order) in orders.iter() {
                report_log(&report_path, &format!("  Order {}: {} {} @ {} (status: {})", 
                    txid, order.vol, order.pair, order.price, order.status))?;
            }
        }
    }
    initialize_database(&config).await?;
    let state_manager =
        initialize_state_manager(&config, kraken_client.clone(), usd_balance).await?;

    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    let (price_tx, price_rx, ohlc_tx, ohlc_rx) = setup_websocket_channels(&config).await?;

    let (pub_ready_tx, mut pub_ready_rx) = mpsc::channel::<()>(1);
    let max_retries = 5;
    let ws_handle = tokio::spawn({
        let config = config.clone();
        let state_manager = state_manager.clone();
        let kraken_client = kraken_client.clone();
        let pub_ready_tx = pub_ready_tx;
        async move {
            let mut retry_count = 0;
            loop {
                match websocket::watch_kraken_prices(
                    config.portfolio.api_pairs.value.clone(),
                    price_tx.clone(),
                    ohlc_tx.clone(),
                    state_manager.clone(),
                    kraken_client.clone(),
                    pub_ready_tx.clone(),
                )
                .await
                {
                    Ok(_) => break,
                    Err(e) => {
                        error!(
                            "WebSocket task failed: {}. Retrying {}/{} in 5 seconds...",
                            e,
                            retry_count + 1,
                            max_retries
                        );
                        record_error("websocket_task");
                        if retry_count >= max_retries {
                            error!("WebSocket task failed after {} retries", max_retries);
                            break;
                        }
                        retry_count += 1;
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        }
    });

    let (priv_ready_tx, mut priv_ready_rx) = mpsc::channel::<()>(1);
    let private_ws_handle = tokio::spawn({
        let state_manager = state_manager.clone();
        let kraken_client = kraken_client.clone();
        let priv_ready_tx = priv_ready_tx;
        async move {
            let mut retry_count = 0;
            let max_retries = 5;
            loop {
                match websocket::watch_kraken_private(
                    state_manager.clone(),
                    kraken_client.clone(),
                    priv_ready_tx.clone(),
                )
                .await
                {
                    Ok(_) => break,
                    Err(e) => {
                        error!(
                            "Private WebSocket task failed: {}. Retrying {}/{} in 5 seconds...",
                            e,
                            retry_count + 1,
                            max_retries
                        );
                        record_error("private_websocket_task");
                        if retry_count >= max_retries {
                            error!("Private WebSocket task failed after {} retries", max_retries);
                            break;
                        }
                        retry_count += 1;
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        }
    });

    // Await WS ready signals with timeout
    if let Err(_) = timeout(Duration::from_secs(30), pub_ready_rx.recv()).await {
        warn!("Public WS ready timeout after 30s; proceeding with fallback");
    } else {
        info!("Public WS subscriptions confirmed");
    }

    if let Err(_) = timeout(Duration::from_secs(30), priv_ready_rx.recv()).await {
        warn!("Private WS ready timeout after 30s; proceeding with fallback");
    } else {
        info!("Private WS subscriptions confirmed");
    }

    let state_manager_for_price = Arc::clone(&state_manager);
    let price_handle = tokio::spawn(async move {
        process_price_updates(state_manager_for_price, price_rx).await;
        error!("Price processing task completed");
    });
    let state_manager_for_ohlc = Arc::clone(&state_manager);
    let ohlc_handle = tokio::spawn(async move {
        process_ohlc_updates(state_manager_for_ohlc, ohlc_rx).await;
        error!("OHLC processing task completed");
    });

    let sweep_handle = {
        let state_manager = state_manager.clone();
        let config = config.clone();
        let kraken_client = kraken_client.clone();
        let shutdown_tx = shutdown_tx.clone();
        tokio::spawn(async move {
            if let Err(e) = trading_logic::global_trade_sweep(state_manager, &kraken_client, &config, shutdown_tx).await {
                error!("Global trade sweep failed: {}", e);
            }
        })
    };

    let mut trading_handles = vec![];

    for pair in config.portfolio.api_pairs.value.clone() {
        let config = config.clone();
        let kraken_client = kraken_client.clone();
        let state_manager = state_manager.clone();
        let pair_clone = pair.clone();
        let shutdown_tx = shutdown_tx.clone();
        let mut completion_rx = state_manager.get_completion_rx(&pair_clone).await.unwrap();

        let handle = tokio::spawn(async move {
            let loop_delay = Duration::from_secs_f64(config.trading_logic.loop_delay_seconds.value.to_f64().unwrap_or(30.0));
            let mut shutdown_rx = shutdown_tx.subscribe();

            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Shutdown signal received for pair {}", pair_clone);
                        break;
                    }
                    _ = tokio::time::sleep(loop_delay) => {
                        let open_orders = match state_manager.get_open_orders(pair_clone.clone()).await {
                            Ok(orders) => orders,
                            Err(e) => {
                                error!("Failed to get open orders for {}: {}", pair_clone, e);
                                continue;
                            }
                        };
                        if !open_orders.is_empty() {
                            continue;
                        }
                        if let Err(e) = trading_logic::fetch_and_store_market_data(&pair_clone, &kraken_client, &config, state_manager.clone()).await {
                            error!("Market data fetch failed for {}: {}", pair_clone, e);
                        }
                        let buy_triggered = match trading_logic::buy_logic(&pair_clone, state_manager.clone(), &kraken_client, &config, &mut completion_rx, shutdown_tx.clone()).await {
                            Ok((triggered, _)) => triggered,
                            Err(e) => {
                                error!("Buy logic failed for {}: {}", pair_clone, e);
                                false
                            }
                        };
                        if buy_triggered {
                            if let Some(sig) = completion_rx.recv().await {
                                match sig {
                                    OrderComplete::Success(_) => info!("Buy completed for {}", pair_clone),
                                    OrderComplete::Error(e) => error!("Buy error for {}: {}", pair_clone, e),
                                    OrderComplete::Shutdown => break,
                                    _ => {}
                                }
                            } else {
                                error!("Completion channel closed for {}", pair_clone);
                                break;
                            }
                        }
                        let sell_triggered = match trading_logic::sell_logic(&pair_clone, state_manager.clone(), &kraken_client, &config, &mut completion_rx, shutdown_tx.clone()).await {
                            Ok(triggered) => triggered,
                            Err(e) => {
                                error!("Sell logic failed for {}: {}", pair_clone, e);
                                false
                            }
                        };
                        if sell_triggered {
                            if let Some(sig) = completion_rx.recv().await {
                                match sig {
                                    OrderComplete::Success(_) => info!("Sell completed for {}", pair_clone),
                                    OrderComplete::Error(e) => error!("Sell error for {}: {}", pair_clone, e),
                                    OrderComplete::Shutdown => break,
                                    _ => {}
                                }
                            } else {
                                error!("Completion channel closed for {}", pair_clone);
                                break;
                            }
                        }
                    }
                }
            }
        });
        trading_handles.push(handle);
    }

    info!(
        "Entering main loop with concurrent trading for {} pairs",
        config.portfolio.api_pairs.value.len()
    );

    let pool_for_task = pool.clone();
    let report_path_periodic = report_path_arc.clone();
    let kraken_client_periodic = kraken_client.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600));
        loop {
            interval.tick().await;
            let _ = export_trades_to_csv(&pool_for_task).await;
            let _ = export_positions_to_csv(&pool_for_task).await;

            // Periodic report summary
            let _ = report_log(&report_path_periodic, "HOURLY SUMMARY");
            if let Ok(balances) = kraken_client_periodic.fetch_balance().await {
                let _ = report_log(&report_path_periodic, "Current balances:");
                for (asset, qty) in balances {
                    let _ = report_log(&report_path_periodic, &format!("  {}: {}", asset, qty));
                }
            }
        }
    });

    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("Received SIGINT, initiating shutdown...");
        },
        _ = ws_handle => {
            error!("WebSocket task terminated unexpectedly");
        },
        _ = private_ws_handle => {
            error!("Private WebSocket task terminated unexpectedly");
        },
        _ = price_handle => {
            error!("Price processing task terminated unexpectedly");
        },
        _ = ohlc_handle => {
            error!("OHLC processing task completed unexpectedly");
        },
        _ = futures_util::future::join_all(trading_handles) => {
            error!("Trading loops terminated unexpectedly");
        }
    }
    info!("Initiating graceful shutdown of StateManager...");
    state_manager.shutdown().await?;
    let end_time = Local::now();
    report_log(&report_path, &format!("BOT SHUTDOWN at {}", end_time.format("%Y-%m-%d %H:%M:%S")))?;
    report_log(&report_path, "Final Kraken balances:")?;
    if let Ok(balances) = kraken_client.fetch_balance().await {
        for (asset, qty) in balances.iter() {
            report_log(&report_path, &format!("  {}: {}", asset, qty))?;
        }
    }
    report_log(&report_path, "Final open orders:")?;
    if let Ok(orders) = kraken_client.fetch_open_orders().await {
        if orders.is_empty() {
            report_log(&report_path, "  None")?;
        } else {
            for (txid, order) in orders.iter() {
                report_log(&report_path, &format!("  Order {}: {} {} @ {} (status: {})", 
                    txid, order.vol, order.pair, order.price, order.status))?;
            }
        }
    }

    let client = pool.get().await?;
    if let Ok(db_pl) = db::get_total_pl_for_crypto(&client).await {
        report_log(&report_path, &format!("DB realized total_pl: {}", db_pl))?;
    }
    report_log(&report_path, "Note: Compare DB total_pl to manual Kraken ledger check for realized PL diff");
    let _ = export_trades_to_csv(&pool).await;
    let _ = export_positions_to_csv(&pool).await;
    let _ = shutdown_tx.send(());
    info!("Shutdown complete");
    Ok(())
}