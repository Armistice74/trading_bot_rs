// db.rs
// description:

// ============================================================================
// IMPORTS
// ============================================================================

use crate::config::Config;
use crate::statemanager::{MarketData, MarketDataHistory, Position, Trade};
use crate::utils::{get_current_time, get_eastern_tz};
use anyhow::{anyhow, Context, Result};
use deadpool::managed::PoolConfig;
use deadpool_postgres::{Pool, Runtime};
use std::fs::OpenOptions;
use std::io::Write;
use tokio_postgres::NoTls;
use tracing::{error, info};
use crate::utils::parse_unix_timestamp;
use rust_decimal::Decimal;
use csv::Writer;
use serde::Serialize;
use std::fs;
use deadpool_postgres::Pool;

// ============================================================================
// Database
// ============================================================================

pub async fn create_pool(config: &Config) -> Result<Pool> {
    let db_config = &config.database;
    let mut cfg = deadpool_postgres::Config::new();
    cfg.host = Some(db_config.host.value.clone());
    cfg.port = Some(db_config.port.value as u16);
    cfg.dbname = Some(db_config.database.value.clone());
    cfg.user = Some(db_config.user.value.clone());
    cfg.password = Some(db_config.password.value.clone());
    cfg.pool = Some(PoolConfig {
        max_size: db_config.max_connections.value as usize,
        ..Default::default()
    });

    let max_retries = 5;
    for attempt in 0..max_retries {
        match cfg.create_pool(Some(Runtime::Tokio1), NoTls) {
            Ok(pool) => return Ok(pool),
            Err(e) => {
                error!(
                    "Failed to create connection pool on attempt {}/{}: {}",
                    attempt + 1,
                    max_retries,
                    e
                );
                if attempt < max_retries - 1 {
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt as u32)))
                        .await;
                } else {
                    return Err(anyhow!(
                        "Failed to create connection pool after {} retries: {}",
                        max_retries, e
                    ));
                }
            }
        }
    }
    Err(anyhow!("Unreachable: max retries exceeded"))
}

pub async fn init_db(config: &Config) -> Result<()> {
    let pool = create_pool(config).await?;
    let mut client = pool
        .get()
        .await
        .context("Failed to get client from pool")?;

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
    transaction
        .execute(
            "CREATE INDEX IF NOT EXISTS idx_trades_pair_timestamp ON trades (pair, timestamp)",
            &[],
        )
        .await?;

    for pair in &config.portfolio.api_pairs.value {
        let table_name = format!("{}_market_data", pair.to_lowercase());
        transaction
            .execute(
                &format!(
                    "CREATE TABLE IF NOT EXISTS {} (
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
                    table_name
                ),
                &[],
            )
            .await?;
        transaction
            .execute(
                &format!(
                    "CREATE INDEX IF NOT EXISTS idx_{}_timestamp ON {} (timestamp)",
                    table_name, table_name
                ),
                &[],
            )
            .await?;
        info!("Created table and index {} for pair {}", table_name, pair);
    }

    transaction.commit().await?;
    info!("Database tables and indexes initialized successfully");
    Ok(())
}

pub async fn update_position(
    client: &mut deadpool_postgres::Client,
    pair: &str,
    position: &Position,
    is_buy: bool,
) -> Result<()> {
    let max_retries = 3;
    for attempt in 0..max_retries {
        let transaction = client
            .transaction()
            .await
            .context("Transaction error")?;

        // Use consistent INSERT ... ON CONFLICT logic for all pairs including USD
        let query = "INSERT INTO positions (pair, total_usd, total_quantity, usd_balance, last_updated, total_fees, total_pl, highest_price_since_buy, last_synced) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (pair) DO UPDATE SET total_usd = EXCLUDED.total_usd, total_quantity = EXCLUDED.total_quantity, usd_balance = EXCLUDED.usd_balance, last_updated = EXCLUDED.last_updated, total_fees = EXCLUDED.total_fees, total_pl = EXCLUDED.total_pl, highest_price_since_buy = EXCLUDED.highest_price_since_buy, last_synced = EXCLUDED.last_synced";

        // Log query parameters for debugging
        info!("DB: Updating position for {} - total_usd={}, total_quantity={}, usd_balance={}, is_buy={}", 
              pair, position.total_usd, position.total_quantity, position.usd_balance, is_buy);

        let result = transaction
            .execute(
                query,
                &[
                    &pair,
                    &position.total_usd,
                    &position.total_quantity,
                    &position.usd_balance,
                    &position.last_updated,
                    &position.total_fees,
                    &position.total_pl,
                    &position.highest_price_since_buy,
                    &position.last_synced,
                ],
            )
            .await;

        match result {
            Ok(rows_affected) => {
                transaction.commit().await?;
                info!("DB: Successfully updated/inserted position for {}, rows_affected: {}, final_usd_balance: {}", 
                      pair, rows_affected, position.usd_balance);
                return Ok(());
            }
            Err(e) => {
                error!(
                    "DB: Failed to update position for {} on attempt {}/{}: {}",
                    pair,
                    attempt + 1,
                    max_retries,
                    e
                );
                let _ = transaction.rollback().await;
                if attempt < max_retries - 1 {
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt as u32)))
                        .await;
                } else {
                    return Err(anyhow!(
                        "Failed to update position for {} after {} retries: {}",
                        pair, max_retries, e
                    ));
                }
            }
        }
    }
    Err(anyhow!("Unreachable: max retries exceeded"))
}

pub async fn update_trade(
    client: &mut deadpool_postgres::Client,
    trade_data: &Trade,
) -> Result<()> {
    let max_retries = 3;
    for attempt in 0..max_retries {
        let transaction = client
            .transaction()
            .await
            .context("Transaction error")?;

        // First, check existing pair if trade_id exists
        let existing_row = transaction
            .query_opt(
                "SELECT pair FROM trades WHERE trade_id = $1",
                &[&trade_data.trade_id],
            )
            .await?;
        let mut use_existing_pair = false;
        let mut existing_pair = trade_data.pair.clone();
        if let Some(row) = existing_row {
            existing_pair = row.get("pair");
            if existing_pair != trade_data.pair {
                let mut file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("log/error_log.txt")
                    .context("Failed to open error log")?;
                writeln!(
                    file,
                    "[{}] Pair mismatch for trade_id {}: existing={}, new={}—skipping pair update",
                    get_current_time().to_string(),
                    trade_data.trade_id, existing_pair, trade_data.pair
                )
                .context("Failed to write to error log")?;
                info!("Pair mismatch for {}: existing={}, new={}—skipping", trade_data.trade_id, existing_pair, trade_data.pair);
                use_existing_pair = true;
            }
        }

        let query = if use_existing_pair {
            // Exclude pair from update
            "INSERT INTO trades (timestamp, pair, trade_id, order_id, type, amount, execution_price, fees, fee_percentage, profit, profit_percentage, reason, avg_cost_basis, slippage, remaining_amount, open, partial_open) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) ON CONFLICT (trade_id) DO UPDATE SET remaining_amount = EXCLUDED.remaining_amount, open = EXCLUDED.open, partial_open = EXCLUDED.partial_open, timestamp = EXCLUDED.timestamp, order_id = EXCLUDED.order_id, type = EXCLUDED.type, amount = EXCLUDED.amount, execution_price = EXCLUDED.execution_price, fees = EXCLUDED.fees, fee_percentage = EXCLUDED.fee_percentage, profit = EXCLUDED.profit, profit_percentage = EXCLUDED.profit_percentage, reason = EXCLUDED.reason, avg_cost_basis = EXCLUDED.avg_cost_basis, slippage = EXCLUDED.slippage"
        } else {
            "INSERT INTO trades (timestamp, pair, trade_id, order_id, type, amount, execution_price, fees, fee_percentage, profit, profit_percentage, reason, avg_cost_basis, slippage, remaining_amount, open, partial_open) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) ON CONFLICT (trade_id) DO UPDATE SET remaining_amount = EXCLUDED.remaining_amount, open = EXCLUDED.open, partial_open = EXCLUDED.partial_open, timestamp = EXCLUDED.timestamp, order_id = EXCLUDED.order_id, type = EXCLUDED.type, amount = EXCLUDED.amount, execution_price = EXCLUDED.execution_price, fees = EXCLUDED.fees, fee_percentage = EXCLUDED.fee_percentage, profit = EXCLUDED.profit, profit_percentage = EXCLUDED.profit_percentage, reason = EXCLUDED.reason, avg_cost_basis = EXCLUDED.avg_cost_basis, slippage = EXCLUDED.slippage, pair = EXCLUDED.pair"
        };

        let result = transaction
            .execute(
                query,
                &[
                    &trade_data.timestamp,
                    &existing_pair,  // Use existing if mismatch
                    &trade_data.trade_id,
                    &trade_data.order_id,
                    &trade_data.trade_type,
                    &trade_data.amount,
                    &trade_data.execution_price,
                    &trade_data.fees,
                    &trade_data.fee_percentage,
                    &trade_data.profit,
                    &trade_data.profit_percentage,
                    &trade_data.reason,
                    &trade_data.avg_cost_basis,
                    &trade_data.slippage,
                    &trade_data.remaining_amount,
                    &trade_data.open,
                    &trade_data.partial_open,
                ],
            )
            .await;

        match result {
            Ok(_) => {
                transaction.commit().await?;
                return Ok(());
            }
            Err(e) => {
                let _ = transaction.rollback().await;
                if e.to_string().contains("duplicate key") {
                    info!(
                        "DB: Skipped inserting trade_id {} for pair {}: already exists",
                        trade_data.trade_id, trade_data.pair
                    );
                    return Ok(());
                }
                error!(
                    "DB: Failed to update trade_id {} for pair {} on attempt {}/{}: {}",
                    trade_data.trade_id,
                    trade_data.pair,
                    attempt + 1,
                    max_retries,
                    e
                );
                if attempt < max_retries - 1 {
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt as u32)))
                        .await;
                } else {
                    return Err(anyhow!(
                        "Failed to update trade_id {} for pair {} after {} retries: {}",
                        trade_data.trade_id, trade_data.pair, max_retries, e
                    ));
                }
            }
        }
    }
    Err(anyhow!("Unreachable: max retries exceeded"))
}

pub async fn get_trades_for_order(
    client: &deadpool_postgres::Client,
    order_id: &str,
    pair: &str,
    startup_time: u64,
) -> Result<Vec<Trade>> {
    let max_retries = 3;
    for attempt in 0..max_retries {
        // Convert startup_time (u64 ms) to string timestamp for TEXT comparison
        let startup_dt = parse_unix_timestamp(&startup_time.to_string())
            .ok_or_else(|| anyhow!("Invalid startup_time: {}", startup_time))?;
        let startup_str = startup_dt
            .with_timezone(&get_eastern_tz())
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();

        match client
            .query(
                "SELECT * FROM trades WHERE order_id = $1 AND pair = $2 AND timestamp >= $3 ORDER BY timestamp ASC",
                &[&order_id, &pair, &startup_str],
            )
            .await
        {
            Ok(rows) => {
                let trades = rows
                    .into_iter()
                    .map(|row| Trade {
                        timestamp: row.get("timestamp"),
                        pair: row.get("pair"),
                        trade_id: row.get("trade_id"),
                        order_id: row.get("order_id"),
                        trade_type: row.get("type"),
                        amount: row.get("amount"),
                        execution_price: row.get("execution_price"),
                        fees: row.get("fees"),
                        fee_percentage: row.get("fee_percentage"),
                        profit: row.get("profit"),
                        profit_percentage: row.get("profit_percentage"),
                        reason: row.get("reason"),
                        avg_cost_basis: row.get("avg_cost_basis"),
                        slippage: row.get("slippage"),
                        remaining_amount: row.get("remaining_amount"),
                        open: row.get("open"),
                        partial_open: row.get("partial_open"),
                    })
                    .collect::<Vec<_>>();
                info!("DB: Fetched {} trades for order_id {} on pair {} (startup filter: >= {})", trades.len(), order_id, pair, startup_str);
                return Ok(trades);
            }
            Err(e) => {
                error!(
                    "DB: Failed to fetch trades for order_id {} on pair {} on attempt {}/{}: {}",
                    order_id, pair, attempt + 1, max_retries, e
                );
                if attempt < max_retries - 1 {
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt as u32)))
                        .await;
                } else {
                    return Err(anyhow!(
                        "Failed to fetch trades for order_id {} on pair {} after {} retries: {}",
                        order_id, pair, max_retries, e
                    ));
                }
            }
        }
    }
    Err(anyhow!("Unreachable: max retries exceeded"))
}

pub async fn store_market_data(
    client: &mut deadpool_postgres::Client,
    market_data: &MarketData,
) -> Result<()> {
    let max_retries = 3;
    for attempt in 0..max_retries {
        let transaction = client
            .transaction()
            .await
            .context("Transaction error")?;
        let result = transaction.execute(
            &format!(
                "INSERT INTO {}_market_data (timestamp, open_price, high_price, low_price, highest_price_period, volume, bid_price, ask_price, bid_depth, ask_depth, liquidity, close_price) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) ON CONFLICT DO NOTHING",
                market_data.pair.to_lowercase()
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
            Ok(_rows_affected) => {
                transaction.commit().await?;
                return Ok(());
            }
            Err(e) => {
                error!(
                    "DB: Failed to store market data for {} on attempt {}/{}: {}",
                    market_data.pair,
                    attempt + 1,
                    max_retries,
                    e
                );
                let _ = transaction.rollback().await;
                if attempt < max_retries - 1 {
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt as u32)))
                        .await;
                } else {
                    return Err(anyhow!(
                        "Failed to store market data for {} after {} retries: {}",
                        market_data.pair, max_retries, e
                    ));
                }
            }
        }
    }
    Err(anyhow!("Unreachable: max retries exceeded"))
}

pub async fn get_all_positions(
    client: &deadpool_postgres::Client,
) -> Result<Vec<Position>> {
    let max_retries = 3;
    for attempt in 0..max_retries {
        match client.query("SELECT * FROM positions", &[]).await {
            Ok(rows) => {
                let positions = rows
                    .into_iter()
                    .map(|row| Position {
                        pair: row.get("pair"),
                        total_usd: row.get("total_usd"),
                        total_quantity: row.get("total_quantity"),
                        usd_balance: row.get("usd_balance"),
                        last_updated: row.get("last_updated"),
                        total_fees: row.get("total_fees"),
                        total_pl: row.get("total_pl"),
                        highest_price_since_buy: row.get("highest_price_since_buy"),
                        last_synced: row.get("last_synced"),
                    })
                    .collect::<Vec<_>>();
                info!("DB: Fetched {} positions", positions.len());
                return Ok(positions);
            }
            Err(e) => {
                error!(
                    "DB: Failed to fetch all positions on attempt {}/{}: {}",
                    attempt + 1,
                    max_retries,
                    e
                );
                if attempt < max_retries - 1 {
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt as u32)))
                        .await;
                } else {
                    return Err(anyhow!(
                        "Failed to fetch all positions after {} retries: {}",
                        max_retries, e
                    ));
                }
            }
        }
    }
    Err(anyhow!("Unreachable: max retries exceeded"))
}

pub async fn get_open_trades(
    client: &deadpool_postgres::Client,
    pair: &str,
) -> Result<Vec<Trade>> {
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
                    execution_price: row.get("execution_price"),
                    fees: row.get("fees"),
                    fee_percentage: row.get("fee_percentage"),
                    profit: row.get("profit"),
                    profit_percentage: row.get("profit_percentage"),
                    reason: row.get("reason"),
                    avg_cost_basis: row.get("avg_cost_basis"),
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
                    return Err(anyhow!("Failed to fetch open trades for {} after {} retries: {}", pair, max_retries, e));
                }
            }
        }
    }
    Err(anyhow!("Unreachable: max retries exceeded"))
}

pub async fn get_market_data_history(
    client: &deadpool_postgres::Client,
    pair: &str,
    since: &str,
    limit: i64,
) -> Result<Vec<MarketDataHistory>> {
    let max_retries = 3;
    for attempt in 0..max_retries {
        match client.query(
            &format!(
                "SELECT high_price, low_price, close_price FROM {}_market_data WHERE timestamp >= $1 ORDER BY timestamp DESC LIMIT $2", 
                pair.to_lowercase()
            ),
            &[&since, &limit],
        ).await {
            Ok(rows) => {
                let history = rows.into_iter().map(|row| MarketDataHistory {
                    high_price: row.get("high_price"),
                    low_price: row.get("low_price"),
                    close_price: row.get("close_price"),
                }).collect::<Vec<_>>();
                return Ok(history);
            }
            Err(e) => {
                error!("DB: Failed to fetch market data history for {} on attempt {}/{}: {}", pair, attempt + 1, max_retries, e);
                if attempt < max_retries - 1 {
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt as u32))).await;
                } else {
                    return Err(anyhow!("Failed to fetch market data history for {} after {} retries: {}", pair, max_retries, e));
                }
            }
        }
    }
    Err(anyhow!("Unreachable: max retries exceeded"))
}

pub async fn get_trade_fees(
    client: &deadpool_postgres::Client,
    pair: &str,
    trade_id: &str,
) -> Result<Option<Decimal>> {
    let max_retries = 3;
    for attempt in 0..max_retries {
        match client
            .query_opt(
                "SELECT fees FROM trades WHERE pair = $1 AND trade_id = $2",
                &[&pair, &trade_id],
            )
            .await
        {
            Ok(row) => {
                info!("DB: Fetched fees for {} trade_id {}", pair, trade_id);
                return Ok(row.map(|row| row.get("fees")));
            }
            Err(e) => {
                error!(
                    "DB: Failed to fetch trade fees for {} trade_id {} on attempt {}/{}: {}",
                    pair,
                    trade_id,
                    attempt + 1,
                    max_retries,
                    e
                );
                if attempt < max_retries - 1 {
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt as u32)))
                        .await;
                } else {
                    return Err(anyhow!(
                        "Failed to fetch trade fees for {} trade_id {} after {} retries: {}",
                        pair, trade_id, max_retries, e
                    ));
                }
            }
        }
    }
    Err(anyhow!("Unreachable: max retries exceeded"))
}

pub async fn get_latest_stop_loss_timestamp(
    client: &deadpool_postgres::Client,
    pair: &str,
) -> Result<Option<String>> {
    let max_retries = 3;
    for attempt in 0..max_retries {
        match client
            .query_opt(
                "SELECT timestamp FROM trades WHERE pair = $1 AND type = 'sell' AND reason IN ('fixed_stop', 'trailing_stop') ORDER BY timestamp DESC LIMIT 1",
                &[&pair],
            )
            .await
        {
            Ok(row) => {
                info!("DB: Fetched latest stop-loss timestamp for {}", pair);
                return Ok(row.map(|row| row.get("timestamp")));
            }
            Err(e) => {
                error!(
                    "DB: Failed to fetch latest stop-loss timestamp for {} on attempt {}/{}: {}",
                    pair,
                    attempt + 1,
                    max_retries,
                    e
                );
                if attempt < max_retries - 1 {
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt as u32)))
                        .await;
                } else {
                    return Err(anyhow!(
                        "Failed to fetch latest stop-loss timestamp for {} after {} retries: {}",
                        pair, max_retries, e
                    ));
                }
            }
        }
    }
    Err(anyhow!("Unreachable: max retries exceeded"))
}

pub async fn check_trade_exists(
    client: &deadpool_postgres::Client,
    pair: &str,
    trade_id: &str,
) -> Result<bool> {
    let max_retries = 3;
    for attempt in 0..max_retries {
        match client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM trades WHERE pair = $1 AND trade_id = $2)",
                &[&pair, &trade_id],
            )
            .await
        {
            Ok(row) => {
                let exists: bool = row.get(0);
                info!(
                    "DB: Checked trade_id {} for pair {}, exists: {}",
                    trade_id, pair, exists
                );
                return Ok(exists);
            }
            Err(e) => {
                error!(
                    "DB: Failed to check trade_id {} for pair {} on attempt {}/{}: {}",
                    trade_id,
                    pair,
                    attempt + 1,
                    max_retries,
                    e
                );
                if attempt < max_retries - 1 {
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt as u32)))
                        .await;
                } else {
                    return Err(anyhow!(
                        "Failed to check trade_id {} for pair {} after {} retries: {}",
                        trade_id, pair, max_retries, e
                    ));
                }
            }
        }
    }
    Err(anyhow!("Unreachable: max retries exceeded"))
}

pub async fn insert_trade(
    client: &mut deadpool_postgres::Client,
    trade_data: &Trade,
) -> Result<()> {
    let max_retries = 3;
    for attempt in 0..max_retries {
        let transaction = client
            .transaction()
            .await
            .context("Transaction error")?;

        let query = "INSERT INTO trades (timestamp, pair, trade_id, order_id, type, amount, execution_price, fees, fee_percentage, profit, profit_percentage, reason, avg_cost_basis, slippage, remaining_amount, open, partial_open) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)";

        let result = transaction
            .execute(
                query,
                &[
                    &trade_data.timestamp,
                    &trade_data.pair,
                    &trade_data.trade_id,
                    &trade_data.order_id,
                    &trade_data.trade_type,
                    &trade_data.amount,
                    &trade_data.execution_price,
                    &trade_data.fees,
                    &trade_data.fee_percentage,
                    &trade_data.profit,
                    &trade_data.profit_percentage,
                    &trade_data.reason,
                    &trade_data.avg_cost_basis,
                    &trade_data.slippage,
                    &trade_data.remaining_amount,
                    &trade_data.open,
                    &trade_data.partial_open,
                ],
            )
            .await;

        match result {
            Ok(_) => {
                transaction.commit().await?;
                info!(
                    "DB: Successfully inserted trade_id {} for pair {}",
                    trade_data.trade_id, trade_data.pair
                );
                return Ok(());
            }
            Err(e) => {
                let _ = transaction.rollback().await;
                if e.to_string().contains("duplicate key") {
                    info!(
                        "DB: Skipped inserting trade_id {} for pair {}: already exists",
                        trade_data.trade_id, trade_data.pair
                    );
                    return Ok(());
                }
                error!(
                    "DB: Failed to insert trade_id {} for pair {} on attempt {}/{}: {}",
                    trade_data.trade_id,
                    trade_data.pair,
                    attempt + 1,
                    max_retries,
                    e
                );
                if attempt < max_retries - 1 {
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt as u32)))
                        .await;
                } else {
                    return Err(anyhow!(
                        "Failed to insert trade_id {} for pair {} after {} retries: {}",
                        trade_data.trade_id, trade_data.pair, max_retries, e
                    ));
                }
            }
        }
    }
    Err(anyhow!("Unreachable: max retries exceeded"))
}

pub async fn get_total_pl_for_pair(
    client: &deadpool_postgres::Client,
    pair: &str,
) -> Result<Decimal> {
    let max_retries = 3;
    for attempt in 0..max_retries {
        match client
            .query_one(
                "SELECT COALESCE(SUM(profit), 0::NUMERIC) FROM trades WHERE pair = $1",
                &[&pair],
            )
            .await
        {
            Ok(row) => {
                let pl: Decimal = row.get(0);
                info!("DB: Fetched total_pl {} for pair {}", pl, pair);
                return Ok(pl);
            }
            Err(e) => {
                error!(
                    "DB: Failed to fetch total_pl for {} on attempt {}/{}: {}",
                    pair,
                    attempt + 1,
                    max_retries,
                    e
                );
                if attempt < max_retries - 1 {
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt as u32)))
                        .await;
                } else {
                    return Err(anyhow!(
                        "Failed to fetch total_pl for {} after {} retries: {}",
                        pair, max_retries, e
                    ));
                }
            }
        }
    }
    Err(anyhow!("Unreachable: max retries exceeded"))
}

pub async fn get_total_pl_for_crypto(
    client: &deadpool_postgres::Client,
) -> Result<Decimal> {
    let max_retries = 3;
    for attempt in 0..max_retries {
        match client
            .query_one(
                "SELECT COALESCE(SUM(total_pl), 0::NUMERIC) FROM positions WHERE pair != 'USD'",
                &[],
            )
            .await
        {
            Ok(row) => {
                let pl: Decimal = row.get(0);
                info!("DB: Fetched total_pl {} for crypto", pl);
                return Ok(pl);
            }
            Err(e) => {
                error!(
                    "DB: Failed to fetch total_pl for crypto on attempt {}/{}: {}",
                    attempt + 1,
                    max_retries,
                    e
                );
                if attempt < max_retries - 1 {
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt as u32)))
                        .await;
                } else {
                    return Err(anyhow!(
                        "Failed to fetch total_pl for crypto after {} retries: {}",
                        max_retries, e
                    ));
                }
            }
        }
    }
    Err(anyhow!("Unreachable: max retries exceeded"))
}

#[derive(Serialize)]
struct TradesRow {
    timestamp: String,
    pair: String,
    trade_id: String,
    order_id: Option<String>,
    #[serde(rename = "type")]
    type_: String,
    amount: Decimal,
    execution_price: Decimal,
    fees: Decimal,
    fee_percentage: Decimal,
    profit: Option<Decimal>,
    profit_percentage: Option<Decimal>,
    reason: Option<String>,
    avg_cost_basis: Option<Decimal>,
    slippage: Option<Decimal>,
    remaining_amount: Decimal,
    open: i32,
    partial_open: i32,
}

#[derive(Serialize)]
struct PositionsRow {
    pair: String,
    total_usd: Decimal,
    total_quantity: Decimal,
    usd_balance: Decimal,
    last_updated: Option<String>,
    total_fees: Decimal,
    total_pl: Decimal,
    highest_price_since_buy: Option<Decimal>,
    last_synced: Option<String>,
}

pub async fn export_trades_to_csv(pool: &deadpool_postgres::Pool) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all("data")?;
    let mut wtr = Writer::from_path("data/trades.csv")?;

    let client = pool.get().await?;
    let rows = client.query(
        "SELECT timestamp, pair, trade_id, order_id, \"type\", amount, execution_price, fees, fee_percentage, profit, profit_percentage, reason, avg_cost_basis, slippage, remaining_amount, open, partial_open FROM trades ORDER BY timestamp DESC",
        &[],
    ).await?;

    for row in rows {
        let trade_row = TradesRow {
            timestamp: row.get(0),
            pair: row.get(1),
            trade_id: row.get(2),
            order_id: row.get(3),
            type_: row.get(4),
            amount: row.get(5),
            execution_price: row.get(6),
            fees: row.get(7),
            fee_percentage: row.get(8),
            profit: row.get(9),
            profit_percentage: row.get(10),
            reason: row.get(11),
            avg_cost_basis: row.get(12),
            slippage: row.get(13),
            remaining_amount: row.get(14),
            open: row.get(15),
            partial_open: row.get(16),
        };
        wtr.serialize(trade_row)?;
    }
    wtr.flush()?;
    Ok(())
}

pub async fn export_positions_to_csv(pool: &deadpool_postgres::Pool) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all("data")?;
    let mut wtr = Writer::from_path("data/positions.csv")?;

    wtr.write_record(&[
        "pair", "total_usd", "total_quantity", "usd_balance", "last_updated",
        "total_fees", "total_pl", "highest_price_since_buy", "last_synced"
    ])?;

    let client = pool.get().await?;
    let rows = client.query(
        "SELECT pair, total_usd, total_quantity, usd_balance, last_updated, total_fees, total_pl, highest_price_since_buy, last_synced FROM positions",
        &[],
    ).await?;

    for row in rows {
        let pair: String = row.get(0);
        let total_usd: Decimal = row.get(1);
        let total_quantity: Decimal = row.get(2);
        let usd_balance: Decimal = row.get(3);
        let last_updated: Option<String> = row.get(4);
        let total_fees: Decimal = row.get(5);
        let total_pl: Decimal = row.get(6);
        let highest_price_since_buy: Option<Decimal> = row.get(7);
        let last_synced: Option<String> = row.get(8);

        wtr.serialize((
            pair, total_usd, total_quantity, usd_balance, last_updated,
            total_fees, total_pl, highest_price_since_buy, last_synced
        ))?;
    }
    wtr.flush()?;
    Ok(())
}