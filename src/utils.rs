// utils.rs
// description: Shared utilities for timestamp parsing and current time.

// IMPORTS
use chrono::{DateTime, TimeZone, Utc, offset::Local};
use rust_decimal::{Decimal, prelude::*};
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::sync::Arc;

pub fn get_current_time() -> DateTime<Local> {
    Local::now()
}

pub fn parse_unix_timestamp(unix_str: &str) -> Option<DateTime<Utc>> {
    let time_dec: Decimal = Decimal::from_str(unix_str).ok()?;
    let secs = time_dec.floor().to_i64().unwrap_or(0);
    let nanos = ((time_dec - Decimal::from(secs)) * Decimal::from(1_000_000_000)).to_u32().unwrap_or(0);
    Utc.timestamp_opt(secs, nanos).single()
}

pub fn get_eastern_tz() -> Local {
    Local
}

pub fn report_log(report_path: &str, message: &str) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .write(true)
        .open(report_path)?;

    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();

    // This handles both single-line and multi-line messages cleanly:
    // - Single-line event → one timestamped line
    // - Multi-line snapshot → timestamp on the first line, subsequent lines indented/no timestamp
    writeln!(file, "[{}] {}", timestamp, message.replace('\n', "\n    "))?;

    Ok(())
}

pub fn report_order_placed(report_path: &str, pair: &str, side: &str, qty: Decimal, price: Decimal, post_only: bool) {
    let post = if post_only { "post_only" } else { "" };
    let msg = format!("ORDER PLACED: {} {} {} @ {} {}", pair, side, qty, price, post).trim_end().to_string();
    let _ = report_log(report_path, &msg);
}

pub fn report_order_success(report_path: &str, pair: &str, order_id: &str) {
    let msg = format!("ORDER SUCCESS: {} order_id={}", pair, order_id);
    let _ = report_log(report_path, &msg);
}

pub fn report_fill(report_path: &str, pair: &str, order_id: &str, filled_qty: Decimal, remaining_qty: Decimal, price: Decimal, fees: Decimal) {
    let msg = format!("FILL: {} order_id={} filled={} remaining={} price={} fees={}", pair, order_id, filled_qty, remaining_qty, price, fees);
    let _ = report_log(report_path, &msg);
}

pub fn report_cancel(report_path: &str, side: &str, pair: &str, order_id: &str, filled_qty: Decimal, reason: &str, current_close: Option<Decimal>, current_bid: Option<Decimal>, current_ask: Option<Decimal>, attempted_price: Option<Decimal>, trade_type: &str) {
    let mut msg = format!("{} CANCELLED (PARTIAL): {}\n    Order ID: {}\n    Filled Qty: {:.8}\n    Reason: {}", side.to_uppercase(), pair, order_id, filled_qty, reason);
    if let (Some(close), Some(bid), Some(ask), Some(attempted)) = (current_close, current_bid, current_ask, attempted_price) {
        let pct_diff = if trade_type == "buy" && ask > Decimal::ZERO {
            ((attempted - ask) / ask * Decimal::from(100)).round_dp(2)
        } else if trade_type == "sell" && bid > Decimal::ZERO {
            ((bid - attempted) / bid * Decimal::from(100)).round_dp(2)
        } else {
            Decimal::ZERO
        };
        let direction = if pct_diff > Decimal::ZERO {
            if trade_type == "buy" { "above ask" } else { "below bid" }
        } else if pct_diff < Decimal::ZERO {
            if trade_type == "buy" { "below ask" } else { "above bid" }
        } else {
            "at market"
        };
        msg.push_str(&format!("\n    Current: close={:.5} bid={:.5} ask={:.5}\n    Attempted: {:.5}\n    Diff: {:.2}% {}", close, bid, ask, attempted, pct_diff.abs(), direction));
    } else {
        msg.push_str("\n    Price data unavailable");
    }
    let _ = report_log(report_path, &msg);
}

pub fn report_order_failed(report_path: &str, side: &str, pair: &str, reason: &str, current_close: Option<Decimal>, current_bid: Option<Decimal>, current_ask: Option<Decimal>, attempted_price: Option<Decimal>) {
    let mut msg = format!("{} FAILED: {}\n    Reason: {}", side.to_uppercase(), pair, reason);
    if let (Some(close), Some(bid), Some(ask), Some(attempted)) = (current_close, current_bid, current_ask, attempted_price) {
        let pct_diff = if side == "buy" && ask > Decimal::ZERO {
            ((attempted - ask) / ask * Decimal::from(100)).round_dp(2)
        } else if side == "sell" && bid > Decimal::ZERO {
            ((bid - attempted) / bid * Decimal::from(100)).round_dp(2)
        } else {
            Decimal::ZERO
        };
        let direction = if pct_diff > Decimal::ZERO {
            if side == "buy" { "above ask" } else { "below bid" }
        } else if pct_diff < Decimal::ZERO {
            if side == "buy" { "below ask" } else { "above bid" }
        } else {
            "at market"
        };
        msg.push_str(&format!("\n    Current: close={:.5} bid={:.5} ask={:.5}\n    Attempted: {:.5}\n    Diff: {:.2}% {}", close, bid, ask, attempted, pct_diff.abs(), direction));
    } else {
        msg.push_str("\n    Price data unavailable");
    }
    let _ = report_log(report_path, &msg);
}

pub fn report_skip(report_path: &str, pair: &str, side: &str, reason: &str) {
    let msg = format!("{} SKIPPED: {} reason={}", side.to_uppercase(), pair, reason);
    let _ = report_log(report_path, &msg);
}

pub fn report_order_attempt(report_path: &str, side: &str, pair: &str, order_id: &str, limit_price: Decimal, ordered_qty: Decimal, reason: &str, close_price: Decimal, best_ask: Decimal, best_bid: Decimal) {
    let slippage = if side == "buy" && best_ask > Decimal::ZERO {
        ((limit_price - best_ask) / best_ask * Decimal::from(100)).abs()
    } else if side == "sell" && best_bid > Decimal::ZERO {
        ((best_bid - limit_price) / best_bid * Decimal::from(100)).abs()
    } else {
        Decimal::ZERO
    };
    let msg = format!(
        "{} ATTEMPTED: {}\n    Order ID: {}\n    Limit Price: {:.5}\n    Ordered Qty: {:.8}\n    Reason: {}\n    Market: close={:.5} best_ask={:.5} best_bid={:.5} slippage={:.2}%",
        side.to_uppercase(), pair, order_id, limit_price, ordered_qty, reason, close_price, best_ask, best_bid, slippage
    );
    let _ = report_log(report_path, &msg);
}

pub fn report_order_partial(report_path: &str, side: &str, pair: &str, order_id: &str, filled_qty: Decimal, remaining_qty: Decimal, avg_price: Decimal, fees: Decimal) {
    let pct = if filled_qty > Decimal::ZERO && (filled_qty + remaining_qty) > Decimal::ZERO {
        (filled_qty / (filled_qty + remaining_qty) * Decimal::from(100)).round_dp(1)
    } else {
        Decimal::ZERO
    };
    let msg = format!(
        "{} PARTIAL FILL: {}\n    Order ID: {}\n    Filled Qty: {:.8} ({:.1}%) / Remaining: {:.8}\n    Avg Fill Price: {:.5}\n    Fees USD: {:.4}",
        side.to_uppercase(), pair, order_id, filled_qty, pct, remaining_qty, avg_price, fees
    );
    let _ = report_log(report_path, &msg);
}

pub fn report_order_full(report_path: &str, side: &str, pair: &str, order_id: &str, filled_qty: Decimal, avg_price: Decimal, fees: Decimal) {
    let msg = format!(
        "{} FULL FILL: {}\n    Order ID: {}\n    Filled Qty: {:.8}\n    Avg Fill Price: {:.5}\n    Fees USD: {:.4}",
        side.to_uppercase(), pair, order_id, filled_qty, avg_price, fees
    );
    let _ = report_log(report_path, &msg);
}

pub fn format_volume_with_commas(volume: Decimal) -> String {
    let mut s = volume.to_string();
    if let Some(dot_pos) = s.find('.') {
        let integer_part = s[..dot_pos].to_string();
        let fractional_part = &s[dot_pos..];
        let mut formatted_integer = String::new();
        for (i, c) in integer_part.chars().rev().enumerate() {
            if i > 0 && i % 3 == 0 {
                formatted_integer.push(',');
            }
            formatted_integer.push(c);
        }
        formatted_integer = formatted_integer.chars().rev().collect();
        format!("{}{}", formatted_integer, fractional_part)
    } else {
        let mut formatted = String::new();
        for (i, c) in s.chars().rev().enumerate() {
            if i > 0 && i % 3 == 0 {
                formatted.push(',');
            }
            formatted.push(c);
        }
        formatted.chars().rev().collect()
    }
}