// utils.rs
// description: Shared utilities for timestamp parsing and current time.

// IMPORTS
use chrono::{DateTime, TimeZone, Utc, offset::Local};
use rust_decimal::{Decimal, prelude::*};
use std::fs::OpenOptions;
use std::io::{self, Write};
use rust_decimal::Decimal;
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

pub fn report_order_failed(report_path: &str, pair: &str, reason: &str) {
    let msg = format!("ORDER FAILED: {} reason={}", pair, reason);
    let _ = report_log(report_path, &msg);
}

pub fn report_fill(report_path: &str, pair: &str, order_id: &str, filled_qty: Decimal, remaining_qty: Decimal, price: Decimal, fees: Decimal) {
    let msg = format!("FILL: {} order_id={} filled={} remaining={} price={} fees={}", pair, order_id, filled_qty, remaining_qty, price, fees);
    let _ = report_log(report_path, &msg);
}

pub fn report_cancel(report_path: &str, pair: &str, order_id: &str, reason: &str) {
    let msg = format!("CANCEL: {} order_id={} reason={}", pair, order_id, reason);
    let _ = report_log(report_path, &msg);
}

pub fn report_skip(report_path: &str, pair: &str, side: &str, reason: &str) {
    let msg = format!("{} SKIPPED: {} reason={}", side.to_uppercase(), pair, reason);
    let _ = report_log(report_path, &msg);
}