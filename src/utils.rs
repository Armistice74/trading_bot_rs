// utils.rs
// description: Shared utilities for timestamp parsing and current time.

// IMPORTS
use chrono::{DateTime, TimeZone, Utc, offset::Local};
use rust_decimal::{Decimal, prelude::*};

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