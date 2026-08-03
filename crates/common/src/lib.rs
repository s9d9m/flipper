//! Shared types for the skyblock-flipper workspace.
//!
//! This crate intentionally contains no business logic and no async runtime
//! dependency. It exists so every other crate (ingestion, parser, pricing,
//! engine, notify) can agree on the same wire-format structs and config
//! shape without depending on each other directly.

use serde::Deserialize;
use std::env;

/// One auction as returned by the Hypixel `/skyblock/auctions` endpoint.
///
/// This only includes the fields the ingestion and parser crates actually
/// need. Hypixel's payload has more fields (item_lore, claimed_bidders,
/// bids, etc.) that are irrelevant to flip detection and are deliberately
/// not deserialized here — serde will just skip them, at zero extra cost
/// on our side since we never allocate for them.
#[derive(Debug, Clone, Deserialize)]
pub struct RawAuction {
    pub uuid: String,
    pub auctioneer: String,
    pub item_name: String,
    pub starting_bid: u64,
    /// Base64-encoded, gzip-compressed NBT. Decoded by the `parser` crate
    /// (Phase 1.4), not here.
    pub item_bytes: String,
    pub bin: Option<bool>,
    pub end: i64,
}

/// One page of the paginated auctions endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct AuctionPageResponse {
    pub success: bool,
    pub page: u32,
    #[serde(rename = "totalPages")]
    pub total_pages: u32,
    #[serde(rename = "totalAuctions")]
    pub total_auctions: u64,
    /// Unix millis. This is the field the ingestion service polls
    /// cheaply and repeatedly to detect a new cache tick before paying
    /// the cost of a full multi-page fetch.
    #[serde(rename = "lastUpdated")]
    pub last_updated: i64,
    pub auctions: Vec<RawAuction>,
}

/// A fully-assembled snapshot: every page merged, from a single cache tick.
#[derive(Debug, Clone)]
pub struct AuctionSnapshot {
    pub last_updated: i64,
    pub auctions: Vec<RawAuction>,
}

/// Runtime configuration, loaded from environment variables (see
/// `.env.example` at the repo root).
#[derive(Debug, Clone)]
pub struct Config {
    pub hypixel_api_key: String,
    pub hypixel_base_url: String,
    /// How often to poll the cheap tick-detection endpoint, in
    /// milliseconds. Kept low (sub-second) per the sniper-mode design —
    /// this is the main lever on detection latency.
    pub tick_poll_interval_ms: u64,
    /// HTTP request timeout, in milliseconds.
    pub request_timeout_ms: u64,
    /// Path to the SQLite database file used for auction/price history
    /// storage (the async, off-hot-path lane — see `storage` crate).
    pub storage_db_path: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing required environment variable: {0}")]
    MissingVar(String),
    #[error("invalid value for environment variable {0}: {1}")]
    InvalidVar(String, String),
}

impl Config {
    /// Loads config from environment variables. Does not read a `.env`
    /// file itself — the binary crate is responsible for calling
    /// `dotenvy::dotenv()` (or equivalent) before this, if desired, so
    /// this crate has no filesystem/dotenv dependency of its own.
    pub fn from_env() -> Result<Self, ConfigError> {
        let hypixel_api_key = env::var("HYPIXEL_API_KEY")
            .map_err(|_| ConfigError::MissingVar("HYPIXEL_API_KEY".into()))?;

        let hypixel_base_url =
            env::var("HYPIXEL_BASE_URL").unwrap_or_else(|_| "https://api.hypixel.net".to_string());

        let tick_poll_interval_ms = env::var("TICK_POLL_INTERVAL_MS")
            .unwrap_or_else(|_| "750".to_string())
            .parse::<u64>()
            .map_err(|_| {
                ConfigError::InvalidVar(
                    "TICK_POLL_INTERVAL_MS".into(),
                    "expected an integer number of milliseconds".into(),
                )
            })?;

        let request_timeout_ms = env::var("REQUEST_TIMEOUT_MS")
            .unwrap_or_else(|_| "5000".to_string())
            .parse::<u64>()
            .map_err(|_| {
                ConfigError::InvalidVar(
                    "REQUEST_TIMEOUT_MS".into(),
                    "expected an integer number of milliseconds".into(),
                )
            })?;

        let storage_db_path =
            env::var("STORAGE_DB_PATH").unwrap_or_else(|_| "auctions.sqlite3".to_string());

        Ok(Config {
            hypixel_api_key,
            hypixel_base_url,
            tick_poll_interval_ms,
            request_timeout_ms,
            storage_db_path,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_minimal_auction_page() {
        let json = r#"
        {
            "success": true,
            "page": 0,
            "totalPages": 1,
            "totalAuctions": 1,
            "lastUpdated": 1690000000000,
            "auctions": [
                {
                    "uuid": "abc123",
                    "auctioneer": "seller-uuid",
                    "item_name": "Hyperion",
                    "starting_bid": 900000000,
                    "item_bytes": "base64gzipdata==",
                    "bin": true,
                    "end": 1690003600000
                }
            ]
        }
        "#;

        let page: AuctionPageResponse = serde_json::from_str(json).unwrap();
        assert!(page.success);
        assert_eq!(page.auctions.len(), 1);
        assert_eq!(page.auctions[0].item_name, "Hyperion");
        assert_eq!(page.auctions[0].bin, Some(true));
    }
}
