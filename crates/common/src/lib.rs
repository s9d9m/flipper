//! Shared types for the skyblock-flipper workspace.
//!
//! This crate intentionally contains no business logic and no async runtime
//! dependency. It exists so every other crate (ingestion, parser, pricing,
//! engine, notify) can agree on the same wire-format structs and config
//! shape without depending on each other directly.

use serde::Deserialize;
use std::env;

/// Default background-backfill item tag list (price coverage session).
/// Two deliberately different categories, both feeding the "consistent
/// coins/hour, occasional massive flip" goal:
/// - endgame weapons/accessories: the rare, huge-ROI items this project
///   also needs to catch (Hyperion, Necron's Handle, etc.) -- the
///   original 6-tag starter list, expanded.
/// - high-volume enchanted crafting materials: essentially no
///   meaningful modifiers (so Tier 1 and Tier 3 collapse to nearly the
///   same thing for them), but they dominate raw AH listing volume,
///   so they reach Tier 1/3 confidence fast and drive the "many
///   consistent 1-10m flips" side of the goal.
///
/// Sourced from general public SkyBlock community knowledge, NOT
/// verified against a live COFL/Hypixel payload (this sandbox has no
/// network access to either -- same caveat already documented for the
/// `fingerprint` crate's NBT tag names). A wrong/nonexistent tag
/// degrades gracefully: `cofl::backfill` treats a failed fetch for one
/// tag as a logged, counted `fetch_errors` and moves on to the next
/// tag, never panicking or blocking the rest of the crawl.
const DEFAULT_COFL_BACKFILL_ITEM_TAGS: &str = "HYPERION,VALKYRIE,ASTRAEA,SCYLLA,NECRON_HANDLE,ASPECT_OF_THE_END,ASPECT_OF_THE_VOID,ASPECT_OF_THE_DRAGONS,LIVID_DAGGER,SHADOW_FURY,GIANTS_SWORD,MIDAS_SWORD,VOODOO_DOLL,SPIRIT_SCEPTRE,SILENT_DEATH,TERMINATOR,JUJU_SHORTBOW,RUNAANS_BOW,MOSQUITO_BOW,BONZO_STAFF,REAPER_FALCHION,PIGMAN_SWORD,ZOMBIE_SWORD,PRISMARINE_BLADE,YETI_SWORD,RAIDER_AXE,EXECUTIVE_AXE,HYPERSONIC_WAND,FLOWER_OF_TRUTH,ICE_SPRAY_WAND,TACTICIAN_SWORD,POOCH_SWORD,EMPEROR_SWORD,SPIDER_QUEEN_STINGER,HEGEMONY_ARTIFACT,WITHER_ARTIFACT,SPEED_TALISMAN,INTIMIDATION_TALISMAN,RING_OF_LOVE,HEALING_RING,POTION_AFFINITY_TALISMAN,CAMPFIRE_TALISMAN,ENCHANTED_COAL,ENCHANTED_IRON,ENCHANTED_GOLD,ENCHANTED_DIAMOND,ENCHANTED_LAPIS_LAZULI,ENCHANTED_REDSTONE,ENCHANTED_EMERALD,ENCHANTED_QUARTZ,ENCHANTED_GLOWSTONE_DUST,ENCHANTED_OBSIDIAN,ENCHANTED_ENDER_PEARL,ENCHANTED_SLIME_BALL,ENCHANTED_SUGAR_CANE,ENCHANTED_SUGAR,ENCHANTED_CACTUS_GREEN,ENCHANTED_CACTUS,ENCHANTED_MELON,ENCHANTED_MELON_BLOCK,ENCHANTED_PUMPKIN,ENCHANTED_RAW_RABBIT,ENCHANTED_RABBIT_HIDE,ENCHANTED_LEATHER,ENCHANTED_MUTTON,ENCHANTED_RAW_CHICKEN,ENCHANTED_COOKED_CHICKEN,ENCHANTED_EGG,ENCHANTED_FEATHER,ENCHANTED_RAW_BEEF,ENCHANTED_RAW_PORKCHOP,ENCHANTED_RAW_FISH,ENCHANTED_RAW_SALMON,ENCHANTED_PUFFERFISH,ENCHANTED_CLOWNFISH,ENCHANTED_PRISMARINE_SHARD,ENCHANTED_PRISMARINE_CRYSTAL,ENCHANTED_SPONGE,ENCHANTED_BONE,ENCHANTED_BONE_BLOCK,ENCHANTED_ROTTEN_FLESH,ENCHANTED_STRING,ENCHANTED_SPIDER_EYE,ENCHANTED_GUNPOWDER,ENCHANTED_NETHERRACK,ENCHANTED_NETHER_STALK,ENCHANTED_BLAZE_ROD,ENCHANTED_MAGMA_CREAM,ENCHANTED_GHAST_TEAR,ENCHANTED_ENDSTONE,ENCHANTED_COBBLESTONE,ENCHANTED_STONE,ENCHANTED_NETHER_BRICK,ENCHANTED_CLAY_BALL,ENCHANTED_CLAY_BLOCK,ENCHANTED_SNOWBALL,ENCHANTED_ICE,ENCHANTED_HAY_BLOCK,ENCHANTED_WHEAT,ENCHANTED_BREAD,ENCHANTED_CARROT,ENCHANTED_POTATO,ENCHANTED_BAKED_POTATO";

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
    /// Bind address for the WebSocket flip-notification server (see
    /// `notify` crate).
    pub websocket_bind_addr: String,
    /// Whether to run the COFL historical-price backfill at startup
    /// (see `cofl` crate). Off by default in environments without
    /// network access to `sky.coflnet.com`.
    pub cofl_backfill_enabled: bool,
    /// Base URL for the COFL (Coflnet, sky.coflnet.com) REST API.
    pub cofl_base_url: String,
    /// SkyBlock item tags to backfill historical prices for. Not an
    /// attempt to cover the whole catalog, but broad enough (price
    /// coverage session: ~100 items spanning endgame weapons/
    /// accessories and high-volume enchanted materials) to move the
    /// needle on `diag_no_price_data_total` — see the `cofl` crate and
    /// `DEFAULT_COFL_BACKFILL_ITEM_TAGS` above.
    pub cofl_backfill_item_tags: Vec<String>,
    /// How many pages of sold-auction history to fetch per item tag.
    pub cofl_backfill_pages_per_tag: u32,
    /// How often (minutes) to re-run the COFL backfill after the
    /// initial startup crawl, so the price cache keeps gaining coverage
    /// and freshness "during operation," not just once at boot (price
    /// coverage session). `0` disables the repeat — startup-only, the
    /// original behavior. Still entirely background: spawned, never
    /// awaited by the ingestion hot path.
    pub cofl_backfill_interval_minutes: u64,
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

        let websocket_bind_addr =
            env::var("WEBSOCKET_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:9001".to_string());

        let cofl_backfill_enabled = env::var("COFL_BACKFILL_ENABLED")
            .unwrap_or_else(|_| "true".to_string())
            .parse::<bool>()
            .map_err(|_| {
                ConfigError::InvalidVar(
                    "COFL_BACKFILL_ENABLED".into(),
                    "expected \"true\" or \"false\"".into(),
                )
            })?;

        let cofl_base_url =
            env::var("COFL_BASE_URL").unwrap_or_else(|_| "https://sky.coflnet.com/api".to_string());

        let cofl_backfill_item_tags = env::var("COFL_BACKFILL_ITEM_TAGS")
            .unwrap_or_else(|_| DEFAULT_COFL_BACKFILL_ITEM_TAGS.to_string())
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let cofl_backfill_pages_per_tag = env::var("COFL_BACKFILL_PAGES_PER_TAG")
            .unwrap_or_else(|_| "3".to_string())
            .parse::<u32>()
            .map_err(|_| {
                ConfigError::InvalidVar(
                    "COFL_BACKFILL_PAGES_PER_TAG".into(),
                    "expected a non-negative integer".into(),
                )
            })?;

        let cofl_backfill_interval_minutes = env::var("COFL_BACKFILL_INTERVAL_MINUTES")
            .unwrap_or_else(|_| "60".to_string())
            .parse::<u64>()
            .map_err(|_| {
                ConfigError::InvalidVar(
                    "COFL_BACKFILL_INTERVAL_MINUTES".into(),
                    "expected a non-negative integer number of minutes (0 disables the repeat)"
                        .into(),
                )
            })?;

        Ok(Config {
            hypixel_api_key,
            hypixel_base_url,
            tick_poll_interval_ms,
            request_timeout_ms,
            storage_db_path,
            websocket_bind_addr,
            cofl_backfill_enabled,
            cofl_base_url,
            cofl_backfill_item_tags,
            cofl_backfill_pages_per_tag,
            cofl_backfill_interval_minutes,
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
