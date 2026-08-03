//! COFL (Coflnet, `sky.coflnet.com`) historical pricing importer.
//!
//! ```text
//! GET /api/auctions/tag/{tag}/sold (paginated, background/startup only)
//!   -> CoflSoldAuction         (raw wire struct, lenient/optional fields)
//!   -> HistoricalSale          (normalized, network-independent)
//!   -> fingerprint_of()        (builds a synthetic ParsedItem, calls the
//!                                SAME fingerprint::fingerprint() live
//!                                auctions use)
//!   -> grouped by Fingerprint, median sale price -> PriceEntry {
//!        source: PriceSource::Historical }
//!   -> PriceCache::update_batch()
//! ```
//!
//! # Hot-path guarantee
//!
//! Nothing in this crate is reachable from the sniper path. `backfill`
//! is an async function meant to run once, before or alongside
//! ingestion startup (see `ingestion-service`'s `main.rs`), never called
//! per-auction or per-tick. The only thing the hot path ever sees is the
//! `PriceEntry` values this crate wrote into `pricing::PriceCache`
//! before detection started — `PriceCache::get` doesn't know or care
//! whether an entry came from COFL or a live tick.
//!
//! # Investigation summary (grounded in Coflnet's own auto-generated
//! OpenAPI TypeScript client, `Coflnet/hypixel-react` on GitHub — this
//! sandbox has no network access to `sky.coflnet.com` itself to verify
//! against a live payload, same limitation as `api.hypixel.net`
//! elsewhere in this workspace)
//!
//! **Confirmed**, from the client's generated schema types:
//! - `GET /api/auctions/tag/{tag}/sold` returns a bare `SoldAuction[]`
//!   (no pagination envelope), paginated via `page`/`pageSize` query
//!   params, with **no server-side attribute/modifier filter** — every
//!   sold auction for that tag comes back, filtering has to happen on
//!   our side.
//! - `SoldAuction` carries a structured `enchantments: {type, level}[]`
//!   and a `flattenedNbt: {[key: string]: string}` map (a flattened
//!   dump of the item's raw NBT `ExtraAttributes`), plus, critically,
//!   **`shortItemBytes: string` — "NBT data as base64 encoded string"**.
//!   If that decodes the same way Hypixel's own `item_bytes` does
//!   (base64 -> gzip -> NBT), it lets us reuse `parser`'s real decoder
//!   directly instead of reconstructing attributes field-by-field. This
//!   crate tries that first ([`decode_short_item_bytes`]) and only
//!   falls back to field reconstruction if it fails.
//!
//! **Not confirmed** (each flagged at its use site below, same
//! treatment as the `fingerprint` crate's NBT-tag-name caveat):
//! - Whether `shortItemBytes` is actually gzip-wrapped like
//!   `item_bytes`, or some other "short"/reduced encoding — "short"
//!   suggests it might not be identical. Two decode attempts are made
//!   (gzip-wrapped, then raw NBT) before falling back.
//! - `flattenedNbt`'s exact key spelling — assumed to preserve
//!   Hypixel's raw tag names verbatim (`hot_potato_count`,
//!   `rarity_upgrades`, `dungeon_item_level`, `art_of_war_count`,
//!   `talisman_enrichment`, `skin`, `modifier`), the same assumption
//!   the `fingerprint` crate already makes and flags.
//! - Whether `enchantments[].type` (a C#/.NET enum) serializes as a
//!   PascalCase string (`"UltimateWise"`) or a numeric id. This is the
//!   single highest-risk assumption in this crate: if COFL's naming
//!   doesn't reverse-map to Hypixel's raw snake_case convention via
//!   [`pascal_to_snake`], a COFL-seeded fingerprint for an enchanted
//!   item will silently never match its live counterpart. Numeric
//!   values are given an opaque `unknown_{n}` label instead of guessed,
//!   so they at least group consistently among themselves without
//!   pretending to match live naming.
//! - Gemstones, runes, and ability scrolls: no confirmed representation
//!   in `flattenedNbt` (a flat string map can't obviously carry nested
//!   compounds or lists without a confirmed flattening convention).
//!   **Deliberately left unpopulated** in the reconstruction fallback
//!   rather than guessed — an absent modifier degrades the fingerprint
//!   gracefully (same as a live item that genuinely has none), where a
//!   wrong guess would silently corrupt it. Real NBT bytes via
//!   `shortItemBytes`, when they decode, are unaffected by this gap.

use base64::prelude::{Engine as _, BASE64_STANDARD};
use fastnbt::Value;
use fingerprint::Fingerprint;
use parser::ParsedItem;
use pricing::{PriceCache, PriceEntry, PriceSource};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{info, warn};

#[derive(Debug, thiserror::Error)]
pub enum CoflError {
    #[error("http request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("cofl api returned status {status} for {url}")]
    UnexpectedStatus { status: u16, url: String },
}

/// A single historical sale, normalized to be independent of COFL's
/// wire format — this is what [`fingerprint_of`] operates on, and what
/// gets unit tested without any network/wiremock involvement.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoricalSale {
    pub item_tag: String,
    pub sale_price: u64,
    /// Unix millis (same unit as the pipeline's `tick` throughout).
    pub sold_at: i64,
    /// The item's real `ExtraAttributes`, decoded from `shortItemBytes`
    /// via the exact same NBT decoder live auctions use. When present,
    /// this is authoritative and `reconstructed` below is ignored.
    pub extra_attributes: Option<Value>,
    /// Lower-confidence reconstruction from COFL's separately exposed
    /// structured/flattened fields, used only when `extra_attributes`
    /// is `None`. See the module doc comment's caveats.
    pub reconstructed: ReconstructedAttributes,
}

/// Best-effort reconstruction of price-relevant modifiers from COFL's
/// structured (`enchantments`) and flattened-NBT fields, used as a
/// fallback when `shortItemBytes` doesn't decode. Deliberately mirrors
/// only the subset of `fingerprint::fingerprint`'s inputs that have a
/// confirmed or reasonably-assumed source in COFL's schema — see the
/// module doc comment for what's excluded and why.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ReconstructedAttributes {
    pub reforge: Option<String>,
    pub enchantments: Vec<(String, i32)>,
    pub hot_potato_count: Option<i32>,
    pub rarity_upgrades: Option<i32>,
    pub dungeon_item_level: Option<i32>,
    pub art_of_war_count: Option<i32>,
    pub talisman_enrichment: Option<String>,
    pub skin: Option<String>,
}

/// Reconstructs the same [`Fingerprint`] a live auction of this item
/// would produce, by building a synthetic [`ParsedItem`] and calling the
/// **same** `fingerprint::fingerprint()` function live auctions use —
/// not a parallel implementation, so live and historical fingerprints
/// are guaranteed consistent by construction, not by keeping two
/// implementations in sync by hand.
pub fn fingerprint_of(sale: &HistoricalSale) -> Fingerprint {
    let extra_attributes = sale
        .extra_attributes
        .clone()
        .unwrap_or_else(|| build_reconstructed_value(&sale.reconstructed));

    let item = ParsedItem {
        uuid: String::new(),
        auctioneer: String::new(),
        skyblock_item_id: sale.item_tag.clone(),
        display_name: sale.item_tag.clone(),
        count: 1,
        starting_bid: sale.sale_price,
        bin: true,
        end: sale.sold_at,
        extra_attributes: Some(extra_attributes),
    };

    fingerprint::fingerprint(&item)
}

fn build_reconstructed_value(r: &ReconstructedAttributes) -> Value {
    let mut map: HashMap<String, Value> = HashMap::new();

    if let Some(reforge) = &r.reforge {
        map.insert("modifier".to_string(), Value::String(reforge.clone()));
    }
    if let Some(v) = r.rarity_upgrades {
        map.insert("rarity_upgrades".to_string(), Value::Int(v));
    }
    if let Some(v) = r.hot_potato_count {
        map.insert("hot_potato_count".to_string(), Value::Int(v));
    }
    if let Some(v) = r.dungeon_item_level {
        map.insert("dungeon_item_level".to_string(), Value::Int(v));
    }
    if let Some(v) = r.art_of_war_count {
        map.insert("art_of_war_count".to_string(), Value::Int(v));
    }
    if let Some(s) = &r.talisman_enrichment {
        map.insert("talisman_enrichment".to_string(), Value::String(s.clone()));
    }
    if let Some(s) = &r.skin {
        map.insert("skin".to_string(), Value::String(s.clone()));
    }
    if !r.enchantments.is_empty() {
        let mut ench_map = HashMap::new();
        for (name, level) in &r.enchantments {
            ench_map.insert(name.clone(), Value::Int(*level));
        }
        map.insert("enchantments".to_string(), Value::Compound(ench_map));
    }
    // gems / runes / ability_scroll intentionally not reconstructed --
    // see the module doc comment.

    Value::Compound(map)
}

/// Tries to decode `encoded` as item NBT the same way a live auction's
/// `item_bytes` would decode (base64 -> gzip -> NBT); if that fails,
/// tries base64 -> raw NBT (no gzip), in case COFL's "short" encoding
/// skips the gzip wrapper. Returns `None` if neither attempt succeeds,
/// signaling the caller to fall back to field reconstruction.
fn decode_short_item_bytes(encoded: &str) -> Option<Value> {
    if let Ok(decoded) = parser::decode_item_bytes(encoded) {
        return decoded.extra_attributes;
    }

    let bytes = BASE64_STANDARD.decode(encoded).ok()?;
    parser::decode_nbt_bytes(&bytes)
        .ok()
        .and_then(|d| d.extra_attributes)
}

/// Best-effort conversion of a PascalCase .NET enum name (COFL's likely
/// wire format for `enchantments[].type`) into Hypixel's raw snake_case
/// NBT tag convention (e.g. `"UltimateWise"` -> `"ultimate_wise"`). NOT
/// verified against a live COFL payload — see the module doc comment.
/// Safe no-op if the input is already snake_case.
fn pascal_to_snake(input: &str) -> String {
    let mut out = String::with_capacity(input.len() + 4);
    for (i, ch) in input.chars().enumerate() {
        if ch.is_uppercase() && i > 0 {
            out.push('_');
        }
        out.extend(ch.to_lowercase());
    }
    out
}

fn value_to_snake_case_label(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(s) if !s.is_empty() => Some(pascal_to_snake(s)),
        // No mapping table for numeric .NET enum ids is available, so
        // this is an opaque, self-consistent label: it groups multiple
        // COFL sales of the same real enchant together (same id ->
        // same label every time), but won't match a live fingerprint's
        // real name.
        serde_json::Value::Number(n) => Some(format!("unknown_{n}")),
        _ => None,
    }
}

fn flat_str(map: &HashMap<String, Option<String>>, key: &str) -> Option<String> {
    map.get(key)
        .and_then(|v| v.clone())
        .filter(|s| !s.is_empty())
}

fn flat_int(map: &HashMap<String, Option<String>>, key: &str) -> Option<i32> {
    flat_str(map, key).and_then(|s| s.parse::<i32>().ok())
}

/// Minimal hand-rolled RFC3339 UTC parser (`YYYY-MM-DDTHH:MM:SS[.fff]Z`)
/// to unix millis, avoiding a chrono/time dependency for one
/// best-effort field. Returns `None` on any format it doesn't
/// recognize rather than guessing — an unparseable `end` just means
/// that sale doesn't influence a fingerprint's `updated_at_tick`, not a
/// hard error.
fn parse_rfc3339_millis(s: &str) -> Option<i64> {
    let s = s.strip_suffix('Z')?;
    let (date, time) = s.split_once('T')?;

    let mut date_parts = date.split('-');
    let year: i64 = date_parts.next()?.parse().ok()?;
    let month: i64 = date_parts.next()?.parse().ok()?;
    let day: i64 = date_parts.next()?.parse().ok()?;

    let (time, frac_millis) = match time.split_once('.') {
        Some((t, frac)) => {
            let frac_digits: String = frac.chars().take(3).collect();
            let frac_digits = format!("{frac_digits:0<3}");
            (t, frac_digits.parse::<i64>().ok()?)
        }
        None => (time, 0),
    };

    let mut time_parts = time.split(':');
    let hour: i64 = time_parts.next()?.parse().ok()?;
    let minute: i64 = time_parts.next()?.parse().ok()?;
    let second: i64 = time_parts.next()?.parse().ok()?;

    let days = days_from_civil(year, month, day);
    let millis = ((days * 24 + hour) * 60 + minute) * 60 * 1000 + second * 1000 + frac_millis;
    Some(millis)
}

/// Howard Hinnant's `days_from_civil`, a well-known correct algorithm
/// for civil-calendar-date -> days-since-unix-epoch, independent of any
/// COFL-specific uncertainty.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[derive(Debug, Deserialize)]
struct CoflEnchantment {
    #[serde(rename = "type")]
    kind: serde_json::Value,
    level: i32,
}

/// Best-effort reconstruction of `SoldAuction`'s JSON shape from
/// Coflnet's generated OpenAPI TypeScript client. Every field the
/// module doc comment doesn't have high confidence in is `Option`, so a
/// per-field mismatch degrades gracefully instead of failing
/// deserialization for the whole record.
#[derive(Debug, Deserialize)]
struct CoflSoldAuction {
    tag: Option<String>,
    #[serde(rename = "startingBid")]
    starting_bid: Option<u64>,
    #[serde(rename = "highestBidAmount")]
    highest_bid_amount: Option<u64>,
    bin: Option<bool>,
    end: Option<String>,
    enchantments: Option<Vec<CoflEnchantment>>,
    #[serde(rename = "shortItemBytes")]
    short_item_bytes: Option<String>,
    #[serde(rename = "flattenedNbt")]
    flattened_nbt: Option<HashMap<String, Option<String>>>,
}

fn normalize(item_tag_hint: &str, raw: CoflSoldAuction) -> Option<HistoricalSale> {
    let item_tag = raw.tag.unwrap_or_else(|| item_tag_hint.to_string());
    let bin = raw.bin.unwrap_or(false);

    // For a BIN auction the sale price is the listing price itself (no
    // bidding). For an auction that sold via bidding, it's the winning
    // bid; a highestBidAmount of 0/absent means nobody bid, i.e. it
    // wasn't actually sold, so it's not a comparable price point.
    let sale_price = if bin {
        raw.starting_bid?
    } else {
        raw.highest_bid_amount.filter(|&v| v > 0)?
    };
    if sale_price == 0 {
        return None;
    }

    let sold_at = raw
        .end
        .as_deref()
        .and_then(parse_rfc3339_millis)
        .unwrap_or(0);

    let extra_attributes = raw
        .short_item_bytes
        .as_deref()
        .and_then(decode_short_item_bytes);

    let reconstructed = ReconstructedAttributes {
        reforge: raw
            .flattened_nbt
            .as_ref()
            .and_then(|m| flat_str(m, "modifier")),
        enchantments: raw
            .enchantments
            .unwrap_or_default()
            .into_iter()
            .filter_map(|e| value_to_snake_case_label(&e.kind).map(|name| (name, e.level)))
            .collect(),
        hot_potato_count: raw
            .flattened_nbt
            .as_ref()
            .and_then(|m| flat_int(m, "hot_potato_count")),
        rarity_upgrades: raw
            .flattened_nbt
            .as_ref()
            .and_then(|m| flat_int(m, "rarity_upgrades")),
        dungeon_item_level: raw
            .flattened_nbt
            .as_ref()
            .and_then(|m| flat_int(m, "dungeon_item_level")),
        art_of_war_count: raw
            .flattened_nbt
            .as_ref()
            .and_then(|m| flat_int(m, "art_of_war_count")),
        talisman_enrichment: raw
            .flattened_nbt
            .as_ref()
            .and_then(|m| flat_str(m, "talisman_enrichment")),
        skin: raw.flattened_nbt.as_ref().and_then(|m| flat_str(m, "skin")),
    };

    Some(HistoricalSale {
        item_tag,
        sale_price,
        sold_at,
        extra_attributes,
        reconstructed,
    })
}

/// COFL REST client. Async, network-bound — never called from the hot
/// path, only from [`backfill`].
pub struct CoflClient {
    http: reqwest::Client,
    base_url: String,
}

impl CoflClient {
    pub fn new(base_url: impl Into<String>) -> Result<Self, CoflError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;
        Ok(Self {
            http,
            base_url: base_url.into(),
        })
    }

    /// Fetches and normalizes one page of recently-sold auctions for
    /// `item_tag`. Not on the hot path — called only from [`backfill`].
    pub async fn fetch_sold_page(
        &self,
        item_tag: &str,
        page: u32,
    ) -> Result<Vec<HistoricalSale>, CoflError> {
        let url = format!(
            "{}/api/auctions/tag/{item_tag}/sold?page={page}",
            self.base_url
        );
        let response = self.http.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(CoflError::UnexpectedStatus {
                status: response.status().as_u16(),
                url,
            });
        }

        let raw: Vec<CoflSoldAuction> = response.json().await?;
        Ok(raw
            .into_iter()
            .filter_map(|r| normalize(item_tag, r))
            .collect())
    }
}

/// Diagnostics for one [`backfill`] run. See the crate/module docs for
/// what each field means; `sales_from_real_nbt` vs
/// `sales_from_reconstruction` is the most useful signal for judging how
/// much the `shortItemBytes` assumption actually held once this runs
/// against live data.
#[derive(Debug, Clone, Copy, Default)]
pub struct ImportStats {
    pub item_tags_attempted: u32,
    pub sales_fetched: u64,
    pub fingerprints_loaded: u64,
    pub cache_entries_seeded: u64,
    pub fetch_errors: u64,
    pub sales_from_real_nbt: u64,
    pub sales_from_reconstruction: u64,
}

/// Backfills `cache` with historical prices for `item_tags`, up to
/// `pages_per_tag` pages of sold-auction history each. Meant to run once
/// at startup, concurrently with (not blocking) ingestion — see
/// `ingestion-service`'s `main.rs`. Rate-limits itself to stay
/// comfortably under COFL's public rate limits (documented ~30 req/10s,
/// 100 req/min); irrelevant to hot-path latency since this never runs
/// anywhere near the detection loop.
pub async fn backfill(
    client: &CoflClient,
    item_tags: &[String],
    pages_per_tag: u32,
    cache: &PriceCache,
) -> ImportStats {
    let mut stats = ImportStats::default();
    let mut grouped: HashMap<Fingerprint, Vec<u64>> = HashMap::new();
    let mut latest_sale_at: HashMap<Fingerprint, i64> = HashMap::new();
    let mut first_request = true;

    for tag in item_tags {
        stats.item_tags_attempted += 1;

        for page in 0..pages_per_tag {
            if !first_request {
                tokio::time::sleep(Duration::from_millis(350)).await;
            }
            first_request = false;

            match client.fetch_sold_page(tag, page).await {
                Ok(sales) => {
                    if sales.is_empty() {
                        break;
                    }
                    for sale in &sales {
                        stats.sales_fetched += 1;
                        if sale.extra_attributes.is_some() {
                            stats.sales_from_real_nbt += 1;
                        } else {
                            stats.sales_from_reconstruction += 1;
                        }

                        let fp = fingerprint_of(sale);
                        grouped.entry(fp).or_default().push(sale.sale_price);
                        latest_sale_at
                            .entry(fp)
                            .and_modify(|t| *t = (*t).max(sale.sold_at))
                            .or_insert(sale.sold_at);
                    }
                }
                Err(err) => {
                    stats.fetch_errors += 1;
                    warn!(item_tag = %tag, page, error = %err, "failed to fetch COFL sold-auction page");
                    break;
                }
            }
        }
    }

    stats.fingerprints_loaded = grouped.len() as u64;

    let updates: Vec<(Fingerprint, PriceEntry)> = grouped
        .into_iter()
        .map(|(fp, mut prices)| {
            prices.sort_unstable();
            let median = prices[prices.len() / 2];
            let entry = PriceEntry {
                estimated_value: median,
                sample_size: prices.len() as u32,
                updated_at_tick: latest_sale_at[&fp],
                source: PriceSource::Historical,
            };
            (fp, entry)
        })
        .collect();

    stats.cache_entries_seeded = updates.len() as u64;
    cache.update_batch(updates);

    info!(
        item_tags_attempted = stats.item_tags_attempted,
        sales_fetched = stats.sales_fetched,
        fingerprints_loaded = stats.fingerprints_loaded,
        cache_entries_seeded = stats.cache_entries_seeded,
        fetch_errors = stats.fetch_errors,
        sales_from_real_nbt = stats.sales_from_real_nbt,
        sales_from_reconstruction = stats.sales_from_reconstruction,
        "COFL historical price backfill complete"
    );

    stats
}

#[cfg(test)]
mod tests {
    use super::*;
    use fastnbt::nbt;
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn encode_item_bytes(value: &Value) -> String {
        let nbt_bytes = fastnbt::to_bytes(value).unwrap();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&nbt_bytes).unwrap();
        let gzipped = encoder.finish().unwrap();
        BASE64_STANDARD.encode(gzipped)
    }

    #[test]
    fn pascal_to_snake_conversions() {
        assert_eq!(pascal_to_snake("UltimateWise"), "ultimate_wise");
        assert_eq!(pascal_to_snake("Sharpness"), "sharpness");
        assert_eq!(pascal_to_snake("already_snake"), "already_snake");
    }

    #[test]
    fn parses_basic_rfc3339_utc_timestamp() {
        assert_eq!(
            parse_rfc3339_millis("2021-01-01T00:00:00Z"),
            Some(1_609_459_200_000)
        );
    }

    #[test]
    fn parses_rfc3339_with_fractional_seconds() {
        assert_eq!(
            parse_rfc3339_millis("2021-01-01T00:00:00.500Z"),
            Some(1_609_459_200_500)
        );
    }

    #[test]
    fn unparseable_timestamp_returns_none_gracefully() {
        assert_eq!(parse_rfc3339_millis("not-a-timestamp"), None);
    }

    #[test]
    fn shortitembytes_path_produces_the_same_fingerprint_as_a_live_auction() {
        let value = nbt!({
            "i": [{
                "Count": 1i8,
                "tag": {
                    "ExtraAttributes": {
                        "id": "HYPERION",
                        "hot_potato_count": 10,
                        "enchantments": { "ultimate_wise": 5 },
                    },
                },
            }],
        });
        let encoded = encode_item_bytes(&value);

        let sale = HistoricalSale {
            item_tag: "HYPERION".to_string(),
            sale_price: 1_000_000,
            sold_at: 1_000,
            extra_attributes: decode_short_item_bytes(&encoded),
            reconstructed: ReconstructedAttributes::default(),
        };

        let live_item = ParsedItem {
            uuid: String::new(),
            auctioneer: String::new(),
            skyblock_item_id: "HYPERION".to_string(),
            display_name: "HYPERION".to_string(),
            count: 1,
            starting_bid: 1_000_000,
            bin: true,
            end: 1_000,
            extra_attributes: parser::decode_item_bytes(&encoded)
                .unwrap()
                .extra_attributes,
        };

        assert_eq!(fingerprint_of(&sale), fingerprint::fingerprint(&live_item));
    }

    #[test]
    fn reconstruction_fallback_matches_real_nbt_when_assumptions_hold() {
        let value = nbt!({
            "i": [{
                "Count": 1i8,
                "tag": {
                    "ExtraAttributes": {
                        "id": "HYPERION",
                        "hot_potato_count": 10,
                        "modifier": "wise",
                    },
                },
            }],
        });
        let encoded = encode_item_bytes(&value);

        let real = HistoricalSale {
            item_tag: "HYPERION".into(),
            sale_price: 1,
            sold_at: 1,
            extra_attributes: decode_short_item_bytes(&encoded),
            reconstructed: ReconstructedAttributes::default(),
        };

        let reconstructed = HistoricalSale {
            item_tag: "HYPERION".into(),
            sale_price: 1,
            sold_at: 1,
            extra_attributes: None,
            reconstructed: ReconstructedAttributes {
                reforge: Some("wise".into()),
                hot_potato_count: Some(10),
                ..Default::default()
            },
        };

        assert_eq!(fingerprint_of(&real), fingerprint_of(&reconstructed));
    }

    #[test]
    fn normalize_prefers_bin_starting_bid_as_sale_price() {
        let raw: CoflSoldAuction = serde_json::from_str(
            r#"{
                "tag": "HYPERION",
                "startingBid": 900000000,
                "highestBidAmount": 0,
                "bin": true,
                "end": "2021-01-01T00:00:00Z"
            }"#,
        )
        .unwrap();

        let sale = normalize("HYPERION", raw).unwrap();
        assert_eq!(sale.sale_price, 900_000_000);
        assert_eq!(sale.sold_at, 1_609_459_200_000);
    }

    #[test]
    fn normalize_uses_highest_bid_for_non_bin_auctions() {
        let raw: CoflSoldAuction = serde_json::from_str(
            r#"{
                "tag": "HYPERION",
                "startingBid": 500000,
                "highestBidAmount": 750000,
                "bin": false,
                "end": "2021-01-01T00:00:00Z"
            }"#,
        )
        .unwrap();

        let sale = normalize("HYPERION", raw).unwrap();
        assert_eq!(sale.sale_price, 750_000);
    }

    #[test]
    fn normalize_skips_unsold_non_bin_auctions() {
        let raw: CoflSoldAuction = serde_json::from_str(
            r#"{
                "tag": "HYPERION",
                "startingBid": 500000,
                "bin": false,
                "end": "2021-01-01T00:00:00Z"
            }"#,
        )
        .unwrap();

        assert!(normalize("HYPERION", raw).is_none());
    }

    #[test]
    fn normalize_falls_back_to_the_hint_tag_when_tag_is_absent() {
        let raw: CoflSoldAuction = serde_json::from_str(
            r#"{
                "startingBid": 100,
                "bin": true,
                "end": "2021-01-01T00:00:00Z"
            }"#,
        )
        .unwrap();

        let sale = normalize("HYPERION", raw).unwrap();
        assert_eq!(sale.item_tag, "HYPERION");
    }

    #[tokio::test]
    async fn fetch_sold_page_parses_a_realistic_response() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/auctions/tag/HYPERION/sold"))
            .and(query_param("page", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {
                    "tag": "HYPERION",
                    "startingBid": 900000000,
                    "highestBidAmount": 0,
                    "bin": true,
                    "end": "2021-01-01T00:00:00Z",
                    "enchantments": [{"type": "UltimateWise", "level": 5}],
                    "flattenedNbt": {"hot_potato_count": "10"}
                }
            ])))
            .mount(&server)
            .await;

        let client = CoflClient::new(server.uri()).unwrap();
        let sales = client.fetch_sold_page("HYPERION", 0).await.unwrap();

        assert_eq!(sales.len(), 1);
        assert_eq!(sales[0].sale_price, 900_000_000);
        assert_eq!(sales[0].reconstructed.hot_potato_count, Some(10));
        assert_eq!(
            sales[0].reconstructed.enchantments,
            vec![("ultimate_wise".to_string(), 5)]
        );
    }

    #[tokio::test]
    async fn fetch_sold_page_surfaces_an_error_on_non_success_status() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/auctions/tag/HYPERION/sold"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let client = CoflClient::new(server.uri()).unwrap();
        let err = client.fetch_sold_page("HYPERION", 0).await.unwrap_err();
        assert!(matches!(
            err,
            CoflError::UnexpectedStatus { status: 500, .. }
        ));
    }

    #[tokio::test]
    async fn backfill_seeds_the_price_cache_from_multiple_sales() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/auctions/tag/HYPERION/sold"))
            .and(query_param("page", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {"tag": "HYPERION", "startingBid": 1000000, "bin": true, "end": "2021-01-01T00:00:00Z"},
                {"tag": "HYPERION", "startingBid": 2000000, "bin": true, "end": "2021-01-02T00:00:00Z"},
            ])))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/auctions/tag/HYPERION/sold"))
            .and(query_param("page", "1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(&server)
            .await;

        let client = CoflClient::new(server.uri()).unwrap();
        let cache = PriceCache::new();

        let stats = backfill(&client, &["HYPERION".to_string()], 5, &cache).await;

        assert_eq!(stats.sales_fetched, 2);
        assert_eq!(stats.fingerprints_loaded, 1);
        assert_eq!(stats.cache_entries_seeded, 1);

        let fp = fingerprint_of(&HistoricalSale {
            item_tag: "HYPERION".into(),
            sale_price: 0,
            sold_at: 0,
            extra_attributes: None,
            reconstructed: ReconstructedAttributes::default(),
        });
        let entry = cache.get(fp).unwrap();
        assert_eq!(entry.sample_size, 2);
        assert_eq!(entry.source, PriceSource::Historical);
        assert_eq!(entry.estimated_value, 2_000_000);
    }
}
