//! Minimal item parser.
//!
//! Phase 1.4: the third stage of the pipeline. Consumes the `RawAuction`
//! values `diff::DiffDetector::diff` already filtered down to new/changed,
//! and decodes each auction's `item_bytes` (base64 -> gzip -> NBT) into a
//! `ParsedItem` — just the fields pricing and storage need: the SkyBlock
//! item id, a human-readable name, and stack count.
//!
//! This is deliberately *not* full NBT normalization. Enchantments, gem
//! slots, dyes, and other price-affecting modifiers are the
//! `ParsedItem::extra_attributes` bucket only, kept as a raw NBT `Value`
//! for the (not-yet-built) fingerprinting stage to interpret — that
//! two-tier split (minimal now, full later) is the same lossy-on-the-hot-
//! path design described in claude.md.

use base64::prelude::{Engine as _, BASE64_STANDARD};
use common::RawAuction;
use fastnbt::Value;
use serde::Deserialize;
use std::io::Read;

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedItem {
    pub uuid: String,
    pub auctioneer: String,
    pub skyblock_item_id: String,
    pub display_name: String,
    pub count: i8,
    pub starting_bid: u64,
    pub bin: bool,
    pub end: i64,
    /// Raw `ExtraAttributes` NBT compound, unparsed beyond `id`. The
    /// fingerprinting stage reads whatever modifiers it needs from this
    /// directly rather than this crate trying to model every one of them.
    pub extra_attributes: Option<Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("item_bytes is not valid base64: {0}")]
    Base64(#[from] base64::DecodeError),

    #[error("item_bytes did not decompress as gzip: {0}")]
    Gzip(#[from] std::io::Error),

    #[error("item_bytes did not decode as the expected NBT item structure: {0}")]
    Nbt(#[from] fastnbt::error::Error),

    #[error("item NBT has no item stack in its root 'i' list")]
    EmptyItemList,

    #[error("item NBT is missing ExtraAttributes.id, the SkyBlock item identifier")]
    MissingSkyblockId,
}

#[derive(Deserialize)]
struct NbtRoot {
    i: Vec<NbtItemStack>,
}

#[derive(Deserialize)]
struct NbtItemStack {
    #[serde(rename = "Count")]
    count: i8,
    tag: Option<NbtTag>,
}

#[derive(Deserialize)]
struct NbtTag {
    #[serde(rename = "ExtraAttributes")]
    extra_attributes: Option<ExtraAttributes>,
    display: Option<Display>,
}

#[derive(Deserialize)]
struct ExtraAttributes {
    id: Option<String>,
    #[serde(flatten)]
    rest: Value,
}

#[derive(Deserialize)]
struct Display {
    #[serde(rename = "Name")]
    name: Option<String>,
}

/// The result of decoding just an item's NBT, independent of which
/// auction (if any) it came from. [`parse_item`] calls
/// [`decode_item_bytes`] and attaches the `RawAuction`-specific fields
/// (uuid, auctioneer, starting_bid, bin, end); other callers with item
/// NBT from a different source (e.g. the `cofl` crate's historical
/// price importer, decoding a third-party API's copy of an item's NBT)
/// can call [`decode_item_bytes`] or [`decode_nbt_bytes`] directly to
/// get the exact same accuracy a live auction would have, instead of
/// reconstructing `extra_attributes` from a separate, less complete
/// field-by-field mapping.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedItem {
    pub skyblock_item_id: String,
    pub display_name: String,
    pub count: i8,
    pub extra_attributes: Option<Value>,
}

/// Decodes already-decompressed NBT bytes (no base64, no gzip) into a
/// [`DecodedItem`]. Exposed separately from [`decode_item_bytes`] for
/// callers whose NBT bytes didn't arrive gzip-wrapped the way Hypixel's
/// own `item_bytes` does.
pub fn decode_nbt_bytes(nbt_bytes: &[u8]) -> Result<DecodedItem, ParseError> {
    let root: NbtRoot = fastnbt::from_bytes(nbt_bytes)?;
    let item = root.i.into_iter().next().ok_or(ParseError::EmptyItemList)?;

    let tag = item.tag;
    let extra_attributes = tag.as_ref().and_then(|t| t.extra_attributes.as_ref());

    let skyblock_item_id = extra_attributes
        .and_then(|attrs| attrs.id.clone())
        .ok_or(ParseError::MissingSkyblockId)?;

    let display_name = tag
        .as_ref()
        .and_then(|t| t.display.as_ref())
        .and_then(|d| d.name.clone())
        .map(|name| strip_color_codes(&name))
        .unwrap_or_else(|| skyblock_item_id.clone());

    Ok(DecodedItem {
        skyblock_item_id,
        display_name,
        count: item.count,
        extra_attributes: extra_attributes.map(|attrs| attrs.rest.clone()),
    })
}

/// Decodes base64-encoded, gzip-compressed item NBT (Hypixel's
/// `item_bytes` wire format) into a [`DecodedItem`].
pub fn decode_item_bytes(item_bytes: &str) -> Result<DecodedItem, ParseError> {
    let compressed = BASE64_STANDARD.decode(item_bytes)?;

    let mut decompressed = Vec::new();
    flate2::read::GzDecoder::new(compressed.as_slice()).read_to_end(&mut decompressed)?;

    decode_nbt_bytes(&decompressed)
}

/// Decodes a single auction's `item_bytes` into a `ParsedItem`.
pub fn parse_item(raw: &RawAuction) -> Result<ParsedItem, ParseError> {
    let decoded = decode_item_bytes(&raw.item_bytes)?;

    Ok(ParsedItem {
        uuid: raw.uuid.clone(),
        auctioneer: raw.auctioneer.clone(),
        skyblock_item_id: decoded.skyblock_item_id,
        display_name: decoded.display_name,
        count: decoded.count,
        starting_bid: raw.starting_bid,
        bin: raw.bin.unwrap_or(false),
        end: raw.end,
        extra_attributes: decoded.extra_attributes,
    })
}

/// Strips Minecraft "§x" formatting codes, leaving a plain-text name.
fn strip_color_codes(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars();
    while let Some(c) = chars.next() {
        if c == '\u{00a7}' {
            chars.next();
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use fastnbt::nbt;
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;

    fn encode_item_bytes(value: &Value) -> String {
        let nbt_bytes = fastnbt::to_bytes(value).unwrap();

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&nbt_bytes).unwrap();
        let gzipped = encoder.finish().unwrap();

        BASE64_STANDARD.encode(gzipped)
    }

    fn raw_auction(item_bytes: String) -> RawAuction {
        RawAuction {
            uuid: "auction-uuid".to_string(),
            auctioneer: "seller-uuid".to_string(),
            item_name: "Hyperion".to_string(),
            starting_bid: 900_000_000,
            item_bytes,
            bin: Some(true),
            end: 1_690_003_600_000,
        }
    }

    #[test]
    fn parses_skyblock_id_and_display_name() {
        let value = nbt!({
            "i": [{
                "Count": 1i8,
                "tag": {
                    "ExtraAttributes": {
                        "id": "HYPERION",
                        "uuid": "item-uuid",
                    },
                    "display": {
                        "Name": "\u{00a7}6Hyperion",
                    },
                },
            }],
        });

        let auction = raw_auction(encode_item_bytes(&value));
        let parsed = parse_item(&auction).expect("should parse");

        assert_eq!(parsed.skyblock_item_id, "HYPERION");
        assert_eq!(parsed.display_name, "Hyperion");
        assert_eq!(parsed.count, 1);
        assert_eq!(parsed.uuid, "auction-uuid");
        assert_eq!(parsed.starting_bid, 900_000_000);
        assert!(parsed.bin);
        assert!(parsed.extra_attributes.is_some());
    }

    #[test]
    fn falls_back_to_skyblock_id_when_display_name_missing() {
        let value = nbt!({
            "i": [{
                "Count": 1i8,
                "tag": {
                    "ExtraAttributes": {
                        "id": "ENCHANTED_BOOK",
                    },
                },
            }],
        });

        let auction = raw_auction(encode_item_bytes(&value));
        let parsed = parse_item(&auction).expect("should parse");

        assert_eq!(parsed.display_name, "ENCHANTED_BOOK");
    }

    #[test]
    fn missing_extra_attributes_id_is_an_error() {
        let value = nbt!({
            "i": [{
                "Count": 1i8,
                "tag": {
                    "display": {
                        "Name": "Mystery Item",
                    },
                },
            }],
        });

        let auction = raw_auction(encode_item_bytes(&value));
        let err = parse_item(&auction).unwrap_err();

        assert!(matches!(err, ParseError::MissingSkyblockId));
    }

    #[test]
    fn empty_item_list_is_an_error() {
        let value = nbt!({ "i": [] });

        let auction = raw_auction(encode_item_bytes(&value));
        let err = parse_item(&auction).unwrap_err();

        assert!(matches!(err, ParseError::EmptyItemList));
    }

    #[test]
    fn invalid_base64_is_an_error() {
        let auction = raw_auction("not valid base64 !!!".to_string());
        let err = parse_item(&auction).unwrap_err();

        assert!(matches!(err, ParseError::Base64(_)));
    }
}
