//! Item fingerprinting.
//!
//! Phase 1.5: the hot-path stage between the parser and the RAM price
//! cache. Computes a single deterministic `Fingerprint` from a
//! `ParsedItem` such that two auctions of "the same" item (same base
//! item id + same price-moving modifiers) fingerprint identically,
//! regardless of unrelated per-instance data (auction uuid, seller,
//! random item uuid, creation timestamp, ...). The price cache is keyed
//! on this `Fingerprint`.
//!
//! This is the lossy tier described in claude.md: it reads a fixed,
//! hand-picked set of `ExtraAttributes` fields known to move SkyBlock
//! auction prices, not the full NBT tree. Full normalization (every gem
//! slot's instance metadata, every cosmetic, pet-specific data) is out
//! of scope here, same as `ParsedItem::extra_attributes` is left raw by
//! the parser for this crate to interpret selectively.
//!
//! NBT tag names for `ExtraAttributes` fields come from public SkyBlock
//! NBT documentation, not a live-verified payload (this workspace has no
//! network access to `api.hypixel.net`). Each field is behind its own
//! small extractor so correcting a tag name later is a one-line change.

use fastnbt::Value;
use parser::ParsedItem;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// A deterministic 64-bit key identifying "the same item" across
/// auctions, for use as a `RAM price cache` map key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Fingerprint(pub u64);

impl std::fmt::Display for Fingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// Computes the price-relevant fingerprint of a parsed auction item.
/// Pure and synchronous: no I/O, no allocation beyond small, bounded
/// sorting buffers for the handful of unordered NBT compounds/lists
/// that can carry more than one entry (enchantments, runes, gems,
/// ability scrolls).
pub fn fingerprint(item: &ParsedItem) -> Fingerprint {
    let mut hasher = DefaultHasher::new();

    item.skyblock_item_id.hash(&mut hasher);

    let attrs = item.extra_attributes.as_ref().and_then(as_compound);

    hash_opt_str(&mut hasher, field_str(attrs, "modifier"));
    (field_int(attrs, "rarity_upgrades").unwrap_or(0) >= 1).hash(&mut hasher);
    field_int(attrs, "hot_potato_count")
        .unwrap_or(0)
        .hash(&mut hasher);
    field_int(attrs, "dungeon_item_level")
        .unwrap_or(0)
        .hash(&mut hasher);
    (field_int(attrs, "art_of_war_count").unwrap_or(0) >= 1).hash(&mut hasher);
    hash_opt_str(&mut hasher, field_str(attrs, "talisman_enrichment"));
    hash_opt_str(&mut hasher, field_str(attrs, "skin"));

    hash_sorted_str_list(&mut hasher, field_string_list(attrs, "ability_scroll"));
    hash_sorted_int_compound(&mut hasher, field_compound(attrs, "runes"));
    hash_sorted_int_compound(&mut hasher, field_compound(attrs, "enchantments"));
    hash_gems(&mut hasher, field_compound(attrs, "gems"));

    Fingerprint(hasher.finish())
}

fn as_compound(value: &Value) -> Option<&HashMap<String, Value>> {
    match value {
        Value::Compound(map) => Some(map),
        _ => None,
    }
}

fn as_int(value: &Value) -> Option<i64> {
    match value {
        Value::Byte(v) => Some(*v as i64),
        Value::Short(v) => Some(*v as i64),
        Value::Int(v) => Some(*v as i64),
        Value::Long(v) => Some(*v),
        _ => None,
    }
}

fn field<'a>(attrs: Option<&'a HashMap<String, Value>>, key: &str) -> Option<&'a Value> {
    attrs.and_then(|m| m.get(key))
}

fn field_str<'a>(attrs: Option<&'a HashMap<String, Value>>, key: &str) -> Option<&'a str> {
    match field(attrs, key) {
        Some(Value::String(s)) => Some(s.as_str()),
        _ => None,
    }
}

fn field_int(attrs: Option<&HashMap<String, Value>>, key: &str) -> Option<i64> {
    field(attrs, key).and_then(as_int)
}

fn field_compound<'a>(
    attrs: Option<&'a HashMap<String, Value>>,
    key: &str,
) -> Option<&'a HashMap<String, Value>> {
    match field(attrs, key) {
        Some(Value::Compound(m)) => Some(m),
        _ => None,
    }
}

fn field_string_list<'a>(attrs: Option<&'a HashMap<String, Value>>, key: &str) -> Vec<&'a str> {
    match field(attrs, key) {
        Some(Value::List(items)) => items
            .iter()
            .filter_map(|v| match v {
                Value::String(s) => Some(s.as_str()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Hashes an `Option<&str>` with a leading presence discriminant, so
/// `None` and `Some("")` can never collide.
fn hash_opt_str<H: Hasher>(hasher: &mut H, value: Option<&str>) {
    match value {
        Some(s) => {
            1u8.hash(hasher);
            s.hash(hasher);
        }
        None => 0u8.hash(hasher),
    }
}

/// Sorts (for determinism regardless of the source list's order) and
/// hashes a list of strings, e.g. applied ability scrolls.
fn hash_sorted_str_list<H: Hasher>(hasher: &mut H, mut items: Vec<&str>) {
    items.sort_unstable();
    (items.len() as u32).hash(hasher);
    for s in items {
        s.hash(hasher);
    }
}

/// Sorts (for determinism regardless of `HashMap` iteration order) and
/// hashes a compound whose leaf values are integers, e.g. enchantments
/// (enchant id -> level) or runes (rune id -> level). Non-integer
/// entries are skipped rather than erroring — an unexpected shape here
/// shouldn't take down fingerprinting for the whole item.
fn hash_sorted_int_compound<H: Hasher>(hasher: &mut H, map: Option<&HashMap<String, Value>>) {
    let Some(map) = map else {
        0u32.hash(hasher);
        return;
    };

    let mut entries: Vec<(&str, i64)> = map
        .iter()
        .filter_map(|(k, v)| as_int(v).map(|n| (k.as_str(), n)))
        .collect();
    entries.sort_unstable_by_key(|(k, _)| *k);

    (entries.len() as u32).hash(hasher);
    for (k, v) in entries {
        k.hash(hasher);
        v.hash(hasher);
    }
}

/// Sorts (for determinism) and hashes gemstone slots. Only each slot's
/// key and a normalized quality marker are kept — nested per-gem
/// instance metadata (e.g. a gem's own uuid) is intentionally dropped.
fn hash_gems<H: Hasher>(hasher: &mut H, map: Option<&HashMap<String, Value>>) {
    let Some(map) = map else {
        0u32.hash(hasher);
        return;
    };

    let mut keys: Vec<&str> = map.keys().map(|k| k.as_str()).collect();
    keys.sort_unstable();

    (keys.len() as u32).hash(hasher);
    for k in keys {
        k.hash(hasher);
        match &map[k] {
            Value::String(s) => {
                1u8.hash(hasher);
                s.hash(hasher);
            }
            Value::Compound(inner) => match inner.get("quality") {
                Some(Value::String(s)) => {
                    2u8.hash(hasher);
                    s.hash(hasher);
                }
                _ => 3u8.hash(hasher),
            },
            _ => 0u8.hash(hasher),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compound(pairs: Vec<(&str, Value)>) -> Value {
        let mut map = HashMap::new();
        for (k, v) in pairs {
            map.insert(k.to_string(), v);
        }
        Value::Compound(map)
    }

    fn base_item(skyblock_item_id: &str, extra_attributes: Option<Value>) -> ParsedItem {
        ParsedItem {
            uuid: "auction-uuid".to_string(),
            auctioneer: "seller-uuid".to_string(),
            skyblock_item_id: skyblock_item_id.to_string(),
            display_name: "Hyperion".to_string(),
            count: 1,
            starting_bid: 900_000_000,
            bin: true,
            end: 1_690_003_600_000,
            extra_attributes,
        }
    }

    #[test]
    fn identical_item_and_modifiers_fingerprint_the_same() {
        let a = base_item(
            "HYPERION",
            Some(compound(vec![(
                "enchantments",
                compound(vec![("ultimate_wise", Value::Int(5))]),
            )])),
        );
        let b = base_item(
            "HYPERION",
            Some(compound(vec![(
                "enchantments",
                compound(vec![("ultimate_wise", Value::Int(5))]),
            )])),
        );

        assert_eq!(fingerprint(&a), fingerprint(&b));
    }

    #[test]
    fn auction_specific_fields_do_not_affect_fingerprint() {
        let mut a = base_item("HYPERION", None);
        let mut b = base_item("HYPERION", None);

        a.uuid = "auction-a".to_string();
        b.uuid = "auction-b".to_string();
        a.auctioneer = "seller-a".to_string();
        b.auctioneer = "seller-b".to_string();
        a.starting_bid = 100;
        b.starting_bid = 900_000_000;
        a.end = 1;
        b.end = 2;
        a.display_name = "one name".to_string();
        b.display_name = "a different name".to_string();

        assert_eq!(fingerprint(&a), fingerprint(&b));
    }

    #[test]
    fn different_enchant_level_changes_fingerprint() {
        let a = base_item(
            "ASPECT_OF_THE_END",
            Some(compound(vec![(
                "enchantments",
                compound(vec![("sharpness", Value::Int(6))]),
            )])),
        );
        let b = base_item(
            "ASPECT_OF_THE_END",
            Some(compound(vec![(
                "enchantments",
                compound(vec![("sharpness", Value::Int(7))]),
            )])),
        );

        assert_ne!(fingerprint(&a), fingerprint(&b));
    }

    #[test]
    fn different_item_id_changes_fingerprint() {
        let a = base_item("HYPERION", None);
        let b = base_item("NECRON_BLADE", None);

        assert_ne!(fingerprint(&a), fingerprint(&b));
    }

    #[test]
    fn recombobulated_changes_fingerprint() {
        let plain = base_item("ASPECT_OF_THE_END", None);
        let recombobulated = base_item(
            "ASPECT_OF_THE_END",
            Some(compound(vec![("rarity_upgrades", Value::Int(1))])),
        );

        assert_ne!(fingerprint(&plain), fingerprint(&recombobulated));
    }

    #[test]
    fn hot_potato_count_changes_fingerprint() {
        let no_books = base_item("HYPERION", None);
        let ten_books = base_item(
            "HYPERION",
            Some(compound(vec![("hot_potato_count", Value::Int(10))])),
        );

        assert_ne!(fingerprint(&no_books), fingerprint(&ten_books));
    }

    #[test]
    fn absent_modifier_differs_from_empty_string_modifier() {
        let absent = base_item("HYPERION", None);
        let empty = base_item(
            "HYPERION",
            Some(compound(vec![("modifier", Value::String(String::new()))])),
        );

        assert_ne!(fingerprint(&absent), fingerprint(&empty));
    }

    #[test]
    fn enchantment_and_gem_hash_is_independent_of_map_insertion_order() {
        let a = base_item(
            "HYPERION",
            Some(compound(vec![
                (
                    "enchantments",
                    compound(vec![
                        ("sharpness", Value::Int(7)),
                        ("ultimate_wise", Value::Int(5)),
                    ]),
                ),
                (
                    "gems",
                    compound(vec![
                        ("COMBAT_0", Value::String("RUBY".to_string())),
                        ("COMBAT_1", Value::String("SAPPHIRE".to_string())),
                    ]),
                ),
            ])),
        );
        let b = base_item(
            "HYPERION",
            Some(compound(vec![
                (
                    "gems",
                    compound(vec![
                        ("COMBAT_1", Value::String("SAPPHIRE".to_string())),
                        ("COMBAT_0", Value::String("RUBY".to_string())),
                    ]),
                ),
                (
                    "enchantments",
                    compound(vec![
                        ("ultimate_wise", Value::Int(5)),
                        ("sharpness", Value::Int(7)),
                    ]),
                ),
            ])),
        );

        assert_eq!(fingerprint(&a), fingerprint(&b));
    }

    #[test]
    fn gem_quality_inside_nested_compound_affects_fingerprint() {
        let flawless = base_item(
            "HYPERION",
            Some(compound(vec![(
                "gems",
                compound(vec![(
                    "COMBAT_0",
                    compound(vec![("quality", Value::String("FLAWLESS".to_string()))]),
                )]),
            )])),
        );
        let perfect = base_item(
            "HYPERION",
            Some(compound(vec![(
                "gems",
                compound(vec![(
                    "COMBAT_0",
                    compound(vec![("quality", Value::String("PERFECT".to_string()))]),
                )]),
            )])),
        );

        assert_ne!(fingerprint(&flawless), fingerprint(&perfect));
    }

    #[test]
    fn skin_and_enrichment_and_ability_scrolls_affect_fingerprint() {
        let plain = base_item("HYPERION", None);
        let skinned = base_item(
            "HYPERION",
            Some(compound(vec![(
                "skin",
                Value::String("HYPERION_ZOMBIE".to_string()),
            )])),
        );
        let enriched = base_item(
            "HYPERION",
            Some(compound(vec![(
                "talisman_enrichment",
                Value::String("critical_chance".to_string()),
            )])),
        );
        let scrolled = base_item(
            "HYPERION",
            Some(compound(vec![(
                "ability_scroll",
                Value::List(vec![Value::String("IMPLOSION_SCROLL".to_string())]),
            )])),
        );

        let fingerprints = [
            fingerprint(&plain),
            fingerprint(&skinned),
            fingerprint(&enriched),
            fingerprint(&scrolled),
        ];

        for i in 0..fingerprints.len() {
            for j in (i + 1)..fingerprints.len() {
                assert_ne!(fingerprints[i], fingerprints[j], "indices {i} and {j}");
            }
        }
    }

    #[test]
    fn missing_extra_attributes_still_fingerprints_deterministically() {
        let a = base_item("ENCHANTED_BOOK", None);
        let b = base_item("ENCHANTED_BOOK", None);

        assert_eq!(fingerprint(&a), fingerprint(&b));
    }
}
