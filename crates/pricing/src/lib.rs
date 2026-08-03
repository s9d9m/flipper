//! In-memory, tiered price cache.
//!
//! Phase 1.6 (extended for tiered pricing): the hot-path stage between
//! fingerprinting and the profit calculator. Maps a fingerprinted item
//! to a price estimate entirely in RAM — no database, no filesystem, no
//! network hop, per claude.md's "no DB on the hot path" rule.
//!
//! # Why tiers
//!
//! An exact-fingerprint match (every modifier identical) is the most
//! accurate price signal but, for anything with real modifiers, also
//! the rarest — two independent auctions of the exact same item rarely
//! share every star/reforge/enchant/gem combination. Requiring an exact
//! match only meant most auctions, including high-value ones like
//! Necron's Handle or Hyperion, got `NoPriceData` even when a
//! perfectly usable *approximate* price was available. [`PriceCache`]
//! now holds three independent lookup tables instead of one:
//!
//! - **Tier 1 (exact)**: the full [`fingerprint::fingerprint`] — every
//!   modifier. Best accuracy, tried first, always wins when present.
//! - **Tier 2 (major modifiers)**: [`fingerprint::major_modifier_key`]
//!   — item id, reforge, recombobulated, stars, hot potato count, and
//!   the single highest-level enchantment. A coarser fallback.
//! - **Tier 3 (base item)**: `skyblock_item_id` alone. The coarsest
//!   fallback — only meant to catch obviously-underpriced listings,
//!   never a fine-grained estimate.
//!
//! [`PriceCache::get`] tries tiers in that order and returns at the
//! first hit — Tier 1 always takes priority. See `engine` for how tier
//! (and [`PriceEntry::confidence`]) feed into stricter profit/ROI bars
//! for coarser tiers, so a lower-quality signal needs a bigger margin
//! before it's trusted enough to alert on.
//!
//! # Why this stays hot-path safe
//!
//! All three tables use the same `ArcSwap`-sharded RCU structure
//! (Tier 1/2) or a single unsharded `ArcSwap` (Tier 3 — its key space,
//! distinct SkyBlock item ids, is small enough that sharding wouldn't
//! meaningfully reduce copy-on-write cost). Every read is a wait-free
//! atomic load plus a hashmap probe, no allocation, no locking — worst
//! case (nothing matches at any tier) is 3 such reads instead of 1;
//! typical case, once the cache is seeded, is a Tier‑1 hit that stops
//! at the first probe. Tier 3's key is a borrowed `&str`, never cloned.
//! See `PriceCache::get`'s doc comment for measured latency.
//!
//! # Design (RCU / sharding, unchanged from the original single-tier
//! cache)
//!
//! Sharding bounds the cost of a background update: writing touches
//! only the one shard containing the changed fingerprints, not the
//! whole map. Writes use [`arc_swap::ArcSwapAny::rcu`], a
//! compare-and-swap retry loop, so concurrent writers to the same shard
//! never lose an update to a race. Each Fingerprint-keyed shard's map
//! uses [`FingerprintHasher`], an identity hasher — a `Fingerprint` is
//! already a well-distributed 64-bit hash, so hashing it again would be
//! pure waste on a path meant to run in nanoseconds.
//!
//! What a `PriceEntry.estimated_value` actually *means* (a median? a
//! trimmed mean? lowest observed BIN?) is deliberately not this crate's
//! problem — that's the caller's (background aggregation in
//! `ingestion`/`cofl`). This cache only stores and serves whatever
//! value it's given, at whichever tier it's given for.
//!
//! # Cross-write merging (market-model session)
//!
//! `update_exact_batch`/`update_major_batch`/`update_base_batch` used
//! to do a plain `HashMap::insert` — the newest write for a key always
//! replaced whatever was cached before, no matter how well-established
//! the old value was. Combined with `ingestion`'s per-tick aggregation
//! (each BIN auction is only ever listed once, so a given fingerprint
//! typically gets exactly one live sighting per relevant tick) and
//! Tier 1 having no confidence floor of its own (see `engine`), that
//! meant a Tier‑1 `estimated_value` was routinely just *one seller's
//! current asking price*, trusted unconditionally — a single mistake
//! or lowball listing could define "market value" for whatever bought
//! against it next tick.
//!
//! [`merge_price_entry`] replaces the blind overwrite with a policy:
//! - **Same key, both `PriceSource::Live`**: merged via a sample-size-
//!   weighted running average, so `sample_size` actually reflects
//!   accumulated evidence instead of resetting every tick. The
//!   incoming value is first passed through [`dampen_outlier`] so one
//!   wild listing can shift the average by only a bounded step, not
//!   redefine it outright — see that function's doc comment.
//! - **Same key, both `PriceSource::Historical`**: the incoming entry
//!   replaces the existing one outright, deliberately *not* merged.
//!   Each `cofl::backfill` cycle already recomputes a full-population
//!   median from whatever sold-auction history it fetched; blending
//!   two already-complete aggregates together wouldn't add
//!   information; it would just mute genuine price movement between
//!   backfill cycles.
//! - **Same key, different `PriceSource`**: the incoming entry
//!   replaces the existing one outright. A live ask and a COFL sold-
//!   price median answer different questions — averaging them would
//!   produce a number that accurately describes neither.
//!
//! None of this touches the read path: `PriceEntry` is still a
//! fixed-size `Copy` struct (deliberately *not* a reservoir of raw
//! samples — see `dampen_outlier`'s doc comment for why a real
//! streaming median was considered and rejected), so `PriceCache::get`
//! is exactly as cheap as before. All the new logic runs inside the
//! background `update_*_batch` RCU closures, which already only ever
//! run once per tick, never on the per-auction hot path.

use arc_swap::ArcSwap;
use fingerprint::Fingerprint;
use std::cell::Cell;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

/// Ceiling on accumulated `sample_size` for a merged (same-source-Live)
/// entry. Four times [`Confidence::High`]'s threshold (50) — generous
/// headroom for confidence to keep meaning something as samples build
/// up, while still bounding two things: (a) the per-entry weight used
/// in [`merge_price_entry`]'s running average (see that function — an
/// uncapped weight would make old data's influence grow forever,
/// making the cache progressively *less* responsive to genuine price
/// drift the longer it runs), and (b) how large `sample_size` can grow
/// without bound in a long-running process.
const MAX_ACCUMULATED_SAMPLE_SIZE: u32 = 200;

/// How far an incoming observation is allowed to move the running
/// average in one merge, expressed as a multiple of the current
/// estimate. An incoming value more than 5x above or below the
/// existing estimate is clamped to that boundary before being folded
/// in, rather than either fully trusted (letting one troll/mistake
/// listing swing the average arbitrarily) or fully rejected (which
/// would make the cache unable to ever track a real, large price move
/// — a nerf or meta shift can legitimately cut a price by more than
/// 5x). Clamping still lets the average walk toward a new true level
/// over a few merges; it just stops one single observation from
/// getting there in one step.
const OUTLIER_DAMPING_FACTOR: u64 = 5;

/// Clamps `incoming` to within [`OUTLIER_DAMPING_FACTOR`]x of `anchor`
/// (the existing cached estimate) before it's folded into a weighted
/// average. This is the crate's answer to "prevent single outliers
/// from dominating": a bounded per-merge step instead of a true
/// median.
///
/// A real streaming/windowed median was considered and rejected: it
/// would require `PriceEntry` to carry a small reservoir of raw
/// sample values instead of one scalar, which stops being a
/// lock-step-cheap `Copy` struct and meaningfully increases the cost
/// of the `ShardMap::clone(current)` full-shard clone every
/// `update_*_batch` already does on the RCU write path. That write
/// path is background, not hot-path, but it isn't free, and speed is
/// priority #1 for this whole project — a clamped running average
/// gets most of the outlier-resistance for a fraction of the cost and
/// zero change to `PriceEntry`'s size.
#[inline]
fn dampen_outlier(anchor: u64, incoming: u64) -> u64 {
    if anchor == 0 {
        // No established estimate to compare against yet -- nothing to
        // dampen against.
        return incoming;
    }
    let lower = (anchor / OUTLIER_DAMPING_FACTOR).max(1);
    let upper = anchor.saturating_mul(OUTLIER_DAMPING_FACTOR);
    incoming.clamp(lower, upper)
}

/// What happened when [`merge_price_entry`] combined an incoming
/// observation with whatever was already cached for that key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MergeOutcome {
    /// Same key, same `PriceSource::Live` on both sides -- folded into
    /// a weighted running average.
    LiveMerged,
    /// Existing entry was replaced outright: either a cross-source
    /// change or a `PriceSource::Historical` refresh (see the module
    /// doc comment for why Historical-over-Historical still replaces
    /// rather than merges).
    Overwritten,
}

/// Combines a newly observed `PriceEntry` with whatever's already
/// cached for the same key. See the module doc comment for the full
/// policy; this is the one place that policy is implemented, shared by
/// all three tiers' write paths.
#[inline]
fn merge_price_entry(existing: PriceEntry, incoming: PriceEntry) -> (PriceEntry, MergeOutcome) {
    if existing.source != incoming.source || incoming.source == PriceSource::Historical {
        return (incoming, MergeOutcome::Overwritten);
    }

    // Both sides are PriceSource::Live: merge.
    let dampened_value = dampen_outlier(existing.estimated_value, incoming.estimated_value);

    let existing_weight = existing.sample_size.min(MAX_ACCUMULATED_SAMPLE_SIZE) as u128;
    let incoming_weight = incoming.sample_size as u128;
    let total_weight = existing_weight + incoming_weight;

    let weighted_sum = existing.estimated_value as u128 * existing_weight
        + dampened_value as u128 * incoming_weight;
    // total_weight is always >= 1: both sample_size fields are always
    // >= 1 by construction (a PriceEntry is never created for zero
    // observations), so this division is never by zero.
    let merged_value = (weighted_sum / total_weight) as u64;

    let merged_sample_size = ((existing.sample_size as u64) + (incoming.sample_size as u64))
        .min(MAX_ACCUMULATED_SAMPLE_SIZE as u64) as u32;

    let merged = PriceEntry {
        estimated_value: merged_value,
        sample_size: merged_sample_size,
        updated_at_tick: existing.updated_at_tick.max(incoming.updated_at_tick),
        source: PriceSource::Live,
    };
    (merged, MergeOutcome::LiveMerged)
}

/// Counts of what a batch of `update_*_batch` writes actually did,
/// returned so the caller can accumulate them into its own
/// diagnostics (see `ingestion/main.rs`). Not hot-path -- computed
/// once per background batch, alongside work that batch was already
/// doing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MergeStats {
    /// Updates folded into an existing entry via the same-source-Live
    /// weighted merge (see [`merge_price_entry`]).
    pub live_merged: u64,
    /// Updates that replaced an existing entry outright (cross-source
    /// change, or a `PriceSource::Historical` refresh).
    pub overwritten: u64,
    /// Updates for a key with no prior cached entry at all.
    pub inserted_new: u64,
}

impl std::ops::AddAssign for MergeStats {
    fn add_assign(&mut self, other: Self) {
        self.live_merged += other.live_merged;
        self.overwritten += other.overwritten;
        self.inserted_new += other.inserted_new;
    }
}

/// Aggregate view across every entry in a [`PriceCache`], computed by
/// walking each tier once. Not hot-path -- meant for periodic
/// diagnostics/logging (e.g. once per tick), the same cost class as
/// the `len()` family of methods this crate already exposes.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct CacheStats {
    pub total_entries: usize,
    pub total_sample_size: u64,
    /// Entries whose `confidence()` is `Confidence::High`.
    pub high_confidence_entries: usize,
}

impl CacheStats {
    /// Mean `sample_size` across every cached entry, at every tier.
    /// `0.0` on an empty cache rather than a division-by-zero panic.
    pub fn average_sample_size(&self) -> f64 {
        if self.total_entries == 0 {
            0.0
        } else {
            self.total_sample_size as f64 / self.total_entries as f64
        }
    }
}

/// Number of independent shards per Fingerprint-keyed tier. Power of
/// two so shard selection is a bitmask, not a division. 64 is small
/// enough that per-shard maps stay cheap to clone-on-write, large
/// enough that a single-fingerprint update doesn't contend with
/// unrelated ones.
const SHARD_COUNT: usize = 64;

/// Identity hasher for [`Fingerprint`] keys. `Fingerprint` is already a
/// uniformly distributed `u64` hash, so this just passes it through
/// instead of paying for a second hashing pass (SipHash) on every
/// lookup.
#[derive(Default)]
pub struct FingerprintHasher(u64);

impl Hasher for FingerprintHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!(
            "FingerprintHasher only supports u64 keys (Fingerprint); \
             got a generic byte sequence instead"
        );
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
}

type FingerprintBuildHasher = BuildHasherDefault<FingerprintHasher>;
type ShardMap = HashMap<Fingerprint, PriceEntry, FingerprintBuildHasher>;

#[inline]
fn shard_index(fingerprint: Fingerprint) -> usize {
    (fingerprint.0 as usize) & (SHARD_COUNT - 1)
}

/// The sharded, lock-free-read, RCU-updated `Fingerprint -> PriceEntry`
/// map that backs both Tier 1 (exact) and Tier 2 (major modifiers) —
/// they're structurally identical, just populated with different key
/// spaces, so the sharding/RCU mechanics are implemented once and
/// reused twice.
struct FingerprintShardedMap {
    shards: Box<[ArcSwap<ShardMap>]>,
}

impl FingerprintShardedMap {
    fn new() -> Self {
        let shards = (0..SHARD_COUNT)
            .map(|_| ArcSwap::from_pointee(ShardMap::default()))
            .collect();
        Self { shards }
    }

    #[inline]
    fn get(&self, fingerprint: Fingerprint) -> Option<PriceEntry> {
        let shard = &self.shards[shard_index(fingerprint)];
        shard.load().get(&fingerprint).copied()
    }

    fn update_batch<I>(&self, updates: I) -> MergeStats
    where
        I: IntoIterator<Item = (Fingerprint, PriceEntry)>,
    {
        let mut by_shard: Vec<Vec<(Fingerprint, PriceEntry)>> =
            (0..SHARD_COUNT).map(|_| Vec::new()).collect();

        for (fp, entry) in updates {
            by_shard[shard_index(fp)].push((fp, entry));
        }

        let mut total_stats = MergeStats::default();

        for (idx, entries) in by_shard.into_iter().enumerate() {
            if entries.is_empty() {
                continue;
            }

            // rcu()'s closure can be re-invoked on a compare-and-swap
            // retry, so stats can't just be accumulated inside it (a
            // retry would double-count). Each invocation instead
            // overwrites this Cell wholesale; once rcu() returns
            // (meaning the *last* invocation's `next` won the CAS),
            // the Cell holds exactly that winning invocation's counts.
            let shard_stats = Cell::new(MergeStats::default());

            self.shards[idx].rcu(|current| {
                let mut next = ShardMap::clone(current);
                let mut stats = MergeStats::default();
                for &(fp, entry) in &entries {
                    match next.get(&fp).copied() {
                        Some(existing) => {
                            let (merged, outcome) = merge_price_entry(existing, entry);
                            next.insert(fp, merged);
                            match outcome {
                                MergeOutcome::LiveMerged => stats.live_merged += 1,
                                MergeOutcome::Overwritten => stats.overwritten += 1,
                            }
                        }
                        None => {
                            next.insert(fp, entry);
                            stats.inserted_new += 1;
                        }
                    }
                }
                shard_stats.set(stats);
                next
            });

            total_stats += shard_stats.get();
        }

        total_stats
    }

    fn len(&self) -> usize {
        self.shards.iter().map(|s| s.load().len()).sum()
    }

    /// Folds this map's entries into `stats` -- see [`PriceCache::stats`].
    fn fold_stats(&self, stats: &mut CacheStats) {
        for shard in self.shards.iter() {
            for entry in shard.load().values() {
                stats.total_entries += 1;
                stats.total_sample_size += entry.sample_size as u64;
                if entry.confidence() == Confidence::High {
                    stats.high_confidence_entries += 1;
                }
            }
        }
    }
}

/// Where a [`PriceEntry`] came from. The profit engine applies a
/// different staleness allowance to each: `Live` prices are a snapshot
/// of this tick's own market and go stale within minutes, while
/// `Historical` prices (backfilled from COFL at startup — see the
/// `cofl` crate) represent a longer-run fair-value baseline that's
/// still legitimately useful hours or days later. Conflating the two
/// under one staleness policy would either make live prices too
/// trusting or historical prices useless immediately after import.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriceSource {
    /// Derived from an auction observed on a recent live ingestion tick.
    Live,
    /// Backfilled from historical sold-auction data (COFL) at startup.
    Historical,
}

/// How much a [`PriceEntry`] should be trusted, derived from its
/// `sample_size`. Ordered `Low < Medium < High` (derive order) so
/// callers can compare against a per-tier minimum with `<`.
///
/// Thresholds (10 / 50) are chosen to be attainable for real SkyBlock
/// items, not just mega-popular ones — a literal "200 sales = high
/// confidence" bar (an illustrative example from the task that
/// prompted this) would make High confidence unreachable for most
/// items, defeating the "many consistent flips" goal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Confidence {
    Low,
    Medium,
    High,
}

impl Confidence {
    pub fn from_sample_size(sample_size: u32) -> Self {
        if sample_size >= 50 {
            Confidence::High
        } else if sample_size >= 10 {
            Confidence::Medium
        } else {
            Confidence::Low
        }
    }
}

/// The minimum data needed for instant valuation of a fingerprinted
/// item: an estimated per-unit value plus enough context (sample size,
/// last-updated tick, source) for the profit calculator to judge how
/// much to trust it. Fixed-size and `Copy` — a cache hit is a memcpy,
/// not an allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceEntry {
    /// Estimated per-unit value, in coins.
    pub estimated_value: u64,
    /// How many observations this estimate is derived from.
    pub sample_size: u32,
    /// Despite the name (kept consistent with the rest of the pipeline,
    /// which calls Hypixel's `lastUpdated` value "tick" throughout),
    /// this is a raw Hypixel epoch-millis timestamp, not a small
    /// sequential counter — compared directly against `evaluate`'s
    /// `current_tick` argument, which is the same kind of value. See
    /// `engine::FlipThresholds` for the staleness comparison and the
    /// bug that comparing this against a tiny default once caused.
    pub updated_at_tick: i64,
    /// Where this estimate came from — see [`PriceSource`].
    pub source: PriceSource,
}

impl PriceEntry {
    /// Derived on demand from `sample_size` — a pure, allocation-free
    /// function, cheap enough to call on the hot path rather than
    /// storing (and risking staleness of) a separately-computed field.
    #[inline]
    pub fn confidence(&self) -> Confidence {
        Confidence::from_sample_size(self.sample_size)
    }
}

/// Which tier a [`PriceLookup`] was satisfied at. See the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriceTier {
    /// Every modifier matched exactly.
    Exact,
    /// Only item id + the highest-impact modifiers matched.
    MajorModifiers,
    /// Only the base item id matched — the coarsest, least trustworthy
    /// tier, meant only to catch obviously-underpriced listings.
    BaseItem,
}

/// The result of a tiered [`PriceCache::get`]: the matched entry and
/// which tier it came from. Fixed-size and `Copy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceLookup {
    pub entry: PriceEntry,
    pub tier: PriceTier,
}

/// A sharded, lock-free-read, RCU-updated, three-tier price cache. Safe
/// to share behind an `Arc` and call `get` from a hot path while
/// another task concurrently calls any `update_*_batch`. See the module
/// docs for the tiering design and hot-path guarantees.
pub struct PriceCache {
    exact: FingerprintShardedMap,
    major: FingerprintShardedMap,
    base: ArcSwap<HashMap<String, PriceEntry>>,
}

impl PriceCache {
    /// Builds an empty cache with all tiers initialized.
    pub fn new() -> Self {
        Self {
            exact: FingerprintShardedMap::new(),
            major: FingerprintShardedMap::new(),
            base: ArcSwap::from_pointee(HashMap::new()),
        }
    }

    /// Tiered lookup: tries the exact fingerprint first (best accuracy,
    /// always wins when present), then the major-modifier key, then the
    /// base item id. Wait-free at every tier — one atomic load plus a
    /// hashmap probe each, no allocation, no locking. Missing prices
    /// fail fast and safely by returning `None` rather than panicking,
    /// blocking, or falling back to a stale default.
    ///
    /// Measured (release build, single-threaded, `cargo test -p
    /// pricing --release -- --ignored --nocapture`): ~55 ns/op for a
    /// Tier‑1 hit (unchanged from the original single-tier cache,
    /// since that's still just one probe); see the crate's benchmark
    /// for the 3-probe worst-case number.
    #[inline]
    pub fn get(
        &self,
        exact: Fingerprint,
        major: Fingerprint,
        base_item_id: &str,
    ) -> Option<PriceLookup> {
        if let Some(entry) = self.get_exact(exact) {
            return Some(PriceLookup {
                entry,
                tier: PriceTier::Exact,
            });
        }
        if let Some(entry) = self.get_major(major) {
            return Some(PriceLookup {
                entry,
                tier: PriceTier::MajorModifiers,
            });
        }
        self.get_base(base_item_id).map(|entry| PriceLookup {
            entry,
            tier: PriceTier::BaseItem,
        })
    }

    #[inline]
    pub fn get_exact(&self, fingerprint: Fingerprint) -> Option<PriceEntry> {
        self.exact.get(fingerprint)
    }

    #[inline]
    pub fn get_major(&self, key: Fingerprint) -> Option<PriceEntry> {
        self.major.get(key)
    }

    #[inline]
    pub fn get_base(&self, item_id: &str) -> Option<PriceEntry> {
        self.base.load().get(item_id).copied()
    }

    /// Applies a batch of Tier‑1 (exact fingerprint) updates. See
    /// [`FingerprintShardedMap::update_batch`] and the module doc
    /// comment's merge policy — not on the hot path, may allocate
    /// freely. Returns counts of what actually happened (merged vs.
    /// overwritten vs. new), for the caller's own diagnostics.
    pub fn update_exact_batch<I>(&self, updates: I) -> MergeStats
    where
        I: IntoIterator<Item = (Fingerprint, PriceEntry)>,
    {
        self.exact.update_batch(updates)
    }

    /// Applies a batch of Tier‑2 (major-modifier key) updates.
    pub fn update_major_batch<I>(&self, updates: I) -> MergeStats
    where
        I: IntoIterator<Item = (Fingerprint, PriceEntry)>,
    {
        self.major.update_batch(updates)
    }

    /// Applies a batch of Tier‑3 (base item id) updates. Unsharded — the
    /// distinct-item-id key space is small enough that a single RCU
    /// clone-on-write stays cheap without sharding.
    pub fn update_base_batch<I>(&self, updates: I) -> MergeStats
    where
        I: IntoIterator<Item = (String, PriceEntry)>,
    {
        let updates: Vec<(String, PriceEntry)> = updates.into_iter().collect();
        if updates.is_empty() {
            return MergeStats::default();
        }

        // Same retry-safe capture pattern as FingerprintShardedMap::
        // update_batch -- see that function's comment.
        let stats_cell = Cell::new(MergeStats::default());

        self.base.rcu(|current| {
            let mut next = HashMap::clone(current);
            let mut stats = MergeStats::default();
            for (item_id, entry) in &updates {
                match next.get(item_id).copied() {
                    Some(existing) => {
                        let (merged, outcome) = merge_price_entry(existing, *entry);
                        next.insert(item_id.clone(), merged);
                        match outcome {
                            MergeOutcome::LiveMerged => stats.live_merged += 1,
                            MergeOutcome::Overwritten => stats.overwritten += 1,
                        }
                    }
                    None => {
                        next.insert(item_id.clone(), *entry);
                        stats.inserted_new += 1;
                    }
                }
            }
            stats_cell.set(stats);
            next
        });

        stats_cell.get()
    }

    pub fn exact_len(&self) -> usize {
        self.exact.len()
    }

    pub fn major_len(&self) -> usize {
        self.major.len()
    }

    pub fn base_len(&self) -> usize {
        self.base.load().len()
    }

    /// Total number of cached entries across all three tiers. Not
    /// hot-path.
    pub fn len(&self) -> usize {
        self.exact_len() + self.major_len() + self.base_len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Walks every entry, at every tier, once. Not hot-path -- meant
    /// for periodic diagnostics (see `ingestion/main.rs`'s per-tick
    /// pricing-model summary), the same cost class as `len()`.
    pub fn stats(&self) -> CacheStats {
        let mut stats = CacheStats::default();
        self.exact.fold_stats(&mut stats);
        self.major.fold_stats(&mut stats);
        for entry in self.base.load().values() {
            stats.total_entries += 1;
            stats.total_sample_size += entry.sample_size as u64;
            if entry.confidence() == Confidence::High {
                stats.high_confidence_entries += 1;
            }
        }
        stats
    }
}

impl Default for PriceCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;

    fn entry(value: u64) -> PriceEntry {
        PriceEntry {
            estimated_value: value,
            sample_size: 1,
            updated_at_tick: 1_000,
            source: PriceSource::Live,
        }
    }

    fn entry_with_samples(value: u64, sample_size: u32) -> PriceEntry {
        PriceEntry {
            estimated_value: value,
            sample_size,
            updated_at_tick: 1_000,
            source: PriceSource::Live,
        }
    }

    fn historical_entry(value: u64, sample_size: u32) -> PriceEntry {
        PriceEntry {
            estimated_value: value,
            sample_size,
            updated_at_tick: 1_000,
            source: PriceSource::Historical,
        }
    }

    #[test]
    fn confidence_thresholds() {
        assert_eq!(Confidence::from_sample_size(0), Confidence::Low);
        assert_eq!(Confidence::from_sample_size(9), Confidence::Low);
        assert_eq!(Confidence::from_sample_size(10), Confidence::Medium);
        assert_eq!(Confidence::from_sample_size(49), Confidence::Medium);
        assert_eq!(Confidence::from_sample_size(50), Confidence::High);
        assert_eq!(Confidence::from_sample_size(10_000), Confidence::High);
    }

    #[test]
    fn confidence_is_ordered() {
        assert!(Confidence::Low < Confidence::Medium);
        assert!(Confidence::Medium < Confidence::High);
    }

    #[test]
    fn price_entry_confidence_matches_sample_size() {
        assert_eq!(entry_with_samples(1, 3).confidence(), Confidence::Low);
        assert_eq!(entry_with_samples(1, 20).confidence(), Confidence::Medium);
        assert_eq!(entry_with_samples(1, 200).confidence(), Confidence::High);
    }

    #[test]
    fn get_on_empty_cache_returns_none_at_every_tier_without_panicking() {
        let cache = PriceCache::new();
        assert_eq!(
            cache.get(Fingerprint(42), Fingerprint(43), "HYPERION"),
            None
        );
    }

    #[test]
    fn exact_tier_hit_is_returned_and_tagged() {
        let cache = PriceCache::new();
        cache.update_exact_batch([(Fingerprint(1), entry(1_000_000))]);

        let lookup = cache
            .get(Fingerprint(1), Fingerprint(99), "HYPERION")
            .unwrap();
        assert_eq!(lookup.entry, entry(1_000_000));
        assert_eq!(lookup.tier, PriceTier::Exact);
    }

    #[test]
    fn falls_back_to_major_modifier_tier_when_exact_misses() {
        let cache = PriceCache::new();
        cache.update_major_batch([(Fingerprint(2), entry(500_000))]);

        let lookup = cache
            .get(Fingerprint(1), Fingerprint(2), "HYPERION")
            .unwrap();
        assert_eq!(lookup.entry, entry(500_000));
        assert_eq!(lookup.tier, PriceTier::MajorModifiers);
    }

    #[test]
    fn falls_back_to_base_item_tier_when_exact_and_major_miss() {
        let cache = PriceCache::new();
        cache.update_base_batch([("HYPERION".to_string(), entry(250_000))]);

        let lookup = cache
            .get(Fingerprint(1), Fingerprint(2), "HYPERION")
            .unwrap();
        assert_eq!(lookup.entry, entry(250_000));
        assert_eq!(lookup.tier, PriceTier::BaseItem);
    }

    #[test]
    fn exact_tier_always_wins_when_all_three_have_data() {
        let cache = PriceCache::new();
        cache.update_exact_batch([(Fingerprint(1), entry(1_000_000))]);
        cache.update_major_batch([(Fingerprint(2), entry(500_000))]);
        cache.update_base_batch([("HYPERION".to_string(), entry(250_000))]);

        let lookup = cache
            .get(Fingerprint(1), Fingerprint(2), "HYPERION")
            .unwrap();
        assert_eq!(lookup.tier, PriceTier::Exact);
        assert_eq!(lookup.entry.estimated_value, 1_000_000);
    }

    #[test]
    fn major_tier_wins_over_base_tier_when_both_have_data() {
        let cache = PriceCache::new();
        cache.update_major_batch([(Fingerprint(2), entry(500_000))]);
        cache.update_base_batch([("HYPERION".to_string(), entry(250_000))]);

        let lookup = cache
            .get(Fingerprint(1), Fingerprint(2), "HYPERION")
            .unwrap();
        assert_eq!(lookup.tier, PriceTier::MajorModifiers);
    }

    #[test]
    fn updating_the_same_exact_fingerprint_with_the_same_live_source_merges_not_overwrites() {
        // market-model session: same key, both PriceSource::Live ->
        // weighted average, sample_size accumulates. This replaces the
        // old blind-overwrite behavior (see
        // cross_source_update_still_overwrites_outright and
        // historical_refresh_still_overwrites_outright below for the
        // cases that *do* still replace outright).
        let cache = PriceCache::new();
        cache.update_exact_batch([(Fingerprint(1), entry(1_000_000))]);
        let stats = cache.update_exact_batch([(Fingerprint(1), entry(2_000_000))]);

        // equal weight (sample_size 1 each) -> simple average.
        assert_eq!(
            cache.get_exact(Fingerprint(1)),
            Some(entry_with_samples(1_500_000, 2))
        );
        assert_eq!(cache.exact_len(), 1);
        assert_eq!(stats.live_merged, 1);
        assert_eq!(stats.overwritten, 0);
        assert_eq!(stats.inserted_new, 0);
    }

    #[test]
    fn updating_the_same_base_item_id_with_the_same_live_source_merges_not_overwrites() {
        let cache = PriceCache::new();
        cache.update_base_batch([("HYPERION".to_string(), entry(1_000_000))]);
        let stats = cache.update_base_batch([("HYPERION".to_string(), entry(2_000_000))]);

        assert_eq!(
            cache.get_base("HYPERION"),
            Some(entry_with_samples(1_500_000, 2))
        );
        assert_eq!(cache.base_len(), 1);
        assert_eq!(stats.live_merged, 1);
    }

    #[test]
    fn cross_source_update_still_overwrites_outright() {
        // A fresh Live sighting replacing a Historical entry (or vice
        // versa) is never blended -- a live ask and a COFL sold-price
        // median answer different questions.
        let cache = PriceCache::new();
        cache.update_exact_batch([(Fingerprint(1), historical_entry(1_000_000, 40))]);
        let stats = cache.update_exact_batch([(Fingerprint(1), entry(2_000_000))]);

        assert_eq!(cache.get_exact(Fingerprint(1)), Some(entry(2_000_000)));
        assert_eq!(stats.live_merged, 0);
        assert_eq!(stats.overwritten, 1);
    }

    #[test]
    fn historical_refresh_still_overwrites_outright() {
        // Same PriceSource::Historical on both sides still replaces,
        // not merges: each cofl::backfill cycle already recomputes a
        // full-population median, so blending two complete aggregates
        // would just mute real price movement between cycles.
        let cache = PriceCache::new();
        cache.update_exact_batch([(Fingerprint(1), historical_entry(1_000_000, 40))]);
        let stats = cache.update_exact_batch([(Fingerprint(1), historical_entry(3_000_000, 60))]);

        assert_eq!(
            cache.get_exact(Fingerprint(1)),
            Some(historical_entry(3_000_000, 60))
        );
        assert_eq!(stats.live_merged, 0);
        assert_eq!(stats.overwritten, 1);
    }

    #[test]
    fn merge_weights_by_existing_sample_size_not_a_plain_average() {
        // A well-established estimate (sample_size 9) should move only
        // a little when a single new observation (sample_size 1)
        // comes in, not jump halfway to it.
        let cache = PriceCache::new();
        cache.update_exact_batch([(Fingerprint(1), entry_with_samples(1_000_000, 9))]);
        cache.update_exact_batch([(Fingerprint(1), entry_with_samples(2_000_000, 1))]);

        // (1_000_000*9 + 2_000_000*1) / 10 = 1_100_000
        let result = cache.get_exact(Fingerprint(1)).unwrap();
        assert_eq!(result.estimated_value, 1_100_000);
        assert_eq!(result.sample_size, 10);
    }

    #[test]
    fn a_single_wild_observation_is_dampened_not_fully_trusted() {
        // Incoming is 100x the established estimate -- far past the 5x
        // damping factor -- so it should be clamped to 5x before being
        // averaged in, not pull the estimate anywhere near the raw
        // 100_000_000 value.
        let cache = PriceCache::new();
        cache.update_exact_batch([(Fingerprint(1), entry_with_samples(1_000_000, 9))]);
        cache.update_exact_batch([(Fingerprint(1), entry_with_samples(100_000_000, 1))]);

        // dampened incoming = 1_000_000 * 5 = 5_000_000.
        // (1_000_000*9 + 5_000_000*1) / 10 = 1_400_000.
        let result = cache.get_exact(Fingerprint(1)).unwrap();
        assert_eq!(result.estimated_value, 1_400_000);
        assert!(
            result.estimated_value < 10_000_000,
            "a single 100x outlier must not come close to dominating the average"
        );
    }

    #[test]
    fn accumulated_sample_size_is_capped() {
        let cache = PriceCache::new();
        cache.update_exact_batch([(Fingerprint(1), entry_with_samples(1_000_000, 199))]);
        cache.update_exact_batch([(Fingerprint(1), entry_with_samples(1_000_000, 50))]);

        // 199 + 50 = 249, capped to MAX_ACCUMULATED_SAMPLE_SIZE (200).
        assert_eq!(cache.get_exact(Fingerprint(1)).unwrap().sample_size, 200);
    }

    #[test]
    fn a_capped_entry_still_responds_to_new_observations() {
        // Once sample_size is capped, the *reported* count stops
        // growing, but the merge weight used internally is also capped
        // (not the true, ever-growing historical count) -- otherwise a
        // long-running entry would become permanently unresponsive to
        // real price movement, since each new observation's weight
        // would shrink toward zero forever. A single new observation
        // should still move a capped entry by a meaningful amount.
        let cache = PriceCache::new();
        cache.update_exact_batch([(Fingerprint(1), entry_with_samples(1_000_000, 200))]);
        cache.update_exact_batch([(Fingerprint(1), entry_with_samples(2_000_000, 1))]);

        // (1_000_000*200 + 2_000_000*1) / 201 ≈ 1_004_975
        let result = cache.get_exact(Fingerprint(1)).unwrap();
        assert!(
            result.estimated_value > 1_000_000,
            "a capped entry must still move in response to new data, got {}",
            result.estimated_value
        );
    }

    #[test]
    fn unrelated_fingerprints_do_not_interfere_across_shards() {
        let cache = PriceCache::new();

        let updates: Vec<_> = (0..500u64)
            .map(|i| (Fingerprint(i), entry(i * 1_000)))
            .collect();
        cache.update_exact_batch(updates);

        assert_eq!(cache.exact_len(), 500);
        for i in 0..500u64 {
            assert_eq!(cache.get_exact(Fingerprint(i)), Some(entry(i * 1_000)));
        }
    }

    #[test]
    fn empty_batches_are_a_no_op_at_every_tier() {
        let cache = PriceCache::new();
        cache.update_exact_batch(std::iter::empty());
        cache.update_major_batch(std::iter::empty());
        cache.update_base_batch(std::iter::empty());
        assert!(cache.is_empty());
    }

    #[test]
    fn base_item_lookup_does_not_require_an_owned_string() {
        let cache = PriceCache::new();
        cache.update_base_batch([("HYPERION".to_string(), entry(1))]);

        // Borrowed &str, not String -- proves the read side needs no
        // allocation to query the base tier.
        let owned = String::from("HYPERION");
        let borrowed: &str = &owned;
        assert_eq!(cache.get_base(borrowed), Some(entry(1)));
    }

    #[test]
    fn concurrent_updates_to_the_same_shard_do_not_lose_data() {
        // Many fingerprints deliberately hashed into the same shard
        // (same low SHARD_COUNT bits, i.e. same value mod SHARD_COUNT
        // via the bitmask), updated concurrently from multiple threads.
        // The rcu() retry loop must ensure every update lands even
        // though they all race on one shard's ArcSwap.
        let cache = Arc::new(PriceCache::new());
        let writers = 8;
        let per_writer = 50u64;

        let mut handles = Vec::new();
        for w in 0..writers {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                let updates: Vec<_> = (0..per_writer)
                    .map(|i| {
                        // fingerprint low bits fixed at 0 so every one of
                        // these lands in the same shard; high bits vary
                        // so each is a distinct map key.
                        let fp = Fingerprint((w * per_writer + i) << 16);
                        (fp, entry(w * per_writer + i))
                    })
                    .collect();
                cache.update_exact_batch(updates);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(cache.exact_len() as u64, writers * per_writer);
    }

    #[test]
    fn reads_never_block_on_a_concurrent_writer() {
        let cache = Arc::new(PriceCache::new());
        cache.update_exact_batch([(Fingerprint(7), entry(500))]);

        let stop = Arc::new(AtomicBool::new(false));

        let writer = {
            let cache = Arc::clone(&cache);
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                let mut v = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    v += 1;
                    cache.update_exact_batch([(Fingerprint(7), entry(v))]);
                }
            })
        };

        // If get() ever blocked on the writer, this loop would hang
        // instead of completing promptly.
        for _ in 0..100_000 {
            let _ = cache.get(Fingerprint(7), Fingerprint(0), "HYPERION");
        }

        stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();
    }

    #[test]
    fn stats_on_empty_cache_is_all_zero_without_panicking() {
        let cache = PriceCache::new();
        let stats = cache.stats();
        assert_eq!(stats.total_entries, 0);
        assert_eq!(stats.total_sample_size, 0);
        assert_eq!(stats.high_confidence_entries, 0);
        assert_eq!(stats.average_sample_size(), 0.0);
    }

    #[test]
    fn stats_aggregates_across_all_three_tiers() {
        let cache = PriceCache::new();
        cache.update_exact_batch([(Fingerprint(1), entry_with_samples(1_000_000, 5))]);
        cache.update_major_batch([(Fingerprint(2), entry_with_samples(2_000_000, 15))]);
        cache.update_base_batch([("HYPERION".to_string(), entry_with_samples(3_000_000, 50))]);

        let stats = cache.stats();
        assert_eq!(stats.total_entries, 3);
        assert_eq!(stats.total_sample_size, 5 + 15 + 50);
        // Only the base-tier entry (sample_size 50) reaches High.
        assert_eq!(stats.high_confidence_entries, 1);
        assert!((stats.average_sample_size() - (70.0 / 3.0)).abs() < 1e-9);
    }

    #[test]
    fn merge_stats_distinguish_new_inserts_from_merges_and_overwrites() {
        let cache = PriceCache::new();

        let first = cache.update_exact_batch([(Fingerprint(1), entry(1_000_000))]);
        assert_eq!(
            first,
            MergeStats {
                live_merged: 0,
                overwritten: 0,
                inserted_new: 1,
            }
        );

        let second = cache.update_exact_batch([(Fingerprint(1), entry(2_000_000))]);
        assert_eq!(
            second,
            MergeStats {
                live_merged: 1,
                overwritten: 0,
                inserted_new: 0,
            }
        );
    }
}

/// Not run by default (`cargo test`); run explicitly for real numbers:
/// `cargo test -p pricing --release -- --ignored --nocapture`.
#[cfg(test)]
mod bench {
    use super::*;
    use std::time::Instant;

    fn seeded_entry(i: u64) -> PriceEntry {
        PriceEntry {
            estimated_value: i * 1_000,
            sample_size: 1,
            updated_at_tick: 1,
            source: PriceSource::Live,
        }
    }

    #[test]
    #[ignore]
    fn get_latency_tier1_hit() {
        let cache = PriceCache::new();

        // Roughly the size of a full Hypixel auction snapshot's worth of
        // distinct fingerprints.
        let population = 50_000u64;
        let updates: Vec<_> = (0..population)
            .map(|i| (Fingerprint(i), seeded_entry(i)))
            .collect();
        cache.update_exact_batch(updates);

        let iterations = 5_000_000u64;
        let start = Instant::now();
        let mut hits = 0u64;
        for i in 0..iterations {
            let fp = Fingerprint(i % population);
            if cache.get(fp, fp, "HYPERION").is_some() {
                hits += 1;
            }
        }
        let elapsed = start.elapsed();

        assert_eq!(hits, iterations);

        let ns_per_op = elapsed.as_nanos() as f64 / iterations as f64;
        println!(
            "PriceCache::get (Tier 1 hit): {iterations} lookups over {population} entries in \
             {elapsed:?} ({ns_per_op:.2} ns/op)"
        );
    }

    #[test]
    #[ignore]
    fn get_latency_worst_case_miss_at_every_tier() {
        let cache = PriceCache::new();

        // Populate all three tiers with *unrelated* keys so every
        // lookup below genuinely misses at each tier in turn before
        // returning None -- the worst case for get().
        let population = 50_000u64;
        let exact: Vec<_> = (0..population)
            .map(|i| (Fingerprint(i), seeded_entry(i)))
            .collect();
        cache.update_exact_batch(exact);
        let major: Vec<_> = (0..population)
            .map(|i| (Fingerprint(i + 1_000_000), seeded_entry(i)))
            .collect();
        cache.update_major_batch(major);
        cache.update_base_batch([("SOME_OTHER_ITEM".to_string(), seeded_entry(1))]);

        let iterations = 5_000_000u64;
        let start = Instant::now();
        let mut misses = 0u64;
        for i in 0..iterations {
            let fp = Fingerprint(i % population + 2_000_000);
            if cache.get(fp, fp, "HYPERION").is_none() {
                misses += 1;
            }
        }
        let elapsed = start.elapsed();

        assert_eq!(misses, iterations);

        let ns_per_op = elapsed.as_nanos() as f64 / iterations as f64;
        println!(
            "PriceCache::get (miss at all 3 tiers): {iterations} lookups in {elapsed:?} \
             ({ns_per_op:.2} ns/op)"
        );
    }
}
