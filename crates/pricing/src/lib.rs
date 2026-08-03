//! In-memory price cache.
//!
//! Phase 1.6: the hot-path stage between fingerprinting and the (not yet
//! built) profit calculator. Maps `Fingerprint -> PriceEntry` entirely in
//! RAM, no database, no filesystem, no network hop, per claude.md's "no
//! DB on the hot path" rule.
//!
//! # Design
//!
//! The cache is sharded into [`SHARD_COUNT`] independent
//! `ArcSwap<HashMap<...>>` slices (RCU / atomic-swap, per the
//! architecture decisions in claude.md). Sharding bounds the cost of a
//! background update: writing touches only the one shard containing the
//! changed fingerprints, not the whole map.
//!
//! Reads ([`PriceCache::get`]) are wait-free: a single atomic pointer
//! load, no mutex, never blocked by a concurrent writer and never
//! blocking one. Writes ([`PriceCache::update_batch`]) use
//! [`arc_swap::ArcSwapAny::rcu`], a compare-and-swap retry loop, so
//! concurrent writers to the same shard never lose an update to a race —
//! the loser just retries against the winner's new snapshot.
//!
//! Each shard's map is keyed with [`FingerprintHasher`], an identity
//! hasher, instead of the default SipHash: a `Fingerprint` is already a
//! well-distributed 64-bit hash produced by the `fingerprint` crate, so
//! hashing it again would be pure waste on the one path in this whole
//! pipeline that's meant to run in nanoseconds.
//!
//! What a `PriceEntry.estimated_value` actually *means* (a median? a
//! trimmed mean? lowest observed BIN?) is deliberately not this crate's
//! problem — that's the profit-calculation engine's job. This cache only
//! stores and serves whatever value it's given.

use arc_swap::ArcSwap;
use fingerprint::Fingerprint;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

/// Number of independent shards. Power of two so shard selection is a
/// bitmask, not a division. 64 is small enough that per-shard maps stay
/// cheap to clone-on-write, large enough that a single-fingerprint
/// update doesn't contend with unrelated ones.
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

/// The minimum data needed for instant valuation of a fingerprinted
/// item: an estimated per-unit value plus enough context (sample size,
/// last-updated tick) for the profit calculator to judge how much to
/// trust it. Fixed-size and `Copy` — a cache hit is a memcpy, not an
/// allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceEntry {
    /// Estimated per-unit value, in coins.
    pub estimated_value: u64,
    /// How many observations this estimate is derived from.
    pub sample_size: u32,
    /// The ingestion tick (Hypixel `lastUpdated` millis) this estimate
    /// was last refreshed at, so a consumer can judge staleness.
    pub updated_at_tick: i64,
}

#[inline]
fn shard_index(fingerprint: Fingerprint) -> usize {
    (fingerprint.0 as usize) & (SHARD_COUNT - 1)
}

/// A sharded, lock-free-read, RCU-updated map from [`Fingerprint`] to
/// [`PriceEntry`]. Safe to share behind an `Arc` and call `get` from a
/// hot path while another task concurrently calls `update_batch`.
pub struct PriceCache {
    shards: Box<[ArcSwap<ShardMap>]>,
}

impl PriceCache {
    /// Builds an empty cache with all shards initialized.
    pub fn new() -> Self {
        let shards = (0..SHARD_COUNT)
            .map(|_| ArcSwap::from_pointee(ShardMap::default()))
            .collect();

        Self { shards }
    }

    /// Looks up the current price estimate for a fingerprint. Wait-free:
    /// one atomic load plus a hashmap probe with no hashing work, no
    /// allocation, no locking. Missing prices fail fast and safely by
    /// returning `None` rather than panicking, blocking, or falling back
    /// to a stale default.
    #[inline]
    pub fn get(&self, fingerprint: Fingerprint) -> Option<PriceEntry> {
        let shard = &self.shards[shard_index(fingerprint)];
        shard.load().get(&fingerprint).copied()
    }

    /// Applies a batch of `(Fingerprint, PriceEntry)` updates in the
    /// background. Groups updates by shard so each touched shard is
    /// cloned-and-swapped at most once, regardless of how many
    /// fingerprints in it changed. Never blocks a concurrent `get`, and
    /// is itself safe to call concurrently from multiple writers (via
    /// RCU compare-and-swap retry — see the module docs).
    ///
    /// Not on the hot path: this may allocate the small per-shard
    /// grouping buffers and shard-sized map clones freely. Only `get`
    /// carries the zero-allocation requirement.
    pub fn update_batch<I>(&self, updates: I)
    where
        I: IntoIterator<Item = (Fingerprint, PriceEntry)>,
    {
        let mut by_shard: Vec<Vec<(Fingerprint, PriceEntry)>> =
            (0..SHARD_COUNT).map(|_| Vec::new()).collect();

        for (fp, entry) in updates {
            by_shard[shard_index(fp)].push((fp, entry));
        }

        for (idx, entries) in by_shard.into_iter().enumerate() {
            if entries.is_empty() {
                continue;
            }

            self.shards[idx].rcu(|current| {
                let mut next = ShardMap::clone(current);
                for &(fp, entry) in &entries {
                    next.insert(fp, entry);
                }
                next
            });
        }
    }

    /// Total number of cached fingerprints across all shards. Not
    /// hot-path — sums a `load()` per shard.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.load().len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
        }
    }

    #[test]
    fn get_on_empty_cache_returns_none_without_panicking() {
        let cache = PriceCache::new();
        assert_eq!(cache.get(Fingerprint(42)), None);
    }

    #[test]
    fn update_then_get_returns_the_stored_value() {
        let cache = PriceCache::new();
        cache.update_batch([(Fingerprint(1), entry(1_000_000))]);

        assert_eq!(cache.get(Fingerprint(1)), Some(entry(1_000_000)));
    }

    #[test]
    fn updating_the_same_fingerprint_overwrites_the_previous_value() {
        let cache = PriceCache::new();
        cache.update_batch([(Fingerprint(1), entry(1_000_000))]);
        cache.update_batch([(Fingerprint(1), entry(2_000_000))]);

        assert_eq!(cache.get(Fingerprint(1)), Some(entry(2_000_000)));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn unrelated_fingerprints_do_not_interfere_across_shards() {
        let cache = PriceCache::new();

        let updates: Vec<_> = (0..500u64)
            .map(|i| (Fingerprint(i), entry(i * 1_000)))
            .collect();
        cache.update_batch(updates);

        assert_eq!(cache.len(), 500);
        for i in 0..500u64 {
            assert_eq!(cache.get(Fingerprint(i)), Some(entry(i * 1_000)));
        }
    }

    #[test]
    fn empty_batch_is_a_no_op() {
        let cache = PriceCache::new();
        cache.update_batch(std::iter::empty());
        assert!(cache.is_empty());
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
                cache.update_batch(updates);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(cache.len() as u64, writers * per_writer);
    }

    #[test]
    fn reads_never_block_on_a_concurrent_writer() {
        let cache = Arc::new(PriceCache::new());
        cache.update_batch([(Fingerprint(7), entry(500))]);

        let stop = Arc::new(AtomicBool::new(false));

        let writer = {
            let cache = Arc::clone(&cache);
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                let mut v = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    v += 1;
                    cache.update_batch([(Fingerprint(7), entry(v))]);
                }
            })
        };

        // If get() ever blocked on the writer, this loop would hang
        // instead of completing promptly.
        for _ in 0..100_000 {
            let _ = cache.get(Fingerprint(7));
        }

        stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();
    }
}

/// Not run by default (`cargo test`); run explicitly for real numbers:
/// `cargo test -p pricing --release -- --ignored --nocapture`.
#[cfg(test)]
mod bench {
    use super::*;
    use std::time::Instant;

    #[test]
    #[ignore]
    fn get_latency_under_realistic_load() {
        let cache = PriceCache::new();

        // Roughly the size of a full Hypixel auction snapshot's worth of
        // distinct fingerprints.
        let population = 50_000u64;
        let updates: Vec<_> = (0..population)
            .map(|i| {
                (
                    Fingerprint(i),
                    PriceEntry {
                        estimated_value: i * 1_000,
                        sample_size: 1,
                        updated_at_tick: 1,
                    },
                )
            })
            .collect();
        cache.update_batch(updates);

        let iterations = 5_000_000u64;
        let start = Instant::now();
        let mut hits = 0u64;
        for i in 0..iterations {
            if cache.get(Fingerprint(i % population)).is_some() {
                hits += 1;
            }
        }
        let elapsed = start.elapsed();

        assert_eq!(hits, iterations);

        let ns_per_op = elapsed.as_nanos() as f64 / iterations as f64;
        println!(
            "PriceCache::get: {iterations} lookups over {population} entries in {elapsed:?} \
             ({ns_per_op:.2} ns/op)"
        );
    }
}
