//! Auction diff detection.
//!
//! Phase 1.3: the second stage of the pipeline. Consumes a full
//! `AuctionSnapshot` per tick and filters it down to only new or changed
//! auctions, so every downstream stage (parser, pricing, engine) does work
//! on the auctions that actually need it instead of the ~90% Hypixel
//! returns unchanged on every tick.
//!
//! No database, no Redis, no persistence across restarts: this is a
//! bounded in-memory map, self-pruning on each auction's own `end`
//! timestamp. Once `end` has passed relative to the current tick the
//! auction cannot legally reappear (sold or expired), so its entry is
//! evicted — that bounds the set by "currently live auctions" rather than
//! needing a separate TTL timer.

use common::{AuctionSnapshot, RawAuction};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SeenAuction {
    starting_bid: u64,
    end: i64,
}

/// Maintains the seen-UUID set and filters snapshots down to new/changed
/// auctions. Not thread-safe by design — one detector per ingestion
/// stream, driven sequentially from the tick loop.
#[derive(Debug, Default)]
pub struct DiffDetector {
    seen: HashMap<String, SeenAuction>,
}

impl DiffDetector {
    pub fn new() -> Self {
        Self {
            seen: HashMap::new(),
        }
    }

    /// Number of auctions currently tracked as live. Exposed for
    /// instrumentation/benchmarking (Phase 3), not used internally.
    pub fn tracked_count(&self) -> usize {
        self.seen.len()
    }

    /// Filters `snapshot.auctions` down to auctions that are new (uuid not
    /// previously seen) or changed (starting_bid or end differs from the
    /// last-seen state). Updates the seen-set and prunes entries whose
    /// `end` has passed as of this snapshot's tick.
    pub fn diff(&mut self, snapshot: AuctionSnapshot) -> Vec<RawAuction> {
        let tick = snapshot.last_updated;
        let mut changed = Vec::new();

        for auction in snapshot.auctions {
            let state = SeenAuction {
                starting_bid: auction.starting_bid,
                end: auction.end,
            };

            let is_new_or_changed = match self.seen.get(&auction.uuid) {
                Some(previous) => *previous != state,
                None => true,
            };

            if is_new_or_changed {
                self.seen.insert(auction.uuid.clone(), state);
                changed.push(auction);
            }
        }

        self.seen.retain(|_, state| state.end > tick);

        changed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn auction(uuid: &str, starting_bid: u64, end: i64) -> RawAuction {
        RawAuction {
            uuid: uuid.to_string(),
            auctioneer: "seller".to_string(),
            item_name: "Hyperion".to_string(),
            starting_bid,
            item_bytes: "base64==".to_string(),
            bin: Some(true),
            end,
        }
    }

    #[test]
    fn first_snapshot_emits_everything_as_new() {
        let mut detector = DiffDetector::new();
        let snapshot = AuctionSnapshot {
            last_updated: 1_000,
            auctions: vec![auction("a", 100, 2_000), auction("b", 200, 2_000)],
        };

        let changed = detector.diff(snapshot);
        assert_eq!(changed.len(), 2);
        assert_eq!(detector.tracked_count(), 2);
    }

    #[test]
    fn unchanged_auction_is_filtered_out_on_next_tick() {
        let mut detector = DiffDetector::new();
        detector.diff(AuctionSnapshot {
            last_updated: 1_000,
            auctions: vec![auction("a", 100, 2_000)],
        });

        let changed = detector.diff(AuctionSnapshot {
            last_updated: 1_500,
            auctions: vec![auction("a", 100, 2_000)],
        });

        assert!(changed.is_empty());
    }

    #[test]
    fn changed_starting_bid_is_emitted_again() {
        let mut detector = DiffDetector::new();
        detector.diff(AuctionSnapshot {
            last_updated: 1_000,
            auctions: vec![auction("a", 100, 2_000)],
        });

        let changed = detector.diff(AuctionSnapshot {
            last_updated: 1_500,
            auctions: vec![auction("a", 150, 2_000)],
        });

        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].starting_bid, 150);
    }

    #[test]
    fn expired_auction_is_pruned_and_does_not_bloat_the_seen_set() {
        let mut detector = DiffDetector::new();
        detector.diff(AuctionSnapshot {
            last_updated: 1_000,
            auctions: vec![auction("a", 100, 1_200)],
        });

        let changed = detector.diff(AuctionSnapshot {
            last_updated: 1_500,
            auctions: vec![auction("b", 999, 3_000)],
        });

        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].uuid, "b");
        // "a" ended (end=1_200) before this tick (1_500) and was not
        // present in this snapshot, so it should have been pruned.
        assert_eq!(detector.tracked_count(), 1);
    }

    #[test]
    fn new_auction_among_unchanged_ones_is_isolated() {
        let mut detector = DiffDetector::new();
        detector.diff(AuctionSnapshot {
            last_updated: 1_000,
            auctions: vec![auction("a", 100, 5_000), auction("b", 200, 5_000)],
        });

        let changed = detector.diff(AuctionSnapshot {
            last_updated: 1_500,
            auctions: vec![
                auction("a", 100, 5_000),
                auction("b", 200, 5_000),
                auction("c", 300, 5_000),
            ],
        });

        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].uuid, "c");
    }

    #[test]
    fn empty_snapshot_is_a_no_op() {
        let mut detector = DiffDetector::new();
        let changed = detector.diff(AuctionSnapshot {
            last_updated: 1_000,
            auctions: vec![],
        });

        assert!(changed.is_empty());
        assert_eq!(detector.tracked_count(), 0);
    }
}
