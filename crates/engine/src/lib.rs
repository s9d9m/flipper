//! Profit calculation engine.
//!
//! Phase 1.7: the hot-path stage between the RAM price cache and flip
//! detection/notification. `evaluate()` is pure and synchronous — no
//! I/O, no allocation, no locking, no `.await` point — so it's safe to
//! call directly inline on the sniper path.
//!
//! This crate deliberately does not depend on `pricing::PriceCache` or
//! `fingerprint::Fingerprint`: the caller does the cache lookup
//! (`PriceCache::get`, already wait-free) and passes in the resulting
//! `Option<PriceLookup>`. That keeps this crate trivially testable (no
//! need to construct a whole cache per test) and keeps the dependency
//! graph a strict pipeline: parser -> fingerprint -> pricing -> engine,
//! with engine only needing the *types* it consumes from pricing.
//!
//! # Tiered pricing and confidence
//!
//! `pricing::PriceCache::get` returns a [`pricing::PriceLookup`] that
//! carries both the [`pricing::PriceEntry`] and which tier it came from
//! (`Exact`, `MajorModifiers`, or `BaseItem`). Exact fingerprint matches
//! (same item, same reforge, same stars, same enchants, same
//! everything) are the most trustworthy signal available *when the
//! price behind them is itself trustworthy* — see the confidence-
//! protection session note below for why that's not the same as "no
//! checks at all." The two fallback tiers are coarser estimates by
//! construction — `MajorModifiers` ignores gems and minor enchants,
//! `BaseItem` ignores every modifier including stars and reforges — so
//! `evaluate()` compensates two ways: it requires a higher minimum
//! confidence (sample size) before trusting either fallback tier at
//! all, and it multiplies the ROI bar those tiers must clear, so a
//! coarse price still has to look *obviously* underpriced, not just
//! nominally profitable, before it's reported. This is what keeps Tier 3
//! ("base item only") limited to the "obvious underpriced opportunities"
//! the task description asks for, without shutting Tier 2/3 out of the
//! "many consistent flips" goal entirely.
//!
//! # Exact tier is not source-blind (confidence-protection session)
//!
//! An exact fingerprint match answers "is this the same item" with
//! total confidence — it says nothing about whether the *price*
//! attached to it is trustworthy. A live sighting reflects one or a
//! few sellers' current asking prices; a `pricing::PriceSource::
//! Historical` (COFL) entry reflects a median of actual completed
//! sales. Live-sourced Tier‑1 entries used to get exactly the same
//! treatment as Historical ones (no confidence floor, no ROI
//! multiplier) purely because they matched at Tier 1 — a live run
//! confirmed this in practice: flips dominated by `tier Exact / Live`
//! backed by cache entries with an average `sample_size` of ~6, well
//! below what the *coarser* tiers already require.
//!
//! `min_live_exact_confidence` and `live_exact_roi_multiplier` close
//! that gap *only* for `(PriceTier::Exact, PriceSource::Live)`.
//! `(PriceTier::Exact, PriceSource::Historical)` gets neither — no
//! floor, `1.0x` multiplier, unchanged — preserving "COFL historical
//! pricing highest trust" literally, not just in spirit. Tier 2/3 are
//! untouched regardless of source; this is scoped to the exact gap a
//! live run demonstrated, not a general threshold rewrite.
//!
//! # Ordering requirement on the caller
//!
//! An auction must be evaluated against price data collected *before*
//! this tick, never against a price derived from itself or its
//! same-tick siblings. If a caller feeds "cheapest BIN observed this
//! tick" into the price cache and then evaluates that same tick's
//! auctions against the just-updated cache, the cheapest listing in the
//! batch would be compared against a "market price" it just set itself
//! — making the best deal look like zero profit while merely-pricier
//! same-tick siblings look like fake losses. `evaluate()` has no way to
//! enforce this itself (it only sees one auction + one price at a
//! time), so the caller must look up prices *before* folding this
//! tick's observations into the cache.
//!
//! # Fee schedule caveat
//!
//! [`FeeSchedule`]'s default (flat 1%, no floor) is a placeholder, not a
//! verified current Hypixel Auction House tax table — this workspace has
//! no network access to confirm it (same situation as the fingerprint
//! crate's NBT tag names). It's one config value to correct once
//! verified, not a redesign.
//!
//! # `tick` is epoch-millis, not a small counter (bug fixed here)
//!
//! `current_tick`/`PriceEntry::updated_at_tick` are Hypixel's raw
//! `lastUpdated` value — epoch milliseconds, passed straight through the
//! whole pipeline as `tick`. An earlier version of
//! `FlipThresholds::default()` set `max_price_age_ticks: 5`, which reads
//! as "5 ticks" but was actually being compared against millisecond
//! deltas — i.e. 5 *milliseconds*. Since Hypixel's cache refreshes every
//! ~60,000ms, that meant no cached price could ever survive from one
//! tick to the next; every live-fed price was already stale by the time
//! it could possibly be reused. The default is now denominated correctly
//! (see [`FlipThresholds`]). This is restoring a broken guard to work as
//! documented, not loosening it.

use parser::ParsedItem;
use pricing::{Confidence, PriceLookup, PriceSource, PriceTier};

/// Resale tax model applied to `estimated_value` when computing net
/// proceeds. See the module-level fee schedule caveat.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FeeSchedule {
    /// Fraction of the resale price taken as tax, e.g. `0.01` for 1%.
    pub tax_rate: f64,
    /// Tax floor in coins, applied even on very cheap resells.
    pub minimum_tax: u64,
}

impl FeeSchedule {
    /// Tax owed on a resale at `resale_price` coins.
    #[inline]
    pub fn tax_on(&self, resale_price: u64) -> u64 {
        let percentage = (resale_price as f64 * self.tax_rate).round();
        // Negative tax_rate isn't a supported configuration; a `round()`
        // result outside u64 range would only happen with a
        // pathological resale_price/tax_rate combination far beyond any
        // real SkyBlock price, so clamp defensively rather than panic.
        let percentage = percentage.clamp(0.0, u64::MAX as f64) as u64;
        percentage.max(self.minimum_tax)
    }
}

impl Default for FeeSchedule {
    fn default() -> Self {
        Self {
            tax_rate: 0.01,
            minimum_tax: 0,
        }
    }
}

/// Configurable pass/fail bar plus data-quality guards. Defaults are
/// deliberately conservative placeholders — claude.md's Phase 2 website
/// already anticipates user-configurable min-profit/min-ROI settings,
/// which is where these should eventually come from instead of
/// `Default`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FlipThresholds {
    /// Minimum expected profit, in coins, to count as a flip.
    pub min_profit: i64,
    /// Minimum ROI, as a percentage (e.g. `10.0` for 10%).
    pub min_roi_percent: f64,
    /// Minimum `PriceEntry.sample_size` to trust a cached price at all.
    pub min_sample_size: u32,
    /// Maximum age, in milliseconds (despite the name — see the module
    /// doc comment), of a `PriceSource::Live` cached price before it's
    /// considered stale and skipped rather than trusted.
    pub max_price_age_ticks: i64,
    /// Maximum age, in milliseconds, of a `PriceSource::Historical`
    /// cached price (backfilled from COFL) before it's considered stale.
    /// Deliberately much larger than `max_price_age_ticks`: historical
    /// data represents a longer-run fair-value baseline, not a live
    /// snapshot, so it stays useful far longer than a live observation
    /// does.
    pub max_historical_price_age_ticks: i64,
    /// ROI above this percentage is treated as implausible — more
    /// likely a bad cache entry (fingerprint collision, a troll listing
    /// skewing the cheapest-BIN sample) than a real flip — and rejected
    /// rather than reported, to avoid eroding trust with false
    /// positives.
    pub max_plausible_roi_percent: f64,
    /// Multiplier applied to `min_roi_percent` (never `min_profit`) when
    /// the price came from Tier 2 (major-modifier fallback) instead of
    /// an exact fingerprint match. `min_profit` is left alone
    /// deliberately: it's what makes the flip worth clicking at all
    /// ("coins/hour"), while the ROI bar is what filters out noise from
    /// the coarser estimate. Values above `1.0` demand a bigger margin
    /// of safety without shutting Tier 2 out of the "many consistent
    /// flips" goal.
    pub major_modifier_roi_multiplier: f64,
    /// Same idea as `major_modifier_roi_multiplier`, applied when
    /// falling all the way back to Tier 3 (base item id only, no
    /// modifiers at all). Substantially larger: Tier 3 can't tell a bare
    /// item from a maxed-out one, so only auctions that are obviously,
    /// dramatically underpriced should pass — matching the task's own
    /// phrasing ("only for obvious underpriced opportunities").
    pub base_item_roi_multiplier: f64,
    /// Minimum confidence a Tier 2 (major-modifier) price must have
    /// before it's trusted at all, independent of ROI. Below this, the
    /// price estimate itself isn't reliable enough to act on regardless
    /// of how good the deal looks.
    pub min_major_modifier_confidence: Confidence,
    /// Minimum confidence a Tier 3 (base-item) price must have. Kept at
    /// `High` by default: base-item-only pricing is the least
    /// discriminating signal available, so it needs the deepest sample
    /// before it's trusted for real money.
    pub min_base_item_confidence: Confidence,
    /// (confidence-protection session) Minimum confidence a
    /// `(PriceTier::Exact, PriceSource::Live)` price must have before
    /// it's trusted at all. Does **not** apply to
    /// `(PriceTier::Exact, PriceSource::Historical)` — COFL medians
    /// stay unconditionally trusted, as before. See the module doc
    /// comment for why an exact fingerprint match alone doesn't imply
    /// a trustworthy *price*.
    pub min_live_exact_confidence: Confidence,
    /// Multiplier applied to `min_roi_percent` when the price is
    /// `(PriceTier::Exact, PriceSource::Live)` and has cleared
    /// `min_live_exact_confidence`. `1.0` (no multiplier) for
    /// `(PriceTier::Exact, PriceSource::Historical)`, exactly as
    /// before.
    pub live_exact_roi_multiplier: f64,
}

impl Default for FlipThresholds {
    fn default() -> Self {
        Self {
            min_profit: 100_000,
            min_roi_percent: 10.0,
            min_sample_size: 1,
            // 3 minutes: Hypixel's cache refreshes roughly every 60s
            // (per claude.md), so this gives a live-fed price a few
            // ticks of slack to be reused before it's considered stale.
            // See the module doc comment for the bug this default fixes
            // (it used to be the nonsensical literal value `5`, i.e. 5
            // milliseconds).
            max_price_age_ticks: 180_000,
            // 30 days: historical data is a slower-moving baseline, not
            // a live snapshot.
            max_historical_price_age_ticks: 30 * 24 * 60 * 60 * 1000,
            max_plausible_roi_percent: 1_000.0,
            // Tier 2 still has to see roughly 1.5x the flat ROI bar --
            // enough to filter out estimate noise from ignored gems/minor
            // enchants without pricing Tier 2 out of ordinary flips.
            major_modifier_roi_multiplier: 1.5,
            // Tier 3 has to look dramatically underpriced (3x the flat
            // bar) before it's worth acting on with no modifier info at
            // all.
            base_item_roi_multiplier: 3.0,
            min_major_modifier_confidence: Confidence::Medium,
            min_base_item_confidence: Confidence::High,
            // Same floor as Tier 2 (10+ accumulated samples): a Live
            // Exact match is precise about *which* item, but a live ask
            // needs the same evidence bar as a coarser-but-still-Live
            // signal before it's trusted for real money.
            min_live_exact_confidence: Confidence::Medium,
            // Deliberately smaller than major_modifier_roi_multiplier
            // (1.5): a Live-Exact match that has cleared its confidence
            // floor is still a strictly more precise item-identity
            // match than Major (which ignores gems/minor enchants
            // regardless of source), so it earns a lighter margin --
            // but not 1.0, since it's still an ask-price signal, not a
            // sold-price one. Keeps a meaningful ordering: Historical-
            // Exact (1.0x) < Live-Exact-confident (1.2x) < Major (1.5x)
            // < Base (3.0x).
            live_exact_roi_multiplier: 1.2,
        }
    }
}

/// The computed numbers for an auction that had usable price data.
/// Fixed-size and `Copy` — producing one is a stack write, not an
/// allocation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProfitCalculation {
    pub estimated_value: u64,
    pub buy_price: u64,
    pub tax: u64,
    /// Signed: a bad deal has negative expected profit, not a panic or
    /// a clamped-to-zero lie.
    pub expected_profit: i64,
    pub roi_percent: f64,
    pub sample_size: u32,
    pub price_age_ticks: i64,
    /// Which pricing tier the price came from. Downstream consumers
    /// (logging, notifications) use this to show how the estimate was
    /// derived; `evaluate()` itself has already applied the
    /// tier-appropriate confidence gate and ROI multiplier by the time
    /// this is produced.
    pub tier: PriceTier,
}

/// The result of evaluating one auction. Every reason an auction isn't
/// a reported flip is a distinct, named variant rather than a bare
/// `None`/`bool`, so a caller (or a future notification stage) can see
/// *why*, not just that it wasn't one.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FlipVerdict {
    /// Passed every threshold and guard: a flip worth reporting.
    Flip(ProfitCalculation),
    /// Numbers were computable but didn't clear the configured
    /// min-profit/min-ROI bar.
    BelowThreshold(ProfitCalculation),
    /// Not a BIN auction — not instantly buyable, so not evaluable as a
    /// sniper-mode flip at all.
    NotBin,
    /// No cached price for this item yet.
    NoPriceData,
    /// Cached price exists but is older than `max_price_age_ticks`.
    StalePrice { age_ticks: i64 },
    /// Cached price exists but is built from too few observations to
    /// trust — either below `min_sample_size` outright, below the
    /// tier-specific confidence floor (`min_major_modifier_confidence` /
    /// `min_base_item_confidence`) for a Tier 2/3 fallback price, or
    /// below `min_live_exact_confidence` for a
    /// `(PriceTier::Exact, PriceSource::Live)` price (confidence-
    /// protection session).
    InsufficientSampleSize { sample_size: u32 },
    /// `starting_bid` or `estimated_value` was zero — no meaningful
    /// ratio can be computed.
    InvalidPriceData,
    /// Computed ROI exceeded `max_plausible_roi_percent` — rejected as
    /// more likely bad data than a real opportunity.
    ImplausibleRoi { roi_percent: f64 },
}

impl FlipVerdict {
    /// Convenience for callers that just want to know if this is worth
    /// alerting on.
    pub fn is_flip(&self) -> bool {
        matches!(self, FlipVerdict::Flip(_))
    }
}

/// Evaluates one parsed auction against an already-looked-up cached
/// price. Pure, synchronous, allocation-free. See the module docs for
/// the ordering requirement on `price` (must be looked up before this
/// tick's own observations are folded into the cache) and the fee
/// schedule caveat.
pub fn evaluate(
    item: &ParsedItem,
    price: Option<PriceLookup>,
    current_tick: i64,
    fees: &FeeSchedule,
    thresholds: &FlipThresholds,
) -> FlipVerdict {
    if !item.bin {
        return FlipVerdict::NotBin;
    }

    let Some(PriceLookup { entry, tier }) = price else {
        return FlipVerdict::NoPriceData;
    };

    if entry.sample_size < thresholds.min_sample_size {
        return FlipVerdict::InsufficientSampleSize {
            sample_size: entry.sample_size,
        };
    }

    // (confidence-protection session) Keyed on (tier, source), not tier
    // alone: an exact fingerprint match says "same item," not "trusted
    // price" -- a Live sighting and a Historical (COFL) median carry
    // different evidence even at identical tiers. See the module doc
    // comment.
    let min_tier_confidence = match (tier, entry.source) {
        (PriceTier::Exact, PriceSource::Live) => Some(thresholds.min_live_exact_confidence),
        (PriceTier::Exact, PriceSource::Historical) => None,
        (PriceTier::MajorModifiers, _) => Some(thresholds.min_major_modifier_confidence),
        (PriceTier::BaseItem, _) => Some(thresholds.min_base_item_confidence),
    };
    if let Some(required) = min_tier_confidence {
        if entry.confidence() < required {
            return FlipVerdict::InsufficientSampleSize {
                sample_size: entry.sample_size,
            };
        }
    }

    let age_ticks = current_tick.saturating_sub(entry.updated_at_tick).max(0);
    let staleness_limit = match entry.source {
        PriceSource::Live => thresholds.max_price_age_ticks,
        PriceSource::Historical => thresholds.max_historical_price_age_ticks,
    };
    if age_ticks > staleness_limit {
        return FlipVerdict::StalePrice { age_ticks };
    }

    if item.starting_bid == 0 || entry.estimated_value == 0 {
        return FlipVerdict::InvalidPriceData;
    }

    let profit = compute(
        item.starting_bid,
        entry.estimated_value,
        entry.sample_size,
        age_ticks,
        tier,
        fees,
    );

    if profit.roi_percent > thresholds.max_plausible_roi_percent {
        return FlipVerdict::ImplausibleRoi {
            roi_percent: profit.roi_percent,
        };
    }

    // Same (tier, source) keying as the confidence gate above.
    let roi_multiplier = match (tier, entry.source) {
        (PriceTier::Exact, PriceSource::Live) => thresholds.live_exact_roi_multiplier,
        (PriceTier::Exact, PriceSource::Historical) => 1.0,
        (PriceTier::MajorModifiers, _) => thresholds.major_modifier_roi_multiplier,
        (PriceTier::BaseItem, _) => thresholds.base_item_roi_multiplier,
    };
    let required_roi_percent = thresholds.min_roi_percent * roi_multiplier;

    if profit.expected_profit >= thresholds.min_profit && profit.roi_percent >= required_roi_percent
    {
        FlipVerdict::Flip(profit)
    } else {
        FlipVerdict::BelowThreshold(profit)
    }
}

/// `i128` intermediates so a large `estimated_value` against a small
/// `buy_price` (or vice versa) can never overflow/underflow the way
/// unsigned `u64` subtraction would on a loss.
fn compute(
    buy_price: u64,
    estimated_value: u64,
    sample_size: u32,
    price_age_ticks: i64,
    tier: PriceTier,
    fees: &FeeSchedule,
) -> ProfitCalculation {
    let tax = fees.tax_on(estimated_value);

    let net_proceeds = estimated_value as i128 - tax as i128;
    let expected_profit_i128 = net_proceeds - buy_price as i128;
    let expected_profit = expected_profit_i128.clamp(i64::MIN as i128, i64::MAX as i128) as i64;

    let roi_percent = (expected_profit as f64 / buy_price as f64) * 100.0;

    ProfitCalculation {
        estimated_value,
        buy_price,
        tax,
        expected_profit,
        roi_percent,
        sample_size,
        price_age_ticks,
        tier,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pricing::PriceEntry;

    fn item(bin: bool, starting_bid: u64) -> ParsedItem {
        ParsedItem {
            uuid: "auction-uuid".to_string(),
            auctioneer: "seller-uuid".to_string(),
            skyblock_item_id: "HYPERION".to_string(),
            display_name: "Hyperion".to_string(),
            count: 1,
            starting_bid,
            bin,
            end: 1_690_003_600_000,
            extra_attributes: None,
        }
    }

    /// Builds a Tier 1 (exact fingerprint match) lookup -- the tier
    /// nearly every pre-existing test in this module cares about. Tier
    /// 2/3-specific behavior gets its own helper, `tiered_price`, below.
    fn price(estimated_value: u64, sample_size: u32, updated_at_tick: i64) -> PriceLookup {
        PriceLookup {
            entry: PriceEntry {
                estimated_value,
                sample_size,
                updated_at_tick,
                source: PriceSource::Live,
            },
            tier: PriceTier::Exact,
        }
    }

    fn historical_price(
        estimated_value: u64,
        sample_size: u32,
        updated_at_tick: i64,
    ) -> PriceLookup {
        PriceLookup {
            entry: PriceEntry {
                estimated_value,
                sample_size,
                updated_at_tick,
                source: PriceSource::Historical,
            },
            tier: PriceTier::Exact,
        }
    }

    fn tiered_price(
        estimated_value: u64,
        sample_size: u32,
        updated_at_tick: i64,
        tier: PriceTier,
    ) -> PriceLookup {
        PriceLookup {
            tier,
            ..price(estimated_value, sample_size, updated_at_tick)
        }
    }

    #[test]
    fn non_bin_auction_is_ignored_regardless_of_price() {
        let verdict = evaluate(
            &item(false, 100),
            Some(price(1_000_000, 5, 100)),
            100,
            &FeeSchedule::default(),
            &FlipThresholds::default(),
        );
        assert_eq!(verdict, FlipVerdict::NotBin);
    }

    #[test]
    fn missing_price_data_is_reported_and_does_not_panic() {
        let verdict = evaluate(
            &item(true, 100),
            None,
            100,
            &FeeSchedule::default(),
            &FlipThresholds::default(),
        );
        assert_eq!(verdict, FlipVerdict::NoPriceData);
    }

    #[test]
    fn insufficient_sample_size_is_skipped() {
        let thresholds = FlipThresholds {
            min_sample_size: 3,
            ..Default::default()
        };
        let verdict = evaluate(
            &item(true, 100),
            Some(price(1_000_000, 1, 100)),
            100,
            &FeeSchedule::default(),
            &thresholds,
        );
        assert_eq!(
            verdict,
            FlipVerdict::InsufficientSampleSize { sample_size: 1 }
        );
    }

    #[test]
    fn stale_price_is_skipped() {
        let thresholds = FlipThresholds {
            max_price_age_ticks: 5,
            ..Default::default()
        };
        // updated at tick 10, evaluated at tick 20 -> age 10, over the
        // limit of 5. sample_size 20 clears min_live_exact_confidence
        // (Medium, 10+) so this test reaches (and stays focused on) the
        // staleness check, not the confidence-protection-session gate.
        let verdict = evaluate(
            &item(true, 100),
            Some(price(1_000_000, 20, 10)),
            20,
            &FeeSchedule::default(),
            &thresholds,
        );
        assert_eq!(verdict, FlipVerdict::StalePrice { age_ticks: 10 });
    }

    #[test]
    fn default_live_staleness_survives_at_least_one_hypixel_tick_interval() {
        // Regression test for the bug documented in the module doc
        // comment: max_price_age_ticks is compared against millisecond
        // deltas, and Hypixel's cache refreshes roughly every 60_000ms
        // (per claude.md). The old default of `5` meant literally no
        // live-fed price could ever survive to the next tick.
        assert!(
            FlipThresholds::default().max_price_age_ticks > 60_000,
            "max_price_age_ticks must be large enough to survive at least \
             one ~60s Hypixel tick interval, or every live price is stale \
             immediately"
        );
    }

    #[test]
    fn live_price_a_few_ticks_old_is_still_usable_under_the_default() {
        let thresholds = FlipThresholds::default();
        // ~2 Hypixel tick intervals old (120s), well under the 3-minute
        // default ceiling. sample_size 20 clears the confidence-
        // protection-session Live-Exact floor so this test stays
        // focused on staleness.
        let verdict = evaluate(
            &item(true, 1_000_000),
            Some(price(2_000_000, 20, 0)),
            120_000,
            &FeeSchedule::default(),
            &thresholds,
        );
        assert!(!matches!(verdict, FlipVerdict::StalePrice { .. }));
    }

    #[test]
    fn historical_price_days_old_is_still_usable() {
        let thresholds = FlipThresholds::default();
        let seven_days_ms = 7 * 24 * 60 * 60 * 1000;

        let verdict = evaluate(
            &item(true, 1_000_000),
            Some(historical_price(2_000_000, 5, 0)),
            seven_days_ms,
            &FeeSchedule::default(),
            &thresholds,
        );
        assert!(!matches!(verdict, FlipVerdict::StalePrice { .. }));
    }

    #[test]
    fn historical_price_beyond_its_own_ceiling_is_stale() {
        let thresholds = FlipThresholds::default();
        let ninety_days_ms = 90 * 24 * 60 * 60 * 1000;

        let verdict = evaluate(
            &item(true, 1_000_000),
            Some(historical_price(2_000_000, 5, 0)),
            ninety_days_ms,
            &FeeSchedule::default(),
            &thresholds,
        );
        assert!(matches!(verdict, FlipVerdict::StalePrice { .. }));
    }

    #[test]
    fn live_price_that_would_be_fine_if_historical_is_stale_as_live() {
        // Same age as historical_price_days_old_is_still_usable above,
        // but source: Live -- must use the much shorter live ceiling,
        // not the historical one, proving the two are not conflated.
        // sample_size 20 clears the Live-Exact confidence floor so this
        // test stays focused on staleness.
        let thresholds = FlipThresholds::default();
        let seven_days_ms = 7 * 24 * 60 * 60 * 1000;

        let verdict = evaluate(
            &item(true, 1_000_000),
            Some(price(2_000_000, 20, 0)),
            seven_days_ms,
            &FeeSchedule::default(),
            &thresholds,
        );
        assert!(matches!(verdict, FlipVerdict::StalePrice { .. }));
    }

    #[test]
    fn out_of_order_ticks_do_not_produce_negative_age_or_panic() {
        // current_tick before the price's updated_at_tick shouldn't
        // happen in practice, but must not panic or report a bogus
        // negative age.
        let verdict = evaluate(
            &item(true, 100),
            Some(price(1_000_000, 5, 100)),
            50,
            &FeeSchedule::default(),
            &FlipThresholds::default(),
        );
        // age_ticks clamps to 0, so this should proceed like a fresh
        // price rather than erroring.
        assert!(!matches!(verdict, FlipVerdict::StalePrice { .. }));
    }

    #[test]
    fn zero_buy_price_is_invalid_not_a_divide_by_zero_panic() {
        // sample_size 20 clears the Live-Exact confidence floor so this
        // test reaches (and stays focused on) the zero-price check.
        let verdict = evaluate(
            &item(true, 0),
            Some(price(1_000_000, 20, 100)),
            100,
            &FeeSchedule::default(),
            &FlipThresholds::default(),
        );
        assert_eq!(verdict, FlipVerdict::InvalidPriceData);
    }

    #[test]
    fn zero_estimated_value_is_invalid() {
        let verdict = evaluate(
            &item(true, 100),
            Some(price(0, 20, 100)),
            100,
            &FeeSchedule::default(),
            &FlipThresholds::default(),
        );
        assert_eq!(verdict, FlipVerdict::InvalidPriceData);
    }

    #[test]
    fn profitable_flip_matches_hand_computed_numbers() {
        let fees = FeeSchedule {
            tax_rate: 0.01,
            minimum_tax: 0,
        };
        let thresholds = FlipThresholds::default();

        // buy 1_000_000, resell at 2_000_000, 1% tax = 20_000.
        // net proceeds = 1_980_000, profit = 980_000, roi = 98%.
        // sample_size 20 clears the Live-Exact confidence floor.
        let verdict = evaluate(
            &item(true, 1_000_000),
            Some(price(2_000_000, 20, 100)),
            100,
            &fees,
            &thresholds,
        );

        match verdict {
            FlipVerdict::Flip(profit) => {
                assert_eq!(profit.tax, 20_000);
                assert_eq!(profit.expected_profit, 980_000);
                assert!((profit.roi_percent - 98.0).abs() < 1e-9);
            }
            other => panic!("expected Flip, got {other:?}"),
        }
    }

    #[test]
    fn thin_profit_below_thresholds_is_not_reported_as_a_flip() {
        let thresholds = FlipThresholds {
            min_profit: 1_000_000,
            min_roi_percent: 5.0,
            ..Default::default()
        };

        // Small real profit that doesn't clear min_profit. sample_size
        // 20 clears the Live-Exact confidence floor so this stays
        // focused on the profit/ROI bar, not sample size.
        let verdict = evaluate(
            &item(true, 1_000_000),
            Some(price(1_050_000, 20, 100)),
            100,
            &FeeSchedule {
                tax_rate: 0.0,
                minimum_tax: 0,
            },
            &thresholds,
        );

        assert!(matches!(verdict, FlipVerdict::BelowThreshold(_)));
        assert!(!verdict.is_flip());
    }

    #[test]
    fn overpriced_auction_is_a_signed_loss_not_a_panic() {
        // sample_size 20 clears the Live-Exact confidence floor.
        let verdict = evaluate(
            &item(true, 2_000_000),
            Some(price(1_000_000, 20, 100)),
            100,
            &FeeSchedule::default(),
            &FlipThresholds::default(),
        );

        match verdict {
            FlipVerdict::BelowThreshold(profit) => {
                assert!(profit.expected_profit < 0);
                assert!(profit.roi_percent < 0.0);
            }
            other => panic!("expected BelowThreshold with a loss, got {other:?}"),
        }
    }

    #[test]
    fn implausible_roi_is_rejected_as_a_false_positive_guard() {
        let thresholds = FlipThresholds {
            max_plausible_roi_percent: 500.0,
            ..Default::default()
        };

        // 100 -> 100_000_000 estimated value is a ~1000x "flip", almost
        // certainly bad data (e.g. a fingerprint collision or a troll
        // listing skewing the cheapest-BIN sample), not real. sample_size
        // 20 clears the Live-Exact confidence floor so this stays
        // focused on plausibility, not sample size.
        let verdict = evaluate(
            &item(true, 100),
            Some(price(100_000_000, 20, 100)),
            100,
            &FeeSchedule::default(),
            &thresholds,
        );

        assert!(matches!(verdict, FlipVerdict::ImplausibleRoi { .. }));
    }

    #[test]
    fn minimum_tax_floor_is_applied_on_cheap_resales() {
        let fees = FeeSchedule {
            tax_rate: 0.01,
            minimum_tax: 500,
        };
        // 1% of 1000 is 10, below the 500 floor.
        assert_eq!(fees.tax_on(1_000), 500);
        // 1% of 1_000_000 is 10_000, above the floor.
        assert_eq!(fees.tax_on(1_000_000), 10_000);
    }

    #[test]
    fn extreme_values_do_not_overflow_or_panic() {
        let verdict = evaluate(
            &item(true, 1),
            Some(price(u64::MAX, 5, 100)),
            100,
            &FeeSchedule::default(),
            &FlipThresholds {
                max_plausible_roi_percent: f64::MAX,
                ..Default::default()
            },
        );
        // Just needs to not panic; the exact verdict isn't the point.
        let _ = verdict;
    }

    // --- Tiered pricing: confidence gates ---

    #[test]
    fn historical_exact_tier_is_trusted_even_at_low_confidence() {
        // Regression guard for "keep COFL historical pricing highest
        // trust" (confidence-protection session): a
        // (PriceTier::Exact, PriceSource::Historical) entry has no
        // confidence floor, unchanged from before -- COFL's median-of-
        // sold-prices stays unconditionally trusted regardless of
        // sample_size.
        let verdict = evaluate(
            &item(true, 1_000_000),
            Some(historical_price(2_000_000, 2, 100)),
            100,
            &FeeSchedule::default(),
            &FlipThresholds::default(),
        );
        assert!(!matches!(
            verdict,
            FlipVerdict::InsufficientSampleSize { .. }
        ));
    }

    #[test]
    fn live_exact_tier_below_medium_confidence_is_rejected() {
        // confidence-protection session: unlike Historical, a
        // (PriceTier::Exact, PriceSource::Live) entry now has its own
        // confidence floor. sample_size 5 < Medium's threshold of 10.
        let verdict = evaluate(
            &item(true, 1_000_000),
            Some(price(2_000_000, 5, 100)),
            100,
            &FeeSchedule::default(),
            &FlipThresholds::default(),
        );
        assert_eq!(
            verdict,
            FlipVerdict::InsufficientSampleSize { sample_size: 5 }
        );
    }

    #[test]
    fn live_exact_tier_at_medium_confidence_passes_the_gate() {
        let verdict = evaluate(
            &item(true, 1_000_000),
            Some(price(2_000_000, 10, 100)),
            100,
            &FeeSchedule::default(),
            &FlipThresholds::default(),
        );
        assert!(!matches!(
            verdict,
            FlipVerdict::InsufficientSampleSize { .. }
        ));
    }

    #[test]
    fn major_modifier_tier_below_medium_confidence_is_rejected() {
        // sample_size 5 < Medium's threshold of 10.
        let verdict = evaluate(
            &item(true, 1_000_000),
            Some(tiered_price(2_000_000, 5, 100, PriceTier::MajorModifiers)),
            100,
            &FeeSchedule::default(),
            &FlipThresholds::default(),
        );
        assert_eq!(
            verdict,
            FlipVerdict::InsufficientSampleSize { sample_size: 5 }
        );
    }

    #[test]
    fn major_modifier_tier_at_medium_confidence_passes_the_gate() {
        let verdict = evaluate(
            &item(true, 1_000_000),
            Some(tiered_price(2_000_000, 10, 100, PriceTier::MajorModifiers)),
            100,
            &FeeSchedule::default(),
            &FlipThresholds::default(),
        );
        assert!(!matches!(
            verdict,
            FlipVerdict::InsufficientSampleSize { .. }
        ));
    }

    #[test]
    fn base_item_tier_below_high_confidence_is_rejected() {
        // sample_size 20 clears Medium (10) but not High (50).
        let verdict = evaluate(
            &item(true, 1_000_000),
            Some(tiered_price(2_000_000, 20, 100, PriceTier::BaseItem)),
            100,
            &FeeSchedule::default(),
            &FlipThresholds::default(),
        );
        assert_eq!(
            verdict,
            FlipVerdict::InsufficientSampleSize { sample_size: 20 }
        );
    }

    #[test]
    fn base_item_tier_at_high_confidence_passes_the_gate() {
        let verdict = evaluate(
            &item(true, 1_000_000),
            Some(tiered_price(2_000_000, 50, 100, PriceTier::BaseItem)),
            100,
            &FeeSchedule::default(),
            &FlipThresholds::default(),
        );
        assert!(!matches!(
            verdict,
            FlipVerdict::InsufficientSampleSize { .. }
        ));
    }

    // --- Tiered pricing: ROI multipliers ---

    #[test]
    fn major_modifier_tier_needs_a_bigger_roi_margin_than_exact() {
        let thresholds = FlipThresholds {
            min_profit: 0,
            min_roi_percent: 20.0,
            ..Default::default()
        };
        // buy 1_000_000, resell 1_250_000, 0% tax => profit 250_000,
        // roi 25%. Clears the flat 20% bar but not 20% * 1.5 = 30%
        // required for Tier 2.
        let fees = FeeSchedule {
            tax_rate: 0.0,
            minimum_tax: 0,
        };

        let exact_verdict = evaluate(
            &item(true, 1_000_000),
            Some(tiered_price(1_250_000, 50, 100, PriceTier::Exact)),
            100,
            &fees,
            &thresholds,
        );
        assert!(exact_verdict.is_flip());

        let major_verdict = evaluate(
            &item(true, 1_000_000),
            Some(tiered_price(1_250_000, 50, 100, PriceTier::MajorModifiers)),
            100,
            &fees,
            &thresholds,
        );
        assert!(matches!(major_verdict, FlipVerdict::BelowThreshold(_)));
    }

    #[test]
    fn live_exact_needs_a_bigger_roi_margin_than_historical_exact() {
        // confidence-protection session: a Live-Exact entry (once past
        // its own confidence floor) still needs more margin than a
        // Historical-Exact one, even though both are Tier 1 -- source
        // matters, not just tier.
        let thresholds = FlipThresholds {
            min_profit: 0,
            min_roi_percent: 20.0,
            ..Default::default()
        };
        // buy 1_000_000, resell 1_220_000, 0% tax => profit 220_000,
        // roi 22%. Clears the flat 20% bar and Historical-Exact's
        // (unmultiplied) 20% bar, but not Live-Exact's 20% * 1.2 = 24%.
        let fees = FeeSchedule {
            tax_rate: 0.0,
            minimum_tax: 0,
        };

        let historical_verdict = evaluate(
            &item(true, 1_000_000),
            Some(historical_price(1_220_000, 50, 100)),
            100,
            &fees,
            &thresholds,
        );
        assert!(historical_verdict.is_flip());

        let live_verdict = evaluate(
            &item(true, 1_000_000),
            Some(price(1_220_000, 50, 100)),
            100,
            &fees,
            &thresholds,
        );
        assert!(matches!(live_verdict, FlipVerdict::BelowThreshold(_)));
    }

    #[test]
    fn live_exact_needs_a_smaller_roi_margin_than_major_modifiers() {
        // Even though both are gated by the same confidence floor
        // (Medium) once Live-sourced, an exact fingerprint match keeps
        // a lighter ROI bar than a coarser Major-tier match -- "keep
        // exact fingerprint priority" holds even under the new
        // source-aware gating.
        let thresholds = FlipThresholds {
            min_profit: 0,
            min_roi_percent: 20.0,
            ..Default::default()
        };
        // 25% ROI clears Live-Exact's 20% * 1.2 = 24% bar but not
        // Major's 20% * 1.5 = 30% bar.
        let fees = FeeSchedule {
            tax_rate: 0.0,
            minimum_tax: 0,
        };

        let live_exact_verdict = evaluate(
            &item(true, 1_000_000),
            Some(price(1_250_000, 50, 100)),
            100,
            &fees,
            &thresholds,
        );
        assert!(live_exact_verdict.is_flip());

        let major_verdict = evaluate(
            &item(true, 1_000_000),
            Some(tiered_price(1_250_000, 50, 100, PriceTier::MajorModifiers)),
            100,
            &fees,
            &thresholds,
        );
        assert!(matches!(major_verdict, FlipVerdict::BelowThreshold(_)));
    }

    #[test]
    fn base_item_tier_needs_an_even_bigger_roi_margin() {
        let thresholds = FlipThresholds {
            min_profit: 0,
            min_roi_percent: 10.0,
            ..Default::default()
        };
        let fees = FeeSchedule {
            tax_rate: 0.0,
            minimum_tax: 0,
        };

        // 20% ROI: clears the flat 10% bar and Tier 2's 15% bar, but not
        // Tier 3's 30% (10% * 3.0) bar.
        let verdict = evaluate(
            &item(true, 1_000_000),
            Some(tiered_price(1_200_000, 50, 100, PriceTier::BaseItem)),
            100,
            &fees,
            &thresholds,
        );
        assert!(matches!(verdict, FlipVerdict::BelowThreshold(_)));

        // 40% ROI clears Tier 3's bar.
        let verdict = evaluate(
            &item(true, 1_000_000),
            Some(tiered_price(1_400_000, 50, 100, PriceTier::BaseItem)),
            100,
            &fees,
            &thresholds,
        );
        assert!(verdict.is_flip());
    }

    #[test]
    fn profit_calculation_reports_which_tier_it_came_from() {
        let verdict = evaluate(
            &item(true, 1_000_000),
            Some(tiered_price(2_000_000, 50, 100, PriceTier::MajorModifiers)),
            100,
            &FeeSchedule::default(),
            &FlipThresholds::default(),
        );
        match verdict {
            FlipVerdict::Flip(profit) => assert_eq!(profit.tier, PriceTier::MajorModifiers),
            other => panic!("expected Flip, got {other:?}"),
        }
    }
}

/// Not run by default (`cargo test`); run explicitly for real numbers:
/// `cargo test -p engine --release -- --ignored --nocapture`.
#[cfg(test)]
mod bench {
    use super::*;
    use pricing::PriceEntry;
    use std::time::Instant;

    #[test]
    #[ignore]
    fn evaluate_latency() {
        let mut auction = ParsedItem {
            uuid: "auction-uuid".to_string(),
            auctioneer: "seller-uuid".to_string(),
            skyblock_item_id: "HYPERION".to_string(),
            display_name: "Hyperion".to_string(),
            count: 1,
            starting_bid: 1_000_000,
            bin: true,
            end: 1_690_003_600_000,
            extra_attributes: None,
        };
        let cached_price = PriceLookup {
            entry: PriceEntry {
                estimated_value: 1_500_000,
                // 20 clears the Live-Exact confidence floor added in
                // the confidence-protection session (min_sample_size
                // Medium = 10+) so this still measures evaluate()'s
                // steady-state cost, not InsufficientSampleSize's
                // early-return cost.
                sample_size: 20,
                updated_at_tick: 1,
                source: PriceSource::Live,
            },
            tier: PriceTier::Exact,
        };
        let fees = FeeSchedule::default();
        let thresholds = FlipThresholds::default();

        let iterations = 5_000_000u64;
        let start = Instant::now();
        let mut flips = 0u64;
        for i in 0..iterations {
            // Vary the buy price per iteration through black_box so
            // LLVM can't constant-fold the whole loop into a single
            // cached computation (it did exactly that in an earlier
            // version of this benchmark with fixed inputs, reporting a
            // nonsensical "5,000,000 calls in 124ns"). No allocation is
            // introduced by this — just a plain field write, keeping
            // this a measurement of evaluate() itself.
            auction.starting_bid = std::hint::black_box(1_000_000 - (i % 1_000));
            if evaluate(&auction, Some(cached_price), 2, &fees, &thresholds).is_flip() {
                flips += 1;
            }
        }
        let elapsed = start.elapsed();

        assert_eq!(flips, iterations);

        let ns_per_op = elapsed.as_nanos() as f64 / iterations as f64;
        println!("engine::evaluate: {iterations} calls in {elapsed:?} ({ns_per_op:.2} ns/op)");
    }
}
