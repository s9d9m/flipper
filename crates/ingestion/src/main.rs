use cofl::CoflClient;
use common::{AuctionSnapshot, Config};
use diff::DiffDetector;
use engine::{FeeSchedule, FlipThresholds, FlipVerdict};
use fingerprint::Fingerprint;
use ingestion::HypixelClient;
use notify::{FlipAlert, FlipDeduplicator, NotificationHub};
use pricing::{PriceCache, PriceEntry, PriceSource, PriceTier};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use std::time::Instant;
use storage::SnapshotStore;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

/// Folds one BIN sighting into a per-tick "cheapest observed BIN" map,
/// keyed however the caller likes (exact fingerprint, major-modifier
/// key, or bare item tag) — the same `min` aggregation applied at all
/// three pricing tiers.
///
/// (market-model session) This intentionally stays `min`, not an
/// average: for a flipping bot the lowest *legitimate* BIN in a tick
/// is the actual opportunity signal, and averaging in the tick's
/// overpriced listings would push the estimate upward, away from what
/// a real flip needs. This is no longer the *only* line of defense
/// against a single lowball/mistake listing distorting the cache,
/// though: the per-tick result built here becomes the `incoming` side
/// of `pricing::PriceCache`'s cross-tick merge (see that crate's
/// module doc comment), which folds it into the *existing* multi-tick
/// average via a sample-size-weighted, outlier-dampened blend rather
/// than letting it overwrite outright. So this function still answers
/// "what's the best price seen this tick" (unchanged); the cache is
/// what now remembers more than one tick's answer at a time.
fn accumulate_cheapest_bin<K: Eq + Hash>(
    map: &mut HashMap<K, PriceEntry>,
    key: K,
    starting_bid: u64,
    tick: i64,
) {
    map.entry(key)
        .and_modify(|existing| {
            existing.sample_size += 1;
            existing.estimated_value = existing.estimated_value.min(starting_bid);
        })
        .or_insert(PriceEntry {
            estimated_value: starting_bid,
            sample_size: 1,
            updated_at_tick: tick,
            source: PriceSource::Live,
        });
}

/// Output/readability session: human-scale label for which pricing
/// tier a flip's price came from, matching the task's own wording
/// ("Exact/Major/Base"). Called once per detected flip — a rare event
/// relative to per-auction volume — never on the per-auction hot path.
#[inline]
fn tier_label(tier: PriceTier) -> &'static str {
    match tier {
        PriceTier::Exact => "Exact",
        PriceTier::MajorModifiers => "Major",
        PriceTier::BaseItem => "Base",
    }
}

/// Output/readability session: human-scale label for where a flip's
/// price came from ("Live" this-tick observation vs "COFL" historical
/// backfill). Same call-site guarantee as [`tier_label`] — flip-only,
/// never per-auction.
#[inline]
fn price_source_label(source: PriceSource) -> &'static str {
    match source {
        PriceSource::Live => "Live",
        PriceSource::Historical => "COFL",
    }
}

/// Output/readability session: formats a coin amount with a human-scale
/// K/M/B suffix for log readability (e.g. `923_000_000` -> `"923.0M"`)
/// instead of a long unbroken digit string. Only ever called when
/// building a flip alert log line — flips are rare (single digits per
/// tick against tens of thousands of evaluated auctions), so the one
/// small `String` allocation here adds no meaningful overhead; this
/// function is never called from the per-auction evaluation loop.
fn format_coins(value: i64) -> String {
    let sign = if value < 0 { "-" } else { "" };
    let abs = value.unsigned_abs();
    if abs >= 1_000_000_000 {
        format!("{sign}{:.1}B", abs as f64 / 1_000_000_000.0)
    } else if abs >= 1_000_000 {
        format!("{sign}{:.1}M", abs as f64 / 1_000_000.0)
    } else if abs >= 1_000 {
        format!("{sign}{:.1}K", abs as f64 / 1_000.0)
    } else {
        format!("{sign}{abs}")
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive("ingestion=info".parse().unwrap()),
        )
        .init();

    let config = match Config::from_env() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("configuration error: {err}");
            std::process::exit(1);
        }
    };

    let storage_db_path = config.storage_db_path.clone();
    let websocket_bind_addr = config.websocket_bind_addr.clone();
    let cofl_backfill_enabled = config.cofl_backfill_enabled;
    let cofl_base_url = config.cofl_base_url.clone();
    let cofl_backfill_item_tags = config.cofl_backfill_item_tags.clone();
    let cofl_backfill_pages_per_tag = config.cofl_backfill_pages_per_tag;
    let cofl_backfill_interval_minutes = config.cofl_backfill_interval_minutes;

    let client = match HypixelClient::new(config) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("failed to construct hypixel client: {err}");
            std::process::exit(1);
        }
    };

    let store = match SnapshotStore::open(&storage_db_path) {
        Ok(store) => store,
        Err(err) => {
            eprintln!("failed to open storage database at {storage_db_path}: {err}");
            std::process::exit(1);
        }
    };

    let ws_listener = match NotificationHub::bind(&websocket_bind_addr).await {
        Ok(listener) => listener,
        Err(err) => {
            eprintln!("failed to bind websocket notification server: {err}");
            std::process::exit(1);
        }
    };
    let notification_hub = Arc::new(NotificationHub::new(256));
    tokio::spawn(NotificationHub::serve(
        Arc::clone(&notification_hub),
        ws_listener,
    ));
    info!(addr = %websocket_bind_addr, "websocket notification server listening");

    let price_cache = Arc::new(PriceCache::new());

    // COFL historical-price backfill: entirely in the background,
    // concurrently with ingestion startup below (not awaited, so a
    // slow/rate-limited backfill can never delay the sniper path's
    // first tick). It only ever calls
    // PriceCache::update_{exact,major,base}_batch, the same non-blocking
    // write paths the live feed uses further down — the hot path
    // (PriceCache::get, engine::evaluate) has no idea whether an entry
    // came from here or a live tick. See the cofl crate for the
    // investigation behind this and its confirmed-vs-assumed caveats.
    //
    // (price coverage session) Runs once immediately, then — if
    // cofl_backfill_interval_minutes > 0 — repeats on that interval for
    // as long as the process runs, so coverage keeps growing/refreshing
    // "during operation" instead of being frozen at whatever the
    // startup crawl produced. Still just a loop of the same
    // already-non-blocking cofl::backfill call; nothing here is awaited
    // by (or can delay) the ingestion loop below.
    if cofl_backfill_enabled {
        let backfill_cache = Arc::clone(&price_cache);
        tokio::spawn(async move {
            let client = match CoflClient::new(cofl_base_url) {
                Ok(client) => client,
                Err(err) => {
                    warn!(error = %err, "failed to construct COFL client; skipping historical backfill");
                    return;
                }
            };
            loop {
                let stats = cofl::backfill(
                    &client,
                    &cofl_backfill_item_tags,
                    cofl_backfill_pages_per_tag,
                    &backfill_cache,
                )
                .await;

                if cofl_backfill_interval_minutes == 0 {
                    info!(
                        cache_entries = backfill_cache.len(),
                        "COFL backfill complete (one-shot, COFL_BACKFILL_INTERVAL_MINUTES=0)"
                    );
                    break;
                }

                info!(
                    exact_entries_seeded = stats.exact_entries_seeded,
                    major_modifier_entries_seeded = stats.major_modifier_entries_seeded,
                    base_item_entries_seeded = stats.base_item_entries_seeded,
                    cache_entries = backfill_cache.len(),
                    next_run_in_minutes = cofl_backfill_interval_minutes,
                    "COFL backfill cycle complete; sleeping until next scheduled run"
                );
                tokio::time::sleep(std::time::Duration::from_secs(
                    cofl_backfill_interval_minutes * 60,
                ))
                .await;
            }
        });
    } else {
        info!("COFL historical backfill disabled (COFL_BACKFILL_ENABLED=false)");
    }

    // Placeholder defaults; claude.md's Phase 2 website already
    // anticipates user-configurable min-profit/min-ROI settings, which
    // is where these should come from instead of Default once that
    // exists.
    let fee_schedule = FeeSchedule::default();
    let flip_thresholds = FlipThresholds::default();

    // Bounded channel: backpressure here is a deliberate signal that
    // downstream processing (diff detection, in Phase 1.3) isn't keeping
    // up — better to surface that loudly than to buffer unboundedly.
    let (tx, mut rx) = mpsc::channel::<AuctionSnapshot>(4);

    let receiver = tokio::spawn(async move {
        let mut detector = DiffDetector::new();
        let mut dedup = FlipDeduplicator::new();

        // TEMPORARY DIAGNOSTIC INSTRUMENTATION — added to root-cause a
        // live run reporting flips_found=0 / below_threshold=0 on every
        // tick. Cumulative across the whole process lifetime (not reset
        // per tick, unlike the other counters below) because any single
        // tick's evaluated-auction count can be tiny once diff detection
        // has filtered ~90% of the snapshot away, which makes per-tick
        // numbers too noisy to diagnose from. Remove once the rejection
        // breakdown below has been read off a live run and the root
        // cause is fixed — this is not meant to be permanent.
        let mut diag_evaluated_total: u64 = 0;
        let mut diag_cache_hit_total: u64 = 0;
        let mut diag_not_bin_total: u64 = 0;
        let mut diag_no_price_data_total: u64 = 0;
        let mut diag_insufficient_sample_total: u64 = 0;
        let mut diag_stale_price_total: u64 = 0;
        let mut diag_invalid_price_total: u64 = 0;
        let mut diag_implausible_roi_total: u64 = 0;
        let mut diag_below_threshold_total: u64 = 0;
        let mut diag_max_expected_profit_ever: Option<i64> = None;
        let mut diag_max_roi_percent_ever: Option<f64> = None;
        // Requirement from the COFL integration task: how many
        // evaluated auctions had a cache hit whose price came from the
        // COFL backfill (PriceSource::Historical) rather than a live
        // tick's own observations.
        let mut diag_historical_price_hit_total: u64 = 0;
        // Tiered-pricing visibility: which tier each cache hit resolved
        // at. Useful for judging, on a live run, whether Tier 2/3
        // fallbacks are actually contributing flips or just adding
        // rejected InsufficientSampleSize verdicts.
        let mut diag_tier_exact_hit_total: u64 = 0;
        let mut diag_tier_major_modifier_hit_total: u64 = 0;
        let mut diag_tier_base_item_hit_total: u64 = 0;
        // market-model session: cumulative counts of what the price
        // cache's cross-tick writes actually did — see
        // pricing::PriceCache's update_*_batch / MergeStats. Merged =
        // a same-source Live observation was blended into an existing
        // estimate instead of replacing it; overwritten = a
        // cross-source change or a fresh Historical (COFL) refresh;
        // inserted_new = first sighting of that key ever.
        let mut diag_live_merged_total: u64 = 0;
        let mut diag_overwritten_total: u64 = 0;
        let mut diag_inserted_new_total: u64 = 0;

        while let Some(snapshot) = rx.recv().await {
            // Output/readability session: wall-clock timer around this
            // tick's whole processing block, purely for the "processing
            // time" summary field below. Instant::now()/.elapsed() are
            // monotonic-clock reads (~tens of ns, no syscall on most
            // platforms) — the same measurement pattern
            // ingestion::HypixelClient already uses for
            // detect_latency_ms/snapshot_fetch_latency_ms. This measures
            // the pipeline, it doesn't change anything about it.
            let tick_started_at = Instant::now();

            let tick = snapshot.last_updated;
            let total = snapshot.auctions.len();
            let changed = detector.diff(snapshot);

            let mut parsed = Vec::with_capacity(changed.len());
            let mut parse_failures = 0usize;
            for auction in &changed {
                match parser::parse_item(auction) {
                    Ok(item) => parsed.push(item),
                    Err(err) => {
                        parse_failures += 1;
                        warn!(uuid = %auction.uuid, error = %err, "failed to parse auction item");
                    }
                }
            }

            // Fingerprinting + profit evaluation + price cache feed, in
            // that specific order. Every auction is evaluated against
            // the cache's state from *prior* ticks first — never
            // against a "market price" derived from itself or its
            // same-tick siblings, which is what would happen if this
            // tick's cheapest-BIN observations were folded into the
            // cache before evaluating this tick's own auctions against
            // it. Only after every auction in this batch has been
            // evaluated do we update the cache for the *next* tick.
            let mut unique_fingerprints: HashSet<Fingerprint> =
                HashSet::with_capacity(parsed.len());
            let mut cheapest_bin_exact: HashMap<Fingerprint, PriceEntry> = HashMap::new();
            let mut cheapest_bin_major: HashMap<Fingerprint, PriceEntry> = HashMap::new();
            let mut cheapest_bin_base: HashMap<String, PriceEntry> = HashMap::new();
            let mut flip_candidates: Vec<FlipAlert> = Vec::new();
            let mut below_threshold = 0usize;

            for item in &parsed {
                let fp = fingerprint::fingerprint(item);
                let major = fingerprint::major_modifier_key(item);
                unique_fingerprints.insert(fp);

                let cached_price = price_cache.get(fp, major, &item.skyblock_item_id);

                diag_evaluated_total += 1;
                if let Some(lookup) = cached_price {
                    diag_cache_hit_total += 1;
                    match lookup.tier {
                        PriceTier::Exact => diag_tier_exact_hit_total += 1,
                        PriceTier::MajorModifiers => diag_tier_major_modifier_hit_total += 1,
                        PriceTier::BaseItem => diag_tier_base_item_hit_total += 1,
                    }
                    if lookup.entry.source == PriceSource::Historical {
                        diag_historical_price_hit_total += 1;
                    }
                }

                match engine::evaluate(item, cached_price, tick, &fee_schedule, &flip_thresholds) {
                    FlipVerdict::Flip(profit) => {
                        diag_max_expected_profit_ever = Some(
                            diag_max_expected_profit_ever
                                .map_or(profit.expected_profit, |m| m.max(profit.expected_profit)),
                        );
                        diag_max_roi_percent_ever = Some(
                            diag_max_roi_percent_ever
                                .map_or(profit.roi_percent, |m| m.max(profit.roi_percent)),
                        );
                        // cached_price is Copy and was only read (not
                        // moved) by evaluate() above, so it's still
                        // available here. Always Some at this point —
                        // evaluate() can't reach FlipVerdict::Flip
                        // without a priced lookup — the fallback is
                        // defensive only, never actually exercised.
                        let price_source = cached_price
                            .map(|lookup| lookup.entry.source)
                            .unwrap_or(PriceSource::Live);
                        flip_candidates.push(FlipAlert::new(
                            item.uuid.clone(),
                            item.display_name.clone(),
                            profit.buy_price,
                            profit.estimated_value,
                            profit.expected_profit,
                            profit.roi_percent,
                            item.end,
                            tier_label(profit.tier),
                            price_source_label(price_source),
                        ));
                    }
                    FlipVerdict::BelowThreshold(profit) => {
                        below_threshold += 1;
                        diag_below_threshold_total += 1;
                        diag_max_expected_profit_ever = Some(
                            diag_max_expected_profit_ever
                                .map_or(profit.expected_profit, |m| m.max(profit.expected_profit)),
                        );
                        diag_max_roi_percent_ever = Some(
                            diag_max_roi_percent_ever
                                .map_or(profit.roi_percent, |m| m.max(profit.roi_percent)),
                        );
                    }
                    FlipVerdict::NotBin => diag_not_bin_total += 1,
                    FlipVerdict::NoPriceData => diag_no_price_data_total += 1,
                    FlipVerdict::InsufficientSampleSize { .. } => {
                        diag_insufficient_sample_total += 1
                    }
                    FlipVerdict::StalePrice { .. } => diag_stale_price_total += 1,
                    FlipVerdict::InvalidPriceData => diag_invalid_price_total += 1,
                    FlipVerdict::ImplausibleRoi { roi_percent } => {
                        diag_implausible_roi_total += 1;
                        // Deliberately tracked even though it's a
                        // rejected verdict: a high implausible-ROI max
                        // is itself a diagnostic signal that
                        // max_plausible_roi_percent may be rejecting
                        // genuine rare-item flips, not just bad data.
                        diag_max_roi_percent_ever = Some(
                            diag_max_roi_percent_ever.map_or(roi_percent, |m| m.max(roi_percent)),
                        );
                    }
                }

                // NOTE: `estimated_value` here is just this tick's
                // cheapest observed BIN listing per key — a placeholder
                // cheap enough to compute inline, not a real fair-value
                // estimate (median, outlier-trimmed, etc.). The engine
                // only consumes whatever value the cache is given; it
                // doesn't validate how it was derived. The same live
                // sighting feeds all three tiers at once: it's an exact
                // match for its own fingerprint, a major-modifier match
                // for its item+reforge+stars+hpc+top-enchant
                // combination, and a base-item match for its bare item
                // id — mirroring how cofl::backfill seeds all three
                // tiers from the same COFL sale.
                if item.bin {
                    accumulate_cheapest_bin(&mut cheapest_bin_exact, fp, item.starting_bid, tick);
                    accumulate_cheapest_bin(
                        &mut cheapest_bin_major,
                        major,
                        item.starting_bid,
                        tick,
                    );
                    accumulate_cheapest_bin(
                        &mut cheapest_bin_base,
                        item.skyblock_item_id.clone(),
                        item.starting_bid,
                        tick,
                    );
                }
            }

            let unique_fingerprint_count = unique_fingerprints.len();
            let priced_fingerprint_count = cheapest_bin_exact.len();
            // market-model session: each update_*_batch call now
            // reports what it actually did (merged/overwrote/inserted)
            // instead of returning (); summed into this tick's totals
            // and folded into the cumulative diag_* counters below.
            // Three small Copy-struct additions -- not on the hot path,
            // this whole block already only runs once per tick.
            let exact_merge_stats = price_cache.update_exact_batch(cheapest_bin_exact);
            let major_merge_stats = price_cache.update_major_batch(cheapest_bin_major);
            let base_merge_stats = price_cache.update_base_batch(cheapest_bin_base);
            diag_live_merged_total += exact_merge_stats.live_merged
                + major_merge_stats.live_merged
                + base_merge_stats.live_merged;
            diag_overwritten_total += exact_merge_stats.overwritten
                + major_merge_stats.overwritten
                + base_merge_stats.overwritten;
            diag_inserted_new_total += exact_merge_stats.inserted_new
                + major_merge_stats.inserted_new
                + base_merge_stats.inserted_new;

            // Dedup (sync, in-memory, batched once per tick) then
            // publish (non-blocking) — see the notify crate's module
            // docs for why this ordering and these two primitives never
            // add latency to detection.
            let new_flips = dedup.filter_new(flip_candidates, tick);
            let flips_found = new_flips.len();
            for alert in &new_flips {
                notification_hub.publish(alert);
                // Output/readability session: all the same structured
                // fields as before (unchanged names, still grep/parse-
                // able), plus a human-readable message built from them.
                // format_coins() allocates two small Strings here, but
                // this arm only runs once per *deduped, new* flip —
                // never per auction, never per tick unless a flip
                // actually fired.
                let buy_fmt = format_coins(alert.buy_price as i64);
                let value_fmt = format_coins(alert.estimated_value as i64);
                let profit_fmt = format_coins(alert.profit);
                info!(
                    uuid = %alert.auction_uuid,
                    item = %alert.item_name,
                    tier = alert.tier,
                    price_source = alert.price_source,
                    buy_price = alert.buy_price,
                    estimated_value = alert.estimated_value,
                    profit = alert.profit,
                    roi_percent = alert.roi_percent,
                    viewauction = %alert.viewauction_command,
                    "FLIP  {}  |  buy {buy_fmt} -> value {value_fmt}  |  profit +{profit_fmt} ({:.1}% ROI)  |  tier {} / {}  |  {}",
                    alert.item_name,
                    alert.roi_percent,
                    alert.tier,
                    alert.price_source,
                    alert.viewauction_command,
                );
            }

            let parsed_count = parsed.len();
            if let Err(err) = store.store(tick, parsed).await {
                warn!(tick, error = %err, "failed to persist parsed auction batch");
            }

            // Output/readability session: elapsed is read once here,
            // after all of this tick's processing (including the
            // storage write above) has finished — see the comment on
            // tick_started_at above for why this is measurement, not a
            // pipeline change.
            let elapsed_ms = tick_started_at.elapsed().as_secs_f64() * 1000.0;

            // market-model session: a full walk over every cached
            // entry, at every tier -- same cost class as the
            // price_cache.len() call already made every tick below,
            // not on the hot path. Gives the average sample size and
            // high-confidence entry count the earlier tasks asked for.
            let cache_stats = price_cache.stats();

            // Full technical dump, unchanged field-for-field from
            // before this session — still available, just moved from
            // info! to debug! (RUST_LOG=debug to see it) so it no
            // longer prints by default every single tick. Nothing here
            // was recomputed differently; this is the same
            // TEMPORARY DIAGNOSTIC INSTRUMENTATION described above the
            // receiver task's diag_* declarations, still cumulative
            // since process start, still pending removal once the
            // flips_found=0 root cause is fully confirmed fixed.
            debug!(
                tick,
                total_auctions = total,
                changed_auctions = changed.len(),
                parsed_auctions = parsed_count,
                parse_failures,
                unique_fingerprints = unique_fingerprint_count,
                priced_fingerprints = priced_fingerprint_count,
                price_cache_size = price_cache.len(),
                flips_found,
                below_threshold,
                tracked_live = detector.tracked_count(),
                dedup_tracked = dedup.tracked_count(),
                elapsed_ms,
                diag_evaluated_total,
                diag_cache_hit_total,
                diag_historical_price_hit_total,
                diag_tier_exact_hit_total,
                diag_tier_major_modifier_hit_total,
                diag_tier_base_item_hit_total,
                diag_not_bin_total,
                diag_no_price_data_total,
                diag_insufficient_sample_total,
                diag_stale_price_total,
                diag_invalid_price_total,
                diag_implausible_roi_total,
                diag_below_threshold_total,
                diag_max_expected_profit_ever = ?diag_max_expected_profit_ever,
                diag_max_roi_percent_ever = ?diag_max_roi_percent_ever,
                // market-model session: cumulative cross-tick merge
                // outcomes (see the diag_live_merged_total declaration
                // above) plus a snapshot of the cache's current
                // aggregate health.
                diag_live_merged_total,
                diag_overwritten_total,
                diag_inserted_new_total,
                cache_total_entries = cache_stats.total_entries,
                cache_average_sample_size = cache_stats.average_sample_size(),
                cache_high_confidence_entries = cache_stats.high_confidence_entries,
                "diffed, parsed, fingerprinted, evaluated, priced, deduped, notified, and stored snapshot"
            );

            // Output/readability session: the everyday-visible summary.
            // Only the handful of numbers actually useful for judging
            // "is the bot healthy" at a glance -- flips this tick, the
            // cache-hit rate and its tier breakdown (cumulative, same
            // counters as the debug! dump above, just distilled), how
            // much is going unpriced, and how long this tick took to
            // process. Cheap: two divisions and one format! call, run
            // once per tick, not per auction.
            let cache_hit_rate_percent = if diag_evaluated_total > 0 {
                (diag_cache_hit_total as f64 / diag_evaluated_total as f64) * 100.0
            } else {
                0.0
            };
            info!(
                tick,
                flips_found,
                cache_hit_total = diag_cache_hit_total,
                evaluated_total = diag_evaluated_total,
                cache_hit_rate_percent,
                tier_exact_total = diag_tier_exact_hit_total,
                tier_major_total = diag_tier_major_modifier_hit_total,
                tier_base_total = diag_tier_base_item_hit_total,
                no_price_data_total = diag_no_price_data_total,
                elapsed_ms,
                "tick {tick}: {flips_found} flip(s) | cache {diag_cache_hit_total}/{diag_evaluated_total} hits \
                 ({cache_hit_rate_percent:.1}%) [exact {diag_tier_exact_hit_total} / major {diag_tier_major_modifier_hit_total} \
                 / base {diag_tier_base_item_hit_total}] | no-price {diag_no_price_data_total} | {elapsed_ms:.1}ms"
            );

            // market-model session: a second, separate concise summary
            // for pricing-model health specifically -- distinct from
            // the flip-detection summary above so neither line gets
            // overloaded. Answers "is the market model actually
            // learning over time": how many observations merged into
            // existing estimates vs. replaced them outright, how big
            // the average sample backing a price is, and how many
            // entries have reached high confidence.
            let avg_sample_size = cache_stats.average_sample_size();
            info!(
                cache_total_entries = cache_stats.total_entries,
                cache_average_sample_size = avg_sample_size,
                cache_high_confidence_entries = cache_stats.high_confidence_entries,
                diag_live_merged_total,
                diag_overwritten_total,
                diag_insufficient_sample_total,
                "pricing model: {} entries, avg sample size {avg_sample_size:.1} | {} high-confidence | \
                 {diag_live_merged_total} live merges, {diag_overwritten_total} replaced (cumulative) | \
                 {diag_insufficient_sample_total} flips rejected for insufficient data (cumulative)",
                cache_stats.total_entries,
                cache_stats.high_confidence_entries,
            );
        }
    });

    if let Err(err) = client.run(0, tx).await {
        error!(error = %err, "ingestion loop exited with an error");
        std::process::exit(1);
    }

    let _ = receiver.await;
}
