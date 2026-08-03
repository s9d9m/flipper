use common::{AuctionSnapshot, Config};
use diff::DiffDetector;
use engine::{FeeSchedule, FlipThresholds, FlipVerdict};
use fingerprint::Fingerprint;
use ingestion::HypixelClient;
use notify::{FlipAlert, FlipDeduplicator, NotificationHub};
use pricing::{PriceCache, PriceEntry};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use storage::SnapshotStore;
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

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

        while let Some(snapshot) = rx.recv().await {
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
            let mut cheapest_bin: HashMap<Fingerprint, PriceEntry> = HashMap::new();
            let mut flip_candidates: Vec<FlipAlert> = Vec::new();
            let mut below_threshold = 0usize;

            for item in &parsed {
                let fp = fingerprint::fingerprint(item);
                unique_fingerprints.insert(fp);

                let cached_price = price_cache.get(fp);

                diag_evaluated_total += 1;
                if cached_price.is_some() {
                    diag_cache_hit_total += 1;
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
                        flip_candidates.push(FlipAlert::new(
                            item.uuid.clone(),
                            item.display_name.clone(),
                            profit.buy_price,
                            profit.estimated_value,
                            profit.expected_profit,
                            profit.roi_percent,
                            item.end,
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
                // cheapest observed BIN listing per fingerprint — a
                // placeholder cheap enough to compute inline, not a
                // real fair-value estimate (median, outlier-trimmed,
                // etc.). The engine only consumes whatever value the
                // cache is given; it doesn't validate how it was
                // derived.
                if item.bin {
                    cheapest_bin
                        .entry(fp)
                        .and_modify(|existing| {
                            existing.sample_size += 1;
                            existing.estimated_value =
                                existing.estimated_value.min(item.starting_bid);
                        })
                        .or_insert(PriceEntry {
                            estimated_value: item.starting_bid,
                            sample_size: 1,
                            updated_at_tick: tick,
                        });
                }
            }

            let unique_fingerprint_count = unique_fingerprints.len();
            let priced_fingerprint_count = cheapest_bin.len();
            price_cache.update_batch(cheapest_bin);

            // Dedup (sync, in-memory, batched once per tick) then
            // publish (non-blocking) — see the notify crate's module
            // docs for why this ordering and these two primitives never
            // add latency to detection.
            let new_flips = dedup.filter_new(flip_candidates, tick);
            let flips_found = new_flips.len();
            for alert in &new_flips {
                notification_hub.publish(alert);
                info!(
                    uuid = %alert.auction_uuid,
                    item = %alert.item_name,
                    buy_price = alert.buy_price,
                    estimated_value = alert.estimated_value,
                    profit = alert.profit,
                    roi_percent = alert.roi_percent,
                    viewauction = %alert.viewauction_command,
                    "flip detected"
                );
            }

            let parsed_count = parsed.len();
            if let Err(err) = store.store(tick, parsed).await {
                warn!(tick, error = %err, "failed to persist parsed auction batch");
            }

            info!(
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
                // TEMPORARY DIAGNOSTIC INSTRUMENTATION, cumulative since
                // process start (see the comment above the receiver
                // task's diag_* declarations) — remove once the
                // flips_found=0 root cause is confirmed and fixed.
                diag_evaluated_total,
                diag_cache_hit_total,
                diag_not_bin_total,
                diag_no_price_data_total,
                diag_insufficient_sample_total,
                diag_stale_price_total,
                diag_invalid_price_total,
                diag_implausible_roi_total,
                diag_below_threshold_total,
                diag_max_expected_profit_ever = ?diag_max_expected_profit_ever,
                diag_max_roi_percent_ever = ?diag_max_roi_percent_ever,
                "diffed, parsed, fingerprinted, evaluated, priced, deduped, notified, and stored snapshot"
            );
        }
    });

    if let Err(err) = client.run(0, tx).await {
        error!(error = %err, "ingestion loop exited with an error");
        std::process::exit(1);
    }

    let _ = receiver.await;
}
