use common::{AuctionSnapshot, Config};
use diff::DiffDetector;
use fingerprint::Fingerprint;
use ingestion::HypixelClient;
use pricing::{PriceCache, PriceEntry};
use std::collections::{HashMap, HashSet};
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

    // Shared with nothing yet inside this binary — the flip detector and
    // profit calculator that will read from it are later, unbuilt steps
    // — but wrapping it in Arc now models how it'll actually be used
    // once a hot-path reader exists on another task.
    let price_cache = std::sync::Arc::new(PriceCache::new());

    // Bounded channel: backpressure here is a deliberate signal that
    // downstream processing (diff detection, in Phase 1.3) isn't keeping
    // up — better to surface that loudly than to buffer unboundedly.
    let (tx, mut rx) = mpsc::channel::<AuctionSnapshot>(4);

    let receiver = tokio::spawn(async move {
        let mut detector = DiffDetector::new();

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

            // Fingerprinting + price cache feed. NOTE: `estimated_value`
            // here is just this tick's cheapest observed BIN listing per
            // fingerprint — a placeholder cheap enough to compute inline,
            // not a real fair-value estimate (median, outlier-trimmed,
            // etc.). That's the profit-calculation engine's job (a later,
            // unbuilt step); this cache only stores/serves whatever value
            // it's given.
            let mut unique_fingerprints: HashSet<Fingerprint> =
                HashSet::with_capacity(parsed.len());
            let mut cheapest_bin: HashMap<Fingerprint, PriceEntry> = HashMap::new();

            for item in &parsed {
                let fp = fingerprint::fingerprint(item);
                unique_fingerprints.insert(fp);

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
                tracked_live = detector.tracked_count(),
                "diffed, parsed, fingerprinted, priced, and stored snapshot"
            );
        }
    });

    if let Err(err) = client.run(0, tx).await {
        error!(error = %err, "ingestion loop exited with an error");
        std::process::exit(1);
    }

    let _ = receiver.await;
}
