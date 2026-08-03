use common::{AuctionSnapshot, Config};
use diff::DiffDetector;
use ingestion::HypixelClient;
use std::collections::HashSet;
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

            // Fingerprinting stage: not consumed by anything yet (the RAM
            // price cache is the next unbuilt step), but computing it here
            // keeps the pipeline shape matching the architecture diagram
            // and gives an early, cheap signal of how much duplicate-item
            // collapsing the price cache will get to do once it exists.
            let unique_fingerprints: HashSet<_> =
                parsed.iter().map(fingerprint::fingerprint).collect();

            let parsed_count = parsed.len();
            let unique_fingerprint_count = unique_fingerprints.len();
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
                tracked_live = detector.tracked_count(),
                "diffed, parsed, fingerprinted, and stored snapshot"
            );
        }
    });

    if let Err(err) = client.run(0, tx).await {
        error!(error = %err, "ingestion loop exited with an error");
        std::process::exit(1);
    }

    let _ = receiver.await;
}
