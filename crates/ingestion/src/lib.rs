//! Hypixel auction ingestion.
//!
//! Responsibility of this crate, and *only* this crate:
//! 1. Detect when Hypixel's auction cache has refreshed, as fast as
//!    reasonably possible, without hammering the full paginated endpoint
//!    on every check.
//! 2. Once a new tick is detected, fetch every page concurrently and
//!    assemble a complete snapshot.
//! 3. Hand that snapshot off (via an mpsc channel) to whatever consumes
//!    it next — diff detection, in Phase 1.3. This crate does not parse
//!    items, does not compute prices, and does not decide what's a flip.
//! That separation is deliberate: this is the one stage of the pipeline
//! gated by something outside our control (Hypixel's cache window), so it
//! needs to be independently benchmarkable and swappable without touching
//! anything downstream.

use common::{AuctionPageResponse, AuctionSnapshot, Config};
use futures::future::join_all;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;
use tracing::{debug, info, warn};

#[derive(Debug, thiserror::Error)]
pub enum IngestionError {
    #[error("http request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("hypixel api returned success=false for page {page}")]
    ApiUnsuccessful { page: u32 },

    #[error("channel closed: downstream consumer is no longer receiving snapshots")]
    ChannelClosed,
}

pub struct HypixelClient {
    http: reqwest::Client,
    config: Config,
}

impl HypixelClient {
    pub fn new(config: Config) -> Result<Self, IngestionError> {
        let http = reqwest::Client::builder()
            // Persistent connection pool: avoids paying a TLS handshake
            // on every poll. This is one of the concrete latency wins
            // called out in the design doc's "receive data" stage.
            .pool_idle_timeout(Duration::from_secs(90))
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .build()?;

        Ok(Self { http, config })
    }

    /// Fetches a single page of the auctions endpoint.
    async fn fetch_page(&self, page: u32) -> Result<AuctionPageResponse, IngestionError> {
        let url = format!(
            "{}/skyblock/auctions?page={}",
            self.config.hypixel_base_url, page
        );

        let response = self
            .http
            .get(&url)
            .header("API-Key", &self.config.hypixel_api_key)
            .send()
            .await?
            .error_for_status()?
            .json::<AuctionPageResponse>()
            .await?;

        if !response.success {
            return Err(IngestionError::ApiUnsuccessful { page });
        }

        Ok(response)
    }

    /// Cheap tick-detection loop. Repeatedly fetches only page 0 (the
    /// smallest possible request that still carries `lastUpdated`) at
    /// `tick_poll_interval_ms` cadence, and returns as soon as the
    /// timestamp differs from `last_known_tick`.
    ///
    /// This is intentionally separate from `fetch_full_snapshot` so the
    /// expensive concurrent multi-page fetch only ever runs once per
    /// actual cache refresh, not once per poll interval.
    async fn wait_for_new_tick(
        &self,
        last_known_tick: i64,
    ) -> Result<AuctionPageResponse, IngestionError> {
        loop {
            let page0 = self.fetch_page(0).await?;

            if page0.last_updated != last_known_tick {
                return Ok(page0);
            }

            debug!(last_known_tick, "no new tick yet, sleeping");
            sleep(Duration::from_millis(self.config.tick_poll_interval_ms)).await;
        }
    }

    /// Fetches every remaining page concurrently and merges them with the
    /// already-fetched page 0 into one snapshot.
    async fn fetch_full_snapshot(
        &self,
        first_page: AuctionPageResponse,
    ) -> Result<AuctionSnapshot, IngestionError> {
        let tick = first_page.last_updated;
        let total_pages = first_page.total_pages;
        let mut auctions = first_page.auctions;

        if total_pages > 1 {
            let remaining_fetches = (1..total_pages).map(|page| self.fetch_page(page));
            let results = join_all(remaining_fetches).await;

            for result in results {
                match result {
                    Ok(page) => {
                        if page.last_updated != tick {
                            // The cache ticked again mid-fetch. For the MVP
                            // we log and keep going with what we have rather
                            // than aborting — a slightly stale page is far
                            // better than no snapshot at all, and the very
                            // next tick-detection cycle will catch up.
                            warn!(
                                expected_tick = tick,
                                got_tick = page.last_updated,
                                "tick changed mid-snapshot-fetch"
                            );
                        }
                        auctions.extend(page.auctions);
                    }
                    Err(err) => {
                        warn!(error = %err, "failed to fetch a page of this snapshot");
                    }
                }
            }
        }

        Ok(AuctionSnapshot {
            last_updated: tick,
            auctions,
        })
    }

    /// Runs forever: detect a new tick, fetch the full snapshot, send it
    /// downstream, repeat. `last_known_tick` should start at `0` (or any
    /// value that can't collide with a real Hypixel timestamp) so the
    /// very first iteration always treats page 0's timestamp as new.
    pub async fn run(
        &self,
        mut last_known_tick: i64,
        tx: Sender<AuctionSnapshot>,
    ) -> Result<(), IngestionError> {
        loop {
            let detect_start = Instant::now();
            let first_page = self.wait_for_new_tick(last_known_tick).await?;
            let detect_latency = detect_start.elapsed();

            let fetch_start = Instant::now();
            let snapshot = self.fetch_full_snapshot(first_page).await?;
            let fetch_latency = fetch_start.elapsed();

            info!(
                tick = snapshot.last_updated,
                auction_count = snapshot.auctions.len(),
                detect_latency_ms = detect_latency.as_millis(),
                snapshot_fetch_latency_ms = fetch_latency.as_millis(),
                "assembled new auction snapshot"
            );

            last_known_tick = snapshot.last_updated;

            tx.send(snapshot)
                .await
                .map_err(|_| IngestionError::ChannelClosed)?;
        }
    }
}
