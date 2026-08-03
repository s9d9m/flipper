//! Flip deduplication and WebSocket notification.
//!
//! Phase 1.8 + 1.9: the last two stages of the pipeline, after
//! `engine::evaluate` has already decided an auction is a
//! `FlipVerdict::Flip` (`engine` is not a dependency of this crate —
//! see below).
//!
//! ```text
//! engine::evaluate() -> Flip -> FlipDeduplicator -> NotificationHub::publish() -> [async] WebSocket fan-out
//! ```
//!
//! This crate deliberately does not depend on `parser` or `engine`:
//! [`FlipAlert`] is built from primitive fields by the caller (which
//! already has both `ParsedItem` and `engine::ProfitCalculation` in
//! scope at the point a `Flip` verdict comes back), keeping this crate
//! trivially testable and decoupled, the same reasoning `engine` uses
//! for not depending on `pricing::PriceCache` or
//! `fingerprint::Fingerprint`.
//!
//! # Why `broadcast`, not `mpsc`
//!
//! [`NotificationHub::publish`] sends through a `tokio::sync::broadcast`
//! channel, not the bounded `mpsc` used elsewhere in this workspace
//! (e.g. `storage::SnapshotStore`). That's a deliberate, load-bearing
//! choice: `mpsc::Sender::send` can block the caller when the channel is
//! full — acceptable for storage, where backpressure is an intentional
//! signal — but never acceptable here. `broadcast::Sender::send` is
//! synchronous, O(1)-ish, and never backpressures on a slow or absent
//! receiver; a lagging receiver just misses old messages, it never makes
//! the sender wait.
//!
//! # Where JSON serialization happens
//!
//! `publish()` serializes a [`FlipAlert`] into a shared `Arc<str>`
//! *once*, at publish time (which only happens on an actual flip — rare
//! relative to per-tick auction volume). Each connected client's own
//! per-connection task then just clones that `Arc` (a refcount bump) and
//! writes it to its socket — no serialization work happens per client,
//! and none of it happens inside the detection loop beyond building that
//! one `Arc<str>`.

use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, warn};

/// A confirmed, ready-to-send flip notification. Constructed by the
/// caller from `ParsedItem` + `engine::ProfitCalculation` fields — see
/// the module docs for why this crate doesn't depend on those types
/// directly.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FlipAlert {
    pub auction_uuid: String,
    /// Ready-to-paste Hypixel command, e.g. `/viewauction <uuid>`.
    pub viewauction_command: String,
    pub item_name: String,
    pub buy_price: u64,
    pub estimated_value: u64,
    /// Signed, matching `engine::ProfitCalculation::expected_profit`.
    pub profit: i64,
    pub roi_percent: f64,
    /// Which pricing tier the estimate came from (`"Exact"` /
    /// `"Major"` / `"Base"`) — output/readability session. A `&'static
    /// str`, not a `pricing::PriceTier`, so this crate still doesn't
    /// depend on `pricing` (same decoupling reasoning as the rest of
    /// this module doc comment): the caller already has the tier in
    /// scope from `engine::ProfitCalculation` and just picks the label.
    pub tier: &'static str,
    /// Where the price came from (`"Live"` this-tick observation or
    /// `"COFL"` historical backfill) — output/readability session, same
    /// `&'static str`-not-`pricing::PriceSource` reasoning as `tier`.
    pub price_source: &'static str,
    /// The auction's own end timestamp (Hypixel `end`, unix millis).
    /// Not part of the public alert payload — used only internally by
    /// [`FlipDeduplicator`] to know when it's safe to forget this uuid,
    /// mirroring `diff::DiffDetector`'s end-timestamp self-pruning.
    #[serde(skip)]
    pub auction_end: i64,
}

impl FlipAlert {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        auction_uuid: String,
        item_name: String,
        buy_price: u64,
        estimated_value: u64,
        profit: i64,
        roi_percent: f64,
        auction_end: i64,
        tier: &'static str,
        price_source: &'static str,
    ) -> Self {
        let viewauction_command = format!("/viewauction {auction_uuid}");
        Self {
            auction_uuid,
            viewauction_command,
            item_name,
            buy_price,
            estimated_value,
            profit,
            roi_percent,
            tier,
            price_source,
            auction_end,
        }
    }
}

/// Tracks which auction uuids have already been alerted on, so the same
/// auction isn't re-reported every tick it remains listed. In-memory
/// only, no TTL timer: an entry is dropped once its auction's own `end`
/// timestamp has passed as of the current tick, the same self-pruning
/// idiom `diff::DiffDetector` already uses for the identical shaped
/// problem — an ended auction can never legally reappear, so forgetting
/// it is always safe.
#[derive(Debug, Default)]
pub struct FlipDeduplicator {
    alerted: HashMap<String, i64>,
}

impl FlipDeduplicator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of auction uuids currently tracked as "already alerted".
    /// Exposed for instrumentation, not used internally.
    pub fn tracked_count(&self) -> usize {
        self.alerted.len()
    }

    /// Filters `candidates` down to the ones not already alerted on,
    /// marking each survivor as now-alerted, then prunes any tracked
    /// uuid whose auction has ended as of `current_tick`. Processes a
    /// whole tick's worth of flip candidates in one batch (mirroring
    /// `diff::DiffDetector::diff`) rather than pruning per-candidate.
    pub fn filter_new(&mut self, candidates: Vec<FlipAlert>, current_tick: i64) -> Vec<FlipAlert> {
        let mut new_flips = Vec::with_capacity(candidates.len());

        for candidate in candidates {
            if !self.alerted.contains_key(&candidate.auction_uuid) {
                self.alerted
                    .insert(candidate.auction_uuid.clone(), candidate.auction_end);
                new_flips.push(candidate);
            }
        }

        self.alerted.retain(|_, end| *end > current_tick);

        new_flips
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NotifyError {
    #[error("failed to bind websocket listener on {addr}: {source}")]
    Bind {
        addr: String,
        source: std::io::Error,
    },
}

/// Owns the fan-out channel and the WebSocket accept loop. Cheap to
/// share behind an `Arc` — `publish` is `&self` and never blocks.
pub struct NotificationHub {
    sender: broadcast::Sender<Arc<str>>,
}

impl NotificationHub {
    /// `capacity` is how many recent alerts a slow receiver can fall
    /// behind by before it starts missing them — irrelevant to the
    /// publisher, which never blocks regardless of this value.
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Fire-and-forget: serializes `alert` once and broadcasts it to
    /// every currently-connected client. Non-blocking — see the module
    /// docs for why `broadcast` guarantees this. Returns the number of
    /// currently-subscribed connections (0 if nobody's connected, which
    /// is not an error — the publish still "succeeds", there's just
    /// nobody to deliver to yet).
    pub fn publish(&self, alert: &FlipAlert) -> usize {
        let payload: Arc<str> = match serde_json::to_string(alert) {
            Ok(json) => Arc::from(json),
            Err(err) => {
                warn!(error = %err, uuid = %alert.auction_uuid, "failed to serialize flip alert");
                return 0;
            }
        };

        self.sender.send(payload).unwrap_or(0)
    }

    /// Binds the listening socket. Separate from [`Self::serve`] so
    /// callers (and tests) can bind to an OS-assigned port (`:0`) and
    /// read back the real address before the accept loop starts.
    pub async fn bind(addr: &str) -> Result<TcpListener, NotifyError> {
        TcpListener::bind(addr)
            .await
            .map_err(|source| NotifyError::Bind {
                addr: addr.to_string(),
                source,
            })
    }

    /// Runs the accept loop forever: every accepted connection gets its
    /// own spawned task forwarding broadcast alerts to that client. A
    /// failed handshake or a single client's I/O error only ends that
    /// one connection's task, never the accept loop or other clients.
    pub async fn serve(hub: Arc<Self>, listener: TcpListener) {
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    // Subscribed *before* the WebSocket handshake even
                    // starts, so there's no window where a client could
                    // finish connecting before the hub is ready to
                    // deliver to it.
                    let rx = hub.sender.subscribe();
                    tokio::spawn(async move {
                        debug!(%addr, "websocket client connecting");
                        handle_connection(stream, rx).await;
                        debug!(%addr, "websocket client disconnected");
                    });
                }
                Err(err) => {
                    warn!(error = %err, "failed to accept websocket connection");
                }
            }
        }
    }
}

async fn handle_connection(stream: TcpStream, mut rx: broadcast::Receiver<Arc<str>>) {
    let ws_stream = match tokio_tungstenite::accept_async(stream).await {
        Ok(ws) => ws,
        Err(err) => {
            warn!(error = %err, "websocket handshake failed");
            return;
        }
    };

    let (mut sink, mut incoming) = ws_stream.split();

    loop {
        tokio::select! {
            alert = rx.recv() => {
                match alert {
                    Ok(payload) => {
                        if sink.send(Message::Text(payload.as_ref().into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "websocket client fell behind, some alerts were dropped for it");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            incoming_msg = incoming.next() => {
                match incoming_msg {
                    None | Some(Ok(Message::Close(_))) | Some(Err(_)) => break,
                    // Clients aren't expected to send anything meaningful
                    // (this is a broadcast-only feed); ignore pings/other
                    // frames rather than reacting to them.
                    Some(Ok(_)) => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn alert(uuid: &str, end: i64) -> FlipAlert {
        FlipAlert::new(
            uuid.to_string(),
            "Hyperion".to_string(),
            1_000_000,
            2_000_000,
            980_000,
            98.0,
            end,
            "Exact",
            "Live",
        )
    }

    #[test]
    fn viewauction_command_is_formatted_from_the_uuid() {
        let a = alert("abc-123", 1_000);
        assert_eq!(a.viewauction_command, "/viewauction abc-123");
    }

    #[test]
    fn serialized_alert_has_the_required_fields_and_omits_auction_end() {
        let a = alert("abc-123", 1_000);
        let value = serde_json::to_value(&a).unwrap();

        assert_eq!(value["auction_uuid"], "abc-123");
        assert_eq!(value["viewauction_command"], "/viewauction abc-123");
        assert_eq!(value["item_name"], "Hyperion");
        assert_eq!(value["buy_price"], 1_000_000);
        assert_eq!(value["estimated_value"], 2_000_000);
        assert_eq!(value["profit"], 980_000);
        assert_eq!(value["roi_percent"], 98.0);
        assert_eq!(value["tier"], "Exact");
        assert_eq!(value["price_source"], "Live");
        assert!(value.get("auction_end").is_none());
    }

    #[test]
    fn first_occurrence_of_a_uuid_is_reported_as_new() {
        let mut dedup = FlipDeduplicator::new();
        let survivors = dedup.filter_new(vec![alert("a", 5_000)], 1_000);

        assert_eq!(survivors.len(), 1);
        assert_eq!(dedup.tracked_count(), 1);
    }

    #[test]
    fn repeat_occurrence_in_a_later_batch_is_filtered_out() {
        let mut dedup = FlipDeduplicator::new();
        dedup.filter_new(vec![alert("a", 5_000)], 1_000);

        let survivors = dedup.filter_new(vec![alert("a", 5_000)], 1_500);
        assert!(survivors.is_empty());
    }

    #[test]
    fn duplicate_uuid_within_the_same_batch_is_only_kept_once() {
        let mut dedup = FlipDeduplicator::new();
        let survivors = dedup.filter_new(vec![alert("a", 5_000), alert("a", 5_000)], 1_000);

        assert_eq!(survivors.len(), 1);
    }

    #[test]
    fn distinct_uuids_in_the_same_batch_are_all_kept() {
        let mut dedup = FlipDeduplicator::new();
        let survivors = dedup.filter_new(
            vec![alert("a", 5_000), alert("b", 5_000), alert("c", 5_000)],
            1_000,
        );

        assert_eq!(survivors.len(), 3);
        assert_eq!(dedup.tracked_count(), 3);
    }

    #[test]
    fn expired_auction_is_pruned_and_uuid_reuse_is_treated_as_new() {
        let mut dedup = FlipDeduplicator::new();
        // Ends at 1_200.
        dedup.filter_new(vec![alert("a", 1_200)], 1_000);

        // Evaluated again at tick 1_500 (after the auction ended) with a
        // different candidate batch not containing "a" — this is the
        // tick where "a" gets pruned.
        dedup.filter_new(vec![alert("b", 5_000)], 1_500);
        assert_eq!(dedup.tracked_count(), 1); // only "b" remains

        // A hypothetical uuid reuse after pruning is treated as new,
        // exactly like diff::DiffDetector's equivalent case.
        let survivors = dedup.filter_new(vec![alert("a", 5_000)], 1_600);
        assert_eq!(survivors.len(), 1);
    }

    #[test]
    fn publish_with_no_subscribers_does_not_error_or_panic() {
        let hub = NotificationHub::new(16);
        let delivered = hub.publish(&alert("a", 1_000));
        assert_eq!(delivered, 0);
    }

    #[tokio::test]
    async fn published_alert_reaches_a_connected_websocket_client() {
        let listener = NotificationHub::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();
        let hub = Arc::new(NotificationHub::new(16));

        tokio::spawn(NotificationHub::serve(Arc::clone(&hub), listener));

        let (ws_stream, _response) = tokio_tungstenite::connect_async(format!("ws://{local_addr}"))
            .await
            .expect("client should connect");
        let (_write, mut read) = ws_stream.split();

        // Give the accept loop a moment to run and subscribe before
        // publishing — accept_async completing on the client side
        // implies the server's handshake completed too, but the
        // subscribe() happens just before that on the server's accept
        // loop, so this is a safety margin, not a hard requirement.
        tokio::time::sleep(Duration::from_millis(20)).await;

        let delivered = hub.publish(&alert("flip-uuid", 1_000));
        assert_eq!(delivered, 1);

        let msg = tokio::time::timeout(Duration::from_secs(2), read.next())
            .await
            .expect("should receive a message before timing out")
            .expect("stream should not end")
            .expect("should not be a websocket error");

        let text = msg.into_text().unwrap();
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["auction_uuid"], "flip-uuid");
        assert_eq!(value["viewauction_command"], "/viewauction flip-uuid");
    }
}
