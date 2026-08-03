use common::Config;
use ingestion::HypixelClient;
use serde_json::json;
use tokio::sync::mpsc;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn test_config(base_url: String) -> Config {
    Config {
        hypixel_api_key: "test-key".to_string(),
        hypixel_base_url: base_url,
        // Fast polling for the test so it doesn't take real wall-clock
        // seconds to observe a tick change.
        tick_poll_interval_ms: 10,
        request_timeout_ms: 2000,
        storage_db_path: ":memory:".to_string(),
        websocket_bind_addr: "127.0.0.1:0".to_string(),
        cofl_backfill_enabled: false,
        cofl_base_url: "http://127.0.0.1:0".to_string(),
        cofl_backfill_item_tags: Vec::new(),
        cofl_backfill_pages_per_tag: 0,
    }
}

fn auction(uuid: &str) -> serde_json::Value {
    json!({
        "uuid": uuid,
        "auctioneer": "seller-uuid",
        "item_name": "Hyperion",
        "starting_bid": 900_000_000u64,
        "item_bytes": "base64gzipdata==",
        "bin": true,
        "end": 1_690_003_600_000i64
    })
}

fn page_response(
    page: u32,
    total_pages: u32,
    last_updated: i64,
    uuids: &[&str],
) -> serde_json::Value {
    json!({
        "success": true,
        "page": page,
        "totalPages": total_pages,
        "totalAuctions": uuids.len(),
        "lastUpdated": last_updated,
        "auctions": uuids.iter().map(|u| auction(u)).collect::<Vec<_>>()
    })
}

/// Verifies the full happy path: page 0 shows an unchanged tick a couple
/// of times (so the poll loop must actually loop, not just check once),
/// then the tick changes and a two-page snapshot is fetched and merged.
#[tokio::test]
async fn detects_new_tick_and_assembles_full_snapshot() {
    let server = MockServer::start().await;

    // Page 0 is polled repeatedly. wiremock serves mocks in the order
    // they'd match unless scoped, so we use `up_to_n_times` to return the
    // stale tick twice before the fresh one takes over.
    Mock::given(method("GET"))
        .and(path("/skyblock/auctions"))
        .and(query_param("page", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page_response(
            0,
            2,
            1_000,
            &["stale-a"],
        )))
        .up_to_n_times(2)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/skyblock/auctions"))
        .and(query_param("page", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page_response(
            0,
            2,
            2_000,
            &["fresh-a"],
        )))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/skyblock/auctions"))
        .and(query_param("page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page_response(
            1,
            2,
            2_000,
            &["fresh-b"],
        )))
        .mount(&server)
        .await;

    let client = HypixelClient::new(test_config(server.uri())).unwrap();
    let (tx, mut rx) = mpsc::channel(1);

    // Start already "at" the stale tick (1000), so wait_for_new_tick has
    // to actually poll a couple of times and observe the 1000 -> 2000
    // transition rather than trivially returning on the first request.
    tokio::spawn(async move {
        let _ = client.run(1_000, tx).await;
    });

    let snapshot = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for a snapshot")
        .expect("channel closed without producing a snapshot");

    assert_eq!(snapshot.last_updated, 2_000);
    assert_eq!(snapshot.auctions.len(), 2);

    let uuids: Vec<&str> = snapshot.auctions.iter().map(|a| a.uuid.as_str()).collect();
    assert!(uuids.contains(&"fresh-a"));
    assert!(uuids.contains(&"fresh-b"));
}
