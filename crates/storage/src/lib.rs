//! Auction snapshot storage.
//!
//! Phase 2 storage stage: persists parsed auctions off the hot path so
//! prices can be analyzed later. The `rusqlite::Connection` lives on its
//! own OS thread, fed via a bounded async channel — nothing in the
//! ingestion/diff/parse pipeline shares it or blocks waiting on disk I/O
//! beyond handing a batch to that channel, per claude.md's "no DB on the
//! hot path" rule.
//!
//! Deliberately SQLite, not ClickHouse/Postgres: no server to run, a
//! single embedded file is enough to start analyzing historical prices.
//! This crate is the only thing in the workspace that knows it's SQLite —
//! swapping the backend later only touches this file.

use parser::ParsedItem;
use rusqlite::{params, Connection};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, mpsc::Sender, oneshot};
use tracing::warn;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("storage writer thread is no longer accepting batches")]
    WriterClosed,
}

struct Batch {
    tick: i64,
    items: Vec<ParsedItem>,
    ack: oneshot::Sender<Result<(), StorageError>>,
}

/// Handle to a background SQLite writer. Cheap to hold onto (just a
/// channel sender) — the connection itself never leaves the writer
/// thread.
pub struct SnapshotStore {
    tx: Sender<Batch>,
}

impl SnapshotStore {
    /// Opens (or creates) the SQLite database at `db_path`, ensures the
    /// schema exists, and starts the background writer thread.
    pub fn open(db_path: &str) -> Result<Self, StorageError> {
        let conn = Connection::open(db_path)?;
        init_schema(&conn)?;

        let (tx, rx) = mpsc::channel::<Batch>(32);
        std::thread::Builder::new()
            .name("storage-writer".into())
            .spawn(move || run_writer(conn, rx))
            .expect("failed to spawn storage writer thread");

        Ok(Self { tx })
    }

    /// Queues one snapshot tick's worth of parsed auctions for
    /// persistence and waits for the writer thread to durably commit
    /// them. A no-op for an empty batch.
    pub async fn store(&self, tick: i64, items: Vec<ParsedItem>) -> Result<(), StorageError> {
        if items.is_empty() {
            return Ok(());
        }

        let (ack, ack_rx) = oneshot::channel();
        self.tx
            .send(Batch { tick, items, ack })
            .await
            .map_err(|_| StorageError::WriterClosed)?;

        ack_rx.await.map_err(|_| StorageError::WriterClosed)?
    }
}

fn init_schema(conn: &Connection) -> Result<(), StorageError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS auctions (
            uuid              TEXT    NOT NULL,
            tick              INTEGER NOT NULL,
            auctioneer        TEXT    NOT NULL,
            skyblock_item_id  TEXT    NOT NULL,
            display_name      TEXT    NOT NULL,
            item_count        INTEGER NOT NULL,
            starting_bid      INTEGER NOT NULL,
            bin               INTEGER NOT NULL,
            end_time          INTEGER NOT NULL,
            observed_at       INTEGER NOT NULL,
            PRIMARY KEY (uuid, tick)
        );
        CREATE INDEX IF NOT EXISTS idx_auctions_item_tick
            ON auctions (skyblock_item_id, tick);",
    )?;
    Ok(())
}

fn run_writer(mut conn: Connection, mut rx: mpsc::Receiver<Batch>) {
    while let Some(batch) = rx.blocking_recv() {
        let result = write_batch(&mut conn, batch.tick, &batch.items);
        if let Err(err) = &result {
            warn!(error = %err, tick = batch.tick, "failed to persist auction batch");
        }
        let _ = batch.ack.send(result);
    }
}

fn write_batch(conn: &mut Connection, tick: i64, items: &[ParsedItem]) -> Result<(), StorageError> {
    let observed_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare_cached(
            "INSERT OR REPLACE INTO auctions
                (uuid, tick, auctioneer, skyblock_item_id, display_name,
                 item_count, starting_bid, bin, end_time, observed_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        )?;

        for item in items {
            stmt.execute(params![
                item.uuid,
                tick,
                item.auctioneer,
                item.skyblock_item_id,
                item.display_name,
                item.count,
                item.starting_bid as i64,
                item.bin,
                item.end,
                observed_at,
            ])?;
        }
    }
    tx.commit()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_db_path() -> std::path::PathBuf {
        let n = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "flipper-storage-test-{}-{}.sqlite3",
            std::process::id(),
            n
        ))
    }

    fn sample_item(uuid: &str, skyblock_item_id: &str) -> ParsedItem {
        ParsedItem {
            uuid: uuid.to_string(),
            auctioneer: "seller".to_string(),
            skyblock_item_id: skyblock_item_id.to_string(),
            display_name: "Hyperion".to_string(),
            count: 1,
            starting_bid: 900_000_000,
            bin: true,
            end: 1_690_003_600_000,
            extra_attributes: None,
        }
    }

    #[tokio::test]
    async fn stores_and_persists_a_batch() {
        let path = temp_db_path();
        let store = SnapshotStore::open(path.to_str().unwrap()).unwrap();

        store
            .store(
                1_000,
                vec![sample_item("a", "HYPERION"), sample_item("b", "HYPERION")],
            )
            .await
            .unwrap();

        let conn = Connection::open(&path).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM auctions", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);

        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn empty_batch_is_a_no_op() {
        let path = temp_db_path();
        let store = SnapshotStore::open(path.to_str().unwrap()).unwrap();

        store.store(1_000, vec![]).await.unwrap();

        let conn = Connection::open(&path).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM auctions", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);

        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn same_uuid_and_tick_overwrites_rather_than_duplicates() {
        let path = temp_db_path();
        let store = SnapshotStore::open(path.to_str().unwrap()).unwrap();

        let mut item = sample_item("a", "HYPERION");
        store.store(1_000, vec![item.clone()]).await.unwrap();

        item.starting_bid = 950_000_000;
        store.store(1_000, vec![item]).await.unwrap();

        let conn = Connection::open(&path).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM auctions", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);

        let starting_bid: i64 = conn
            .query_row("SELECT starting_bid FROM auctions", [], |row| row.get(0))
            .unwrap();
        assert_eq!(starting_bid, 950_000_000);

        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn same_uuid_different_tick_keeps_price_history() {
        let path = temp_db_path();
        let store = SnapshotStore::open(path.to_str().unwrap()).unwrap();

        store
            .store(1_000, vec![sample_item("a", "HYPERION")])
            .await
            .unwrap();
        store
            .store(2_000, vec![sample_item("a", "HYPERION")])
            .await
            .unwrap();

        let conn = Connection::open(&path).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM auctions", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);

        std::fs::remove_file(&path).ok();
    }
}
