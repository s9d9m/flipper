# claude.md — Project Context for skyblock-flipper

This file exists to bring a fresh Claude Code CLI session up to speed on
this project without re-explaining it from scratch. Read this before
making changes.

## What this project is

A Hypixel SkyBlock Auction House **flip finder**: detects profitable BIN
(buy-it-now) auctions and alerts the user, faster than SkyCofl Premium+.

## Priority order (this governs every decision)

1. Auction detection speed
2. Processing latency
3. Notification speed
4. Accuracy of flip detection
5. Historical analytics / features

**A feature that improves analytics but adds latency gets delayed.**
Historical databases, dashboards, and full analytics run asynchronously,
off the critical path, after a flip has already been detected and sent.

## The one fact that shapes the whole architecture

Hypixel's `/skyblock/auctions` endpoint is cached server-side for
roughly 60 seconds and is paginated. There is no push feed — every
competitor, including SkyCofl, is polling the same capped resource.
"Sub-100ms flip detection" means sub-100ms *from the moment we observe
new data*, not from when the auction was actually created. The real
competitive edge comes from:
- detecting Hypixel's cache refresh (the "tick") as fast as possible
- processing what we get with near-zero added latency
- correct pricing, so a fast wrong answer doesn't beat a fair-value one

## Architecture decisions already made (with rationale)

- **Language: Rust** for the entire hot path (poller → parser →
  fingerprint → price lookup → profit calc → notify). No GC pauses, no
  heap allocation on the hot path where avoidable. Go was considered
  for I/O-bound edge services (gateway) but the MVP keeps everything in
  Rust for now.
- **No database, no ML inference, no network hop of any kind on the hot
  path.** These were deliberately removed from the original "analytics
  platform" design when the goal was reframed to pure latency. ClickHouse
  (historical sales), PostgreSQL (user config), and Redis (secondary
  cache) all exist only in the async/background lane.
- **Messaging: NATS**, not Kafka — chosen for latency and operational
  simplicity, not durability (flips are worthless seconds after Hypixel's
  next tick). NATS only sits between "matched flip" and "fan-out to many
  connected clients," not between internal pipeline stages — those are
  plain in-process function calls.
- **Notifications: WebSocket is the primary and only latency-critical
  channel.** Discord webhooks and browser push are secondary, best-effort,
  fired after the WebSocket send, never blocking or gating it (Discord
  webhooks specifically add 50–300ms+ and have their own rate limits —
  unacceptable on the hot path).
- **Item fingerprinting is two-tier and *lossy* on the hot path:** a
  minimal fingerprint (item ID + only the modifiers that affect price)
  is computed at flip-detection time with zero heap allocation. Full
  normalization (every gemstone slot, every attribute, dyes, skins) is
  deferred to an async pass after the flip is already dispatched.
- **Price cache lives entirely in RAM**, sharded by fingerprint hash,
  updated by a background task, read lock-free (RCU/atomic-swap style).
  The hot path never blocks on a cache miss — it skips and lets the
  background system price the item for next time.

## Sniper-mode critical path (the only thing the hot path should do)

```
Auction Feed → Rust Processor → RAM Price Cache → Profit Calculator → WebSocket Notification
```

Everything else (ClickHouse writes, full item normalization, stats
recomputation, Discord/push delivery, confidence/risk scoring) is
fire-and-forget, off this path.

## Realistic latency budget (from design discussion)

```
Auction ingestion (tick detection + fetch, amortized):   5–30 ms   ← main lever
Parsing (zero-copy, per item):                            0.05–0.2 ms
Fingerprinting (minimal, hot-path variant):                 ~1–5 µs
Price lookup (RAM cache):                                   <1 µs
Profit calculation:                                          <1 µs
Threshold / filter match:                                     ~1–10 µs
Notification hand-off to socket buffer:                        0.1–1 ms
─────────────────────────────────────────────────────────────────
Compute total (parse → notify hand-off):                     ~0.2–1.3 ms
Full pipeline incl. ingestion:                                 ~5–30 ms
Network delivery to client (uncontrollable):                    +10–60 ms
```

Honest caveat: "ingestion" is a race against Hypixel's cache tick, not a
pure engineering problem — a naive fixed-interval poller can add
hundreds of ms to seconds of pure waste versus active tick-detection.

## Implementation rules for this codebase

- Production-quality code only. No pseudocode, no placeholders, no TODO
  comments.
- Every component must compile and run before moving to the next one.
- Keep latency-sensitive code simple and isolated in its own crate so it
  can be benchmarked independently later (Phase 3).
- Anything not required to detect a flip gets moved off the hot path.
- Build incrementally — one crate/component at a time, tested before
  moving on. Do not generate the whole project in one shot.

## Build order (roadmap)

**Phase 1 — Core Backend**
1. Rust project setup ✅ done
2. Hypixel auction ingestion service ✅ done (see below) — **verified
   live against the real Hypixel API**: ~46,800 auctions/snapshot
3. Auction diff detection ✅ done (see below)
4. Minimal item parser ✅ done (see below)
4.5. Auction/price storage ✅ done, added ahead of the original order
   (see below) — the user asked for this alongside the parser under the
   name "Phase 2: parsing and storage"; it is *not* the roadmap's
   "Phase 2 — Website" below, which is still unbuilt. Persistence was
   originally slated for after fingerprinting/pricing (step 6, "price
   cache") but pulling it forward doesn't block anything else and gives
   a place to accumulate price history immediately.
5. Item fingerprinting ✅ done (see below)
6. In-memory price cache — **next step, not yet built**
7. Profit calculation engine — not built
8. Flip detection — not built
9. WebSocket notification server — not built

Target output of Phase 1: a user connects to the site and receives live
profitable flip alerts.

**Phase 2 — Website:** live flip feed (item name, image, buy price,
estimated value, profit, ROI, `/viewauction` copy button), user settings
for minimum profit/ROI. No unnecessary UI polish. (Not started — don't
confuse with the "Phase 2: parsing and storage" naming used in session
chat above; that work is Phase 1 steps 4 and 4.5 in this roadmap.)

**Phase 3 — Optimization:** benchmark auction ingestion latency, parsing
latency, fingerprint generation, price lookup, notification latency —
then optimize whatever the actual measured bottleneck is (not a guess).

## Current repo state

```
skyblock-flipper/
├── Cargo.toml                  Workspace manifest. Shared release
│                                profile (opt-level=3, thin LTO, single
│                                codegen unit) so future benchmarks are
│                                comparing real release builds.
├── .env.example                 HYPIXEL_API_KEY, HYPIXEL_BASE_URL,
│                                TICK_POLL_INTERVAL_MS, REQUEST_TIMEOUT_MS,
│                                STORAGE_DB_PATH
├── .gitignore
├── README.md                    Explains file-by-file purpose, setup,
│                                and a toolchain gotcha (see below)
├── claude.md                    This file
└── crates/
    ├── common/                  Shared wire-format types + Config
    │   src/lib.rs                (RawAuction, AuctionPageResponse,
    │                             AuctionSnapshot, Config, ConfigError).
    │                             No tokio/reqwest dependency — kept
    │                             minimal since every other crate
    │                             depends on this one.
    └── ingestion/                DONE: Hypixel polling service.
        src/lib.rs                 HypixelClient: fetch_page(),
        │                          wait_for_new_tick() (cheap repeated
        │                          poll of page 0 only), fetch_full_
        │                          snapshot() (concurrent multi-page
        │                          fetch on tick change), run() (ties
        │                          it together, logs detect_latency_ms
        │                          / snapshot_fetch_latency_ms per
        │                          cycle — instrumentation exists from
        │                          day one for Phase 3).
        │                          Produces AuctionSnapshot values onto
        │                          an mpsc channel. Does NOT parse
        │                          items, price anything, or decide
        │                          what's a flip.
        src/main.rs                 Standalone runnable binary. Channel
        │                          receiver runs each snapshot through
        │                          diff::DiffDetector, then parser::
        │                          parse_item on each changed auction
        │                          (parse failures are logged and
        │                          skipped, not fatal), then
        │                          fingerprint::fingerprint on every
        │                          parsed item (collected into a
        │                          HashSet purely to log how many
        │                          unique fingerprints this batch
        │                          collapsed to — not consumed by
        │                          anything downstream yet, since the
        │                          price cache doesn't exist), then
        │                          hands the parsed batch to storage::
        │                          SnapshotStore::store(tick, items).
        │                          Logs total/changed/parsed/failed/
        │                          unique-fingerprint counts and the
        │                          diff detector's live-tracked count
        │                          per snapshot.
        tests/tick_detection.rs      Integration test against a local
                                    wiremock mock server — proves
                                    tick-detection + concurrent-fetch +
                                    merge logic without needing live
                                    Hypixel access.
    └── diff/                     DONE: auction diff detection.
        src/lib.rs                  DiffDetector: in-memory
                                   HashMap<uuid, SeenAuction> keyed by
                                   auction uuid, storing starting_bid +
                                   end. diff(snapshot) emits only
                                   auctions that are new or whose
                                   starting_bid/end changed since last
                                   seen, then self-prunes any entry
                                   whose end has passed as of the
                                   current tick — bounds the set to
                                   "currently live auctions" with no
                                   TTL timer, no database, no Redis.
                                   Synchronous, no tokio dependency.
                                   6 unit tests cover: first-snapshot
                                   all-new, unchanged filtered out,
                                   changed starting_bid re-emitted,
                                   expired-auction pruning, isolating a
                                   new auction among unchanged ones,
                                   and an empty-snapshot no-op.
    ├── parser/                    DONE: minimal item parser.
    │   src/lib.rs                  parse_item(&RawAuction) ->
    │                              Result<ParsedItem, ParseError>.
    │                              Decodes item_bytes: base64 (base64
    │                              crate) -> gzip (flate2) -> NBT
    │                              (fastnbt). Extracts skyblock_item_id
    │                              (ExtraAttributes.id), a color-code-
    │                              stripped display_name, stack count,
    │                              and the auction economics already on
    │                              RawAuction. ExtraAttributes beyond
    │                              `id` is kept as a raw fastnbt::Value
    │                              (`extra_attributes` field) for the
    │                              not-yet-built fingerprinting stage to
    │                              interpret — deliberately not modeling
    │                              every enchant/gem/hpb here, matching
    │                              the two-tier lossy design in this
    │                              file. Errors (bad base64, bad gzip,
    │                              bad NBT, empty item list, missing
    │                              skyblock id) are non-fatal to the
    │                              pipeline — main.rs logs and skips.
    │                              5 unit tests build synthetic item
    │                              NBT with fastnbt's own nbt!/to_bytes
    │                              and round-trip it through parse_item.
    ├── fingerprint/                DONE: hot-path item fingerprinting.
    │   src/lib.rs                   fingerprint(&ParsedItem) ->
    │                               Fingerprint(u64). Pure/sync, no I/O,
    │                               no async. Hashes skyblock_item_id
    │                               plus a fixed set of ExtraAttributes
    │                               fields known to move SkyBlock
    │                               prices: modifier (reforge),
    │                               rarity_upgrades (recombobulated),
    │                               hot_potato_count, dungeon_item_level
    │                               (stars), art_of_war_count, talisman_
    │                               enrichment, skin, ability_scroll
    │                               (sorted list), runes and
    │                               enchantments (sorted compounds), and
    │                               gems (sorted by slot, only each
    │                               slot's quality kept). Deliberately
    │                               ignores auction-instance fields
    │                               (uuid/auctioneer/end/bin/
    │                               starting_bid), ExtraAttributes.uuid/
    │                               timestamp (random per item copy),
    │                               display_name, and count (stack size
    │                               affects total price, not per-unit
    │                               identity) — see the design rationale
    │                               in the crate's module doc comment.
    │                               NBT tag names are sourced from
    │                               public SkyBlock documentation, not a
    │                               live-verified payload (no network
    │                               access to api.hypixel.net from this
    │                               workspace) — dungeon_item_level and
    │                               art_of_war_count are the lower-
    │                               confidence ones and worth checking
    │                               against a real captured item_bytes
    │                               once you have live access. Uses
    │                               std::collections::hash_map::
    │                               DefaultHasher::new() (fixed seed,
    │                               not the randomized RandomState) so
    │                               the fingerprint is deterministic
    │                               across runs. Scalar fields hash by
    │                               reference with zero allocation;
    │                               unordered NBT compounds/lists
    │                               (enchantments, runes, gems, ability
    │                               scrolls) need a small bounded Vec of
    │                               borrowed keys sorted before hashing,
    │                               since fastnbt's Value::Compound is a
    │                               plain HashMap with no iteration-
    │                               order guarantee — the one deliberate
    │                               compromise on strict zero-alloc,
    │                               scoped to O(enchant/gem count), not
    │                               O(full NBT tree). 11 unit tests cover
    │                               identical items matching, auction-
    │                               specific fields not affecting the
    │                               hash, each modifier changing the
    │                               hash, None-vs-empty-string not
    │                               colliding, and hash independence
    │                               from HashMap insertion order.
    └── storage/                   DONE: async auction/price storage.
        src/lib.rs                  SnapshotStore::open(db_path) opens
                                   (creates) a SQLite file (rusqlite,
                                   "bundled" feature — no system SQLite
                                   needed) and spawns a dedicated OS
                                   thread that owns the Connection.
                                   store(tick, Vec<ParsedItem>) sends a
                                   batch over a bounded tokio mpsc
                                   channel and awaits a oneshot ack from
                                   the writer thread, so callers get a
                                   real Result without ever touching
                                   rusqlite themselves. One row per
                                   (uuid, tick) via INSERT OR REPLACE —
                                   same uuid at a later tick is a new
                                   row (price history), same uuid at the
                                   same tick overwrites (idempotent
                                   retries). Schema: single `auctions`
                                   table + an index on
                                   (skyblock_item_id, tick) for
                                   per-item price lookups later. This
                                   crate is the only place that knows
                                   the backend is SQLite — swapping to
                                   ClickHouse/Postgres later only
                                   touches this file. 4 unit tests cover
                                   persistence, empty-batch no-op,
                                   same-tick overwrite, and cross-tick
                                   history.
```

Not yet created: `crates/pricing` (the in-memory price cache),
`crates/engine`, `crates/notify`, `crates/flipper-server`, `web/`.

## Toolchain notes (read before running cargo update)

Built and tested against **rustc 1.75**. `Cargo.lock` pins
`url = 2.4.1` / `idna = 0.4.0` because newer versions of `idna` pull in
the ICU4X crate family, which requires edition2024 (unsupported on
1.75). If you're on a modern toolchain (1.80+), this pin is not required
— `cargo update -p url -p idna` is safe, just re-run
`cargo test --workspace` after.

## Verified working (as of last session)

- **Live run against the real Hypixel API confirmed by the user**:
  ingestion connects, ticks are detected, and full snapshots assemble
  at ~46,800 auctions each (e.g. `tick=1785736343562 auctions=46840`).
  The "not yet verified" caveat from earlier sessions is resolved.
- `cargo build --workspace` — passes (debug and `--release`)
- `cargo test --workspace` — passes (28 tests: 1 common, 6 diff, 5
  parser, 11 fingerprint, 4 storage, 1 ingestion wiremock integration,
  plus doc-tests), on rustc 1.94 (the `url`/`idna` pin from the
  toolchain notes below was not needed)
- `cargo clippy --workspace --all-targets` — clean on `parser`,
  `storage`, and `fingerprint`; pre-existing doc-comment lint warnings
  remain in `ingestion/src/lib.rs` only (unrelated to this session's
  changes)
- `ingestion-service`'s channel receiver now runs the full
  diff → parse → fingerprint → store pipeline per snapshot:
  `diff::DiffDetector` isolates new/changed auctions,
  `parser::parse_item` decodes each one's NBT (parse failures are
  logged and skipped, not fatal), `fingerprint::fingerprint` computes
  each parsed item's price-identity key (collected into a `HashSet`
  purely to log the unique-fingerprint count for this batch — nothing
  consumes it yet), and `storage::SnapshotStore` persists the parsed
  batch to SQLite. Per-snapshot log line reports total/changed/parsed/
  failed/unique-fingerprint counts plus the diff detector's live-
  tracked count.
- Binary starts, loads config, and fails gracefully (structured error
  log, non-zero exit, no panic) when the network is unreachable

## Immediate next step

**Phase 1.6: in-memory price cache.** Keyed on `fingerprint::
Fingerprint`, sharded, updated by a background task, read lock-free
(RCU/atomic-swap style) per the architecture decisions above — the
hot path never blocks on a cache miss, it skips and lets the
background system price that fingerprint for next time. Feeds off the
same parsed+fingerprinted stream `ingestion-service` already produces
per snapshot (today that stream only goes to storage; the cache needs
to observe it too, probably by computing a per-fingerprint running
price estimate from `storage`'s accumulating history, or from the live
stream directly — worth deciding which before writing code). This is
what the profit calculation engine (step 7) will read from.
