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
6. In-memory price cache ✅ done (see below)
7. Profit calculation engine ✅ done (see below) — note: this step's
   scope, per the user's explicit requirements, already includes the
   pass/fail threshold decision ("whether it passes thresholds"), so
   what's left of step 8 below is narrower than originally scoped:
   deduping so the same auction isn't re-reported as a flip on every
   tick it remains listed, plus whatever orchestration the notification
   stage (step 9) needs.
8. Flip detection (dedup) ✅ done (see below)
9. WebSocket notification server ✅ done (see below) — the
   `/viewauction <uuid>` requirement from session chat is satisfied:
   every `FlipAlert` carries a `viewauction_command` field built at
   construction time.

**All 9 numbered Phase 1 steps are now built.** Target output of
Phase 1 ("a user connects to the site and receives live profitable
flip alerts") is technically achievable today for any WebSocket
client, though there's no *site* yet — that's Phase 2. What's left
that isn't a numbered step:
- The pricing feed is still `main.rs`'s placeholder "cheapest BIN
  this tick," not a real fair-value estimate (noted since Phase 1.6,
  never addressed — see the ingestion/main.rs entry below).
- The ingestion tick-detection/fetch latency flagged below
  (`detect_latency_ms=238`, `snapshot_fetch_latency_ms=1074`) still
  dwarfs the rest of the compute pipeline and hasn't been touched.
- Phase 2 (website) and Phase 3 (optimization) below are both
  unstarted.

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
│                                STORAGE_DB_PATH, WEBSOCKET_BIND_ADDR
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
        src/main.rs                 Standalone runnable binary. Before
        │                          the ingestion loop starts: binds the
        │                          WebSocket listener via notify::
        │                          NotificationHub::bind(&config.
        │                          websocket_bind_addr) (hard error /
        │                          process exit if the bind fails, same
        │                          treatment as a storage-open failure),
        │                          builds an Arc<NotificationHub>, and
        │                          spawns NotificationHub::serve(...)
        │                          as its own background task before
        │                          the ingestion channel receiver task
        │                          is even spawned.
        │                          Channel receiver runs each snapshot
        │                          through diff::DiffDetector, then
        │                          parser::parse_item on each changed
        │                          auction (parse failures are logged
        │                          and skipped, not fatal), then
        │                          fingerprint::fingerprint on every
        │                          parsed item. For each item, looks up
        │                          pricing::PriceCache::get(fp) — the
        │                          cache's state from *prior* ticks —
        │                          and passes it to engine::evaluate().
        │                          A FlipVerdict::Flip builds a
        │                          notify::FlipAlert (from ParsedItem +
        │                          ProfitCalculation fields) and
        │                          collects it into a per-tick
        │                          flip_candidates Vec — it does NOT
        │                          publish immediately, see dedup
        │                          ordering below. BelowThreshold is
        │                          counted; every other verdict
        │                          (NotBin/NoPriceData/StalePrice/etc.)
        │                          is silently skipped. Only *after*
        │                          every item in the batch has been
        │                          evaluated does it fold this tick's
        │                          cheapest-BIN-per-fingerprint
        │                          observations into the price cache
        │                          via update_batch() — this ordering
        │                          is load-bearing: evaluating against
        │                          a cache already updated with this
        │                          same tick's data would let an
        │                          auction get judged against a
        │                          "market price" derived from itself
        │                          or its same-tick siblings. NOTE:
        │                          "cheapest BIN this tick" remains a
        │                          placeholder value source, not a
        │                          real fair-value estimate. Then runs
        │                          notify::FlipDeduplicator::filter_new
        │                          on the whole tick's flip_candidates
        │                          batch at once; for each surviving
        │                          (genuinely new) alert, calls
        │                          notification_hub.publish(alert)
        │                          (non-blocking) and logs a "flip
        │                          detected" info! line with the full
        │                          alert payload. Then hands the parsed
        │                          batch to storage::SnapshotStore::
        │                          store(tick, items). Logs total/
        │                          changed/parsed/failed/unique-
        │                          fingerprint/priced-fingerprint
        │                          counts, the price cache's total
        │                          size, flips_found (now: len of the
        │                          deduped survivors, not raw Flip
        │                          count), below_threshold, the diff
        │                          detector's live-tracked count, and
        │                          dedup.tracked_count() per snapshot.
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
    ├── pricing/                    DONE: in-memory price cache.
    │   src/lib.rs                   PriceCache: SHARD_COUNT=64
    │                               independent ArcSwap<HashMap<
    │                               Fingerprint, PriceEntry,
    │                               FingerprintHasher>> shards (the
    │                               arc-swap crate's RCU/atomic-swap
    │                               pattern named in the architecture
    │                               decisions above). Shard index is
    │                               fingerprint.0 & (SHARD_COUNT - 1).
    │                               get(fingerprint) is wait-free: one
    │                               atomic load + a hashmap probe using
    │                               FingerprintHasher (an identity
    │                               hasher — Fingerprint is already a
    │                               well-distributed u64, so this skips
    │                               a redundant SipHash pass), no
    │                               allocation, no locking, never
    │                               blocked by a concurrent writer.
    │                               Missing entries return None rather
    │                               than panicking or blocking.
    │                               update_batch(...) groups updates by
    │                               shard, then does one .rcu() per
    │                               touched shard (clone-on-write +
    │                               compare-and-swap retry, so
    │                               concurrent writers to the same
    │                               shard never lose an update to a
    │                               race, and never block a concurrent
    │                               get()). PriceEntry is a fixed-size
    │                               Copy struct: estimated_value (u64
    │                               coins), sample_size (u32),
    │                               updated_at_tick (i64) — what
    │                               estimated_value means (median?
    │                               trimmed mean?) is deliberately left
    │                               to the caller/profit engine, not
    │                               this crate. 7 unit tests (empty
    │                               cache, insert/read, overwrite,
    │                               500 entries across shards, empty
    │                               batch no-op, concurrent same-shard
    │                               writers not losing data, reads not
    │                               blocking a concurrent writer) plus
    │                               one #[ignore]'d benchmark
    │                               (`cargo test -p pricing --release
    │                               -- --ignored --nocapture`) —
    │                               measured ~55 ns/op for get() over
    │                               50,000 entries on the machine this
    │                               was built on, well inside the <1 µs
    │                               budget line below.
    ├── engine/                     DONE: profit calculation engine.
    │   src/lib.rs                   evaluate(item, price: Option<
    │                               PriceEntry>, current_tick, fees:
    │                               &FeeSchedule, thresholds:
    │                               &FlipThresholds) -> FlipVerdict.
    │                               Pure/sync, no I/O, no async,
    │                               depends only on parser (ParsedItem)
    │                               and pricing (PriceEntry type only —
    │                               NOT PriceCache; the caller does the
    │                               cache lookup and passes the
    │                               Option<PriceEntry> in, which is
    │                               also why this crate doesn't depend
    │                               on the fingerprint crate at all).
    │                               Profit formula: tax =
    │                               max(estimated_value * tax_rate,
    │                               minimum_tax); expected_profit =
    │                               (estimated_value - tax) -
    │                               starting_bid, computed via i128
    │                               intermediates so a loss can't
    │                               overflow/underflow u64 subtraction;
    │                               roi_percent = expected_profit /
    │                               starting_bid * 100. FlipThresholds
    │                               and FeeSchedule are both injected,
    │                               not hardcoded — see the crate's
    │                               module doc comment for the fee-
    │                               schedule caveat (flat 1% tax, no
    │                               verified live tax table available
    │                               from this sandbox — same situation
    │                               as fingerprint's NBT tag names) and
    │                               the load-bearing ordering
    │                               requirement on the caller (must
    │                               look up the price *before* folding
    │                               this tick's own observations into
    │                               the cache — see main.rs above).
    │                               FlipVerdict is a named-variant enum
    │                               (Flip/BelowThreshold/NotBin/
    │                               NoPriceData/StalePrice/
    │                               InsufficientSampleSize/
    │                               InvalidPriceData/ImplausibleRoi),
    │                               not a bare bool/Option, so a caller
    │                               can see *why* an auction wasn't
    │                               reported. False-positive guards:
    │                               BIN-only scope, minimum sample
    │                               size, price staleness cutoff, a
    │                               zero-input guard, and a max-
    │                               plausible-ROI ceiling (an
    │                               implausibly good "flip" is treated
    │                               as more likely bad cache data than
    │                               a real opportunity). 13 unit tests
    │                               cover every FlipVerdict variant,
    │                               overflow/panic safety at extreme
    │                               values, and a hand-computed profit/
    │                               tax/ROI example. One #[ignore]'d
    │                               benchmark (`cargo test -p engine
    │                               --release -- --ignored --nocapture`)
    │                               measured ~2.9 ns/op for evaluate()
    │                               — note this doesn't include the
    │                               cache lookup itself (~55 ns,
    │                               measured separately in pricing),
    │                               since evaluate() takes an already-
    │                               resolved price.
    ├── notify/                     DONE: flip dedup + WebSocket
    │   src/lib.rs                  notification.
    │                               FlipAlert { auction_uuid,
    │                               viewauction_command, item_name,
    │                               buy_price, estimated_value, profit,
    │                               roi_percent, #[serde(skip)]
    │                               auction_end } — the caller builds
    │                               this from primitive ParsedItem +
    │                               ProfitCalculation fields; this
    │                               crate depends on neither `parser`
    │                               nor `engine`, same decoupling
    │                               reasoning as `engine` not depending
    │                               on `pricing::PriceCache`. `new()`
    │                               builds viewauction_command as
    │                               `format!("/viewauction {uuid}")`.
    │                               FlipDeduplicator: HashMap<uuid,
    │                               auction_end> tracking already-
    │                               alerted uuids — the exact same
    │                               self-pruning-by-end-timestamp idiom
    │                               as diff::DiffDetector, applied to
    │                               the identically-shaped problem.
    │                               filter_new(candidates, tick)
    │                               processes a whole tick's flip
    │                               candidates as one batch (mirrors
    │                               DiffDetector::diff), not per-
    │                               candidate, then prunes once.
    │                               NotificationHub: owns a
    │                               tokio::sync::broadcast::Sender<
    │                               Arc<str>> (not mpsc — see the
    │                               crate's module doc comment for why
    │                               broadcast's never-backpressures-the-
    │                               sender property is load-bearing
    │                               here, unlike storage's deliberately
    │                               backpressuring mpsc). publish(&self,
    │                               &FlipAlert) -> usize serializes to
    │                               JSON once into a shared Arc<str>
    │                               and broadcasts it — non-blocking,
    │                               &self not &mut self, safe to call
    │                               from the detection loop directly.
    │                               bind(addr) and serve(hub, listener)
    │                               are split so callers/tests can bind
    │                               to an OS-assigned port and read
    │                               back the real address before the
    │                               accept loop starts; serve() spawns
    │                               one task per connected client
    │                               (subscribed to the broadcast channel
    │                               *before* the WebSocket handshake
    │                               even starts, closing any race
    │                               window), forwarding alerts via
    │                               tokio_tungstenite until the client
    │                               disconnects or the connection
    │                               errors — one client's failure never
    │                               affects the accept loop or other
    │                               clients. 9 unit tests cover the
    │                               alert payload shape (including that
    │                               auction_end is NOT serialized),
    │                               dedup batch/pruning semantics
    │                               (mirroring DiffDetector's own test
    │                               suite), publish-with-no-subscribers
    │                               not erroring, and one real end-to-
    │                               end test that binds a real socket,
    │                               connects a real tokio-tungstenite
    │                               client, publishes, and asserts the
    │                               client receives the correctly-
    │                               shaped JSON.
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

Not yet created: `crates/flipper-server`, `web/`.

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
- `cargo test --workspace` — passes (55 run + 2 `#[ignore]`'d
  benchmarks = 57 tests: 1 common, 6 diff, 5 parser, 11 fingerprint, 7
  pricing (+1 benchmark), 13 engine (+1 benchmark), 9 notify, 4
  storage, 1 ingestion wiremock integration, plus doc-tests), on
  rustc 1.94 (the `url`/`idna` pin from the toolchain notes below was
  not needed)
- `cargo clippy --workspace --all-targets` — clean on `parser`,
  `storage`, `fingerprint`, `pricing`, `engine`, `notify`, and
  `common`; pre-existing doc-comment lint warnings remain in
  `ingestion/src/lib.rs` only (unrelated to this session's changes)
- `ingestion-service` now runs the complete pipeline end to end:
  diff → parse → fingerprint → evaluate → dedup → publish (WebSocket)
  → price cache update → store, per snapshot. See the
  `crates/ingestion/src/main.rs` entry above for the exact wiring, the
  evaluate-before-update ordering requirement, the dedup-before-
  publish ordering, and the "cheapest BIN this tick" placeholder-
  pricing caveat (still unaddressed).
- **Manually smoke-tested**: started `target/release/ingestion-service`
  with a fake API key — confirmed the WebSocket listener binds and
  logs "websocket notification server listening" *before* the
  ingestion loop attempts its first Hypixel request, and that the
  process still exits cleanly (non-zero, no panic) when that request
  fails in this network-restricted sandbox. Did not verify an actual
  end-to-end flip alert against live Hypixel data (no network access
  here) — the `notify` crate's own integration test covers the
  publish-to-connected-client path with a real socket instead.
- `pricing::PriceCache::get` benchmarked at ~55 ns/op over 50,000
  entries; `engine::evaluate` benchmarked at ~2.9 ns/op (excludes the
  cache lookup itself) — both release build, single-threaded. See the
  respective crate entries above for how to reproduce. Note: an
  earlier version of the engine benchmark used fixed inputs every
  iteration and LLVM constant-folded the whole loop, reporting a
  nonsensical "5,000,000 calls in 124ns" — fixed by varying the input
  per iteration through `std::hint::black_box`. Worth remembering if
  a future micro-benchmark in this workspace reports a suspiciously
  round or tiny number. `notify` has no dedicated benchmark (its hot-
  path-adjacent cost is `broadcast::Sender::send`, a tokio primitive,
  not custom logic worth re-benchmarking).
- Binary starts, loads config, and fails gracefully (structured error
  log, non-zero exit, no panic) when the network is unreachable

## Flagged for a future pass (not yet acted on)

The user pointed out, correctly, that the biggest speed win left on
the table is not in this compute pipeline — it's ingestion's tick
detection and fetch latency. Real numbers from a live run:
`detect_latency_ms=238`, `snapshot_fetch_latency_ms=1074`. Both dwarf
anything downstream (fingerprinting is µs-scale, price lookup is now
measured at ~55 ns). This wasn't addressed in Phase 1.6 since it
wasn't in scope for the cache, but it's the natural next lever once
the core pipeline (steps 7-9) is complete, or worth an early
Phase 3-style pass if it's blocking real usage sooner.

## Known issue: flips_found=0 on live runs (diagnosis in progress)

A live run reported `flips_found=0` and `below_threshold=0` on every
tick — i.e. essentially nothing reaches `engine::evaluate` far enough
to even hit the min-profit/min-ROI check, let alone pass it. This
confirms the "Replace the placeholder pricing feed" item flagged (but
never acted on) since Phase 1.6 was not just a nice-to-have — it looks
like the actual root cause of zero detections.

**Root-cause hypothesis, grounded in code review (not yet confirmed
against live data — this sandbox has no network access to
`api.hypixel.net`):**

1. `diff::DiffDetector::diff()` emits an auction **exactly once** —
   the tick it's first listed. A BIN auction's `starting_bid`/`end`
   never change after listing, so every later tick it's
   `is_new_or_changed == false` and silently dropped (confirmed by the
   crate's own `unchanged_auction_is_filtered_out_on_next_tick` test —
   this is working exactly as designed for its original purpose).
2. `ingestion-service`'s `cheapest_bin` feed into `price_cache` is
   built **only** from that tick's `parsed` set — i.e. only from
   auctions newly listed *this specific tick*.
3. `engine::evaluate` (correctly, by the Phase 1.7 ordering
   requirement) looks up the cache's state from **prior** ticks only,
   never this tick's own data.

Put together: a fingerprint only gets a cache hit if **two different
auctions, from two different sellers at two different times, produce
the exact same `Fingerprint`.** For a plain unmodified item that's
plausible. For anything with real modifiers (reforge, stars, hot
potato books, specific enchant levels, gems) — which is essentially
every real listing of the exact high-value items this project cares
about, Necron's Handle and Hyperion included, since owners virtually
always customize those differently — two independent listings
essentially never match exactly. `NoPriceData` should dominate, and
because `min_sample_size`/staleness/threshold checks all happen *after*
a cache hit, everything falls out at the very first gate. This is
architecturally consistent with `flips_found=0` **and**
`below_threshold=0` simultaneously.

Checked and ruled out as the cause: a bug in `pricing::PriceCache`
itself. `get()` and `update_batch()` use the identical `shard_index()`
and `FingerprintHasher`, and the crate's own tests already prove
insert-then-get works correctly. The mechanism is sound; it's being
fed data that structurally can't produce hits for uniquely-modified
items.

**What was added this session (not a fix — diagnostics only, per
explicit instruction to diagnose before changing behavior):**
`ingestion-service`'s receiver task now tracks cumulative-since-start
counters for every `FlipVerdict` variant
(`diag_evaluated_total`, `diag_cache_hit_total`, `diag_not_bin_total`,
`diag_no_price_data_total`, `diag_insufficient_sample_total`,
`diag_stale_price_total`, `diag_invalid_price_total`,
`diag_implausible_roi_total`, `diag_below_threshold_total`) plus
`diag_max_expected_profit_ever` and `diag_max_roi_percent_ever`
(tracked across `Flip`, `BelowThreshold`, *and* `ImplausibleRoi` — a
high implausible-ROI max would itself be a signal that
`max_plausible_roi_percent` is rejecting genuine rare-item flips, not
just bad data). All clearly marked `// TEMPORARY DIAGNOSTIC
INSTRUMENTATION` in `main.rs`, meant to be removed once the root cause
is confirmed and fixed, not left in permanently. Cumulative rather
than per-tick because diff detection can make any single tick's
evaluated-auction count too small to read anything from.
Zero added hot-path cost beyond a few extra integer compares/increments
per already-evaluated auction — no new I/O, allocation, or locking.

**What to look for in the next live run's logs** to confirm/refute the
hypothesis: `diag_no_price_data_total` should dominate every other
counter by a wide margin, `diag_cache_hit_total` should be small
relative to `diag_evaluated_total`, and `diag_max_roi_percent_ever`
being populated (even via the `ImplausibleRoi` path) would mean real
opportunities are being seen but rejected, not that nothing's out
there at all.

**Not yet decided — needs the confirmed numbers first:** if this
hypothesis holds, the real fix is a pricing-strategy change (e.g. a
coarser fallback fingerprint tier — item id + only the modifiers that
matter most, ignoring the long tail — used when the exact fingerprint
misses; or seeding/backfilling the cache from `storage`'s accumulating
history instead of only this tick's new listings), not lowering
thresholds or removing the plausibility/sample-size guards, which the
user was explicit about not doing blindly.

## Immediate next step

**Confirm the flips_found=0 diagnosis against a live run**, using the
new `diag_*` fields above, before deciding on a fix. Once confirmed,
the fix belongs in the pricing strategy (see the "not yet decided"
note above), not in the engine's guards/thresholds. Other open items,
unblocked by this and listed in priority-neutral order:

1. **Ingestion latency.** Flagged since Phase 1.6, still real:
   `detect_latency_ms=238`, `snapshot_fetch_latency_ms=1074` from a
   live run dwarf the entire rest of the pipeline (fingerprinting is
   µs-scale, price lookup ~55 ns, evaluate ~2.9 ns). This is the
   "detecting Hypixel's cache refresh as fast as possible" lever
   called out as the main competitive edge at the top of this file,
   and it's the one part of the stack that hasn't been touched since
   the very first session.
2. **Phase 2 — Website**, per the Build order section above: live
   flip feed UI, user-configurable min-profit/min-ROI settings (which
   `engine::FlipThresholds` and `pricing`'s placeholder feed are
   already structured to accept once they exist).
3. **Phase 3 — Optimization**: now that every stage has some latency
   number (measured or estimated), a real profiling pass against live
   Hypixel traffic would confirm or correct the budget table above
   rather than relying on synthetic benchmarks.
