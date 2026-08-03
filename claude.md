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
  cache) all exist only in the async/background lane. In practice the
  historical-sales role is filled by SQLite (`storage` crate, own auction
  observations) and COFL (`cofl` crate, third-party historical sold-
  auction data used to seed the price cache at startup) rather than
  ClickHouse — see those crates' entries below for why.
- **`PriceEntry` carries a `source: Live | Historical`.** A live-fed
  price (this tick's own cheapest-BIN observation) and a COFL-backfilled
  historical price are different kinds of signal with different
  freshness expectations, and `engine::FlipThresholds` applies a
  different staleness ceiling to each (minutes vs. 30 days) — see the
  `pricing`/`engine`/`cofl` entries below.
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
client, though there's no *site* yet — that's Phase 2.

10. COFL historical pricing backend ✅ done, added beyond the original
    9-step list (see the `cofl` crate entry below) — partially
    addresses the "placeholder pricing feed" gap noted below by seeding
    `PriceCache` with real historical sold-auction data at startup, so
    the bot has price knowledge before any live auction has taught it
    anything. Still leaves the *live* feed's "cheapest BIN this tick"
    as a placeholder — COFL only ever writes `PriceSource::Historical`
    entries, which a live-fed `PriceSource::Live` entry for the same
    fingerprint will still overwrite once observed.

What's left that isn't a numbered step:
- The *live* pricing feed is still `main.rs`'s placeholder "cheapest
  BIN this tick," not a real fair-value estimate (noted since
  Phase 1.6, never addressed — see the ingestion/main.rs entry below).
  COFL backfill (step 10) softens this for the startup/cold-start case
  but doesn't replace it.
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
│                                STORAGE_DB_PATH, WEBSOCKET_BIND_ADDR,
│                                COFL_BACKFILL_ENABLED, COFL_BASE_URL,
│                                COFL_BACKFILL_ITEM_TAGS,
│                                COFL_BACKFILL_PAGES_PER_TAG
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
        │                          is even spawned. Then, if
        │                          config.cofl_backfill_enabled
        │                          (default true; false logs and skips
        │                          — set false in network-restricted
        │                          environments like this sandbox),
        │                          spawns cofl::backfill(...) as its
        │                          own background tokio task against a
        │                          cloned Arc<PriceCache> — NOT awaited,
        │                          so a slow/rate-limited backfill can
        │                          never delay the ingestion loop's
        │                          first tick; it only ever calls
        │                          PriceCache::update_batch, the same
        │                          non-blocking write path the live
        │                          feed below uses.
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
        │                          real fair-value estimate; this
        │                          entry's source is always
        │                          PriceSource::Live (COFL-seeded
        │                          entries only ever come from the
        │                          background backfill task above and
        │                          use PriceSource::Historical). Then
        │                          runs
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
    │                              Internally refactored (COFL session)
    │                              into two reusable, standalone
    │                              functions so a non-`RawAuction`
    │                              caller can get the exact same
    │                              decoding accuracy: decode_item_bytes
    │                              (base64 -> gzip -> NBT, what
    │                              parse_item itself calls) and
    │                              decode_nbt_bytes (already-decompressed
    │                              NBT -> DecodedItem, for callers whose
    │                              bytes didn't arrive gzip-wrapped).
    │                              Both return DecodedItem {
    │                              skyblock_item_id, display_name,
    │                              count, extra_attributes }; parse_item
    │                              just attaches the RawAuction-specific
    │                              fields (uuid, auctioneer,
    │                              starting_bid, bin, end). The `cofl`
    │                              crate calls decode_item_bytes/
    │                              decode_nbt_bytes directly on COFL's
    │                              shortItemBytes field instead of
    │                              reconstructing ExtraAttributes from a
    │                              separate, less complete field-by-
    │                              field mapping — see the cofl entry
    │                              below. This refactor is behavior-
    │                              preserving: all 5 original unit tests
    │                              (synthetic item NBT round-tripped
    │                              through parse_item via fastnbt's own
    │                              nbt!/to_bytes) pass unmodified.
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
    │                               updated_at_tick (i64 — actually a
    │                               raw Hypixel epoch-millis timestamp
    │                               despite the "tick" name; see the
    │                               engine entry's bug-fix note below),
    │                               and (added in the COFL session)
    │                               source: PriceSource (Live |
    │                               Historical) so engine::evaluate can
    │                               apply a different staleness ceiling
    │                               to a live-fed price vs a COFL-
    │                               backfilled one. What estimated_value
    │                               means (median? trimmed mean?) is
    │                               still deliberately left to the
    │                               caller/profit engine, not this
    │                               crate. 7 unit tests (empty
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
    │                               **CONFIRMED BUG FIXED (COFL
    │                               session):** `tick`/`updated_at_tick`
    │                               are raw Hypixel epoch-millis, not a
    │                               small sequential counter, but
    │                               FlipThresholds::default().
    │                               max_price_age_ticks was `5` — i.e.
    │                               5 *milliseconds*. Since Hypixel's
    │                               cache refreshes every ~60,000ms, no
    │                               live-fed price could ever survive
    │                               to the next tick; every one was
    │                               already stale by the time it could
    │                               be reused. This is very likely a
    │                               major contributor to the
    │                               flips_found=0 issue below,
    │                               independent of the fingerprint-
    │                               sparsity hypothesis. Fixed: default
    │                               is now 180_000 (3 minutes) for
    │                               PriceSource::Live, plus a new
    │                               max_historical_price_age_ticks
    │                               field (default 30 days) applied
    │                               instead when the entry's source is
    │                               PriceSource::Historical — evaluate()
    │                               picks the ceiling based on
    │                               entry.source. 5 new tests cover
    │                               this: a regression test asserting
    │                               the default survives at least one
    │                               ~60s tick interval, a live price a
    │                               few ticks old still usable, a
    │                               historical price days old still
    │                               usable, a historical price beyond
    │                               30 days stale, and the same age
    │                               being fine for Historical but stale
    │                               for Live (proving the two ceilings
    │                               aren't conflated).
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
    ├── cofl/                       DONE: COFL historical pricing
    │   src/lib.rs                  backend (Coflnet, sky.coflnet.com).
    │                               Investigated via Coflnet's own
    │                               auto-generated OpenAPI TypeScript
    │                               client (Coflnet/hypixel-react on
    │                               GitHub) since sky.coflnet.com itself
    │                               403s non-browser fetches from this
    │                               sandbox, same limitation as
    │                               api.hypixel.net — see the crate's
    │                               module doc comment for the full
    │                               confirmed-vs-assumed breakdown.
    │                               Pipeline: GET /api/auctions/tag/
    │                               {tag}/sold (paginated, no server-
    │                               side attribute filter, confirmed
    │                               free/public, ~30req/10s+100req/min
    │                               rate limit) -> CoflSoldAuction (raw
    │                               wire struct, every uncertain field
    │                               Option<> so a mismatch degrades a
    │                               field, not the whole record) ->
    │                               HistoricalSale (normalized, network-
    │                               independent, unit tested without
    │                               wiremock) -> fingerprint_of()
    │                               (builds a synthetic ParsedItem,
    │                               calls the SAME fingerprint::
    │                               fingerprint() live auctions use —
    │                               not a parallel implementation) ->
    │                               grouped by Fingerprint, median sale
    │                               price -> PriceEntry { source:
    │                               Historical } -> PriceCache::
    │                               update_batch(). Two-tier accuracy:
    │                               primary path decodes SoldAuction.
    │                               shortItemBytes ("NBT data as base64
    │                               encoded string" per its doc comment)
    │                               via parser::decode_item_bytes (or
    │                               decode_nbt_bytes if the gzip-
    │                               wrapped attempt fails, in case
    │                               "short" means no gzip) — when this
    │                               succeeds it's byte-for-byte the
    │                               same ExtraAttributes a live auction
    │                               would produce, zero guessing.
    │                               Fallback (ReconstructedAttributes):
    │                               reforge from flattenedNbt["modifier"],
    │                               enchantments from the structured
    │                               enchantments[] field (each type
    │                               name run through pascal_to_snake —
    │                               the single highest-risk assumption
    │                               in this crate: unconfirmed whether
    │                               COFL serializes enum names as
    │                               PascalCase strings or numeric ids;
    │                               numeric ids get an opaque
    │                               unknown_{n} label so they at least
    │                               group self-consistently), stars/
    │                               recomb/hpc/aow/enrichment/skin from
    │                               flattenedNbt using the same assumed
    │                               key names fingerprint already uses.
    │                               Gems/runes/ability scrolls are
    │                               deliberately left unpopulated in
    │                               the fallback — no confirmed
    │                               flattening convention for nested/
    │                               list NBT in a flat string map, and
    │                               an absent modifier degrades
    │                               gracefully where a wrong guess
    │                               would silently corrupt the
    │                               fingerprint. ImportStats tracks
    │                               (all 3 requirements from the task):
    │                               item_tags_attempted, sales_fetched,
    │                               fingerprints_loaded,
    │                               cache_entries_seeded, fetch_errors,
    │                               and sales_from_real_nbt vs
    │                               sales_from_reconstruction (the
    │                               single most useful live-run signal
    │                               for judging whether the
    │                               shortItemBytes assumption held).
    │                               backfill() rate-limits itself
    │                               (350ms between requests) and is
    │                               meant to run once at startup,
    │                               concurrently with (not blocking)
    │                               ingestion — see main.rs above. 13
    │                               unit tests, including one proving
    │                               the shortItemBytes decode path
    │                               produces byte-identical fingerprints
    │                               to a live auction, one proving the
    │                               reconstruction fallback matches the
    │                               real-NBT path when the assumed key
    │                               names hold (a regression check on
    │                               the assumptions themselves), and a
    │                               wiremock-based end-to-end test of
    │                               backfill() seeding the cache from a
    │                               multi-sale, multi-page response.
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
- `cargo test --workspace` — passes (75 run + 2 `#[ignore]`'d
  benchmarks = 77 tests: 1 common, 6 diff, 5 parser, 11 fingerprint, 7
  pricing (+1 benchmark), 18 engine (+1 benchmark, +5 new staleness
  tests from the COFL session's bug fix), 9 notify, 13 cofl, 4
  storage, 1 ingestion wiremock integration, plus doc-tests), on
  rustc 1.94 (the `url`/`idna` pin from the toolchain notes below was
  not needed)
- `cargo clippy --workspace --all-targets` — clean on `parser`,
  `storage`, `fingerprint`, `pricing`, `engine`, `notify`, `cofl`, and
  `common`; pre-existing doc-comment lint warnings remain in
  `ingestion/src/lib.rs` only (unrelated to this session's changes)
- `ingestion-service` now runs the complete pipeline end to end:
  diff → parse → fingerprint → evaluate → dedup → publish (WebSocket)
  → price cache update → store, per snapshot, with the COFL backfill
  seeding the cache concurrently in the background at startup. See the
  `crates/ingestion/src/main.rs` entry above for the exact wiring, the
  evaluate-before-update ordering requirement, the dedup-before-
  publish ordering, and the "cheapest BIN this tick" placeholder-
  pricing caveat (still unaddressed for the *live* feed specifically —
  COFL now covers the historical/startup case).
- **Manually smoke-tested**: started `target/release/ingestion-service`
  with a fake API key, both with `COFL_BACKFILL_ENABLED=true` and
  `=false` — confirmed the WebSocket listener binds and logs
  "websocket notification server listening" *before* the ingestion
  loop attempts its first Hypixel request, that the COFL-disabled path
  logs and skips cleanly, and that the process still exits cleanly
  (non-zero, no panic) when the Hypixel request fails in this
  network-restricted sandbox (which also can't reach
  `sky.coflnet.com`, so the enabled path's actual backfill attempt was
  not observed completing or failing — only that it didn't crash
  startup). Did not verify an actual end-to-end flip alert or COFL
  backfill against live data — the `notify` and `cofl` crates' own
  integration tests cover their respective paths with a real socket /
  wiremock server instead.
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

## Known issue: flips_found=0 on live runs (partially fixed this session)

A live run reported `flips_found=0` and `below_threshold=0` on every
tick — i.e. essentially nothing reaches `engine::evaluate` far enough
to even hit the min-profit/min-ROI check, let alone pass it. This
confirms the "Replace the placeholder pricing feed" item flagged (but
never acted on) since Phase 1.6 was not just a nice-to-have — it looks
like the actual root cause of zero detections.

**Update from the COFL integration session — a second, independently
confirmed root cause found and fixed, plus a mitigation for the first
one:**
1. **Confirmed (not hypothesized) via code review**: `engine::
   FlipThresholds::default().max_price_age_ticks` was `5`, compared
   directly against millisecond deltas (`tick` is Hypixel's raw
   epoch-millis `lastUpdated`, not a small counter, despite the name).
   Hypixel's cache refreshes every ~60,000ms, so *no* live-fed price
   could ever survive to the next tick — every one was stale
   immediately. This alone could fully explain `flips_found=0`
   independent of the fingerprint-sparsity hypothesis below. **Fixed**:
   see the `engine` crate entry above (new default 180_000ms / 3
   minutes for live prices, plus a separate 30-day ceiling for
   historical ones).
2. The fingerprint-sparsity hypothesis below is **still valid and
   unconfirmed against live data**, but the COFL backfill (new `cofl`
   crate) now directly mitigates it for high-value items: instead of
   waiting for two live auctions to coincidentally share an exact
   fingerprint, `PriceCache` can be seeded at startup with historical
   sold-auction data for specific fingerprints.

Both fixes together should meaningfully change the picture on the next
live run — worth re-running with the `diag_*` fields below (now
including `diag_historical_price_hit_total`) before assuming anything
else is wrong.

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

**What was added (not a fix at the time — diagnostics only, per
explicit instruction to diagnose before changing behavior; the
staleness bug fix above came from a *separate* task's explicit
instructions, not from loosening these diagnostics' findings):**
`ingestion-service`'s receiver task tracks cumulative-since-start
counters for every `FlipVerdict` variant
(`diag_evaluated_total`, `diag_cache_hit_total`, `diag_not_bin_total`,
`diag_no_price_data_total`, `diag_insufficient_sample_total`,
`diag_stale_price_total`, `diag_invalid_price_total`,
`diag_implausible_roi_total`, `diag_below_threshold_total`) plus
`diag_max_expected_profit_ever` and `diag_max_roi_percent_ever`
(tracked across `Flip`, `BelowThreshold`, *and* `ImplausibleRoi` — a
high implausible-ROI max would itself be a signal that
`max_plausible_roi_percent` is rejecting genuine rare-item flips, not
just bad data), and (added in the COFL session)
`diag_historical_price_hit_total` — how many evaluated auctions hit a
`PriceSource::Historical` (COFL-seeded) cache entry specifically. All
clearly marked `// TEMPORARY DIAGNOSTIC INSTRUMENTATION` in `main.rs`,
meant to be removed once the root cause is confirmed and fixed, not
left in permanently. Cumulative rather than per-tick because diff
detection can make any single tick's evaluated-auction count too small
to read anything from. Zero added hot-path cost beyond a few extra
integer compares/increments per already-evaluated auction — no new
I/O, allocation, or locking.

**What to look for in the next live run's logs** to confirm/refute the
remaining (fingerprint-sparsity) hypothesis, now that the staleness
bug is fixed: `diag_no_price_data_total` should still dominate for
uniquely-modified items even with fresh live data, `diag_cache_hit_total`
should be small relative to `diag_evaluated_total` for the *live* feed
specifically, `diag_historical_price_hit_total` being nonzero confirms
the COFL backfill is actually reaching evaluate() (and `cofl::
ImportStats.sales_from_real_nbt` vs `sales_from_reconstruction`,
logged separately at startup, tells you whether COFL's `shortItemBytes`
assumption held — see the `cofl` crate entry above), and
`diag_max_roi_percent_ever` being populated (even via the
`ImplausibleRoi` path) would mean real opportunities are being seen
but rejected, not that nothing's out there at all.

**Not yet decided — needs the confirmed numbers first:** whether the
COFL backfill alone resolves the fingerprint-sparsity hypothesis for
the item tags it covers, or whether a coarser fallback fingerprint
tier (item id + only the modifiers that matter most, used when the
exact fingerprint misses) is still needed on top of it for items COFL
doesn't cover or for the live feed between backfills. Not lowering
thresholds or removing the plausibility/sample-size guards, which the
user was explicit about not doing blindly.

## Immediate next step

**Run it live** — this is the single highest-value next action, not
new code. Two independent, code-confirmed fixes (staleness bug) and
mitigations (COFL backfill) landed this session for `flips_found=0`
without ever having live Hypixel/COFL access to verify either one end
to end. A live run would confirm or correct, in priority order:
1. Whether `flips_found` is now nonzero at all.
2. Whether `cofl::ImportStats` (logged at startup) shows
   `sales_from_real_nbt > 0` — confirms the `shortItemBytes` decode
   assumption, the single highest-leverage unverified assumption in
   the whole COFL integration (if it's 0, everything fell back to
   `ReconstructedAttributes`, and the `pascal_to_snake` enchant-name
   assumption becomes the next thing to check).
3. Whether `diag_historical_price_hit_total` is nonzero — confirms
   COFL-seeded prices are actually reaching `evaluate()`.
4. Whether `diag_no_price_data_total` still dominates for the *live*
   feed specifically (now that staleness isn't confounding the
   reading) — confirms or refutes how much the fingerprint-sparsity
   hypothesis still matters beyond what COFL covers.

Other open items, unblocked by this and listed in priority-neutral
order:

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
