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
- **Pricing is three-tier (tiered pricing session):** `PriceCache::get`
  tries an exact fingerprint match first (Tier 1), falls back to a
  coarser "major modifier" key — item + reforge/stars/hot-potato-count/
  top-enchant, ignoring gems and minor enchants — (Tier 2), and finally
  falls back to the bare item id alone (Tier 3). All three lookups stay
  on the hot path (no I/O, no locking, no allocation) — see the
  `pricing` entry below for the latency numbers. `engine::evaluate`
  compensates for the coarser tiers' lower trustworthiness with a
  minimum-confidence gate (derived from `PriceEntry.sample_size`) and a
  per-tier ROI multiplier, rather than ever loosening `min_profit`
  itself — see the `engine` entry below. `cofl::backfill` and the live
  ingestion feed both seed all three tiers from the same observations
  (one sale/sighting contributes to its own exact fingerprint, its
  major-modifier bucket, and its base-item bucket simultaneously) — see
  the `cofl` and `ingestion/main.rs` entries below.
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
│                                COFL_BACKFILL_PAGES_PER_TAG,
│                                COFL_BACKFILL_INTERVAL_MINUTES (new,
│                                price coverage session — see below)
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
    │                             depends on this one. (price coverage
    │                             session) Config.cofl_backfill_item_tags'
    │                             default grew from a 6-item hardcoded
    │                             list to DEFAULT_COFL_BACKFILL_ITEM_TAGS,
    │                             a ~100-item curated list (endgame
    │                             weapons/accessories + high-volume
    │                             enchanted materials — see the const's
    │                             own doc comment for the full rationale
    │                             and the same "not live-verified"
    │                             caveat already applied to fingerprint's
    │                             NBT tag names elsewhere in this
    │                             project). Added a new field,
    │                             cofl_backfill_interval_minutes (env
    │                             COFL_BACKFILL_INTERVAL_MINUTES, default
    │                             60, 0 disables), consumed by the new
    │                             periodic re-backfill loop in
    │                             ingestion/main.rs — see below.
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
        │                          spawns a loop around cofl::backfill(...)
        │                          as its own background tokio task
        │                          against a cloned Arc<PriceCache> — NOT
        │                          awaited, so a slow/rate-limited
        │                          backfill can never delay the ingestion
        │                          loop's first tick; it only ever calls
        │                          PriceCache::update_exact_batch/
        │                          update_major_batch/update_base_batch,
        │                          the same non-blocking write paths the
        │                          live feed below uses. (price coverage
        │                          session) The single startup-only call
        │                          became a `loop { ...; sleep(...) }`:
        │                          runs backfill() once immediately, then
        │                          — if config.cofl_backfill_interval_
        │                          minutes > 0 (default 60) — sleeps that
        │                          many minutes and runs it again,
        │                          indefinitely, logging each cycle's
        │                          ImportStats and the cache's running
        │                          total size. interval_minutes == 0
        │                          breaks after the first run (old
        │                          startup-only behavior, still the
        │                          default in the wiremock integration
        │                          test's config). This finally
        │                          implements the "seed/update... before
        │                          and during operation" requirement from
        │                          the original COFL task, which the
        │                          first COFL session only half-built
        │                          (startup-only, never repeated).
        │                          Channel receiver runs each snapshot
        │                          through diff::DiffDetector, then
        │                          parser::parse_item on each changed
        │                          auction (parse failures are logged
        │                          and skipped, not fatal), then, for
        │                          each parsed item,
        │                          fingerprint::fingerprint (Tier 1 key)
        │                          AND (tiered pricing session)
        │                          fingerprint::major_modifier_key
        │                          (Tier 2 key). Looks up
        │                          pricing::PriceCache::get(exact, major,
        │                          &item.skyblock_item_id) — the cache's
        │                          state from *prior* ticks, tried Tier 1
        │                          then Tier 2 then Tier 3 (base item id,
        │                          no extra key needed) inside get()
        │                          itself — and passes the resulting
        │                          Option<PriceLookup> to
        │                          engine::evaluate(), which applies the
        │                          tier-appropriate confidence gate and
        │                          ROI multiplier (see the engine entry
        │                          above). A FlipVerdict::Flip builds a
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
        │                          cheapest-BIN observations into the
        │                          price cache — now via a shared
        │                          accumulate_cheapest_bin() helper
        │                          (generic over the key type) called
        │                          three times per BIN item, once per
        │                          tier (exact fingerprint, major-
        │                          modifier key, bare item id — mirroring
        │                          how cofl::backfill seeds all three
        │                          tiers from one COFL sale), then
        │                          update_exact_batch/update_major_batch/
        │                          update_base_batch — this ordering is
        │                          still load-bearing: evaluating against
        │                          a cache already updated with this
        │                          same tick's data would let an
        │                          auction get judged against a
        │                          "market price" derived from itself
        │                          or its same-tick siblings. This
        │                          entry's source is always
        │                          PriceSource::Live (COFL-seeded
        │                          entries only ever come from the
        │                          background backfill task above and
        │                          use PriceSource::Historical). Three
        │                          new cumulative diagnostic counters
        │                          (diag_tier_exact_hit_total,
        │                          diag_tier_major_modifier_hit_total,
        │                          diag_tier_base_item_hit_total) track
        │                          which tier each cache hit resolved
        │                          at, logged alongside the existing
        │                          diag_* fields — useful for judging on
        │                          a live run whether Tier 2/3 fallbacks
        │                          are actually contributing flips or
        │                          just adding rejected
        │                          InsufficientSampleSize verdicts.
        │                          (market-model session)
        │                          accumulate_cheapest_bin() deliberately
        │                          still takes the *minimum* starting_bid
        │                          seen this tick per key, not an average
        │                          — for a flipping bot the lowest
        │                          legitimate BIN in a tick is the actual
        │                          opportunity signal, and averaging in a
        │                          tick's overpriced listings would push
        │                          the estimate upward, away from what a
        │                          real flip needs. What changed instead
        │                          is what happens to that per-tick
        │                          minimum once it reaches the cache: it's
        │                          no longer a blind overwrite (see the
        │                          pricing entry's new "Cross-write
        │                          merging" section) — it's now the
        │                          incoming side of a sample-size-weighted,
        │                          outlier-dampened merge against
        │                          whatever's already cached, so a single
        │                          tick's cheapest-BIN can no longer
        │                          permanently redefine an established
        │                          multi-tick estimate on its own. The
        │                          three update_*_batch calls now return
        │                          MergeStats, summed into three new
        │                          cumulative counters
        │                          (diag_live_merged_total,
        │                          diag_overwritten_total,
        │                          diag_inserted_new_total), and a new
        │                          price_cache.stats() call once per tick
        │                          (same cost class as the pre-existing
        │                          price_cache.len() call) feeds
        │                          cache_total_entries/
        │                          cache_average_sample_size/
        │                          cache_high_confidence_entries into both
        │                          the debug! dump and a new dedicated
        │                          "pricing model" info! summary line
        │                          (separate from the flip-detection
        │                          summary line below, so neither gets
        │                          overloaded), e.g. "pricing model: 812
        │                          entries, avg sample size 6.7 | 94
        │                          high-confidence | 2310 live merges, 145
        │                          replaced (cumulative) | 3021 flips
        │                          rejected for insufficient data
        │                          (cumulative)". Then runs
        │                          notify::FlipDeduplicator::filter_new
        │                          on the whole tick's flip_candidates
        │                          batch at once; for each surviving
        │                          (genuinely new) alert, calls
        │                          notification_hub.publish(alert)
        │                          (non-blocking). Then hands the parsed
        │                          batch to storage::SnapshotStore::
        │                          store(tick, items).
        │                          (output/readability session) Logging
        │                          was restructured into three tiers,
        │                          none of it touching the pipeline
        │                          above — see the module's new
        │                          format_coins/tier_label/
        │                          price_source_label helpers, all pure
        │                          and only ever called on an actual
        │                          flip or once per tick, never per
        │                          auction:
        │                          1. Per-flip: the info! line inside
        │                             the new_flips loop keeps every
        │                             field it had before (uuid, item,
        │                             buy_price, estimated_value,
        │                             profit, roi_percent, viewauction)
        │                             plus two new ones (tier,
        │                             price_source), and now also
        │                             carries a human-readable message
        │                             built from the same data, e.g.
        │                             "FLIP  Hyperion  |  buy 800.0M ->
        │                             value 1.2B  |  profit +380.0M
        │                             (47.5% ROI)  |  tier Exact / Live
        │                             |  /viewauction abc-123". Coin
        │                             amounts get a human-scale K/M/B
        │                             suffix via format_coins() instead
        │                             of a long digit string. Still
        │                             fires only once per genuinely-new
        │                             flip (post-dedup), same as before.
        │                          2. Per-tick summary (info!): the
        │                             previous single mega-line dumping
        │                             ~20 raw fields every tick was
        │                             split. A new, concise info! line
        │                             carries only tick, flips_found
        │                             (this tick), the cumulative
        │                             cache-hit rate and its tier
        │                             breakdown, cumulative no-price-
        │                             data count, and elapsed_ms (new —
        │                             wall-clock time for this tick's
        │                             whole processing block, measured
        │                             via std::time::Instant around the
        │                             loop body; pure measurement, adds
        │                             no pipeline behavior, same pattern
        │                             ingestion::HypixelClient already
        │                             uses for detect_latency_ms), e.g.
        │                             "tick 1785...: 2 flip(s) | cache
        │                             118/43585 hits (0.3%) [exact 107 /
        │                             major 2 / base 9] | no-price
        │                             43467 | 4.2ms".
        │                          3. Per-tick full dump (debug!, was
        │                             info!): every field the old mega-
        │                             line had (total/changed/parsed/
        │                             failed/unique-fingerprint/priced-
        │                             fingerprint counts, price cache
        │                             size, flips_found, below_threshold,
        │                             tracked_live, dedup_tracked, all
        │                             the TEMPORARY DIAGNOSTIC
        │                             INSTRUMENTATION diag_* cumulative
        │                             counters, now also elapsed_ms) is
        │                             untouched field-for-field, just
        │                             demoted from info! to debug! so it
        │                             no longer prints by default —
        │                             still available via RUST_LOG=debug,
        │                             satisfying "keep machine-readable
        │                             logs available if needed" without
        │                             spamming the default view every
        │                             tick.
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
    │                               (tiered pricing session) Also exports
    │                               major_modifier_key(&ParsedItem) ->
    │                               Fingerprint — Tier 2's coarser key.
    │                               Hashes only skyblock_item_id,
    │                               modifier (reforge), rarity_upgrades
    │                               (recomb, as a bool — any amount of
    │                               upgrade counts the same), hot_potato_
    │                               count, dungeon_item_level (stars),
    │                               and the single highest-level
    │                               enchantment (tie-broken by
    │                               lexicographically smallest name) —
    │                               deliberately excludes gems and every
    │                               enchant but the top one. The top-
    │                               enchantment inclusion is a deliberate
    │                               design call, not an oversight: most
    │                               enchanted items' skyblock_item_id is
    │                               generically ENCHANTED_BOOK regardless
    │                               of which enchant it is, so without at
    │                               least the dominant enchant, Tier 2
    │                               would be useless for the entire
    │                               enchanted-book market. Picks the
    │                               highest level via a linear running-
    │                               max fold (no allocation, unlike the
    │                               full fingerprint's sorted-Vec
    │                               approach — Tier 2 only needs one
    │                               enchant, not a stable sort of all of
    │                               them). 5 new unit tests (16 total in
    │                               the crate) cover: gems affecting the
    │                               full fingerprint but not the major-
    │                               modifier key, stars/reforge still
    │                               affecting the major-modifier key,
    │                               picking the highest-level enchant
    │                               correctly, the key changing when the
    │                               top enchant changes, and determinism
    │                               with no ExtraAttributes at all.
    ├── pricing/                    DONE: in-memory price cache. Rewritten
    │   src/lib.rs                   in the tiered-pricing session into a
    │                               three-tier structure — see the
    │                               "Pricing is three-tier" architecture
    │                               bullet above for the motivation.
    │                               PriceCache now holds three maps: `exact`
    │                               and `major` are each a
    │                               FingerprintShardedMap (the original
    │                               SHARD_COUNT=64 ArcSwap<HashMap<
    │                               Fingerprint, PriceEntry,
    │                               FingerprintHasher>> sharding/RCU logic,
    │                               extracted into a private struct and
    │                               reused for both, since both tiers key
    │                               by Fingerprint — Tier 1 the full
    │                               fingerprint, Tier 2 the coarser
    │                               fingerprint::major_modifier_key); `base`
    │                               is a single unsharded
    │                               ArcSwap<HashMap<String, PriceEntry>>
    │                               keyed by bare item id — Tier 3's key
    │                               space (distinct SkyBlock item ids) is
    │                               small enough that sharding wouldn't
    │                               meaningfully cut contention. Shard index
    │                               is still fingerprint.0 & (SHARD_COUNT -
    │                               1). get(exact, major, base_item_id) ->
    │                               Option<PriceLookup> tries Tier 1, then
    │                               Tier 2, then Tier 3 in that fixed order
    │                               (never the other way — "keep exact
    │                               fingerprint as the highest priority" is
    │                               a hard requirement, not just a
    │                               preference) and tags the hit with which
    │                               tier it came from
    │                               (PriceLookup { entry: PriceEntry, tier:
    │                               PriceTier }, PriceTier ∈ {Exact,
    │                               MajorModifiers, BaseItem}); individual
    │                               get_exact/get_major/get_base accessors
    │                               are also public for callers that only
    │                               need one tier (e.g. cofl's tests). All
    │                               lookups stay wait-free: one atomic load
    │                               per tier tried + a hashmap probe, no
    │                               allocation, no locking — Tier 3's
    │                               `&str` lookup against
    │                               HashMap<String, _> works via the Borrow
    │                               trait, so it never requires an owned
    │                               String either (explicitly tested).
    │                               update_exact_batch/update_major_batch/
    │                               update_base_batch each do their own
    │                               RCU (.rcu() per touched shard for the
    │                               sharded tiers, one .rcu() for the
    │                               unsharded base tier) — same
    │                               clone-on-write + compare-and-swap
    │                               semantics as before, still never
    │                               blocking a concurrent get(). PriceEntry
    │                               is unchanged in shape (estimated_value:
    │                               u64, sample_size: u32,
    │                               updated_at_tick: i64 — raw Hypixel
    │                               epoch-millis despite the "tick" name,
    │                               see the engine entry's bug-fix note —
    │                               source: PriceSource), plus a new
    │                               confidence() -> Confidence method.
    │                               (market-model session) update_*_batch
    │                               no longer does a blind HashMap::insert
    │                               on every write — see the crate's new
    │                               "Cross-write merging" module doc
    │                               section for the full policy, summarized:
    │                               merge_price_entry(existing, incoming)
    │                               is called whenever a key already has a
    │                               cached entry. Same key + both
    │                               PriceSource::Live -> merged via a
    │                               sample-size-weighted running average
    │                               (weight = sample_size, capped at
    │                               MAX_ACCUMULATED_SAMPLE_SIZE=200 so a
    │                               long-running entry doesn't become
    │                               permanently unresponsive to real price
    │                               drift), with the incoming value first
    │                               passed through dampen_outlier() (clamps
    │                               it to within OUTLIER_DAMPING_FACTOR=5x
    │                               of the existing estimate before
    │                               averaging in, so one troll/mistake
    │                               listing moves the estimate by a bounded
    │                               step instead of redefining it outright).
    │                               Same key + both PriceSource::Historical,
    │                               or a cross-source change either
    │                               direction -> incoming replaces existing
    │                               outright, deliberately not merged (each
    │                               cofl::backfill cycle already recomputes
    │                               a full-population median, and a live ask
    │                               shouldn't be blended with a sold-price
    │                               median). A true streaming median (store
    │                               raw sample values, not one scalar) was
    │                               considered and rejected: it would grow
    │                               PriceEntry past a trivial Copy struct
    │                               and meaningfully increase the cost of
    │                               the ShardMap::clone(current) full-shard
    │                               clone update_batch already does every
    │                               write — background-lane, not hot-path,
    │                               but not free either, and speed is
    │                               priority #1. The read path (get/
    │                               get_exact/get_major/get_base) is
    │                               completely untouched by any of this —
    │                               PriceEntry's size didn't change.
    │                               update_exact_batch/update_major_batch/
    │                               update_base_batch now return MergeStats
    │                               { live_merged, overwritten,
    │                               inserted_new } instead of () (existing
    │                               callers that ignored the return value,
    │                               e.g. cofl::backfill, still compile
    │                               unchanged — Rust allows discarding a
    │                               non-() value in statement position, and
    │                               MergeStats is deliberately not
    │                               #[must_use]). New PriceCache::stats()
    │                               -> CacheStats { total_entries,
    │                               total_sample_size,
    │                               high_confidence_entries } walks every
    │                               entry at every tier once — same cost
    │                               class as the existing len() family, not
    │                               hot-path, meant for once-per-tick
    │                               diagnostics (see ingestion/main.rs
    │                               below). CacheStats::average_sample_size()
    │                               derives the mean on demand (0.0 on an
    │                               empty cache, no divide-by-zero panic).
    │                               Confidence is a 3-value ordered enum
    │                               (Low < Medium < High) derived from
    │                               sample_size via
    │                               Confidence::from_sample_size: High
    │                               requires >= 50, Medium >= 10, else Low.
    │                               These thresholds are a deliberate
    │                               deviation from the task's own
    │                               illustrative example ("200 = high, 5 =
    │                               low") — a literal 200-sample bar would
    │                               make High confidence essentially
    │                               unreachable for most SkyBlock items,
    │                               undermining the "many consistent
    │                               flips" goal the thresholds are supposed
    │                               to serve; 10/50 keeps the intent
    │                               (more samples = more trust) while
    │                               staying reachable. What estimated_value
    │                               means (median? trimmed mean?) is still
    │                               deliberately left to the caller/profit
    │                               engine, not this crate. 25 unit tests
    │                               (confidence thresholds/ordering, get()
    │                               falling back correctly through all
    │                               three tiers in priority order, exact
    │                               always winning when all three have
    │                               data, cross-shard isolation, empty-batch
    │                               no-ops at every tier, concurrent
    │                               writers not losing data, reads never
    │                               blocking a writer, the Tier-3
    │                               borrowed-&str-lookup property, plus 9
    │                               new market-model-session tests: same-
    │                               source-Live updates merge not overwrite
    │                               (both the exact and base tiers), cross-
    │                               source updates still overwrite outright,
    │                               same-source-Historical updates still
    │                               overwrite outright, merging weights by
    │                               existing sample_size rather than a flat
    │                               average, a single wildly-off observation
    │                               gets dampened rather than dominating,
    │                               accumulated sample_size is capped, a
    │                               capped entry still responds to new
    │                               data, stats() on an empty cache, and
    │                               stats() aggregating correctly across
    │                               all three tiers) plus two #[ignore]'d
    │                               benchmarks (`cargo test -p pricing
    │                               --release -- --ignored --nocapture`) —
    │                               measured ~70-110 ns/op for a Tier-1 hit
    │                               over 50,000 entries across different
    │                               runs in this session (the read path
    │                               itself is provably untouched by the
    │                               market-model-session changes — the
    │                               variance is this sandbox's shared CPU,
    │                               not a code regression) and ~370-380
    │                               ns/op for the worst case (a miss at all
    │                               three tiers) — both still well inside
    │                               the <1 µs budget line below.
    ├── engine/                     DONE: profit calculation engine.
    │   src/lib.rs                   evaluate(item, price: Option<
    │                               PriceLookup>, current_tick, fees:
    │                               &FeeSchedule, thresholds:
    │                               &FlipThresholds) -> FlipVerdict.
    │                               (tiered pricing session) Signature
    │                               changed from Option<PriceEntry> to
    │                               Option<PriceLookup> — the caller still
    │                               does the cache lookup and passes the
    │                               result in, same decoupling as before,
    │                               just carrying which tier the price
    │                               came from alongside the entry itself.
    │                               Pure/sync, no I/O, no async,
    │                               depends only on parser (ParsedItem)
    │                               and pricing (PriceLookup/PriceEntry/
    │                               PriceSource/PriceTier/Confidence
    │                               types only — NOT PriceCache; this
    │                               crate still doesn't depend on the
    │                               fingerprint crate at all).
    │                               Before the profit math, two gates run,
    │                               keyed on (tier, source) — not tier
    │                               alone, as of the confidence-protection
    │                               session (see below): (1) a minimum-
    │                               confidence gate — entry.confidence()
    │                               must be >=
    │                               thresholds.min_major_modifier_confidence
    │                               (default Confidence::Medium, i.e.
    │                               sample_size >= 10) for Tier 2, >=
    │                               thresholds.min_base_item_confidence
    │                               (default Confidence::High, i.e.
    │                               sample_size >= 50) for Tier 3, or >=
    │                               thresholds.min_live_exact_confidence
    │                               (default Confidence::Medium) for
    │                               (Tier 1, PriceSource::Live) — otherwise
    │                               FlipVerdict::InsufficientSampleSize;
    │                               (Tier 1, PriceSource::Historical) is
    │                               the only case with no floor at all;
    │                               (2) an ROI multiplier — roi_percent
    │                               must clear min_roi_percent *
    │                               thresholds.major_modifier_roi_multiplier
    │                               (1.5) for Tier 2, * base_item_roi_
    │                               multiplier (3.0) for Tier 3, *
    │                               live_exact_roi_multiplier (1.2) for
    │                               (Tier 1, Live), or the flat
    │                               min_roi_percent for (Tier 1,
    │                               Historical) only — while min_profit is
    │                               deliberately left unmultiplied
    │                               everywhere, since it's what makes a
    │                               flip worth clicking at all
    │                               ("coins/hour"), not a data-quality
    │                               signal. The 1.2 value for Live-Exact
    │                               is deliberately less than Tier 2's 1.5:
    │                               an exact fingerprint match is still a
    │                               strictly more precise item-identity
    │                               match than Major regardless of source,
    │                               so it earns a lighter margin once
    │                               past its own confidence floor —
    │                               preserving a meaningful ordering
    │                               (Historical-Exact 1.0x < Live-Exact-
    │                               confident 1.2x < Major 1.5x < Base
    │                               3.0x) rather than letting Live-Exact
    │                               collapse to the same number as Major
    │                               by coincidence. ProfitCalculation
    │                               gained a `tier: PriceTier` field so
    │                               downstream consumers (logging,
    │                               notifications) can see how a reported
    │                               flip's price was derived.
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
    │                               a real opportunity). 30 unit tests
    │                               (13 original + 5 staleness + 8 from
    │                               the tiered-pricing session + 4 new in
    │                               the confidence-protection session:
    │                               Historical-Exact still trusted at low
    │                               confidence — the regression guard for
    │                               "keep COFL highest trust" — Live-Exact
    │                               rejected below Medium confidence and
    │                               passing at/above it, Live-Exact
    │                               needing a bigger ROI margin than
    │                               Historical-Exact, and Live-Exact
    │                               needing a *smaller* ROI margin than
    │                               Major — proving "keep exact
    │                               fingerprint priority" still holds
    │                               under the new source-aware gating; 2
    │                               pre-existing tests renamed/replaced,
    │                               ~10 more had their PriceLookup's
    │                               sample_size bumped from 5 to 20 in
    │                               their fixtures so they keep testing
    │                               what they originally tested — e.g.
    │                               staleness, zero-price, plausibility —
    │                               rather than getting short-circuited
    │                               by the new Live-Exact confidence
    │                               floor before reaching the logic under
    │                               test) cover every FlipVerdict variant,
    │                               overflow/panic safety at extreme
    │                               values, and a hand-computed profit/
    │                               tax/ROI example. One #[ignore]'d
    │                               benchmark (`cargo test -p engine
    │                               --release -- --ignored --nocapture`)
    │                               measured ~23 ns/op for evaluate()
    │                               post-confidence-protection (was ~3.8
    │                               ns/op pre-session — a real, repeatable
    │                               increase, not noise, from the extra
    │                               (tier, source) tuple matches; still
    │                               ~40x under the "<1 µs" profit-
    │                               calculation budget in the latency
    │                               table below, and evaluate() remains
    │                               dwarfed by every other pipeline stage)
    │                               — note this doesn't include the cache
    │                               lookup itself (~70-110 ns for a Tier-1
    │                               hit across this session's runs,
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
    │                               roi_percent, tier, price_source,
    │                               #[serde(skip)] auction_end } — the
    │                               caller builds this from primitive
    │                               ParsedItem + ProfitCalculation
    │                               fields; this crate depends on
    │                               neither `parser` nor `engine`, same
    │                               decoupling reasoning as `engine` not
    │                               depending on `pricing::PriceCache`.
    │                               (output/readability session) tier
    │                               and price_source are `&'static str`
    │                               (`"Exact"/"Major"/"Base"`,
    │                               `"Live"/"COFL"`), not
    │                               `pricing::PriceTier`/`PriceSource` —
    │                               the caller (already holding those
    │                               enums from `engine::
    │                               ProfitCalculation`/`pricing::
    │                               PriceLookup`) picks the label, so
    │                               this crate still doesn't gain a
    │                               `pricing` dependency, preserving the
    │                               same decoupling this module doc
    │                               comment already argues for. Both
    │                               fields flow straight into the
    │                               serialized WebSocket JSON payload
    │                               too, at no extra cost (same one
    │                               `serde_json::to_string` call
    │                               `NotificationHub::publish` already
    │                               made). `new()` builds
    │                               viewauction_command as
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
    │                               wiremock) -> fingerprint_of() /
    │                               major_modifier_key_of() (tiered
    │                               pricing session: both build a shared
    │                               synthetic_item() ParsedItem, then call
    │                               the SAME fingerprint::fingerprint() /
    │                               fingerprint::major_modifier_key() live
    │                               auctions use — not parallel
    │                               implementations) -> grouped three ways
    │                               in parallel from the same sales — by
    │                               exact Fingerprint, by major-modifier
    │                               Fingerprint, and by bare item tag —
    │                               median sale price per group (shared
    │                               median_entries() helper, generic over
    │                               the key type) -> PriceEntry { source:
    │                               Historical } -> PriceCache::
    │                               update_exact_batch() /
    │                               update_major_batch() /
    │                               update_base_batch(). One COFL sale now
    │                               seeds all three pricing tiers at once
    │                               — it's simultaneously an exact match
    │                               for its own fingerprint, a
    │                               major-modifier match for its item +
    │                               reforge/stars/hpc/top-enchant
    │                               combination, and a base-item match for
    │                               its bare item tag — without any
    │                               additional network round-trip. Two-tier
    │                               accuracy (unrelated to pricing tiers —
    │                               this is about how faithfully one
    │                               sale's modifiers are reconstructed):
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
    │                               item_tags_attempted, sales_fetched,
    │                               fingerprints_loaded, fetch_errors,
    │                               sales_from_real_nbt vs
    │                               sales_from_reconstruction (the
    │                               single most useful live-run signal
    │                               for judging whether the
    │                               shortItemBytes assumption held), and
    │                               (renamed/split in the tiered-pricing
    │                               session from the old single
    │                               cache_entries_seeded field)
    │                               exact_entries_seeded,
    │                               major_modifier_entries_seeded, and
    │                               base_item_entries_seeded — seeded from
    │                               the same sales, not additional
    │                               fetches, so these three can differ a
    │                               lot from each other (e.g. many
    │                               distinct exact fingerprints but few
    │                               base item tags) without anything being
    │                               wrong. backfill() rate-limits itself
    │                               via COFL_REQUEST_DELAY and is meant to
    │                               run at startup, optionally repeating
    │                               periodically (see main.rs below),
    │                               concurrently with (not blocking)
    │                               ingestion. (price coverage session)
    │                               COFL_REQUEST_DELAY raised 350ms ->
    │                               650ms: 350ms works out to ~171
    │                               req/min, which quietly exceeded the
    │                               ~100 req/min limit already documented
    │                               in this same doc comment -- a
    │                               plausible silent contributor to
    │                               truncated coverage (mid-crawl
    │                               fetch_errors) even before the tag
    │                               list was broadened. Also restructured
    │                               to flush each item tag's three tiers
    │                               to the cache as soon as that tag's
    │                               pages are fetched (per-tag grouping
    │                               maps, reset each iteration) instead
    │                               of accumulating every tag in memory
    │                               and writing once after the whole
    │                               crawl -- with a ~100-item tag list
    │                               (see the `common` crate below) a
    │                               single end-of-crawl write would have
    │                               delayed every price from becoming
    │                               usable until the entire multi-minute
    │                               crawl finished; per-tag flushing
    │                               means the first tag's prices land in
    │                               the cache within about one tag's
    │                               worth of requests (a few seconds).
    │                               ImportStats' three `*_entries_seeded`
    │                               counters now accumulate across all
    │                               tags (`+=` per tag) rather than being
    │                               computed once at the end. 14 unit
    │                               tests (same count, behavior-
    │                               preserving refactor — end-state
    │                               assertions unchanged since the total
    │                               written data is the same, just
    │                               written progressively), including
    │                               one proving
    │                               the shortItemBytes decode path
    │                               produces byte-identical fingerprints
    │                               to a live auction, one proving the
    │                               reconstruction fallback matches the
    │                               real-NBT path when the assumed key
    │                               names hold (a regression check on
    │                               the assumptions themselves), a
    │                               wiremock-based end-to-end test of
    │                               backfill() seeding all three tiers
    │                               from a multi-sale, multi-page
    │                               response, and (new) a test proving
    │                               two sales with different modifiers
    │                               land in different Tier 1/2 buckets
    │                               but the same Tier 3 bucket.
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
- `cargo test --workspace` — passes (98 run + 3 `#[ignore]`'d
  benchmarks = 101 tests: 1 common, 6 diff, 5 parser, 16 fingerprint
  (+5 major_modifier_key tests from the tiered-pricing session), 16
  pricing (+2 benchmarks — rewritten three-tier `PriceCache`, tiered
  pricing session), 26 engine (+1 benchmark, +8 new tier/confidence
  tests from the tiered-pricing session on top of the COFL session's
  +5 staleness tests), 9 notify, 14 cofl (+1 tiered-seeding test), 4
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
- `pricing::PriceCache::get` benchmarked at ~70-110 ns/op for a Tier-1
  hit over 50,000 entries across this workspace's sessions (~311-380
  ns/op worst case, a miss at all three tiers); `engine::evaluate`
  benchmarked at ~23 ns/op post-confidence-protection (was ~3.8 ns/op
  before that session — see "Confidence-protection session" above for
  why the increase is real, not noise, and still negligible in
  context) — both exclude the cache lookup itself, both release build,
  single-threaded. See the respective crate entries
  above for how to reproduce. Note: an
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
- (price coverage session) `cargo build --workspace` (debug and
  `--release`), `cargo test --workspace` (still 98 passed / 3 ignored
  benchmarks — this session's changes were background-lane
  restructuring, not new hot-path logic, so no new tests were added;
  existing `cofl` tests already cover the per-tag-flush refactor since
  they assert end-state, which is unchanged), and
  `cargo clippy --workspace --all-targets` (clean except the same
  pre-existing `ingestion/src/lib.rs` warnings) all still pass after:
  broadening `DEFAULT_COFL_BACKFILL_ITEM_TAGS`, adding
  `cofl_backfill_interval_minutes`, restructuring `cofl::backfill` to
  flush per-tag, raising `COFL_REQUEST_DELAY`, and adding the periodic
  re-backfill loop in `ingestion/main.rs`. Not yet run live — this
  sandbox still has no network access to `sky.coflnet.com` or
  `api.hypixel.net` to observe the actual before/after effect on
  `diag_no_price_data_total` / `diag_cache_hit_total` / the per-tier
  hit counters.
- (output/readability session) `cargo build --workspace` (debug and
  `--release`), `cargo test --workspace` (still 98 passed / 3 ignored
  — `notify`'s existing test suite covers the two new `FlipAlert`
  fields, `format_coins`/`tier_label`/`price_source_label` are plain
  pure functions not worth dedicated unit tests over), and
  `cargo clippy --workspace --all-targets` (clean except the same
  pre-existing `ingestion/src/lib.rs` warnings) all pass after adding
  `tier`/`price_source` to `FlipAlert` and restructuring
  `ingestion/main.rs`'s logging into per-flip/per-tick-summary/per-
  tick-debug-dump (see the `notify` and `ingestion main.rs` entries
  above). Visually verified the exact log line formatting (not just
  that it compiles) by copying the same `format_coins`/tracing macro
  calls into a disposable throwaway binary (built and run outside the
  workspace, deleted after) with values matching the live numbers from
  the price coverage session — confirmed both the per-flip line
  ("FLIP  Hyperion  |  buy 800.0M -> value 1.2B  |  profit +380.0M
  (47.5% ROI)  |  tier Exact / Live  |  /viewauction abc-123-def") and
  the per-tick summary line ("tick 1785736343562: 2 flip(s) | cache
  118/43585 hits (0.3%) [exact 107 / major 2 / base 9] | no-price
  43467 | 4.2ms") render as designed, with the full structured
  `key=value` fields still present after the message text. Not
  re-verified against an actual live-running `ingestion-service`
  process end to end — same network-access limitation as every other
  live-run caveat in this file.
- (market-model session) `cargo fmt --all`, `cargo test --workspace`
  (108 run + 3 `#[ignore]`'d benchmarks = 111 tests — `pricing` grew
  from 16 to 25 unit tests, see its entry above; every other crate's
  count unchanged), `cargo clippy --workspace --all-targets` (clean
  except the same pre-existing `ingestion/src/lib.rs` warnings), and
  `cargo build --release` all pass after the `PriceCache` merge/
  dampening/stats rewrite and the `ingestion/main.rs` diagnostics
  wiring. Confirmed via `cargo metadata --no-deps` (worth re-running
  after any future restructuring rather than assuming): the package is
  `ingestion`, the binary target is `ingestion-service` — that's the
  name to use with `cargo run --bin ingestion-service` /
  `./target/release/ingestion-service` (there is exactly one `[[bin]]`
  in the whole workspace, so plain `cargo run --release` at the
  workspace root also resolves to it unambiguously). Ran the actual
  release binary (`HYPIXEL_API_KEY=test-key-not-real
  COFL_BACKFILL_ENABLED=false ./target/release/ingestion-service`, and
  again with `COFL_BACKFILL_ENABLED=true` and a 1-tag/1-page COFL
  config) — both start cleanly, bind the WebSocket, log correctly, and
  fail gracefully (structured error, non-zero exit, no panic) on the
  expected `sandbox has no network access to api.hypixel.net`
  restriction; same limitation as every other live-run caveat in this
  file, so no live tick was actually processed through the new merge/
  diagnostics code path this way. Separately visually verified the new
  "pricing model" summary line's exact rendering (same disposable-
  throwaway-binary technique as the output/readability session above)
  with representative numbers: "pricing model: 812 entries, avg sample
  size 6.7 | 94 high-confidence | 2310 live merges, 145 replaced
  (cumulative) | 3021 flips rejected for insufficient data
  (cumulative)". `pricing::PriceCache::get` re-benchmarked at ~70-110
  ns/op for a Tier-1 hit across repeated runs in this session (up from
  the tiered-pricing session's ~55-73 ns/op baseline) — the `get()`
  code path is provably unchanged by this session's diff (verified via
  `git diff` on the function bodies), so this is read as this sandbox's
  shared-CPU noise, not a real regression; worth a clean re-benchmark
  on dedicated hardware if the gap needs to be nailed down precisely.
- (confidence-protection session) `cargo fmt --all` (no changes
  needed), `cargo test --workspace` (111 tests — `engine` grew from 26
  to 30 unit tests, see its entry above; every other crate's count
  unchanged), `cargo clippy --workspace --all-targets` (clean except
  the same pre-existing `ingestion/src/lib.rs` warnings), and
  `cargo build --release` all pass after adding the `(tier, source)`-
  keyed confidence gate and ROI multiplier to `engine::evaluate`. Also
  re-ran the release binary smoke test (`HYPIXEL_API_KEY=test-key-not-
  real COFL_BACKFILL_ENABLED=false ./target/release/ingestion-service`)
  — still starts, binds, and fails gracefully on the expected network
  restriction. `engine::evaluate` re-benchmarked at ~23 ns/op,
  confirmed via three repeated runs to be a real, repeatable increase
  from the ~3.8 ns/op tiered-pricing-session baseline (not noise, in
  contrast to `pricing::PriceCache::get`'s variance in the market-
  model session above) — see "Confidence-protection session" below for
  why this is still negligible relative to the pipeline's real
  bottlenecks.

## Price coverage investigation (price coverage session)

A live run reported `flips_found=8`, `diag_cache_hit_total=118` against
`diag_no_price_data_total=43467` (tier breakdown: exact=107, major=2,
base=9) — i.e. well under 1% of evaluated auctions had *any* usable
price, at any tier. Root-caused via code review (again, no live
COFL/Hypixel access from this sandbox to confirm the fix's actual
effect):

1. **Dominant cause: `COFL_BACKFILL_ITEM_TAGS` only covered 6 items.**
   `cofl::backfill` is the only source of Tier 1/2/3 coverage for items
   the live feed hasn't organically produced a repeat sighting for yet,
   and it only ever fetches sold-auction history for the tags it's
   told about. With 6 tags, every other item in a ~46,800-auction
   snapshot — thousands of distinct `skyblock_item_id`s — started with
   *zero* entries at every tier, Tier 3 (bare item id) included. This
   is not a fingerprint-specificity problem (Tier 1 being narrow is
   intentional, see the tiered-pricing session's design), it's a
   coverage problem: Tier 2/3 exist to compensate for Tier 1 missing,
   but can't help an item COFL never fetched at all.
2. **Compounding, independently-found bug: COFL request pacing quietly
   exceeded COFL's own documented rate limit.** `COFL_REQUEST_DELAY`
   was 350ms between requests (~171 req/min), against a documented
   ~100 req/min limit (already recorded in the `cofl` crate's own doc
   comment, just never checked against the actual delay value used).
   A plausible silent contributor to `fetch_errors` truncating
   per-tag crawls before they finished, on top of the narrow tag list.
3. **Secondary factor: `backfill()` only ran once, at startup**,
   despite the original COFL task's explicit "seed/update... before
   **and during** operation" requirement — coverage was frozen at
   whatever the initial 6-tag crawl produced, with no mechanism to grow
   or refresh it while the process kept running.

**Fixed, all background-lane only, zero hot-path change:**
- `DEFAULT_COFL_BACKFILL_ITEM_TAGS` (`crates/common/src/lib.rs`)
  expanded from 6 to ~100 items (endgame weapons/accessories for the
  "occasional massive flip" side of the goal, plus high-volume
  enchanted crafting materials for the "many consistent 1-10m flips"
  side, since those trade in huge numbers with essentially no
  meaningful modifiers and so reach Tier 1/3 confidence fast). Sourced
  from public SkyBlock community knowledge, not a live-verified
  payload — same caveat already applied elsewhere in this project to
  NBT tag names; a wrong/nonexistent tag degrades gracefully (counted
  as a `fetch_errors`, never panics or blocks the rest of the crawl).
- `COFL_REQUEST_DELAY` raised 350ms → 650ms (`crates/cofl/src/lib.rs`)
  to actually stay under the documented ~100 req/min limit.
- `cofl::backfill` restructured to flush each tag's three tiers to the
  cache immediately after that tag's pages are fetched, instead of
  accumulating the whole (now much longer) crawl in memory and writing
  once at the end — coverage now grows progressively, with the first
  tag's prices usable within seconds instead of waiting for the full
  multi-minute crawl.
- New `cofl_backfill_interval_minutes` config (default 60, `0` =
  startup-only) drives a periodic re-run of `cofl::backfill` in
  `ingestion/main.rs`'s already-spawned, never-awaited background task
  — finally implementing the "during operation" half of the original
  COFL requirement.

**Deliberately not done this session** (flagged below instead): making
the live feed's own per-tick cache writes accumulate sample size across
ticks instead of overwriting. Right now `PriceCache::update_*_batch`
always overwrites a key's entry with the latest batch's value —
combined with diff detection only listing each BIN auction once, a
given fingerprint/major-key/base-id's `sample_size` from the live feed
alone rarely climbs past single digits before being overwritten by the
next tick's (usually smaller) observation, capping how much Tier 1/2
confidence can organically grow from live data independent of COFL.
Fixing this safely (merge/rolling-average instead of overwrite) is a
real lever, but needs a considered design for how it interacts with
`PriceSource` (Live-over-Live should probably merge; a fresh Live
observation replacing a stale Historical one should probably still be
a straight overwrite, not a blend) and with the new periodic COFL
re-backfill (each COFL run recomputes its own full-population median,
which should probably overwrite rather than merge with a prior run) —
see "Flagged for a future pass" below.

**What to check on the next live run**, in priority order: whether
`diag_cache_hit_total` / `diag_no_price_data_total`'s ratio improved
materially; whether the per-tier split (exact/major/base hit counts)
shifted now that Tier 2/3 have real coverage beyond 6 items; whether
`cofl::ImportStats` logged at each backfill cycle shows `fetch_errors`
staying low (confirms the rate-limit fix actually mattered); and
whether the periodic re-backfill's second cycle (after
`cofl_backfill_interval_minutes`, default 60 minutes) shows growing
`*_entries_seeded` counts as COFL accumulates more sold-auction history
over time.

## Market-model session: price-accuracy audit and fix

The user manually tested the sniper and found real resale prices
didn't match displayed `estimated_value` — flips were being found, but
valuations were sometimes wrong. Investigated via code review (no live
Hypixel/COFL access in this sandbox to reproduce directly — confirmed
again this session: `api.hypixel.net`/`sky.coflnet.com` both 403 at
the proxy). Root cause, confirmed against the exact source (not
inferred): `PriceCache::update_*_batch` did a blind `HashMap::insert`
on every write, and `ingestion::main`'s per-tick aggregation
(`accumulate_cheapest_bin`) builds a fresh per-key map every tick — so
a Tier‑1 (exact fingerprint) `estimated_value` was routinely just *one
seller's current asking price*, `sample_size` effectively reset to 1
every tick, and Tier 1 has no confidence floor of its own to catch
that (see `engine::evaluate`, `PriceTier::Exact => None`). A single
mistake or lowball listing could define "market value" for whatever
got evaluated against it next tick — a concrete, worked (not live-
captured) example is in this session's earlier turn.

Two smaller, related findings from the same audit, **not acted on this
session** (out of scope — the user's follow-up task was specifically
the sample-accumulation fix, not these):
- `fingerprint::hash_gems`'s compound-shaped-gem branch (`crates/
  fingerprint/src/lib.rs`) hashes only a gem slot's `quality`, not
  which gem *type* occupies it — two items with different, very
  differently-priced gems (e.g. Ruby vs. Jasper) at the same quality
  could fingerprint identically at Tier 1. Unconfirmed whether real
  Hypixel gem NBT is actually compound-shaped (same "not live-
  verified" caveat this crate already carries throughout) vs. the
  bare-string shape the code also handles correctly.
- COFL's historical median has no time-window control (no `sort`/
  date-range parameter is sent to `/sold?page=N`), while the entry's
  staleness gate is anchored to the single *most recent* sale in the
  group (`cofl::median_entries`) — a group could blend old and new
  sales while still looking "fresh" under the 30-day ceiling.

**Fixed this session** (the sample-accumulation root cause):
`pricing::PriceCache` now merges same-key, same-`PriceSource::Live`
writes via a sample-size-weighted running average instead of
overwriting, with incoming values passed through a bounded outlier
clamp first (`dampen_outlier`, max 5x move per merge) and accumulated
`sample_size` capped (200) so old data's weight doesn't grow forever.
Cross-source writes and same-source-Historical refreshes still replace
outright (COFL already recomputes a full-population median per cycle;
a live ask and a sold-price median shouldn't be blended). See the
`pricing` crate entry above for the full design and the new
`MergeStats`/`CacheStats` diagnostics, and the `ingestion main.rs`
entry for how they're surfaced. Per explicit instruction, the intra-
tick "cheapest BIN this tick" aggregation (`accumulate_cheapest_bin`)
was deliberately **not** changed to an average — for a flipping bot
the lowest legitimate BIN in a tick is the real opportunity signal,
not something to dilute; the fix is entirely in what happens to that
per-tick minimum once it reaches the cache across ticks, not in how
it's computed within one tick.

**Not yet observed against live data**: whether
`MAX_ACCUMULATED_SAMPLE_SIZE`/`OUTLIER_DAMPING_FACTOR`'s chosen values
(200 / 5x) behave well against real SkyBlock listing volume and
volatility, and whether `diag_live_merged_total` /
`cache_average_sample_size` / `cache_high_confidence_entries` actually
show the cache building real multi-tick evidence over a live run's
lifetime rather than staying dominated by single-observation entries.

## Confidence-protection session: Live-Exact was source-blind

Live user test data (after the market-model session's aggregation
fix): processing stayed fast (~50ms/50k auctions), merging worked (302
live merges, ~6 avg sample size, 20k+ cached entries), but flips were
still dominated by `tier Exact / Live` — e.g. "FLIP Snowy Gillsplash
Gloves | buy 67M -> value 78.9M | Exact / Live". Root cause, confirmed
directly against `engine::evaluate`'s source: the confidence gate and
ROI multiplier were keyed on `tier` alone —
`PriceTier::Exact => None` unconditionally, meaning *any*
`(PriceTier::Exact, PriceSource::Live)` entry was trusted with no
confidence floor and the unmultiplied `1.0x` ROI bar, identical
treatment to a COFL `Historical` median built from real completed
sales. Tier 2/3 already had confidence floors specifically *because*
they're coarser matches; Tier 1 never got the analogous protection
against a *thin-sample* match, because "exact fingerprint" was
conflated with "trustworthy price."

**Fixed**: `FlipThresholds` gained `min_live_exact_confidence`
(default `Confidence::Medium`, 10+ samples) and
`live_exact_roi_multiplier` (default `1.2x`), applied only to
`(PriceTier::Exact, PriceSource::Live)` — both gates now key on
`(tier, source)`, not `tier` alone (see the `engine` entry above for
the exact match arms and the reasoning behind `1.2` specifically:
smaller than Tier 2's `1.5x` since an exact match is still more
precise than a coarser one regardless of source, preserving a
meaningful trust ordering rather than letting Live-Exact collapse to
Major's number by coincidence). `(PriceTier::Exact,
PriceSource::Historical)` is completely unaffected — no floor, `1.0x`
multiplier, exactly as before — verified by a new dedicated regression
test (`historical_exact_tier_is_trusted_even_at_low_confidence`) so
"keep COFL historical pricing highest trust" isn't just a comment, it's
enforced by the test suite. Tier 2/3 are untouched regardless of
source; this is scoped exactly to the gap the live data demonstrated.

**Expected impact**: given the reported ~6 average sample size across
the cache (below the new floor of 10), a meaningful share of
Exact/Live flips should now be rejected as `InsufficientSampleSize`
until accumulation (market-model session) has had more ticks to build
real evidence. COFL flips and Tier 2/3 Live flips are unaffected.
`engine::evaluate`'s own benchmark moved from ~3.8 ns/op to ~23 ns/op —
a real, repeatable cost from the extra `(tier, source)` matching, but
still ~40x under the "<1 µs" profit-calculation budget and nowhere
close to `PriceCache::get()`'s ~70-110 ns or any I/O-bound stage.

**Not yet observed against live data**: whether `Confidence::Medium`
(10) and `1.2x` are well-calibrated, or whether Exact/Live flip volume
drops more (or less) than expected once real accumulation has had time
to run. Worth checking `diag_insufficient_sample_total`'s growth rate
and the `tier Exact / Live` share of reported flips on the next live
run.

## Flagged for a future pass (not yet acted on)

The user pointed out, correctly, that the biggest speed win left on
the table is not in this compute pipeline — it's ingestion's tick
detection and fetch latency. Real numbers from a live run:
`detect_latency_ms=238`, `snapshot_fetch_latency_ms=1074`. Both dwarf
anything downstream (fingerprinting is µs-scale, price lookup is now
measured at ~73 ns for a Tier-1 hit). This wasn't addressed in
Phase 1.6 since it wasn't in scope for the cache, but it's the natural
next lever once the core pipeline (steps 7-9) is complete, or worth an
early Phase 3-style pass if it's blocking real usage sooner.

**Live-feed sample accumulation** — flagged in the price-coverage
session, **now done** (market-model session). See the "Cross-write
merging" section of the `pricing` crate's module doc comment and the
`pricing`/`ingestion main.rs` entries above for the full design
(sample-size-weighted merge, outlier dampening, capped accumulation,
`MergeStats`/`CacheStats` diagnostics). What's still open from the
original write-up: whether `MAX_ACCUMULATED_SAMPLE_SIZE` (200) and
`OUTLIER_DAMPING_FACTOR` (5x) are well-calibrated for real SkyBlock
trade volumes and price volatility — chosen by reasoning from the
`Confidence::High` threshold and general market-shift plausibility,
not from live data (this sandbox still has no network access to
verify against). Worth revisiting once
`diag_live_merged_total`/`diag_overwritten_total` and
`cache_average_sample_size`/`cache_high_confidence_entries` (new
diagnostics, see below) have been read off a live run.

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
itself. `get()` and `update_batch()` (renamed/split into
`update_exact_batch()`/`update_major_batch()`/`update_base_batch()` in
the later tiered-pricing session, same underlying logic) use the
identical `shard_index()` and `FingerprintHasher`, and the crate's own
tests already prove insert-then-get works correctly. The mechanism is
sound; it's being fed data that structurally can't produce hits for
uniquely-modified items — which the tiered-pricing session's Tier 2/3
fallbacks now directly address.

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

**Decided and implemented (tiered pricing session):** the coarser
fallback fingerprint tier flagged above as "not yet decided" is now
built — see the "Pricing is three-tier" architecture bullet and the
`fingerprint`/`pricing`/`engine`/`cofl`/`ingestion main.rs` entries
above for the full design. Both COFL and the live feed now seed all
three tiers (exact fingerprint, major-modifier key, bare item id) from
the same observations, so items COFL doesn't cover and the live feed
between backfills both get *some* fallback price instead of only a
Tier-1-or-nothing outcome. This did not lower `min_profit`,
`min_roi_percent`, `min_sample_size`, or `max_plausible_roi_percent`
for Tier 1 at all — the two fallback tiers get a *stricter* effective
bar (higher confidence floor, ROI multiplier), never a looser one, per
the explicit "do not loosen everything blindly" instruction. Still not
yet confirmed against live data (this sandbox has no network access to
either `api.hypixel.net` or `sky.coflnet.com`): whether Tier 2/3 in
practice meaningfully raise `flips_found` on top of what the COFL
backfill and staleness fix already contribute, and whether the
`major_modifier_roi_multiplier`/`base_item_roi_multiplier` defaults
(1.5 / 3.0) and confidence thresholds (Medium=10/High=50 samples) are
well-calibrated for real SkyBlock trade volumes — worth revisiting
once `diag_tier_exact_hit_total` /
`diag_tier_major_modifier_hit_total` / `diag_tier_base_item_hit_total`
(new diagnostic counters, see the `ingestion main.rs` entry above) have
been read off a live run.

## Immediate next step

**Run it live again** — this is still the single highest-value next
action, not new code. The first live run (which prompted the price
coverage session above) confirmed `flips_found` is nonzero (8) — the
staleness fix and tiered pricing are both doing *something* — but
surfaced the coverage gap the last session's fixes fixed:
`diag_no_price_data_total=43467` vs `diag_cache_hit_total=118`
(exact=107, major=2, base=9). A follow-up live run, after the price
coverage session's changes, would confirm or correct, in priority
order:
1. Whether `diag_cache_hit_total`'s share of total evaluated auctions
   improved materially now that COFL covers ~100 items instead of 6 —
   this is the headline number the whole session was aimed at.
2. Whether the per-tier split (`diag_tier_exact_hit_total` /
   `diag_tier_major_modifier_hit_total` / `diag_tier_base_item_hit_total`)
   shifted — Tier 2/3 should now see meaningfully more hits than the
   2/9 observed pre-fix, since backfill seeds all three tiers per tag
   and there are now ~17x more tags.
3. Whether `cofl::ImportStats`' `fetch_errors` stays low across a full
   backfill cycle — confirms the 350ms→650ms rate-limit fix actually
   mattered (a still-high `fetch_errors` would point at a different
   cause, e.g. a tag in the new ~100-item list that COFL genuinely
   doesn't recognize).
4. Whether `cofl::ImportStats.sales_from_real_nbt > 0` — still the
   single highest-leverage unverified assumption in the whole COFL
   integration (unrelated to this session's changes, still open from
   the original COFL session).
5. Whether `flips_found` itself grew, and by how much — the actual
   coins/hour-relevant outcome, not just the intermediate cache-hit
   metric.

Other open items, unblocked by this and listed in priority-neutral
order:

1. **Ingestion latency.** Flagged since Phase 1.6, still real:
   `detect_latency_ms=238`, `snapshot_fetch_latency_ms=1074` from a
   live run dwarf the entire rest of the pipeline (fingerprinting is
   µs-scale, price lookup ~70-110 ns for a Tier-1 hit / ~311-380 ns
   worst case across all three tiers, evaluate ~23 ns). This is the
   "detecting Hypixel's cache refresh as fast as possible" lever called
   out as the main competitive edge at the top of this file, and it's
   the one part of the stack that hasn't been touched since the very
   first session.
2. **Phase 2 — Website**, per the Build order section above: live
   flip feed UI, user-configurable min-profit/min-ROI settings (which
   `engine::FlipThresholds` and `pricing`'s placeholder feed are
   already structured to accept once they exist).
3. **Phase 3 — Optimization**: now that every stage has some latency
   number (measured or estimated), a real profiling pass against live
   Hypixel traffic would confirm or correct the budget table above
   rather than relying on synthetic benchmarks.
4. **(market-model session) Two smaller price-accuracy findings, not
   yet acted on**: the `fingerprint::hash_gems` compound-shape branch
   dropping gem *type* (only `quality` is hashed), and COFL's
   historical median having no time-window control despite a
   staleness gate anchored to only the newest sample in the group. See
   "Market-model session: price-accuracy audit and fix" above for the
   full detail — both need either explicit authorization to fix or
   live COFL/Hypixel access to confirm the underlying assumption
   first.
5. **(market-model session) Calibrate `MAX_ACCUMULATED_SAMPLE_SIZE`/
   `OUTLIER_DAMPING_FACTOR`** (200 / 5x) against real data once a live
   run is possible — chosen by reasoning from `Confidence::High` and
   general market-shift plausibility, not observed behavior.
