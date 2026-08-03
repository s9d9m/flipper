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
2. Hypixel auction ingestion service ✅ done (see below)
3. Auction diff detection ✅ done (see below)
4. Minimal item parser — **next step, not yet built**
5. Item fingerprinting — not built
6. In-memory price cache — not built
7. Profit calculation engine — not built
8. Flip detection — not built
9. WebSocket notification server — not built

Target output of Phase 1: a user connects to the site and receives live
profitable flip alerts.

**Phase 2 — Website:** live flip feed (item name, image, buy price,
estimated value, profit, ROI, `/viewauction` copy button), user settings
for minimum profit/ROI. No unnecessary UI polish.

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
│                                TICK_POLL_INTERVAL_MS, REQUEST_TIMEOUT_MS
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
        │                          diff::DiffDetector and prints only
        │                          the new/changed auctions — Phase 1.4
        │                          (parser) will replace this println!
        │                          with real item parsing.
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
```

Not yet created: `crates/parser`, `crates/pricing`, `crates/engine`,
`crates/notify`, `crates/flipper-server`, `web/`.

## Toolchain notes (read before running cargo update)

Built and tested against **rustc 1.75**. `Cargo.lock` pins
`url = 2.4.1` / `idna = 0.4.0` because newer versions of `idna` pull in
the ICU4X crate family, which requires edition2024 (unsupported on
1.75). If you're on a modern toolchain (1.80+), this pin is not required
— `cargo update -p url -p idna` is safe, just re-run
`cargo test --workspace` after.

## Verified working (as of last session)

- `cargo build --workspace` — passes (debug and `--release`)
- `cargo test --workspace` — passes (9 tests: 1 common, 6 diff, 1
  ingestion wiremock integration, plus doc-tests), on rustc 1.94 (the
  `url`/`idna` pin from the toolchain notes below was not needed)
- `cargo clippy --workspace --all-targets` — clean except pre-existing
  doc-comment lint warnings in `ingestion/src/lib.rs` (unrelated to
  diff detection)
- `ingestion-service` now runs every snapshot through
  `diff::DiffDetector` before printing, so new/changed auctions are
  isolated end-to-end from the channel through diff detection
- Binary starts, loads config, and fails gracefully (structured error
  log, non-zero exit, no panic) when the network is unreachable
- Not yet verified: a live run against the real Hypixel API (the
  environment this was built in can't reach `api.hypixel.net` — only
  package registries were reachable). This needs to be confirmed on a
  machine with real network access and a real `HYPIXEL_API_KEY`.

## Immediate next step

**Phase 1.4: minimal item parser.** Consumes the `Vec<RawAuction>`
`diff::DiffDetector::diff` now emits and decodes `item_bytes`
(base64 → gzip → NBT) into just the fields that affect price (item ID
and the handful of modifiers pricing cares about) — not full NBT
normalization, which is deferred to the async pass per the
fingerprinting design note above. This is the last stage before
fingerprinting and pricing can start.
