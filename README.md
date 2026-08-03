# skyblock-flipper

Latency-first Hypixel SkyBlock AH flip finder. See the design docs for the
full architecture and rationale; this README covers what's actually built
so far and how to run it.

## Status

**Phase 1, step 2 of 9 complete: auction ingestion service.**

Everything else in the roadmap (diff detection, parser, price cache,
profit engine, flip detection, WebSocket notifications, frontend) is not
yet built. Do not assume any crate other than `common` and `ingestion`
exists yet.

## What's built and why (file by file)

```
Cargo.toml                  Workspace manifest. Sets a shared release
                             profile (opt-level=3, thin LTO, single
                             codegen unit) so every future latency
                             benchmark is comparing release-quality
                             builds from day one.

crates/common/
  Cargo.toml                 Minimal deps (serde, serde_json, thiserror).
                              No tokio/reqwest here on purpose — this
                              crate is imported by every other crate,
                              including ones that shouldn't need to pull
                              in an async runtime just for a type
                              definition.
  src/lib.rs                  RawAuction / AuctionPageResponse: the wire
                              format of Hypixel's API, deserialized with
                              serde (unused fields like item_lore are
                              simply not declared, so serde skips them
                              for free). AuctionSnapshot: our own merged
                              representation of a full poll cycle.
                              Config: typed env-var loading with a real
                              error type (ConfigError), no panics.

crates/ingestion/
  Cargo.toml                  tokio (async runtime), reqwest w/ rustls
                              (avoids linking against system OpenSSL —
                              see "toolchain notes" below for why this
                              mattered), tracing (structured logs with
                              per-stage latency fields), wiremock as a
                              dev-dependency for offline HTTP testing.
  src/lib.rs                   HypixelClient — the entire ingestion
                              service:
                                - fetch_page(): one page, typed response.
                                - wait_for_new_tick(): cheap repeated
                                  polling of page 0 only, per the
                                  sniper-mode design — this is the piece
                                  that determines how fast we notice
                                  Hypixel's cache refreshed.
                                - fetch_full_snapshot(): once a new tick
                                  is seen, fetches all remaining pages
                                  concurrently (tokio::join_all) and
                                  merges them.
                                - run(): the loop that ties the above
                                  together and logs detect_latency_ms /
                                  snapshot_fetch_latency_ms per cycle —
                                  this instrumentation exists from day
                                  one so Phase 3 benchmarking has real
                                  data to work from instead of starting
                                  from zero.
                              This crate does not parse items, price
                              anything, or decide what's a flip. It only
                              produces AuctionSnapshot values and hands
                              them to a channel.
  src/main.rs                   Standalone runnable binary for testing
                              this stage in isolation. The channel
                              receiver currently just prints snapshot
                              summaries — Phase 1.3 (diff detection)
                              will replace that block with real
                              downstream processing.
  tests/tick_detection.rs        Integration test against a local
                              wiremock mock server. Proves the
                              tick-detection -> concurrent-fetch ->
                              merge logic is correct without needing
                              live Hypixel access, which most CI/sandbox
                              environments (including the one this was
                              built in) can't reach.
```

## Setup

```bash
cp .env.example .env
# edit .env and set HYPIXEL_API_KEY (get one from https://developer.hypixel.net)

cargo build --workspace
cargo test --workspace
cargo run --release --bin ingestion-service
```

Expect log lines like:

```
INFO ingestion: assembled new auction snapshot tick=... auction_count=... detect_latency_ms=... snapshot_fetch_latency_ms=...
received snapshot: tick=... auctions=...
```

## Toolchain notes

This was built and tested against **rustc 1.75** (the version available
via `apt install rustc cargo` in the build environment — no internet
access to `rustup`/`static.rust-lang.org` was available there). A few
transitive dependencies (`url` → `idna` → the ICU4X crate family) have
since moved to edition2024, which 1.75 can't parse. `Cargo.lock` pins
`url = 2.4.1` / `idna = 0.4.0`, which resolve to the pre-ICU4X
`unicode-bidi`/`unicode-normalization`-based implementation and compile
fine on 1.75+.

**If you're on a modern toolchain (1.80+) via `rustup`, this pin is not
required** and you can safely `cargo update -p url -p idna` to pick up
current versions — just re-run `cargo test --workspace` afterward to
confirm nothing changed behaviorally (it shouldn't; this is a
transitive dependency of `reqwest`'s URL parsing, not something this
codebase touches directly).

## Next step

Phase 1.3: diff detection. Consumes `AuctionSnapshot` from the channel
`ingestion::HypixelClient::run` produces, maintains an in-memory
seen-UUID set (short TTL), and emits only new/changed auctions
downstream — this is what keeps the parser/pricing/engine crates from
redoing work on the ~90% of auctions that are unchanged between polls.
