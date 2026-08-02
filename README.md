# VIBE Screener — source excerpt

### What this is, in three sentences

**[VIBE Screener](https://vibescreener.app) is a live product** — a real-time
analytics service for crypto derivatives, watching every instrument on three
exchanges at once and putting that in front of a trader as charts, a filterable
instrument list and cross-exchange signals. **This repository is not the
product.** It is a curated excerpt of its source code — about 3 500 lines of
implementation and 1 550 lines of tests — published so that the code can be read
and evaluated as part of an application to the President Tech Award 2026, whose
rules ask applicants to make part of their code available for review.

The running service is at **[vibescreener.app](https://vibescreener.app)**. What
the whole system looks like, and where these files sit inside it, is in
[ARCHITECTURE.md](ARCHITECTURE.md). Terms of use are in [LICENSE](LICENSE).

## This is not a runnable application

Please read this before cloning, so that nothing here is mistaken for a broken
build.

**You cannot start this.** There is no entry point, no server, no `main` and no
`start` script — deliberately. Running the real service takes twelve
long-running processes, a PostgreSQL instance with the TimescaleDB extension, a
Redis instance, a database schema built by twenty migrations, and live
credentials at several exchanges. None of that is published, so nothing here is
wired to a runtime. These are **libraries and their tests**, lifted out of the
running system.

**What you can do is compile it and run its tests**, which is how it should be
evaluated:

```
npm install
npm run typecheck   # tsc --noEmit, strict
npm test            # 77 tests, 9 files
```

No database, no Redis, no network access and no credentials are required. The
tests are the honest demonstration: they exercise the same code the production
service runs, against recorded venue frames and fake clients.

Start with [ARCHITECTURE.md](ARCHITECTURE.md) if you want to know what the whole
system looks like and where these files sit inside it.

---

## What the whole system does

Market data from three exchanges — six API domains, since a venue's spot and
perpetual-futures planes are separate systems — is collected, normalised behind
one adapter interface, and split into two independent paths: a live path that
reaches the browser over WebSocket, and a journal that lands in TimescaleDB.

```mermaid
flowchart LR
    V["Exchanges<br/>WebSocket + REST"]
    I["Ingest<br/>one process per API domain"]
    R[("Redis<br/>pub/sub · hot cache<br/>· bounded streams")]
    P["Persist<br/>single writer"]
    D[("PostgreSQL<br/>+ TimescaleDB")]
    F["Fan-out<br/>uWebSockets"]
    G["API gateway<br/>Fastify + tRPC"]
    B["Browser<br/>one WebSocket per browser"]

    V --> I
    I --> R
    R --> P
    P --> D
    R --> F
    F -- protobuf --> B
    G -- JSON --> B
    D --> G
    D --> I
```

The excerpt covers the shaded middle of that picture: how we talk to an
exchange, how we stay inside its rate limits, how candles are derived and
repaired, and how history is read back.

---

## Every file, in one line each

Thirty-three files. Nine of them are tests, and they sit next to what they test.

**`src/adapters/` — the venue boundary**

| File | |
|---|---|
| `types.ts` | the entire contract between the platform and an exchange — read this first |
| `bybit/index.ts` | the venue's two domains as configuration: endpoints, limits, budget shares |
| `bybit/adapter.ts` | the venue's implementation of the contract, assembled from the parts below |
| `bybit/dialect.ts` | the venue's own JSON, normalised — where "is this bar closed" is answered |
| `bybit/connection.ts` | one upstream socket: its stream set, its control frames, its own backoff |
| `bybit/pool.ts` | many sockets as one: assignment, pacing, and reconciling toward a desired state |
| `bybit/ticker-engine.ts` | the venue sends partial ticker deltas; this reconstructs full state from them |
| `bybit/exchange-info.ts` | the venue's instrument catalogue, paginated, into a venue-neutral shape |

**`src/rest/` — outbound request budget**

| File | |
|---|---|
| `dispatcher.ts` | a weight-budgeted, priority-aware request queue with venue-driven pauses |
| `budget-split.ts` | who may spend how much of a venue's limit, asserted at every boot |
| `pause-share.ts` | one process gets rate-limited; the others need to know within milliseconds |

**`src/workers/` and `src/db/` — history**

| File | |
|---|---|
| `workers/gap-fill.ts` | detect a hole in stored history and repair it, idempotently |
| `db/history-reader.ts` | read closed bars back, paging strictly older with a cursor |
| `db/klines.ts` | the three anchor series and the row shape they share |

**`src/core/` — the shared vocabulary**

| File | |
|---|---|
| `topics.ts` | the naming scheme every stream, cache key and channel is derived from |
| `timeframes.ts` | which timeframe derives from which anchor, and where each bucket starts |
| `derive.ts` | the derivation itself: one algorithm, no per-timeframe branches |
| `persist.ts` | the contract of the journal — streams, watermarks, bounded writes |
| `market-metrics.ts` | per-domain state for open interest, funding, mark and index prices |
| `instruments.ts` | one formalised key inside an open metadata column, so three consumers agree |
| `venue-symbol.ts` | what a listing looks like once it stops being venue-shaped |
| `connection-registry.ts` | which upstream sockets exist and what each is carrying |
| `proxy-agent.ts` | reaching a venue that is unreachable from the deployment region |
| `env.ts` | the narrow configuration surface this excerpt reads (see below) |

---

## Why this slice

The selection is not "the easy parts" or "the parts with no secrets in them".
Each subsystem was chosen because it answers a question a reviewer would
reasonably ask about a system like this.

### 1. The venue seam — `src/adapters/`

*"You say three exchanges. What happens when you add a fourth?"*

[`adapters/types.ts`](src/adapters/types.ts) is the entire contract between the
platform and an exchange: eleven members, with capabilities advertised **as
data** rather than discovered by asking "is this exchange X". The rule the file
enforces is that no exchange-specific code exists anywhere else, so adding an
exchange means writing an adapter and registering it — the platform does not
change.

One honest qualification, since the code says so too: the connection pool for
this exchange was adapted from the first one rather than factored into something
shared. The scheduling and idle logic is genuinely the same and belongs in one
place. That deduplication is a known, deliberately deferred piece of work — the
boundary that matters held, and moving working connection machinery around
mid-flight is how you lose data.

[`adapters/bybit/`](src/adapters/bybit) is one complete implementation of that
contract — dialect, connection, pool, instrument catalogue and a ticker delta
engine, with its tests. It is a genuinely awkward venue to integrate: the
per-symbol ticker stream sends partial deltas, funding and open interest ride
that same stream rather than a dedicated one, and a minute boundary can pack a
closed bar and its successor into a single frame on one market but two frames on
another. Those are exactly the details that make a "just parse the JSON" adapter
wrong in production, so they are worth reading.

### 2. Staying inside a venue's rate limit — `src/rest/`

*"How do you avoid getting banned?"*

This is the piece we would point at first. The interesting part is not the token
bucket in [`dispatcher.ts`](src/rest/dispatcher.ts); it is the observation in
[`budget-split.ts`](src/rest/budget-split.ts) that the obvious invariant was
false in a way no local check could see. A venue counts requests per host and IP.
We run several processes against one venue. Each process independently obeyed
"never exceed the venue budget", each was individually correct, and the fleet was
still at 1.8× the limit. The fix is a declared static split asserted at every
boot, so configuration drift crashes a process rather than earning a ban later.

The comments also record the alternative we rejected — a shared dynamic window
over Redis — and why: it requires an `await` inside the drain loop, a partial
Redis failure produces the exact overshoot it exists to prevent, and it destroys
the only cross-process priority isolation the system has. What *is* shared is
only the pause, because a 429 is the venue's own verdict rather than our model of
it ([`pause-share.ts`](src/rest/pause-share.ts)).

### 3. Timeframe derivation — `src/core/derive.ts`

*"Do your five-minute candles agree with your database?"*

One algorithm over an anchor mapping, with zero per-timeframe branches, folding
bars in an order-independent way that is deliberately identical to what the
database would compute. The test asserts that equivalence directly, because it is
the property that lets a chart mix live derived bars with stored history and see
a single continuous series.

### 4. Self-healing history — `src/workers/gap-fill.ts`

*"A socket dropped for ninety seconds. Now what?"*

Every reconnect is assumed to have cost us bars until proven otherwise, so the
heal runs generously and is a no-op by construction when there was nothing to
fix. The subtle parts are in the tests: a gap wider than one page must fill
completely regardless of which end of the range the venue paginates from, and a
venue whose deep-history endpoint has a *smaller* page cap than its recent one
will otherwise strand bars in a hole that nothing ever revisits.

### 5. Reading history back — `src/db/history-reader.ts`

*"How does infinite scroll-back not duplicate or skip a bar?"*

Closed bars only, never the in-progress bucket; an exclusive upper-bound cursor
for paging strictly older data. It lives on the shared side because two packages
that may not import each other both need it, and an import-boundary linter
enforces that.

---

## What was changed for publication

Nothing was rewritten to look better. Four changes were made so the excerpt could
stand alone, and they are listed here rather than left for a reader to discover:

1. **Comments were translated and de-referenced.** The originals cite internal
   documents, dated design rulings and tracker items. The reasoning is intact —
   including the reasoning behind decisions we later reversed — but the pointers
   into private documents are gone.
2. **The environment module is narrowed.** The real service validates one large
   configuration schema at boot and fails closed. That schema also carries the
   settings of the abuse-defence layer, which is deliberately out of scope here,
   so [`core/env.ts`](src/core/env.ts) declares only the venue endpoint overrides
   this code actually reads.
3. **One test lost coverage, not correctness.** The budget-split boot assertion
   normally runs over every venue the fleet deploys. With one venue published, it
   runs over that venue's two domains. The mechanism is identical; the coverage
   is narrower, and the test says so.
4. **A shared proxy-agent helper was moved.** In the full repository it happens to
   live inside the first venue's directory and is imported by the others — an
   untidy seam we have recorded. Here it sits in `core/` where it belongs.

## What is deliberately absent

Authentication, sessions, rate limiting, the circuit breaker, the abuse-defence
layer and their configuration are **not** published, and not because they are
unfinished. The service is live with real users; the tuning values of those
systems are the one category of code where publication would hand an attacker a
free advantage. The data plane has no such property, so it is published in full.

Also absent, simply because they are outside the story this excerpt tells: the
WebSocket fan-out edge, the tier-policy engine, the HTTP gateway, the client, the
database schema and migrations, and the other two exchange adapters.
