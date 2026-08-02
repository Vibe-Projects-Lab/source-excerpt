// The venue boundary.
//
// An adapter is a source plus a normalizer, and one adapter serves one API
// domain: a single (exchange, market) pair. An exchange's spot and futures
// planes get separate adapters because they are separate systems — different
// endpoints, different symbol spellings, different limits and different ways
// of failing.
//
// The rule this file exists to enforce: no exchange-specific code lives
// anywhere else. Everything the platform needs to know about an exchange is
// either in the interface below or in the configuration object it carries.
//
// The shapes carry a stream name generically rather than naming the streams we
// happen to use, so a new kind of market data is a new value here, not a change
// to the machinery.
import type { HotBar, StreamName, TopicKey } from '../core/topics.js';
import type { RestDispatcher, RestPriority } from '../rest/dispatcher.js';
import type { RestRole } from '../rest/budget-split.js';
import type { MetricsHub } from '../core/market-metrics.js';
import type { ConnectionRegistry } from '../core/connection-registry.js';
import type { VenueSymbol } from '../core/venue-symbol.js';

/** One normalized candle event. Venues disagree about how they mark a bar as
 *  closed; the dialect layer settles that before anything reaches here. */
export interface NormalizedKline {
  topic: TopicKey;
  bar: HotBar;
  /** true = the bar just closed (exactly-once persist + BAR_CLOSE emit). */
  closed: boolean;
}

/** Venue domain configuration — structurally venue-neutral. It began life named
 *  after the first venue we integrated, and was generalised when the second one
 *  proved which fields were really venue-specific. */
export interface VenueDomainConfig {
  apiDomain: string; // process identity, e.g. "bybit-spot" / "bybit-linear"
  exchange: string; // registry exchange id, e.g. "bybit"
  marketType: 'spot' | 'perpetual';
  wsBase: string;
  restBase: string;
  restKlinesPath: string;
  // --- REST budget. The unit is host(restBase) + outbound IP. ---
  /** The VENUE-enforced weight budget per minute for `host(restBase)` per
   *  outbound IP — the number the venue itself counts, and therefore a number
   *  that must NEVER be handed whole to any single dispatcher. Domains sharing
   *  one REST host (a venue's spot and futures planes usually do) MUST declare
   *  the same value; the boot assertion fails otherwise. The per-dispatcher
   *  shares live in `restRoleBudgetPerMin`. */
  weightBudgetPerMin: number;
  /** Per-ROLE dispatcher budgets carved out of the host budget. `ingest` is
   *  this domain's own ingest process; `jobs` is the shared background worker,
   *  which builds one dispatcher per domain. The invariant, asserted at every
   *  boot: the sum over all roles and all domains on one host must not exceed
   *  weightBudgetPerMin.
   *
   *  The split is deliberately STATIC and role-weighted, with ingest given the
   *  larger share wherever real demand justifies it. A shared dynamic window
   *  was reviewed and rejected: it needs an await inside the dispatcher's drain
   *  loop, which breaks the synchronous window invariants; a partial Redis
   *  failure produces the very overshoot it exists to prevent; and sharing one
   *  window destroys the only cross-process priority isolation there is. */
  restRoleBudgetPerMin: Record<RestRole, number>;
  /** The floor this domain's live path is known to need, in weight per minute.
   *  Boot
   *  asserts that the ingest share still covers it AFTER the dispatcher's own
   *  safety factor, so a future re-split cannot silently starve the hot path.
   *  Only meaningful where demand is a real fraction of the budget — for
   *  instance a futures domain sweeping open interest for every instrument
   *  every minute, which is a four-figure weight cost on its own. */
  restIngestFloorPerMin?: number;
  exchangeInfoPath: string;
  exchangeInfoWeight: number;
  /** Max klines per REST request (the recent-history endpoint). */
  klinesMaxLimit: number;
  /** Venues whose DEEP-history endpoint has a SMALLER page cap than the recent
   *  one — some exchanges page deep history in smaller chunks. Gap repair
   *  sizes a window to `maxLimit` once its start is older than `horizonMs`, so
   *  the requested window never exceeds the deep page and no bars are stranded.
   *  Absent = this exchange's deep and recent caps are the same. */
  deepHistory?: { horizonMs: number; maxLimit: number };
  /** [maxLimit, weight] tiers, ascending — weight of a klines call by limit. */
  klinesWeightTiers: readonly (readonly [number, number])[];
  // --- WS pacing (used by the venue connection pool) ---
  /** Venue cap on inbound control messages per second per connection. */
  wsSubscribeMsgPerSec: number;
  /** Venue cap on streams per connection. */
  maxStreamsPerConn: number;
  /** Venue cap on stream args per ONE subscribe frame (Bybit v5: 10).
   *  Absent = the pool's own batch default. */
  maxArgsPerSubscribe?: number;
  /** Cap on concurrent in-flight REST requests (bounds minute-window bursts
   *  for venues with short rolling limits, e.g. Bybit 600 req/5s). */
  restMaxConcurrent?: number;
  /** Optional proxy for the WebSocket connection, for deployments that cannot
   *  REST stays direct (not fenced on the dev network). */
  wsProxyUrl?: string;
  /** The venue's spelling of the BTC/USDT symbol — the corr-vs-BTC anchor
   *  Absent = the common spelling 'BTCUSDT'; some exchanges spell
   *  it 'BTC-USDT' and would otherwise silently never resolve the anchor. */
  btcSymbol?: string;
}

/** Normalized all-market ticker item (EPHEMERAL full-state batch element). */
export interface NormalizedTicker {
  exchangeSymbol: string;
  lastPrice: number;
  priceChangePct24h: number;
  /** Quote-asset 24h volume (coin list shows quote/USD volume by default). */
  volume24h: number;
  /** Base-asset 24h volume — the "volume in coin" display unit. NaN when the
   *  venue's frame does not carry it; the emitter guards on presence. */
  baseVolume24h: number;
  ts: number;
}

/** Normalized REST kline bar (venue positional shapes stay in dialects). */
export interface RestKlineBar {
  ts: number;
  o: number;
  h: number;
  l: number;
  c: number;
  v: number;
}

/** Market metrics carried over the WebSocket. Venues split into two families
 *  here: some put funding and open interest on the ticker stream, others never
 *  emit them at all and have to be polled over REST. An adapter declares which
 *  family it belongs to through `capabilities.wsMetrics`. */
export interface NormalizedVenueMetric {
  exchangeSymbol: string;
  fundingRate?: number;
  nextFundingMs?: number;
  markPrice?: number;
  /** The exchange's own published index price. */
  indexPrice?: number;
  oiContracts?: number;
  oiUsd?: number;
  /** Observation time of the open-interest fields, set by the adapter from the
   *  moment the venue actually reported them — the frame that carried the key,
   *  or the channel payload's own timestamp — and NOT the batch flush time.
   *  The metrics journal buckets by this, which is what makes a frozen upstream
   *  value harmless: it can only ever re-address its own old bucket, never
   *  forward-fill the present. */
  oiTs?: number;
  /** Observation time of the funding fields (same contract). */
  fundingTs?: number;
  ts: number;
}

/** What a connection pool reports back to the process that owns it. */
export interface PoolCallbacks {
  onKline(evt: NormalizedKline): void;
  onTickerArr(items: NormalizedTicker[]): void;
  /** Optional — WS-metrics venues only (capabilities.wsMetrics). */
  onMetrics?(items: NormalizedVenueMetric[]): void;
  onMalformed(): void;
  /** SUBSCRIBE confirmed by the venue (SubscriptionManager → 'active'). */
  onSubscribed(streams: string[]): void;
  /** Streams whose connection healed or moved. Every such event triggers a
   *  history heal, because a socket that went away may have dropped bars.
   *  'planned-migration' is the case where the venue ANNOUNCED the disconnect
   *  in advance and the pool handed the streams over before the drop — the heal
   *  then covers only the handover window. */
  onStreamsHealed(
    streams: string[],
    reason: 'reconnect' | 'reassigned' | 'planned-migration',
  ): void;
  onReconnect(connId: string, attempt: number): void;
}

export interface RestKlinesReq {
  exchangeSymbol: string;
  /** Venue-agnostic TF token ('1m'…); default '1m'. Dialects map to venue
   *  interval names. */
  tf?: string;
  startMs?: number;
  endMs?: number;
  limit: number;
  priority?: RestPriority;
  /** Drop the still-forming bar (ms now) — venue pages include it last. */
  dropFormingAt?: number;
}

export interface RestKlinesPage {
  bars: RestKlineBar[];
  /** RAW row count BEFORE parsing/filtering — pagination termination signal
   *  (`rawCount < limit` = venue history exhausted). */
  rawCount: number;
}

export interface ParsedStreamName {
  kind: 'kline' | 'ticker' | 'other';
  exchangeSymbolLower?: string;
}

/** Metrics REST poll legs (venues without WS-carried metrics). */
export interface VenueMetricsPollOpts {
  dispatcher: RestDispatcher;
  hub: MetricsHub;
  marketType: 'spot' | 'perpetual';
  symbols: () => ReadonlySet<string>;
  fundingPollMs: number;
  oiPollMs: number;
  dealsPollMs: number;
  log: (msg: string) => void;
  /** Called after each SUCCESSFUL poll cycle. The loops are self-throttling —
   *  each awaits its own run before scheduling the next — so under a tight REST
   *  budget they silently stretch: a user-facing metric simply gets older while
   *  every dashboard reads healthy, because weight used goes DOWN and queue
   *  depth stays flat. This callback is the seam that makes that visible. */
  onCycle?: (loopName: string) => void;
}

/**
 * The venue seam: EVERYTHING venue-specific the core needs, bundled per API
 * domain. Three properties are load-bearing — all venue code sits behind one
 * boundary, capabilities are advertised as data rather than discovered by
 * probing, and the venue's own limits are declared by the adapter that knows
 * them.
 */
export interface ExchangeAdapter {
  readonly cfg: VenueDomainConfig;
  /** Capabilities as DATA, not as methods to probe. The core branches on
 *  these flags; it never asks "is this venue X". */
  readonly capabilities: { deals24h: boolean; wsMetrics: boolean; kline1s: boolean };
  fetchSymbols(dispatcher: RestDispatcher): Promise<VenueSymbol[]>;
  /** ONE budgeted REST-klines path for warmup/gap-fill/bootstrap/verify —
   *  wire params, weights and row parsing stay inside the dialect. */
  fetchKlines(dispatcher: RestDispatcher, req: RestKlinesReq): Promise<RestKlinesPage>;
  klineStreamName(exchangeSymbol: string, tf: string): string | null;
  /** Venue streams co-subscribed PER INSTRUMENT beside its kline (e.g. Bybit
   *  per-symbol tickers). Empty for venues with an all-market ticker. */
  perInstrumentStreams(exchangeSymbol: string): string[];
  /** Venue streams subscribed ONCE per domain (e.g. Binance !miniTicker@arr). */
  globalStreams(): string[];
  parseStreamName(name: string): ParsedStreamName;
  createPool(
    streamsPerConn: number,
    registry: ConnectionRegistry,
    cb: PoolCallbacks,
  ): VenueStreamSource;
  /** REST metric poll legs; absent when metrics ride the WS (wsMetrics). */
  startMetricsPolls?(opts: VenueMetricsPollOpts): () => void;
  /** Bulk monthly kline archive, when the venue publishes one. Returns null
   *  when that month does not exist (the normal way to find a listing edge).
   *
   *  Absent on most exchanges, and that is a finding rather than a gap: one
   *  stopped publishing its candle archive years ago, and rebuilding hours from
   *  the tick archive it does publish costs orders of magnitude more transfer
   *  than simply making the ordinary history requests; another returns 404 on
   *  every archive path it documents.
   *
   *  Does NOT go through the RestDispatcher: a different host, so it spends no
   *  part of the venue weight budget. */
  fetchArchiveMonth?(req: {
    exchangeSymbol: string;
    tf: string;
    year: number;
    month: number;
  }): Promise<RestKlineBar[] | null>;
}

export interface StreamRequest {
  exchangeSymbol: string;
  stream: StreamName;
  tf?: string;
}

/**
 * A venue stream source for one API domain: a pool of WS connections that
 * reconciles toward the desired stream set via paced control frames.
 */
export interface VenueStreamSource {
  readonly apiDomain: string;
  /** Desired-state delta; the pool assigns/paces/acks internally. */
  applySubscriptions(add: string[], remove: string[]): void;
  streamCount(): number;
  killConnection(connId: string): boolean;
  /** Test hook: simulate a connection FAILURE, which takes the reassignment
   *  path, as opposed to a kill, which takes the reconnect path. The two are
   *  different code paths and both need exercising. Optional per exchange. */
  failConnection?(connId: string): boolean;
  connectionIds(): string[];
  shutdown(): void;
}
