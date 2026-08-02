// Bybit ticker aggregation engine: the venue has NO all-market ticker topic
// (F4 matrix, re-verified) — every instrument co-subscribes `tickers.{sym}`
// and THIS engine aggregates our platform ticker_arr batches. R1-pinned
// semantics baked in:
//  - spot frames: type 'snapshot' only (full record each time);
//  - linear: ONE snapshot on (re)subscribe, then DELTAS carrying ONLY the
//    changed fields + symbol (32 distinct partial field sets observed) —
//    merge present fields, NEVER clear absent ones, NEVER emit before the
//    first snapshot (a delta alone is not a price);
//  - nextFundingTime arrives ONLY in snapshots — it must survive delta merges;
//  - price24hPcnt is a FRACTION string ('0.0157' = 1.57%);
//  - volume24h = BASE units, turnover24h = QUOTE units (the swap: OUR
//    volume24h field is quote volume);
//  - funding/OI/markPrice ride these same frames on linear (wsMetrics) —
//    no REST sweeps for Bybit.
import type { NormalizedTicker, NormalizedVenueMetric } from '../types.js';
import { bybitNum as num } from './dialect.js';

interface TickerRecord {
  lastPrice?: string;
  price24hPcnt?: string;
  volume24h?: string; // BASE
  turnover24h?: string; // QUOTE
  fundingRate?: string;
  nextFundingTime?: string | number;
  markPrice?: string;
  indexPrice?: string; // venue's official index — a later pass
  openInterest?: string;
  openInterestValue?: string;
}

interface SymbolState {
  rec: TickerRecord;
  hasSnapshot: boolean;
  ts: number;
  /** observation times of the OI / funding fields — stamped ONLY
   *  when a frame actually carried those keys. `ts` moves on every delta
   *  (price ticks included), so it cannot answer "when was OI last seen";
   *  the metrics journal buckets by these. */
  oiTs?: number;
  fundingTs?: number;
}

const MERGE_KEYS: readonly (keyof TickerRecord)[] = [
  'lastPrice',
  'price24hPcnt',
  'volume24h',
  'turnover24h',
  'fundingRate',
  'nextFundingTime',
  'markPrice',
  'indexPrice',
  'openInterest',
  'openInterestValue',
];

export interface TickerEngineCallbacks {
  onTickerArr(items: NormalizedTicker[]): void;
  onMetrics?(items: NormalizedVenueMetric[]): void;
}

export class BybitTickerEngine {
  private readonly bySymbol = new Map<string, SymbolState>();
  private readonly changed = new Set<string>();
  private timer: ReturnType<typeof setInterval> | null = null;

  constructor(
    private readonly emitMetrics: boolean, // linear only
    private readonly cb: TickerEngineCallbacks,
    flushMs = 1000, // matches Binance's ~1s array cadence
  ) {
    this.timer = setInterval(() => this.flush(), flushMs);
    this.timer.unref?.();
  }

  /** One `tickers.{sym}` frame in. Returns false when not a ticker frame. */
  ingest(frame: { topic?: string; type?: string; ts?: number; data?: unknown }): boolean {
    if (typeof frame?.topic !== 'string' || !frame.topic.startsWith('tickers.')) {
      return false;
    }
    const symbol = frame.topic.slice('tickers.'.length);
    if (!symbol || typeof frame.data !== 'object' || frame.data === null) return true;
    const data = frame.data as TickerRecord;
    const ts = typeof frame.ts === 'number' ? frame.ts : Date.now();
    let st = this.bySymbol.get(symbol);
    if (frame.type === 'snapshot') {
      // Full replace — also the reconnect self-heal (venue re-sends a
      // snapshot on resubscribe, wiping any delta drift).
      st = { rec: { ...data }, hasSnapshot: true, ts };
      if (data.openInterest !== undefined || data.openInterestValue !== undefined) st.oiTs = ts;
      if (data.fundingRate !== undefined) st.fundingTs = ts;
      this.bySymbol.set(symbol, st);
      this.changed.add(symbol);
      return true;
    }
    // Delta before the first snapshot = unusable partial state — drop.
    if (!st?.hasSnapshot) return true;
    for (const k of MERGE_KEYS) {
      const v = data[k];
      if (v !== undefined) {
        (st.rec as Record<string, unknown>)[k] = v;
      }
    }
    // field observation times advance ONLY with their fields.
    if (data.openInterest !== undefined || data.openInterestValue !== undefined) st.oiTs = ts;
    if (data.fundingRate !== undefined) st.fundingTs = ts;
    st.ts = ts;
    this.changed.add(symbol);
    return true;
  }

  /** Emit changed symbols as platform batches (called on the flush timer). */
  flush(): void {
    if (this.changed.size === 0) return;
    const tickers: NormalizedTicker[] = [];
    const metrics: NormalizedVenueMetric[] = [];
    for (const symbol of this.changed) {
      const st = this.bySymbol.get(symbol);
      if (!st?.hasSnapshot) continue;
      const last = num(st.rec.lastPrice);
      const pctFraction = num(st.rec.price24hPcnt);
      const quoteVol = num(st.rec.turnover24h); // QUOTE → our volume24h
      const baseVol = num(st.rec.volume24h); // BASE → our baseVolume24h
      if (Number.isFinite(last) && last > 0 && Number.isFinite(pctFraction) && Number.isFinite(quoteVol)) {
        tickers.push({
          exchangeSymbol: symbol,
          lastPrice: last,
          priceChangePct24h: pctFraction * 100, // fraction → percent
          volume24h: quoteVol,
          baseVolume24h: baseVol, // NaN-safe: emitter presence-guards
          ts: st.ts,
        });
      }
      if (this.emitMetrics) {
        const fundingRate = num(st.rec.fundingRate);
        const nextFundingMs = num(st.rec.nextFundingTime);
        const markPrice = num(st.rec.markPrice);
        const indexPrice = num(st.rec.indexPrice);
        const oiContracts = num(st.rec.openInterest);
        const oiUsd = num(st.rec.openInterestValue);
        const m: NormalizedVenueMetric = { exchangeSymbol: symbol, ts: st.ts };
        if (Number.isFinite(fundingRate)) m.fundingRate = fundingRate;
        if (Number.isFinite(nextFundingMs) && nextFundingMs > 0) m.nextFundingMs = nextFundingMs;
        if (Number.isFinite(markPrice) && markPrice > 0) m.markPrice = markPrice;
        if (Number.isFinite(indexPrice) && indexPrice > 0) m.indexPrice = indexPrice;
        if (Number.isFinite(oiContracts)) m.oiContracts = oiContracts;
        if (Number.isFinite(oiUsd)) m.oiUsd = oiUsd;
        if ((m.oiContracts !== undefined || m.oiUsd !== undefined) && st.oiTs !== undefined) {
          m.oiTs = st.oiTs;
        }
        if (m.fundingRate !== undefined && st.fundingTs !== undefined) {
          m.fundingTs = st.fundingTs;
        }
        if (Object.keys(m).length > 2) metrics.push(m);
      }
    }
    this.changed.clear();
    if (tickers.length > 0) this.cb.onTickerArr(tickers);
    if (metrics.length > 0) this.cb.onMetrics?.(metrics);
  }

  /** Delisted instrument cleanup (mirror of the pool's stream teardown). */
  remove(symbol: string): void {
    this.bySymbol.delete(symbol);
    this.changed.delete(symbol);
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }
}
