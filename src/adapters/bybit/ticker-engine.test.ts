// R1-pinned semantics: linear snapshot→delta merge (present fields only,
// nextFundingTime only in snapshots), no-emit-before-snapshot, fraction→pct,
// the volume/turnover SWAP, funding/OI extraction, snapshot reset on resub.
import { describe, expect, it, vi } from 'vitest';
import type { NormalizedTicker, NormalizedVenueMetric } from '../types.js';
import { BybitTickerEngine } from './ticker-engine.js';

const SNAPSHOT = {
  topic: 'tickers.BTCUSDT',
  type: 'snapshot',
  ts: 1000,
  data: {
    symbol: 'BTCUSDT',
    lastPrice: '63929.8',
    price24hPcnt: '0.0157', // FRACTION = 1.57%
    volume24h: '5000.5', // BASE
    turnover24h: '319000000.1', // QUOTE
    fundingRate: '0.00003777',
    nextFundingTime: '1784390400000',
    markPrice: '63930.1',
    indexPrice: '63928.4',
    openInterest: '54810.581',
    openInterestValue: '3504321000.5',
  },
};

function collect(emitMetrics: boolean) {
  const tickers: NormalizedTicker[][] = [];
  const metrics: NormalizedVenueMetric[][] = [];
  const engine = new BybitTickerEngine(emitMetrics, {
    onTickerArr: (t) => tickers.push(t),
    onMetrics: (m) => metrics.push(m),
  });
  return { engine, tickers, metrics };
}

describe('BybitTickerEngine — observation times', () => {
  it('oiTs advances ONLY with deltas that carry an OI key — price ticks do not touch it', () => {
    const { engine, metrics } = collect(true);
    engine.ingest(SNAPSHOT);
    engine.flush();
    expect(metrics[0]![0]!.oiTs).toBe(1000);
    expect(metrics[0]![0]!.fundingTs).toBe(1000);

    // Pure price delta: st.ts moves, oiTs must NOT (the audit's Bybit case —
    // a frozen OI under a live ticker looked freshly observed).
    engine.ingest({
      topic: 'tickers.BTCUSDT',
      type: 'delta',
      ts: 5000,
      data: { lastPrice: '64000.0' },
    });
    engine.flush();
    const m = metrics[1]![0]!;
    expect(m.oiTs).toBe(1000); // frozen observation stays in the past
    expect(m.fundingTs).toBe(1000);

    // A delta that actually carries OI advances the observation.
    engine.ingest({
      topic: 'tickers.BTCUSDT',
      type: 'delta',
      ts: 9000,
      data: { openInterest: '54811.0' },
    });
    engine.flush();
    expect(metrics[2]![0]!.oiTs).toBe(9000);
    expect(metrics[2]![0]!.fundingTs).toBe(1000);
    engine.stop();
  });
});

describe('BybitTickerEngine', () => {
  it('delta BEFORE snapshot never emits (partial state is not a price)', () => {
    const { engine, tickers } = collect(false);
    engine.ingest({ topic: 'tickers.BTCUSDT', type: 'delta', ts: 1, data: { lastPrice: '1' } });
    engine.flush();
    expect(tickers).toHaveLength(0);
    engine.stop();
  });

  it('snapshot emits with the volume/turnover SWAP and fraction→pct', () => {
    const { engine, tickers } = collect(false);
    engine.ingest(SNAPSHOT);
    engine.flush();
    expect(tickers).toHaveLength(1);
    const t = tickers[0]![0]!;
    expect(t.exchangeSymbol).toBe('BTCUSDT');
    expect(t.lastPrice).toBeCloseTo(63929.8);
    expect(t.priceChangePct24h).toBeCloseTo(1.57); // 0.0157 → %
    expect(t.volume24h).toBeCloseTo(319000000.1); // OUR volume24h = their turnover (QUOTE)
    expect(t.baseVolume24h).toBeCloseTo(5000.5); // base
    engine.stop();
  });

  it('delta merges ONLY present fields; absent fields survive (nextFundingTime)', () => {
    const { engine, tickers, metrics } = collect(true);
    engine.ingest(SNAPSHOT);
    engine.flush();
    // Partial delta: only lastPrice + OI change (32 distinct field sets).
    engine.ingest({
      topic: 'tickers.BTCUSDT',
      type: 'delta',
      ts: 2000,
      data: { symbol: 'BTCUSDT', lastPrice: '64000.0', openInterest: '54900.1' },
    });
    engine.flush();
    const t = tickers[1]![0]!;
    expect(t.lastPrice).toBeCloseTo(64000.0);
    expect(t.volume24h).toBeCloseTo(319000000.1); // untouched by the delta
    const m = metrics[1]![0]!;
    expect(m.oiContracts).toBeCloseTo(54900.1);
    expect(m.nextFundingMs).toBe(1784390400000); // snapshot-only field SURVIVES
    expect(m.fundingRate).toBeCloseTo(0.00003777);
    expect(m.markPrice).toBeCloseTo(63930.1);
    expect(m.indexPrice).toBeCloseTo(63928.4); // E+ 2026-07-24: venue index fact
    engine.stop();
  });

  it('flush emits only CHANGED symbols; a new snapshot resets delta drift', () => {
    const { engine, tickers } = collect(false);
    engine.ingest(SNAPSHOT);
    engine.flush();
    engine.flush(); // nothing changed — no emit
    expect(tickers).toHaveLength(1);
    // Reconnect: venue re-sends a snapshot — full replace (drift wiped).
    engine.ingest({
      ...SNAPSHOT,
      ts: 3000,
      data: { ...SNAPSHOT.data, lastPrice: '65000.1' },
    });
    engine.flush();
    expect(tickers[1]![0]!.lastPrice).toBeCloseTo(65000.1);
    engine.stop();
  });

  it('spot engine (emitMetrics=false) never calls onMetrics', () => {
    const onMetrics = vi.fn();
    const engine = new BybitTickerEngine(false, {
      onTickerArr: () => {},
      onMetrics,
    });
    engine.ingest(SNAPSHOT);
    engine.flush();
    expect(onMetrics).not.toHaveBeenCalled();
    engine.stop();
  });

  it('empty-string numerics reject the ticker item, not the batch', () => {
    const { engine, tickers } = collect(false);
    engine.ingest({
      topic: 'tickers.XUSDT',
      type: 'snapshot',
      ts: 1,
      data: { symbol: 'XUSDT', lastPrice: '', price24hPcnt: '0.01', turnover24h: '5' },
    });
    engine.ingest(SNAPSHOT);
    engine.flush();
    expect(tickers[0]!.map((t) => t.exchangeSymbol)).toEqual(['BTCUSDT']);
    engine.stop();
  });
});
