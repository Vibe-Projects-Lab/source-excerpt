// gap-fill must heal a MULTI-PAGE gap completely
// regardless of the venue's pagination direction. Before the fix it forward-
// paginated with startMs+limit only: a venue that answers a start-only query
// with the NEWEST page (OKX/Bybit direction unconfirmed) let the cursor jump to
// ~now after page 1, stranding older bars as permanent klines_1m holes.
//
// A live multi-page heal against the venue is the real proof, but the venue's
// REST endpoints are not reachable from every network, so this drives
// the SAME gapFillOne walk against a fake adapter that can paginate either way
// and asserts: (a) a >1-page gap fills 100% in BOTH directions, (b) NO duplicate
// and NO one-bar hole at the window seam, (c) an empty/sparse window advances
// (never stalls). A newest-first venue is exactly the case the old code broke —
// so this test failing on the pre-fix code IS the regression guard.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { Redis } from 'ioredis';
import { gapFillInstruments, type GapFillDeps } from './gap-fill.js';
import type {
  ExchangeAdapter,
  RestKlineBar,
  RestKlinesPage,
  RestKlinesReq,
  VenueDomainConfig,
} from '../adapters/types.js';

const MIN = 60_000;
// Minute-aligned fixed "now" so the walk's live-edge math is deterministic.
const NOW = 1_700_000_040_000;
const KLINES_MAX = 300;

function bar(ts: number): RestKlineBar {
  return { ts, o: 1, h: 2, l: 0.5, c: 1.5, v: 10 };
}

/** A venue series of 1m bars over [firstTs, lastTs] (inclusive), optionally with
 *  a set of missing openTimes (a genuine venue gap). */
function makeSeries(firstTs: number, count: number, missing: Set<number> = new Set()): RestKlineBar[] {
  const out: RestKlineBar[] = [];
  for (let i = 0; i < count; i++) {
    const ts = firstTs + i * MIN;
    if (!missing.has(ts)) out.push(bar(ts));
  }
  return out;
}

/** Fake adapter whose fetchKlines honors startMs/endMs/limit/dropFormingAt and
 *  returns the OLDEST-`limit` or NEWEST-`limit` bars of the window (the parse
 *  step sorts ascending, mirroring the real dialects). */
function fakeAdapter(venue: RestKlineBar[], direction: 'oldest' | 'newest'): ExchangeAdapter {
  const cfg = { apiDomain: 'fake-spot', klinesMaxLimit: KLINES_MAX } as VenueDomainConfig;
  return {
    cfg,
    async fetchKlines(_d: unknown, req: RestKlinesReq): Promise<RestKlinesPage> {
      const inWin = venue
        .filter(
          (b) =>
            (req.startMs === undefined || b.ts >= req.startMs) &&
            (req.endMs === undefined || b.ts <= req.endMs) &&
            (req.dropFormingAt === undefined || b.ts < req.dropFormingAt),
        )
        .sort((a, b) => a.ts - b.ts);
      const page = direction === 'newest' ? inWin.slice(-req.limit) : inWin.slice(0, req.limit);
      return { bars: page, rawCount: page.length };
    },
  } as unknown as ExchangeAdapter;
}

function makeDeps(adapter: ExchangeAdapter, filled: RestKlineBar[]): GapFillDeps {
  const redis = {
    get: async () => null, // sinceMs is provided → watermark GET unused
    pipeline: () => {
      const ops: unknown[] = [];
      return {
        xadd: (..._a: unknown[]) => {
          ops.push(_a);
        },
        exec: async () => ops.map(() => [null, 'ok'] as const),
      };
    },
  } as unknown as Redis;
  return {
    redis,
    dispatcher: {} as never,
    adapter,
    streamMaxlen: 1000,
    onBars: (_id, bars) => {
      filled.push(...bars); // journaled bars (called only after a clean xadd)
    },
  };
}

describe('gap-fill — multi-page gap heals in both pagination directions', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
  });
  afterEach(() => vi.useRealTimers());

  // 500-bar gap, KLINES_MAX=300 → >1 page → the seam falls mid-data.
  const FIRST = NOW - 500 * MIN; // first venue bar (openTime)
  const LAST_CLOSED = NOW - MIN; // last closed bar (forming NOW-bar excluded)

  for (const direction of ['oldest', 'newest'] as const) {
    it(`fills every bar of a 500-bar gap (${direction}-first venue), no dup, no hole`, async () => {
      const venue = makeSeries(FIRST, 500); // [FIRST .. NOW-MIN], 500 bars
      const filled: RestKlineBar[] = [];
      const deps = makeDeps(fakeAdapter(venue, direction), filled);

      const res = await gapFillInstruments(deps, [
        { instrumentId: 'iid-1', exchangeSymbol: 'BTCUSDT', sinceMs: FIRST - MIN },
      ]);

      const gotTs = filled.map((b) => b.ts).sort((a, b) => a - b);
      const expectedTs = venue.filter((b) => b.ts <= LAST_CLOSED).map((b) => b.ts);
      expect(res.barsFilled).toBe(500);
      expect(res.instrumentsHealed).toBe(1);
      expect(gotTs).toEqual(expectedTs); // exact set → no hole, no gap at the seam
      expect(new Set(gotTs).size).toBe(gotTs.length); // no duplicate at the seam
      expect(gotTs).toHaveLength(500);
    });

    it(`advances past a genuinely-missing sub-range without stalling (${direction}-first)`, async () => {
      // Venue truly lacks 50 bars in the middle of page 1 (halt) — the walk must
      // fill the rest and never stop at the hole.
      const missing = new Set<number>();
      for (let i = 100; i < 150; i++) missing.add(FIRST + i * MIN);
      const venue = makeSeries(FIRST, 500, missing); // 450 real bars
      const filled: RestKlineBar[] = [];
      const deps = makeDeps(fakeAdapter(venue, direction), filled);

      const res = await gapFillInstruments(deps, [
        { instrumentId: 'iid-1', exchangeSymbol: 'BTCUSDT', sinceMs: FIRST - MIN },
      ]);

      const gotTs = filled.map((b) => b.ts).sort((a, b) => a - b);
      const expectedTs = venue.filter((b) => b.ts <= LAST_CLOSED).map((b) => b.ts);
      expect(res.barsFilled).toBe(450);
      expect(gotTs).toEqual(expectedTs);
      // the bars right after the missing range ARE present (the walk continued)
      expect(gotTs).toContain(FIRST + 150 * MIN);
      expect(gotTs).not.toContain(FIRST + 120 * MIN);
    });
  }
});

//: a BOUNDED target — {sinceMs, untilMs} from the close
// contiguity check — must fetch the hole and ONLY the hole. The trap the bound
// exists for: without untilMs a past-hole cursor walks since → now and
// re-fetches days per instrument (the 6M-bar / ~97%-duplicate mechanism).
describe('gap-fill — bounded hole targets', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
  });
  afterEach(() => vi.useRealTimers());

  it('fetches exactly the hole interior (exclusive bounds), not since → now', async () => {
    // Venue has a full day of bars; our hole is (H0, H0+41m): bars H0 and
    // H0+41m exist locally, the 40 between are missing (the OKX incident
    // shape).
    const first = NOW - 1440 * MIN;
    const venue = makeSeries(first, 1440);
    const H0 = NOW - 300 * MIN;
    const filled: RestKlineBar[] = [];
    const requests: { startMs?: number; endMs?: number }[] = [];
    const adapter = fakeAdapter(venue, 'oldest');
    const inner = adapter.fetchKlines.bind(adapter);
    adapter.fetchKlines = async (d, req) => {
      requests.push({ startMs: req.startMs, endMs: req.endMs });
      return inner(d, req);
    };
    const deps = makeDeps(adapter, filled);

    const res = await gapFillInstruments(deps, [
      {
        instrumentId: 'iid-1',
        exchangeSymbol: 'BTCUSDT',
        sinceMs: H0,
        untilMs: H0 + 41 * MIN,
      },
    ]);

    const gotTs = filled.map((b) => b.ts).sort((a, b) => a - b);
    expect(res.barsFilled).toBe(40); // the interior only
    expect(gotTs[0]).toBe(H0 + MIN);
    expect(gotTs[gotTs.length - 1]).toBe(H0 + 40 * MIN);
    // ONE gap-sized request, and it never reached past the hole's far edge.
    expect(requests).toHaveLength(1);
    expect(requests[0]?.endMs).toBeLessThanOrEqual(H0 + 40 * MIN);
  });

  it('a hole wider than one page walks in windows and still stops at untilMs', async () => {
    const first = NOW - 2000 * MIN;
    const venue = makeSeries(first, 2000);
    const H0 = NOW - 900 * MIN; // hole of 500 interior bars > KLINES_MAX=300
    const UNTIL = H0 + 501 * MIN;
    const filled: RestKlineBar[] = [];
    const deps = makeDeps(fakeAdapter(venue, 'newest'), filled);

    const res = await gapFillInstruments(deps, [
      { instrumentId: 'iid-1', exchangeSymbol: 'BTCUSDT', sinceMs: H0, untilMs: UNTIL },
    ]);

    const gotTs = filled.map((b) => b.ts).sort((a, b) => a - b);
    expect(res.barsFilled).toBe(500);
    expect(gotTs[0]).toBe(H0 + MIN);
    expect(gotTs[gotTs.length - 1]).toBe(UNTIL - MIN);
  });

  it('an unbounded target keeps the live-edge walk (reconnect semantics unchanged)', async () => {
    const first = NOW - 100 * MIN;
    const venue = makeSeries(first, 100);
    const filled: RestKlineBar[] = [];
    const deps = makeDeps(fakeAdapter(venue, 'oldest'), filled);

    const res = await gapFillInstruments(deps, [
      { instrumentId: 'iid-1', exchangeSymbol: 'BTCUSDT', sinceMs: first - MIN },
    ]);
    expect(res.barsFilled).toBe(100); // everything closed up to NOW-MIN
  });
});

// a venue whose DEEP-history endpoint caps the
// page SMALLER than its recent one (OKX: 100 vs 300) must have its gap-fill window
// sized to the deep cap once the window is older than the horizon — else a wide
// window over-reaches the page and strands the window's oldest bars (silent hole).
describe('gap-fill Deep-history paging: — window sized to the deep page cap', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
  });
  afterEach(() => vi.useRealTimers());

  const HORIZON_MIN = 200;
  const DEEP_CAP = 50;

  /** OKX-like: newest-first, AND on a DEEP window (endMs older than the horizon)
   *  the venue serves a smaller page (DEEP_CAP), exactly like /history-candles. */
  function fakeOkxAdapter(venue: RestKlineBar[]): ExchangeAdapter {
    const cfg = {
      apiDomain: 'fake-okx',
      klinesMaxLimit: KLINES_MAX,
      deepHistory: { horizonMs: HORIZON_MIN * MIN, maxLimit: DEEP_CAP },
    } as VenueDomainConfig;
    return {
      cfg,
      async fetchKlines(_d: unknown, req: RestKlinesReq): Promise<RestKlinesPage> {
        const deep = req.endMs !== undefined && req.endMs < NOW - HORIZON_MIN * MIN;
        const cap = deep ? Math.min(req.limit, DEEP_CAP) : req.limit;
        const inWin = venue
          .filter(
            (b) =>
              (req.startMs === undefined || b.ts >= req.startMs) &&
              (req.endMs === undefined || b.ts <= req.endMs) &&
              (req.dropFormingAt === undefined || b.ts < req.dropFormingAt),
          )
          .sort((a, b) => a.ts - b.ts);
        return { bars: inWin.slice(-cap), rawCount: inWin.slice(-cap).length }; // newest-first cap
      },
    } as unknown as ExchangeAdapter;
  }

  it('fills a >horizon (all-deep) gap with no hole (window == deep page)', async () => {
    // 250 bars entirely older than the horizon → every window is deep.
    const first = NOW - 500 * MIN;
    const venue = makeSeries(first, 250); // [NOW-500m .. NOW-251m]
    const filled: RestKlineBar[] = [];
    const deps = makeDeps(fakeOkxAdapter(venue), filled);

    const res = await gapFillInstruments(deps, [
      { instrumentId: 'iid-1', exchangeSymbol: 'BTC-USDT', sinceMs: first - MIN },
    ]);

    const gotTs = filled.map((b) => b.ts).sort((a, b) => a - b);
    expect(res.barsFilled).toBe(250); // ALL of them — pre-fix (300-min window) strands ~200
    expect(gotTs).toEqual(venue.map((b) => b.ts));
    expect(new Set(gotTs).size).toBe(gotTs.length); // no dup at the (deep) seam
  });
});
