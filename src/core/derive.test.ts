// Derivation must agree with what the database would compute for the same
// bucket, regardless of the order bars arrive in.
import { describe, expect, it } from 'vitest';
import type { HotBar } from './topics.js';
import { TfDeriver, aggregateBars, foldBar, aggToBar } from './derive.js';

const T0 = Date.parse('2026-07-03T12:00:00Z');
const MIN = 60_000;

function bar(i: number, o: number, h: number, l: number, c: number, v: number): HotBar {
  return { ts: T0 + i * MIN, o, h, l, c, v };
}

/** Pseudo-random but deterministic bar series. */
function series(n: number, seed = 42): HotBar[] {
  let x = seed;
  const rnd = () => {
    x = (x * 1103515245 + 12345) % 2 ** 31;
    return x / 2 ** 31;
  };
  const bars: HotBar[] = [];
  let price = 100;
  for (let i = 0; i < n; i++) {
    const o = price;
    const c = o * (1 + (rnd() - 0.5) * 0.01);
    const h = Math.max(o, c) * (1 + rnd() * 0.005);
    const l = Math.min(o, c) * (1 - rnd() * 0.005);
    bars.push({ ts: T0 + i * MIN, o, h, l, c, v: Math.round(rnd() * 100) });
    price = c;
  }
  return bars;
}

describe('foldBar is equivalent to the SQL-style recompute', () => {
  it('incremental fold equals reference aggregate for 60 random bars', () => {
    const bars = series(60);
    let agg = null as ReturnType<typeof foldBar> | null;
    for (const b of bars) agg = foldBar(agg, b);
    const folded = aggToBar(agg as NonNullable<typeof agg>, T0);
    const reference = aggregateBars(bars, T0);
    expect(folded).toEqual(reference);
  });

  it('o=first/c=last are ordered by bar ts, never arrival order', () => {
    const b1 = bar(0, 10, 12, 9, 11, 1);
    const b2 = bar(1, 11, 13, 10, 12, 1);
    // Arrival order reversed:
    let agg = foldBar(null, b2);
    agg = foldBar(agg, b1);
    const out = aggToBar(agg, T0);
    expect(out.o).toBe(10); // first by ts
    expect(out.c).toBe(12); // last by ts
    expect(out.h).toBe(13);
    expect(out.l).toBe(9);
    expect(out.v).toBe(2);
  });
});

describe('TfDeriver boundaries', () => {
  it('emits a 5m close exactly when the 5th minute closes', () => {
    const d = new TfDeriver(['5m'], T0);
    const bars = series(5);
    const emitted: { tf: string; closed: HotBar }[] = [];
    for (const b of bars) {
      const { closes } = d.onClose(b);
      emitted.push(...closes);
    }
    expect(emitted).toHaveLength(1);
    const closed = emitted[0]?.closed as HotBar;
    expect(closed).toEqual(aggregateBars(bars, T0));
    expect(emitted[0]?.tf).toBe('5m');
  });

  it('new forming bar opens at the next bucket seeded from the close', () => {
    const d = new TfDeriver(['5m'], T0);
    const bars = series(5);
    let newForming: HotBar | undefined;
    for (const b of bars) {
      const { closes } = d.onClose(b);
      if (closes[0]) newForming = closes[0].newForming;
    }
    expect(newForming?.ts).toBe(T0 + 5 * MIN);
    expect(newForming?.v).toBe(0);
    expect(newForming?.o).toBe(bars[4]?.c);
  });

  it('forming bar = closedAgg ⊕ live 1m diff', () => {
    const d = new TfDeriver(['15m'], T0);
    const closed = series(2);
    for (const b of closed) d.onClose(b);
    const live = bar(2, 100, 200, 50, 150, 7);
    const updates = d.onDiff(live);
    expect(updates).toHaveLength(1);
    const forming = updates[0]?.forming as HotBar;
    const reference = aggregateBars([...closed, live], T0);
    expect(forming).toEqual(reference);
  });

  it('sparse gap: elapsed bucket closes with what it had', () => {
    const d = new TfDeriver(['5m'], T0);
    d.onClose(bar(0, 10, 11, 9, 10.5, 1));
    d.onClose(bar(1, 10.5, 12, 10, 11, 1));
    // Minutes 2-4 missing; the next close is already in the NEXT bucket.
    const { closes } = d.onClose(bar(6, 11, 11.5, 10.8, 11.2, 1));
    expect(closes).toHaveLength(1);
    const closed = closes[0]?.closed as HotBar;
    expect(closed.ts).toBe(T0);
    expect(closed.v).toBe(2); // only the two bars that existed
    expect(closed.c).toBe(11);
  });

  it('multi-TF: 1h close arrives with the 60th minute, 5m every 5th', () => {
    const d = new TfDeriver(['5m', '1h'], T0);
    const bars = series(60);
    const byTf = new Map<string, HotBar[]>();
    for (const b of bars) {
      for (const c of d.onClose(b).closes) {
        const list = byTf.get(c.tf) ?? [];
        list.push(c.closed);
        byTf.set(c.tf, list);
      }
    }
    expect(byTf.get('5m')).toHaveLength(12);
    expect(byTf.get('1h')).toHaveLength(1);
    expect(byTf.get('1h')?.[0]).toEqual(aggregateBars(bars, T0));
    // 5m bars stitched back == the hour bar
    const from5m = aggregateBars(byTf.get('5m') as HotBar[], T0);
    const hour = byTf.get('1h')?.[0] as HotBar;
    expect(from5m?.h).toBe(hour.h);
    expect(from5m?.l).toBe(hour.l);
    expect(from5m?.o).toBe(hour.o);
    expect(from5m?.c).toBe(hour.c);
    expect(from5m?.v).toBeCloseTo(hour.v, 8);
  });

  it('seed() resumes a partially-elapsed bucket', () => {
    const bars = series(3);
    const d = new TfDeriver(['5m'], T0);
    // Simulate restart mid-bucket: seed from "DB" aggregate of 3 closed bars.
    let agg = null as ReturnType<typeof foldBar> | null;
    for (const b of bars) agg = foldBar(agg, b);
    d.seed('5m', T0, agg);
    const b4 = series(5)[3] as HotBar;
    const b5 = series(5)[4] as HotBar;
    d.onClose(b4);
    const { closes } = d.onClose(b5);
    expect(closes).toHaveLength(1);
    expect(closes[0]?.closed).toEqual(aggregateBars(series(5), T0));
  });
});
