// Deriving coarser timeframes from the stored anchor.
//
// One algorithm over the anchor table, with no per-timeframe branches: a new
// timeframe is a configuration entry, never engine work.
//
// The aggregation is deliberately identical to what the database would compute
// for the same bucket — open from the first bar, close from the last, high and
// low from the extremes, volume summed, ordered by bar time rather than arrival
// order. That equivalence is what lets a chart show live derived bars and
// stored history as one continuous series.
//
// This is a pure state machine; the process that hosts it owns all the I/O.
import type { HotBar } from './topics.js';
import { tfFloor, tfNext, type Timeframe } from './timeframes.js';

/** Running aggregate of the CLOSED 1m bars inside one TF bucket. */
export interface TfAgg {
  o: number;
  h: number;
  l: number;
  c: number;
  v: number;
  firstTs: number;
  lastTs: number;
}

export function foldBar(agg: TfAgg | null, bar: HotBar): TfAgg {
  if (!agg) {
    return {
      o: bar.o,
      h: bar.h,
      l: bar.l,
      c: bar.c,
      v: bar.v,
      firstTs: bar.ts,
      lastTs: bar.ts,
    };
  }
  return {
    // first/last by bar timestamp, tolerant of out-of-order folds.
    o: bar.ts < agg.firstTs ? bar.o : agg.o,
    c: bar.ts > agg.lastTs ? bar.c : agg.c,
    h: Math.max(agg.h, bar.h),
    l: Math.min(agg.l, bar.l),
    v: agg.v + bar.v,
    firstTs: Math.min(agg.firstTs, bar.ts),
    lastTs: Math.max(agg.lastTs, bar.ts),
  };
}

export function aggToBar(agg: TfAgg, bucketStart: number): HotBar {
  return { ts: bucketStart, o: agg.o, h: agg.h, l: agg.l, c: agg.c, v: agg.v };
}

/** Reference recompute for tests/verification (must equal folded output). */
export function aggregateBars(bars: HotBar[], bucketStart: number): HotBar | null {
  const sorted = [...bars].sort((a, b) => a.ts - b.ts);
  let agg: TfAgg | null = null;
  for (const b of sorted) agg = foldBar(agg, b);
  return agg ? aggToBar(agg, bucketStart) : null;
}

export interface TfDiffUpdate {
  tf: Timeframe;
  forming: HotBar;
}

export interface TfCloseUpdate {
  tf: Timeframe;
  /** Completed TF bar (sparse-tolerant: aggregates whatever 1m bars existed). */
  closed: HotBar;
  /** Forming bar opening the next bucket (seeded from the close, v=0). */
  newForming: HotBar;
}

interface TfState {
  bucketStart: number;
  closedAgg: TfAgg | null;
}

/**
 * Per-instrument deriver over the DERIVED subset of active timeframes.
 * Feed it the 1m stream; it yields per-TF forming updates and closes.
 */
export class TfDeriver {
  private readonly state = new Map<Timeframe, TfState>();

  constructor(tfs: readonly Timeframe[], now = Date.now()) {
    for (const tf of tfs) {
      if (tf === '1m') continue; // 1m is the source, never derived
      this.state.set(tf, { bucketStart: tfFloor(tf, now), closedAgg: null });
    }
  }

  timeframes(): Timeframe[] {
    return [...this.state.keys()];
  }

  /** Seed the current bucket's closed one-minute aggregate (called
 *  AFTER gap-fill, from DB rows). */
  seed(tf: Timeframe, bucketStart: number, agg: TfAgg | null): void {
    const st = this.state.get(tf);
    if (!st) return;
    st.bucketStart = bucketStart;
    st.closedAgg = agg;
  }

  /** 1m forming-bar diff → per-TF forming bars (closedAgg ⊕ live 1m bar). */
  onDiff(bar1m: HotBar): TfDiffUpdate[] {
    const out: TfDiffUpdate[] = [];
    for (const [tf, st] of this.state) {
      if (tfFloor(tf, bar1m.ts) !== st.bucketStart) {
        // The live 1m bar already belongs to the NEXT bucket while no close
        // rolled us over (e.g. process started mid-minute) — roll silently.
        st.bucketStart = tfFloor(tf, bar1m.ts);
        st.closedAgg = null;
      }
      const merged = foldBar(st.closedAgg ? { ...st.closedAgg } : null, bar1m);
      out.push({ tf, forming: aggToBar(merged, st.bucketStart) });
    }
    return out;
  }

  /** Closed 1m bar → folds into buckets; emits TF closes on boundaries. */
  onClose(bar1m: HotBar): { closes: TfCloseUpdate[]; forming: TfDiffUpdate[] } {
    const closes: TfCloseUpdate[] = [];
    const forming: TfDiffUpdate[] = [];
    for (const [tf, st] of this.state) {
      const barBucket = tfFloor(tf, bar1m.ts);
      if (barBucket !== st.bucketStart) {
        // Sparse rollover: bucket(s) elapsed without their final minute (gap
        // or quiet venue). Close the old bucket with what it has, if anything.
        if (st.closedAgg) {
          const closed = aggToBar(st.closedAgg, st.bucketStart);
          closes.push({
            tf,
            closed,
            newForming: {
              ts: barBucket,
              o: closed.c,
              h: closed.c,
              l: closed.c,
              c: closed.c,
              v: 0,
            },
          });
        }
        st.bucketStart = barBucket;
        st.closedAgg = null;
      }
      st.closedAgg = foldBar(st.closedAgg, bar1m);
      if (bar1m.ts + 60_000 >= tfNext(tf, st.bucketStart)) {
        // Final minute of the bucket closed → the TF bar is complete.
        const closed = aggToBar(st.closedAgg, st.bucketStart);
        const nextStart = tfNext(tf, st.bucketStart);
        closes.push({
          tf,
          closed,
          newForming: {
            ts: nextStart,
            o: closed.c,
            h: closed.c,
            l: closed.c,
            c: closed.c,
            v: 0,
          },
        });
        st.bucketStart = nextStart;
        st.closedAgg = null;
      } else {
        forming.push({
          tf,
          forming: aggToBar(st.closedAgg, st.bucketStart),
        });
      }
    }
    return { closes, forming };
  }
}
