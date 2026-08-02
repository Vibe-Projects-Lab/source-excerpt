// Closed-bar history reader, shared by two callers that may not import each
// other: the ingest workers (cache warmup) and the HTTP gateway (the
// deep-history endpoint behind the client's scroll-back). An import-boundary
// linter forbids the gateway from reaching into the data-plane package, so the
// reader lives on the shared side and both sides import it from here.
//
// Reads CLOSED bars only, never the in-progress bucket. Anchor timeframes read
// their relation directly; derived timeframes aggregate the nearest lower
// anchor, using an aggregation definitionally identical to the one the storage
// layer would apply. The optional `beforeMs` cursor pages STRICTLY OLDER
// buckets — an exclusive upper bound, which is what makes the client's
// scroll-back free of both gaps and duplicates.
import type { Sql } from 'postgres';
import { ANCHOR_RELATION } from './klines.js';
import { TF_ANCHOR, tfFloor, tfFixedMs, type Timeframe } from '../core/timeframes.js';
import type { HotBar } from '../core/topics.js';

/** SQL interval literal per fixed-duration TF (anchors included). Calendar TFs
 *  (1w Monday-origin / 1M) are NOT in the active set — they get their
 *  time_bucket forms when activated. Single source: the tf-correctness script
 *  must never carry its own copy. */
export const BUCKET_INTERVAL: Partial<Record<Timeframe, string>> = {
  '1m': '1 minute',
  '5m': '5 minutes',
  '15m': '15 minutes',
  '30m': '30 minutes',
  '1h': '1 hour',
  '2h': '2 hours',
  '4h': '4 hours',
  '6h': '6 hours',
  '12h': '12 hours',
  '1d': '1 day',
};

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** instrumentIds are interpolated into raw SQL (client.unsafe) — they are
 *  always server-generated UUIDs today, but validate anyway so no future
 *  caller can ever route a venue/user string into this literal (review
 *  finding: latent injection trap). */
export function uuidArrayFilter(instrumentIds?: string[]): string {
  if (!instrumentIds) return '';
  for (const id of instrumentIds) {
    if (!UUID_RE.test(id)) {
      throw new Error(`history-reader: non-UUID instrument id rejected: ${id}`);
    }
  }
  return `AND instrument_id = ANY('{${instrumentIds.join(',')}}'::uuid[])`;
}

/**
 * Newest stored bucket start STRICTLY BELOW `beforeMs`, or null when there is
 * nothing older. This is how `/history` distinguishes "the instrument's history
 * ends here" from "there is a hole wider than one scan window".
 *
 * Why a probe and not a blind jump: `closedTfBars` only scans
 * `tfMs · (limit + 2)` back from the cursor, which is 5 h 02 min at 1m. MEASURED
 * on live data — 19 of 20 sampled instruments already carry 1m gaps wider than
 * that, the largest 1 d 11 h 56 m, which would need EIGHT blind jumps to clear.
 * A jump also returns an empty page, and an empty page prepends nothing, so the
 * client's visible-range listener never fires again and the chart simply stops
 * with no message at all.
 *
 * MEASURED cost on `klines_1m` at 145 641 148 rows / 55 chunks, for an
 * instrument sitting behind that 1 d 11 h gap: planning 12.2 ms, execution
 * 1.5 ms — the ordered append stops at the first chunk holding a row and most
 * branches are never executed.
 */
export async function newestBucketBefore(
  client: Sql,
  tf: Timeframe,
  instrumentId: string,
  beforeMs: number,
  notOlderThanMs: number,
): Promise<number | null> {
  const anchor = TF_ANCHOR[tf];
  const relation = ANCHOR_RELATION[anchor];
  if (!UUID_RE.test(instrumentId)) {
    throw new Error(`history-reader: non-UUID instrument id rejected: ${instrumentId}`);
  }
  const rows = (await client.unsafe(`
    SELECT (extract(epoch FROM max(ts)) * 1000)::bigint AS ts_ms
    FROM ${relation}
    WHERE instrument_id = '${instrumentId}'
      AND ts < '${new Date(beforeMs).toISOString()}'
      AND ts >= '${new Date(notOlderThanMs).toISOString()}'`)) as unknown as {
    ts_ms: string | null;
  }[];
  const ms = rows[0]?.ts_ms;
  if (ms == null) return null;
  // Return the BUCKET the row belongs to, so a derived TF gets a cursor it can
  // actually page from.
  return tfFloor(tf, Number(ms));
}

export interface BarRow {
  instrument_id: string;
  ts_ms: string;
  o: number;
  h: number;
  l: number;
  c: number;
  v: number;
}

function groupBars(rows: BarRow[]): Map<string, HotBar[]> {
  const out = new Map<string, HotBar[]>();
  for (const r of rows) {
    const list = out.get(r.instrument_id) ?? [];
    list.push({
      ts: Number(r.ts_ms),
      o: Number(r.o),
      h: Number(r.h),
      l: Number(r.l),
      c: Number(r.c),
      v: Number(r.v),
    });
    out.set(r.instrument_id, list);
  }
  for (const list of out.values()) list.sort((a, b) => a.ts - b.ts);
  return out;
}

/**
 * Last ≤`limit` CLOSED bars of `tf` per instrument, ending BEFORE `beforeMs`
 * (exclusive) when the cursor is given, else ending before the current bucket.
 * Ascending per instrument. Anchor TFs read their relation directly; derived
 * TFs aggregate the nearest lower anchor.
 */
export async function closedTfBars(
  client: Sql,
  tf: Timeframe,
  opts: { limit?: number; instrumentIds?: string[]; beforeMs?: number } = {},
): Promise<Map<string, HotBar[]>> {
  const limit = opts.limit ?? 300;
  const anchor = TF_ANCHOR[tf];
  const relation = ANCHOR_RELATION[anchor];
  // Never return the in-progress bucket; a `beforeMs` cursor additionally pages
  // strictly older buckets (exclusive upper bound = min(floor, beforeMs)).
  const floorMs = tfFloor(tf, Date.now());
  const cutoffMs = opts.beforeMs !== undefined ? Math.min(floorMs, opts.beforeMs) : floorMs;
  const cutoff = new Date(cutoffMs).toISOString();
  // Perf lower bound: scan only the window that can contain `limit` buckets
  // (chunk-exclusion + PK (instrument_id, ts) cover the range — no new index).
  const spanMs = (tfFixedMs(tf) ?? 31 * 86_400_000) * (limit + 2);
  const lower = new Date(cutoffMs - spanMs).toISOString();
  const filter = uuidArrayFilter(opts.instrumentIds);

  if (tf === anchor) {
    const rows = (await client.unsafe(`
      SELECT instrument_id, ts_ms, o, h, l, c, v FROM (
        SELECT instrument_id,
               (extract(epoch FROM ts) * 1000)::bigint AS ts_ms,
               o, h, l, c, v,
               row_number() OVER (PARTITION BY instrument_id ORDER BY ts DESC) AS rn
        FROM ${relation}
        WHERE ts >= '${lower}' AND ts < '${cutoff}' ${filter}
      ) s WHERE rn <= ${limit}`)) as unknown as BarRow[];
    return groupBars(rows);
  }

  const interval = BUCKET_INTERVAL[tf];
  if (!interval) {
    throw new Error(`history-reader: calendar timeframe ${tf} not active`);
  }

  // LIVE-EDGE GUARD (kline-storage refactor). A derived bucket is only real
  // once its anchor covers the bucket's whole span. While klines_1h was a
  // continuous aggregate with real-time aggregation that was automatic — the
  // view computed the missing tail on the fly. It is a plain table now, so the
  // newest hour exists only after the roll-up tick, and the newest 4h bucket
  // would otherwise be returned computed from THREE hours. That truncated bar
  // is not just wrong on screen: ingest-main seeds derived series from this
  // reader with "DB wins on overlap", so it lands in the hot cache and stays
  // until the next reseed.
  //
  // So: drop a bucket that extends past the anchor's newest bar. Applied ONLY
  //  - to TFs anchored on 1h/1d — the 1m-anchored ones (5m/15m/30m) have the
  //    same shape but a persist lag of seconds, and that behaviour predates
  //    this refactor;
  //  - at the live edge (no `beforeMs`) — during deep pagination the newest
  //    row IN THE SCANNED WINDOW is not the newest row that exists, and the
  //    guard would wrongly clip every page.
  const anchorMs = tfFixedMs(anchor) ?? 3_600_000;
  const tfMs = tfFixedMs(tf) ?? 0;
  const guardEdge = anchor !== '1m' && opts.beforeMs === undefined && tfMs > 0;
  const edgeGuard = guardEdge
    ? `AND ts_ms + ${tfMs} <= newest_src_ms + ${anchorMs}`
    : '';

  const rows = (await client.unsafe(`
    SELECT instrument_id, ts_ms, o, h, l, c, v FROM (
      SELECT g.*,
             max(g.last_src_ms) OVER (PARTITION BY g.instrument_id) AS newest_src_ms
      FROM (
        SELECT instrument_id,
               (extract(epoch FROM time_bucket(INTERVAL '${interval}', ts)) * 1000)::bigint AS ts_ms,
               first(o, ts) AS o, max(h) AS h, min(l) AS l, last(c, ts) AS c, sum(v) AS v,
               (extract(epoch FROM max(ts)) * 1000)::bigint AS last_src_ms,
               row_number() OVER (
                 PARTITION BY instrument_id
                 ORDER BY time_bucket(INTERVAL '${interval}', ts) DESC
               ) AS rn
        FROM ${relation}
        WHERE ts >= '${lower}'
          AND time_bucket(INTERVAL '${interval}', ts) < '${cutoff}' ${filter}
        GROUP BY instrument_id, time_bucket(INTERVAL '${interval}', ts)
      ) g
    ) s WHERE rn <= ${limit} ${edgeGuard}`)) as unknown as BarRow[];
  return groupBars(rows);
}
