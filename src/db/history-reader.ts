// Reading stored candles back.
//
// Shared by two callers that may not import each other — the ingest side, which
// warms its cache, and the HTTP side, which serves the client's scroll-back. An
// import-boundary rule forbids one from reaching into the other, so the reader
// lives on the shared side.
//
// Two properties define it. It returns closed bars only, never the bucket
// currently forming, because a partial bar written anywhere durable is wrong
// forever. And its cursor pages strictly older buckets through an exclusive
// upper bound, which is what makes scrolling back free of both gaps and
// duplicates.
//
// Timeframes that are stored read their table directly; the rest aggregate the
// nearest lower stored series, using the same aggregation the storage layer
// would apply.
import type { Sql } from 'postgres';
import { ANCHOR_RELATION } from './klines.js';
import { TF_ANCHOR, tfFloor, tfFixedMs, type Timeframe } from '../core/timeframes.js';
import type { HotBar } from '../core/topics.js';

/** SQL interval literal per fixed-duration TF (anchors included). Calendar TFs
 *  (1w Monday-origin / 1M) are NOT in the active set — they get their
 *  bucket forms when they are. This table is the single source; nothing else
 *  may carry its own copy of these literals. */
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
 * Newest stored bucket strictly older than the cursor, or null when there is
 * nothing older at all. This is what lets a caller distinguish "this
 * instrument's history ends here" from "there is a hole wider than one scan
 * window".
 *
 * Why a probe rather than simply jumping back a fixed distance: the page reader
 * scans a bounded window back from the cursor, and a hole can be wider than
 * that window. Jumping blindly would need an unpredictable number of attempts,
 * and each miss returns an empty page. An empty page prepends nothing, so the
 * client's scroll listener never fires again and the chart stops with no
 * explanation — a silent failure, which is the worst kind.
 *
 * The probe is cheap despite scanning an open-ended range: the query is an
 * ordered scan that stops at the first stored row it finds, so almost none of
 * the range is ever visited.
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

  // LIVE-EDGE GUARD. A derived bucket is only real once the series it derives
  // from covers the bucket's whole span. While the hourly series was a
  // materialised view this was automatic — the view computed the missing tail
  // on demand. It is a plain table now, so the newest hour exists only after
  // the roll-up runs, and a four-hour bucket built at that moment would be
  // computed from three hours of data.
  //
  // A truncated bar is worse than a missing one. It is not only wrong on
  // screen: the ingest side seeds its derived series from this reader and lets
  // stored data win on overlap, so the wrong bar reaches the cache and stays
  // there until the next reseed.
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
