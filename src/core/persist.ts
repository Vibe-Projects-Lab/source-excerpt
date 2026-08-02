// Persist-pipeline contract. Server-side only.
//
// Closed one-minute candles ride a bounded Redis Stream from the ingest
// normalizer — and from the gap-fill library — to a single persist worker,
// which is the ONLY writer of live candle rows and of the watermark. Exactly
// one instance of that worker runs; scaling it later means sharding consumers
// by instrument, never round-robin.
//
// The bulk history backfill is the one exception to the single-writer rule: it
// inserts BEHIND the watermark and never touches it.

export const PERSIST_STREAM = 'persist:klines_1m';
export const PERSIST_GROUP = 'persist';

/** metrics journal (OI/funding minute snapshots → metrics_1m). Same
 *  bounded-stream → single-persist-worker shape as klines; separate stream so
 *  the two consumers never share a blocking read. ⚠ Data is IRREPLACEABLE
 *  (venues serve OI history only at ≥5m/~30d) — the stream MAXLEN is sized
 *  for ~a day of buffer, not klines' throughput. */
export const METRICS_PERSIST_STREAM = 'persist:metrics_1m';
export const METRICS_PERSIST_GROUP = 'persist-metrics';

/** Durable bootstrap queue: listings sync enqueues new instruments
 *  here (1B); the bootstrap worker consumes newest-first (1E). Bounded like
 *  the persist stream — a lost entry re-enters via verify. */
export const BOOTSTRAP_STREAM = 'bootstrap:queue';
export const BOOTSTRAP_GROUP = 'bootstrap';

/** Gap-fill request stream: the persist worker's startup
 *  watermark-lag check posts instruments here; each ingest domain consumes
 *  with its OWN group (sees all, heals only its instruments, acks all). */
export const GAPFILL_REQUEST_STREAM = 'gapfill:requests';

/** Structural slice of an ioredis client for bounded XADD. */
interface XAddClient {
  xadd(
    key: string,
    maxlenToken: 'MAXLEN',
    approx: '~',
    threshold: string,
    id: '*',
    ...fields: string[]
  ): Promise<unknown>;
}

/** Bounded-journal XADD — ONE call shape for every producer
 *  (the MAXLEN preamble was hand-typed at 4 sites). */
export function xaddBounded(
  redis: XAddClient,
  stream: string,
  maxlen: number,
  fields: string[],
): Promise<unknown> {
  return redis.xadd(stream, 'MAXLEN', '~', String(maxlen), '*', ...fields);
}

/** Flat XREAD field array → keyed map — single decode for all stream
 *  consumers (three divergent implementations). */
export function fieldsToMap(fields: string[]): Map<string, string> {
  const m = new Map<string, string>();
  for (let i = 0; i + 1 < fields.length; i += 2) {
    m.set(fields[i] as string, fields[i + 1] as string);
  }
  return m;
}

/** Structural slice of an ioredis client (shared carries no redis dep). */
interface XGroupClient {
  xgroup(
    subcommand: 'CREATE',
    key: string,
    group: string,
    id: string,
    mkstream: 'MKSTREAM',
  ): Promise<unknown>;
}

/** Idempotent consumer-group creation from '0' (entries XADDed before the
 *  group existed are still consumed). ONE implementation: there used to be
 *  three copies of this same dance. */
export async function ensureConsumerGroup(
  redis: XGroupClient,
  stream: string,
  group: string,
): Promise<void> {
  try {
    await redis.xgroup('CREATE', stream, group, '0', 'MKSTREAM');
  } catch (err) {
    if (!(err instanceof Error && err.message.includes('BUSYGROUP'))) throw err;
  }
}

/** True when a consumer-group read failed because the group/stream vanished
 *  (e.g. total Redis state loss — dev Redis has no persistence): the
 *  consumer must recreate the group and continue, never wedge. */
export function isNoGroupError(err: unknown): boolean {
  return err instanceof Error && err.message.includes('NOGROUP');
}

/** watermark_live = ts of last persisted closed 1m candle, Redis-cached. */
export function watermarkKey(instrumentId: string): string {
  return `wm:kline_1m:${instrumentId}`;
}

/** The watermark SET must be a MAX: an out-of-order writer must never
 *  drag it backwards and re-open a healed gap. Lua keeps the read-modify-write
 *  atomic. Lives here rather than in the persist worker because gave verify
 *  a second writer — a shared invariant needs a shared implementation. */
export const WATERMARK_MAX_LUA = `local c = redis.call('GET', KEYS[1])
if not c or tonumber(ARGV[1]) > tonumber(c) then redis.call('SET', KEYS[1], ARGV[1]) end`;

export interface PersistEntry {
  instrumentId: string;
  /** Bar open time, ms UTC. */
  ts: number;
  o: number;
  h: number;
  l: number;
  c: number;
  v: number;
}

/** Flat field list for XADD. */
export function persistEntryToFields(e: PersistEntry): string[] {
  return [
    'iid',
    e.instrumentId,
    'ts',
    String(e.ts),
    'o',
    String(e.o),
    'h',
    String(e.h),
    'l',
    String(e.l),
    'c',
    String(e.c),
    'v',
    String(e.v),
  ];
}

// ---------------------------------------------------------------------------
// metrics journal entries (metrics_1m). One entry = one (instrument,
// minute-bucket) snapshot of whatever fields were OBSERVED in that minute —
// ts is the minute bucket of the VALUE's own observation time (adapter-set
// oiTs/fundingTs), never the sampling timer's clock, so a frozen upstream
// value can only ever re-address its own old bucket, which is an idempotent
// no-op, rather than forward-fill the present. At least one value field is
// required; the persist worker merges same-bucket entries per column, first
// write winning.
// ---------------------------------------------------------------------------

export interface MetricsPersistEntry {
  instrumentId: string;
  /** Minute-bucket open time, ms UTC (bucket of the observation time). */
  ts: number;
  oiContracts?: number;
  oiUsd?: number;
  fundingRate?: number;
}

const METRIC_VALUE_KEYS = ['oic', 'oiu', 'fr'] as const;
type MetricFieldKey = (typeof METRIC_VALUE_KEYS)[number];
const METRIC_FIELD_MAP: Record<MetricFieldKey, keyof MetricsPersistEntry> = {
  oic: 'oiContracts',
  oiu: 'oiUsd',
  fr: 'fundingRate',
};

/** Flat field list for XADD; omits absent metric fields. */
export function metricsEntryToFields(e: MetricsPersistEntry): string[] {
  const out = ['iid', e.instrumentId, 'ts', String(e.ts)];
  if (e.oiContracts !== undefined) out.push('oic', String(e.oiContracts));
  if (e.oiUsd !== undefined) out.push('oiu', String(e.oiUsd));
  if (e.fundingRate !== undefined) out.push('fr', String(e.fundingRate));
  return out;
}

/** Structural parse; null = malformed (bad ts alignment, no value fields,
 *  non-finite numbers). */
export function parseMetricsEntry(fields: string[]): MetricsPersistEntry | null {
  const map = fieldsToMap(fields);
  const iid = map.get('iid');
  if (!iid) return null;
  const ts = Number(map.get('ts'));
  if (!Number.isFinite(ts) || ts <= 0 || ts % 60_000 !== 0) return null;
  const entry: MetricsPersistEntry = { instrumentId: iid, ts };
  let hasValue = false;
  for (const k of METRIC_VALUE_KEYS) {
    const raw = map.get(k);
    if (raw === undefined) continue;
    const n = Number(raw);
    if (!Number.isFinite(n)) return null; // present but garbage = malformed
    (entry as unknown as Record<string, unknown>)[METRIC_FIELD_MAP[k]] = n;
    hasValue = true;
  }
  return hasValue ? entry : null;
}

/** Structural parse of an XREADGROUP entry field list; null = malformed. */
export function parsePersistEntry(fields: string[]): PersistEntry | null {
  const map = fieldsToMap(fields);
  const iid = map.get('iid');
  if (!iid) return null;
  const nums: Record<'ts' | 'o' | 'h' | 'l' | 'c' | 'v', number> = {
    ts: NaN,
    o: NaN,
    h: NaN,
    l: NaN,
    c: NaN,
    v: NaN,
  };
  for (const k of ['ts', 'o', 'h', 'l', 'c', 'v'] as const) {
    const raw = map.get(k);
    if (raw === undefined) return null;
    const n = Number(raw);
    if (!Number.isFinite(n)) return null;
    nums[k] = n;
  }
  if (nums.ts <= 0 || nums.ts % 60_000 !== 0) return null; // 1m open time
  return { instrumentId: iid, ...nums };
}
