// Timeframe registry + boundary math.
// Client-safe: no env, no node imports.
//
// The FULL anchor mapping is encoded here, so derivation is one
// generic "nearest lower anchor" algorithm over this table — enabling a new
// TF later is a config change, never engine work. the first release actively serves
// ACTIVE (Build Plan STAGE-1 scope line); the rest are valid but unwired.
//
// Tokens follow the Binance kline convention ('1m' minute vs '1M' month —
// case carries meaning). Calendar boundaries (documented defaults, spec is
// silent): 1w = Monday 00:00 UTC (matches Binance weekly), 1d/1M = UTC.

export const TIMEFRAMES = [
  '1m',
  '5m',
  '15m',
  '30m',
  '1h',
  '2h',
  '4h',
  '6h',
  '12h',
  '1d',
  '1w',
  '1M',
] as const;

export type Timeframe = (typeof TIMEFRAMES)[number];

/** Anchor series that are STORED (1m raw + 1h/1D continuous aggregates). */
export type AnchorTimeframe = '1m' | '1h' | '1d';

/** 5m/15m/30m ← 1m; 2h/4h/6h/12h ← 1h; 1W/1M ← 1D. Anchors ← themselves. */
export const TF_ANCHOR: Record<Timeframe, AnchorTimeframe> = {
  '1m': '1m',
  '5m': '1m',
  '15m': '1m',
  '30m': '1m',
  '1h': '1h',
  '2h': '1h',
  '4h': '1h',
  '6h': '1h',
  '12h': '1h',
  '1d': '1d',
  '1w': '1d',
  '1M': '1d',
};

export function isTimeframe(v: string): v is Timeframe {
  return (TIMEFRAMES as readonly string[]).includes(v);
}

export function isAnchor(tf: Timeframe): boolean {
  return TF_ANCHOR[tf] === tf;
}

/** Stage-1 actively-served set (Build Plan: "kline_1m (+ server-derived
 *  5m/15m/1h/4h/1D)"). Server processes may override via ACTIVE_TIMEFRAMES
 *  env; the override must be a subset of TIMEFRAMES. */
export const DEFAULT_ACTIVE_TIMEFRAMES: readonly Timeframe[] = [
  '1m',
  '5m',
  '15m',
  '1h',
  '4h',
  '1d',
];

const MINUTE = 60_000;
const HOUR = 3_600_000;
const DAY = 86_400_000;
const WEEK = 7 * DAY;
// 1970-01-05 was a Monday: shift the epoch so weeks floor to Monday 00:00 UTC.
const MONDAY_EPOCH_OFFSET = 4 * DAY;

const FIXED_MS: Partial<Record<Timeframe, number>> = {
  '1m': MINUTE,
  '5m': 5 * MINUTE,
  '15m': 15 * MINUTE,
  '30m': 30 * MINUTE,
  '1h': HOUR,
  '2h': 2 * HOUR,
  '4h': 4 * HOUR,
  '6h': 6 * HOUR,
  '12h': 12 * HOUR,
  '1d': DAY,
};

/** Bucket start (ms, UTC) containing `ts`. */
export function tfFloor(tf: Timeframe, ts: number): number {
  const fixed = FIXED_MS[tf];
  if (fixed !== undefined) return Math.floor(ts / fixed) * fixed;
  if (tf === '1w') {
    return (
      Math.floor((ts - MONDAY_EPOCH_OFFSET) / WEEK) * WEEK + MONDAY_EPOCH_OFFSET
    );
  }
  // '1M' — calendar month ("calendar logic for 1M"), UTC.
  const d = new Date(ts);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1);
}

/** Start of the bucket AFTER the one containing `ts`. */
export function tfNext(tf: Timeframe, ts: number): number {
  const start = tfFloor(tf, ts);
  if (tf === '1M') {
    const d = new Date(start);
    return Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 1);
  }
  if (tf === '1w') return start + WEEK;
  return start + (FIXED_MS[tf] as number);
}

/** Fixed bucket duration in ms; undefined for calendar TFs ('1M'). */
export function tfFixedMs(tf: Timeframe): number | undefined {
  if (tf === '1w') return WEEK;
  return FIXED_MS[tf];
}

/** Seconds pass-through set") — never
 *  persisted, but venue REST/WS still speaks these tokens. */
const SECONDS_MS: Record<string, number> = {
  '1s': 1_000,
  '5s': 5_000,
  '15s': 15_000,
  '30s': 30_000,
};

/**
 * Bucket duration for a caller holding a PLAIN STRING tf — adapter dialects
 * parse venue rows before anything has narrowed the token to `Timeframe`.
 * Unknown or absent → `fallbackMs` (default one minute, which is what every
 * pre-refactor caller assumed unconditionally).
 *
 * This exists because the forming-bar guard in the REST row parsers was a
 * hardcoded 60_000: harmless while 1m was the only timeframe ever fetched,
 * silent corruption the moment the anchor writers ask a venue for 1h bars.
 */
export function tfDurationMs(tf: string | undefined, fallbackMs = MINUTE): number {
  if (tf === undefined) return fallbackMs;
  const seconds = SECONDS_MS[tf];
  if (seconds !== undefined) return seconds;
  if (!isTimeframe(tf)) return fallbackMs;
  return tfFixedMs(tf) ?? fallbackMs;
}
