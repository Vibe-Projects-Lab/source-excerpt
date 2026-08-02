// The stored candle series and the row shape they share.
//
// A standing rule: these tables and their policies are created only by raw SQL
// migrations and are never modelled in the ORM schema. The schema generator
// cannot represent a time-series table and would propose dropping and
// recreating them — a data-loss migration written by a tool that believes it is
// fixing a drift.
//
// All three series are plain tables. The hourly and daily ones were once
// materialised views over the minute series; they now take direct writes,
// because the minute series became a rolling window and a view cannot outlive
// its source.
import type { AnchorTimeframe } from '../core/timeframes.js';

export const KLINES_1M_TABLE = 'klines_1m';

/** Anchor series → physical relation. All three are plain hypertables; the
 *  names did NOT change at the refactor, so every reader is untouched. */
export const ANCHOR_RELATION: Record<AnchorTimeframe, string> = {
  '1m': 'klines_1m',
  '1h': 'klines_1h',
  '1d': 'klines_1d',
};

/** Row shape shared by all three anchors (identical column names). */
export interface KlineRow {
  instrument_id: string;
  ts: Date;
  o: number;
  h: number;
  l: number;
  c: number;
  v: number;
}
