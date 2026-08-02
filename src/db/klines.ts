// Hand-written surface for the TimescaleDB candle storage.
//
// A standing rule: the hypertables and their policies are created ONLY by
// raw-SQL migration steps and are NEVER modelled in the ORM schema. The schema
// generator cannot represent a hypertable, and would emit drop-and-recreate
// diffs against them — which is a data-loss migration written by a tool that
// believes it is fixing a drift.
//
// All three anchor series are plain hypertables. The hourly and daily ones used
// to be continuous aggregates over the one-minute series; they now accept
// direct writes from the roll-up, repair and seed workers, because the
// one-minute series became a rolling window and an aggregate cannot outlive its
// source.

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
