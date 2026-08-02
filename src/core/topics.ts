// Canonical topic key:
//   {exchange}:{market_type}:{exchange_symbol}:{stream}[:{tf}]
// e.g. binance:perpetual:BTCUSDT:kline:1m
// Stream-type-agnostic from day 1: depth/trades are valid
// stream names in this same keyspace, just not subscribed in the first release.

export const STREAM_NAMES = [
  'kline',
  'ticker_arr',
  'metrics_arr', // S5/C13 market-metrics batch (Std+ policy-gated, Rule 1)
  'decorr', // cross-venue decorrelation batch (the backlog; global `_cross` topic)
  'decorr_full', // decorrelation modal full per-venue legs (PRO-gated, global `_cross` topic)
  'depth', // reserved — wired WITH Density (never subscribed in the first release/1)
  'trades', // reserved — Splash/Algo/Whale
  'agg_trade', // reserved
] as const;

export type StreamName = (typeof STREAM_NAMES)[number];

export interface TopicKey {
  exchange: string;
  marketType: string;
  exchangeSymbol: string;
  stream: StreamName;
  tf?: string;
}

export function topicToString(t: TopicKey): string {
  const base = `${t.exchange}:${t.marketType}:${t.exchangeSymbol}:${t.stream}`;
  return t.tf ? `${base}:${t.tf}` : base;
}

/** The ONE global decorr topic (the backlog): sentinel exchange `_cross`
 *  (the state is cross-venue by definition), market sentinel `all`, symbol
 *  `_all`. Batches carry ONLY decorrelated coins — tiny payload, one
 *  subscription per client. */
export const DECORR_TOPIC: TopicKey = {
  exchange: '_cross',
  marketType: 'all',
  exchangeSymbol: '_all',
  stream: 'decorr',
};

/** Decorrelation modal's full-legs twin (the product owner 2026-07-22, PRO-gated charts.pro)
 *  — same `_cross`/`all`/`_all` sentinel. RESERVED: the WS topic is no longer
 *  published (the modal now PULLS on demand via the gateway `decorr.legs`
 *  query so it can show ANY coin, gapping or not — the WS timer-batch could
 *  only carry currently-decorrelated coins). The `decorr_full` policy row +
 *  flag are still consumed by that query's tier gate. */
export const DECORR_FULL_TOPIC: TopicKey = {
  exchange: '_cross',
  marketType: 'all',
  exchangeSymbol: '_all',
  stream: 'decorr_full',
};

/** Redis hash the decorr worker rewrites each tick: field = coins.id, value =
 *  JSON({legs, ts}) — every coin's fresh per-venue prices. The gateway's
 *  `decorr.legs` query HGETs one field per decorrelation modal open. Rewritten via a temp key + RENAME so departed coins prune
 *  and a reader never sees a torn/stale-lingering snapshot. */
export const DECORR_LEGS_HASH = 'decorr:legs';

export function topicFromString(s: string): TopicKey | null {
  const parts = s.split(':');
  if (parts.length < 4 || parts.length > 5) return null;
  const [exchange, marketType, exchangeSymbol, stream, tf] = parts;
  if (!exchange || !marketType || !exchangeSymbol || !stream) return null;
  if (!(STREAM_NAMES as readonly string[]).includes(stream)) return null;
  return { exchange, marketType, exchangeSymbol, stream: stream as StreamName, tf };
}

// Redis channel for the live path (radio) — F2.
export const pubChannel = (topic: string) => `pub:${topic}`;
// Redis keys for the hot cache + per-topic REQUIRED seq.
// The closed-bars blob and the forming bar are SEPARATE keys on purpose:
// the blob (~20 KB) is rewritten only on BAR_CLOSE (1/min/instrument), the
// tiny forming key on every diff — at 25-35k instruments (the first release) writing
// the full blob per diff would dominate Redis traffic.
export const hotKey = (topic: string) => `hot:${topic}`;
export const formingKey = (topic: string) => `hot:${topic}:forming`;
export const seqKey = (topic: string) => `seq:${topic}`;

// Hot-cache shapes (Redis, snapshot source per F1 attach sequence).
// Written by ingest, read by fan-out. Compact keys — internal cache format,
// not a wire format (the wire is the protobuf Envelope).
export interface HotBar {
  ts: number; // bar open time, UTC ms
  o: number;
  h: number;
  l: number;
  c: number;
  v: number;
}

/** Value at hotKey(): closed bars + seq. Forming bar lives at formingKey(). */
export interface HotClosedBlob {
  /** Per-topic monotonic seq of the last REQUIRED message (BarClose). */
  seq: number;
  /** Screen-fit closed bars, oldest → newest. */
  bars: HotBar[];
}
