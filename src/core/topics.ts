// The naming scheme every stream, cache key and channel is derived from:
//
//   {exchange}:{market}:{symbol}:{stream}[:{timeframe}]
//
// One canonical string, built in one place, so a subscription, a cache entry
// and a published message can never disagree about what they refer to.
//
// The stream name is a value in an open set rather than a fixed list of the
// streams we currently use, which is what allows a new kind of market data to
// be added without touching the routing machinery.
export const STREAM_NAMES = [
  'kline',
  'ticker_arr',
  'metrics_arr', // open interest, funding, mark and index prices
  'decorr', // cross-venue price divergence
  'decorr_full', // the same, with the per-exchange breakdown
  'depth', // reserved — order books
  'trades', // reserved
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

/** The single global topic for cross-exchange divergence. It is cross-venue by
 *  definition, so the exchange, market and symbol positions carry sentinels
 *  rather than a real instrument. Batches carry only the coins currently
 *  diverging, which keeps the payload small and lets a client hold one
 *  subscription instead of thousands. */
export const DECORR_TOPIC: TopicKey = {
  exchange: '_cross',
  marketType: 'all',
  exchangeSymbol: '_all',
  stream: 'decorr',
};

/** The same state with the per-exchange breakdown, on the same sentinel key.
 *  RESERVED: this topic is no longer published. A periodic batch could only
 *  ever carry the coins that were diverging when it was built, so the detailed
 *  view is fetched on demand instead, which lets it answer for any coin. */
export const DECORR_FULL_TOPIC: TopicKey = {
  exchange: '_cross',
  marketType: 'all',
  exchangeSymbol: '_all',
  stream: 'decorr_full',
};

/** Where the current per-exchange prices live, one field per coin, rewritten
 *  wholesale each tick. The rewrite goes through a temporary key and a rename
 *  so that coins which stopped diverging are pruned rather than left stale, and
 *  a reader can never observe a half-written snapshot. */
export const DECORR_LEGS_HASH = 'decorr:legs';

export function topicFromString(s: string): TopicKey | null {
  const parts = s.split(':');
  if (parts.length < 4 || parts.length > 5) return null;
  const [exchange, marketType, exchangeSymbol, stream, tf] = parts;
  if (!exchange || !marketType || !exchangeSymbol || !stream) return null;
  if (!(STREAM_NAMES as readonly string[]).includes(stream)) return null;
  return { exchange, marketType, exchangeSymbol, stream: stream as StreamName, tf };
}

// The live path: one channel per topic.
export const pubChannel = (topic: string) => `pub:${topic}`;
// The cache that serves a client's first snapshot, plus a sequence number per
// topic so a client can tell whether it missed a required message.
//
// The closed bars and the bar currently forming are separate keys on purpose.
// The closed-bar blob is rewritten once a minute per instrument; the forming
// bar changes on every tick. Merging them would mean rewriting the whole blob
// on every tick, for every instrument — which at this instrument count would
// dominate all other cache traffic.
export const hotKey = (topic: string) => `hot:${topic}`;
export const formingKey = (topic: string) => `hot:${topic}:forming`;
export const seqKey = (topic: string) => `seq:${topic}`;

// The cache shapes. Written by the ingest side, read by the delivery side.
// Keys are short because this is an internal cache format, not a wire format —
// what goes on the wire is a protobuf message.
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
