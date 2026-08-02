// The exchange's own JSON, normalized into the platform's shapes.
//
// This is where a venue's quirks are absorbed so nothing downstream has to know
// about them. The ones that matter here, each of which is a bug if missed:
//  - a candle frame carries an ARRAY, and a minute boundary can pack the closed
//    bar and its successor into one frame — so iterate every item, never the
//    first;
//  - the frame's own type field always says "snapshot", so closed-ness comes
//    from a separate flag, and the bar's timestamp is its bucket open, not the
//    frame's send time;
//  - numbers arrive as strings, and volume is in base units while turnover is
//    in quote units — the platform's volume field is the quote one, so these
//    two are deliberately swapped on the way in;
//  - the REST history endpoint returns rows newest-first and selects by bucket
//    overlap, so a bar whose bucket merely contains the requested start is
//    included and has to be filtered out;
//  - an empty string is a legitimate value and must become NaN, not zero.
import type { TopicKey } from '../../core/topics.js';
import { tfDurationMs } from '../../core/timeframes.js';
import type {
  NormalizedKline,
  RestKlineBar,
  VenueDomainConfig,
} from '../types.js';

/** Venue-agnostic TF token → Bybit v5 interval. Only '1m' rides the wire in
 *  the first release (derived TFs = TfDeriver); the full map keeps fetchKlines honest
 *  for any caller. NO seconds intervals exist on Bybit (capabilities.kline1s
 *  = false). */
const TF_TO_INTERVAL: Record<string, string> = {
  '1m': '1',
  '3m': '3',
  '5m': '5',
  '15m': '15',
  '30m': '30',
  '1h': '60',
  '2h': '120',
  '4h': '240',
  '6h': '360',
  '12h': '720',
  '1d': 'D',
};

const INTERVAL_TO_TF = new Map(Object.entries(TF_TO_INTERVAL).map(([tf, i]) => [i, tf]));

export function tfToInterval(tf: string): string | null {
  return TF_TO_INTERVAL[tf] ?? null;
}

/** '' and undefined must become NaN. Number('') is 0, which would silently
 *  turn a missing value into a real price. */
const num = (v: unknown): number => {
  if (typeof v === 'number') return v;
  if (typeof v !== 'string' || v === '') return NaN;
  return Number(v);
};

// ---------------------------------------------------------------------------
// WS kline frames
// ---------------------------------------------------------------------------

interface BybitKlineItem {
  start?: number;
  open?: string;
  high?: string;
  low?: string;
  close?: string;
  volume?: string; // BASE units
  turnover?: string; // QUOTE units
  confirm?: boolean;
  interval?: string;
}

interface BybitTopicFrame {
  topic?: string;
  type?: string;
  ts?: number;
  data?: unknown;
}

/** Normalize one kline frame → EVERY contained bar (multi-item frames are
 *  real). Structural rejects drop the ITEM, not the frame. Returns null when
 *  the frame is not a kline frame at all. */
export function normalizeKlineFrame(
  raw: unknown,
  cfg: Pick<VenueDomainConfig, 'exchange' | 'marketType'>,
): NormalizedKline[] | null {
  const msg = raw as BybitTopicFrame;
  if (typeof msg?.topic !== 'string' || !msg.topic.startsWith('kline.')) return null;
  const parts = msg.topic.split('.');
  const interval = parts[1];
  const exchangeSymbol = parts[2];
  if (!interval || !exchangeSymbol || !Array.isArray(msg.data)) return null;
  const tf = INTERVAL_TO_TF.get(interval);
  if (!tf) return null;
  const topic: TopicKey = {
    exchange: cfg.exchange,
    marketType: cfg.marketType,
    exchangeSymbol,
    stream: 'kline',
    tf,
  };
  const out: NormalizedKline[] = [];
  for (const item of msg.data as BybitKlineItem[]) {
    const o = num(item?.open);
    const h = num(item?.high);
    const l = num(item?.low);
    const c = num(item?.close);
    const v = num(item?.volume); // BASE volume — matches Binance k.v semantics
    const ts = typeof item?.start === 'number' ? item.start : NaN;
    if (
      [o, h, l, c, v, ts].some((x) => !Number.isFinite(x)) ||
      o <= 0 ||
      h <= 0 ||
      l <= 0 ||
      c <= 0 ||
      v < 0 ||
      ts <= 0 ||
      ts > Date.now() + 60 * 60 * 1000 // future-ts structural reject
    ) {
      continue;
    }
    out.push({
      topic,
      bar: { ts, o, h, l, c, v },
      closed: item.confirm === true, // the ONLY close signal (type lies)
    });
  }
  return out;
}

// ---------------------------------------------------------------------------
// REST klines
// ---------------------------------------------------------------------------

export interface BybitRestEnvelope {
  retCode?: number;
  retMsg?: string;
  result?: { list?: unknown[]; nextPageCursor?: string };
}

/** Unwrap the v5 envelope; retCode≠0 throws (10006/10018 = rate-limit class,
 *  named in the error for the dispatcher logs). */
export function unwrapEnvelope(body: BybitRestEnvelope, what: string): unknown[] {
  if (body?.retCode !== 0) {
    const code = body?.retCode ?? 'no-retCode';
    const rateLimited = code === 10006 || code === 10018 ? ' [RATE-LIMIT]' : '';
    throw new Error(`bybit ${what} retCode=${code}${rateLimited}: ${body?.retMsg ?? ''}`);
  }
  return body.result?.list ?? [];
}

/**
 * Parse v5 kline rows → ascending RestKlineBar[]. Rows arrive REVERSE
 * chronological; `sinceMs` applies the strict ts>=start filter (bucket-
 * overlap edge bar); `dropFormingAt` drops the unfinished bar.
 */
export function parseBybitKlineRows(
  rows: unknown[],
  opts: { tf?: string; sinceMs?: number; dropFormingAt?: number } = {},
): RestKlineBar[] {
  // Real bucket duration, not a hardcoded minute — see the same note in the
  // binance dialect: at tf='1h' a 60_000 guard keeps the IN-PROGRESS hour and
  // the anchor writers would persist a partial bar as closed. Default '1m'
  // leaves every existing caller byte-identical.
  const barMs = tfDurationMs(opts.tf);
  const bars: RestKlineBar[] = [];
  for (const row of rows) {
    if (!Array.isArray(row)) continue;
    const [tsStr, o, h, l, c, volume] = row as unknown[];
    const bar: RestKlineBar = {
      ts: num(tsStr),
      o: num(o),
      h: num(h),
      l: num(l),
      c: num(c),
      v: num(volume), // BASE volume (turnover = quote, deliberately unused)
    };
    if (!Object.values(bar).every((x) => Number.isFinite(x))) continue;
    if (opts.sinceMs !== undefined && bar.ts < opts.sinceMs) continue;
    if (opts.dropFormingAt !== undefined && bar.ts + barMs > opts.dropFormingAt) {
      continue;
    }
    bars.push(bar);
  }
  bars.reverse(); // newest-first → ascending (platform order)
  return bars;
}

// ---------------------------------------------------------------------------
// Stream names
// ---------------------------------------------------------------------------

export function klineStreamName(exchangeSymbol: string, tf: string): string | null {
  const interval = tfToInterval(tf);
  return interval ? `kline.${interval}.${exchangeSymbol}` : null;
}

export function tickerStreamName(exchangeSymbol: string): string {
  return `tickers.${exchangeSymbol}`;
}

export function parseStreamName(name: string): {
  kind: 'kline' | 'ticker' | 'other';
  exchangeSymbolLower?: string;
} {
  if (name.startsWith('kline.')) {
    const sym = name.split('.')[2];
    return sym ? { kind: 'kline', exchangeSymbolLower: sym.toLowerCase() } : { kind: 'other' };
  }
  if (name.startsWith('tickers.')) {
    const sym = name.split('.')[1];
    return sym ? { kind: 'ticker', exchangeSymbolLower: sym.toLowerCase() } : { kind: 'other' };
  }
  return { kind: 'other' };
}

export { num as bybitNum };
