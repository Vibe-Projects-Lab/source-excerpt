// The exchange's implementation of the adapter interface, assembled from the
// dialect, the connection pool and the instrument catalogue.
//
// Capabilities are declared as data rather than discovered by asking "is this
// exchange X": this venue carries funding and open interest on its ticker
// stream, so it needs no separate REST polling for them; it publishes no trade
// count and no sub-minute candles.
import type {
  ExchangeAdapter,
  RestKlinesPage,
  RestKlinesReq,
} from '../types.js';
import type { RestDispatcher } from '../../rest/dispatcher.js';
import { bybitDomainConfig } from './index.js';
import {
  klineStreamName,
  parseBybitKlineRows,
  parseStreamName,
  tfToInterval,
  tickerStreamName,
  unwrapEnvelope,
  type BybitRestEnvelope,
} from './dialect.js';
import { BybitConnectionPool } from './pool.js';
import { fetchBybitSymbols } from './exchange-info.js';

export function bybitAdapter(apiDomain: string): ExchangeAdapter {
  const cfg = bybitDomainConfig(apiDomain);
  return {
    cfg,
    capabilities: { deals24h: false, wsMetrics: true, kline1s: false },
    fetchSymbols: (dispatcher: RestDispatcher) => fetchBybitSymbols(dispatcher, cfg),
    async fetchKlines(dispatcher: RestDispatcher, req: RestKlinesReq): Promise<RestKlinesPage> {
      const interval = tfToInterval(req.tf ?? '1m');
      if (!interval) {
        throw new Error(`bybit: unsupported timeframe ${req.tf ?? '1m'}`);
      }
      const params: Record<string, string> = {
        category: cfg.marketType === 'spot' ? 'spot' : 'linear',
        symbol: req.exchangeSymbol,
        interval,
        limit: String(req.limit),
      };
      if (req.startMs !== undefined) params.start = String(req.startMs);
      if (req.endMs !== undefined) params.end = String(req.endMs);
      const body = await dispatcher.request<BybitRestEnvelope>(
        cfg.restKlinesPath,
        params,
        1, // per-IP request counting: every call costs 1
        req.priority,
      );
      const rows = unwrapEnvelope(body, `kline ${req.exchangeSymbol}`);
      return {
        bars: parseBybitKlineRows(rows, {
          tf: req.tf ?? '1m',
          // Bucket-overlap edge bar: keep strict ts>=start semantics
          // so the platform cursors never double-count the edge bucket.
          ...(req.startMs !== undefined ? { sinceMs: req.startMs } : {}),
          ...(req.dropFormingAt !== undefined ? { dropFormingAt: req.dropFormingAt } : {}),
        }),
        rawCount: rows.length,
      };
    },
    klineStreamName: (exchangeSymbol, tf) => klineStreamName(exchangeSymbol, tf),
    // No all-market ticker on Bybit — every instrument co-subscribes its
    // per-symbol tickers stream (aggregated by the pool's ticker engine).
    perInstrumentStreams: (exchangeSymbol) => [tickerStreamName(exchangeSymbol)],
    globalStreams: () => [],
    parseStreamName,
    createPool: (streamsPerConn, registry, cb) =>
      new BybitConnectionPool(cfg, streamsPerConn, registry, cb),
    // wsMetrics venue: no REST poll legs (funding/OI arrive via tickers).
  };
}
