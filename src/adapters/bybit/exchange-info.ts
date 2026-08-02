// Bybit v5 instruments-info dialect → venue-neutral VenueSymbol[] (Rule 3).
// R1-pinned facts: lotSizeFilter KEY NAMES DIFFER per category (spot:
// minOrderQty/maxOrderQty/minOrderAmt/maxMarketOrderQty; linear:
// minOrderQty/maxOrderQty/maxMktOrderQty/minNotionalValue), spot has NO
// launchTime, linear rows carry contractType (keep LinearPerpetual only —
// dated futures skipped, inverse never fetched: recorded out like COIN-M).
// Bybit reports NO contract addresses → identity = curated-native /
// provisional paths until the contract-enrichment worker fills coins
// addresses. Cursor pagination is PER CATEGORY — cursors never mix.
import {
  normalizeDecimalString,
  type InstrumentInfo,
} from '../../core/instruments.js';
import type { RestDispatcher } from '../../rest/dispatcher.js';
import type { VenueSymbol } from '../../core/venue-symbol.js';
import type { VenueDomainConfig } from '../types.js';
import { unwrapEnvelope, type BybitRestEnvelope } from './dialect.js';

interface RawLotSizeFilter {
  minOrderQty?: string;
  maxOrderQty?: string;
  // spot
  minOrderAmt?: string;
  maxMarketOrderQty?: string;
  // linear
  maxMktOrderQty?: string;
  minNotionalValue?: string;
}

interface RawInstrument {
  symbol?: string;
  baseCoin?: string;
  quoteCoin?: string;
  status?: string;
  contractType?: string;
  launchTime?: string | number;
  priceFilter?: { tickSize?: string };
  lotSizeFilter?: RawLotSizeFilter;
}

function parseInstrumentInfo(raw: RawInstrument, marketType: string): InstrumentInfo {
  const lot = raw.lotSizeFilter;
  const out: InstrumentInfo = {};
  const put = (key: keyof Omit<InstrumentInfo, 'max_orders'>, v: unknown): void => {
    const norm = normalizeDecimalString(v);
    if (norm !== null) out[key] = norm;
  };
  put('tick_size', raw.priceFilter?.tickSize);
  put('min_qty', lot?.minOrderQty);
  put('max_limit_qty', lot?.maxOrderQty);
  // Category dialects (R1): spot maxMarketOrderQty vs linear maxMktOrderQty.
  put('max_market_qty', lot?.maxMktOrderQty ?? lot?.maxMarketOrderQty);
  // min notional: spot = minOrderAmt (quote), linear = minNotionalValue.
  put(
    'min_notional',
    marketType === 'spot' ? lot?.minOrderAmt : lot?.minNotionalValue,
  );
  // Bybit instruments-info carries no price cap/floor ratios, max-orders or
  // liquidation fee — absent stays absent (presence-tracked shape).
  return out;
}

export async function fetchBybitSymbols(
  dispatcher: RestDispatcher,
  cfg: VenueDomainConfig,
): Promise<VenueSymbol[]> {
  const category = cfg.marketType === 'spot' ? 'spot' : 'linear';
  const out: VenueSymbol[] = [];
  let cursor = '';
  let pages = 0;
  do {
    const params: Record<string, string> = { category, limit: '1000' };
    if (cursor !== '') params.cursor = cursor;
    const body = await dispatcher.request<BybitRestEnvelope>(
      cfg.exchangeInfoPath,
      params,
      cfg.exchangeInfoWeight,
    );
    const list = unwrapEnvelope(body, `instruments-info ${category}`) as RawInstrument[];
    for (const raw of list) {
      if (!raw.symbol || !raw.baseCoin || !raw.quoteCoin || !raw.status) continue;
      // Linear domain: only live perpetuals map to market_type 'perpetual'
      // (LinearFutures = dated, skipped — same posture as Binance PERPETUAL).
      if (cfg.marketType === 'perpetual' && raw.contractType !== 'LinearPerpetual') {
        continue;
      }
      const launch = Number(raw.launchTime);
      out.push({
        symbol: raw.symbol,
        base: raw.baseCoin,
        quote: raw.quoteCoin,
        status: raw.status === 'Trading' ? 'active' : 'halted',
        contractAddress: null, // Bybit does not report contract addresses
        network: null,
        listedAt: Number.isFinite(launch) && launch > 0 ? new Date(launch) : null,
        info: parseInstrumentInfo(raw, cfg.marketType),
      });
    }
    cursor = body.result?.nextPageCursor ?? '';
    pages += 1;
  } while (cursor !== '' && pages < 20);
  return out;
}
