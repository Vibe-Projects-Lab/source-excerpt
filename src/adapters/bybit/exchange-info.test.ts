// instruments-info dialect: per-category key differences (R1), contractType
// filter, cursor pagination, launchTime, Trading→active mapping.
import { describe, expect, it, vi } from 'vitest';
import type { RestDispatcher } from '../../rest/dispatcher.js';
import { bybitDomainConfig } from './index.js';
import { fetchBybitSymbols } from './exchange-info.js';

const SPOT_ITEM = {
  symbol: 'BTCUSDT',
  baseCoin: 'BTC',
  quoteCoin: 'USDT',
  status: 'Trading',
  priceFilter: { tickSize: '0.01' },
  lotSizeFilter: {
    basePrecision: '0.000001',
    minOrderQty: '0.000048',
    maxOrderQty: '71.7',
    minOrderAmt: '1',
    maxOrderAmt: '4000000',
    maxMarketOrderQty: '30.6',
  },
};

const LINEAR_PERP = {
  symbol: 'BTCUSDT',
  baseCoin: 'BTC',
  quoteCoin: 'USDT',
  status: 'Trading',
  contractType: 'LinearPerpetual',
  launchTime: '1585526400000',
  priceFilter: { tickSize: '0.1' },
  lotSizeFilter: {
    minOrderQty: '0.001',
    maxOrderQty: '1190',
    maxMktOrderQty: '119',
    minNotionalValue: '5',
    qtyStep: '0.001',
  },
};

const LINEAR_DATED = { ...LINEAR_PERP, symbol: 'BTC-27FEB26', contractType: 'LinearFutures' };

function dispatcherOf(pages: unknown[]): RestDispatcher {
  const request = vi.fn();
  for (const p of pages) request.mockResolvedValueOnce(p);
  return { request } as unknown as RestDispatcher;
}

describe('fetchBybitSymbols', () => {
  it('spot: maps keys (minOrderAmt→min_notional, maxMarketOrderQty) + no launchTime', async () => {
    const d = dispatcherOf([
      { retCode: 0, result: { list: [SPOT_ITEM], nextPageCursor: '' } },
    ]);
    const out = await fetchBybitSymbols(d, bybitDomainConfig('bybit-spot'));
    expect(out).toHaveLength(1);
    const s = out[0]!;
    expect(s.status).toBe('active');
    expect(s.listedAt).toBeNull();
    expect(s.contractAddress).toBeNull();
    expect(s.info).toEqual({
      tick_size: '0.01',
      min_qty: '0.000048',
      max_limit_qty: '71.7',
      max_market_qty: '30.6',
      min_notional: '1',
    });
  });

  it('linear: LinearPerpetual only; minNotionalValue + maxMktOrderQty + launchTime', async () => {
    const d = dispatcherOf([
      {
        retCode: 0,
        result: { list: [LINEAR_PERP, LINEAR_DATED], nextPageCursor: '' },
      },
    ]);
    const out = await fetchBybitSymbols(d, bybitDomainConfig('bybit-linear'));
    expect(out.map((s) => s.symbol)).toEqual(['BTCUSDT']); // dated skipped
    const s = out[0]!;
    expect(s.listedAt?.getTime()).toBe(1585526400000);
    expect(s.info.min_notional).toBe('5');
    expect(s.info.max_market_qty).toBe('119');
  });

  it('cursor pagination walks pages until empty cursor', async () => {
    const d = dispatcherOf([
      { retCode: 0, result: { list: [SPOT_ITEM], nextPageCursor: 'abc' } },
      {
        retCode: 0,
        result: {
          list: [{ ...SPOT_ITEM, symbol: 'ETHUSDT', baseCoin: 'ETH' }],
          nextPageCursor: '',
        },
      },
    ]);
    const out = await fetchBybitSymbols(d, bybitDomainConfig('bybit-spot'));
    expect(out.map((s) => s.symbol)).toEqual(['BTCUSDT', 'ETHUSDT']);
  });

  it('non-Trading status maps to halted; retCode!=0 throws', async () => {
    const d = dispatcherOf([
      {
        retCode: 0,
        result: { list: [{ ...SPOT_ITEM, status: 'PreLaunch' }], nextPageCursor: '' },
      },
    ]);
    const out = await fetchBybitSymbols(d, bybitDomainConfig('bybit-spot'));
    expect(out[0]!.status).toBe('halted');
    const bad = dispatcherOf([{ retCode: 10001, retMsg: 'params error' }]);
    await expect(fetchBybitSymbols(bad, bybitDomainConfig('bybit-spot'))).rejects.toThrow(
      /retCode=10001/,
    );
  });
});
