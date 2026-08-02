// The exchange's two API domains as configuration: endpoints, venue limits,
// and this domain's share of the shared request budget.
//
// Both domains talk to one REST host, which is why they must agree on the host
// budget and split it explicitly — see budget-split.ts for why that split is
// declared rather than assumed. Base URLs are overridable by environment so a
// moved hostname never requires a code change.
import { loadEnv } from '../../core/env.js';
import type { VenueDomainConfig } from '../types.js';

const BYBIT_SPOT_DEFAULTS: VenueDomainConfig = {
  apiDomain: 'bybit-spot',
  exchange: 'bybit',
  marketType: 'spot',
  wsBase: 'wss://stream.bybit.com/v5/public/spot',
  restBase: 'https://api.bybit.com',
  restKlinesPath: '/v5/market/kline',
  weightBudgetPerMin: 7200, // api.bybit.com per-IP budget (host budget)
  restRoleBudgetPerMin: { ingest: 1500, jobs: 1000, seed: 500 }, // seed carved out of jobs; host total unchanged
  exchangeInfoPath: '/v5/market/instruments-info',
  exchangeInfoWeight: 1,
  klinesMaxLimit: 1000,
  klinesWeightTiers: [[1000, 1]], // every request costs 1 (per-IP req counting)
  wsSubscribeMsgPerSec: 5,
  maxStreamsPerConn: 300,
  maxArgsPerSubscribe: 10, // v5 cap: ≤10 args per subscribe frame
  restMaxConcurrent: 2, // bound bursts inside the 5s rolling window
};

const BYBIT_LINEAR_DEFAULTS: VenueDomainConfig = {
  ...BYBIT_SPOT_DEFAULTS,
  apiDomain: 'bybit-linear',
  marketType: 'perpetual',
  wsBase: 'wss://stream.bybit.com/v5/public/linear',
};

export function bybitDomainConfig(apiDomain: string): VenueDomainConfig {
  const env = loadEnv();
  switch (apiDomain) {
    case 'bybit-spot':
      return {
        ...BYBIT_SPOT_DEFAULTS,
        wsBase: env.BYBIT_SPOT_WS_BASE ?? BYBIT_SPOT_DEFAULTS.wsBase,
        restBase: env.BYBIT_REST_BASE ?? BYBIT_SPOT_DEFAULTS.restBase,
        wsProxyUrl: env.BYBIT_SPOT_PROXY_URL,
      };
    case 'bybit-linear':
      return {
        ...BYBIT_LINEAR_DEFAULTS,
        wsBase: env.BYBIT_LINEAR_WS_BASE ?? BYBIT_LINEAR_DEFAULTS.wsBase,
        restBase: env.BYBIT_REST_BASE ?? BYBIT_LINEAR_DEFAULTS.restBase,
        wsProxyUrl: env.BYBIT_LINEAR_PROXY_URL,
      };
    default:
      throw new Error(`unknown bybit api domain: ${apiDomain}`);
  }
}
