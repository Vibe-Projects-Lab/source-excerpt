// Bybit v5 API domains (venue #2). ONE REST domain (api.bybit.com), WS
// endpoints per market — the F4 capability matrix's own description; two
// ingest processes (spot / linear) match the platform's one-(exchange,
// market)-per-process model. The ONLY cross-process invariant is the SHARED
// per-IP REST budget: official cap 600 req/5s = 7200/min; FOUR dispatchers
// share this IP (2 ingest + 2 jobs) → 1500/min each = 6000/min total, ≈450/5s
// after the dispatcher's 0.9 safety — 25% headroom. made this split a
// DECLARED invariant instead of a comment: weightBudgetPerMin now carries the
// 7200 host budget, restRoleBudgetPerMin the 1500 shares (same deployed
// numbers, now boot-asserted). Base URLs env-overridable (F4: alternate
// domains are config).
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
