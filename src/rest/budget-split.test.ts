// The split arithmetic is the fix; these tests are its specification. The
// deployed configurations are checked too, through the same factories the
// processes use, so a new exchange or a re-cut that over-commits a shared host
// fails here as well as at startup.
import { describe, expect, it } from 'vitest';
import { assertRestBudgetSplit, restBudgetUnit, roleBudget } from './budget-split.js';
import type { VenueDomainConfig } from '../adapters/types.js';
import { bybitDomainConfig } from '../adapters/bybit/index.js';

function cfg(over: Partial<VenueDomainConfig>): VenueDomainConfig {
  return {
    apiDomain: 'v-spot',
    restBase: 'https://venue.test',
    weightBudgetPerMin: 100,
    restRoleBudgetPerMin: { ingest: 60, jobs: 30, seed: 10 },
    ...over,
  } as VenueDomainConfig;
}

describe('roleBudget', () => {
  it('returns the declared per-role share', () => {
    const c = cfg({});
    expect(roleBudget(c, 'ingest')).toBe(60);
    expect(roleBudget(c, 'jobs')).toBe(30);
    expect(roleBudget(c, 'seed')).toBe(10);
  });

  it('rejects a missing/non-positive share loudly', () => {
    const c = cfg({ restRoleBudgetPerMin: { ingest: 0, jobs: 30, seed: 10 } });
    expect(() => roleBudget(c, 'ingest')).toThrow(/positive/);
  });
});

describe('restBudgetUnit', () => {
  it('keys on host + outbound IP, defaulting to primary', () => {
    expect(restBudgetUnit('https://api.bybit.com')).toBe('api.bybit.com#primary');
    expect(restBudgetUnit('https://api.bybit.com/v5', 'proxy-jp')).toBe(
      'api.bybit.com#proxy-jp',
    );
  });
});

describe('assertRestBudgetSplit', () => {
  it('passes a host whose shares sum exactly to the budget', () => {
    expect(() => assertRestBudgetSplit([cfg({})])).not.toThrow();
  });

  it('sums across DOMAINS sharing one host — the bybit/okx shape', () => {
    const spot = cfg({ apiDomain: 'v-spot', weightBudgetPerMin: 200 });
    const swap = cfg({ apiDomain: 'v-swap', weightBudgetPerMin: 200 });
    // 2 × (60 + 30 + 10) = 200 — exactly the host budget: fine.
    expect(() => assertRestBudgetSplit([spot, swap])).not.toThrow();
    // A third domain on the same host over-commits: 300 > 200.
    const third = cfg({ apiDomain: 'v-margin', weightBudgetPerMin: 200 });
    expect(() => assertRestBudgetSplit([spot, swap, third])).toThrow(/over-committed/);
  });

  it('rejects domains that disagree on their shared host budget', () => {
    const a = cfg({ apiDomain: 'v-spot', weightBudgetPerMin: 200 });
    const b = cfg({ apiDomain: 'v-swap', weightBudgetPerMin: 300 });
    expect(() => assertRestBudgetSplit([a, b])).toThrow(/disagree/);
  });

  it('keeps hosts independent (binance-spot vs fapi never sum)', () => {
    const spot = cfg({ restBase: 'https://api.venue.test' });
    const fut = cfg({ apiDomain: 'v-fut', restBase: 'https://fapi.venue.test' });
    expect(() => assertRestBudgetSplit([spot, fut])).not.toThrow();
  });

  it('rejects an ingest share whose EFFECTIVE budget (0.9 safety) is under the measured demand floor', () => {
    // 60 × 0.9 = 54 < 55 — steady-state demand alone would exhaust the window.
    const c = cfg({ restIngestFloorPerMin: 55 });
    expect(() => assertRestBudgetSplit([c])).toThrow(/demand floor/);
    expect(() =>
      assertRestBudgetSplit([cfg({ restIngestFloorPerMin: 54 })]),
    ).not.toThrow();
  });

  it('the real deployed split holds (the boot gate, run here too)', () => {
    // In the full system this case loads every venue configuration the fleet
    // deploys and asserts the same invariant the boot gate asserts, so a
    // re-cut that over-commits a shared host fails in CI rather than as a ban
    // from the venue. This excerpt ships one venue, so it can only run the
    // check over that venue's two domains — the mechanism is identical, the
    // coverage is narrower. See README, "What was changed".
    const fleet = [bybitDomainConfig('bybit-spot'), bybitDomainConfig('bybit-linear')];
    expect(() => assertRestBudgetSplit(fleet)).not.toThrow();
    expect(
      bybitDomainConfig('bybit-spot').restRoleBudgetPerMin.ingest +
        bybitDomainConfig('bybit-spot').restRoleBudgetPerMin.jobs +
        bybitDomainConfig('bybit-linear').restRoleBudgetPerMin.ingest +
        bybitDomainConfig('bybit-linear').restRoleBudgetPerMin.jobs,
    ).toBeLessThanOrEqual(bybitDomainConfig('bybit-spot').weightBudgetPerMin);
  });
});
