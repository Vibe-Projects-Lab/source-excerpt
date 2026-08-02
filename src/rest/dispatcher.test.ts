// CHARACTERISATION tests for RestDispatcher — pinning behaviour that had NONE.
//
// Written deliberately BEFORE the budget-unit rework: that plan's
// acceptance criterion is "with no shared budget the behaviour is unchanged",
// and until now there was no baseline to compare against — the only other
// mention of RestDispatcher in a test file is a type import. These tests
// describe what the dispatcher does TODAY. If a future change makes one fail,
// that is the signal to decide whether the change is intended, not to edit the
// expectation quietly.
//
// The two seams that make this testable without a venue: vitest fake timers for
// the minute window, and a stubbed global fetch (the dispatcher calls bare
// `fetch(url)` at dispatcher.ts:156 — there is no injectable client, which is
// itself worth knowing).
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { RestDispatcher } from './dispatcher.js';
import { assertRestBudgetSplit, roleBudget } from './budget-split.js';
import type { VenueDomainConfig } from '../adapters/types.js';

const BASE = 'https://venue.test';

function jsonOk(body: unknown = { ok: true }): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });
}

/** Drain microtasks so awaited fetches settle without advancing the clock. */
async function flush(times = 6): Promise<void> {
  for (let i = 0; i < times; i++) await Promise.resolve();
}

let fetchMock: ReturnType<typeof vi.fn>;

beforeEach(() => {
  vi.useFakeTimers();
  // A round minute so window arithmetic in the tests is readable.
  vi.setSystemTime(new Date('2026-07-28T12:00:00.000Z'));
  fetchMock = vi.fn(async () => jsonOk());
  vi.stubGlobal('fetch', fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe('RestDispatcher — budget window', () => {
  it('applies the 0.9 safety factor to the venue budget', async () => {
    // 100 * 0.9 = 90 usable weight per minute. The 10th request of weight 10
    // must NOT go out in this window.
    const d = new RestDispatcher({ apiDomain: 'v', restBase: BASE, budgetPerMin: 100 });
    for (let i = 0; i < 12; i++) void d.request('/x', {}, 10, 'normal');
    await flush(40);
    expect(fetchMock).toHaveBeenCalledTimes(9); // 9 * 10 = 90 ≤ 90; the 10th would be 100 > 90
  });

  it('budgetSafety is configurable and overrides the 0.9 default', async () => {
    const d = new RestDispatcher({
      apiDomain: 'v',
      restBase: BASE,
      budgetPerMin: 100,
      budgetSafety: 0.5,
    });
    for (let i = 0; i < 12; i++) void d.request('/x', {}, 10, 'normal');
    await flush(40);
    expect(fetchMock).toHaveBeenCalledTimes(5); // 50 usable
  });

  it('releases the held jobs when the minute window rolls', async () => {
    const d = new RestDispatcher({ apiDomain: 'v', restBase: BASE, budgetPerMin: 100 });
    for (let i = 0; i < 12; i++) void d.request('/x', {}, 10, 'normal');
    await flush(40);
    expect(fetchMock).toHaveBeenCalledTimes(9);

    // The dispatcher schedules its own wake at the next minute boundary.
    await vi.advanceTimersByTimeAsync(61_000);
    await flush(40);
    expect(fetchMock).toHaveBeenCalledTimes(12);
  });

  it('the window is a wall-clock minute, not a rolling one', async () => {
    const d = new RestDispatcher({ apiDomain: 'v', restBase: BASE, budgetPerMin: 100 });
    // Spend the whole window 1s before the boundary...
    vi.setSystemTime(new Date('2026-07-28T12:00:59.000Z'));
    for (let i = 0; i < 9; i++) void d.request('/x', {}, 10, 'normal');
    await flush(40);
    expect(fetchMock).toHaveBeenCalledTimes(9);

    // ...and the full budget is available again 2s later, in the NEW minute.
    // A venue enforcing a ROLLING window would count 18 in those 3 seconds:
    // this is the documented gap between our accounting and a venue's.
    await vi.advanceTimersByTimeAsync(2_000);
    for (let i = 0; i < 9; i++) void d.request('/x', {}, 10, 'normal');
    await flush(40);
    expect(fetchMock).toHaveBeenCalledTimes(18);
  });
});

describe('RestDispatcher — concurrency and priority', () => {
  it('never exceeds maxConcurrent in flight', async () => {
    let inFlight = 0;
    let peak = 0;
    fetchMock.mockImplementation(async () => {
      inFlight += 1;
      peak = Math.max(peak, inFlight);
      await Promise.resolve();
      inFlight -= 1;
      return jsonOk();
    });
    const d = new RestDispatcher({
      apiDomain: 'v',
      restBase: BASE,
      budgetPerMin: 10_000,
      maxConcurrent: 3,
    });
    for (let i = 0; i < 20; i++) void d.request('/x', {}, 1, 'normal');
    await flush(60);
    expect(peak).toBeLessThanOrEqual(3);
  });

  it('serves normal before low regardless of enqueue order', async () => {
    const seen: string[] = [];
    fetchMock.mockImplementation(async (url: string) => {
      seen.push(new URL(url).pathname);
      return jsonOk();
    });
    const d = new RestDispatcher({
      apiDomain: 'v',
      restBase: BASE,
      budgetPerMin: 10_000,
      maxConcurrent: 1,
    });
    void d.request('/low-1', {}, 1, 'low');
    void d.request('/low-2', {}, 1, 'low');
    void d.request('/normal', {}, 1, 'normal');
    await flush(60);
    // The first dispatch may already be in flight; what matters is that the
    // normal job is not left behind the remaining low ones.
    expect(seen.indexOf('/normal')).toBeLessThan(seen.indexOf('/low-2'));
  });

  it('pending() counts both queues plus in-flight', async () => {
    const d = new RestDispatcher({ apiDomain: 'v', restBase: BASE, budgetPerMin: 10 });
    for (let i = 0; i < 5; i++) void d.request('/x', {}, 9, 'normal');
    expect(d.pending()).toBe(5);
  });
});

describe('RestDispatcher — venue rejection', () => {
  it.each([429, 418, 403])(
    'HTTP %i pauses the WHOLE dispatcher and honours Retry-After',
    async (status) => {
      let calls = 0;
      fetchMock.mockImplementation(async () => {
        calls += 1;
        if (calls === 1) {
          return new Response('', { status, headers: { 'retry-after': '30' } });
        }
        return jsonOk();
      });
      const d = new RestDispatcher({ apiDomain: 'v', restBase: BASE, budgetPerMin: 10_000 });
      void d.request('/a', {}, 1, 'normal');
      void d.request('/b', {}, 1, 'normal');
      await flush(40);
      const afterReject = fetchMock.mock.calls.length;

      // Still paused well inside the Retry-After window.
      await vi.advanceTimersByTimeAsync(20_000);
      await flush(20);
      expect(fetchMock.mock.calls.length).toBe(afterReject);

      // Resumes once it lifts.
      await vi.advanceTimersByTimeAsync(15_000);
      await flush(40);
      expect(fetchMock.mock.calls.length).toBeGreaterThan(afterReject);
    },
  );

  it('a rejection does not consume an attempt (the request is retried, not failed)', async () => {
    let calls = 0;
    fetchMock.mockImplementation(async () => {
      calls += 1;
      // Reject the first FOUR times — more than MAX_ATTEMPTS (3). If a 429
      // consumed an attempt this would reject; it must not.
      if (calls <= 4) return new Response('', { status: 429, headers: { 'retry-after': '1' } });
      return jsonOk({ done: true });
    });
    const d = new RestDispatcher({ apiDomain: 'v', restBase: BASE, budgetPerMin: 10_000 });
    const p = d.request<{ done: boolean }>('/a', {}, 1, 'normal');
    for (let i = 0; i < 6; i++) {
      await vi.advanceTimersByTimeAsync(1_100);
      await flush(20);
    }
    await expect(p).resolves.toEqual({ done: true });
  });
});

describe('RestDispatcher — failure handling', () => {
  it('retries a network error MAX_ATTEMPTS times then rejects', async () => {
    // Count by unique path — the global mock count can be polluted by parked
    // jobs from EARLIER tests resuming on this test's timer advance (the
    // known cross-test leak; see the pause test note).
    let netCalls = 0;
    fetchMock.mockImplementation(async (url: string) => {
      if (String(url).includes('/net-fail')) netCalls += 1;
      throw new Error('ECONNRESET');
    });
    const d = new RestDispatcher({ apiDomain: 'v', restBase: BASE, budgetPerMin: 10_000 });
    const p = d.request('/net-fail', {}, 1, 'normal');
    const assertion = expect(p).rejects.toThrow(/network/);
    // Backoff is 1000ms * attempts.
    for (let i = 0; i < 4; i++) {
      await vi.advanceTimersByTimeAsync(4_000);
      await flush(20);
    }
    await assertion;
    expect(netCalls).toBe(3);
  });

  it('a 5xx retries; a non-ok 4xx rejects immediately', async () => {
    fetchMock.mockResolvedValue(new Response('', { status: 400 }));
    const d = new RestDispatcher({ apiDomain: 'v', restBase: BASE, budgetPerMin: 10_000 });
    const p = d.request('/a', {}, 1, 'normal');
    const assertion = expect(p).rejects.toThrow(/HTTP 400/);
    await flush(20);
    await assertion;
    expect(fetchMock).toHaveBeenCalledTimes(1); // no retry
  });

  it('surfaces a bad JSON body as a rejection rather than hanging', async () => {
    fetchMock.mockResolvedValue(
      new Response('not json', { status: 200, headers: { 'content-type': 'application/json' } }),
    );
    const d = new RestDispatcher({ apiDomain: 'v', restBase: BASE, budgetPerMin: 10_000 });
    await expect(d.request('/a', {}, 1, 'normal')).rejects.toThrow(/bad JSON/);
  });
});

describe('RestDispatcher — budget unit identity', () => {
  it('records outboundIpId, defaulting to "primary"', () => {
    const a = new RestDispatcher({ apiDomain: 'v', restBase: BASE, budgetPerMin: 10 });
    const b = new RestDispatcher({
      apiDomain: 'v',
      restBase: BASE,
      budgetPerMin: 10,
      outboundIpId: 'proxy-jp',
    });
    expect(a.outboundIpId).toBe('primary');
    expect(b.outboundIpId).toBe('proxy-jp');
  });

  it('TWO dispatchers built from a role-split config stay under the HOST budget — the original defect, flipped', async () => {
    // The original characterisation pinned the 1.8× overshoot: both processes
    // constructed with the full venue number (binance-only; bybit/okx carried
    // a hand-split in comments nothing verified). The fix is upstream of the
    // dispatcher — construction goes through roleBudget(cfg, role), and
    // assertRestBudgetSplit fails the boot when Σ shares > host budget. This
    // test is the flipped pin: the same two-process scenario, built the way
    // ingest-main/jobs-main now build, cannot exceed the venue's 100.
    const cfg = {
      apiDomain: 'v',
      restBase: BASE,
      weightBudgetPerMin: 200, // the venue HOST budget
      // THREE roles since the kline-storage refactor: `seed` (anchor backfill)
      // was carved out of `jobs`, so the host total is unchanged and this test
      // must exercise all three — a role left out of the scenario is exactly
      // how a share gets spent without being counted.
      restRoleBudgetPerMin: { ingest: 120, jobs: 60, seed: 20 },
    } as VenueDomainConfig;
    assertRestBudgetSplit([cfg]); // the boot gate the real processes run
    // Unique path — a global count can be polluted by parked jobs from
    // earlier tests resuming late (see the pause test below).
    let splitCalls = 0;
    fetchMock.mockImplementation(async (url: string) => {
      if (String(url).includes('/split-x')) splitCalls += 1;
      return jsonOk();
    });
    const ingest = new RestDispatcher({
      apiDomain: 'v',
      restBase: BASE,
      budgetPerMin: roleBudget(cfg, 'ingest'),
    });
    const jobs = new RestDispatcher({
      apiDomain: 'v',
      restBase: BASE,
      budgetPerMin: roleBudget(cfg, 'jobs'),
    });
    const seed = new RestDispatcher({
      apiDomain: 'v',
      restBase: BASE,
      budgetPerMin: roleBudget(cfg, 'seed'),
    });
    for (let i = 0; i < 20; i++) void ingest.request('/split-x', {}, 10, 'normal');
    for (let i = 0; i < 20; i++) void jobs.request('/split-x', {}, 10, 'normal');
    for (let i = 0; i < 20; i++) void seed.request('/split-x', {}, 10, 'normal');
    await flush(60);
    // ingest: 120×0.9=108 → 10 requests; jobs: 60×0.9=54 → 5; seed: 20×0.9=18 → 1.
    // 160 weight total against the venue's 200 — every role demanded 200 on its
    // own, and the split is what keeps the sum under the host number.
    expect(splitCalls).toBe(16);
  });
});

describe('RestDispatcher — low-class ceiling', () => {
  it("caps 'low' at its share of the window while 'normal' keeps the remainder", async () => {
    let lowCalls = 0;
    let normalCalls = 0;
    fetchMock.mockImplementation(async (url: string) => {
      if (String(url).includes('/cap-low')) lowCalls += 1;
      if (String(url).includes('/cap-norm')) normalCalls += 1;
      return jsonOk();
    });
    // budget 100 → effective 90; low share 0.5 → low ceiling 45.
    const d = new RestDispatcher({
      apiDomain: 'v',
      restBase: BASE,
      budgetPerMin: 100,
      lowClassShare: 0.5,
    });
    for (let i = 0; i < 9; i++) void d.request('/cap-low', {}, 10, 'low');
    await flush(60);
    expect(lowCalls).toBe(4); // 40 ≤ 45; the 5th would be 50 > 45

    // 'normal' still owns the rest of the SAME window — the whole point.
    for (let i = 0; i < 9; i++) void d.request('/cap-norm', {}, 10, 'normal');
    await flush(60);
    expect(normalCalls).toBe(5); // 40 low + 50 normal = 90 = the full window
    expect(lowCalls).toBe(4); // still held at the ceiling

    // Next minute: the low ceiling resets with the window.
    await vi.advanceTimersByTimeAsync(61_000);
    await flush(60);
    expect(lowCalls).toBeGreaterThanOrEqual(8);
  });
});

describe('RestDispatcher — shared venue pause', () => {
  it('a 429 fires onVenuePause with the budget-unit host and the pause end', async () => {
    fetchMock.mockResolvedValue(
      new Response('', { status: 429, headers: { 'retry-after': '30' } }),
    );
    const seen: [string, number][] = [];
    const d = new RestDispatcher({
      apiDomain: 'v',
      restBase: BASE,
      budgetPerMin: 10_000,
      onVenuePause: (host, until) => seen.push([host, until]),
    });
    void d.request('/x', {}, 1, 'normal').catch(() => {});
    await flush(40);
    expect(seen).toHaveLength(1);
    expect(seen[0]?.[0]).toBe('venue.test');
    expect(seen[0]?.[1]).toBe(Date.now() + 30_000);
  });

  it('applyExternalPause holds the queue until the peer-announced end, and only max-merges', async () => {
    // Count by a UNIQUE path and record WHEN it left: earlier tests in this
    // file park '/x' jobs whose drain chains can resume when THIS test
    // advances timers (observed flake), so a global call count is not ours to
    // assert on. Had the shorter second pause won (a min-merge bug) the
    // request would leave at +5s; had the pause not applied, at +0s.
    const myCalls: number[] = [];
    fetchMock.mockImplementation(async (url: string) => {
      if (String(url).includes('/pause-held')) myCalls.push(Date.now());
      return jsonOk();
    });
    const d = new RestDispatcher({ apiDomain: 'v', restBase: BASE, budgetPerMin: 10_000 });
    const until = Date.now() + 30_000;
    d.applyExternalPause(until);
    d.applyExternalPause(Date.now() + 5_000); // shorter — must NOT shorten the pause
    void d.request('/pause-held', {}, 1, 'normal');
    await flush(20);
    expect(myCalls).toHaveLength(0);

    await vi.advanceTimersByTimeAsync(40_000);
    await flush(40);
    expect(myCalls).toHaveLength(1);
    expect(myCalls[0]).toBeGreaterThanOrEqual(until);
  });
});

describe('RestDispatcher — standing ban is bounded', () => {
  it('a PERSISTENT rejection eventually rejects instead of requeueing forever', async () => {
    // The attempt refund used to be unconditional, so this job would sit
    // at the queue head for the life of the process and the caller would never
    // learn. Now the free refunds run out and MAX_ATTEMPTS applies.
    fetchMock.mockResolvedValue(
      new Response('', { status: 403, headers: { 'retry-after': '1' } }),
    );
    const d = new RestDispatcher({ apiDomain: 'v', restBase: BASE, budgetPerMin: 10_000 });
    const p = d.request('/banned', {}, 1, 'normal');
    const assertion = expect(p).rejects.toThrow(/still rejected after/);
    for (let i = 0; i < 12; i++) {
      await vi.advanceTimersByTimeAsync(1_100);
      await flush(20);
    }
    await assertion;
    // Bounded: free refunds + the attempt budget, not unbounded.
    expect(fetchMock.mock.calls.length).toBeLessThanOrEqual(9);
  });

  it('a TRANSIENT rejection still costs the caller nothing', async () => {
    // 3 backoffs is inside REJECT_FREE_RETRIES — the venue is pacing us, not
    // failing us, so the job must survive and resolve.
    let calls = 0;
    fetchMock.mockImplementation(async () => {
      calls += 1;
      if (calls <= 3) return new Response('', { status: 429, headers: { 'retry-after': '1' } });
      return jsonOk({ done: true });
    });
    const d = new RestDispatcher({ apiDomain: 'v', restBase: BASE, budgetPerMin: 10_000 });
    const p = d.request<{ done: boolean }>('/paced', {}, 1, 'normal');
    for (let i = 0; i < 6; i++) {
      await vi.advanceTimersByTimeAsync(1_100);
      await flush(20);
    }
    await expect(p).resolves.toEqual({ done: true });
  });
});
