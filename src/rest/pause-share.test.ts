// The message-application seam, tested without a message bus. The wiring is a
// subscription and a callback; what has to be right is routing a pause to the
// dispatchers it applies to, and surviving malformed input on a best-effort
// channel.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { RestDispatcher } from './dispatcher.js';
import { applyRestPauseMessage, encodeRestPause } from './pause-share.js';

let fetchMock: ReturnType<typeof vi.fn>;

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date('2026-07-28T12:00:00.000Z'));
  fetchMock = vi.fn(
    async () =>
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
  );
  vi.stubGlobal('fetch', fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

async function flush(times = 20): Promise<void> {
  for (let i = 0; i < times; i++) await Promise.resolve();
}

describe('applyRestPauseMessage', () => {
  it('pauses exactly the dispatchers on the announced host — including same-process siblings', async () => {
    const bybitSpot = new RestDispatcher({
      apiDomain: 'bybit-spot',
      restBase: 'https://api.bybit.com',
      budgetPerMin: 1000,
    });
    const bybitLinear = new RestDispatcher({
      apiDomain: 'bybit-linear',
      restBase: 'https://api.bybit.com',
      budgetPerMin: 1000,
    });
    const okx = new RestDispatcher({
      apiDomain: 'okx-spot',
      restBase: 'https://www.okx.com',
      budgetPerMin: 1000,
    });
    const all = [bybitSpot, bybitLinear, okx];

    // Deterministic form: record WHEN each request left — a paused host
    // releasing early shows as a bybit call stamped before `until`.
    const calls: { url: string; at: number }[] = [];
    fetchMock.mockImplementation(async (url: string) => {
      calls.push({ url: String(url), at: Date.now() });
      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    });

    const until = Date.now() + 60_000;
    const applied = applyRestPauseMessage(all, encodeRestPause('api.bybit.com', until));
    expect(applied).toBe(2); // both bybit domains, never okx

    void bybitSpot.request('/a', {}, 1);
    void bybitLinear.request('/b', {}, 1);
    void okx.request('/c', {}, 1);
    await flush();
    // Only the un-paused host went out immediately.
    expect(calls.map((c) => c.url)).toEqual(['https://www.okx.com/c']);

    await vi.advanceTimersByTimeAsync(61_000);
    await flush(40);
    expect(fetchMock).toHaveBeenCalledTimes(3);
    for (const c of calls.filter((c) => c.url.includes('bybit'))) {
      expect(c.at).toBeGreaterThanOrEqual(until);
    }
  });

  it('ignores malformed frames and foreign hosts (best-effort channel contract)', () => {
    const d = new RestDispatcher({
      apiDomain: 'v',
      restBase: 'https://venue.test',
      budgetPerMin: 1000,
    });
    expect(applyRestPauseMessage([d], 'not json')).toBe(0);
    expect(applyRestPauseMessage([d], '{"host":42,"until":1}')).toBe(0);
    expect(applyRestPauseMessage([d], '{"host":"venue.test"}')).toBe(0);
    expect(
      applyRestPauseMessage([d], encodeRestPause('other.test', Date.now() + 60_000)),
    ).toBe(0);
  });
});
