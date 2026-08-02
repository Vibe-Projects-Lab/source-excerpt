// The cursor is the load-bearing behaviour here. The database client is mocked
// so the test can capture the query itself and assert the cutoff and the row
// limit — no live database required.
import { describe, expect, it } from 'vitest';
import type { Sql } from 'postgres';
import { closedTfBars } from './history-reader.js';

function mockClient(): { client: Sql; queries: string[] } {
  const queries: string[] = [];
  // client.unsafe(sql) is the only method the reader calls.
  const unsafe = (sql: string): Promise<unknown[]> => {
    queries.push(sql);
    return Promise.resolve([]);
  };
  return { client: { unsafe } as unknown as Sql, queries };
}

const ISO = (ms: number) => new Date(ms).toISOString();

describe('closedTfBars before-cursor', () => {
  it('without a cursor: upper bound is the current bucket floor (never the forming bar)', async () => {
    const { client, queries } = mockClient();
    await closedTfBars(client, '1m', { limit: 50 });
    const q = queries[0]!;
    // 1m floor is the top of the current minute — the query cuts strictly below.
    const floor = Math.floor(Date.now() / 60_000) * 60_000;
    expect(q).toContain(`ts < '${ISO(floor)}'`);
    expect(q).toContain('rn <= 50');
  });

  it('with a cursor OLDER than the floor: upper bound becomes the cursor (pages older)', async () => {
    const { client, queries } = mockClient();
    const before = Math.floor((Date.now() - 3 * 3600_000) / 60_000) * 60_000; // 3h ago, bucket-aligned
    await closedTfBars(client, '1m', { limit: 300, beforeMs: before });
    expect(queries[0]!).toContain(`ts < '${ISO(before)}'`);
  });

  it('with a cursor in the FUTURE: still clamped to the floor (min(floor, before))', async () => {
    const { client, queries } = mockClient();
    const future = Date.now() + 3600_000;
    await closedTfBars(client, '1m', { beforeMs: future });
    const floor = Math.floor(Date.now() / 60_000) * 60_000;
    expect(queries[0]!).toContain(`ts < '${ISO(floor)}'`);
  });

  it('derived TF (5m) uses time_bucket aggregation with the cursor cutoff', async () => {
    const { client, queries } = mockClient();
    const before = Math.floor((Date.now() - 86_400_000) / 300_000) * 300_000;
    await closedTfBars(client, '5m', { limit: 100, beforeMs: before });
    const q = queries[0]!;
    expect(q).toContain("time_bucket(INTERVAL '5 minutes', ts)");
    expect(q).toContain(`time_bucket(INTERVAL '5 minutes', ts) < '${ISO(before)}'`);
    expect(q).toContain('rn <= 100');
  });

  it('rejects a non-UUID instrument id (injection guard)', async () => {
    const { client } = mockClient();
    await expect(closedTfBars(client, '1m', { instrumentIds: ['not-a-uuid'] })).rejects.toThrow(
      /non-UUID/,
    );
  });

  it('a valid UUID filter is interpolated as a uuid[] ANY clause', async () => {
    const { client, queries } = mockClient();
    const id = '019f20a3-066b-73a2-99b0-ee0fbd031464';
    await closedTfBars(client, '1m', { instrumentIds: [id] });
    expect(queries[0]!).toContain(`instrument_id = ANY('{${id}}'::uuid[])`);
  });
});
