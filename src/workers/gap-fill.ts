// Detecting a hole in stored history and repairing it.
//
// Fetch from the watermark forward through the budgeted request queue, and feed
// the closed bars into the same write path the live stream uses — which ends in
// an insert that does nothing on conflict. Re-running is therefore a no-op by
// construction, and that is what makes it safe to trigger generously.
//
// And it is triggered generously: at startup, on every reconnect, on every
// connection reassignment, and whenever the writer notices a watermark falling
// behind. A socket that went away is assumed to have cost us bars until proven
// otherwise.
//
// Instruments with no watermark at all are skipped. An instrument with no
// history is the bulk backfill's problem, not this one's.
import type { Redis } from 'ioredis';
import type { Counter } from 'prom-client';
import { PERSIST_STREAM, persistEntryToFields, watermarkKey } from '../core/persist.js';
import type { ExchangeAdapter, RestKlineBar } from '../adapters/types.js';
import type { RestDispatcher, RestPriority } from '../rest/dispatcher.js';

export interface GapFillMetrics {
  runs: Counter<'api_domain'>;
  barsFilled: Counter<'api_domain'>;
  instrumentsHealed: Counter<'api_domain'>;
  /** runs>0 with no bars is ambiguous — idle or 100% failing. */
  failures: Counter<'api_domain'>;
}

/** Same value shape as the dialect's REST bar — alias, not a redeclaration
 *  (structural typing hid the conceptual identity). */
export type GapFillBar = RestKlineBar;

export interface GapFillDeps {
  redis: Redis;
  dispatcher: RestDispatcher;
  adapter: ExchangeAdapter;
  streamMaxlen: number;
  metrics?: GapFillMetrics;
  /**
   * Called per fetched page AFTER journaling — the ingest host merges the
   * healed bars into its in-memory topic state + Redis hot cache (one REST
   * fetch serves both the journal and the snapshot path).
   */
  onBars?: (instrumentId: string, bars: GapFillBar[]) => void | Promise<void>;
}

export interface GapFillTarget {
  instrumentId: string;
  exchangeSymbol: string;
  /**
   * Pre-outage position captured AT TRIGGER TIME (adapter-start: watermark
   * read BEFORE subscribing; reconnect: the topic's frozen lastClosedTs;
   * watermark-lag request: the stale watermark the persist-worker saw).
   * Without it, a live close that lands before this sweep reaches the
   * instrument advances the watermark PAST the hole and the gap-fill no-ops
   * — the failure a kill test exposed, where most instruments went unhealed.
   */
  sinceMs?: number;
  /**
   * EXCLUSIVE upper bound of a KNOWN hole — the openTime of the first
   * bar that already exists past it (the live close whose contiguity jump
   * exposed the hole). Bars fetched: (sinceMs, untilMs) exclusive on both
   * ends. Without the bound, a past-hole cursor would walk since → now and
   * re-fetch days per instrument — the exact mechanism that produced the
   * start-up heal that re-fetched years of history to write almost nothing new.
   * Absent = heal to the live
   * edge (reconnect / adapter-start / stale-poll semantics, unchanged).
   */
  untilMs?: number;
}

/** Runaway guard. A real reconnect/outage gap is
   *  bounded (cold instruments are skipped → bootstrap; delisted/halted are not
   *  gap-fill targets), so this only trips on a degenerate cursor. At the venue
   *  max limit it spans years of 1m bars — well past any real gap — so stopping
   *  there and leaving the rest to verify is the safe failure, not data loss. */
const GAPFILL_MAX_PAGES = 5000;

/** Heal one instrument; returns bars journaled. */
async function gapFillOne(
  deps: GapFillDeps,
  target: GapFillTarget,
  priority: RestPriority,
): Promise<number> {
  const since =
    target.sinceMs !== undefined && target.sinceMs > 0
      ? target.sinceMs
      : Number(await deps.redis.get(watermarkKey(target.instrumentId)));
  if (!Number.isFinite(since) || since <= 0) return 0; // cold → bootstrap
  // a bounded target stops at the hole's far edge instead of the live one.
  const until = target.untilMs;
  let cursor = since + 60_000;
  let filled = 0;
  let pages = 0;

  for (;;) {
    const now = Date.now();
    if (cursor >= now - 60_000) break; // nothing closed past the cursor
    if (until !== undefined && cursor >= until) break; // hole healed
    if (++pages > GAPFILL_MAX_PAGES) {
      console.error(
        `gap-fill: ${target.exchangeSymbol} exceeded ${GAPFILL_MAX_PAGES} pages ` +
          `from ${since} — stopping (verify backstops the remainder)`,
      );
      break;
    }
    // Gap-sized request: a 3-minute blip costs weight 1, not the max-limit
    // tier — keeps routine heals and blind-domain stale-polls cheap. A
    // bounded hole sizes to ITS width, not the distance to now.
    const sizeEdge = until !== undefined ? Math.min(now, until) : now;
    const gapMinutes = Math.ceil((sizeEdge - cursor) / 60_000);
    // Deep-history paging: once this window's START is older than the
    // venue's deep-history horizon, the adapter serves it from the deeper endpoint
    // whose page cap is SMALLER. Size the window to that cap so
    // the request never over-reaches the page and strands the window's oldest bars
    // — the exact silent-hole class the endMs windowing itself closes.
    const dh = deps.adapter.cfg.deepHistory;
    const maxLimit =
      dh && cursor < now - dh.horizonMs ? dh.maxLimit : deps.adapter.cfg.klinesMaxLimit;
    const limit = Math.max(10, Math.min(maxLimit, gapMinutes + 5));
    // Bound EACH page as a half-open window [cursor,
    // windowEnd) via endMs, so a venue that paginates NEWEST-first can't return
    // only the newest page and strand older bars in a multi-page gap. endMs is
    // the INCLUSIVE last-bar openTime of this page (unified adapter convention:
    // Binance endTime / Bybit end / OKX after=endMs+1). We advance by the WHOLE
    // window — never by the last returned bar — so a sparse or empty window
    // (a genuinely-missing sub-range) can't stall the walk or loop it.
    const windowEnd = cursor + limit * 60_000; // exclusive; next window's start
    // never request past the hole's far edge — the bar AT untilMs exists.
    const endMs = Math.min(
      windowEnd - 60_000,
      now,
      until !== undefined ? until - 60_000 : Number.POSITIVE_INFINITY,
    );
    const { bars } = await deps.adapter.fetchKlines(deps.dispatcher, {
      exchangeSymbol: target.exchangeSymbol,
      startMs: cursor,
      endMs,
      limit,
      priority,
      dropFormingAt: now,
    });
    if (bars.length > 0) {
      // Canary: a bar outside the requested half-open window means
      // an adapter endMs bound/direction mismatch — journal what we got (still
      // idempotent) but surface the misconfiguration.
      if (bars.some((b) => b.ts < cursor || b.ts >= windowEnd)) {
        console.error(
          `gap-fill: ${target.exchangeSymbol} returned bars outside ` +
            `[${cursor}, ${windowEnd}) — check adapter endMs semantics`,
        );
      }
      const pipe = deps.redis.pipeline();
      for (const bar of bars) {
        pipe.xadd(
          PERSIST_STREAM,
          'MAXLEN',
          '~',
          String(deps.streamMaxlen),
          '*',
          ...persistEntryToFields({ instrumentId: target.instrumentId, ...bar }),
        );
      }
      const results = await pipe.exec();
      // A failed XADD must not be treated as journaled: abort
      // this instrument — the per-target catch logs it and verify
      // remains the safety net.
      const firstErr = results?.find(([err]) => err)?.[0];
      if (firstErr) throw firstErr;
      await deps.onBars?.(target.instrumentId, bars);
      filled += bars.length;
    }
    cursor = windowEnd; // deterministic window advance
  }
  return filled;
}

/** Heal a set of instruments (one reconnect/reassignment batch). */
export async function gapFillInstruments(
  deps: GapFillDeps,
  targets: GapFillTarget[],
  priority: RestPriority = 'normal',
): Promise<{ barsFilled: number; instrumentsHealed: number; failures: number }> {
  deps.metrics?.runs.inc({ api_domain: deps.adapter.cfg.apiDomain });
  let barsFilled = 0;
  let instrumentsHealed = 0;
  let failures = 0;
  for (const target of targets) {
    try {
      const filled = await gapFillOne(deps, target, priority);
      if (filled > 0) {
        barsFilled += filled;
        instrumentsHealed += 1;
        deps.metrics?.barsFilled.inc({ api_domain: deps.adapter.cfg.apiDomain }, filled);
      }
    } catch (err) {
      // One instrument failing must not abort the sweep; verify
      // catches anything left behind. count it — a silently-failing sweep
      // used to look identical to an idle one.
      failures += 1;
      console.error(`gap-fill failed for ${target.exchangeSymbol}`, err);
    }
  }
  if (failures > 0) {
    deps.metrics?.failures.inc({ api_domain: deps.adapter.cfg.apiDomain }, failures);
  }
  if (instrumentsHealed > 0) {
    deps.metrics?.instrumentsHealed.inc(
      { api_domain: deps.adapter.cfg.apiDomain },
      instrumentsHealed,
    );
  }
  return { barsFilled, instrumentsHealed, failures };
}
