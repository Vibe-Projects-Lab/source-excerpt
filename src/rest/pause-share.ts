// Share the pause, not the budget.
//
// A rate-limit rejection from an exchange is the exchange's own verdict about
// our address. That makes it the one signal genuinely safe to share between
// processes: our accounting of what we have spent is a model and can be wrong,
// but a rejection is a fact. The pause is published, and peers merge it into
// the check their request loop already performs.
//
// Nothing asynchronous enters the request path. If the message bus is
// unavailable the publish is dropped and every process falls back to exactly
// its own local behaviour, which is what it did before this file existed.
//
// One process may run several dispatchers against the same host, because an
// exchange's spot and futures domains usually share one. Routing the pause by
// host therefore pauses the sibling too.
import type { Redis } from 'ioredis';
import type { RestDispatcher } from './dispatcher.js';

export const REST_PAUSE_CHANNEL = 'rest:pause';

export interface RestPauseMsg {
  host: string;
  /** Epoch ms the pause lifts (dispatcher clock of the announcing process —
   *  same box today; a few ms of skew only lengthens the pause). */
  until: number;
}

export function encodeRestPause(host: string, untilMs: number): string {
  return JSON.stringify({ host, until: untilMs } satisfies RestPauseMsg);
}

/** Apply one wire message to the dispatchers whose budget unit it names.
 *  Malformed input is ignored by contract (best-effort channel — a bad frame
 *  must never take down ingest). Returns how many dispatchers were touched
 *  (test seam). */
export function applyRestPauseMessage(
  dispatchers: readonly RestDispatcher[],
  raw: string,
): number {
  let msg: RestPauseMsg;
  try {
    msg = JSON.parse(raw) as RestPauseMsg;
  } catch {
    return 0;
  }
  if (typeof msg?.host !== 'string' || !Number.isFinite(msg.until)) return 0;
  let applied = 0;
  for (const d of dispatchers) {
    if (d.restHost !== msg.host) continue;
    d.applyExternalPause(msg.until);
    applied += 1;
  }
  return applied;
}

/** Wire a set of dispatchers into the shared-pause channel.
 *  - `publisher`: any live command connection (publish is fire-and-forget).
 *  - `subscriber`: a DEDICATED connection (ioredis: a subscribed connection
 *    cannot issue commands). Reconnects ride ioredis' own retry.
 *  Returns the publish hook to hand to DispatcherConfig.onVenuePause. */
export function wireRestPauseShare(opts: {
  publisher: Redis;
  subscriber: Redis;
  dispatchers: readonly RestDispatcher[];
  log?: (msg: string) => void;
}): { publishPause: (host: string, untilMs: number) => void } {
  const { publisher, subscriber, dispatchers, log } = opts;
  void subscriber
    .subscribe(REST_PAUSE_CHANNEL)
    .catch((err) => log?.(`rest pause-share subscribe failed: ${String(err)}`));
  subscriber.on('message', (channel: string, raw: string) => {
    if (channel !== REST_PAUSE_CHANNEL) return;
    applyRestPauseMessage(dispatchers, raw);
  });
  return {
    publishPause: (host, untilMs) => {
      // Self-delivery is harmless: the announcer is already paused at least
      // this far, so max-merge is a no-op there.
      void publisher
        .publish(REST_PAUSE_CHANNEL, encodeRestPause(host, untilMs))
        .catch((err) => log?.(`rest pause-share publish failed: ${String(err)}`));
    },
  };
}
