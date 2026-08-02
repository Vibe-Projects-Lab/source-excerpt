// REST dispatcher. The unit of budget is host(restBase) + outbound IP, not the
// venue and not the process. The IP pool starts with ONE entry ('primary'):
// adding an address or an API key later is a configuration row, not a rewrite.
//
// ⚠ A single dispatcher cannot exceed ITS OWN window by construction — but the
// venue counts per host and IP across ALL our processes, and more than one
// process talks to the same venue. So `budgetPerMin` here is this dispatcher's
// declared SHARE (see budget-split.ts), never the venue's host budget. Handing
// the full venue number to two processes is how a fleet overshoots a limit it
// believes it cannot exceed; the split arithmetic is therefore asserted at
// every boot, so configuration drift crashes a process instead of earning a
// ban later.
//
// Priority classes: 'low' = bulk history sweeps; 'normal' = listing sync, gap
// healing, cache warmup. Normal always drains first.
//
// Cross-process coordination is deliberately limited to the PAUSE. A 429, 418
// or 403 is venue ground truth — unlike our own estimated weight table, which
// is a model — so it is shared over Redis pub/sub (pause-share.ts) and consumed
// by the existing synchronous `pausedUntil` check. No await ever enters
// drain(), and a Redis outage degrades to exactly the per-process behaviour.
import type { Counter, Gauge } from 'prom-client';

export type RestPriority = 'normal' | 'low';

export interface DispatcherMetrics {
  requests: Counter<'api_domain' | 'priority'>;
  /** rest_host = URL host of restBase — the budget-unit label, so alerting
   *  rules can sum egress per venue HOST across every domain that shares it. */
  weightUsed: Counter<'api_domain' | 'rest_host'>;
  http429: Counter<'api_domain'>;
  queueDepth: Gauge<'api_domain' | 'priority'>;
}

export interface DispatcherConfig {
  apiDomain: string;
  restBase: string;
  /** THIS dispatcher's weight-per-minute share — roleBudget(cfg, role), never
   *  the venue's whole weightBudgetPerMin. A shared field name once let one
   *  venue's full number reach two processes at once: a 1.8x overshoot that
   *  every local invariant reported as healthy. */
  budgetPerMin: number;
  /** Fraction of the share the dispatcher permits itself. Alerting pages at
   *  80% utilization, so the default stays under the alarm and leaves headroom
   *  for the warmups a user can actually see. */
  budgetSafety?: number;
  maxConcurrent?: number;
  /** Ceiling for the 'low' class as a share of THIS dispatcher's effective
   *  budget (0-1; default 1 = uncapped). 'normal' always has the whole window;
   *  'low' — metric sweeps and bulk history — stops at its share, so a hungry
   *  sweep can never starve a reconnect heal. Set on the ingest dispatchers;
   *  the background worker stays uncapped because it issues everything at
   *  'low' and has nothing to starve. */
  lowClassShare?: number;
  /** Recorded from day one, so a multi-IP pool is a configuration change. */
  outboundIpId?: string;
  /** Pause-share hook: fired when THIS dispatcher receives a venue rejection
   *  (429/418/403), carrying the budget-unit host and the end of the pause.
   *  Wire it to a Redis publish; peers apply it via applyExternalPause. */
  onVenuePause?: (restHost: string, untilMs: number) => void;
}

interface Job {
  path: string;
  params: Record<string, string>;
  weight: number;
  priority: RestPriority;
  attempts: number;
  /** Venue backoffs seen by THIS job (429/418/403), counted separately from
   *  `attempts` so a transient pace-limit stays free but a standing ban cannot
   *  requeue forever. */
  rejections: number;
  resolve: (body: unknown) => void;
  reject: (err: Error) => void;
}

const MAX_ATTEMPTS = 3;
/** How many venue backoffs a single job may absorb for free before they start
 *  consuming its attempt budget. Covers an ordinary Retry-After pace limit, and
 *  a venue ban window on the order of ten minutes, without letting a standing
 *  ban loop forever. */
const REJECT_FREE_RETRIES = 5;

export class RestDispatcher {
  readonly apiDomain: string;
  readonly outboundIpId: string;
  /** URL host of restBase — the budget-unit key, and the pause-share route. */
  readonly restHost: string;

  private readonly queues: Record<RestPriority, Job[]> = { normal: [], low: [] };
  private readonly budget: number;
  /** 'low'-class ceiling within the minute window. */
  private readonly lowBudget: number;
  private readonly maxConcurrent: number;
  private windowStart = 0;
  private windowWeight = 0;
  private windowWeightLow = 0;
  private inFlight = 0;
  private pausedUntil = 0;
  private wakeTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    private readonly cfg: DispatcherConfig,
    private readonly metrics?: DispatcherMetrics,
  ) {
    this.apiDomain = cfg.apiDomain;
    this.outboundIpId = cfg.outboundIpId ?? 'primary';
    this.restHost = new URL(cfg.restBase).host;
    this.budget = Math.floor(cfg.budgetPerMin * (cfg.budgetSafety ?? 0.9));
    this.lowBudget = Math.floor(this.budget * (cfg.lowClassShare ?? 1));
    this.maxConcurrent = cfg.maxConcurrent ?? 4;
  }

  /** Pause-share consume: a peer process saw a venue rejection on our budget
   *  unit. Max-merge only — the pause never shortens. Synchronous by design:
   *  drain() reads `pausedUntil` on its own tick and reschedules, so there is
   *  no timer churn and nothing asynchronous touches the request path. */
  applyExternalPause(untilMs: number): void {
    if (untilMs > this.pausedUntil) this.pausedUntil = untilMs;
  }

  request<T>(
    path: string,
    params: Record<string, string>,
    weight: number,
    priority: RestPriority = 'normal',
  ): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      this.queues[priority].push({
        path,
        params,
        weight,
        priority,
        attempts: 0,
        rejections: 0,
        resolve: resolve as (body: unknown) => void,
        reject,
      });
      this.updateQueueGauges();
      this.drain();
    });
  }

  /** Depth across both queues (used by shutdown/tests). */
  pending(): number {
    return this.queues.normal.length + this.queues.low.length + this.inFlight;
  }

  private updateQueueGauges(): void {
    this.metrics?.queueDepth.set(
      { api_domain: this.apiDomain, priority: 'normal' },
      this.queues.normal.length,
    );
    this.metrics?.queueDepth.set(
      { api_domain: this.apiDomain, priority: 'low' },
      this.queues.low.length,
    );
  }

  private rollWindow(now: number): void {
    const window = Math.floor(now / 60_000);
    if (window !== this.windowStart) {
      this.windowStart = window;
      this.windowWeight = 0;
      this.windowWeightLow = 0;
    }
  }

  private scheduleWake(at: number): void {
    if (this.wakeTimer) return;
    const delay = Math.max(at - Date.now(), 50);
    this.wakeTimer = setTimeout(() => {
      this.wakeTimer = null;
      this.drain();
    }, delay);
  }

  private drain(): void {
    const now = Date.now();
    if (now < this.pausedUntil) {
      this.scheduleWake(this.pausedUntil);
      return;
    }
    this.rollWindow(now);
    while (this.inFlight < this.maxConcurrent) {
      const job = this.queues.normal[0] ?? this.queues.low[0];
      if (!job) return;
      if (this.windowWeight + job.weight > this.budget) {
        // Budget window exhausted — wake at the next minute boundary.
        this.scheduleWake((this.windowStart + 1) * 60_000);
        return;
      }
      if (
        job.priority === 'low' &&
        this.windowWeightLow + job.weight > this.lowBudget
      ) {
        // The 'low' class hit its ceiling — hold it for the next window and
        // leave this window's remainder to any 'normal' work that arrives. A
        // reconnect heal must never queue behind a metrics sweep.
        this.scheduleWake((this.windowStart + 1) * 60_000);
        return;
      }
      this.queues[job.priority].shift();
      this.updateQueueGauges();
      this.windowWeight += job.weight;
      if (job.priority === 'low') this.windowWeightLow += job.weight;
      this.metrics?.weightUsed.inc(
        { api_domain: this.apiDomain, rest_host: this.restHost },
        job.weight,
      );
      this.inFlight += 1;
      void this.execute(job).finally(() => {
        this.inFlight -= 1;
        this.drain();
      });
    }
  }

  private async execute(job: Job): Promise<void> {
    job.attempts += 1;
    const qs = new URLSearchParams(job.params).toString();
    const url = `${this.cfg.restBase}${job.path}${qs ? `?${qs}` : ''}`;
    let res: Response;
    try {
      res = await fetch(url);
    } catch (err) {
      this.retryOrFail(job, new Error(`${this.apiDomain} network: ${String(err)}`));
      return;
    }
    this.metrics?.requests.inc({
      api_domain: this.apiDomain,
      priority: job.priority,
    });
    if (res.status === 429 || res.status === 418 || res.status === 403) {
      // Venue backoff: honour Retry-After and requeue at the FRONT, so the
      // same request goes out first when the pause lifts. 403 belongs in this
      // set because at least one venue answers an IP ban with 403 rather than
      // 429; without the pause such a 403 hot-loops through callers' retries.
      this.metrics?.http429.inc({ api_domain: this.apiDomain });
      const retryAfter = Number(res.headers.get('retry-after') ?? '60');
      this.pausedUntil = Date.now() + Math.max(retryAfter, 1) * 1000;
      // The rejection is venue ground truth for the whole budget unit (host +
      // IP), not just for this process — so announce it. Fire and forget: the
      // hook must never throw into the response path.
      try {
        this.cfg.onVenuePause?.(this.restHost, this.pausedUntil);
      } catch {
        // pause-share is best-effort by contract
      }
      // `job.attempts -= 1` used to be UNCONDITIONAL here. A rejection never
      // consumed an attempt, so a PERSISTENT rejection — an IP ban answered
      // with 403, which outlives its own Retry-After — requeued the same job at
      // the head of the queue forever, with no attempt budget and no way for
      // the caller to ever learn. The refund is right for a transient 429: the
      // venue is pacing us, not failing us. It must simply not be unbounded.
      // Refund up to REJECT_FREE_RETRIES, then let the rejection age the job
      // like any other failure, so MAX_ATTEMPTS eventually rejects and the
      // caller can react.
      job.rejections += 1;
      if (job.rejections <= REJECT_FREE_RETRIES) job.attempts -= 1;
      if (job.attempts >= MAX_ATTEMPTS) {
        job.reject(
          new Error(
            `${this.apiDomain} HTTP ${res.status} ${job.path} — still rejected after ` +
              `${job.rejections} venue backoffs`,
          ),
        );
        this.updateQueueGauges();
        this.scheduleWake(this.pausedUntil);
        return;
      }
      this.queues[job.priority].unshift(job);
      this.updateQueueGauges();
      this.scheduleWake(this.pausedUntil);
      return;
    }
    if (res.status >= 500) {
      this.retryOrFail(job, new Error(`${this.apiDomain} HTTP ${res.status}`));
      return;
    }
    if (!res.ok) {
      job.reject(new Error(`${this.apiDomain} HTTP ${res.status} ${job.path}`));
      return;
    }
    try {
      job.resolve(await res.json());
    } catch (err) {
      job.reject(new Error(`${this.apiDomain} bad JSON: ${String(err)}`));
    }
  }

  private retryOrFail(job: Job, err: Error): void {
    if (job.attempts >= MAX_ATTEMPTS) {
      job.reject(err);
      return;
    }
    setTimeout(() => {
      this.queues[job.priority].unshift(job);
      this.updateQueueGauges();
      this.drain();
    }, 1000 * job.attempts);
  }
}
