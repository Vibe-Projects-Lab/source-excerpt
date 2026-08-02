// The budget-unit fix, and a good example of an invariant that was false in a
// way no local check could see.
//
// A venue enforces its REST budget per HOST and outbound IP, not per API
// domain. A venue's spot and futures domains routinely share one REST host, and
// our own spot and futures ingest processes are separate — so several of our
// processes are counted by the venue as one client.
//
// Before this module, "cannot exceed the venue budget by construction" was only
// true PER PROCESS. One venue's full number was handed to both the ingest and
// the background worker, a latent 1.8x overshoot that measured 1614 against a
// 2400 budget in production. Other venues carried a hand-computed split in a
// configuration comment that nothing verified — and one of those comments had
// miscounted its own dispatchers: it divided by three where four were deployed,
// so the real ceiling was 800 per minute against a 600 planning figure.
//
// The fix is STATIC and declared. Every venue configuration states the host
// budget (`weightBudgetPerMin`) and the per-role shares
// (`restRoleBudgetPerMin`), and this module is the single place that checks the
// arithmetic — at every boot of every process that builds a dispatcher, so
// configuration drift is a crash here rather than a ban from the venue later.
//
// A shared dynamic window over Redis was reviewed and rejected. The decisive
// argument: it requires an await inside the dispatcher's drain loop, which
// breaks the synchronous window invariants; a partial Redis failure produces
// exactly the overshoot the scheme exists to prevent; and sharing one window
// destroys the only cross-process priority isolation the system has.
import type { VenueDomainConfig } from '../adapters/types.js';

/** `seed` is the anchor backfill: the workers that walk hourly and daily
 *  history, seeding it and repairing it against the venue. It gets its OWN
 *  dispatcher share, carved out of `jobs` so every host total is unchanged —
 *  and so that a multi-day backfill can never starve verification, which is the
 *  only thing healing the live one-minute window. A backfill that punches holes
 *  in the live journal is a real failure mode, not a theoretical one. */
export type RestRole = 'ingest' | 'jobs' | 'seed';

/** Every role that carries a dispatcher share. The boot assert iterates THIS —
 *  it used to name `ingest` and `jobs` literally, so adding a role would have
 *  silently dropped it out of the over-commitment check, removing the very
 *  protection this module exists to provide. */
export const REST_ROLES: readonly RestRole[] = ['ingest', 'jobs', 'seed'];

/** The budget share THIS role's dispatcher may spend (its own minute window).
 *  Pass the result as DispatcherConfig.budgetPerMin — never cfg.weightBudgetPerMin. */
export function roleBudget(cfg: VenueDomainConfig, role: RestRole): number {
  const share = cfg.restRoleBudgetPerMin[role];
  if (!Number.isFinite(share) || share <= 0) {
    throw new Error(
      `${cfg.apiDomain}: restRoleBudgetPerMin.${role} must be a positive number, got ${share}`,
    );
  }
  return share;
}

/** The budget-unit key. outboundIpId is folded in from day one, so adding a
 *  second egress address later widens the unit without a change of semantics;
 *  every dispatcher today runs on 'primary'. */
export function restBudgetUnit(restBase: string, outboundIpId = 'primary'): string {
  return `${new URL(restBase).host}#${outboundIpId}`;
}

/** Boot gate: walk EVERY registered domain config and fail loudly when the
 *  declared split cannot hold the venue's number. Runs in each process that
 *  builds a dispatcher — a config drift is a crash at boot, not a silent ban
 *  risk in week two. Checks, per budget unit (host + outbound IP):
 *   1. all domains on the unit declare the SAME host budget;
 *   2. Σ of every dispatcher share on the unit (ingest + jobs per domain)
 *      ≤ the host budget;
 *   3. where a domain declares a measured ingest demand floor, the ingest
 *      share still covers it after the dispatcher's 0.9 safety margin. */
export function assertRestBudgetSplit(cfgs: readonly VenueDomainConfig[]): void {
  const byUnit = new Map<string, VenueDomainConfig[]>();
  for (const cfg of cfgs) {
    const unit = restBudgetUnit(cfg.restBase);
    const list = byUnit.get(unit) ?? [];
    list.push(cfg);
    byUnit.set(unit, list);
  }
  for (const [unit, list] of byUnit) {
    const budgets = new Set(list.map((c) => c.weightBudgetPerMin));
    if (budgets.size > 1) {
      throw new Error(
        `REST budget split: domains on ${unit} disagree on the host budget: ` +
          list.map((c) => `${c.apiDomain}=${c.weightBudgetPerMin}`).join(', '),
      );
    }
    const hostBudget = list[0]!.weightBudgetPerMin;
    let sum = 0;
    // Iterate REST_ROLES, never a hand-written list: a new role omitted here
    // would be spent against the venue while passing the check that exists to
    // stop exactly that.
    for (const c of list) for (const role of REST_ROLES) sum += roleBudget(c, role);
    if (sum > hostBudget) {
      throw new Error(
        `REST budget split: ${unit} is over-committed — Σ role shares ${sum} > ` +
          `host budget ${hostBudget} (` +
          list
            .map(
              (c) =>
                `${c.apiDomain}: ` +
                REST_ROLES.map((r) => `${r} ${c.restRoleBudgetPerMin[r]}`).join(' + '),
            )
            .join('; ') +
          ')',
      );
    }
    for (const c of list) {
      const floor = c.restIngestFloorPerMin;
      if (floor === undefined) continue;
      // The dispatcher spends at most share × 0.9 (budgetSafety default) — the
      // floor must fit under the EFFECTIVE ceiling, or steady-state demand
      // alone exhausts the window and healing starves behind it.
      const effective = Math.floor(c.restRoleBudgetPerMin.ingest * 0.9);
      if (effective < floor) {
        throw new Error(
          `REST budget split: ${c.apiDomain} ingest share ${c.restRoleBudgetPerMin.ingest} ` +
            `(effective ${effective} after 0.9 safety) is below the measured ` +
            `steady-state demand floor ${floor} — raise the share or re-measure`,
        );
      }
    }
  }
}
