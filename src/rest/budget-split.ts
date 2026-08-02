// Who may spend how much of an exchange's rate limit, checked at startup.
//
// This exists because of an invariant that was false in a way no single process
// could detect. An exchange enforces its limit per host and address; its spot
// and futures domains usually share one host; and our spot and futures ingest
// processes are separate. Several of our processes are therefore counted by the
// exchange as one client, while each of them independently obeys "never exceed
// the venue budget" and is, on its own, correct.
//
// The fix is static and declared rather than negotiated at runtime. Every venue
// configuration states the host budget and the per-role shares carved out of
// it, and this module is the single place that checks the sum — at every boot of
// every process that builds a dispatcher, so configuration drift becomes a
// crash here instead of a ban later.
//
// A shared dynamic window over Redis was considered and rejected: it requires
// an await inside the request loop, which breaks the synchronous accounting; a
// partial Redis failure produces the very overshoot it exists to prevent; and
// one shared window destroys the only cross-process priority isolation there
// is.
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
 *   2. the sum of EVERY role's share, across every domain on the unit, does
 *      not exceed the host budget — the roles are read from the role table, not
 *      listed here, because a role omitted from a hand-written list is exactly
 *      how a share gets spent without being counted;
 *   3. where a domain declares a known demand floor for its live path, the
 *      ingest share still covers it after the safety margin. */
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
