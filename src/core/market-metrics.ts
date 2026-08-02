// MetricsHub — per-domain market-metrics state (open interest, funding, mark
// and index prices). Venue pollers merge patches in; the publisher drains
// full-state batches out on a separate, access-gated topic.
//
// The separate topic is not a transport detail. Access to this data is decided
// server-side, and data a subscriber may not have must never be transmitted at
// all — so it cannot ride the shared ticker frame, which everyone receives.
//
// Exchange-agnostic on purpose: venue paths and payload shapes live in the
// adapter legs, never here.

export interface MetricValues {
  oiUsd?: number;
  oiContracts?: number;
  /** OBSERVATION time of the OI fields, adapter-set (Binance:
   *  Date.now() at REST fetch; Bybit: the delta that actually carried an OI
   *  key; OKX: the open-interest channel's own payload ts). The metrics
   *  journal buckets by THIS, never by the shared `ts` — `ts` is touched by
   *  every patch (price ticks included), so a frozen OI under a live ticker
   *  would otherwise forward-fill the present (). */
  oiTs?: number;
  fundingRate?: number;
  nextFundingMs?: number;
  /** observation time of the funding fields (same contract as oiTs). */
  fundingTs?: number;
  deals24h?: number;
  /** Venue-published mark price. Was internal-only (oiUsd input); since the
   *  a later pass it rides metrics_arr as a published FACT for the
   *  decorrelation modal. */
  markPrice?: number;
  /** Venue-published official INDEX price (its spot-basket reference) —
   *  a later pass; perpetual venues only. */
  indexPrice?: number;
  ts: number;
}

export class MetricsHub {
  private readonly values = new Map<string, MetricValues>();

  /** Presence-merge: only fields present in the patch overwrite. */
  merge(exchangeSymbol: string, patch: Partial<MetricValues> & { ts: number }): void {
    const cur = this.values.get(exchangeSymbol);
    if (!cur) {
      this.values.set(exchangeSymbol, { ...patch });
      return;
    }
    for (const [k, v] of Object.entries(patch)) {
      if (v !== undefined) (cur as unknown as Record<string, unknown>)[k] = v;
    }
  }

  get(exchangeSymbol: string): MetricValues | undefined {
    return this.values.get(exchangeSymbol);
  }

  /** Delisting cleanup — a removed instrument must not haunt the batch. */
  remove(exchangeSymbol: string): void {
    this.values.delete(exchangeSymbol);
  }

  entries(): IterableIterator<[string, MetricValues]> {
    return this.values.entries();
  }

  get size(): number {
    return this.values.size;
  }
}
