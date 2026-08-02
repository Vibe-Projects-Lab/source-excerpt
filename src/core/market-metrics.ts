// Per-domain state for the market metrics that are not candles: open interest,
// funding, mark and index prices.
//
// Exchange-specific collection lives in the adapters; this only holds the
// merged state and hands out full-state batches.
//
// These batches travel on their own topic rather than riding the shared ticker
// frame, and that is not a transport detail: access to this data is decided on
// the server, and data a subscriber may not have must never be sent at all.
// Putting it on the frame everyone receives would make that impossible.
export interface MetricValues {
  oiUsd?: number;
  oiContracts?: number;
  /** OBSERVATION time of the OI fields, adapter-set (Binance:
   *  Date.now() at REST fetch; Bybit: the delta that actually carried an OI
   *  key; OKX: the open-interest channel's own payload ts). The metrics
   *  journal buckets by THIS, never by the shared `ts` — `ts` is touched by
   *  every patch (price ticks included), so a frozen OI under a live ticker
   *  would otherwise forward-fill the present. */
  oiTs?: number;
  fundingRate?: number;
  nextFundingMs?: number;
  /** observation time of the funding fields (same contract as oiTs). */
  fundingTs?: number;
  deals24h?: number;
  /** The exchange's own mark price. Originally an internal input used to value
   *  open interest; it is now published in its own right, because comparing
   *  prices across exchanges needs each exchange's own reference. */
  markPrice?: number;
  /** The exchange's official index price — its own spot-basket reference.
   *  Perpetual markets only. */
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
