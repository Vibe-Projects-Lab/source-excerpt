// Per-instrument price precision. The instruments table carries an open jsonb
// column for venue metadata; this module owns the ONE formalised key inside it,
// so that the writer (the listing sync), the reader (the instrument catalogue
// endpoint) and the consumer (the chart's price formatter) can never drift on
// its shape:
//
//   instrument_metadata.instrument_info.tick_size : string   e.g. "0.001"
//
// tick_size is a DECIMAL STRING exactly as venues quote it (covers 0.25/0.05
// steps a bare "decimals" integer cannot); display precision derives from it.

/** The formalised sub-structure for a venue's trading rules. Decimal fields are
 *  canonical STRINGS; other keys are expected to appear later, so writers must
 *  MERGE into this object and never overwrite siblings they do not know. */
export interface InstrumentInfo {
  /** Venue price step ('0.001') — drives chart priceFormat (). */
  tick_size?: string;
  /** Min trade amount, base asset (LOT_SIZE.minQty). */
  min_qty?: string;
  /** Max LIMIT order amount, base asset (LOT_SIZE.maxQty). */
  max_limit_qty?: string;
  /** Max MARKET order amount, base asset (MARKET_LOT_SIZE.maxQty) — the
   *  "can this wall be filled in one shot" number. */
  max_market_qty?: string;
  /** Min order notional, quote units (MIN_NOTIONAL/NOTIONAL). */
  min_notional?: string;
  /** Max open orders on the symbol (MAX_NUM_ORDERS). */
  max_orders?: number;
  /** Limit-price cap/floor multipliers as the venue quotes them
   *  (PERCENT_PRICE[_BY_SIDE]) — raw, semantics differ per venue/market. */
  price_cap_ratio?: string;
  price_floor_ratio?: string;
  /** Futures liquidation clearance fee rate ('0.0125'), venue-reported. */
  liquidation_fee?: string;
}

/** Ordered display keys for the trading-rules UI (tick_size renders first,
 *  separately — it is also the chart contract). */
export const TRADING_RULE_KEYS = [
  'min_qty',
  'max_limit_qty',
  'max_market_qty',
  'min_notional',
  'max_orders',
  'price_cap_ratio',
  'price_floor_ratio',
  'liquidation_fee',
] as const satisfies readonly (keyof InstrumentInfo)[];

/**
 * Venue decimal → canonical string: plain non-negative decimal, > 0,
 * trailing zeros stripped ('0.00100000' → '0.001', '1.000' → '1').
 * Anything else (exponent forms, negatives, zero, garbage) → null.
 */
export function normalizeDecimalString(raw: unknown): string | null {
  if (typeof raw !== 'string' && typeof raw !== 'number') return null;
  const s = String(raw).trim();
  if (!/^\d+(\.\d+)?$/.test(s)) return null;
  if (Number(s) <= 0) return null;
  if (!s.includes('.')) return s;
  return s.replace(/0+$/, '').replace(/\.$/, '');
}

/** alias — the tick is just the price-step decimal. */
export const normalizeTickSize = normalizeDecimalString;

/** Positive integer counts (max_orders). */
export function normalizeCount(raw: unknown): number | null {
  const n = typeof raw === 'string' ? Number(raw) : typeof raw === 'number' ? raw : NaN;
  return Number.isInteger(n) && n > 0 ? n : null;
}

/** Extract + validate the KNOWN instrument_info keys from untyped jsonb —
 *  returns a CLEAN object (only valid keys present). */
export function readInstrumentInfo(instrumentMetadata: unknown): InstrumentInfo {
  if (typeof instrumentMetadata !== 'object' || instrumentMetadata === null) return {};
  const info = (instrumentMetadata as { instrument_info?: unknown }).instrument_info;
  if (typeof info !== 'object' || info === null) return {};
  const raw = info as Record<string, unknown>;
  const out: InstrumentInfo = {};
  const tick = normalizeDecimalString(raw['tick_size']);
  if (tick !== null) out.tick_size = tick;
  for (const key of TRADING_RULE_KEYS) {
    if (key === 'max_orders') {
      const n = normalizeCount(raw[key]);
      if (n !== null) out.max_orders = n;
    } else {
      const v = normalizeDecimalString(raw[key]);
      if (v !== null) out[key] = v;
    }
  }
  return out;
}

/** Venue values win, but a venue that stops reporting a field never CLEARS a
 *  recorded one (same posture as the tick rule). */
export function mergeInstrumentInfo(current: InstrumentInfo, venue: InstrumentInfo): InstrumentInfo {
  return { ...current, ...venue };
}

export function sameInstrumentInfo(a: InstrumentInfo, b: InstrumentInfo): boolean {
  const keys: readonly (keyof InstrumentInfo)[] = ['tick_size', ...TRADING_RULE_KEYS];
  return keys.every((k) => a[k] === b[k]);
}

/** True when the venue reported at least one field worth persisting. */
export function hasInstrumentInfo(info: InstrumentInfo): boolean {
  return Object.keys(info).length > 0;
}

/** Safe extraction from an untyped `instrument_metadata` jsonb value. */
export function readTickSize(instrumentMetadata: unknown): string | null {
  if (typeof instrumentMetadata !== 'object' || instrumentMetadata === null) return null;
  const info = (instrumentMetadata as { instrument_info?: unknown }).instrument_info;
  if (typeof info !== 'object' || info === null) return null;
  return normalizeTickSize((info as InstrumentInfo).tick_size);
}

export interface TickPriceFormat {
  /** Digits after the decimal point ('0.25' → 2, '1' → 0). */
  precision: number;
  /** The tick itself as a number — LW priceFormat.minMove. */
  minMove: number;
}

/**
 * tick_size → Lightweight-Charts priceFormat inputs. Null on absent/invalid
 * input or absurd precision (>12 dp) — callers keep their default/heuristic.
 */
export function tickToPriceFormat(tick: string | null | undefined): TickPriceFormat | null {
  const norm = normalizeTickSize(tick);
  if (norm === null) return null;
  const dot = norm.indexOf('.');
  const precision = dot === -1 ? 0 : norm.length - dot - 1;
  if (precision > 12) return null;
  return { precision, minMove: Number(norm) };
}
