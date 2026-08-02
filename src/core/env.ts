// Environment surface for this excerpt.
//
// DEVIATION FROM THE PRODUCTION SYSTEM — see README, "What was changed".
// The real service validates one large environment schema at boot with Zod and
// fails closed on anything malformed. That schema also carries the settings of
// the abuse-defence layer, which is out of scope here, so this excerpt declares
// only the venue endpoint overrides the adapter below actually reads.
//
// The pattern worth taking from the original is not the parsing: it is that
// base URLs are configuration. A venue that moves a hostname, or a deployment
// that has to reach one through a proxy, must never require a code change.

export interface VenueEnv {
  BYBIT_SPOT_WS_BASE?: string;
  BYBIT_LINEAR_WS_BASE?: string;
  BYBIT_REST_BASE?: string;
  BYBIT_SPOT_PROXY_URL?: string;
  BYBIT_LINEAR_PROXY_URL?: string;
}

const KEYS = [
  'BYBIT_SPOT_WS_BASE',
  'BYBIT_LINEAR_WS_BASE',
  'BYBIT_REST_BASE',
  'BYBIT_SPOT_PROXY_URL',
  'BYBIT_LINEAR_PROXY_URL',
] as const;

export function loadEnv(): VenueEnv {
  const out: VenueEnv = {};
  for (const key of KEYS) {
    const value = process.env[key];
    // An empty string is an unset variable, not an empty hostname.
    if (value !== undefined && value !== '') out[key] = value;
  }
  return out;
}
