// The configuration this excerpt reads.
//
// DEVIATION FROM THE PRODUCTION SYSTEM — see the README. The real service
// validates one schema at startup and refuses to boot on anything malformed.
// That schema also carries settings that are out of scope for a public excerpt,
// so what remains here is only the endpoint overrides this code actually uses.
//
// The pattern worth taking from the original is not the parsing but the rule:
// base URLs are configuration. An exchange that moves a hostname, or a
// deployment that must reach one through a proxy, never requires a code change.
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
