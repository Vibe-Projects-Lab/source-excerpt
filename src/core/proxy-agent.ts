// Reaching an exchange that is not reachable from the deployment region.
//
// A blocked endpoint is a configuration problem, not a code problem: the
// connection layer takes an optional proxy URL and picks the agent from its
// scheme. Everything else about the connection is identical either way.
import type { Agent } from 'node:http';
import { HttpsProxyAgent } from 'https-proxy-agent';
import { SocksProxyAgent } from 'socks-proxy-agent';

export function proxyAgentFor(proxyUrl?: string): Agent | undefined {
  if (!proxyUrl) return undefined;
  if (proxyUrl.startsWith('socks')) return new SocksProxyAgent(proxyUrl);
  return new HttpsProxyAgent(proxyUrl);
}
