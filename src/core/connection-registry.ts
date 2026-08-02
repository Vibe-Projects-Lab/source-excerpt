// Which upstream connections exist and what each one is carrying.
//
// Per connection: where it points, whether it is healthy, when it last heard
// anything, which streams are assigned to it, and how many times it has
// reconnected. Held in memory and exported as metrics rather than stored — it
// describes the current moment, and nothing needs it after a restart.
//
// The assigned stream set matters beyond observability: after a disruption it
// is the authority for what the connection should carry, and the list of
// streams whose history may need repairing.
import { Gauge, type Registry } from 'prom-client';

export type ConnectionStatus = 'connecting' | 'open' | 'reconnecting' | 'closed';

export interface ConnectionInfo {
  connectionId: string;
  apiDomain: string;
  outboundIpId: string;
  wsUrl: string;
  status: ConnectionStatus;
  lastMessageAt: number;
  /** Last DATA frame (ack frames excluded). */
  lastDataAt: number;
  assignedStreamCount: number;
  reconnectAttempts: number;
  connectedAt: number;
}

export class ConnectionRegistry {
  private readonly entries = new Map<string, ConnectionInfo>();
  private readonly connectionsGauge: Gauge<'api_domain' | 'status'>;
  private readonly streamsGauge: Gauge<'api_domain'>;
  private readonly streamKeys = new Map<string, () => string[]>();

  constructor(
    registry: Registry,
    private readonly apiDomain: string,
    private readonly outboundIpId = 'primary',
  ) {
    this.connectionsGauge = new Gauge({
      name: 'vibe_adapter_ws_connections',
      help: 'Upstream WebSocket connections by status',
      labelNames: ['api_domain', 'status'],
      registers: [registry],
    });
    this.streamsGauge = new Gauge({
      name: 'vibe_adapter_streams_assigned',
      help: 'Streams assigned across the connection pool',
      labelNames: ['api_domain'],
      registers: [registry],
    });
  }

  upsert(
    connectionId: string,
    wsUrl: string,
    patch: Partial<ConnectionInfo>,
    assignedStreamKeys?: () => string[],
  ): void {
    const prev = this.entries.get(connectionId);
    this.entries.set(connectionId, {
      connectionId,
      apiDomain: this.apiDomain,
      outboundIpId: this.outboundIpId,
      wsUrl,
      status: 'connecting',
      lastMessageAt: 0,
      lastDataAt: 0,
      assignedStreamCount: 0,
      reconnectAttempts: 0,
      connectedAt: 0,
      ...prev,
      ...patch,
    });
    if (assignedStreamKeys) this.streamKeys.set(connectionId, assignedStreamKeys);
    this.refreshGauges();
  }

  remove(connectionId: string): void {
    this.entries.delete(connectionId);
    this.streamKeys.delete(connectionId);
    this.refreshGauges();
  }

  /** The full view, including the assigned stream set — what reassignment and
   *  any debugging session actually need. */
  snapshot(): (ConnectionInfo & { assignedStreamKeys: string[] })[] {
    return [...this.entries.values()].map((e) => ({
      ...e,
      assignedStreamKeys: this.streamKeys.get(e.connectionId)?.() ?? [],
    }));
  }

  private refreshGauges(): void {
    this.connectionsGauge.reset();
    let streams = 0;
    for (const e of this.entries.values()) {
      this.connectionsGauge.inc({ api_domain: e.apiDomain, status: e.status });
      streams += e.assignedStreamCount;
    }
    this.streamsGauge.set({ api_domain: this.apiDomain }, streams);
  }
}
