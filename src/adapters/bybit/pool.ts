// Many upstream connections presented as one stream source.
//
// The pool assigns streams to connections, paces the control frames so the
// venue's inbound limit is respected, reconciles toward a desired stream set
// after any disruption, and reports which streams were healed so history can be
// repaired for exactly those.
//
// It also owns the ticker aggregation engine, because this exchange publishes
// no all-market ticker: per-symbol frames have to be assembled into batches
// here before anything downstream sees them.
import { klineStreamName, normalizeKlineFrame } from './dialect.js';
import { BybitWsConnection } from './connection.js';
import { BybitTickerEngine } from './ticker-engine.js';
import { proxyAgentFor } from '../../core/proxy-agent.js';
import type {
  PoolCallbacks,
  VenueDomainConfig,
  VenueStreamSource,
} from '../types.js';
import type { ConnectionRegistry } from '../../core/connection-registry.js';

const SLOT_MS = 200;
const SLOTS = 5;
const ACK_TIMEOUT_MS = 10_000;
const FAIL_AFTER_ATTEMPTS = 5;
const IDLE_KILL_MS = 60_000;
const SWEEP_MS = 5_000;

export class BybitConnectionPool implements VenueStreamSource {
  readonly apiDomain: string;

  private readonly conns = new Map<string, BybitWsConnection>();
  /** stream name → owning connection id (assignment authority). */
  private readonly owner = new Map<string, string>();
  private pendingSub: string[] = [];
  private pendingUnsub: string[] = [];
  private nextConnSeq = 1;
  private slotTimer: ReturnType<typeof setTimeout> | null = null;
  private sweepTimer: ReturnType<typeof setInterval> | null = null;
  private stopped = false;
  private readonly streamsPerConn: number;
  private readonly subBatch: number;
  private readonly tickerEngine: BybitTickerEngine;

  constructor(
    private readonly cfg: VenueDomainConfig,
    streamsPerConn: number,
    private readonly registry: ConnectionRegistry,
    private readonly cb: PoolCallbacks,
  ) {
    this.apiDomain = cfg.apiDomain;
    // Venue cap is config — the env default can never exceed it.
    this.streamsPerConn = Math.min(streamsPerConn, cfg.maxStreamsPerConn);
    this.subBatch = cfg.maxArgsPerSubscribe ?? 10;
    this.tickerEngine = new BybitTickerEngine(cfg.marketType === 'perpetual', {
      onTickerArr: (items) => this.cb.onTickerArr(items),
      onMetrics: (items) => this.cb.onMetrics?.(items),
    });
    this.scheduleSlot();
    this.sweepTimer = setInterval(() => this.sweep(), SWEEP_MS);
  }

  /** Desired-state delta from the reconciler (SubscriptionManager). */
  applySubscriptions(add: string[], remove: string[]): void {
    for (const s of add) {
      const unsubIdx = this.pendingUnsub.indexOf(s);
      if (unsubIdx >= 0) this.pendingUnsub.splice(unsubIdx, 1);
      if (!this.owner.has(s) && !this.pendingSub.includes(s)) {
        this.pendingSub.push(s);
      }
    }
    for (const s of remove) {
      const subIdx = this.pendingSub.indexOf(s);
      if (subIdx >= 0) this.pendingSub.splice(subIdx, 1);
      if (this.owner.has(s) && !this.pendingUnsub.includes(s)) {
        this.pendingUnsub.push(s);
      }
      // A delisted symbol's ticker state must not linger in the engine.
      if (s.startsWith('tickers.')) {
        this.tickerEngine.remove(s.slice('tickers.'.length));
      }
    }
  }

  streamCount(): number {
    return this.owner.size;
  }

  pendingCount(): number {
    return this.pendingSub.length;
  }

  /** Test hook: hard socket kill, exercising the in-place reconnect heal. */
  killConnection(connId: string): boolean {
    const conn = this.conns.get(connId);
    if (!conn) return false;
    conn.terminate();
    return true;
  }

  /** Declared connection failure: permanently remove the connection and
   *  reassign its assigned_stream_keys[] at slot pace. */
  failConnection(connId: string): boolean {
    const conn = this.conns.get(connId);
    if (!conn) return false;
    const streams = [...conn.assigned];
    conn.destroy();
    this.conns.delete(connId);
    this.registry.remove(connId);
    for (const s of streams) {
      if (this.owner.get(s) === connId) this.owner.delete(s);
    }
    this.pendingSub.push(...streams);
    console.warn(
      `${this.cfg.apiDomain} ${connId}: declared failed — ${streams.length} streams reassigned`,
    );
    if (streams.length > 0) this.cb.onStreamsHealed(streams, 'reassigned');
    return true;
  }

  connectionIds(): string[] {
    return [...this.conns.keys()];
  }

  shutdown(): void {
    this.stopped = true;
    this.tickerEngine.stop();
    if (this.slotTimer) clearTimeout(this.slotTimer);
    if (this.sweepTimer) clearInterval(this.sweepTimer);
    for (const conn of this.conns.values()) {
      conn.destroy();
      this.registry.remove(conn.id);
    }
    this.conns.clear();
    this.owner.clear();
  }

  // --- slot scheduler (wall-clock phased: Date.now()%1000 — ) ---

  private scheduleSlot(): void {
    if (this.stopped) return;
    const now = Date.now();
    const next = Math.floor(now / SLOT_MS) * SLOT_MS + SLOT_MS;
    this.slotTimer = setTimeout(() => {
      const slot = Math.floor(Date.now() / SLOT_MS) % SLOTS;
      try {
        this.slotTick(slot);
      } catch (err) {
        console.error(`${this.cfg.apiDomain} pool slot ${slot} failed`, err);
      }
      this.scheduleSlot();
    }, next - now);
  }

  private slotTick(slot: number): void {
    if (slot === 0 || slot === 3) this.drainSubscribes();
    else if (slot === 1) this.drainUnsubscribes();
  }

  private drainSubscribes(): void {
    if (this.pendingSub.length === 0) return;
    for (const conn of this.conns.values()) {
      if (this.pendingSub.length === 0) return;
      if (!conn.isOpen) continue;
      const capacity = this.streamsPerConn - conn.assigned.size;
      if (capacity <= 0) continue;
      const batch = this.pendingSub.splice(0, Math.min(this.subBatch, capacity));
      if (conn.subscribe(batch)) {
        for (const s of batch) this.owner.set(s, conn.id);
        this.touchRegistry(conn);
      } else {
        this.pendingSub.unshift(...batch); // socket flapped mid-slot — retry
      }
    }
    if (this.pendingSub.length > 0 && !this.hasSpareCapacity()) {
      this.spawnConnection();
    }
  }

  private drainUnsubscribes(): void {
    if (this.pendingUnsub.length === 0) return;
    const byConn = new Map<string, string[]>();
    const drained = this.pendingUnsub;
    this.pendingUnsub = [];
    for (const s of drained) {
      const connId = this.owner.get(s);
      if (!connId) continue;
      const list = byConn.get(connId) ?? [];
      list.push(s);
      byConn.set(connId, list);
    }
    for (const [connId, streams] of byConn) {
      const conn = this.conns.get(connId);
      if (!conn) continue;
      // One control frame per connection per slot: overflow re-queues.
      const batch = streams.slice(0, this.subBatch);
      const rest = streams.slice(this.subBatch);
      conn.unsubscribe(batch);
      for (const s of batch) this.owner.delete(s);
      if (rest.length > 0) this.pendingUnsub.push(...rest);
      this.touchRegistry(conn);
    }
  }

  private hasSpareCapacity(): boolean {
    for (const conn of this.conns.values()) {
      if (
        (conn.isOpen && conn.assigned.size < this.streamsPerConn) ||
        (!conn.isOpen && conn.attempt === 0 && !conn.everConnected)
      ) {
        return true;
      }
    }
    return false;
  }

  private spawnConnection(): BybitWsConnection {
    const id = `${this.cfg.apiDomain}#${this.nextConnSeq++}`;
    const url = this.cfg.wsBase; // v5 endpoint IS the full path (no /ws)
    const agent = proxyAgentFor(this.cfg.wsProxyUrl);
    const conn = new BybitWsConnection(id, url, {
      onParsed: (_connId, msg) => this.routeMessage(msg),
      onUnparseable: () => this.cb.onMalformed(),
      onOpen: (connId, isReconnect) => {
        const c = this.conns.get(connId);
        if (c) {
          this.registry.upsert(connId, url, {
            status: 'open',
            connectedAt: Date.now(),
            reconnectAttempts: 0,
            assignedStreamCount: c.assigned.size,
          });
        }
        if (isReconnect && c && c.assigned.size > 0) {
          this.cb.onStreamsHealed([...c.assigned], 'reconnect');
        }
      },
      onClosed: (connId) => {
        const c = this.conns.get(connId);
        if (!c) return;
        this.registry.upsert(connId, url, {
          status: 'reconnecting',
          assignedStreamCount: c.assigned.size,
        });
      },
      onAttempt: (connId, attempt) => {
        this.registry.upsert(connId, url, { reconnectAttempts: attempt });
        this.cb.onReconnect(connId, attempt);
      },
      onSubscribeConfirmed: (_connId, streams) => this.cb.onSubscribed(streams),
      onSubscribeFailed: (connId, streams, detail) => {
        const stillOwned = streams.filter((s) => this.owner.get(s) === connId);
        console.error(
          `${this.cfg.apiDomain} ${connId} subscribe rejected (${detail}) — requeueing ${stillOwned.length}/${streams.length} streams`,
        );
        for (const s of stillOwned) this.owner.delete(s);
        this.pendingSub.push(...stillOwned);
      },
    }, agent);
    this.conns.set(id, conn);
    this.registry.upsert(id, url, { status: 'connecting' }, () => [
      ...conn.assigned,
    ]);
    return conn;
  }

  private routeMessage(msg: unknown): void {
    const klines = normalizeKlineFrame(msg, this.cfg);
    if (klines) {
      // Multi-item frames are REAL (closed bar + new forming in one frame,
      // Every item in the frame is an event; a frame where all of them fail to
      // parse is malformed, not empty.
      if (klines.length === 0) {
        this.cb.onMalformed();
        return;
      }
      for (const evt of klines) this.cb.onKline(evt);
      return;
    }
    if (this.tickerEngine.ingest(msg as Parameters<BybitTickerEngine['ingest']>[0])) {
      return;
    }
    // Other venue frames are ignored.
  }

  private touchRegistry(conn: BybitWsConnection): void {
    this.registry.upsert(conn.id, conn.url, {
      status: conn.isOpen ? 'open' : 'reconnecting',
      lastMessageAt: conn.lastMessageAt,
      lastDataAt: conn.lastDataAt,
      assignedStreamCount: conn.assigned.size,
      reconnectAttempts: conn.attempt,
    });
  }

  private sweep(): void {
    const now = Date.now();
    for (const conn of this.conns.values()) {
      // Ack timeouts → requeue through slot 3 (recovery).
      for (const [id, pending] of conn.pendingAcks) {
        if (now - pending.sentAt <= ACK_TIMEOUT_MS) continue;
        conn.pendingAcks.delete(id);
        if (pending.op !== 'sub') continue;
        for (const s of pending.streams) {
          conn.assigned.delete(s);
          if (this.owner.get(s) === conn.id) this.owner.delete(s);
        }
        this.pendingSub.push(...pending.streams);
        console.warn(
          `${this.cfg.apiDomain} ${conn.id}: ${pending.streams.length} subscribe acks timed out — requeued`,
        );
      }

      // Idle policy: only sockets that HAD data and went silent (pongs are
      // NOT data — a fenced endpoint stays connected, freshness metric owns
      // the alarm).
      if (
        conn.isOpen &&
        conn.lastDataAt > 0 &&
        now - conn.lastDataAt > IDLE_KILL_MS &&
        now - conn.connectedAt > IDLE_KILL_MS
      ) {
        console.warn(`${this.cfg.apiDomain} ${conn.id}: data-idle — recycling socket`);
        conn.terminate();
        continue;
      }

      // Declared failure: reassign assigned_stream_keys[] to the pool.
      if (!conn.isOpen && conn.attempt >= FAIL_AFTER_ATTEMPTS) {
        this.failConnection(conn.id);
        continue;
      }

      this.touchRegistry(conn);
    }
  }
}

export { klineStreamName };
