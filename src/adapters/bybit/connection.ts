// One upstream WebSocket connection: the streams assigned to it, the control
// frames that subscribe and unsubscribe them, its heartbeat, and its own
// reconnect backoff.
//
// The pool decides what this connection carries; the connection only sends what
// it is told and rebuilds its own subscriptions after a reconnect from the set
// it was assigned — never from whatever the socket happened to be carrying.
//
// Venue specifics absorbed here: acknowledged control frames with a string
// request id, a JSON heartbeat whose reply counts as liveness but never as
// data, and a cap on how many streams may ride one subscribe frame.
import WebSocket from 'ws';
import type { Agent } from 'node:http';

const RECONNECT_CAP_MS = 30_000;
const RESUB_BATCH = 10; // v5: ≤10 args per subscribe frame
const RESUB_SPACING_MS = 250;
const PING_INTERVAL_MS = 20_000;

export interface BybitConnectionCallbacks {
  /** Parsed non-control JSON frame (kline/tickers topic frames). */
  onParsed(connId: string, msg: unknown): void;
  onUnparseable(connId: string): void;
  onOpen(connId: string, isReconnect: boolean): void;
  onClosed(connId: string): void;
  onAttempt(connId: string, attempt: number): void;
  onSubscribeConfirmed(connId: string, streams: string[]): void;
  onSubscribeFailed(connId: string, streams: string[], detail: string): void;
}

interface PendingAck {
  streams: string[];
  op: 'sub' | 'unsub';
  sentAt: number;
}

export class BybitWsConnection {
  /** Streams this connection is responsible for (confirmed + in flight). */
  readonly assigned = new Set<string>();
  readonly pendingAcks = new Map<string, PendingAck>();

  attempt = 0;
  everConnected = false;
  /** Last DATA frame (acks/pongs excluded) — idle policy input. */
  lastDataAt = 0;
  lastMessageAt = 0;
  connectedAt = 0;

  private ws: WebSocket | null = null;
  private stopped = false;
  private nextAckId = 1;
  private resubTimer: ReturnType<typeof setTimeout> | null = null;
  private pingTimer: ReturnType<typeof setInterval> | null = null;

  constructor(
    readonly id: string,
    readonly url: string,
    private readonly cb: BybitConnectionCallbacks,
    private readonly agent?: Agent,
  ) {
    this.open();
  }

  get isOpen(): boolean {
    return this.ws?.readyState === WebSocket.OPEN;
  }

  private open(): void {
    if (this.stopped) return;
    const ws = new WebSocket(this.url, {
      handshakeTimeout: 10_000,
      agent: this.agent,
    });
    this.ws = ws;

    ws.on('open', () => {
      const isReconnect = this.everConnected;
      this.everConnected = true;
      this.attempt = 0;
      this.connectedAt = Date.now();
      this.lastMessageAt = Date.now();
      this.startPing();
      if (this.assigned.size > 0) this.resubscribeAll();
      this.cb.onOpen(this.id, isReconnect);
    });

    ws.on('message', (data) => {
      this.lastMessageAt = Date.now();
      let parsed: unknown;
      try {
        parsed = JSON.parse(String(data));
      } catch {
        this.cb.onUnparseable(this.id);
        return;
      }
      const ctl = parsed as {
        op?: string;
        success?: boolean;
        ret_msg?: string;
        req_id?: string;
      };
      // Control plane: pongs + sub/unsub acks (never data for idle policy).
      if (ctl.op === 'pong' || ctl.ret_msg === 'pong') return;
      if (typeof ctl.req_id === 'string' && this.pendingAcks.has(ctl.req_id)) {
        const pending = this.pendingAcks.get(ctl.req_id) as PendingAck;
        this.pendingAcks.delete(ctl.req_id);
        if (pending.op === 'sub') {
          if (ctl.success === false) {
            for (const s of pending.streams) this.assigned.delete(s);
            this.cb.onSubscribeFailed(this.id, pending.streams, ctl.ret_msg ?? 'ack failed');
          } else {
            this.cb.onSubscribeConfirmed(this.id, pending.streams);
          }
        }
        return;
      }
      if (typeof ctl.op === 'string') return; // stray control frame — not data
      this.lastDataAt = Date.now();
      this.cb.onParsed(this.id, parsed);
    });

    const retry = () => {
      this.ws = null;
      this.stopPing();
      if (this.resubTimer) {
        clearTimeout(this.resubTimer);
        this.resubTimer = null;
      }
      // In-flight acks died with the socket; reconnect resubscribes the whole
      // assigned set, so pending bookkeeping just clears.
      this.pendingAcks.clear();
      this.cb.onClosed(this.id);
      if (this.stopped) return;
      this.attempt += 1;
      this.cb.onAttempt(this.id, this.attempt);
      const delay =
        Math.min(1000 * 2 ** (this.attempt - 1), RECONNECT_CAP_MS) +
        Math.random() * 1000;
      setTimeout(() => this.open(), delay);
    };

    ws.on('close', retry);
    ws.on('error', () => {
      // 'close' follows 'error'; retry handled there.
    });
  }

  private startPing(): void {
    this.stopPing();
    this.pingTimer = setInterval(() => {
      if (this.isOpen) this.ws?.send(JSON.stringify({ op: 'ping' }));
    }, PING_INTERVAL_MS);
    this.pingTimer.unref?.();
  }

  private stopPing(): void {
    if (this.pingTimer) clearInterval(this.pingTimer);
    this.pingTimer = null;
  }

  /** Paced full re-subscribe of the assigned set (reconnect heal). */
  private resubscribeAll(): void {
    const all = [...this.assigned];
    let offset = 0;
    const sendNext = () => {
      this.resubTimer = null;
      if (!this.isOpen || offset >= all.length) return;
      const batch = all.slice(offset, offset + RESUB_BATCH);
      offset += RESUB_BATCH;
      this.sendControl('subscribe', batch, 'sub');
      if (offset < all.length) {
        this.resubTimer = setTimeout(sendNext, RESUB_SPACING_MS);
      }
    };
    sendNext();
  }

  /** Pool-paced SUBSCRIBE: registers the streams as assigned + in flight. */
  subscribe(streams: string[]): boolean {
    if (!this.isOpen || streams.length === 0) return false;
    for (const s of streams) this.assigned.add(s);
    this.sendControl('subscribe', streams, 'sub');
    return true;
  }

  /** Pool-paced UNSUBSCRIBE: streams leave the assigned set immediately. */
  unsubscribe(streams: string[]): boolean {
    if (streams.length === 0) return true;
    for (const s of streams) this.assigned.delete(s);
    if (!this.isOpen) return true; // gone from desired state; nothing on wire
    this.sendControl('unsubscribe', streams, 'unsub');
    return true;
  }

  private sendControl(op: 'subscribe' | 'unsubscribe', args: string[], kind: 'sub' | 'unsub'): void {
    const reqId = String(this.nextAckId++);
    this.pendingAcks.set(reqId, { streams: args, op: kind, sentAt: Date.now() });
    this.ws?.send(JSON.stringify({ op, args, req_id: reqId }));
  }

  /** Hard socket kill (idle policy / kill-connection test hook) — retries. */
  terminate(): void {
    this.ws?.terminate();
  }

  /** Permanent removal (pool reassignment) — no retry. */
  destroy(): void {
    this.stopped = true;
    this.stopPing();
    if (this.resubTimer) clearTimeout(this.resubTimer);
    this.ws?.terminate();
    this.ws = null;
  }
}
