import { SocketLike, SocketCloseInfo, SocketMessageInfo } from './types';

export type SimulatedCondition = 'good' | 'fair' | 'poor' | 'nat-flushed';

/**
 * A deterministic, controllable fake transport used by the demo and tests.
 *
 * This exists so we can *prove* adaptive behaviour (learning a NAT timeout,
 * backing off, recovering) without a real flaky network or a real 2-5 minute
 * wait for a carrier to actually flush a NAT table. The harness (`demo.ts`)
 * drives this class's `simulateIdleTimeoutAt`, `setCondition`, and
 * `forceSilentDrop` to reproduce exactly the symptoms described in Part 3 of
 * the brief (heartbeats succeed for 2-3 minutes then silently die, etc.).
 *
 * Crucially, a NAT flush is modeled as a *silent* failure: no close event,
 * no error - outbound sends simply vanish and inbound messages stop
 * arriving, exactly like a real stateful NAT dropping a mapping. This is
 * what forces the manager to detect deadness via heartbeat *timeout*
 * (absence of an ack) rather than via `onclose`/`onerror`, which mirrors
 * production reality.
 */
export class SimulatedSocket implements SocketLike {
  onopen: (() => void) | null = null;
  onclose: ((event: SocketCloseInfo) => void) | null = null;
  onerror: ((err: unknown) => void) | null = null;
  onmessage: ((event: SocketMessageInfo) => void) | null = null;

  private alive = true;
  /** When true, sends are swallowed and no messages arrive - simulates a NAT-dropped mapping. */
  private blackholed = false;
  private condition: SimulatedCondition = 'good';
  private readonly log: (msg: string) => void;

  /** The idle timeout this simulated network path will actually enforce. */
  private readonly trueIdleTimeoutMs: number;
  private lastActivityAt: number;
  private idleCheckTimer: ReturnType<typeof setInterval> | null = null;

  constructor(
    _url: string,
    opts: { trueIdleTimeoutMs?: number; log?: (msg: string) => void } = {}
  ) {
    this.trueIdleTimeoutMs = opts.trueIdleTimeoutMs ?? 150_000; // 2.5 min, mid-range of 30s-5min
    this.lastActivityAt = Date.now();
    this.log = opts.log ?? (() => {});

    // "Connect" asynchronously, like a real socket.
    setTimeout(() => {
      if (!this.alive) return;
      this.onopen?.();
      this.startIdleWatch();
    }, 50 + Math.random() * 100);
  }

  private startIdleWatch(): void {
    this.idleCheckTimer = setInterval(() => {
      if (!this.alive || this.blackholed) return;
      const idleFor = Date.now() - this.lastActivityAt;
      if (idleFor >= this.trueIdleTimeoutMs) {
        this.log(
          `[network] NAT/idle timeout reached after ${Math.round(idleFor / 1000)}s idle - blackholing connection`
        );
        this.blackholed = true;
      }
    }, 1000);
  }

  send(data: string): void {
    if (!this.alive) return;
    if (this.blackholed) {
      // Real NAT behavior: the send just disappears, no error raised
      // locally. The OS layer doesn't know the mapping is gone until it
      // itself times out or the app gives up waiting for a reply.
      return;
    }
    this.lastActivityAt = Date.now();

    const latency = this.currentLatencyMs();
    const dropped = Math.random() < this.currentLossRate();
    if (dropped) return;

    setTimeout(() => {
      if (!this.alive || this.blackholed) return;
      this.lastActivityAt = Date.now();
      this.respond(data);
    }, latency);
  }

  /** Simple echo/ack protocol: heartbeats get ack'd, intents get ack'd, anything else is echoed as an "event". */
  private respond(data: string): void {
    try {
      const msg = JSON.parse(data);
      if (msg.type === 'heartbeat') {
        this.deliver({ type: 'heartbeat-ack', id: msg.id, serverTime: Date.now() });
      } else if (msg.type === 'intent') {
        this.deliver({ type: 'intent-ack', id: msg.id });
      }
    } catch {
      // ignore malformed
    }
  }

  private deliver(obj: unknown): void {
    this.onmessage?.({ data: JSON.stringify(obj) });
  }

  /** Push a server-initiated event in, as if the backend emitted one (e.g. "flash sale"). */
  pushServerEvent(payload: unknown): void {
    if (!this.alive || this.blackholed) return;
    this.lastActivityAt = Date.now();
    this.deliver(payload);
  }

  close(_code?: number, reason?: string): void {
    if (!this.alive) return;
    this.alive = false;
    if (this.idleCheckTimer) clearInterval(this.idleCheckTimer);
    this.onclose?.({ code: 1000, reason: reason ?? 'closed' });
  }

  /** Test/demo hook: simulate an abrupt silent NAT drop right now, regardless of idle time. */
  forceSilentDrop(): void {
    this.blackholed = true;
  }

  setCondition(condition: SimulatedCondition): void {
    this.condition = condition;
    if (condition === 'nat-flushed') {
      this.forceSilentDrop();
    }
  }

  private currentLatencyMs(): number {
    switch (this.condition) {
      case 'good':
        return 40 + Math.random() * 40;
      case 'fair':
        return 150 + Math.random() * 250;
      case 'poor':
        return 400 + Math.random() * 800;
      default:
        return 100;
    }
  }

  private currentLossRate(): number {
    switch (this.condition) {
      case 'good':
        return 0.0;
      case 'fair':
        return 0.05;
      case 'poor':
        return 0.25;
      default:
        return 0;
    }
  }
}
