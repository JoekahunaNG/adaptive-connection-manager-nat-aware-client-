import { EventEmitter } from 'events';
import { Outbox } from './outbox';
import {
  AdaptiveConnectionManagerOptions,
  ConnectionMetrics,
  ConnectionState,
  NetworkQuality,
  SocketLike,
} from './types';

const DEFAULTS = {
  initialIdleTimeoutMs: 45_000, // conservative: near the low end of the 30s-5min carrier range
  minIdleTimeoutMs: 20_000,
  maxIdleTimeoutMs: 280_000,
  emaAlphaDown: 0.5, // react fast to a shorter observed timeout - safety first
  emaAlphaUp: 0.1, // probe upward slowly - a premature upward move costs a silent drop
  probeUpAfterSuccesses: 8,
  heartbeatSafetyMarginMin: 0.7,
  heartbeatSafetyMarginMax: 0.85,
  heartbeatJitterMs: 2000,
  heartbeatAckTimeoutMs: 8000,
  baseBackoffMs: 1000,
  maxBackoffMs: 60_000,
};

let idCounter = 0;
function nextId(prefix: string): string {
  idCounter += 1;
  return `${prefix}-${Date.now()}-${idCounter}`;
}

/**
 * AdaptiveConnectionManager
 * ==========================
 * Client-side connection manager for Clockit's phone companion app. Owns a
 * single logical connection to the backend event stream and is responsible
 * for keeping it alive (or quickly, cheaply resurrecting it) across the
 * hostile conditions described in the brief: NAT idle eviction, OEM process
 * freezing, and spotty 2G/3G links.
 *
 * Why an EMA-learned heartbeat instead of a fixed interval?
 * A fixed "safe" interval (e.g. always every 20s) would work everywhere but
 * waste battery and radio wake-ups on networks with a generous (5 min) NAT
 * timeout. A fixed "efficient" interval (e.g. every 4 min) would work on
 * generous networks but die constantly on aggressive ones (30s). Since the
 * brief is explicit that this varies by carrier/region, we instead *learn*
 * the effective timeout per-connection and keep heartbeats at 70-85% of it -
 * aggressive networks get frequent heartbeats (correctness), generous ones
 * get infrequent ones (battery), and the system self-corrects as conditions
 * change (e.g. switching from wifi to a 2G cell tower mid-session).
 *
 * Responsibility boundary: this class owns the phone<->backend WebSocket
 * path only. BLE delivery to the watch and watch-side UI are out of scope
 * here (see design doc Part 1.2), but `notifyBleLiveness()` and
 * `protect()/unprotect()` are the two integration seams this class exposes
 * to that layer and to the Android foreground-service/CompanionDeviceManager
 * wiring, respectively.
 */
export class AdaptiveConnectionManager extends EventEmitter {
  private readonly opts: Required<
    Pick<
      AdaptiveConnectionManagerOptions,
      | 'url'
      | 'initialIdleTimeoutMs'
      | 'minIdleTimeoutMs'
      | 'maxIdleTimeoutMs'
      | 'emaAlphaDown'
      | 'emaAlphaUp'
      | 'probeUpAfterSuccesses'
      | 'heartbeatSafetyMarginMin'
      | 'heartbeatSafetyMarginMax'
      | 'heartbeatJitterMs'
      | 'heartbeatAckTimeoutMs'
      | 'baseBackoffMs'
      | 'maxBackoffMs'
    >
  >;
  private readonly socketFactory: (url: string) => SocketLike;
  private readonly now: () => number;
  private readonly outbox: Outbox;

  private socket: SocketLike | null = null;
  private state: ConnectionState = 'disconnected';
  private intentionallyClosed = false;

  // --- Learned timeout state ---
  private learnedIdleTimeoutMs: number;
  private consecutiveHeartbeatSuccesses = 0;

  // --- Heartbeat state ---
  private heartbeatTimer: ReturnType<typeof setTimeout> | null = null;
  private heartbeatAckTimer: ReturnType<typeof setTimeout> | null = null;
  private pendingHeartbeatId: string | null = null;
  private pendingHeartbeatSentAt = 0;
  private lastHeartbeatRttMs: number | null = null;
  private lastActivityAt = 0;

  // --- Reconnect state ---
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private consecutiveFailures = 0;
  private lastSilentDropAt: number | null = null;

  // --- Protection / network quality (externally reported) ---
  private protectedMode = false;
  private networkQuality: NetworkQuality = 'good';

  constructor(options: AdaptiveConnectionManagerOptions) {
    super();
    this.opts = {
      url: options.url,
      initialIdleTimeoutMs: options.initialIdleTimeoutMs ?? DEFAULTS.initialIdleTimeoutMs,
      minIdleTimeoutMs: options.minIdleTimeoutMs ?? DEFAULTS.minIdleTimeoutMs,
      maxIdleTimeoutMs: options.maxIdleTimeoutMs ?? DEFAULTS.maxIdleTimeoutMs,
      emaAlphaDown: options.emaAlphaDown ?? DEFAULTS.emaAlphaDown,
      emaAlphaUp: options.emaAlphaUp ?? DEFAULTS.emaAlphaUp,
      probeUpAfterSuccesses: options.probeUpAfterSuccesses ?? DEFAULTS.probeUpAfterSuccesses,
      heartbeatSafetyMarginMin: options.heartbeatSafetyMarginMin ?? DEFAULTS.heartbeatSafetyMarginMin,
      heartbeatSafetyMarginMax: options.heartbeatSafetyMarginMax ?? DEFAULTS.heartbeatSafetyMarginMax,
      heartbeatJitterMs: options.heartbeatJitterMs ?? DEFAULTS.heartbeatJitterMs,
      heartbeatAckTimeoutMs: options.heartbeatAckTimeoutMs ?? DEFAULTS.heartbeatAckTimeoutMs,
      baseBackoffMs: options.baseBackoffMs ?? DEFAULTS.baseBackoffMs,
      maxBackoffMs: options.maxBackoffMs ?? DEFAULTS.maxBackoffMs,
    };
    this.learnedIdleTimeoutMs = this.opts.initialIdleTimeoutMs;
    this.now = options.now ?? (() => Date.now());
    this.socketFactory = options.socketFactory ?? this.defaultSocketFactory();
    this.outbox = new Outbox(options.outboxPersistPath ?? null);
  }

  private defaultSocketFactory(): (url: string) => SocketLike {
    return (url: string) => {
      // Lazy require so environments without `ws` (and the demo, which
      // always injects SimulatedSocket) never need it installed.
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      const WS = require('ws');
      const raw = new WS(url);
      const wrapper: SocketLike = {
        send: (data) => raw.send(data),
        close: (code, reason) => raw.close(code, reason),
        onopen: null,
        onclose: null,
        onerror: null,
        onmessage: null,
      };
      raw.on('open', () => wrapper.onopen?.());
      raw.on('close', (code: number, reason: Buffer) => wrapper.onclose?.({ code, reason: reason?.toString() }));
      raw.on('error', (err: unknown) => wrapper.onerror?.(err));
      raw.on('message', (data: Buffer) => wrapper.onmessage?.({ data: data.toString() }));
      return wrapper;
    };
  }

  // =========================================================================
  // Public API
  // =========================================================================

  connect(): void {
    if (this.state === 'connected' || this.state === 'connecting') return;
    this.intentionallyClosed = false;
    this.openSocket();
  }

  disconnect(): void {
    this.intentionallyClosed = true;
    this.clearHeartbeatTimers();
    this.clearReconnectTimer();
    this.socket?.close(1000, 'client disconnect');
    this.setState('disconnected');
  }

  /**
   * Simulates entering Android's foreground-service / CompanionDeviceManager
   * "protected" window (e.g. right after a BLE liveness signal suggests the
   * user is actively engaging, per design doc Part 1.2). While protected we
   * trust the OS not to freeze us imminently, so we don't need to pad
   * heartbeats defensively - see `effectiveHeartbeatMargin()`.
   */
  protect(): void {
    if (this.protectedMode) return;
    this.protectedMode = true;
    this.emit('protectedModeChanged', { protected: true });
    // A protection window is exactly the moment to confirm liveness quickly
    // if we're mid-backoff - don't make the user wait out a stale timer.
    if (this.state === 'reconnecting') {
      this.reconnectNow('entered protected window');
    }
  }

  unprotect(): void {
    if (!this.protectedMode) return;
    this.protectedMode = false;
    this.emit('protectedModeChanged', { protected: false });
  }

  /**
   * Called by the BLE layer when the watch reports liveness (e.g. still on
   * wrist, still in range). This is a *cheap* signal compared to a full
   * phone-side heartbeat: it tells us the companion relay path is probably
   * fine and lets us (a) defer an about-to-fire heartbeat a little rather
   * than waking the radio twice in quick succession, and (b) treat any
   * in-flight backoff as lower priority to shorten, since the user is
   * actively present.
   */
  notifyBleLiveness(): void {
    this.emit('bleLiveness');
    if (this.state === 'connected' && this.heartbeatTimer) {
      // Nudge the next heartbeat out by a small amount rather than fully
      // resetting it - we still want *some* phone-side confirmation on a
      // regular cadence, BLE liveness is corroborating evidence, not a
      // substitute for it.
      this.rescheduleHeartbeat(this.msUntilNextHeartbeat() + 3000);
    } else if (this.state === 'reconnecting') {
      this.reconnectNow('BLE liveness signal during backoff');
    }
  }

  /** Report current radio/network quality from the OS layer (e.g. NetworkCallback signal strength). */
  reportNetworkQuality(quality: NetworkQuality): void {
    this.networkQuality = quality;
  }

  /**
   * Enqueue a user intent (e.g. the double-tap "buy") for delivery. Returns
   * immediately once the item is durably queued - delivery and ack happen
   * asynchronously and are reflected via 'intentQueued' / 'intentAcked'.
   * This is what makes the double-tap -> transaction path survive a frozen
   * process or a dead connection: the intent is on disk before this method
   * returns, and will be replayed on the next successful connection no
   * matter what happens to the process in between.
   */
  sendIntent(payload: unknown): string {
    const id = nextId('intent');
    const item = this.outbox.enqueue(id, payload, this.now());
    this.emit('intentQueued', item);
    this.flushOutboxItem(item.id);
    return id;
  }

  getMetrics(): ConnectionMetrics {
    return {
      state: this.state,
      learnedIdleTimeoutMs: Math.round(this.learnedIdleTimeoutMs),
      consecutiveFailures: this.consecutiveFailures,
      lastHeartbeatRttMs: this.lastHeartbeatRttMs,
      reconnectAttempts: this.reconnectAttempts,
      outboxSize: this.outbox.size(),
      lastSilentDropAt: this.lastSilentDropAt,
      protectedMode: this.protectedMode,
      currentHeartbeatIntervalMs: Math.round(this.plannedHeartbeatIntervalMs()),
      networkQuality: this.networkQuality,
    };
  }

  // =========================================================================
  // Connection lifecycle
  // =========================================================================

  private openSocket(): void {
    this.setState('connecting');
    const socket = this.socketFactory(this.opts.url);
    this.socket = socket;

    socket.onopen = () => this.handleOpen();
    socket.onclose = (evt) => this.handleClose(evt.reason ?? 'unknown');
    socket.onerror = (err) => this.handleError(err);
    socket.onmessage = (evt) => this.handleMessage(evt.data);
  }

  private handleOpen(): void {
    this.setState('connected');
    this.reconnectAttempts = 0;
    this.consecutiveFailures = 0;
    this.lastActivityAt = this.now();
    this.emit('connected');
    this.flushAllOutbox();
    this.scheduleHeartbeat();
  }

  private handleClose(reason: string): void {
    this.clearHeartbeatTimers();
    const wasConnected = this.state === 'connected';
    this.socket = null;
    this.emit('disconnected', { reason });
    if (this.intentionallyClosed) {
      this.setState('disconnected');
      return;
    }
    if (wasConnected) {
      // A clean close after being connected is *not* evidence about the
      // idle timeout (we weren't idling out, the server/OS closed us for
      // some other reason) so we don't feed the EMA here.
    }
    this.scheduleReconnect('socket closed: ' + reason);
  }

  private handleError(_err: unknown): void {
    // onerror is typically followed by onclose for real sockets; we let
    // handleClose drive reconnect to avoid double-scheduling. We still
    // count it towards failure pressure for backoff sizing.
    this.consecutiveFailures += 1;
  }

  private handleMessage(raw: string): void {
    this.lastActivityAt = this.now();
    let msg: any;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    if (msg.type === 'heartbeat-ack' && msg.id === this.pendingHeartbeatId) {
      this.handleHeartbeatAck();
      return;
    }
    if (msg.type === 'intent-ack' && typeof msg.id === 'string') {
      const acked = this.outbox.ack(msg.id);
      if (acked) this.emit('intentAcked', acked);
      return;
    }
    // Anything else is a server-pushed, time-critical event (e.g. "flash
    // sale", "stop-loss triggered") for the watch engagement layer.
    this.emit('event', msg);
  }

  // =========================================================================
  // Heartbeat / idle-timeout learning
  // =========================================================================

  /** 70-85% of the learned timeout, widened slightly in poor conditions to avoid false-positive failures. */
  private effectiveHeartbeatMargin(): number {
    const [lo, hi] = [this.opts.heartbeatSafetyMarginMin, this.opts.heartbeatSafetyMarginMax];
    const margin = lo + Math.random() * (hi - lo);
    // Poor-quality links have higher loss/latency, so a heartbeat is more
    // likely to be delayed or dropped for reasons unrelated to NAT eviction.
    // Protected mode means we're not worried about being frozen, so we can
    // afford to sit closer to the generous end of the margin and save battery.
    if (this.networkQuality === 'poor') return Math.min(margin, lo); // bias toward more-frequent
    if (this.protectedMode) return Math.max(margin, hi * 0.95);
    return margin;
  }

  private plannedHeartbeatIntervalMs(): number {
    const base = this.learnedIdleTimeoutMs * this.effectiveHeartbeatMargin();
    const jitter = Math.random() * this.opts.heartbeatJitterMs;
    // Absolute floor, independent of minIdleTimeoutMs: this guards against
    // a pathological busy-heartbeat loop if learnedIdleTimeoutMs is ever
    // configured (or, via a future bug, learned) down near zero - we never
    // want to wake the radio more than once every 2s no matter what the
    // learned value says. In realistic deployments (NAT timeouts of tens of
    // seconds to minutes) this floor never binds; it only matters as a
    // safety net. Note this means extremely small `minIdleTimeoutMs` values
    // (as used in fast unit tests) won't produce proportionally fast
    // heartbeats - tests account for that explicitly rather than lowering
    // this floor for everyone.
    return Math.max(2000, base - jitter);
  }

  private msUntilNextHeartbeat(): number {
    return this.plannedHeartbeatIntervalMs();
  }

  private scheduleHeartbeat(): void {
    this.clearHeartbeatTimers();
    const intervalMs = this.plannedHeartbeatIntervalMs();
    this.heartbeatTimer = setTimeout(() => this.sendHeartbeat(), intervalMs);
  }

  private rescheduleHeartbeat(inMs: number): void {
    if (this.heartbeatTimer) clearTimeout(this.heartbeatTimer);
    this.heartbeatTimer = setTimeout(() => this.sendHeartbeat(), inMs);
  }

  private sendHeartbeat(): void {
    if (!this.socket || this.state !== 'connected') return;
    const id = nextId('hb');
    this.pendingHeartbeatId = id;
    this.pendingHeartbeatSentAt = this.now();
    this.socket.send(JSON.stringify({ type: 'heartbeat', id }));

    this.heartbeatAckTimer = setTimeout(
      () => this.handleHeartbeatTimeout(),
      this.opts.heartbeatAckTimeoutMs
    );
  }

  private handleHeartbeatAck(): void {
    if (this.heartbeatAckTimer) {
      clearTimeout(this.heartbeatAckTimer);
      this.heartbeatAckTimer = null;
    }
    this.lastHeartbeatRttMs = this.now() - this.pendingHeartbeatSentAt;
    this.pendingHeartbeatId = null;
    this.consecutiveFailures = 0;
    this.consecutiveHeartbeatSuccesses += 1;
    this.maybeProbeTimeoutUpward();
    this.scheduleHeartbeat();
  }

  /**
   * No ack arrived in time. This is our *only* signal for a NAT-style silent
   * drop (see SimulatedSocket docs / design doc Part 3): there's no
   * onclose/onerror, just silence. We treat the elapsed idle time as
   * evidence of an upper bound on the true timeout and adjust down via EMA,
   * then force the dead socket closed and reconnect.
   */
  private handleHeartbeatTimeout(): void {
    const idleFor = this.now() - this.lastActivityAt;
    this.emit('heartbeatTimeout', { intervalMs: idleFor });
    this.consecutiveHeartbeatSuccesses = 0;
    this.lastSilentDropAt = this.now();
    this.learnIdleTimeoutDown(idleFor);

    this.pendingHeartbeatId = null;
    this.clearHeartbeatTimers();
    // The socket object is still technically "open" from the transport's
    // point of view (that's what makes this a silent drop) - force it
    // closed so we don't leak it. We detach its callbacks first: closing it
    // would otherwise cascade into handleClose() -> scheduleReconnect(),
    // and we're about to call scheduleReconnect() ourselves below with a
    // more accurate reason - without detaching, both paths fire and
    // reconnectAttempts/backoff get double-counted for a single failure.
    const deadSocket = this.socket;
    this.socket = null;
    if (deadSocket) {
      deadSocket.onclose = null;
      deadSocket.onerror = null;
      deadSocket.onmessage = null;
      try {
        deadSocket.close(4000, 'heartbeat timeout');
      } catch {
        /* already gone */
      }
    }
    this.emit('disconnected', { reason: 'heartbeat timeout (likely NAT/idle drop)' });
    this.scheduleReconnect('heartbeat timeout (likely NAT/idle drop)');
  }

  private learnIdleTimeoutDown(observedMs: number): void {
    const previous = this.learnedIdleTimeoutMs;
    // The failure happened at `observedMs` of idle time, which includes our
    // own ack-timeout grace period - subtract it so we learn the *network's*
    // timeout, not network timeout + our grace period.
    const inferredCeiling = Math.max(
      this.opts.minIdleTimeoutMs,
      observedMs - this.opts.heartbeatAckTimeoutMs
    );
    const alpha = this.opts.emaAlphaDown;
    const next = clamp(
      alpha * inferredCeiling + (1 - alpha) * previous,
      this.opts.minIdleTimeoutMs,
      this.opts.maxIdleTimeoutMs
    );
    // Only move down - a failure is never evidence the timeout is longer.
    this.learnedIdleTimeoutMs = Math.min(next, previous);
    if (this.learnedIdleTimeoutMs !== previous) {
      this.emit('idleTimeoutLearned', { previousMs: previous, newMs: this.learnedIdleTimeoutMs, direction: 'down' });
    }
  }

  /**
   * After a run of clean heartbeats, cautiously nudge the learned timeout
   * back up. Rationale: networks change (wifi -> cell, or a carrier with a
   * generous NAT timeout) and a manager that only ever ratchets down will
   * converge to the worst timeout it has ever seen and stay there forever,
   * burning battery on a network that could support far less frequent
   * heartbeats. We probe *slowly* (low alpha, requires N successes) because
   * the cost of probing too far is a silent drop, while the cost of probing
   * too slowly is just some extra battery - an intentionally asymmetric
   * trade-off (see design doc Part 4.2).
   */
  private maybeProbeTimeoutUpward(): void {
    if (this.consecutiveHeartbeatSuccesses < this.opts.probeUpAfterSuccesses) return;
    this.consecutiveHeartbeatSuccesses = 0;
    const previous = this.learnedIdleTimeoutMs;
    const alpha = this.opts.emaAlphaUp;
    const probeTarget = Math.min(this.opts.maxIdleTimeoutMs, previous * 1.5);
    const next = clamp(
      alpha * probeTarget + (1 - alpha) * previous,
      this.opts.minIdleTimeoutMs,
      this.opts.maxIdleTimeoutMs
    );
    if (next > previous) {
      this.learnedIdleTimeoutMs = next;
      this.emit('idleTimeoutLearned', { previousMs: previous, newMs: next, direction: 'up' });
    }
  }

  private clearHeartbeatTimers(): void {
    if (this.heartbeatTimer) {
      clearTimeout(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    if (this.heartbeatAckTimer) {
      clearTimeout(this.heartbeatAckTimer);
      this.heartbeatAckTimer = null;
    }
  }

  // =========================================================================
  // Reconnection
  // =========================================================================

  private scheduleReconnect(reasonForLog: string): void {
    if (this.intentionallyClosed) return;
    this.setState('reconnecting');
    this.consecutiveFailures += 1;
    this.reconnectAttempts += 1;

    const delay = this.computeBackoffMs(this.reconnectAttempts);
    this.emit('reconnecting', { attempt: this.reconnectAttempts, delayMs: delay });
    this.clearReconnectTimer();
    this.reconnectTimer = setTimeout(() => this.openSocket(), delay);
  }

  /** Full-jitter exponential backoff, scaled by observed network quality. */
  private computeBackoffMs(attempt: number): number {
    const qualityMultiplier = this.networkQuality === 'poor' ? 1.6 : this.networkQuality === 'fair' ? 1.2 : 1.0;
    const exp = Math.min(this.opts.maxBackoffMs, this.opts.baseBackoffMs * 2 ** (attempt - 1) * qualityMultiplier);
    // Full jitter (0..exp) rather than "exp +/- jitter": with many phones
    // reconnecting after a shared infra blip, full jitter avoids a
    // thundering herd hitting the backend in lockstep.
    return Math.round(Math.random() * exp);
  }

  /** Jump the reconnect timer to "now" - used by protect()/notifyBleLiveness() to shorten a wait we now have reason to believe is unnecessary. */
  private reconnectNow(_reason: string): void {
    if (this.state !== 'reconnecting') return;
    this.clearReconnectTimer();
    this.openSocket();
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  // =========================================================================
  // Outbox flushing
  // =========================================================================

  private flushAllOutbox(): void {
    for (const item of this.outbox.all()) {
      this.flushOutboxItem(item.id);
    }
  }

  private flushOutboxItem(id: string): void {
    if (this.state !== 'connected' || !this.socket) return;
    const item = this.outbox.all().find((i) => i.id === id);
    if (!item) return;
    this.outbox.markAttempt(id, this.now());
    this.socket.send(JSON.stringify({ type: 'intent', id: item.id, payload: item.payload }));
  }

  // =========================================================================
  // Misc
  // =========================================================================

  private setState(state: ConnectionState): void {
    this.state = state;
  }
}

function clamp(v: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, v));
}
