import { randomUUID } from 'crypto';
import { EventEmitter } from 'events';
import { EventCursor } from './eventCursor';
import { Outbox } from './outbox';
import {
  AdaptiveConnectionManagerOptions,
  ConnectionMetrics,
  ConnectionState,
  IntentStatus,
  NetworkQuality,
  OutboxItem,
  SendIntentOptions,
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
  minPullForwardIntervalMs: 5000,
  bleProbeIdleFraction: 0.5,
  bleProbeCooldownMs: 10_000,
  intentAckDeadlineMs: 5000,
  eventAckDelayMs: 1000,
};

// Ack-timeout scaling per reported network quality. A poor 2G/3G link can
// take seconds to round-trip a healthy heartbeat; waiting the "good" budget
// there would tear down live connections as false silent drops.
const ACK_TIMEOUT_MULTIPLIER: Record<NetworkQuality, number> = {
  good: 1,
  fair: 1.5,
  poor: 2.5,
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
      | 'minPullForwardIntervalMs'
      | 'bleProbeIdleFraction'
      | 'bleProbeCooldownMs'
      | 'intentAckDeadlineMs'
      | 'eventAckDelayMs'
    >
  >;
  private readonly socketFactory: (url: string) => SocketLike;
  private readonly now: () => number;
  private readonly outbox: Outbox;
  private readonly eventCursor: EventCursor;

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
  // Only heartbeats sent on the regular schedule (at 70-85% of the learned
  // timeout) are evidence that the timeout could be longer. Early probes
  // fire at shorter idle times and must not drive upward probing.
  private pendingHeartbeatIsScheduled = false;
  // Captured at send time so a quality change mid-flight can't skew how the
  // timeout is interpreted.
  private pendingHeartbeatAckTimeoutMs = 0;
  private pendingHeartbeatQuality: NetworkQuality = 'good';
  private currentHeartbeatIntervalMs = 0;
  private lastHeartbeatRttMs: number | null = null;
  private lastActivityAt = 0;
  private lastBleProbeAt = Number.NEGATIVE_INFINITY;

  // --- Reconnect state ---
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private consecutiveFailures = 0;
  private lastSilentDropAt: number | null = null;
  private lastPullForwardAt = Number.NEGATIVE_INFINITY;

  // --- Per-intent timers, keyed by intent id ---
  private intentExpiryTimers = new Map<string, ReturnType<typeof setTimeout>>();
  private intentAckDeadlineTimers = new Map<string, ReturnType<typeof setTimeout>>();

  // --- Server event stream ---
  private eventAckTimer: ReturnType<typeof setTimeout> | null = null;
  // The cursor we last asked to resume from because of a gap, so one gap
  // produces one resume request rather than one per out-of-order event.
  private gapResumeRequestedFor: number | null = null;

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
      minPullForwardIntervalMs: options.minPullForwardIntervalMs ?? DEFAULTS.minPullForwardIntervalMs,
      bleProbeIdleFraction: options.bleProbeIdleFraction ?? DEFAULTS.bleProbeIdleFraction,
      bleProbeCooldownMs: options.bleProbeCooldownMs ?? DEFAULTS.bleProbeCooldownMs,
      intentAckDeadlineMs: options.intentAckDeadlineMs ?? DEFAULTS.intentAckDeadlineMs,
      eventAckDelayMs: options.eventAckDelayMs ?? DEFAULTS.eventAckDelayMs,
    };
    this.eventCursor = new EventCursor(options.eventCursorPersistPath ?? null);
    this.learnedIdleTimeoutMs = this.opts.initialIdleTimeoutMs;
    this.now = options.now ?? (() => Date.now());
    this.socketFactory = options.socketFactory ?? this.defaultSocketFactory();
    this.outbox = new Outbox(options.outboxPersistPath ?? null);
    const backupPath = this.outbox.corruptBackupPath;
    if (backupPath) {
      // Deferred so listeners attached right after construction still see it.
      process.nextTick(() => this.emit('outboxCorrupt', { backupPath }));
    }
    // Items reloaded after a restart may have expired while the process was
    // dead; a 0ms timer fires after the caller has attached listeners.
    for (const item of this.outbox.all()) this.armExpiry(item);
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
    this.clearConnectionTimers();
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
   * Called by the BLE layer when the watch reports liveness (still on wrist,
   * still in range). It says the user is present - it says nothing about
   * the phone's carrier NAT mapping, so it must never postpone a heartbeat:
   * a frequent liveness signal that deferred heartbeats would starve them
   * entirely and guarantee an idle-timeout drop.
   *
   * What it *does* change is how much a fast failure-detection is worth.
   * With an engaged user, an intent may be seconds away, so:
   *  - connected but quiet for a while -> probe now (rate-limited) rather
   *    than discover a dead path only after the user double-taps;
   *  - mid-backoff -> pull the reconnect forward (rate-limited).
   */
  notifyBleLiveness(): void {
    this.emit('bleLiveness');
    if (this.state === 'connected') {
      const now = this.now();
      const quietFor = now - this.lastActivityAt;
      const quietThreshold = this.learnedIdleTimeoutMs * this.opts.bleProbeIdleFraction;
      if (
        this.pendingHeartbeatId === null &&
        quietFor >= quietThreshold &&
        now - this.lastBleProbeAt >= this.opts.bleProbeCooldownMs
      ) {
        this.lastBleProbeAt = now;
        this.clearHeartbeatTimers();
        this.sendHeartbeat(false);
      }
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
   * asynchronously and are reflected via 'intentQueued' / 'intentAcked' /
   * 'intentResult' (or 'intentExpired' if `ttlMs` runs out first).
   * This is what makes the double-tap -> transaction path survive a frozen
   * process or a dead connection: the intent is on disk before this method
   * returns, and will be replayed on the next successful connection no
   * matter what happens to the process in between.
   *
   * The id is a random UUID: it's the backend's idempotency key, so it must
   * stay unique across devices and app reinstalls, which a timestamp plus
   * an in-process counter does not guarantee.
   */
  sendIntent(payload: unknown, options: SendIntentOptions = {}): string {
    const id = randomUUID();
    const now = this.now();
    const validUntil = options.ttlMs !== undefined ? now + options.ttlMs : null;
    const item = this.outbox.enqueue(id, payload, now, validUntil);
    this.emit('intentQueued', item);
    this.armExpiry(item);
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
      currentHeartbeatIntervalMs: Math.round(this.currentHeartbeatIntervalMs),
      networkQuality: this.networkQuality,
      lastEventSeq: this.eventCursor.get(),
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
    this.lastActivityAt = this.now();
    this.emit('connected');
    // Resume first, so the backend starts replaying missed events (and any
    // intent results lost with the last socket) before new intents land.
    this.gapResumeRequestedFor = null;
    this.sendResume();
    this.flushAllOutbox();
    this.scheduleHeartbeat();
  }

  private sendResume(): void {
    this.socket?.send(JSON.stringify({ type: 'resume', lastEventSeq: this.eventCursor.get() }));
  }

  private handleClose(reason: string): void {
    this.clearConnectionTimers();
    this.socket = null;
    this.emit('disconnected', { reason });
    if (this.intentionallyClosed) {
      this.setState('disconnected');
      return;
    }
    // An explicit close is not evidence about the NAT idle timeout (the
    // server/OS closed us for some other reason), so the EMA isn't fed here.
    this.scheduleReconnect('socket closed: ' + reason);
  }

  private handleError(_err: unknown): void {
    // Real sockets always follow onerror with onclose, and handleClose ->
    // scheduleReconnect does the failure counting; counting here as well
    // would score one failure twice.
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
      this.settleIntent(msg.id);
      return;
    }
    if (msg.type === 'stream-head' && typeof msg.seq === 'number') {
      // Reply to a fresh client's resume. Without a cursor, a first event
      // lost in a drop could never be asked for again.
      if (this.eventCursor.get() === null) this.eventCursor.set(msg.seq);
      return;
    }
    if (msg.type === 'resync-required' && typeof msg.oldestSeq === 'number') {
      // Everything before oldestSeq is gone server-side. Move the cursor up
      // to it so the replay that follows is accepted as contiguous.
      this.eventCursor.set(msg.oldestSeq - 1);
      this.emit('resyncRequired', { oldestSeq: msg.oldestSeq });
      return;
    }
    if (typeof msg.seq === 'number') {
      this.handleSequencedMessage(msg);
      return;
    }
    this.dispatchServerMessage(msg);
  }

  /**
   * Server events are stamped with a monotonic `seq`. We accept them strictly
   * in order: duplicates (a replay overlapping what we already have) are
   * dropped, and a gap means something was lost, so we ask the backend to
   * resume from our cursor rather than deliver out of order. A single TCP
   * connection doesn't reorder, so gaps should only come from server-side
   * bugs or a racing replay - this is a guard, not the normal path.
   */
  private handleSequencedMessage(msg: any): void {
    const last = this.eventCursor.get();
    if (last !== null && msg.seq <= last) {
      this.scheduleEventAck(); // re-ack so the backend stops resending
      return;
    }
    if (last !== null && msg.seq > last + 1) {
      if (this.gapResumeRequestedFor !== last) {
        this.gapResumeRequestedFor = last;
        this.sendResume();
      }
      return;
    }
    this.dispatchServerMessage(msg);
    // Advance after dispatch: a crash in between replays this event on
    // restart instead of losing it (at-least-once).
    this.eventCursor.set(msg.seq);
    this.scheduleEventAck();
  }

  private dispatchServerMessage(msg: any): void {
    if (msg.type === 'intent-result' && typeof msg.id === 'string') {
      // A result implies receipt, so it also settles the intent if the
      // intent-ack itself was lost in flight.
      this.settleIntent(msg.id);
      const status: IntentStatus = msg.status === 'executed' ? 'executed' : 'rejected';
      this.emit('intentResult', {
        id: msg.id,
        status,
        ...(typeof msg.reason === 'string' ? { reason: msg.reason } : {}),
      });
      return;
    }
    // Anything else is a server-pushed, time-critical event (e.g. "flash
    // sale", "stop-loss triggered") for the watch engagement layer.
    this.emit('event', msg);
  }

  /** One ack for the highest seq per window, so a burst of events costs one radio write. */
  private scheduleEventAck(): void {
    if (this.eventAckTimer) return;
    this.eventAckTimer = setTimeout(() => {
      this.eventAckTimer = null;
      const seq = this.eventCursor.get();
      if (seq !== null && this.state === 'connected') {
        this.socket?.send(JSON.stringify({ type: 'event-ack', seq }));
      }
    }, this.opts.eventAckDelayMs);
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

  private scheduleHeartbeat(): void {
    this.clearHeartbeatTimers();
    const intervalMs = this.plannedHeartbeatIntervalMs();
    this.currentHeartbeatIntervalMs = intervalMs;
    this.heartbeatTimer = setTimeout(() => this.sendHeartbeat(true), intervalMs);
  }

  private effectiveAckTimeoutMs(): number {
    return this.opts.heartbeatAckTimeoutMs * ACK_TIMEOUT_MULTIPLIER[this.networkQuality];
  }

  private sendHeartbeat(scheduled: boolean): void {
    if (!this.socket || this.state !== 'connected' || this.pendingHeartbeatId !== null) return;
    this.heartbeatTimer = null;
    const id = nextId('hb');
    this.pendingHeartbeatId = id;
    this.pendingHeartbeatIsScheduled = scheduled;
    this.pendingHeartbeatSentAt = this.now();
    this.pendingHeartbeatAckTimeoutMs = this.effectiveAckTimeoutMs();
    this.pendingHeartbeatQuality = this.networkQuality;
    this.socket.send(JSON.stringify({ type: 'heartbeat', id }));

    this.heartbeatAckTimer = setTimeout(
      () => this.handleHeartbeatTimeout(),
      this.pendingHeartbeatAckTimeoutMs
    );
  }

  private handleHeartbeatAck(): void {
    if (this.heartbeatAckTimer) {
      clearTimeout(this.heartbeatAckTimer);
      this.heartbeatAckTimer = null;
    }
    this.lastHeartbeatRttMs = this.now() - this.pendingHeartbeatSentAt;
    this.pendingHeartbeatId = null;
    // A round trip, not a socket open, is what proves the path healthy: a
    // carrier can accept the TCP handshake and still blackhole the session
    // seconds later. Resetting backoff on open would let a connect-then-die
    // loop retry at base delay forever.
    this.consecutiveFailures = 0;
    this.reconnectAttempts = 0;
    if (this.pendingHeartbeatIsScheduled) {
      this.consecutiveHeartbeatSuccesses += 1;
      this.maybeProbeTimeoutUpward();
    }
    this.scheduleHeartbeat();
  }

  /**
   * No ack arrived in time. This is our *only* signal for a NAT-style silent
   * drop (see SimulatedSocket docs / design doc Part 3): there's no
   * onclose/onerror, just silence. We treat the elapsed idle time as
   * evidence of an upper bound on the true timeout and adjust down via EMA,
   * then force the dead socket closed and reconnect.
   *
   * Exception: on a 'poor' link a missing ack is at least as likely to be
   * packet loss as NAT eviction. We still reconnect (the path is unusable
   * either way) but don't feed the EMA, or every lossy stretch would ratchet
   * the learned timeout down and cost battery for no NAT benefit.
   */
  private handleHeartbeatTimeout(): void {
    const idleFor = this.now() - this.lastActivityAt;
    const usedForLearning = this.pendingHeartbeatQuality !== 'poor';
    this.emit('heartbeatTimeout', {
      intervalMs: idleFor,
      ackTimeoutMs: this.pendingHeartbeatAckTimeoutMs,
      usedForLearning,
    });
    this.consecutiveHeartbeatSuccesses = 0;
    this.lastSilentDropAt = this.now();
    if (usedForLearning) this.learnIdleTimeoutDown(idleFor);

    this.pendingHeartbeatId = null;
    this.clearConnectionTimers();
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
      observedMs - this.pendingHeartbeatAckTimeoutMs
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

  /** Everything tied to the current socket; called whenever that socket goes away. */
  private clearConnectionTimers(): void {
    this.clearHeartbeatTimers();
    this.clearIntentAckDeadlines();
    if (this.eventAckTimer) {
      // The next resume carries the cursor, which acks implicitly.
      clearTimeout(this.eventAckTimer);
      this.eventAckTimer = null;
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

  /**
   * Jump the reconnect timer to "now" - used by protect()/notifyBleLiveness()
   * when an engaged user makes a fast reconnect worth an extra radio wake-up.
   * Rate-limited: during a real outage the watch keeps reporting liveness,
   * and honoring every signal would turn backoff into a reconnect storm.
   * Within the cooldown the normal backoff timer is left to run.
   */
  private reconnectNow(_reason: string): void {
    if (this.state !== 'reconnecting') return;
    const now = this.now();
    if (now - this.lastPullForwardAt < this.opts.minPullForwardIntervalMs) return;
    this.lastPullForwardAt = now;
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
    const item = this.outbox.get(id);
    if (!item) return;
    // Its expiry timer may not have fired yet (timers are coarse, and a
    // process thawed from a freeze runs queued callbacks late).
    if (item.validUntil !== null && this.now() >= item.validUntil) {
      this.expireIntent(id);
      return;
    }
    this.outbox.markAttempt(id, this.now());
    this.socket.send(
      JSON.stringify({ type: 'intent', id: item.id, payload: item.payload, validUntil: item.validUntil })
    );
    this.armAckDeadline(id);
  }

  /**
   * An unacked intent is the most latency-sensitive thing in the system, so
   * it shouldn't have to wait for the next scheduled heartbeat to find out
   * the path is dead. If the ack is late, probe immediately: a healthy path
   * acks the heartbeat and the intent ack was just slow; a dead one fails
   * the heartbeat, reconnects, and replays the intent.
   */
  private armAckDeadline(id: string): void {
    this.clearTimer(this.intentAckDeadlineTimers, id);
    this.intentAckDeadlineTimers.set(
      id,
      setTimeout(() => {
        this.intentAckDeadlineTimers.delete(id);
        if (!this.outbox.get(id) || this.pendingHeartbeatId !== null) return;
        this.clearHeartbeatTimers();
        this.sendHeartbeat(false);
      }, this.opts.intentAckDeadlineMs)
    );
  }

  private armExpiry(item: OutboxItem): void {
    if (item.validUntil === null) return;
    const timer = setTimeout(() => this.expireIntent(item.id), Math.max(0, item.validUntil - this.now()));
    // An intent waiting to expire shouldn't on its own keep the process alive.
    timer.unref?.();
    this.intentExpiryTimers.set(item.id, timer);
  }

  private expireIntent(id: string): void {
    this.clearTimer(this.intentExpiryTimers, id);
    this.clearTimer(this.intentAckDeadlineTimers, id);
    const item = this.outbox.remove(id);
    if (item) this.emit('intentExpired', item);
  }

  /** The backend has the intent (ack or result): stop retrying and stop its timers. */
  private settleIntent(id: string): void {
    this.clearTimer(this.intentExpiryTimers, id);
    this.clearTimer(this.intentAckDeadlineTimers, id);
    const acked = this.outbox.remove(id);
    if (acked) this.emit('intentAcked', acked);
  }

  private clearIntentAckDeadlines(): void {
    // Re-armed per item when the outbox is replayed on the next connect.
    for (const timer of this.intentAckDeadlineTimers.values()) clearTimeout(timer);
    this.intentAckDeadlineTimers.clear();
  }

  private clearTimer(timers: Map<string, ReturnType<typeof setTimeout>>, id: string): void {
    const timer = timers.get(id);
    if (timer) {
      clearTimeout(timer);
      timers.delete(id);
    }
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
