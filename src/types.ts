/**
 * Shared types for the Adaptive Connection Manager.
 *
 * Design note: the manager depends on a `SocketLike` abstraction rather than
 * the `ws` library or the browser `WebSocket` directly. This lets the same
 * manager run against:
 *   - a real WebSocket (web/Node client),
 *   - the SimulatedSocket used by the demo/test harness to reproduce NAT
 *     flushes, latency, and packet loss on demand without a real network.
 * On Android this interface would be backed by OkHttp's WebSocket or a
 * similar client; the manager's logic never changes.
 */

export type ConnectionState =
  | 'disconnected'
  | 'connecting'
  | 'connected'
  | 'reconnecting';

export type NetworkQuality = 'good' | 'fair' | 'poor';

export interface SocketCloseInfo {
  code?: number;
  reason?: string;
}

export interface SocketMessageInfo {
  data: string;
}

/** Minimal surface the manager needs from a transport. */
export interface SocketLike {
  send(data: string): void;
  close(code?: number, reason?: string): void;
  onopen: (() => void) | null;
  onclose: ((event: SocketCloseInfo) => void) | null;
  onerror: ((err: unknown) => void) | null;
  onmessage: ((event: SocketMessageInfo) => void) | null;
}

export type SocketFactory = (url: string) => SocketLike;

/** An outbound item (a user intent, e.g. "double-tap buy") awaiting ack. */
export interface OutboxItem {
  id: string;
  /** Opaque payload the backend understands, e.g. { type: 'intent', ... } */
  payload: unknown;
  enqueuedAt: number;
  /**
   * Wall-clock deadline after which acting on this intent is wrong (e.g. the
   * flash sale it was buying into has closed). null = never expires. Sent to
   * the backend too, which rejects it if it arrives late.
   */
  validUntil: number | null;
  attempts: number;
  lastAttemptAt: number | null;
}

export type IntentStatus = 'executed' | 'rejected';

export interface IntentResult {
  id: string;
  status: IntentStatus;
  /** Backend-supplied reason for a rejection, e.g. 'expired', 'sold-out'. */
  reason?: string;
}

export interface SendIntentOptions {
  /** How long from now the intent stays actionable. Omit for no expiry. */
  ttlMs?: number;
}

export interface ConnectionMetrics {
  state: ConnectionState;
  /** Current EMA-learned effective idle timeout for this network path. */
  learnedIdleTimeoutMs: number;
  /** Consecutive heartbeat/connection failures since the last success. */
  consecutiveFailures: number;
  /** Round-trip time of the most recently acked heartbeat, if any. */
  lastHeartbeatRttMs: number | null;
  /** How many reconnect attempts have been made in the current backoff run. */
  reconnectAttempts: number;
  /** Items still waiting for a server ack. */
  outboxSize: number;
  /** Timestamp of the last time we inferred a NAT-style silent drop. */
  lastSilentDropAt: number | null;
  /** Whether the connection is currently "protected" (foreground service / companion device active). */
  protectedMode: boolean;
  /** Interval of the most recently scheduled heartbeat (post safety-margin + jitter); 0 before the first. */
  currentHeartbeatIntervalMs: number;
  networkQuality: NetworkQuality;
  /** Highest server event seq handled, or null before the first sequenced event. */
  lastEventSeq: number | null;
}

export interface AdaptiveConnectionManagerOptions {
  url: string;
  /** Defaults to a real WebSocket via the `ws` package if omitted. */
  socketFactory?: SocketFactory;

  /**
   * Starting assumption for the NAT/OS idle timeout, before we've learned
   * anything about this specific network path. Chosen conservatively near
   * the low end of the documented 30s-5min carrier range (see design doc,
   * Part 1 item 2) so we don't lose the connection while still calibrating.
   */
  initialIdleTimeoutMs?: number;
  minIdleTimeoutMs?: number;
  maxIdleTimeoutMs?: number;
  /** EMA smoothing factor applied when a *shorter* timeout is observed (0-1). */
  emaAlphaDown?: number;
  /** EMA smoothing factor applied when cautiously probing *upward* (0-1). */
  emaAlphaUp?: number;
  /** Consecutive clean heartbeat cycles required before probing the timeout upward. */
  probeUpAfterSuccesses?: number;

  /** Heartbeat fires at a random point within [min, max] * learnedIdleTimeoutMs. */
  heartbeatSafetyMarginMin?: number; // default 0.70
  heartbeatSafetyMarginMax?: number; // default 0.85
  /** Absolute jitter (ms) subtracted from the computed interval, randomized. */
  heartbeatJitterMs?: number;
  /**
   * How long to wait for a heartbeat ack before declaring the path dead, on
   * a 'good' network. Scaled up 1.5x on 'fair' and 2.5x on 'poor'.
   */
  heartbeatAckTimeoutMs?: number;

  baseBackoffMs?: number;
  maxBackoffMs?: number;
  /**
   * Minimum gap between two "pull the pending reconnect forward" actions
   * (from protect() or BLE liveness). Without it, a watch pinging liveness
   * every few seconds during an outage would bypass backoff entirely.
   */
  minPullForwardIntervalMs?: number;

  /**
   * On BLE liveness, send an early heartbeat if the backend path has been
   * silent for more than this fraction of the learned idle timeout.
   */
  bleProbeIdleFraction?: number;
  /** Minimum gap between two BLE-triggered early heartbeats. */
  bleProbeCooldownMs?: number;

  /**
   * If a sent intent isn't acked within this window, probe the connection
   * with an immediate heartbeat instead of waiting for the next scheduled
   * one (which could be minutes away on a generous network).
   */
  intentAckDeadlineMs?: number;

  /**
   * Where to persist the outbox between process restarts (Android process
   * death is explicit in the brief). Pass null to keep it in-memory only
   * (used by unit tests). In the real client this would be a small SQLite
   * table or DataStore file instead of flat JSON.
   */
  outboxPersistPath?: string | null;

  /**
   * Where to persist the last handled server event `seq`, so a resume after
   * process death replays exactly what was missed. null = in-memory only.
   */
  eventCursorPersistPath?: string | null;
  /**
   * Event acks are batched: one `event-ack` for the highest handled seq is
   * sent at most this often, rather than one radio write per event.
   */
  eventAckDelayMs?: number;

  /** Injectable clock. Timers still use the real setTimeout. */
  now?: () => number;
}

/** Events the manager emits. See AdaptiveConnectionManager class docs. */
export interface ConnectionManagerEvents {
  connected: () => void;
  disconnected: (info: { reason: string }) => void;
  /**
   * A server-pushed event. Sequenced events carry `seq` and are delivered
   * in order, at least once (a replay after a crash can repeat the last one).
   */
  event: (payload: unknown) => void;
  /**
   * We were offline longer than the backend retains events, so some were
   * lost. Replay continues from `oldestSeq`; the app should refresh its
   * state (e.g. a REST fetch of open offers) to cover the gap.
   */
  resyncRequired: (info: { oldestSeq: number }) => void;
  intentQueued: (item: OutboxItem) => void;
  /** The backend has durably received the intent. Not the same as executed - see intentResult. */
  intentAcked: (item: OutboxItem) => void;
  /** The backend's verdict on an intent, delivered after intentAcked. */
  intentResult: (result: IntentResult) => void;
  /**
   * The intent's validUntil passed before it was acked, so it was dropped
   * from the outbox. If `attempts > 0` it may still have reached the
   * backend; its intentResult (if any) is the source of truth.
   */
  intentExpired: (item: OutboxItem) => void;
  reconnecting: (info: { attempt: number; delayMs: number }) => void;
  /**
   * `usedForLearning` is false when the heartbeat was sent on a 'poor'
   * network: the manager reconnects but doesn't treat it as NAT evidence.
   */
  heartbeatTimeout: (info: { intervalMs: number; ackTimeoutMs: number; usedForLearning: boolean }) => void;
  idleTimeoutLearned: (info: { previousMs: number; newMs: number; direction: 'down' | 'up' }) => void;
  protectedModeChanged: (info: { protected: boolean }) => void;
  bleLiveness: () => void;
  outboxCorrupt: (info: { backupPath: string }) => void;
}
