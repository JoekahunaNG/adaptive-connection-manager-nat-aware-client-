/**
 * Shared types for the Adaptive Connection Manager.
 *
 * Design note: the manager depends on a `SocketLike` abstraction rather than
 * the `ws` library or the browser `WebSocket` directly. This lets the same
 * manager run against:
 *   - a real WebSocket (web/Node client),
 *   - the SimulatedSocket used by the demo/test harness to deterministically
 *     reproduce NAT flushes, latency, and packet loss without a real network.
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
  attempts: number;
  lastAttemptAt: number | null;
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
  /** Current heartbeat interval actually in use (post safety-margin + jitter). */
  currentHeartbeatIntervalMs: number;
  networkQuality: NetworkQuality;
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
  /** How long to wait for a heartbeat ack before declaring the path dead. */
  heartbeatAckTimeoutMs?: number;

  baseBackoffMs?: number;
  maxBackoffMs?: number;

  /**
   * Where to persist the outbox between process restarts (Android process
   * death is explicit in the brief). Pass null to keep it in-memory only
   * (used by unit tests). In the real client this would be a small SQLite
   * table or DataStore file instead of flat JSON.
   */
  outboxPersistPath?: string | null;

  /** Injectable clock, overridden in tests for determinism. */
  now?: () => number;

  /** Injectable timer functions, overridden in tests to run in fake time. */
  setTimeout?: (fn: () => void, ms: number) => unknown;
  clearTimeout?: (handle: unknown) => void;
}

/** Events the manager emits. See AdaptiveConnectionManager class docs. */
export interface ConnectionManagerEvents {
  connected: () => void;
  disconnected: (info: { reason: string }) => void;
  event: (payload: unknown) => void;
  intentQueued: (item: OutboxItem) => void;
  intentAcked: (item: OutboxItem) => void;
  reconnecting: (info: { attempt: number; delayMs: number }) => void;
  heartbeatTimeout: (info: { intervalMs: number }) => void;
  idleTimeoutLearned: (info: { previousMs: number; newMs: number; direction: 'down' | 'up' }) => void;
  protectedModeChanged: (info: { protected: boolean }) => void;
  bleLiveness: () => void;
}
