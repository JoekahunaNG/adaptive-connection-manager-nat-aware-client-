/**
 * Demo / test harness for AdaptiveConnectionManager.
 *
 * Run with: npm run demo
 *
 * This wires the manager up to a SimulatedSocket (not a real server) so the
 * whole scenario runs deterministically in under a minute instead of
 * requiring a real multi-minute NAT flush. It walks through, in order:
 *
 *   1. Normal connect + a few healthy heartbeats (watch RTT get logged).
 *   2. The *true* simulated NAT idle timeout (7s) is shorter than our
 *      starting assumption (10s) - watch the manager detect the silent
 *      drop via heartbeat timeout (not close/error) and learn the timeout
 *      downward over a couple of corrections, exactly like it would on an
 *      aggressive real carrier.
 *   3. A user intent (double-tap) sent while briefly disconnected - queued
 *      to the outbox, then flushed and acked automatically on reconnect.
 *   4. A server-pushed time-critical event ("flash sale") delivered while
 *      connected.
 *   5. Network quality degrading to 'poor' - backoff grows more aggressively
 *      on the next drop.
 *   6. A forced NAT flush (explicit, not idle-based) while connected.
 *   7. A BLE liveness signal arriving mid-backoff - reconnect is pulled
 *      forward instead of waiting out the scheduled delay.
 *   8. protect()/unprotect() toggling (foreground-service simulation).
 *
 * Metrics are printed on a timer throughout so you can see
 * learnedIdleTimeoutMs converge and consecutiveFailures/backoff move.
 */
import { AdaptiveConnectionManager } from './adaptiveConnectionManager';
import { SimulatedSocket } from './simulatedSocket';

const START = Date.now();
function t(): string {
  return `[t+${((Date.now() - START) / 1000).toFixed(1)}s]`;
}
function log(msg: string): void {
  console.log(`${t()} ${msg}`);
}

let activeSocket: SimulatedSocket | null = null;

const manager = new AdaptiveConnectionManager({
  url: 'wss://events.clockit.example/stream',
  initialIdleTimeoutMs: 10_000, // our starting guess
  minIdleTimeoutMs: 3_000,
  maxIdleTimeoutMs: 60_000,
  heartbeatAckTimeoutMs: 2_000,
  baseBackoffMs: 500,
  maxBackoffMs: 8_000,
  outboxPersistPath: null, // in-memory for the demo; see test/ for persistence proof
  socketFactory: (url) => {
    const sock = new SimulatedSocket(url, {
      trueIdleTimeoutMs: 7_000, // the *real* network only tolerates 7s idle - shorter than our guess
      log,
    });
    activeSocket = sock;
    return sock;
  },
});

manager.on('connected', () => log('✅ connected'));
manager.on('disconnected', ({ reason }) => log(`❌ disconnected (${reason})`));
manager.on('reconnecting', ({ attempt, delayMs }) => log(`🔄 reconnecting - attempt ${attempt} in ${delayMs}ms`));
manager.on('heartbeatTimeout', ({ intervalMs }) =>
  log(`💔 heartbeat timeout after ${intervalMs}ms idle - treating as silent NAT drop`)
);
manager.on('idleTimeoutLearned', ({ previousMs, newMs, direction }) =>
  log(`🧠 learned idle timeout ${direction === 'down' ? '↓' : '↑'}: ${previousMs}ms -> ${Math.round(newMs)}ms`)
);
manager.on('intentQueued', (item) => log(`📥 intent queued: ${item.id}`));
manager.on('intentAcked', (item) => log(`📨 intent acked: ${item.id}`));
manager.on('event', (payload) => log(`📣 server event received: ${JSON.stringify(payload)}`));
manager.on('protectedModeChanged', ({ protected: p }) => log(p ? '🛡️  protected mode ON' : '🛡️  protected mode OFF'));
manager.on('bleLiveness', () => log('⌚ BLE liveness signal from watch'));

function printMetrics(): void {
  const m = manager.getMetrics();
  log(
    `📊 state=${m.state} learnedIdleTimeout=${m.learnedIdleTimeoutMs}ms heartbeatInterval=${m.currentHeartbeatIntervalMs}ms ` +
      `rtt=${m.lastHeartbeatRttMs ?? '-'}ms failures=${m.consecutiveFailures} reconnectAttempts=${m.reconnectAttempts} ` +
      `outbox=${m.outboxSize} quality=${m.networkQuality} protected=${m.protectedMode}`
  );
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Poll metrics until the manager reaches `state`, or give up after `maxMs`. */
async function waitForState(state: string, maxMs: number): Promise<boolean> {
  const deadline = Date.now() + maxMs;
  while (Date.now() < deadline) {
    if (manager.getMetrics().state === state) return true;
    await sleep(150);
  }
  return false;
}

async function main() {
  log('=== Clockit AdaptiveConnectionManager demo ===');
  log('Starting idle-timeout guess: 10000ms. True simulated network idle timeout: 7000ms.');
  log('Expect the manager to silently fail, then learn the timeout downward.\n');

  const metricsTimer = setInterval(printMetrics, 4000);

  manager.connect();
  await sleep(3000);

  // --- Server pushes a time-critical event while we're connected ---
  log('--- Backend emits a time-critical event ---');
  activeSocket?.pushServerEvent({ type: 'flash-sale', message: 'Flash sale - 12 units left', sku: 'SKU-42' });
  await sleep(1000);

  // --- User double-taps -> intent sent while connected ---
  log('--- User double-taps while connected (should ack quickly) ---');
  manager.sendIntent({ action: 'buy', sku: 'SKU-42', qty: 1 });
  await sleep(3000);

  // Let the idle timeout mismatch play out: our heartbeats are scheduled at
  // 70-85% of the *learned* (currently 10s) timeout, i.e. ~7-8.5s, right
  // around where the true 7s NAT timeout already kills the mapping. Give it
  // a couple of cycles to detect and correct.
  log('--- Waiting through idle-timeout learning cycles (silent drops expected) ---');
  await sleep(26_000);

  // --- Degrade network quality, then force an explicit NAT flush ---
  log('--- Network quality degrades to poor ---');
  manager.reportNetworkQuality('poor');
  await sleep(2000);

  log('--- Forcing an explicit NAT flush while connected ---');
  activeSocket?.setCondition('nat-flushed');
  // Wait for the manager to actually detect the drop (via heartbeat
  // timeout, since a NAT flush is silent) and enter backoff, rather than a
  // fixed sleep - the exact detection time depends on where in the current
  // heartbeat cycle the flush landed.
  const reachedBackoff = await waitForState('reconnecting', 15_000);
  log(reachedBackoff ? '(now in reconnecting/backoff state)' : '(did not observe backoff in time)');

  // --- While backing off, a BLE liveness signal arrives - should pull reconnect forward ---
  log('--- BLE liveness signal arrives mid-backoff ---');
  manager.notifyBleLiveness();
  await sleep(2000);

  // --- User double-taps again, this time while we may still be mid-reconnect ---
  log('--- User double-taps again (may be queued if not yet connected) ---');
  manager.sendIntent({ action: 'buy', sku: 'SKU-77', qty: 2 });
  await sleep(4000);

  // --- Protected-mode window (foreground service active) ---
  log('--- Entering protected mode (foreground service active) ---');
  manager.reportNetworkQuality('good');
  manager.protect();
  await sleep(4000);
  manager.unprotect();

  await sleep(3000);
  printMetrics();

  log('\n=== Demo complete ===');
  clearInterval(metricsTimer);
  manager.disconnect();
  await sleep(200);
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
