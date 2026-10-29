/**
 * Demo / test harness for AdaptiveConnectionManager.
 *
 * Run with: npm run demo
 *
 * This wires the manager up to a SimulatedSocket (not a real server) so the
 * whole scenario runs in under a minute instead of
 * requiring a real multi-minute NAT flush. It walks through, in order:
 *
 *   1. Normal connect + a few healthy heartbeats (watch RTT get logged).
 *   2. The *true* simulated NAT idle timeout (5s) is shorter than our
 *      starting assumption (10s) - watch the manager detect the silent
 *      drop via heartbeat timeout (not close/error) and learn the timeout
 *      downward over a couple of corrections, exactly like it would on an
 *      aggressive real carrier.
 *   3. A user intent (double-tap) sent while briefly disconnected - queued
 *      to the outbox, then flushed and acked automatically on reconnect.
 *   4. A server-pushed time-critical event ("flash sale") delivered while
 *      connected.
 *   5. Network quality degrading to 'poor' - the ack timeout widens (2.5x),
 *      backoff grows more aggressively on the next drop, and that drop is
 *      not used to lower the learned timeout.
 *   6. A forced NAT flush (explicit, not idle-based) while connected, with
 *      a server event published into the dead path - it is lost on the
 *      wire but replayed by seq when the client resumes.
 *   7. A double-tap whose offer window (TTL) closes before the connection
 *      is back - it expires locally instead of being replayed late.
 *   8. A BLE liveness signal arriving mid-backoff - reconnect is pulled
 *      forward instead of waiting out the scheduled delay.
 *   9. protect()/unprotect() toggling (foreground-service simulation).
 *
 * Metrics are printed on a timer throughout so you can see
 * learnedIdleTimeoutMs converge and consecutiveFailures/backoff move.
 */
import { AdaptiveConnectionManager } from './adaptiveConnectionManager';
import { SimulatedBackend } from './simulatedBackend';
import { SimulatedSocket } from './simulatedSocket';

const START = Date.now();
function t(): string {
  return `[t+${((Date.now() - START) / 1000).toFixed(1)}s]`;
}
function log(msg: string): void {
  console.log(`${t()} ${msg}`);
}

let activeSocket: SimulatedSocket | null = null;
// Outlives individual sockets: holds the sequenced event log for resume.
const backend = new SimulatedBackend();

const manager = new AdaptiveConnectionManager({
  url: 'wss://events.clockit.example/stream',
  initialIdleTimeoutMs: 10_000, // our starting guess
  minIdleTimeoutMs: 3_000,
  maxIdleTimeoutMs: 60_000,
  heartbeatJitterMs: 500, // small, so the first heartbeats (6.5-8.5s) reliably overshoot the 5s true timeout
  heartbeatAckTimeoutMs: 2_000,
  baseBackoffMs: 500,
  maxBackoffMs: 8_000,
  outboxPersistPath: null, // in-memory for the demo; see test/ for persistence proof
  socketFactory: (url) => {
    const sock = new SimulatedSocket(url, {
      trueIdleTimeoutMs: 5_000, // the *real* network only tolerates 5s idle - shorter than our guess
      log,
      backend,
    });
    activeSocket = sock;
    return sock;
  },
});

manager.on('connected', () => log('✅ connected'));
manager.on('disconnected', ({ reason }) => log(`❌ disconnected (${reason})`));
manager.on('reconnecting', ({ attempt, delayMs }) => log(`🔄 reconnecting - attempt ${attempt} in ${delayMs}ms`));
manager.on('heartbeatTimeout', ({ intervalMs, ackTimeoutMs, usedForLearning }) =>
  log(
    `💔 heartbeat timeout after ${intervalMs}ms idle (waited ${ackTimeoutMs}ms for ack) - ` +
      (usedForLearning ? 'treating as silent NAT drop' : 'poor network, reconnecting without lowering learned timeout')
  )
);
manager.on('idleTimeoutLearned', ({ previousMs, newMs, direction }) =>
  log(`🧠 learned idle timeout ${direction === 'down' ? '↓' : '↑'}: ${Math.round(previousMs)}ms -> ${Math.round(newMs)}ms`)
);
manager.on('intentQueued', (item) => log(`📥 intent queued: ${item.id}`));
manager.on('intentAcked', (item) => log(`📨 intent received by backend: ${item.id}`));
manager.on('intentResult', ({ id, status, reason }) =>
  log(`${status === 'executed' ? '✅' : '⛔'} intent ${status}${reason ? ` (${reason})` : ''}: ${id}`)
);
manager.on('intentExpired', (item) => log(`⌛ intent expired before delivery: ${item.id}`));
manager.on('event', (payload) => log(`📣 server event received: ${JSON.stringify(payload)}`));
manager.on('protectedModeChanged', ({ protected: p }) => log(p ? '🛡️  protected mode ON' : '🛡️  protected mode OFF'));
manager.on('bleLiveness', () => log('⌚ BLE liveness signal from watch'));

function printMetrics(): void {
  const m = manager.getMetrics();
  log(
    `📊 state=${m.state} learnedIdleTimeout=${m.learnedIdleTimeoutMs}ms heartbeatInterval=${m.currentHeartbeatIntervalMs}ms ` +
      `rtt=${m.lastHeartbeatRttMs ?? '-'}ms failures=${m.consecutiveFailures} reconnectAttempts=${m.reconnectAttempts} ` +
      `outbox=${m.outboxSize} quality=${m.networkQuality} protected=${m.protectedMode} lastEventSeq=${m.lastEventSeq ?? '-'}`
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
  log('Starting idle-timeout guess: 10000ms. True simulated network idle timeout: 5000ms.');
  log('Expect the manager to silently fail, then learn the timeout downward.\n');

  const metricsTimer = setInterval(printMetrics, 4000);

  manager.connect();
  await sleep(3000);

  // --- Server pushes a time-critical event while we're connected ---
  log('--- Backend emits a time-critical event ---');
  backend.publish({ type: 'flash-sale', message: 'Flash sale - 12 units left', sku: 'SKU-42' });
  await sleep(1000);

  // --- User double-taps -> intent sent while connected ---
  log('--- User double-taps while connected (should ack quickly) ---');
  manager.sendIntent({ action: 'buy', sku: 'SKU-42', qty: 1 }, { ttlMs: 30_000 }); // flash-sale window
  await sleep(3000);

  // Let the idle timeout mismatch play out: our heartbeats are scheduled at
  // 70-85% of the *learned* (currently 10s) timeout minus jitter, i.e.
  // ~6.5-8.5s, past the true 5s NAT timeout. Each drop pulls the learned
  // value down until heartbeats land inside the 5s window.
  log('--- Waiting through idle-timeout learning cycles (silent drops expected) ---');
  await sleep(26_000);

  // --- Degrade network quality, then force an explicit NAT flush ---
  log('--- Network quality degrades to poor ---');
  manager.reportNetworkQuality('poor');
  await sleep(2000);

  // A learning-phase drop may have just landed; flushing a socket that's
  // already dead would demonstrate nothing, so wait for a live one.
  await waitForState('connected', 10_000);
  log('--- Forcing an explicit NAT flush while connected (poor network: expect a longer ack wait, no learning) ---');
  activeSocket?.setCondition('nat-flushed');
  log('--- Backend emits an event while the path is silently dead (should arrive after reconnect, via resume) ---');
  backend.publish({ type: 'price-alert', message: 'SKU-77 dropped to 4,500', sku: 'SKU-77' });
  // Wait for the manager to actually detect the drop (via heartbeat
  // timeout, since a NAT flush is silent) and enter backoff, rather than a
  // fixed sleep - the exact detection time depends on where in the current
  // heartbeat cycle the flush landed.
  const reachedBackoff = await waitForState('reconnecting', 15_000);
  log(reachedBackoff ? '(now in reconnecting/backoff state)' : '(did not observe backoff in time)');

  // --- A double-tap on an offer with almost no time left, while offline ---
  log('--- User double-taps an offer with 100ms left while disconnected (should expire, never sent) ---');
  manager.sendIntent({ action: 'buy', sku: 'SKU-LAST-SECOND', qty: 1 }, { ttlMs: 100 });
  await sleep(300);

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
