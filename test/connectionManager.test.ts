/**
 * Lightweight test harness - no framework dependency, just Node's built-in
 * `assert`, so `npm test` has zero extra install surface. Each test uses
 * short, intentionally-aggressive timeouts (hundreds of ms, not minutes) so
 * the whole suite runs in a few seconds while still exercising real,
 * un-mocked timer-driven behavior end to end.
 *
 * Run with: npm test
 */
import * as assert from 'assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { AdaptiveConnectionManager } from '../src/adaptiveConnectionManager';
import { EventCursor } from '../src/eventCursor';
import { Outbox } from '../src/outbox';
import { SimulatedBackend } from '../src/simulatedBackend';
import { SimulatedSocket } from '../src/simulatedSocket';
import { IntentResult, OutboxItem, SocketLike } from '../src/types';

type TestFn = () => Promise<void>;
const tests: { name: string; fn: TestFn }[] = [];
function test(name: string, fn: TestFn): void {
  tests.push({ name, fn });
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate: () => boolean, maxMs: number, intervalMs = 25): Promise<void> {
  const deadline = Date.now() + maxMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await sleep(intervalMs);
  }
  throw new Error(`waitFor: condition not met within ${maxMs}ms`);
}

// ---------------------------------------------------------------------------

test('Outbox persists across a simulated process restart', async () => {
  const tmp = path.join(os.tmpdir(), `clockit-outbox-test-${Date.now()}.json`);
  try {
    const first = new Outbox(tmp);
    first.enqueue('intent-1', { action: 'buy', sku: 'ABC' }, Date.now());
    assert.strictEqual(first.size(), 1);
    assert.ok(fs.existsSync(tmp), 'outbox should have written to disk');

    // Simulate the process dying and a fresh Outbox instance loading from
    // the same path - this is the exact scenario the brief calls out:
    // "Handle cases where phone process was frozen ... mid-flow".
    const second = new Outbox(tmp);
    assert.strictEqual(second.size(), 1, 'outbox should survive process restart');
    assert.strictEqual(second.all()[0].id, 'intent-1');

    const acked = second.ack('intent-1');
    assert.ok(acked);
    assert.strictEqual(second.size(), 0);

    const third = new Outbox(tmp);
    assert.strictEqual(third.size(), 0, 'acked items should not reappear after restart');
  } finally {
    if (fs.existsSync(tmp)) fs.unlinkSync(tmp);
  }
});

test('Learns the idle timeout downward after a silent (NAT-style) drop', async () => {
  const manager = new AdaptiveConnectionManager({
    url: 'wss://test.invalid/stream',
    initialIdleTimeoutMs: 5000,
    minIdleTimeoutMs: 500,
    maxIdleTimeoutMs: 20000,
    heartbeatAckTimeoutMs: 400,
    baseBackoffMs: 50,
    maxBackoffMs: 300,
    outboxPersistPath: null,
    socketFactory: (url) => new SimulatedSocket(url, { trueIdleTimeoutMs: 1200 }),
  });

  let learnedDown: { previousMs: number; newMs: number } | null = null;
  manager.on('idleTimeoutLearned', (info) => {
    if (info.direction === 'down' && !learnedDown) learnedDown = info;
  });

  manager.connect();
  try {
    await waitFor(() => learnedDown !== null, 8000);
    assert.ok(learnedDown, 'expected a downward EMA correction after the silent drop');
    const info = learnedDown as unknown as { previousMs: number; newMs: number };
    assert.ok(info.newMs < info.previousMs, 'learned timeout should have decreased');
    assert.ok(info.newMs >= 500, 'should respect minIdleTimeoutMs floor');

    const metrics = manager.getMetrics();
    assert.ok(
      metrics.learnedIdleTimeoutMs <= info.previousMs,
      'metrics should reflect the corrected (lower) timeout'
    );
  } finally {
    manager.disconnect();
  }
});

test('A queued intent sent while disconnected is flushed and acked once connected', async () => {
  const manager = new AdaptiveConnectionManager({
    url: 'wss://test.invalid/stream',
    heartbeatAckTimeoutMs: 1000,
    outboxPersistPath: null,
    socketFactory: (url) => new SimulatedSocket(url, { trueIdleTimeoutMs: 60_000 }),
  });

  let queuedId: string | null = null;
  let ackedId: string | null = null;
  manager.on('intentQueued', (item) => (queuedId = item.id));
  manager.on('intentAcked', (item) => (ackedId = item.id));

  try {
    // Not connected yet - this should queue, not throw or silently drop.
    const id = manager.sendIntent({ action: 'buy', sku: 'XYZ' });
    assert.strictEqual(queuedId, id);
    assert.strictEqual(manager.getMetrics().outboxSize, 1);
    assert.strictEqual(manager.getMetrics().state, 'disconnected');

    manager.connect();
    await waitFor(() => ackedId === id, 3000);
    assert.strictEqual(manager.getMetrics().outboxSize, 0, 'acked intent should leave the outbox');
  } finally {
    manager.disconnect();
  }
});

test('protect() pulls a pending reconnect forward instead of waiting out backoff', async () => {
  const manager = new AdaptiveConnectionManager({
    url: 'wss://test.invalid/stream',
    initialIdleTimeoutMs: 2000,
    heartbeatAckTimeoutMs: 300,
    baseBackoffMs: 5000, // deliberately large - if protect() didn't pull forward, this test would time out
    maxBackoffMs: 20000,
    outboxPersistPath: null,
    socketFactory: (url) => new SimulatedSocket(url, { trueIdleTimeoutMs: 600 }),
  });

  try {
    manager.connect();
    await waitFor(() => manager.getMetrics().state === 'reconnecting', 8000);

    const pulledForwardAt = Date.now();
    manager.protect();
    await waitFor(() => manager.getMetrics().state === 'connected', 2000);
    const elapsed = Date.now() - pulledForwardAt;
    assert.ok(
      elapsed < 2000,
      `expected protect() to reconnect well under the ${5000}ms backoff, took ${elapsed}ms`
    );
  } finally {
    manager.disconnect();
  }
});

test('Reconnect backoff does not exceed maxBackoffMs even after repeated failures', async () => {
  const manager = new AdaptiveConnectionManager({
    url: 'wss://test.invalid/stream',
    initialIdleTimeoutMs: 300,
    heartbeatAckTimeoutMs: 100,
    baseBackoffMs: 50,
    maxBackoffMs: 400,
    outboxPersistPath: null,
    // trueIdleTimeoutMs shorter than the ack timeout window effectively
    // guarantees every connection dies almost immediately, forcing many
    // reconnect cycles so we can observe backoff saturate.
    socketFactory: (url) => new SimulatedSocket(url, { trueIdleTimeoutMs: 150 }),
  });

  const delays: number[] = [];
  const attempts: number[] = [];
  manager.on('reconnecting', ({ attempt, delayMs }) => {
    attempts.push(attempt);
    delays.push(delayMs);
  });

  manager.connect();
  try {
    // Note: plannedHeartbeatIntervalMs() has a deliberate 2s absolute floor
    // (see its comment in adaptiveConnectionManager.ts) that doesn't scale
    // down with this test's artificially tiny timeouts, so each
    // detect-fail -> backoff -> reconnect cycle here takes ~2.1-2.5s even
    // though baseBackoffMs/maxBackoffMs are tiny. 5 cycles within 18s is a
    // realistic budget for *this* floor, not a claim about production
    // timing (where learnedIdleTimeoutMs is tens of seconds and the floor
    // never binds).
    await waitFor(() => delays.length >= 5, 18_000);
  } finally {
    manager.disconnect();
  }

  for (const d of delays) {
    assert.ok(d <= 400, `backoff delay ${d}ms exceeded maxBackoffMs`);
  }
  // Every connection here opens and then dies before any heartbeat is acked.
  // Socket-open alone must not reset backoff, or the attempt counter (and so
  // the backoff ceiling) would restart at 1 on every cycle.
  attempts.forEach((a, i) => assert.strictEqual(a, i + 1, `attempt counter reset: ${attempts.join(',')}`));
});

test('Outbox survives a crash mid-write (stale partial temp file)', async () => {
  const tmp = path.join(os.tmpdir(), `clockit-outbox-crash-${Date.now()}.json`);
  try {
    const first = new Outbox(tmp);
    first.enqueue('intent-1', { action: 'buy' }, Date.now());

    // Simulate the process being killed half-way through the next persist:
    // the temp file holds truncated JSON and was never renamed into place.
    fs.writeFileSync(`${tmp}.tmp`, '[{"id":"intent-1","payl');

    const second = new Outbox(tmp);
    assert.strictEqual(second.corruptBackupPath, null);
    assert.strictEqual(second.size(), 1, 'the last complete write must still load');
  } finally {
    for (const f of [tmp, `${tmp}.tmp`]) if (fs.existsSync(f)) fs.unlinkSync(f);
  }
});

test('A corrupt outbox file is moved aside and reported, not silently discarded', async () => {
  const tmp = path.join(os.tmpdir(), `clockit-outbox-corrupt-${Date.now()}.json`);
  let backupPath: string | null = null;
  try {
    fs.writeFileSync(tmp, 'not json at all');
    const manager = new AdaptiveConnectionManager({
      url: 'wss://test.invalid/stream',
      outboxPersistPath: tmp,
      socketFactory: (url) => new SimulatedSocket(url),
    });
    manager.on('outboxCorrupt', (info) => (backupPath = info.backupPath));
    await sleep(10);

    assert.ok(backupPath, 'expected an outboxCorrupt event');
    assert.ok(fs.existsSync(backupPath as unknown as string), 'corrupt file should be preserved for diagnosis');
    assert.strictEqual(fs.readFileSync(backupPath as unknown as string, 'utf-8'), 'not json at all');
    assert.strictEqual(manager.getMetrics().outboxSize, 0);
  } finally {
    for (const f of [tmp, `${tmp}.tmp`, backupPath]) if (f && fs.existsSync(f)) fs.unlinkSync(f);
  }
});

test('Frequent BLE liveness never starves phone-side heartbeats', async () => {
  let heartbeatsSent = 0;
  const manager = new AdaptiveConnectionManager({
    url: 'wss://test.invalid/stream',
    initialIdleTimeoutMs: 4000, // scheduled heartbeat at ~2.8-3.4s
    heartbeatJitterMs: 0,
    outboxPersistPath: null,
    socketFactory: (url) => {
      const sock = new SimulatedSocket(url, { trueIdleTimeoutMs: 60_000 });
      const send = sock.send.bind(sock);
      sock.send = (data: string) => {
        if (JSON.parse(data).type === 'heartbeat') heartbeatsSent += 1;
        send(data);
      };
      return sock;
    },
  });

  // A watch reporting liveness every 200ms. The previous implementation
  // pushed the heartbeat back on every signal, so none were ever sent.
  const pinger = setInterval(() => manager.notifyBleLiveness(), 200);
  manager.connect();
  try {
    await sleep(7000);
    assert.ok(heartbeatsSent >= 2, `expected heartbeats to keep flowing, got ${heartbeatsSent}`);
    assert.strictEqual(manager.getMetrics().state, 'connected');
  } finally {
    clearInterval(pinger);
    manager.disconnect();
  }
});

test('BLE liveness during an outage cannot bypass backoff (no reconnect storm)', async () => {
  let socketsOpened = 0;
  const unreachable = (): SocketLike => {
    socketsOpened += 1;
    const sock: SocketLike = {
      send: () => {},
      close: () => {},
      onopen: null,
      onclose: null,
      onerror: null,
      onmessage: null,
    };
    setTimeout(() => {
      sock.onerror?.(new Error('network unreachable'));
      sock.onclose?.({ code: 1006, reason: 'network unreachable' });
    }, 20);
    return sock;
  };

  const manager = new AdaptiveConnectionManager({
    url: 'wss://test.invalid/stream',
    // Full jitter draws from [0, delay], so a ~10s base still fires inside
    // the 3s window sometimes. At 1e9ms the odds are negligible, which
    // leaves only pull-forwards able to open sockets.
    baseBackoffMs: 1e9,
    maxBackoffMs: 1e9,
    minPullForwardIntervalMs: 5000,
    outboxPersistPath: null,
    socketFactory: unreachable,
  });

  manager.connect();
  await waitFor(() => manager.getMetrics().state === 'reconnecting', 1000);
  const pinger = setInterval(() => manager.notifyBleLiveness(), 100);
  try {
    await sleep(3000);
    // ~30 liveness signals. Without the cooldown each one opens a socket.
    // Expect the initial connect plus exactly one pull-forward.
    assert.strictEqual(socketsOpened, 2, `expected 2 connection attempts, got ${socketsOpened}`);
  } finally {
    clearInterval(pinger);
    manager.disconnect();
  }
});

/** Opens immediately and acks heartbeats after a fixed delay, or never if `ackAfterMs` is null. */
function fixedLatencySocket(ackAfterMs: number | null, stats = { acks: 0 }): SocketLike {
  const sock: SocketLike = {
    send: (data: string) => {
      const msg = JSON.parse(data);
      if (msg.type !== 'heartbeat' || ackAfterMs === null) return;
      setTimeout(() => {
        stats.acks += 1;
        sock.onmessage?.({ data: JSON.stringify({ type: 'heartbeat-ack', id: msg.id }) });
      }, ackAfterMs);
    },
    close: () => {},
    onopen: null,
    onclose: null,
    onerror: null,
    onmessage: null,
  };
  setTimeout(() => sock.onopen?.(), 10);
  return sock;
}

test('On a poor network a slow-but-healthy link is not torn down', async () => {
  let timeouts = 0;
  const stats = { acks: 0 };
  const manager = new AdaptiveConnectionManager({
    url: 'wss://test.invalid/stream',
    initialIdleTimeoutMs: 4000, // 'poor' pins the margin at 70%: heartbeat every 2.8s + 0.7s ack
    minIdleTimeoutMs: 500,
    heartbeatJitterMs: 0,
    heartbeatAckTimeoutMs: 400, // 700ms acks would fail this budget on 'good'; 'poor' allows 1000ms
    outboxPersistPath: null,
    socketFactory: () => fixedLatencySocket(700, stats),
  });
  manager.on('heartbeatTimeout', () => (timeouts += 1));
  manager.reportNetworkQuality('poor');

  manager.connect();
  try {
    await sleep(8000); // acks land at ~3.5s and ~7.0s
    assert.strictEqual(timeouts, 0, 'a 700ms ack is within the widened poor-network budget');
    assert.ok(stats.acks >= 2, `expected heartbeats to keep being acked, got ${stats.acks}`);
    assert.strictEqual(manager.getMetrics().state, 'connected');
    assert.strictEqual(manager.getMetrics().learnedIdleTimeoutMs, 4000);
  } finally {
    manager.disconnect();
  }
});

test('A heartbeat timeout on a poor network reconnects but does not lower the learned timeout', async () => {
  let timeoutInfo: { ackTimeoutMs: number; usedForLearning: boolean } | null = null;
  let learnedDown = false;
  const manager = new AdaptiveConnectionManager({
    url: 'wss://test.invalid/stream',
    initialIdleTimeoutMs: 4000,
    minIdleTimeoutMs: 500, // low enough that a timeout *would* move the EMA if it were fed
    heartbeatJitterMs: 0,
    heartbeatAckTimeoutMs: 400,
    baseBackoffMs: 10_000,
    maxBackoffMs: 20_000,
    outboxPersistPath: null,
    socketFactory: () => fixedLatencySocket(null),
  });
  manager.on('heartbeatTimeout', (info) => (timeoutInfo = timeoutInfo ?? info));
  manager.on('idleTimeoutLearned', (info) => {
    if (info.direction === 'down') learnedDown = true;
  });
  manager.reportNetworkQuality('poor');

  manager.connect();
  try {
    await waitFor(() => timeoutInfo !== null, 6000);
    const info = timeoutInfo as unknown as { ackTimeoutMs: number; usedForLearning: boolean };
    assert.strictEqual(info.ackTimeoutMs, 1000, 'poor network should scale the 400ms ack timeout by 2.5x');
    assert.strictEqual(info.usedForLearning, false);
    assert.strictEqual(learnedDown, false, 'loss on a poor link is not NAT evidence');
    assert.strictEqual(manager.getMetrics().learnedIdleTimeoutMs, 4000);
    assert.strictEqual(manager.getMetrics().state, 'reconnecting');
  } finally {
    manager.disconnect();
  }
});

test('An intent is acked (received) and then gets a separate executed result', async () => {
  const order: string[] = [];
  let result: IntentResult | null = null;
  const manager = new AdaptiveConnectionManager({
    url: 'wss://test.invalid/stream',
    outboxPersistPath: null,
    socketFactory: (url) => new SimulatedSocket(url, { trueIdleTimeoutMs: 60_000 }),
  });
  manager.on('intentAcked', () => order.push('acked'));
  manager.on('intentResult', (r) => {
    order.push('result');
    result = r;
  });

  manager.connect();
  try {
    await waitFor(() => manager.getMetrics().state === 'connected', 1000);
    const id = manager.sendIntent({ action: 'buy', sku: 'SKU-1' });
    assert.match(id, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/, 'intent id should be a UUID');
    await waitFor(() => result !== null, 2000);
    assert.deepStrictEqual(order, ['acked', 'result']);
    assert.deepStrictEqual(result, { id, status: 'executed' });
  } finally {
    manager.disconnect();
  }
});

test('An intent whose TTL runs out while offline expires and is never sent', async () => {
  let sent = 0;
  let expired: OutboxItem | null = null;
  const manager = new AdaptiveConnectionManager({
    url: 'wss://test.invalid/stream',
    outboxPersistPath: null,
    socketFactory: (url) => {
      const sock = new SimulatedSocket(url, { trueIdleTimeoutMs: 60_000 });
      const send = sock.send.bind(sock);
      sock.send = (data: string) => {
        if (JSON.parse(data).type === 'intent') sent += 1;
        send(data);
      };
      return sock;
    },
  });
  manager.on('intentExpired', (item) => (expired = item));

  try {
    const id = manager.sendIntent({ action: 'buy', sku: 'FLASH' }, { ttlMs: 150 });
    await waitFor(() => expired !== null, 1000);
    assert.strictEqual((expired as unknown as OutboxItem).id, id);
    assert.strictEqual(manager.getMetrics().outboxSize, 0);

    manager.connect();
    await waitFor(() => manager.getMetrics().state === 'connected', 1000);
    await sleep(200);
    assert.strictEqual(sent, 0, 'an expired intent must not be replayed on reconnect');
  } finally {
    manager.disconnect();
  }
});

test('An intent that expired while the process was dead is reported on restart', async () => {
  const tmp = path.join(os.tmpdir(), `clockit-outbox-ttl-${Date.now()}.json`);
  try {
    new Outbox(tmp).enqueue('intent-stale', { action: 'buy' }, Date.now() - 10_000, Date.now() - 5000);

    const manager = new AdaptiveConnectionManager({
      url: 'wss://test.invalid/stream',
      outboxPersistPath: tmp,
      socketFactory: (url) => new SimulatedSocket(url),
    });
    let expiredId: string | null = null;
    manager.on('intentExpired', (item) => (expiredId = item.id));
    await waitFor(() => expiredId !== null, 500);
    assert.strictEqual(expiredId, 'intent-stale');
    assert.strictEqual(new Outbox(tmp).size(), 0, 'expiry should be persisted');
  } finally {
    for (const f of [tmp, `${tmp}.tmp`]) if (fs.existsSync(f)) fs.unlinkSync(f);
  }
});

test('The backend is the source of truth for an intent that expires in flight', async () => {
  const events: string[] = [];
  let result: IntentResult | null = null;
  const manager = new AdaptiveConnectionManager({
    url: 'wss://test.invalid/stream',
    outboxPersistPath: null,
    // 'good' latency is 40-80ms each way, so a 20ms TTL is valid when sent
    // and stale by the time it reaches the backend.
    socketFactory: (url) => new SimulatedSocket(url, { trueIdleTimeoutMs: 60_000 }),
  });
  manager.on('intentExpired', () => events.push('expired'));
  manager.on('intentAcked', () => events.push('acked'));
  manager.on('intentResult', (r) => (result = r));

  manager.connect();
  try {
    await waitFor(() => manager.getMetrics().state === 'connected', 1000);
    manager.sendIntent({ action: 'buy' }, { ttlMs: 20 });
    await waitFor(() => result !== null, 2000);
    assert.deepStrictEqual(events, ['expired'], 'locally expired first, so the late ack is not re-reported');
    assert.strictEqual((result as unknown as IntentResult).status, 'rejected');
    assert.strictEqual((result as unknown as IntentResult).reason, 'expired');
  } finally {
    manager.disconnect();
  }
});

test('A late intent ack triggers an immediate heartbeat instead of waiting for the schedule', async () => {
  const sockets: SimulatedSocket[] = [];
  let timeoutAt: number | null = null;
  let ackedAt: number | null = null;
  const manager = new AdaptiveConnectionManager({
    url: 'wss://test.invalid/stream',
    initialIdleTimeoutMs: 30_000, // next scheduled heartbeat ~21-25s away
    heartbeatAckTimeoutMs: 300,
    intentAckDeadlineMs: 300,
    baseBackoffMs: 50,
    maxBackoffMs: 100,
    outboxPersistPath: null,
    socketFactory: (url) => {
      const sock = new SimulatedSocket(url, { trueIdleTimeoutMs: 60_000 });
      sockets.push(sock);
      return sock;
    },
  });
  manager.on('heartbeatTimeout', () => (timeoutAt = timeoutAt ?? Date.now()));
  manager.on('intentAcked', () => (ackedAt = Date.now()));

  manager.connect();
  try {
    await waitFor(() => manager.getMetrics().state === 'connected', 1000);
    sockets[0].forceSilentDrop();
    const sentAt = Date.now();
    manager.sendIntent({ action: 'buy' });

    await waitFor(() => ackedAt !== null, 3000);
    assert.ok(timeoutAt !== null, 'the dead path should have been detected by an early probe');
    // deadline (300) + heartbeat ack timeout (300) + slack
    assert.ok((timeoutAt as unknown as number) - sentAt < 1200, `probe took ${(timeoutAt as unknown as number) - sentAt}ms`);
    assert.ok(sockets.length >= 2, 'should have reconnected and replayed on a fresh socket');
  } finally {
    manager.disconnect();
  }
});

/** Options for a manager that detects a silent drop in ~2.3s (the 2s heartbeat floor + ack timeout). */
function fastDetectOptions(backend: SimulatedBackend, sockets: SimulatedSocket[]) {
  return {
    url: 'wss://test.invalid/stream',
    initialIdleTimeoutMs: 2000,
    minIdleTimeoutMs: 500,
    heartbeatJitterMs: 0,
    heartbeatAckTimeoutMs: 300,
    baseBackoffMs: 50,
    maxBackoffMs: 100,
    eventAckDelayMs: 100,
    outboxPersistPath: null,
    socketFactory: (url: string) => {
      const sock = new SimulatedSocket(url, { trueIdleTimeoutMs: 60_000, backend });
      sockets.push(sock);
      return sock;
    },
  };
}

// The resume message rides the simulated latency (40-80ms on 'good'), so
// the backend only knows where to send live events slightly after open.
const RESUME_SETTLE_MS = 200;

test('Server events published during a silent drop are replayed after reconnect, in order, once', async () => {
  const backend = new SimulatedBackend();
  const sockets: SimulatedSocket[] = [];
  const seqs: number[] = [];
  const manager = new AdaptiveConnectionManager(fastDetectOptions(backend, sockets));
  manager.on('event', (e) => seqs.push((e as { seq: number }).seq));

  manager.connect();
  try {
    await waitFor(() => manager.getMetrics().state === 'connected', 1000);
    await sleep(RESUME_SETTLE_MS);
    backend.publish({ type: 'flash-sale', sku: 'A' });
    await waitFor(() => seqs.length === 1, 1000);

    sockets[0].forceSilentDrop();
    backend.publish({ type: 'flash-sale', sku: 'B' }); // lost on the wire
    backend.publish({ type: 'flash-sale', sku: 'C' }); // lost on the wire

    await waitFor(() => seqs.length >= 3, 6000);
    await sleep(300);
    assert.deepStrictEqual(seqs, [1, 2, 3]);
    assert.ok(sockets.length >= 2, 'delivery of 2 and 3 should have come via a resumed connection');
    assert.strictEqual(manager.getMetrics().lastEventSeq, 3);
    await waitFor(() => backend.retainedEventCount() === 0, 1000); // batched event-ack trimmed the log
  } finally {
    manager.disconnect();
  }
});

test('Duplicate sequenced events are dropped and a gap triggers one resume request', async () => {
  const sent: { type: string; lastEventSeq?: number | null }[] = [];
  let sock: SocketLike | null = null;
  const seqs: number[] = [];
  const manager = new AdaptiveConnectionManager({
    url: 'wss://test.invalid/stream',
    outboxPersistPath: null,
    socketFactory: () => {
      const s: SocketLike = {
        send: (d: string) => sent.push(JSON.parse(d)),
        close: () => {},
        onopen: null,
        onclose: null,
        onerror: null,
        onmessage: null,
      };
      setTimeout(() => s.onopen?.(), 10);
      sock = s;
      return s;
    },
  });
  manager.on('event', (e) => seqs.push((e as { seq: number }).seq));
  const deliver = (seq: number) =>
    (sock as unknown as SocketLike).onmessage?.({ data: JSON.stringify({ type: 'price', seq }) });

  manager.connect();
  try {
    await waitFor(() => manager.getMetrics().state === 'connected', 1000);
    deliver(5); // fresh client: first seq accepted as-is
    deliver(5); // duplicate
    deliver(7); // gap: 6 missing
    deliver(7); // still the same gap - must not resend resume
    deliver(6);
    deliver(7);
    assert.deepStrictEqual(seqs, [5, 6, 7]);
    const resumes = sent.filter((m) => m.type === 'resume').map((m) => m.lastEventSeq);
    assert.deepStrictEqual(resumes, [null, 5], 'initial resume, then exactly one for the gap');
  } finally {
    manager.disconnect();
  }
});

test('The event cursor survives a restart, so only missed events are replayed', async () => {
  const cursorPath = path.join(os.tmpdir(), `clockit-cursor-${Date.now()}.json`);
  const backend = new SimulatedBackend();
  try {
    const firstSeqs: number[] = [];
    const first = new AdaptiveConnectionManager({
      ...fastDetectOptions(backend, []),
      eventCursorPersistPath: cursorPath,
    });
    first.on('event', (e) => firstSeqs.push((e as { seq: number }).seq));
    first.connect();
    await waitFor(() => first.getMetrics().state === 'connected', 1000);
    await sleep(RESUME_SETTLE_MS);
    backend.publish({ type: 'news', n: 1 });
    backend.publish({ type: 'news', n: 2 });
    await waitFor(() => firstSeqs.length === 2, 1000);
    first.disconnect(); // stands in for process death

    backend.publish({ type: 'news', n: 3 }); // happens while the app is dead

    const secondSeqs: number[] = [];
    const second = new AdaptiveConnectionManager({
      ...fastDetectOptions(backend, []),
      eventCursorPersistPath: cursorPath,
    });
    second.on('event', (e) => secondSeqs.push((e as { seq: number }).seq));
    assert.strictEqual(second.getMetrics().lastEventSeq, 2, 'cursor should load from disk');
    second.connect();
    try {
      await waitFor(() => secondSeqs.length >= 1, 1000);
      await sleep(200);
      assert.deepStrictEqual(secondSeqs, [3]);
    } finally {
      second.disconnect();
    }
  } finally {
    for (const f of [cursorPath, `${cursorPath}.tmp`]) if (fs.existsSync(f)) fs.unlinkSync(f);
  }
});

test('An intent result lost in a disconnect is replayed on resume, and the intent runs once', async () => {
  const backend = new SimulatedBackend();
  const sockets: SimulatedSocket[] = [];
  const results: IntentResult[] = [];
  const manager = new AdaptiveConnectionManager(fastDetectOptions(backend, sockets));
  manager.once('intentAcked', () => sockets[sockets.length - 1].forceSilentDrop()); // result is ~50ms behind the ack
  manager.on('intentResult', (r) => results.push(r));

  manager.connect();
  try {
    await waitFor(() => manager.getMetrics().state === 'connected', 1000);
    await sleep(RESUME_SETTLE_MS); // so the fresh client gets a stream-head cursor first
    const id = manager.sendIntent({ action: 'buy' });
    await waitFor(() => results.length >= 1, 6000);
    await sleep(300);
    assert.deepStrictEqual(results, [{ id, status: 'executed' }]);
    assert.ok(sockets.length >= 2, 'the result should have arrived on a resumed connection');
  } finally {
    manager.disconnect();
  }
});

test('A client further behind than the backend retains gets resyncRequired, then the rest', async () => {
  const cursorPath = path.join(os.tmpdir(), `clockit-cursor-resync-${Date.now()}.json`);
  const backend = new SimulatedBackend({ maxLogSize: 2 });
  try {
    new EventCursor(cursorPath).set(1); // last saw seq 1
    for (let i = 0; i < 5; i++) backend.publish({ type: 'news', n: i + 1 }); // retains only 4, 5

    let resync: { oldestSeq: number } | null = null;
    const seqs: number[] = [];
    const manager = new AdaptiveConnectionManager({
      ...fastDetectOptions(backend, []),
      eventCursorPersistPath: cursorPath,
    });
    manager.on('resyncRequired', (info) => (resync = info));
    manager.on('event', (e) => seqs.push((e as { seq: number }).seq));
    manager.connect();
    try {
      await waitFor(() => seqs.length >= 2, 1000);
      assert.deepStrictEqual(resync, { oldestSeq: 4 });
      assert.deepStrictEqual(seqs, [4, 5]);
    } finally {
      manager.disconnect();
    }
  } finally {
    for (const f of [cursorPath, `${cursorPath}.tmp`]) if (fs.existsSync(f)) fs.unlinkSync(f);
  }
});

// ---------------------------------------------------------------------------

async function main() {
  let passed = 0;
  let failed = 0;
  for (const { name, fn } of tests) {
    try {
      await fn();
      console.log(`✓ ${name}`);
      passed += 1;
    } catch (err) {
      console.error(`✗ ${name}`);
      console.error(err instanceof Error ? err.stack : err);
      failed += 1;
    }
  }
  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed > 0 ? 1 : 0);
}

main();
