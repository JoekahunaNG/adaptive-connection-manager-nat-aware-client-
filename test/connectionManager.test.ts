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
import { Outbox } from '../src/outbox';
import { SimulatedSocket } from '../src/simulatedSocket';

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
  manager.on('reconnecting', ({ delayMs }) => delays.push(delayMs));

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
