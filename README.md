# Clockit Senior Software Engineer Assessment

Real-time Event-to-Transaction Streaming Infrastructure — submission by
[Joseph Haruna](mailto:harunajoseph55@gmail.com).

This repo contains all four parts of the assessment:

| Part | Where |
|---|---|
| 1. System design | [`docs/DESIGN.md`](docs/DESIGN.md) |
| 2. Coding exercise - `AdaptiveConnectionManager` | [`src/`](src), demo in [`src/demo.ts`](src/demo.ts) |
| 3. Debugging & edge cases | [`docs/PART_3_AND_4.md`](docs/PART_3_AND_4.md) |
| 4. Trade-offs & senior judgement | [`docs/PART_3_AND_4.md`](docs/PART_3_AND_4.md) |

**Language:** TypeScript (one of the brief's accepted options alongside the
preferred Kotlin). This covers the full logic - EMA idle-timeout learning,
adaptive heartbeat, quality-aware ack timeout and backoff, a crash-safe
persistent outbox with intent TTLs, sequenced server events with resume
after reconnect, and the `protect()`/`notifyBleLiveness()` integration
seams - independent of any single platform SDK, with a scripted demo and
test suite. The design doc (Part 1) is explicit throughout about how each
piece maps onto real Android APIs (foreground service,
`CompanionDeviceManager`, BLE GATT, OEM-specific background restrictions).
A Kotlin port against the real Android platform types is planned as a
follow-up.

## Running it

```bash
npm install

# Runs a scripted scenario against a simulated (but realistic) network:
# idle-timeout learning through repeated silent NAT-style drops, intents
# acked then executed, degraded network quality (longer ack wait, no
# learning from lossy timeouts), a forced NAT flush with a server event
# published into the dead path and replayed after resume, an intent whose
# TTL runs out while offline, a BLE-liveness signal pulling a pending
# reconnect forward, and protect()/unprotect().
npm run demo

# Runs the test suite (plain Node `assert`, no framework dependency):
npm test
```

Each command runs in about a minute and needs no real network or
backend - `src/simulatedSocket.ts` is a small, controllable fake transport
built specifically to reproduce the brief's failure modes (silent drops,
variable latency/loss, forced NAT flushes) on demand, and
`src/simulatedBackend.ts` holds the server-side state that must outlive a
socket (the sequenced event log, idempotent intent processing), so the adaptive
behavior described in the design doc is something you can watch happen, not
just read about. Latency, loss and jitter are randomized, so exact timings
differ run to run; the tests assert on behavior, not exact numbers.

## Code layout

```
src/
  index.ts                     - package entry point
  types.ts                     - shared types/interfaces (SocketLike, metrics, options, events)
  adaptiveConnectionManager.ts - the deliverable: AdaptiveConnectionManager
  outbox.ts                    - persistent (disk-backed) queue for unacked outbound intents
  eventCursor.ts               - persisted last-handled server event seq, for resume
  fsUtil.ts                    - atomic write (temp file, fsync, rename) shared by both
  simulatedSocket.ts           - controllable fake transport for the demo/tests
  simulatedBackend.ts          - fake backend: sequenced event log, resume, idempotent intents
  demo.ts                      - scripted scenario, see above
test/
  connectionManager.test.ts    - 21 tests: outbox persistence and crash safety, EMA
                                 learning, backoff bounds and reset rules, BLE liveness
                                 (no heartbeat starvation, no reconnect storm),
                                 poor-network ack timeout, intent ack/result/TTL/deadline,
                                 event resume, dedupe, cursor persistence, resync
docs/
  DESIGN.md                   - Part 1: architecture, connection manager design,
                                 end-to-end flow, African-network failure modes
  PART_3_AND_4.md              - Parts 3 & 4: debugging and trade-off answers
```

## A couple of implementation notes worth flagging

- `AdaptiveConnectionManager` depends on a small `SocketLike` interface, not
  directly on `ws` or the browser `WebSocket`. The demo and tests inject
  `SimulatedSocket`; a real deployment would inject a thin wrapper around a
  platform WebSocket client (e.g. OkHttp's on Android) - the manager's logic
  is identical either way, which is also what makes it unit-testable without
  a real network.
- The outbox persists synchronously on enqueue (a few hundred bytes,
  infrequent) specifically so an intent is durable on disk *before*
  `sendIntent()` returns control to the caller - see the comment in
  `src/outbox.ts` for why that matters given the brief's "process was
  frozen mid-flow" requirement. Writes are atomic, so a kill mid-write
  can't truncate the queue.
- Delivery is at-least-once in both directions, with dedupe at the
  receiver: intents by UUID on the backend, server events by `seq` on the
  client. `intentAcked` means "the backend has it"; the outcome arrives
  separately as `intentResult`, on the sequenced stream so it survives a
  disconnect. See DESIGN.md 2.3.
- While building this I hit and fixed a real bug worth calling out rather
  than hiding: an early version double-counted reconnect attempts (and
  therefore skewed backoff) because forcing a dead socket closed after a
  heartbeat timeout would cascade into the normal close handler's
  `scheduleReconnect()` *and* get called explicitly a second time. Fixed by
  detaching the dead socket's callbacks before closing it. See the comment
  above `handleHeartbeatTimeout()` in `adaptiveConnectionManager.ts`.
- A second one found in review: an earlier `notifyBleLiveness()` pushed the
  next heartbeat back on every signal, so a watch reporting liveness every
  few seconds starved heartbeats entirely and guaranteed a NAT drop -
  exactly when the user was engaged. Liveness now never delays a heartbeat
  (it can only trigger an early one), and a regression test covers it.
- Known limitation: one learned idle timeout per manager instance, not per
  network (see DESIGN.md 2.1).
