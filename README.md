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
adaptive heartbeat, quality-aware backoff reconnection, a persistent outbox,
and the `protect()`/`notifyBleLiveness()` integration seams - independent of
any single platform SDK, with a deterministic, fully-working demo and test
suite. The design doc (Part 1) is explicit throughout about how each piece
maps onto real Android APIs (foreground service, `CompanionDeviceManager`,
BLE GATT, OEM-specific background restrictions). A from-scratch Kotlin
port, written against the real Android platform types, is in progress as a
follow-up in a separate repo.

## Running it

```bash
npm install

# Runs a scripted scenario against a simulated (but realistic) network:
# idle-timeout learning, a silent NAT-style drop and recovery, an explicit
# forced NAT flush, degraded network quality, a BLE-liveness signal pulling
# a pending reconnect forward, protect()/unprotect(), and an intent queued
# while disconnected that gets flushed + acked automatically on reconnect.
npm run demo

# Runs the test suite (plain Node `assert`, no framework dependency):
npm test
```

Both commands run in well under a minute and need no real network or
backend - `src/simulatedSocket.ts` is a small, controllable fake transport
built specifically to reproduce the brief's failure modes (silent drops,
variable latency/loss, forced NAT flushes) deterministically, so the
adaptive behavior described in the design doc is something you can watch
happen, not just read about.

## Code layout

```
src/
  types.ts                   - shared types/interfaces (SocketLike, metrics, options, events)
  outbox.ts                  - persistent (disk-backed) queue for unacked outbound intents
  simulatedSocket.ts          - deterministic fake transport for the demo/tests
  adaptiveConnectionManager.ts - the deliverable: AdaptiveConnectionManager
  demo.ts                     - scripted scenario, see above
test/
  connectionManager.test.ts   - outbox persistence, EMA learning, backoff bounds,
                                 protect()-pulls-reconnect-forward, queued-intent replay
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
  frozen mid-flow" requirement.
- While building this I hit and fixed a real bug worth calling out rather
  than hiding: an early version double-counted reconnect attempts (and
  therefore skewed backoff) because forcing a dead socket closed after a
  heartbeat timeout would cascade into the normal close handler's
  `scheduleReconnect()` *and* get called explicitly a second time. Fixed by
  detaching the dead socket's callbacks before closing it. See the comment
  above `handleHeartbeatTimeout()` in `adaptiveConnectionManager.ts`.
