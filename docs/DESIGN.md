# Clockit: Real-time Event-to-Transaction Streaming Infrastructure

**Part 1 - System Design**

This document covers the client-side and supporting architecture for reliable
event delivery and intent-to-transaction flow on mobile+smartwatch, under
the constraints called out in the brief: carrier NAT idle eviction (30s-5min),
aggressive Android OEM background killers (Transsion, Xiaomi, etc.), a
BLE-connected smartwatch as the primary engagement surface, and a goal of
low-friction intent (double-tap) -> completed transaction.

The reference implementation of the phone-side connection manager described
in section 2 lives in `src/adaptiveConnectionManager.ts` (Part 2 of this
submission); this document explains *why* it's built the way it is.

---

## 1. High-level architecture

```
                                   ┌─────────────────────────────┐
                                   │        Backend / Cloud       │
                                   │                               │
                                   │  Event Sources                │
                                   │  (pricing engine, market data,│
                                   │   promo engine, order book)   │
                                   │             │                 │
                                   │             ▼                 │
                                   │   Event Fan-out / WS Gateway  │
                                   │   (per-session idle-timeout   │
                                   │    config, backpressure,      │
                                   │    at-least-once delivery)    │
                                   │             │                 │
                                   │             ▼                 │
                                   │   Transaction Execution       │
                                   │   Service (idempotent intent  │
                                   │   processing, ack to client)  │
                                   └──────────────┬────────────────┘
                                                   │ WebSocket (wss://)
                                                   │ over carrier network
                                                   ▼
                        ┌──────────────────────────────────────────────┐
                        │              Phone Companion App               │
                        │                                                │
                        │  ┌──────────────────────────────────────────┐  │
                        │  │   AdaptiveConnectionManager (Part 2)     │  │
                        │  │   - learned NAT idle-timeout (EMA)       │  │
                        │  │   - adaptive heartbeat + jitter          │  │
                        │  │   - quality-aware backoff reconnection   │  │
                        │  │   - persistent outbox (survives process  │  │
                        │  │     death)                                │  │
                        │  │   - protect()/unprotect() <-> Android    │  │
                        │  │     foreground service /                 │  │
                        │  │     CompanionDeviceManager                │  │
                        │  └───────────────┬──────────────┬───────────┘  │
                        │                  │ events        │ intents     │
                        │                  ▼               │             │
                        │        Relay / Sync Layer ────────┘             │
                        │        (de-dupe, order, persist                 │
                        │         to local store; bridges                 │
                        │         phone <-> watch over BLE)                │
                        └───────────────┬──────────────────┬──────────────┘
                                         │ BLE GATT          │ BLE GATT
                                         │ (events, liveness) │ (intents)
                                         ▼                    │
                        ┌──────────────────────────────────┐ │
                        │       Smartwatch Companion         │ │
                        │                                     │ │
                        │  BLE Channel (link layer)           │ │
                        │       │                              │
                        │       ▼                              │
                        │  Watch Engagement Layer               │
                        │  - renders event (e.g. "Flash sale")  │
                        │  - captures double-tap gesture        │
                        │  - local liveness ping back to phone  │
                        │       │                                │
                        │       └── double-tap intent ───────────┘
                        └────────────────────────────────────┘
```

**Why this shape.** The phone is the only component that holds a real IP
connection to the backend; the watch never opens its own socket (see
section 4.3 for why). The phone's `AdaptiveConnectionManager` is
deliberately narrow in scope - it owns connection health and delivery
guarantees for a single logical stream, nothing about BLE or UI - so it can
be unit-tested in isolation (see `test/connectionManager.test.ts`) against a
simulated transport, independent of any Android or BLE stack.

---

## 2. Phone Connection Manager - detailed design

### 2.1 Detecting / learning NAT idle timeouts

Carriers flush idle TCP (and effectively WebSocket) mappings anywhere from
30 seconds to 5 minutes, and this varies by carrier, by region, and
sometimes by time of day as NAT tables get pressured. A fixed interval
either wastes battery (safe-but-slow) or dies constantly (fast-but-wasteful)
depending on which network a given phone is on.

**Approach:** start with a conservative assumption (we default to 45s - near
the aggressive end of the documented range) and *learn* the actual
ceiling empirically per-connection via an exponentially-weighted moving
average:

- A NAT flush is **silent** - no TCP RST reaches the app layer, no
  `onclose`/`onerror`. The only reliable signal is **absence of a heartbeat
  ack within a bounded window**. When that happens, we treat the elapsed
  idle time (minus our own ack-grace-period) as evidence of an *upper
  bound* on the real timeout, and fold it into the EMA with a fast-reacting
  alpha (0.5) - we'd rather overreact to one bad data point than get burned
  twice.
- We never raise the learned timeout off the back of a failure - only
  lower it.
- To avoid permanently converging to the single worst network a device has
  ever seen (and then over-heartbeating forever on a generous network), we
  cautiously **probe upward** after a run of clean heartbeat cycles, using a
  much slower alpha (0.1) so a bad probe costs very little (one extra silent
  drop, immediately re-detected and re-corrected) while a *missing* probe
  costs ongoing battery.

This is an intentionally asymmetric trade-off: fast to tighten, slow to
loosen. See `learnIdleTimeoutDown()` / `maybeProbeTimeoutUpward()` in
`adaptiveConnectionManager.ts`.

### 2.2 Adaptive heartbeat / keep-alive strategy

Heartbeats fire at a random point within **70-85% of the currently learned
idle timeout**, minus additional jitter (up to 2s by default):

- The 70-85% band leaves headroom so the heartbeat *lands* and gets acked
  before the real timeout would hit, even accounting for ack latency.
- Randomizing within the band (rather than a fixed 77.5%) and layering
  jitter on top avoids every phone on the same carrier/cell synchronizing
  their heartbeats into a thundering herd against the gateway.
- The band shifts based on live conditions: on a **poor**-quality link we
  bias toward the tighter (more frequent) end, since heartbeats themselves
  are more likely to be delayed or lost for reasons unrelated to NAT
  eviction; in a **protected** window (foreground service active - see 2.4)
  we bias toward the looser end, since we're not at risk of being frozen
  and can afford to save battery.
- An absolute 2-second floor prevents a pathological busy-loop regardless
  of what the learned value says.

### 2.3 Reconnection logic (including resume/replay)

On any detected failure (clean close, error, or heartbeat timeout):

1. Enter `reconnecting` state, increment the attempt counter.
2. Compute delay via **full-jitter exponential backoff**: `delay =
   random(0, min(maxBackoff, base * 2^attempt * qualityMultiplier))`. Full
   jitter (not "exponential +/- a little") is deliberate: if a shared piece
   of infra blips and many phones fail at once, full jitter spreads their
   retries out instead of having them all hit the gateway in lockstep at
   the same multiples of the base delay.
3. `qualityMultiplier` widens the backoff further under `poor` network
   conditions (1.6x) and moderately under `fair` (1.2x) - retrying
   aggressively into a link that's already struggling just burns battery
   and radio time for more failures.
4. On reconnect, **resume** is implicit and stateless-by-design on the
   client: there's no sequence-number negotiation handshake required before
   resuming, because...
5. **Replay** is handled by the outbox (2.5), not by the socket layer: on
   every successful `connected` transition, every outbox item still
   unacked is re-sent, oldest first. The backend's intent processing is
   idempotent (keyed by the intent's client-generated id), so a duplicate
   send from an ack that was itself lost in-flight is safe to re-process.
6. Two external signals can **pull a pending backoff forward** rather than
   making the user wait it out: entering a protected window
   (`protect()`) and a BLE liveness signal from the watch
   (`notifyBleLiveness()`) - both are evidence the user is actively present
   and a fast reconnect is worth the extra radio wake-up.

### 2.4 Integration with foreground service / companion device APIs

The manager exposes `protect()` / `unprotect()` as its entire surface
toward the Android platform layer - it doesn't know or care *how* protection
is implemented, just whether it's currently active. In the real app, these
map to:

- **`protect()`** called when: the watch reports the user is actively
  engaged (a BLE liveness burst, or the watch UI for an event is open), or
  a time-critical event has just been delivered and we're expecting an
  imminent intent (e.g. right after pushing "flash sale" to the watch).
  Underneath, this starts a short-lived Android foreground service (with a
  visible, accurate notification - critical, since OEMs like Transsion
  still freeze apps that fake a foreground notification without real
  foreground-service semantics - see Part 3) and/or leans on
  `CompanionDeviceManager`'s `startObservingDevicePresence` /
  association-backed background exemptions where available.
- **`unprotect()`** called on a timeout after the engagement window closes
  (e.g. 30-60s after the last BLE liveness signal with no intent), to stop
  paying the foreground-service cost (visible notification, slightly higher
  battery/CPU priority) once it's no longer earning its keep.
- The connection manager *reacts* to protection state (wider heartbeat
  margin - 2.2) but does not manage the service lifecycle itself; that
  separation keeps it testable without any Android framework dependency, as
  demonstrated by the Node-only test suite.

### 2.5 Interaction with the BLE watch link

- **Phone -> watch:** server-pushed events and (implicitly) connection
  health are relayed to the watch over BLE GATT by the Relay/Sync layer
  (outside this manager's scope, see architecture diagram). The watch
  itself never needs to know about NAT timeouts or backoff.
- **Watch -> phone:** two things flow back: (a) **intents** (the
  double-tap), which the relay layer hands to
  `AdaptiveConnectionManager.sendIntent()`, and (b) **liveness**, a cheap,
  frequent "still on wrist / still in range" signal that the relay layer
  forwards to `notifyBleLiveness()`. Liveness is treated as *corroborating*
  evidence, not a substitute for a real phone-side heartbeat: it nudges an
  about-to-fire heartbeat slightly later (avoid waking the radio twice back
  to back) and, if we're mid-backoff, pulls reconnection forward
  immediately, since the presence of an engaged user raises the value of a
  fast reconnect well above its battery cost.

---

## 3. End-to-end flow: event -> watch -> double-tap -> transaction

**Happy path**

```
Backend emits "Flash sale - 12 units left"
  -> Event Fan-out pushes over the phone's open WebSocket
  -> AdaptiveConnectionManager.handleMessage() emits 'event'
  -> Relay layer forwards over BLE to the watch
  -> Watch renders it, starts a short attention window
  -> User double-taps
  -> Watch sends the intent back over BLE
  -> Relay layer calls sendIntent(payload)
  -> Outbox durably persists it, then an immediate send is attempted
  -> If connected: backend acks -> intentAcked -> watch shows confirmation
```

**Phone process frozen mid-flow** (the scenario the brief calls out
explicitly): the sequence is identical up through "Outbox durably persists
it" - `Outbox.enqueue()` performs a synchronous `fs.writeFileSync` before
`sendIntent()` returns (see `outbox.ts`), specifically so that the intent is
safe on disk *before* control returns to a caller that might be killed a
moment later. If the OEM killer freezes/kills the process right after that:

- Nothing is lost - the intent sits on disk, not just in memory.
- When the process is next resumed (OS wakes it for a push, or the user
  reopens the app), `AdaptiveConnectionManager`'s constructor reloads the
  outbox from disk and `connect()` is called as part of normal startup.
- On `connected`, `flushAllOutbox()` re-sends every unacked item, oldest
  first. The backend's idempotent intent processing means this is safe even
  if the original send had actually gone through and only the local ack
  was lost.

**Network dropped mid-flow** (NAT flush between the double-tap and the ack):
the send either silently vanishes (full NAT drop) or times out waiting for
an ack neither of which raises an immediate error. The outbox item is
*not* removed until an explicit `intent-ack` arrives, so:

- The next heartbeat cycle detects the dead connection (2.1/2.3) within one
  bounded heartbeat-interval + ack-timeout window, not an indefinite hang.
- Reconnection + outbox replay (above) recovers the intent automatically,
  with no special-casing needed for "was this a process freeze or a network
  drop" - both converge on the same recovery path.

---

## 4. Top 5 failure modes (African mobile conditions) and mitigations

| # | Failure mode | Why it happens here specifically | Mitigation in this design |
|---|---|---|---|
| 1 | **Aggressive, variable NAT idle eviction** (30s on some carriers, much longer on others, and inconsistent even within one carrier under load) | Pre-paid/budget carrier infrastructure runs NAT tables under heavy subscriber-per-IP pressure; conservative idle timeouts save them carrier-side state. | EMA-learned, per-connection idle timeout with a fast-tighten/slow-loosen heartbeat (2.1/2.2), rather than one fixed interval tuned for a single carrier. |
| 2 | **OEM background killers freezing the process** (Transsion, Xiaomi, and similar skins kill backgrounded apps aggressively to protect battery-life benchmarks, sometimes *despite* a running foreground service) | These OEMs dominate the region's device mix and compete heavily on advertised battery life, which incentivizes very aggressive, sometimes non-standard, background-kill heuristics. | `protect()`/`unprotect()` integration with real foreground-service + CompanionDeviceManager semantics (2.4); a disk-persisted outbox so a kill never loses an intent (3); diagnostics to detect OEM-specific freeze patterns (see Part 3, item 2). |
| 3 | **High-latency / lossy 2G-3G links** (intents or heartbeats delayed seconds, or dropped outright, especially outside major metro coverage) | Coverage outside Lagos/Nairobi/Accra-tier metros is frequently 2G/3G-only, and even metro 4G degrades hard indoors or at cell-edge during peak hours. | `reportNetworkQuality()` widens the heartbeat ack-timeout tolerance and biases heartbeats more frequent under `poor`, while *also* backing off reconnection more aggressively (quality-aware backoff, 2.3) so we don't hammer an already-struggling link. |
| 4 | **Intermittent connectivity / frequent handoffs** (wifi<->cellular switches in homes/offices with unreliable wifi, or cell-tower handoffs on the move) | Shared/metered home and office wifi is common and frequently drops; commuting on packed transit causes frequent tower handoffs. | Every handoff looks like "connection died" to the manager, which is exactly the case the backoff+replay path already handles - no special-case needed, which keeps the failure surface small. The EMA's upward probe (2.1) also means a handoff onto a *better* network (e.g. wifi) is detected and exploited (less frequent heartbeats) rather than staying pinned to the worst network seen that session. |
| 5 | **Battery-saver / data-saver OS modes triggered more aggressively** (many users run with Android's Battery Saver or Data Saver on by default, due to device cost/charging-access constraints and expensive mobile data) | Device and data costs are a larger share of income here, so users lean on these OS power/data-saving modes far more than in markets where they're an edge case. | The connection manager treats these the same as "OEM freeze risk" from the platform's point of view - `protect()` is the one integration point that has to account for Battery Saver's additional restrictions on top of OEM-specific ones, keeping the core manager logic unaware of *which* restriction is in play (see Part 4.1 for the related product trade-off). |

---

*See `README.md` for how to run the demo harness that exercises most of
this document's claims (idle-timeout learning, silent-drop detection,
quality-aware backoff, BLE-liveness-pulls-reconnect, protect/unprotect) end
to end, and `PART_3_AND_4.md` for the debugging and trade-off answers.*
