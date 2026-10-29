# Part 3 - Debugging & Edge Cases

Production logs from Lagos and Nairobi show:

- Heartbeats succeed for 2-3 minutes, then connection silently dies
- After process death, events are delayed 4-15 minutes
- Double-tap intents sometimes take >8 seconds to reach backend
- Some Transsion devices show the foreground notification but still get
  frozen after ~1 minute

## 1) Most likely root causes

**Heartbeats succeed 2-3 min, then silently die.**
Three candidate causes, not mutually exclusive:
- The real NAT/carrier idle timeout for this path is roughly in the
  2-3 minute band, and if the client was still using a pre-adaptive or
  too-loosely-scheduled heartbeat, the margin wasn't tight enough to land a
  heartbeat before eviction.
- Independent of NAT: Android Doze / App Standby can suspend background
  network access on a per-app basis after a period of inactivity even
  though the process itself is alive - if the heartbeat timer is a plain
  `Handler`/`AlarmManager` callback rather than an exact or
  Doze-exempt alarm, the OS can silently *defer* the heartbeat itself,
  which looks identical from the client's point of view to "the heartbeat
  fired but got no ack."
- Self-inflicted, by an adaptive client like the one in Part 2: after a run
  of clean heartbeats the EMA probes the learned timeout *upward*. If the
  true timeout sits just above where heartbeats currently land, a few
  minutes of successes push the interval past it, the next heartbeat
  arrives after eviction, and the client tightens again - a repeating
  "works for a few minutes, then drops" sawtooth. The diagnostics below
  distinguish it: drops cluster right after an upward
  `idleTimeoutLearned` event.

**Events delayed 4-15 minutes after process death.**
This range is a strong signature of Doze/App-Standby **maintenance-window
batching** - if event delivery relies on normal-priority FCM or a
periodic `WorkManager` job rather than high-priority FCM, Android batches
delivery into windows that widen as the app's App Standby bucket gets less
favorable (more infrequent use -> longer windows), landing squarely in the
4-15 minute range reported. This is a wake-trigger problem, not a transport
problem - the socket isn't even being attempted because the process hasn't
been resumed yet.

**Double-tap intents sometimes take >8 seconds.**
Most likely: the connection had already silently died (NAT/Doze, as above)
but the client hadn't *detected* it yet - the first send after a drop
vanishes into the blackhole, and the client only learns something is wrong
when the *next regularly scheduled heartbeat* times out, which could be
several seconds away depending on where in the cycle the intent happened to
be sent. Secondary contributors: BLE GATT congestion/retry on the
watch-to-phone hop, and radio promotion latency (RRC idle/inactive ->
connected state transitions can themselves take seconds on weaker 2G/3G
links).

**Transsion devices frozen after ~1 minute despite a visible notification.**
Transsion's HiOS/XOS skins (Tecno, Infinix, itel) are well known to run
their own manufacturer-specific background-task freezer *on top of* stock
Android's battery optimization system, with its own allow-list
(commonly an "auto-start"/"background activity" toggle buried in a
vendor-specific "Phone Manager" app) that a standard foreground-service
notification does **not** automatically satisfy. The ~1 minute mark is
consistent with a vendor-specific wakelock/CPU-usage heuristic rather than
anything in stock Android's foreground-service contract.

## 2) Diagnostic data to add to the client

A structured, periodically-uploaded event log (not just error logs) with:

- Connection-state transitions with reason, every heartbeat sent/acked
  timestamp + RTT, `learnedIdleTimeoutMs` over time, and every detected
  silent-drop with the observed idle duration.
- OS power-state snapshot at each heartbeat/failure:
  `PowerManager.isDeviceIdleMode()` (Doze), `UsageStatsManager`'s current
  App Standby bucket, Battery Saver on/off, and whether a foreground
  service was active at that moment.
- Device identity: manufacturer, model, and OS/skin version, so failures
  can be sliced by OEM cohort.
- Per-intent latency breakdown with four timestamps: watch-tap time (from
  the watch), BLE-received-on-phone time, outbox-enqueue time, and
  ack-received time - this turns "sometimes >8s" from a guess into a
  hop-by-hop measurement.
- Wake-trigger provenance for event delivery: which mechanism actually
  resumed the process (high-priority FCM, normal FCM, WorkManager,
  AlarmManager, user-launch) and the gap between requested and actual fire
  time.
- Whether the user has granted the OEM-specific auto-start/background
  permission (queryable on most Transsion/Xiaomi-class skins), correlated
  against freeze timestamps.

## 3) Concrete changes to the connection manager and watch relay

- **Decouple per-intent ack timeout from the heartbeat cycle.** Every
  `sendIntent()` should carry its own short deadline (e.g. 5-8s)
  independent of the next scheduled heartbeat; missing that deadline should
  itself trigger an immediate heartbeat probe/reconnect rather than waiting
  for the regularly scheduled one. This directly targets the >8s cases,
  which are currently bottlenecked on heartbeat cadence rather than actual
  network recovery time. *(Implemented in Part 2: `intentAckDeadlineMs`,
  default 5s. Pair it with a shorter heartbeat ack timeout while an intent
  is outstanding, since the probe's own wait adds to the total.)*
- **Stop the probe sawtooth.** Remember the learned value at which an
  upward probe last failed, and treat it as a ceiling that later probes
  approach more slowly (or not at all for some hours on that network). The
  battery saved by probing past a known-bad ceiling is far smaller than
  the cost of a silent drop every few minutes.
- **Use high-priority FCM data messages, not periodic jobs, for
  time-critical event wake-ups**, and take a short bounded wakelock on
  receipt so the reconnect-and-flush sequence can't be re-suspended by Doze
  mid-flight. This targets the 4-15 minute delivery delays directly.
- **Let BLE liveness proactively trigger a phone-side health check**, not
  just pull a pending reconnect forward: if the watch is reporting active
  liveness but the phone hasn't heartbeated recently, that's itself reason
  to probe immediately rather than wait. *(Implemented in Part 2: a quiet
  connection is probed on liveness, rate-limited, and liveness never
  postpones a scheduled heartbeat.)*
- **Detect known-aggressive OEM builds by manufacturer/model** and, on
  first run (and periodically thereafter), explicitly guide the user
  through the vendor-specific auto-start/background permission flow, since
  diagnostics show the standard foreground-service notification is
  insufficient on these skins.
- **Add a local BLE ack/retry layer on the watch relay** with its own short
  timeout and small retry budget before escalating a failure to the phone
  relay, so a congested GATT hop is visible in diagnostics instead of
  silently eating part of the >8s budget.

---

# Part 4 - Trade-offs & Senior Judgement

## 1) Persistent foreground service vs. high-priority FCM + watch push

Default to **FCM (high-priority) + watch push** for the overwhelming
majority of a user's day - it's near-free on battery, survives the app
process not running at all, and is "fast enough" for most time-critical
events. Promote to a **short-lived foreground service** only during an
actual engagement window: right after a time-critical event is delivered
and we're expecting an imminent intent, or while BLE liveness shows the
user actively engaged (e.g. mid-interaction with a "stop-loss triggered"
alert). Drop back down automatically after a short idle timeout. The
service should never run continuously - beyond the battery cost, a
permanently-visible notification trains users to disable or force-kill the
app via battery settings, which defeats the purpose far more thoroughly
than the latency a short, well-timed service window would have cost.

## 2) Balancing battery impact against delivery reliability

Treat it as a *per-moment* trade-off, not a global one - reliability spend
should track the value of being fast *right now*, not be maximized
uniformly all the time. The design in this submission encodes that in
several places: the EMA learns just enough heartbeat frequency to survive
the current network rather than the worst case ever seen; `protect()` only
activates during real engagement windows; backoff widens under poor network
quality instead of hammering a struggling link; and the EMA is deliberately
asymmetric (fast to tighten after a failure, slow to loosen) because a few
extra heartbeats are cheap but a missed one is expensive. One trade-off
that *isn't* worth making: outbox durability. Persisting a few hundred
bytes synchronously is essentially free, so it should always be on - the
real trade-off is entirely in how eagerly we chase a live connection, never
in whether we remember what we owe the backend. Where possible, push the
tuning knobs (heartbeat margins, backoff caps) server-side/remote-config
rather than compiling one global answer into a shipped binary, since the
right balance will keep shifting as more field data comes in.

## 3) Should the smartwatch ever maintain its own upstream connection?

Generally no, for this product as specified. Most companion watches at this
price/market tier are BLE-only with no independent cellular radio, so
"its own connection" usually isn't even available; even where it is, watch
batteries are far more constrained than phones, and running two independent
connection managers doubles the NAT/backoff/outbox complexity while
introducing a new problem this design currently avoids entirely - which
device "owns" sending a given intent if either one can. The phone-as-relay
assumption holds for nearly all real engagement moments, since BLE range is
short and the phone is almost always present when the watch is actively
being used. The one scenario worth planning for later, explicitly as v2
scope rather than a default: a cellular-capable watch SKU with a *dormant*
fallback connection that only activates once BLE-to-phone liveness has been
lost for some threshold, for high-value time-critical events where
"left the phone at home" resilience matters enough to justify reintroducing
all the same NAT/battery problems on a second, more constrained device.

## 4) Evolving to 500k concurrent users, mixed 2G/3G/4G, heavy OEM restrictions

- **Backend:** shard the WebSocket gateway fleet with session-affine
  routing; make heartbeat/idle-timeout defaults push-configurable per
  session (carrier/region-tunable) rather than compiled into the client, so
  learned-safe defaults can roll out without an app release; put a durable
  log upstream of the gateway (so a gateway deploy/restart never drops an
  in-flight event) - this fits naturally with the client's existing
  at-least-once-plus-idempotent-processing assumption, it doesn't require
  changing it.
- **Observability:** the Part 3 diagnostic fields need to roll up into
  dashboards sliced by carrier/OEM/region/App-Standby-bucket, so engineering
  effort goes toward whichever cohort is actually degrading rather than
  chasing individual device reports.
- **Client:** at this scale, a single global EMA default will always be
  wrong for a meaningful tail of devices - feed aggregated (non-identifying)
  learned-timeout and OEM-freeze-pattern statistics back into better
  per-device-model defaults shipped via remote config, so the fleet
  collectively gets smarter rather than each device learning from scratch.
  On-device, key the learned timeout by network (the Part 2 reference keeps
  one value per instance, so every wifi <-> cellular handoff re-learns from
  the wrong starting point).
- **Capacity planning:** with heavy 2G/3G mix, connection *churn*
  (handoffs, drops, reconnects) will dominate backend load more than
  steady-state message volume - size and load-test for churn-per-second,
  and keep the reconnect/auth handshake as close to a single round trip as
  possible, since it will run constantly at this scale.
- The underlying philosophy doesn't change: scaling to 500k heterogeneous
  devices is a better-defaults-and-more-observability problem, not a
  more-complex-protocol problem. The adaptive heartbeat, quality-aware
  backoff, and durable-outbox primitives already generalize; what has to
  scale is the feedback loop that keeps tuning them.
