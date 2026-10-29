/**
 * A fake backend that outlives individual sockets, for the demo and tests.
 *
 * SimulatedSocket alone models one connection; anything the server sends
 * while that connection is silently dead is simply gone. Real resume needs
 * server state that survives reconnects, which is what this holds:
 *
 *  - an ordered event log, each entry stamped with a monotonic `seq`;
 *  - replay of everything after the client's `lastEventSeq` on `resume`,
 *    or `resync-required` if the client is older than the retained log;
 *    a fresh client (null cursor) gets `stream-head` so it has a cursor;
 *  - log trimming on the client's batched `event-ack`;
 *  - idempotent intent processing keyed by intent id, with each result
 *    published on the sequenced stream so a result lost in a disconnect is
 *    replayed like any other event.
 *
 * Single-client by design: a real backend keeps one cursor/log per user
 * stream. Not modelled: persistence of the log itself, auth, multi-device.
 */
export interface BackendClient {
  deliverFromServer(message: object): void;
}

interface LogEntry {
  seq: number;
  message: Record<string, unknown>;
}

export class SimulatedBackend {
  private log: LogEntry[] = [];
  private nextSeq = 1;
  private readonly maxLogSize: number;
  private readonly processedIntents = new Set<string>();
  private client: BackendClient | null = null;

  constructor(opts: { maxLogSize?: number } = {}) {
    this.maxLogSize = opts.maxLogSize ?? 1000;
  }

  /** Emit a server event (e.g. "flash sale"). Logged even if no client is reachable right now. */
  publish(payload: Record<string, unknown>): number {
    const seq = this.nextSeq++;
    const message = { ...payload, seq };
    this.log.push({ seq, message });
    // Retention cap: an unacked client that falls further behind than this
    // gets resync-required instead of an unbounded replay.
    if (this.log.length > this.maxLogSize) this.log.shift();
    this.client?.deliverFromServer(message);
    return seq;
  }

  handleResume(client: BackendClient, lastEventSeq: number | null): void {
    this.client = client;
    if (lastEventSeq === null) {
      // Fresh client: live events only, but tell it where "now" is so its
      // next resume can recover anything lost after this point.
      client.deliverFromServer({ type: 'stream-head', seq: this.nextSeq - 1 });
      return;
    }
    this.trimThrough(lastEventSeq);
    const oldestRetained = this.log[0]?.seq ?? this.nextSeq;
    if (lastEventSeq < oldestRetained - 1) {
      client.deliverFromServer({ type: 'resync-required', oldestSeq: oldestRetained });
    }
    for (const entry of this.log) {
      if (entry.seq > lastEventSeq) client.deliverFromServer(entry.message);
    }
  }

  handleEventAck(seq: number): void {
    this.trimThrough(seq);
  }

  /** The caller has already sent intent-ack. Executes once per id, however often it's replayed. */
  handleIntent(msg: { id: string; validUntil?: number | null }, processingMs: number): void {
    if (this.processedIntents.has(msg.id)) return; // duplicate replay: its result is already in the log
    this.processedIntents.add(msg.id);
    const expired = typeof msg.validUntil === 'number' && Date.now() > msg.validUntil;
    setTimeout(() => {
      this.publish(
        expired
          ? { type: 'intent-result', id: msg.id, status: 'rejected', reason: 'expired' }
          : { type: 'intent-result', id: msg.id, status: 'executed' }
      );
    }, processingMs);
  }

  /** Events still held for (re)delivery - i.e. not yet acked by the client. */
  retainedEventCount(): number {
    return this.log.length;
  }

  private trimThrough(seq: number): void {
    while (this.log.length > 0 && this.log[0].seq <= seq) this.log.shift();
  }
}
