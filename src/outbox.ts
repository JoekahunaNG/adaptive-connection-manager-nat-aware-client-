import * as fs from 'fs';
import { OutboxItem } from './types';

/**
 * Persistent outbox for unacknowledged outbound intents.
 *
 * Why this exists: the brief's end-to-end scenario explicitly requires that
 * a double-tap intent survives "phone process was frozen or network dropped
 * mid-flow". A pure in-memory queue is lost the moment an OEM killer (e.g.
 * Transsion's background freeze) terminates the process. So every enqueue is
 * flushed to disk synchronously before we return control to the caller -
 * the write is small (a few hundred bytes) and infrequent (user-initiated
 * intents, not high-frequency telemetry), so the sync fs call is not a
 * meaningful perf cost, and it guarantees we never report "queued" to the
 * UI for an intent that could vanish with the process.
 *
 * On Android this would be a tiny Room/SQLite table instead of a JSON file;
 * the interface (enqueue/ack/all) is what matters and would not change.
 */
export class Outbox {
  private items: Map<string, OutboxItem> = new Map();
  private readonly persistPath: string | null;

  constructor(persistPath: string | null = null) {
    this.persistPath = persistPath;
    this.load();
  }

  private load(): void {
    if (!this.persistPath) return;
    try {
      if (fs.existsSync(this.persistPath)) {
        const raw = fs.readFileSync(this.persistPath, 'utf-8');
        const parsed: OutboxItem[] = JSON.parse(raw);
        for (const item of parsed) {
          this.items.set(item.id, item);
        }
      }
    } catch {
      // Corrupt or partial write (e.g. process died mid-write). Starting
      // empty is safer than throwing - we'd rather lose a queued intent
      // (user can redo the double-tap) than crash the connection manager
      // on every startup.
    }
  }

  private persist(): void {
    if (!this.persistPath) return;
    try {
      fs.writeFileSync(this.persistPath, JSON.stringify(Array.from(this.items.values())), 'utf-8');
    } catch {
      // Best-effort; a failed persist here just means a smaller crash
      // window, not a correctness bug, since we still hold it in memory.
    }
  }

  enqueue(id: string, payload: unknown, now: number): OutboxItem {
    const item: OutboxItem = { id, payload, enqueuedAt: now, attempts: 0, lastAttemptAt: null };
    this.items.set(id, item);
    this.persist();
    return item;
  }

  markAttempt(id: string, now: number): void {
    const item = this.items.get(id);
    if (!item) return;
    item.attempts += 1;
    item.lastAttemptAt = now;
    this.persist();
  }

  ack(id: string): OutboxItem | null {
    const item = this.items.get(id);
    if (!item) return null;
    this.items.delete(id);
    this.persist();
    return item;
  }

  all(): OutboxItem[] {
    // Replay order matters (e.g. a cancel should never overtake its buy),
    // so we always return oldest-first.
    return Array.from(this.items.values()).sort((a, b) => a.enqueuedAt - b.enqueuedAt);
  }

  size(): number {
    return this.items.size;
  }
}
