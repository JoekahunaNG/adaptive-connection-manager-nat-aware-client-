import * as fs from 'fs';
import { writeFileAtomicSync } from './fsUtil';
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
  /**
   * Set when the persisted file existed but could not be parsed. The bad
   * file is moved aside (never deleted) so its intents can be recovered or
   * at least diagnosed, and the owner can surface this instead of the
   * queue silently coming back empty.
   */
  readonly corruptBackupPath: string | null;

  constructor(persistPath: string | null = null) {
    this.persistPath = persistPath;
    this.corruptBackupPath = this.load();
  }

  private load(): string | null {
    if (!this.persistPath || !fs.existsSync(this.persistPath)) return null;
    try {
      const parsed: unknown = JSON.parse(fs.readFileSync(this.persistPath, 'utf-8'));
      if (!Array.isArray(parsed)) throw new Error('outbox file is not an array');
      for (const item of parsed as OutboxItem[]) {
        // Files written before validUntil existed have no such field.
        this.items.set(item.id, { ...item, validUntil: item.validUntil ?? null });
      }
      return null;
    } catch {
      // persist() replaces the file atomically, so a crash mid-write can't
      // produce this - only disk corruption or a foreign write can. Keep
      // the evidence rather than overwriting it on the next enqueue.
      const backup = `${this.persistPath}.corrupt-${Date.now()}`;
      try {
        fs.renameSync(this.persistPath, backup);
      } catch {
        return null;
      }
      return backup;
    }
  }

  /** Atomic replace (see writeFileAtomicSync), so a crash mid-write can't lose every queued intent. */
  private persist(): void {
    if (!this.persistPath) return;
    try {
      writeFileAtomicSync(this.persistPath, JSON.stringify(Array.from(this.items.values())));
    } catch {
      // The previous complete file is still intact on disk and the item is
      // still held in memory, so a failed persist narrows durability for
      // this one change rather than corrupting existing state.
    }
  }

  enqueue(id: string, payload: unknown, now: number, validUntil: number | null = null): OutboxItem {
    const item: OutboxItem = { id, payload, enqueuedAt: now, validUntil, attempts: 0, lastAttemptAt: null };
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

  /** Remove an item (acked or expired). Returns it, or null if it was already gone. */
  remove(id: string): OutboxItem | null {
    const item = this.items.get(id);
    if (!item) return null;
    this.items.delete(id);
    this.persist();
    return item;
  }

  ack(id: string): OutboxItem | null {
    return this.remove(id);
  }

  get(id: string): OutboxItem | null {
    return this.items.get(id) ?? null;
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
