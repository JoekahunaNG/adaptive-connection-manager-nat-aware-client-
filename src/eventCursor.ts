import * as fs from 'fs';
import { writeFileAtomicSync } from './fsUtil';

/**
 * Persists the highest server event `seq` this client has handled, so a
 * resume after process death asks the backend for exactly what was missed.
 *
 * The cursor is written *after* the event has been handed to listeners, so
 * a crash in between replays that event on restart rather than losing it:
 * delivery is at-least-once, and consumers dedupe by `seq`.
 *
 * Values:
 *  - null: fresh install, nothing seen yet - resume "live only".
 *  - 0:    the file existed but was unreadable - ask for everything the
 *          backend still retains (it answers resync-required if that's
 *          not enough), rather than silently skipping events.
 */
export class EventCursor {
  private value: number | null;

  constructor(private readonly persistPath: string | null = null) {
    this.value = this.load();
  }

  private load(): number | null {
    if (!this.persistPath || !fs.existsSync(this.persistPath)) return null;
    try {
      const parsed = JSON.parse(fs.readFileSync(this.persistPath, 'utf-8'));
      return typeof parsed?.lastEventSeq === 'number' ? parsed.lastEventSeq : 0;
    } catch {
      return 0;
    }
  }

  get(): number | null {
    return this.value;
  }

  set(seq: number): void {
    this.value = seq;
    if (!this.persistPath) return;
    try {
      writeFileAtomicSync(this.persistPath, JSON.stringify({ lastEventSeq: seq }));
    } catch {
      // In-memory value is still correct for this process; worst case after
      // a restart is replaying a few events, which consumers dedupe.
    }
  }
}
