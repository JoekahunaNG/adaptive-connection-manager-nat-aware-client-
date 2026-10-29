import * as fs from 'fs';

/**
 * Write-to-temp, fsync, then rename over the real file. rename() replaces
 * the target atomically, so a process killed mid-write (the OEM-killer
 * case) leaves either the old complete file or the new complete file on
 * disk - never a truncated one. Throws on failure; callers decide whether
 * that's fatal.
 */
export function writeFileAtomicSync(path: string, data: string): void {
  const tmp = `${path}.tmp`;
  const fd = fs.openSync(tmp, 'w');
  try {
    fs.writeSync(fd, data);
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
  fs.renameSync(tmp, path);
}
