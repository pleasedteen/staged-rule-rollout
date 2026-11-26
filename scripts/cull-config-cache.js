#!/usr/bin/env node
/**
 * Remove one process's durable config cache from its local store.
 *
 * Used by the validator to model a *cold restart with missing configuration*:
 * the process's state directory, journal, boot counter and transport ledger
 * stay intact — only `cache:<name>:*` documents are deleted, so the next boot
 * genuinely lacks the version and must be re-provisioned over local HTTP.
 *
 * Works with both storage engines (node:sqlite and the lock-file JSON store).
 * Intentionally has no third-party imports; pass paths as arguments.
 *
 * Usage: node cull-config-cache.js <data-dir> <process-name>
 */
'use strict';

const { existsSync, readFileSync, writeFileSync } = require('node:fs');
const { join } = require('node:path');

function main() {
  const [dataDir, name] = process.argv.slice(2);
  if (!dataDir || !name) {
    process.stderr.write('usage: cull-config-cache.js <data-dir> <process-name>\n');
    process.exit(2);
  }
  const file = join(dataDir, 'local', `${name}.sqlite`);
  if (!existsSync(file)) {
    process.stdout.write(JSON.stringify({ ok: true, name, removed: 0, note: 'no local store yet' }) + '\n');
    return;
  }
  const prefix = `cache:${name}:`;
  let removed = 0;

  // The local store keeps the `.sqlite` extension regardless of engine, so the
  // file format must be detected from its content rather than its name.
  const head = readFileSync(file);
  const looksSqlite = head.subarray(0, 16).toString('latin1') === 'SQLite format 3\0';

  if (looksSqlite) {
    const sqlite = require('node:sqlite');
    if (!sqlite?.DatabaseSync) {
      throw new Error('local store is SQLite but node:sqlite is unavailable');
    }
    const db = new sqlite.DatabaseSync(file);
    try {
      const rows = db.prepare("SELECT k FROM kv WHERE k LIKE ?").all(`${prefix}%`);
      for (const row of rows) {
        db.prepare('DELETE FROM kv WHERE k = ?').run(row.k);
        removed += 1;
      }
    } finally {
      db.close();
    }
    process.stdout.write(JSON.stringify({ ok: true, name, engine: 'sqlite', removed }) + '\n');
    return;
  }

  // File-engine fallback: a plain JSON document of key → value.
  const data = JSON.parse(head.toString('utf8'));
  for (const key of Object.keys(data)) {
    if (key.startsWith(prefix)) {
      delete data[key];
      removed += 1;
    }
  }
  writeFileSync(file, JSON.stringify(data), 'utf8');
  process.stdout.write(JSON.stringify({ ok: true, name, engine: 'file', removed }) + '\n');
}

main();
