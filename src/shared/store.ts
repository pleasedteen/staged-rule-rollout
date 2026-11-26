import {
  closeSync,
  mkdirSync,
  openSync,
  readFileSync,
  renameSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { dirname } from 'node:path';
import type {
  BindingsView,
  ConfigDocument,
  CoordinatorState,
  ProgressEvent,
  RequestLedgerEntry,
  RequestRecord,
  StoredProgressRecord,
  Version,
} from './types';
import { bundleMeta } from './util';

/**
 * Durable key/value state with a zero-dependency fallback.
 *
 * Engines:
 *   - SQLite via `node:sqlite` (Node >= 22.5), WAL mode;
 *   - an atomically rewritten JSON document protected by a lock file.
 *
 * Both expose the same tiny API, so callers never branch on the engine.
 * Writes are synchronous: a crash right after `set` leaves a complete
 * snapshot, which is what makes the restart/recovery guarantees testable.
 */

export interface KvStore {
  readonly kind: 'sqlite' | 'file';
  readonly location: string;
  get<T>(key: string): T | undefined;
  set(key: string, value: unknown): void;
  delete(key: string): void;
  keys(prefix?: string): string[];
  close(): void;
}

interface SqliteDatabase {
  exec(sql: string): void;
  prepare(sql: string): {
    get(...args: unknown[]): unknown;
    all(...args: unknown[]): unknown[];
    run(...args: unknown[]): unknown;
  };
  close(): void;
}

class FileKvStore implements KvStore {
  readonly kind = 'file' as const;
  readonly location: string;

  constructor(file: string) {
    this.location = file;
    mkdirSync(dirname(file), { recursive: true });
  }

  /** Always read fresh: several processes share the cluster file. */
  private snapshot(): Record<string, unknown> {
    try {
      return JSON.parse(readFileSync(this.location, 'utf8')) as Record<string, unknown>;
    } catch {
      return {};
    }
  }

  get<T>(key: string): T | undefined {
    return this.snapshot()[key] as T | undefined;
  }

  set(key: string, value: unknown): void {
    this.mutate((data) => {
      data[key] = value;
    });
  }

  delete(key: string): void {
    this.mutate((data) => {
      delete data[key];
    });
  }

  keys(prefix = ''): string[] {
    return Object.keys(this.snapshot()).filter((key) => key.startsWith(prefix));
  }

  private mutate(fn: (data: Record<string, unknown>) => void): void {
    const lock = `${this.location}.lock`;
    const locked = this.acquire(lock);
    try {
      const data = this.snapshot();
      fn(data);
      const tmp = `${this.location}.${process.pid}.tmp`;
      writeFileSync(tmp, JSON.stringify(data), 'utf8');
      renameSync(tmp, this.location);
    } finally {
      if (locked) rmSync(lock, { force: true });
    }
  }

  /** Spin briefly; lock windows are sub-millisecond. */
  private acquire(lock: string): boolean {
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline) {
      try {
        closeSync(openSync(lock, 'wx'));
        return true;
      } catch {
        const wait = Date.now() + 1;
        while (Date.now() < wait) {
          /* spin */
        }
      }
    }
    return false;
  }

  close(): void {
    /* every mutation is already durable */
  }
}

class SqliteKvStore implements KvStore {
  readonly kind = 'sqlite' as const;
  readonly location: string;
  private readonly db: SqliteDatabase;

  constructor(file: string, Database: new (path: string) => SqliteDatabase) {
    this.location = file;
    mkdirSync(dirname(file), { recursive: true });
    this.db = new Database(file);
    // Set the busy timeout FIRST: every subsequent PRAGMA/DDL may need the
    // write lock, and several processes open this file during a cluster boot.
    this.db.exec('PRAGMA busy_timeout = 15000');
    // Rollback-journal mode with mmap disabled. WAL relies on a shared-memory
    // file that some container/network filesystems map unsafely (SIGBUS);
    // DELETE journaling is portable and still crash-safe at the committed
    // boundary, which is all the recovery model needs.
    this.db.exec('PRAGMA journal_mode = DELETE');
    this.db.exec('PRAGMA synchronous = FULL');
    this.db.exec('PRAGMA mmap_size = 0');
    this.db.exec('CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT NOT NULL)');
  }

  get<T>(key: string): T | undefined {
    const row = this.db.prepare('SELECT v FROM kv WHERE k = ?').get(key) as
      | { v?: string }
      | undefined;
    return row?.v === undefined ? undefined : (JSON.parse(row.v) as T);
  }

  set(key: string, value: unknown): void {
    this.db
      .prepare('INSERT INTO kv (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v')
      .run(key, JSON.stringify(value));
  }

  delete(key: string): void {
    this.db.prepare('DELETE FROM kv WHERE k = ?').run(key);
  }

  keys(prefix = ''): string[] {
    const rows = this.db.prepare('SELECT k FROM kv WHERE k LIKE ?').all(`${prefix}%`) as {
      k: string;
    }[];
    return rows.map((row) => row.k);
  }

  close(): void {
    this.db.close();
  }
}

let engine: 'auto' | 'sqlite' | 'file' =
  (process.env.RULES_STORE as 'auto' | 'sqlite' | 'file' | undefined) ?? 'auto';

export function setStoreEngine(next: 'auto' | 'sqlite' | 'file'): void {
  engine = next;
}

function sqliteModule(): { DatabaseSync?: new (path: string) => SqliteDatabase } | undefined {
  try {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    return require('node:sqlite') as { DatabaseSync?: new (path: string) => SqliteDatabase };
  } catch {
    return undefined;
  }
}

export function storeEngine(): 'sqlite' | 'file' {
  if (engine === 'file') return 'file';
  return sqliteModule()?.DatabaseSync ? 'sqlite' : 'file';
}

export function openStore(file: string, engineOverride?: 'sqlite' | 'file'): KvStore {
  if (engineOverride === 'file') return new FileKvStore(file);
  if (engine !== 'file') {
    try {
      const mod = sqliteModule();
      if (mod?.DatabaseSync) return new SqliteKvStore(file, mod.DatabaseSync);
      if (engine === 'sqlite' || engineOverride === 'sqlite') {
        throw new Error('node:sqlite is unavailable');
      }
    } catch (err) {
      if (engine === 'sqlite' || engineOverride === 'sqlite') throw err;
      process.stderr.write(
        `[store] node:sqlite unavailable (${(err as Error).message}); using JSON store\n`,
      );
    }
  }
  return new FileKvStore(file);
}

/**
 * Cluster store: always the lock+atomic-rename file engine.
 *
 * This store is opened by the coordinator *and every entry* concurrently. On
 * container / network / FUSE filesystems SQLite cannot guarantee multi-process
 * locking (observed: `disk I/O error` / `readonly database` under concurrent
 * writers), whereas the lock-file + atomic-rename engine is portable. Local
 * per-process stores still use SQLite, where there is only one writer.
 */
export function openClusterStore(dataDir: string): KvStore {
  return new FileKvStore(clusterStorePath(dataDir));
}

/**
 * Two tiers of storage:
 *   - cluster: coordinator state, bindings, configs, request ledger/records.
 *     Shared by the coordinator and all entries. Uses the multi-process-safe
 *     file engine (see `openClusterStore`).
 *   - local: config cache, execution journal and boot counters for one
 *     process. Never shared, so it can use SQLite with a single writer.
 */
export function clusterStorePath(dataDir: string): string {
  return `${dataDir}/cluster.state.json`;
}

export function localStorePath(dataDir: string, processId: string): string {
  return `${dataDir}/local/${processId}.sqlite`;
}

export function wipeStore(dataDir: string): void {
  for (const file of [clusterStorePath(dataDir)]) {
    for (const suffix of ['', '-journal', '-wal', '-shm', '.lock', '.tmp']) {
      rmSync(`${file}${suffix}`, { force: true });
    }
  }
  rmSync(`${dataDir}/local`, { recursive: true, force: true });
}

/* ------------------------------- Coordinator ------------------------------- */

const STATE_KEY = 'coordinator:state';
const BINDINGS_KEY = 'coordinator:bindings';

export class CoordinatorRepository {
  constructor(readonly store: KvStore) {}

  loadState(): CoordinatorState | undefined {
    return this.store.get<CoordinatorState>(STATE_KEY);
  }

  saveState(state: CoordinatorState): void {
    state.seq += 1;
    state.updatedAt = Date.now();
    this.store.set(STATE_KEY, state);
  }

  loadBindings(): BindingsView | undefined {
    return this.store.get<BindingsView>(BINDINGS_KEY);
  }

  saveBindings(bindings: BindingsView): void {
    this.store.set(BINDINGS_KEY, { ...bindings, updatedAt: Date.now() });
  }
}

/* -------------------------------- Config ---------------------------------- */

export class ConfigRepository {
  constructor(private readonly store: KvStore) {}

  publish(doc: ConfigDocument): { digest: string; size: number } {
    const meta = bundleMeta(doc);
    this.store.set(`config:${doc.version}`, doc);
    this.store.set(`config-meta:${doc.version}`, { version: doc.version, ...meta });
    return meta;
  }

  get(version: Version): ConfigDocument | undefined {
    return this.store.get<ConfigDocument>(`config:${version}`);
  }

  meta(version: Version): { version: Version; digest: string; size: number } | undefined {
    return this.store.get(`config-meta:${version}`);
  }

  versions(): Version[] {
    return this.store
      .keys('config-meta:')
      .map((key) => key.slice('config-meta:'.length))
      .sort();
  }
}

/* --------------------------------- Ledger --------------------------------- */

export class LedgerRepository {
  constructor(private readonly store: KvStore) {}

  putEntry(entry: RequestLedgerEntry): void {
    this.store.set(`ledger:${entry.requestId}`, entry);
  }

  getEntry(requestId: string): RequestLedgerEntry | undefined {
    return this.store.get<RequestLedgerEntry>(`ledger:${requestId}`);
  }

  getRecord(requestId: string): RequestRecord | undefined {
    return this.store.get<RequestRecord>(`record:${requestId}`);
  }

  saveRecord(record: RequestRecord): void {
    this.store.set(`record:${record.requestId}`, record);
  }

  entries(): RequestLedgerEntry[] {
    return this.store
      .keys('ledger:')
      .map((key) => this.store.get<RequestLedgerEntry>(key))
      .filter((entry): entry is RequestLedgerEntry => Boolean(entry));
  }

  records(): RequestRecord[] {
    return this.store
      .keys('record:')
      .map((key) => this.store.get<RequestRecord>(key))
      .filter((record): record is RequestRecord => Boolean(record));
  }
}

/* --------------------------------- Cache ---------------------------------- */

/** Persistent config cache for one process. */
export class LocalConfigCache {
  private readonly prefix: string;

  constructor(
    private readonly store: KvStore,
    processId: string,
  ) {
    this.prefix = `cache:${processId}:`;
  }

  get(version: Version): ConfigDocument | undefined {
    return this.store.get<ConfigDocument>(`${this.prefix}${version}`);
  }

  put(doc: ConfigDocument): void {
    this.store.set(`${this.prefix}${doc.version}`, doc);
  }

  versions(): Version[] {
    return this.store
      .keys(this.prefix)
      .map((key) => key.slice(this.prefix.length))
      .sort();
  }
}

/* ----------------------------- Progress ledger ---------------------------- */

/**
 * Durable, idempotent accumulator of segment progress evidence.
 *
 * Lives in the cluster store so the coordinator owns the single aggregate even
 * when several producers report concurrently. Semantics that the query path
 * depends on:
 *   - keyed by (requestId, slot) — one segment can never appear twice;
 *   - re-delivered / late events can only fill a missing field, never change a
 *     field already observed (no regress, no reorder);
 *   - every accepted event is counted so the aggregate is observable.
 */
export class ProgressRepository {
  constructor(private readonly store: KvStore) {}

  private key(requestId: string): string {
    return `progress:${requestId}`;
  }

  get(requestId: string): StoredProgressRecord | undefined {
    return this.store.get<StoredProgressRecord>(this.key(requestId));
  }

  /** Merge one event; returns whether it added anything new. */
  apply(event: ProgressEvent): 'applied' | 'duplicate' | 'stale' {
    return this.applyMany([event]).outcomes[0] ?? 'stale';
  }

  /**
   * Merge a batch with one read-modify-write per affected request. Producers
   * report in bursts; coalescing keeps contention on the shared cluster file
   * proportional to requests rather than to events.
   */
  applyMany(events: ProgressEvent[]): {
    outcomes: ('applied' | 'duplicate' | 'stale')[];
    applied: number;
    duplicate: number;
    stale: number;
  } {
    const byRequest = new Map<string, { event: ProgressEvent; index: number }[]>();
    events.forEach((event, index) => {
      const list = byRequest.get(event.requestId);
      if (list) list.push({ event, index });
      else byRequest.set(event.requestId, [{ event, index }]);
    });
    const outcomes: ('applied' | 'duplicate' | 'stale')[] = new Array(events.length).fill('stale');
    let applied = 0;
    let duplicate = 0;
    let stale = 0;
    for (const [requestId, list] of byRequest) {
      const record = this.get(requestId) ?? this.blank(requestId);
      for (const { event, index } of list) {
        const outcome = applyEventToRecord(record, event);
        outcomes[index] = outcome;
        if (outcome === 'applied') applied += 1;
        else if (outcome === 'duplicate') duplicate += 1;
        else stale += 1;
      }
      this.store.set(this.key(requestId), record);
    }
    return { outcomes, applied, duplicate, stale };
  }

  private blank(requestId: string): StoredProgressRecord {
    return { requestId, segments: {}, nextOrder: 0, updatedAt: 0, events: 0 };
  }
}

function applyEventToRecord(
  record: StoredProgressRecord,
  event: ProgressEvent,
): 'applied' | 'duplicate' | 'stale' {
  const existing = record.segments[event.slot];
  const segment = existing ?? {
    slot: event.slot,
    service: event.service,
    order: record.nextOrder,
    firstSeenAt: event.at,
  };
  if (!existing) record.nextOrder += 1;
  segment.firstSeenAt = Math.min(segment.firstSeenAt, event.at);
  record.events += 1;

  let changed = !existing;
  let duplicate = false;

  if (event.stage === 'service_done') {
    if (!segment.serviceCompleted) {
      segment.serviceCompleted = {
        version: event.version,
        instance: event.instance,
        producer: event.producer,
        at: event.at,
        recovered: Boolean(event.recovered),
        healed: Boolean(event.healed),
        ok: event.ok !== false,
        ms: event.ms,
      };
      changed = true;
    } else if (segment.serviceCompleted.at === event.at) {
      duplicate = true;
    } else if (segment.serviceCompleted.version !== event.version) {
      // Two independent proofs of completion disagreeing on the version is
      // itself a gap worth surfacing; keep the first and flag it.
      if (!segment.conflict) {
        segment.conflict = true;
        changed = true;
      }
    }
  } else if (!segment.entryReceipt) {
    segment.entryReceipt = {
      version: event.version,
      entry: event.entry ?? event.producer,
      instance: event.instance,
      at: event.at,
    };
    changed = true;
  } else if (segment.entryReceipt.at === event.at) {
    duplicate = true;
  } else if (segment.entryReceipt.version !== event.version) {
    if (!segment.conflict) {
      segment.conflict = true;
      changed = true;
    }
  }

  record.segments[event.slot] = segment;
  record.updatedAt = Math.max(record.updatedAt, event.at);
  if (duplicate && !changed) return 'duplicate';
  return changed ? 'applied' : 'stale';
}

/* -------------------------------- Journal --------------------------------- */

export interface JournalEntry {
  key: string;
  requestId: string;
  version: Version;
  service: string;
  slot: string;
  input: unknown;
  output?: unknown;
  error?: string;
  errorCode?: string;
  state: 'pending' | 'done';
  /** True if the unit was completed by the boot-time healer. */
  healed?: boolean;
  createdAt: number;
  updatedAt: number;
  bootInstance: string;
}

/**
 * Execution-service write-ahead journal.
 *
 * A service persists the unit of work (request × slot × service, including the
 * pinned version and input) before doing any work. If the process dies during
 * the simulated delay, the replacement recomputes the same output on boot;
 * entry-side retries then read a completed result rather than a lost request.
 */
export class ServiceJournal {
  private readonly prefix: string;

  constructor(
    private readonly store: KvStore,
    serviceName: string,
  ) {
    this.prefix = `journal:${serviceName}:`;
  }

  get(key: string): JournalEntry | undefined {
    return this.store.get<JournalEntry>(`${this.prefix}${key}`);
  }

  put(entry: JournalEntry): void {
    this.store.set(`${this.prefix}${entry.key}`, entry);
  }

  pending(): JournalEntry[] {
    return this.store
      .keys(this.prefix)
      .map((key) => this.store.get<JournalEntry>(key))
      .filter((entry): entry is JournalEntry => Boolean(entry) && entry!.state === 'pending');
  }

  all(): JournalEntry[] {
    return this.store
      .keys(this.prefix)
      .map((key) => this.store.get<JournalEntry>(key))
      .filter((entry): entry is JournalEntry => Boolean(entry));
  }
}

/* ------------------------------- Boot counters ---------------------------- */

export class ProcessState {
  constructor(
    private readonly store: KvStore,
    private readonly processId: string,
  ) {}

  /** Increment and return the boot sequence for this process name. */
  boot(): number {
    const key = `boot:${this.processId}`;
    const next = (this.store.get<number>(key) ?? 0) + 1;
    this.store.set(key, next);
    return next;
  }

  instance(): string {
    return `${this.processId}#${this.store.get<number>(`boot:${this.processId}`) ?? 0}`;
  }
}

/* ----------------------------- Transport ledger ---------------------------- */

/**
 * Durable accounting of full-config bytes crossing process boundaries.
 *
 * The middleware only ever ships a configuration document once per
 * (process, version) — requests carry the version string, never the payload.
 * This ledger makes that invariant observable: its total must not grow when
 * request volume grows under a fixed topology, version set and lifecycle.
 */
export class TransportLedger {
  constructor(private readonly store: KvStore) {}

  addConfigBytes(role: string, version: Version, bytes: number): void {
    const key = 'transport:config-version';
    const map = this.store.get<Record<string, number>>(key) ?? {};
    map[version] = (map[version] ?? 0) + bytes;
    this.store.set(key, map);
    const roles = this.store.get<Record<string, number>>('transport:config-role') ?? {};
    roles[role] = (roles[role] ?? 0) + bytes;
    this.store.set('transport:config-role', roles);
  }

  addFetch(role: string): void {
    const fetches = this.store.get<Record<string, number>>('transport:config-fetches') ?? {};
    fetches[role] = (fetches[role] ?? 0) + 1;
    this.store.set('transport:config-fetches', fetches);
  }

  summary(): {
    configBytes: number;
    byVersion: Record<string, number>;
    byRole: Record<string, number>;
    fetches: Record<string, number>;
  } {
    const byVersion = this.store.get<Record<string, number>>('transport:config-version') ?? {};
    const byRole = this.store.get<Record<string, number>>('transport:config-role') ?? {};
    const fetches = this.store.get<Record<string, number>>('transport:config-fetches') ?? {};
    const configBytes = Object.values(byVersion).reduce((sum, value) => sum + value, 0);
    return { configBytes, byVersion, byRole, fetches };
  }
}
