import type { ConfigDocument, Version } from './types';
import type { KvStore, LocalConfigCache } from './store';
import { TransportLedger } from './store';
import { jsonRequest } from './http';
import { canonicalJson, errorMessage, sha256 } from './util';

export interface ConfigTransport {
  bytes: number;
  fetches: number;
  perVersion: Record<Version, number>;
}

export interface ConfigSource {
  /** Stable name used in provision/source accounting. */
  name: string;
  baseUrl: string;
}

/**
 * Persistent config cache with single-flight fetch and source fail-over.
 *
 * Config bytes cross the wire at most once per (process, version) — never per
 * request. Concurrent callers for one version share a single fetch, and the
 * document is persisted so a restarted process can serve requests pinned to
 * that version without contacting the coordinator again.
 *
 * Sources are tried in order: the coordinator is the preferred source, but if
 * it is unreachable (or drops before a full, verified body is accepted) each
 * remaining source — peer entries holding their own durable cache — is tried
 * until one completes the document. A truncated or corrupt transfer therefore
 * never poisons the cache and never counts as transport bytes: accounting
 * happens only after the full body parses and the digest verifies.
 *
 * Two accounting modes:
 *   - with a ledger (entries), every verified fetch records its bytes here;
 *   - without a ledger (execution services), `fetchOnly` persists the body but
 *     leaves accounting to the service's single serialized install path, so a
 *     fetch and a concurrent push of the same version can never double-count.
 */
export class ConfigCache {
  private inflight = new Map<Version, Promise<ConfigDocument>>();
  private expected = new Map<Version, string>();
  readonly transport: ConfigTransport = { bytes: 0, fetches: 0, perVersion: {} };
  /** version → source that last completed a full verified delivery. */
  readonly sourceByVersion = new Map<Version, string>();
  /** version → byte size of the last full verified body received. */
  private readonly lastBytes = new Map<Version, number>();

  constructor(
    private readonly cache: LocalConfigCache,
    private readonly sources: ConfigSource[],
    private readonly ledger?: {
      store: KvStore;
      role: string;
      sink?: KvStore;
    },
  ) {}

  /** Convenience for callers that only know a single coordinator URL. */
  static withCoordinator(
    cache: LocalConfigCache,
    coordinator: string,
    ledger?: { store: KvStore; role: string; sink?: KvStore },
  ): ConfigCache {
    return new ConfigCache(cache, [{ name: 'coordinator', baseUrl: coordinator }], ledger);
  }

  /** Add a fallback source (e.g. a peer entry); coordinator stays preferred. */
  addSource(source: ConfigSource): void {
    if (this.sources.some((existing) => existing.baseUrl === source.baseUrl)) return;
    this.sources.push(source);
  }

  private recordBytes(version: Version, bytes: number, source: string): void {
    this.transport.bytes += bytes;
    this.transport.fetches += 1;
    this.transport.perVersion[version] = (this.transport.perVersion[version] ?? 0) + bytes;
    this.sourceByVersion.set(version, source);
    if (this.ledger) {
      const transport = new TransportLedger(this.ledger.store, this.ledger.sink);
      transport.addConfigBytes(this.ledger.role, version, bytes);
      transport.addFetch(this.ledger.role);
    }
  }

  has(version: Version): boolean {
    return this.cache.get(version) !== undefined;
  }

  /** Persist a document pushed by a peer (verified by the caller). */
  put(doc: ConfigDocument): void {
    this.cache.put(doc);
  }

  cached(version: Version): ConfigDocument | undefined {
    const doc = this.cache.get(version);
    if (!doc) return undefined;
    const digest = this.expected.get(version);
    if (digest && sha256(canonicalJson(doc)) !== digest) return undefined;
    return doc;
  }

  versions(): Version[] {
    return this.cache.versions();
  }

  setExpectedDigest(version: Version, digest: string): void {
    this.expected.set(version, digest);
  }

  /** Byte size of the most recent verified body received for `version`. */
  lastFetchBytes(version: Version): number {
    return this.lastBytes.get(version) ?? 0;
  }

  /** Fetch once (shared by concurrent callers), verify the digest, persist. */
  async get(version: Version, timeoutMs = 2500): Promise<ConfigDocument> {
    return this.run(version, timeoutMs, true);
  }

  /**
   * Fetch and persist WITHOUT recording transport bytes and WITHOUT the
   * in-process single-flight guard. The caller (an execution service) provides
   * its own coarser per-version lock that also covers concurrent peer pushes,
   * which the built-in single-flight cannot see. Accounting stays with the
   * caller via {@link lastFetchBytes}.
   */
  async fetchOnly(version: Version, timeoutMs = 2500): Promise<ConfigDocument> {
    const hit = this.cached(version);
    if (hit) return hit;
    return this.fetch(version, timeoutMs, false);
  }

  private run(version: Version, timeoutMs: number, record: boolean): Promise<ConfigDocument> {
    const hit = this.cached(version);
    if (hit) return Promise.resolve(hit);
    const running = this.inflight.get(version);
    if (running) return running;

    const promise = this.fetch(version, timeoutMs, record).finally(() =>
      this.inflight.delete(version),
    );
    this.inflight.set(version, promise);
    return promise;
  }

  /** Try to fetch; never throws. Used by preflight warm-up. */
  async warm(version: Version, timeoutMs = 2500): Promise<boolean> {
    try {
      await this.get(version, timeoutMs);
      return true;
    } catch {
      return false;
    }
  }

  private async fetch(
    version: Version,
    timeoutMs: number,
    record: boolean,
  ): Promise<ConfigDocument> {
    const errors: string[] = [];
    for (const source of this.sources) {
      const res = await jsonRequest<ConfigDocument>(source.baseUrl, {
        path: `/configs/${encodeURIComponent(version)}`,
        timeoutMs,
      });
      if (!res.ok || !res.body || typeof res.body !== 'object') {
        errors.push(`${source.name}: ${res.error ?? JSON.stringify(res.body ?? res.status)}`);
        continue;
      }
      const doc = res.body;
      if (doc.version !== version) {
        errors.push(`${source.name}: returned ${doc.version} for ${version}`);
        continue;
      }
      const digest = sha256(canonicalJson(doc));
      const expected = this.expected.get(version);
      if (expected && expected !== digest) {
        errors.push(`${source.name}: digest mismatch expected ${expected} got ${digest}`);
        continue;
      }
      // Full body received and verified. Persist first; record the size and
      // source regardless of who accounts, and only write the transport ledger
      // when this cache owns accounting. A truncated/failed transfer reaches
      // none of these lines, so partial bytes never count.
      this.cache.put(doc);
      this.lastBytes.set(version, res.bytesIn);
      this.sourceByVersion.set(version, source.name);
      if (record) this.recordBytes(version, res.bytesIn, source.name);
      return doc;
    }
    throw new Error(
      `config ${version} unavailable from every source [${this.sources
        .map((source) => source.name)
        .join(', ')}]: ${errors.join(' | ')}`,
    );
  }

  static describeError(err: unknown): string {
    return errorMessage(err);
  }
}
