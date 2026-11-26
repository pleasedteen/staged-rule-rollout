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

/**
 * Persistent config cache with single-flight fetch.
 *
 * Config bytes cross the wire at most once per (process, version) — never per
 * request. Concurrent callers for one version share a single fetch, and the
 * document is persisted so a restarted process can serve requests pinned to
 * that version without contacting the coordinator again.
 *
 * Every byte accepted from the network is also recorded in a durable transport
 * ledger, so the "no growth with request volume" invariant is measurable
 * across restarts rather than only in-process.
 */
export class ConfigCache {
  private inflight = new Map<Version, Promise<ConfigDocument>>();
  private expected = new Map<Version, string>();
  readonly transport: ConfigTransport = { bytes: 0, fetches: 0, perVersion: {} };

  constructor(
    private readonly cache: LocalConfigCache,
    private readonly coordinatorUrl: string,
    private readonly ledger?: { store: KvStore; role: string },
  ) {}

  private recordBytes(version: Version, bytes: number): void {
    this.transport.bytes += bytes;
    this.transport.fetches += 1;
    this.transport.perVersion[version] = (this.transport.perVersion[version] ?? 0) + bytes;
    if (this.ledger) {
      const transport = new TransportLedger(this.ledger.store);
      transport.addConfigBytes(this.ledger.role, version, bytes);
      transport.addFetch(this.ledger.role);
    }
  }

  has(version: Version): boolean {
    return this.cache.get(version) !== undefined;
  }

  /** Persist a document pushed by the coordinator (verified by the caller). */
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

  /** Fetch once (shared by concurrent callers), verify the digest, persist. */
  async get(version: Version, timeoutMs = 2500): Promise<ConfigDocument> {
    const hit = this.cached(version);
    if (hit) return hit;
    const running = this.inflight.get(version);
    if (running) return running;

    const promise = this.fetch(version, timeoutMs).finally(() => this.inflight.delete(version));
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

  private async fetch(version: Version, timeoutMs: number): Promise<ConfigDocument> {
    const res = await jsonRequest<ConfigDocument>(this.coordinatorUrl, {
      path: `/configs/${encodeURIComponent(version)}`,
      timeoutMs,
    });
    if (!res.ok || !res.body || typeof res.body !== 'object') {
      throw new Error(
        `config ${version} unavailable from coordinator: ${
          res.error ?? JSON.stringify(res.body ?? res.status)
        }`,
      );
    }
    const doc = res.body;
    if (doc.version !== version) {
      throw new Error(`coordinator returned ${doc.version} for ${version}`);
    }
    const digest = sha256(canonicalJson(doc));
    const expected = this.expected.get(version);
    if (expected && expected !== digest) {
      throw new Error(`config ${version} digest mismatch: expected ${expected}, got ${digest}`);
    }
    this.cache.put(doc);
    this.recordBytes(version, res.bytesIn);
    return doc;
  }

  static describeError(err: unknown): string {
    return errorMessage(err);
  }
}
