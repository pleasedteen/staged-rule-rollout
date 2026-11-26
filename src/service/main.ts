import type {
  ConfigDocument,
  ExecuteResponse,
  Topology,
  Version,
} from '../shared/types';
import {
  localStorePath,
  openStore,
  openClusterTransportSink,
  LocalConfigCache,
  ProcessState,
  ServiceJournal,
  TransportLedger,
  type JournalEntry,
  type KvStore,
} from '../shared/store';
import { ConfigCache } from '../shared/configCache';
import { startJsonServer, type JsonServer } from '../shared/http';
import { applyRule, RuleError } from '../shared/engine';
import {
  configSources,
  fatal,
  loadTopology,
  logEvent,
  parseArgs,
  serviceUrl,
} from '../shared/runtime';
import { canonicalJson, sha256, sleep } from '../shared/util';

interface Transport {
  configBytes: number;
  configFetches: number;
  requestBytes: number;
  requests: number;
}

interface ProvisionEvent {
  version: string;
  source: string;
  bytes: number;
  at: number;
}

/**
 * Execution service.
 *
 * Every unit of work is journaled before it runs, keyed by
 * `requestId | slot | version`. A restart therefore has two recovery paths:
 *   - a unit that was mid-flight is recomputed deterministically during boot;
 *   - a unit that finished before the crash is answered from the journal.
 * Either way the entry receives the result for the version the request pinned
 * at first acceptance, never a "latest" interpretation.
 */
class ExecutionService {
  private readonly cache: ConfigCache;
  private readonly journal: ServiceJournal;
  private readonly ledger: TransportLedger;
  private readonly transport: Transport = {
    configBytes: 0,
    configFetches: 0,
    requestBytes: 0,
    requests: 0,
  };
  /**
   * Per-version install locks. Every way a config can arrive — an entry's
   * back-fill push or this process's own coordinator/peer fetch — funnels
   * through one serialized install per version, so overlapping delivery of the
   * same missing version completes (and provisions) exactly once.
   */
  private readonly installLocks = new Map<string, Promise<void>>();
  /** Versions this boot has already recorded a provision for (in-memory dedup). */
  private readonly provisionedVersions = new Set<string>();
  /** First-time provisions observed by this boot (durable count lives in the ledger). */
  private readonly provisionEvents: ProvisionEvent[] = [];
  private instance = '';
  private bootNo = 0;
  private inflight = 0;
  private server?: JsonServer;

  constructor(
    private readonly name: string,
    private readonly topology: Topology,
    private readonly local: KvStore,
    dataDir: string,
  ) {
    // No ledger inside ConfigCache: the service accounts every accepted body
    // in the single authoritative install path below (see `recordInstall`),
    // so pull and push paths can never double-count a byte or a provision.
    this.cache = new ConfigCache(
      new LocalConfigCache(local, name),
      configSources(topology),
    );
    this.journal = new ServiceJournal(local, name);
    // Local durability plus a separate cluster-wide sink. Services never open
    // the shared cluster state database; the sink is its own lock-file KV.
    this.ledger = new TransportLedger(local, openClusterTransportSink(dataDir));
  }

  static async start(name: string, topology: Topology, dataDir: string): Promise<ExecutionService> {
    // Services never touch the cluster database: everything a service needs
    // (config cache, execution journal, boot counter, transport) is local.
    // This removes 13 processes' worth of contention on one file at boot.
    const local = openStore(localStorePath(dataDir, name));
    const service = new ExecutionService(name, topology, local, dataDir);
    await service.boot();
    return service;
  }

  private async boot(): Promise<void> {
    this.bootNo = new ProcessState(this.local, this.name).boot();
    this.instance = `${this.name}#${this.bootNo}`;
    // Restore cumulative transport counters from the durable local ledger so
    // the in-memory view is continuous across a restart instead of resetting
    // to zero. Historical traffic therefore survives the reboot in both tiers.
    this.restoreTransportCounters();
    const pending = this.journal.pending();
    let healed = 0;
    for (const entry of pending) {
      const doc = await this.docFor(entry.version);
      if (!doc) {
        logEvent(this.name, 'pending unit cannot heal: config missing', {
          version: entry.version,
          requestId: entry.requestId,
        });
        continue;
      }
      const done = this.compute(entry, doc);
      this.journal.put({ ...done, healed: true, bootInstance: this.instance, updatedAt: Date.now() });
      healed += 1;
    }
    if (healed > 0) logEvent(this.name, 'healed journaled units after restart', { healed });

    // Publish the role's existing (possibly historical) counters so the
    // cluster aggregate includes this service from the moment it boots.
    this.ledger.publishSnapshot(`service:${this.name}`, this.bootNo);

    const port = this.topology.services.find((service) => service.name === this.name)?.port;
    if (!port) throw new Error(`service ${this.name} missing from topology`);
    this.server = await startJsonServer(port, (ctx) => this.handle(ctx));
    logEvent(this.name, 'listening', {
      url: this.server.url,
      instance: this.instance,
      versions: this.cache.versions(),
      journal: this.journal.all().length,
    });
  }

  private async docFor(version: Version): Promise<ConfigDocument | undefined> {
    const cached = this.cache.cached(version);
    if (cached) return cached;
    try {
      await this.ensureVersion(version, 800);
      return this.cache.cached(version);
    } catch {
      return undefined;
    }
  }

  /**
   * Run `job` under the per-version install lock. Every way a config can
   * arrive — an entry's back-fill push or this process's own ordered-source
   * fetch — funnels through here, so overlapping delivery of the same missing
   * version is serialized and can provision at most once.
   */
  private withInstallLock(version: Version, job: () => Promise<void>): Promise<void> {
    const previous = this.installLocks.get(version) ?? Promise.resolve();
    const next = previous.then(job, job).finally(() => {
      if (this.installLocks.get(version) === next) this.installLocks.delete(version);
    });
    this.installLocks.set(version, next);
    return next;
  }

  /**
   * Single authoritative accounting for one fully received, verified body.
   * Must be called inside the per-version lock. Every verified delivery
   * counts its full bytes (redeliveries included); a *provision* — the first
   * time this boot sees the version — is recorded exactly once. The complete
   * counters are then re-published as this role's durable cluster snapshot.
   */
  private recordInstall(version: Version, source: string, bytes: number): void {
    const role = `service:${this.name}`;
    this.transport.configBytes += bytes;
    this.transport.configFetches += 1;
    this.ledger.addConfigBytes(role, version, bytes);
    this.ledger.addFetch(role);
    if (!this.provisionedVersions.has(version)) {
      this.provisionedVersions.add(version);
      this.ledger.addProvision(role, version, source);
      this.provisionEvents.push({ version, source, bytes, at: Date.now() });
      logEvent(this.name, 'config provisioned', { version, source, instance: this.instance });
    }
    this.ledger.publishSnapshot(role, this.bootNo);
  }

  /**
   * Make `version` available, fetching from the ordered sources when the local
   * cache misses. The fetch (ConfigCache single-flight) and any concurrent peer
   * push are serialized by the install lock, so the missing version provisions
   * at most once even when both paths overlap.
   */
  private ensureVersion(version: Version, timeoutMs: number): Promise<void> {
    if (this.cache.has(version)) return Promise.resolve();
    const existing = this.installLocks.get(version);
    if (existing) return existing;
    return this.withInstallLock(version, async () => {
      if (this.cache.has(version)) return;
      // fetchOnly persists without accounting: the identical full body is
      // accounted here, under the same lock and ledger as pushed deliveries.
      await this.cache.fetchOnly(version, timeoutMs);
      const bytes = this.cache.lastFetchBytes(version);
      const source = this.cache.sourceByVersion.get(version) ?? 'fetch';
      this.recordInstall(version, source, bytes);
    });
  }

  /**
   * Verify and install one full document delivered over HTTP.
   *
   * The HTTP layer only invokes this after the request body has been fully
   * read, so `bytes` is always the complete received body. Bytes are accounted
   * on every verified delivery — including a redelivery of an already-installed
   * version — while a provision is recorded only the first time per version.
   */
  private async installDoc(
    doc: ConfigDocument,
    digest: string | undefined,
    bytes: number,
    source = 'push',
  ): Promise<{
    already: boolean;
    digest: string;
  }> {
    const version = doc.version;
    const actual = sha256(canonicalJson(doc));
    if (digest && digest !== actual) {
      throw new Error(`digest mismatch expected ${digest} got ${actual}`);
    }
    const wasInstalled =
      this.cache.has(version) &&
      sha256(canonicalJson(this.cache.cached(version)!)) === actual;

    await this.withInstallLock(version, async () => {
      this.cache.put(doc);
      this.recordInstall(version, source, bytes);
    });

    return { already: wasInstalled, digest: actual };
  }

  /** A missing version became available; durable + in-memory provision record. */
  private restoreTransportCounters(): void {
    const summary = this.ledger.summary();
    const role = `service:${this.name}`;
    this.transport.configBytes = summary.configBytes;
    this.transport.configFetches = summary.fetches[role] ?? 0;
  }

  /** Pure, deterministic application of one rule; also used during boot heal. */
  private compute(entry: JournalEntry, doc: ConfigDocument): JournalEntry {
    const rule = doc.services[this.name];
    if (!rule) {
      return {
        ...entry,
        state: 'done',
        output: undefined,
        error: `service ${this.name} has no rule in ${doc.version}`,
        updatedAt: Date.now(),
      };
    }
    try {
      const output = applyRule(this.name, rule, entry.input);
      return { ...entry, state: 'done', output, error: undefined, updatedAt: Date.now() };
    } catch (err) {
      const ruleError = err as RuleError;
      return {
        ...entry,
        state: 'done',
        output: undefined,
        error: ruleError.message,
        errorCode: ruleError.code,
        updatedAt: Date.now(),
      };
    }
  }

  private async handle(ctx: {
    method: string;
    path: string;
    query: URLSearchParams;
    body: any;
    bytes: number;
  }): Promise<{ status?: number; body?: unknown }> {
    const { path, method } = ctx;

    if (path === '/health' && method === 'GET') {
      return {
        status: 200,
        body: { ok: true, role: 'service', name: this.name, instance: this.instance, uptimeMs: 0 },
      };
    }

    if (path === '/admin/versions' && method === 'GET') {
      return {
        status: 200,
        body: {
          name: this.name,
          instance: this.instance,
          versions: this.cache.versions(),
          journal: { total: this.journal.all().length, pending: this.journal.pending().length },
        },
      };
    }

    if (path === '/admin/config' && method === 'POST') {
      const doc = ctx.body?.doc as ConfigDocument | undefined;
      const digest = ctx.body?.digest as string | undefined;
      // Optional provenance: a coordinator push says "coordinator", an entry
      // back-fill names the entry. Defaults to "push" for anonymous callers.
      const source = (ctx.body?.source as string | undefined) ?? 'push';
      if (!doc || typeof doc.version !== 'string') {
        return { status: 400, body: { ok: false, code: 'bad_config', error: 'doc missing' } };
      }
      try {
        // ctx.bytes is the fully received body; installDoc serializes
        // overlapping deliveries of the same version and does the accounting.
        const result = await this.installDoc(doc, digest, ctx.bytes, source);
        return {
          status: 200,
          body: {
            ok: true,
            version: doc.version,
            digest: result.digest,
            cached: result.already,
            provisioned: !result.already,
            instance: this.instance,
          },
        };
      } catch (err) {
        return {
          status: 400,
          body: { ok: false, code: 'digest_mismatch', error: (err as Error).message },
        };
      }
    }

    if (path === '/execute' && method === 'POST') {
      return this.execute(ctx);
    }

    if (path === '/admin/journal' && method === 'GET') {
      const all = this.journal.all();
      return {
        status: 200,
        body: {
          ok: true,
          instance: this.instance,
          total: all.length,
          pending: all.filter((entry) => entry.state === 'pending').length,
          healed: all.filter((entry) => entry.healed).length,
        },
      };
    }

    if (path === '/admin/transport' && method === 'GET') {
      const durable = this.ledger.summary();
      // This-boot provisioning view: the in-memory event list is reset on every
      // process start, so it answers "how many times did the missing version
      // have to be delivered during THIS cold start", whereas `provisions` is
      // the durable, cross-restart history (which deliberately accumulates).
      const bootProvisions: Record<string, { count: number; sources: Record<string, number> }> = {};
      for (const event of this.provisionEvents) {
        const entry = (bootProvisions[event.version] ??= { count: 0, sources: {} });
        entry.count += 1;
        entry.sources[event.source] = (entry.sources[event.source] ?? 0) + 1;
      }
      return {
        status: 200,
        body: {
          ok: true,
          name: this.name,
          instance: this.instance,
          bootNo: this.bootNo,
          transport: {
            // Authoritative count: every full body is recorded exactly in
            // recordInstall (both fetch and push paths), so no extra cache
            // counter is added here.
            configBytes: this.transport.configBytes,
            configFetches: this.transport.configFetches,
            requestBytes: this.transport.requestBytes,
            requests: this.transport.requests,
            versionsFetched: durable.byVersion,
            // Durable, cross-restart totals.
            provisions: durable.provisions[`service:${this.name}`] ?? {},
            // This-boot-only view for the exactly-once-per-cold-start check.
            bootProvisions,
            provisionEvents: this.provisionEvents,
            sources: Object.fromEntries(this.cache.sourceByVersion.entries()),
          },
        },
      };
    }

    if (path === '/admin/urls' && method === 'GET') {
      return {
        status: 200,
        body: { name: this.name, ports: { self: serviceUrl(this.topology, this.name) } },
      };
    }

    return { status: 404, body: { ok: false, code: 'not_found', error: `${method} ${path}` } };
  }

  private async execute(ctx: {
    body: any;
    bytes: number;
  }): Promise<{ status?: number; body?: unknown }> {
    const requestId = ctx.body?.requestId as string | undefined;
    const version = ctx.body?.version as Version | undefined;
    const slot = (ctx.body?.slot as string | undefined) ?? 'root';
    const service = (ctx.body?.service as string | undefined) ?? this.name;
    const input = ctx.body?.input as unknown;

    if (!requestId || !version || service !== this.name) {
      return {
        status: 400,
        body: { ok: false, code: 'bad_request', error: 'requestId, version and service are required' },
      };
    }

    this.transport.requests += 1;
    this.transport.requestBytes += ctx.bytes;

    const doc = await this.docFor(version);
    if (!doc) {
      const response: ExecuteResponse = {
        ok: false,
        service: this.name,
        instance: this.instance,
        version,
        ms: 0,
        recovered: false,
        error: `service ${this.name} does not have rule set ${version}`,
        code: 'version_not_installed',
      };
      return {
        status: 409,
        body: { ...response, installed: this.cache.versions() },
      };
    }

    const key = `${requestId}|${slot}|${version}`;
    const existing = this.journal.get(key);
    if (existing && existing.state === 'done') {
      const recovered = Boolean(existing.healed) || existing.bootInstance !== this.instance;
      const response: ExecuteResponse = {
        ok: !existing.error,
        service: this.name,
        instance: this.instance,
        version: existing.version,
        output: existing.output,
        ms: 0,
        recovered,
        error: existing.error,
        code: existing.errorCode,
      };
      return {
        status: existing.error ? 422 : 200,
        body: { ...response, cached: true },
      };
    }

    const startedAt = Date.now();
    this.inflight += 1;
    const pending: JournalEntry = {
      key,
      requestId,
      version,
      service: this.name,
      slot,
      input,
      state: 'pending',
      createdAt: existing?.createdAt ?? startedAt,
      updatedAt: startedAt,
      bootInstance: this.instance,
      healed: false,
    };
    this.journal.put(pending);

    const rule = doc.services[this.name];
    const delayMs = rule?.delayMs ?? 0;
    try {
      if (delayMs > 0) await sleep(delayMs);
      const done = this.compute(pending, doc);
      this.journal.put({ ...done, updatedAt: Date.now() });
      const response: ExecuteResponse = {
        ok: !done.error,
        service: this.name,
        instance: this.instance,
        version,
        output: done.output,
        ms: Date.now() - startedAt,
        recovered: false,
        error: done.error,
        code: done.errorCode,
      };
      return { status: done.error ? 422 : 200, body: response };
    } finally {
      this.inflight -= 1;
    }
  }

  async close(): Promise<void> {
    await this.server?.close();
    this.local.close();
  }

}

async function main(): Promise<void> {
  const args = parseArgs();
  const topology = loadTopology(args.topology);
  const name = args.name;
  if (!name) throw new Error('--name is required');
  const service = await ExecutionService.start(name, topology, args.dataDir);
  const shutdown = async (): Promise<void> => {
    await service.close();
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown());
  process.on('SIGINT', () => void shutdown());
}

main().catch(fatal);
