import type {
  ConfigDocument,
  ExecuteResponse,
  ServiceInstanceDef,
  Topology,
  Version,
} from '../shared/types';
import {
  localStorePath,
  openStore,
  openServiceJournalStore,
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
  coordinatorUrl,
  fatal,
  instanceUrl,
  instancesOf,
  loadTopology,
  logEvent,
  parseArgs,
} from '../shared/runtime';
import { canonicalJson, sha256, sleep } from '../shared/util';

interface Transport {
  configBytes: number;
  configFetches: number;
  requestBytes: number;
  requests: number;
}

/**
 * One runnable instance of an execution service.
 *
 * Rules and routes are still configured by *service name*: every instance of
 * `fx-tag` executes the same `fx-tag` rule from the same config version. An
 * instance is simply a process that can be started and stopped on its own.
 *
 * Two storage tiers make failover correct:
 *   - the config cache, transport counters and boot number are local to the
 *     instance, so each instance receives a config version at most once and
 *     request volume never affects config distribution;
 *   - the write-ahead journal is shared by every instance of the service. If
 *     an instance persists a unit's result and dies before its acknowledgement
 *     reaches the entry, the retry may land on a sibling, which answers from
 *     the durable journal with the original result instead of recomputing.
 */
class ExecutionService {
  private readonly cache: ConfigCache;
  private readonly journal: ServiceJournal;
  private readonly transport: Transport = {
    configBytes: 0,
    configFetches: 0,
    requestBytes: 0,
    requests: 0,
  };
  private instance = '';
  private bootNo = 0;
  private inflight = 0;
  private server?: JsonServer;
  /** One-shot fault used by the restart/ack-loss fixtures. */
  private crashAfterJournal = false;

  constructor(
    private readonly serviceName: string,
    private readonly instanceName: string,
    private readonly def: ServiceInstanceDef,
    private readonly topology: Topology,
    private readonly local: KvStore,
    private readonly journalStore: KvStore,
  ) {
    this.cache = new ConfigCache(
      new LocalConfigCache(local, instanceName),
      coordinatorUrl(topology),
      { store: local, role: `service:${instanceName}` },
    );
    this.journal = new ServiceJournal(journalStore, serviceName);
  }

  /**
   * Resolve which instance to run. `--instance` is optional when the service
   * has exactly one instance (whose name equals the service name in the legacy
   * single-instance topology), so old deployments need no new flags.
   */
  static resolveInstance(
    topology: Topology,
    serviceName: string,
    instanceName?: string,
  ): ServiceInstanceDef {
    const instances = instancesOf(topology, serviceName);
    if (instances.length === 0) throw new Error(`service ${serviceName} missing from topology`);
    if (instanceName) {
      const found = instances.find((candidate) => candidate.name === instanceName);
      if (!found) throw new Error(`instance ${instanceName} of ${serviceName} missing from topology`);
      return found;
    }
    if (instances.length > 1) {
      throw new Error(
        `service ${serviceName} has ${instances.length} instances; pass --instance <${instances
          .map((candidate) => candidate.name)
          .join('|')}>`,
      );
    }
    return instances[0];
  }

  static async start(
    topology: Topology,
    serviceName: string,
    instanceName: string | undefined,
    dataDir: string,
  ): Promise<ExecutionService> {
    const def = ExecutionService.resolveInstance(topology, serviceName, instanceName);
    // Per-instance local store (config cache + boot counter); shared journal.
    const local = openStore(localStorePath(dataDir, def.name));
    const journalStore = openServiceJournalStore(dataDir, serviceName);
    const service = new ExecutionService(serviceName, def.name, def, topology, local, journalStore);
    await service.boot();
    return service;
  }

  private async boot(): Promise<void> {
    this.bootNo = new ProcessState(this.local, this.instanceName).boot();
    this.instance = `${this.instanceName}#${this.bootNo}`;
    // Heal only units this instance started in a previous life. Units left
    // pending by a *live* sibling are that sibling's business; if the sibling
    // is gone, a retry landing here recomputes them deterministically.
    const mine = `${this.instanceName}#`;
    const pending = this.journal
      .pending()
      .filter((entry) => (entry.bootInstance ?? '').startsWith(mine));
    let healed = 0;
    for (const entry of pending) {
      const doc = await this.docFor(entry.version);
      if (!doc) {
        logEvent(this.instanceName, 'pending unit cannot heal: config missing', {
          version: entry.version,
          requestId: entry.requestId,
        });
        continue;
      }
      const done = this.compute(entry, doc);
      this.journal.put({
        ...done,
        healed: true,
        originInstance: done.originInstance ?? entry.bootInstance,
        bootInstance: this.instance,
        updatedAt: Date.now(),
      });
      healed += 1;
    }
    if (healed > 0) logEvent(this.instanceName, 'healed journaled units after restart', { healed });

    this.server = await startJsonServer(this.def.port, (ctx) => this.handle(ctx));
    logEvent(this.instanceName, 'listening', {
      url: this.server.url,
      service: this.serviceName,
      instance: this.instance,
      versions: this.cache.versions(),
      journal: this.journal.all().length,
    });
  }

  private async docFor(version: Version): Promise<ConfigDocument | undefined> {
    const cached = this.cache.cached(version);
    if (cached) return cached;
    try {
      return await this.cache.get(version, 800);
    } catch {
      return undefined;
    }
  }

  /** Pure, deterministic application of one rule; also used during boot heal. */
  private compute(entry: JournalEntry, doc: ConfigDocument): JournalEntry {
    const rule = doc.services[this.serviceName];
    if (!rule) {
      return {
        ...entry,
        state: 'done',
        output: undefined,
        error: `service ${this.serviceName} has no rule in ${doc.version}`,
        updatedAt: Date.now(),
      };
    }
    try {
      const output = applyRule(this.serviceName, rule, entry.input);
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
        body: {
          ok: true,
          role: 'service',
          name: this.serviceName,
          instanceName: this.instanceName,
          instance: this.instance,
          port: this.def.port,
          uptimeMs: 0,
        },
      };
    }

    if (path === '/admin/versions' && method === 'GET') {
      return {
        status: 200,
        body: {
          name: this.serviceName,
          instanceName: this.instanceName,
          instance: this.instance,
          port: this.def.port,
          versions: this.cache.versions(),
          journal: { total: this.journal.all().length, pending: this.journal.pending().length },
        },
      };
    }

    if (path === '/admin/config' && method === 'POST') {
      const doc = ctx.body?.doc as ConfigDocument | undefined;
      const digest = ctx.body?.digest as string | undefined;
      if (!doc || typeof doc.version !== 'string') {
        return { status: 400, body: { ok: false, code: 'bad_config', error: 'doc missing' } };
      }
      const actual = sha256(canonicalJson(doc));
      if (digest && digest !== actual) {
        return {
          status: 400,
          body: { ok: false, code: 'digest_mismatch', error: `expected ${digest}, got ${actual}` },
        };
      }
      this.transport.configBytes += ctx.bytes;
      this.transport.configFetches += 1;
      const already =
        this.cache.has(doc.version) &&
        sha256(canonicalJson(this.cache.cached(doc.version)!)) === actual;
      this.cache.put(doc);
      // Count full-config bytes crossing the process boundary, durably in the
      // instance's own local store (no cross-process write contention).
      new TransportLedger(this.local).addConfigBytes(
        `service:${this.instanceName}`,
        doc.version,
        ctx.bytes,
      );
      return {
        status: 200,
        body: {
          ok: true,
          version: doc.version,
          digest: actual,
          cached: already,
          service: this.serviceName,
          instance: this.instance,
        },
      };
    }

    if (path === '/execute' && method === 'POST') {
      return this.execute(ctx);
    }

    if (path === '/admin/fault' && method === 'POST') {
      const mode = ctx.body?.mode as string | undefined;
      if (mode !== 'crash-after-journal') {
        return { status: 400, body: { ok: false, error: `unknown fault mode ${String(mode)}` } };
      }
      this.crashAfterJournal = true;
      return { status: 200, body: { ok: true, armed: mode, instance: this.instance } };
    }

    if (path === '/admin/journal' && method === 'GET') {
      const all = this.journal.all();
      return {
        status: 200,
        body: {
          ok: true,
          service: this.serviceName,
          instanceName: this.instanceName,
          instance: this.instance,
          total: all.length,
          pending: all.filter((entry) => entry.state === 'pending').length,
          healed: all.filter((entry) => entry.healed).length,
        },
      };
    }

    if (path === '/admin/transport' && method === 'GET') {
      return {
        status: 200,
        body: {
          ok: true,
          name: this.instanceName,
          service: this.serviceName,
          transport: {
            configBytes: this.transport.configBytes + this.cache.transport.bytes,
            configFetches: this.transport.configFetches + this.cache.transport.fetches,
            requestBytes: this.transport.requestBytes,
            requests: this.transport.requests,
            versionsFetched: this.cache.transport.perVersion,
          },
        },
      };
    }

    if (path === '/admin/urls' && method === 'GET') {
      return {
        status: 200,
        body: {
          name: this.serviceName,
          instanceName: this.instanceName,
          ports: { self: instanceUrl(this.topology, this.serviceName, this.instanceName) },
        },
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
    const service = (ctx.body?.service as string | undefined) ?? this.serviceName;
    const input = ctx.body?.input as unknown;

    if (!requestId || !version || service !== this.serviceName) {
      return {
        status: 400,
        body: { ok: false, code: 'bad_request', error: 'requestId, version and service are required' },
      };
    }

    this.transport.requests += 1;
    this.transport.requestBytes += ctx.bytes;

    const key = `${requestId}|${slot}|${version}`;
    const existing = this.journal.get(key);

    // A durable result is authoritative even if this instance never received
    // the config version: the unit was already executed, deterministically,
    // under the pinned version. This is what lets a failover reuse a result
    // whose acknowledgement was lost.
    if (existing && existing.state === 'done') {
      const response: ExecuteResponse = {
        ok: !existing.error,
        service: this.serviceName,
        instance: this.instance,
        version: existing.version,
        output: existing.output,
        ms: 0,
        recovered: Boolean(existing.healed) || existing.bootInstance !== this.instance,
        cached: true,
        originInstance: existing.originInstance ?? existing.bootInstance,
        error: existing.error,
        code: existing.errorCode,
      };
      return { status: existing.error ? 422 : 200, body: response };
    }

    const doc = await this.docFor(version);
    if (!doc) {
      const response: ExecuteResponse = {
        ok: false,
        service: this.serviceName,
        instance: this.instance,
        version,
        ms: 0,
        recovered: false,
        error: `service ${this.serviceName} does not have rule set ${version}`,
        code: 'version_not_installed',
      };
      return { status: 409, body: { ...response, installed: this.cache.versions() } };
    }

    const startedAt = Date.now();
    this.inflight += 1;
    const pending: JournalEntry = {
      key,
      requestId,
      version,
      service: this.serviceName,
      slot,
      input,
      state: 'pending',
      originInstance: existing?.originInstance ?? this.instance,
      createdAt: existing?.createdAt ?? startedAt,
      updatedAt: startedAt,
      bootInstance: this.instance,
      healed: false,
    };
    this.journal.put(pending);

    const rule = doc.services[this.serviceName];
    const delayMs = rule?.delayMs ?? 0;
    try {
      if (delayMs > 0) await sleep(delayMs);
      const done = this.compute(pending, doc);
      this.journal.put({ ...done, updatedAt: Date.now() });
      if (this.crashAfterJournal) {
        // Durable result written; the acknowledgement never leaves. This is
        // exactly the "result landed but receipt was lost" window.
        this.crashAfterJournal = false;
        logEvent(this.instanceName, 'fault: crashing after journal write', { key });
        process.exit(137);
      }
      const response: ExecuteResponse = {
        ok: !done.error,
        service: this.serviceName,
        instance: this.instance,
        version,
        output: done.output,
        ms: Date.now() - startedAt,
        recovered: false,
        cached: false,
        originInstance: this.instance,
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
    this.journalStore.close();
  }
}

async function main(): Promise<void> {
  const args = parseArgs();
  const topology = loadTopology(args.topology);
  const name = args.name;
  if (!name) throw new Error('--name is required');
  const service = await ExecutionService.start(topology, name, args.instance, args.dataDir);
  const shutdown = async (): Promise<void> => {
    await service.close();
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown());
  process.on('SIGINT', () => void shutdown());
}

main().catch(fatal);
