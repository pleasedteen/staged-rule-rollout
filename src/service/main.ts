import type {
  ConfigDocument,
  ExecuteResponse,
  Topology,
  Version,
} from '../shared/types';
import {
  localStorePath,
  openStore,
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
import { ProgressOutbox } from '../shared/progress';
import {
  coordinatorUrl,
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
  /**
   * Completion evidence for the progress side channel. Purely additive: the
   * write-ahead journal stays the service's exclusive record of what it ran;
   * this outbox only publishes that a unit finished (with the version proof).
   */
  private readonly progress: ProgressOutbox;
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

  constructor(
    private readonly name: string,
    private readonly topology: Topology,
    private readonly local: KvStore,
  ) {
    this.cache = new ConfigCache(new LocalConfigCache(local, name), coordinatorUrl(topology), {
      store: local,
      role: `service:${name}`,
    });
    this.journal = new ServiceJournal(local, name);
    this.progress = new ProgressOutbox(local, coordinatorUrl(topology), name, 'service');
  }

  static async start(name: string, topology: Topology, dataDir: string): Promise<ExecutionService> {
    // Services never touch the cluster database: everything a service needs
    // (config cache, execution journal, boot counter) is local. This removes
    // 13 processes' worth of contention on one SQLite file at boot.
    const local = openStore(localStorePath(dataDir, name));
    const service = new ExecutionService(name, topology, local);
    await service.boot();
    return service;
  }

  private async boot(): Promise<void> {
    this.bootNo = new ProcessState(this.local, this.name).boot();
    this.instance = `${this.name}#${this.bootNo}`;
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
      this.publishCompletion(done, { recovered: true, healed: true });
      healed += 1;
    }
    if (healed > 0) logEvent(this.name, 'healed journaled units after restart', { healed });

    // Drain evidence buffered before a restart (e.g. coordinator was down).
    this.progress.start();

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
      return await this.cache.get(version, 800);
    } catch {
      return undefined;
    }
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

  /**
   * Publish a completion proof for one journaled unit. Only called once the
   * unit is durably `done` in the journal, so the evidence always corresponds
   * to a finished execution and never invents a segment that did not run.
   */
  private publishCompletion(
    entry: JournalEntry,
    flags: { recovered?: boolean; healed?: boolean; ms?: number } = {},
  ): void {
    this.progress.emit({
      stage: 'service_done',
      instance: this.instance,
      requestId: entry.requestId,
      slot: entry.slot,
      service: entry.service,
      version: entry.version,
      ok: !entry.error,
      recovered: Boolean(flags.recovered),
      healed: Boolean(flags.healed),
      ms: flags.ms,
    });
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
      const already = this.cache.has(doc.version) && sha256(canonicalJson(this.cache.cached(doc.version)!)) === actual;
      this.cache.put(doc);
      // Count full-config bytes crossing the process boundary, durably in the
      // service's own local store (no cross-process write contention).
      new TransportLedger(this.local).addConfigBytes(`service:${this.name}`, doc.version, ctx.bytes);
      return {
        status: 200,
        body: { ok: true, version: doc.version, digest: actual, cached: already, instance: this.instance },
      };
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
      return {
        status: 200,
        body: {
          ok: true,
          name: this.name,
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
      // A retry that hits a finished unit still proves the segment completed;
      // report it so late/repeated delivery can fill evidence gaps without
      // duplicating the segment (the coordinator keys by request × slot).
      this.publishCompletion(existing, { recovered: true, healed: Boolean(existing.healed) });
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
      const finishedAt = Date.now();
      this.journal.put({ ...done, updatedAt: finishedAt });
      this.publishCompletion(done, { recovered: false, healed: false, ms: finishedAt - startedAt });
      const response: ExecuteResponse = {
        ok: !done.error,
        service: this.name,
        instance: this.instance,
        version,
        output: done.output,
        ms: finishedAt - startedAt,
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
    this.progress.stop();
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
