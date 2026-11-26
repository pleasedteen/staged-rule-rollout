import type {
  ApiError,
  BindingsView,
  ConfigDocument,
  EntryProcessDef,
  GroupId,
  RequestLedgerEntry,
  RequestReceipt,
  RequestRecord,
  RouteStep,
  SegmentResult,
  ServiceName,
  Topology,
  Version,
} from '../shared/types';
import {
  openClusterStore,
  localStorePath,
  openStore,
  openClusterTransportSink,
  LedgerRepository,
  LocalConfigCache,
  ProcessState,
  TransportLedger,
  type KvStore,
} from '../shared/store';
import { ConfigCache } from '../shared/configCache';
import { jsonRequest, startJsonServer, type JsonServer } from '../shared/http';
import { PeerClient } from '../shared/peer';
import { joinValues, routePath, RuleError } from '../shared/engine';
import {
  configSources,
  coordinatorUrl,
  fatal,
  loadTopology,
  logEvent,
  parseArgs,
  serviceUrl,
} from '../shared/runtime';
import { newId, sleep } from '../shared/util';

interface BindingState {
  epoch: number;
  versions: Record<GroupId, Version>;
  digests: Record<Version, string>;
  pendingEpoch: number | null;
}

interface TransportTotals {
  configBytes: number;
  configFetches: number;
  requestBytes: number;
  requests: number;
}

/**
 * Local entry (gateway).
 *
 * Responsibilities:
 *   - accept a request against the group's *currently effective* version and
 *     persist that pin in a shared ledger before replying;
 *   - execute the group route: sequential `next` plus parallel `branch` nodes
 *     joined by the node's `join` op, always passing the pinned version to
 *     every downstream segment;
 *   - resume execution from the ledger, possibly on a different entry, after
 *     an entry restart or a cross-entry retry.
 *
 * The coordinator being down does not stop acceptance: bindings are cached
 * locally and refreshed whenever the coordinator is reachable.
 */
class EntryGateway {
  private readonly cache: ConfigCache;
  private readonly ledger: LedgerRepository;
  private readonly peers = new PeerClient();
  private readonly binding: BindingState = {
    epoch: 0,
    versions: {},
    digests: {},
    pendingEpoch: null,
  };
  /** Payload held between prepare and commit; null when nothing is staged. */
  private staged: {
    epoch: number;
    versions: Record<GroupId, Version>;
    digests: Record<Version, string>;
  } | null = null;
  private readonly transport: TransportTotals = {
    configBytes: 0,
    configFetches: 0,
    requestBytes: 0,
    requests: 0,
  };
  private inflight = 0;
  private instance = '';
  private bootNo = 0;
  private server?: JsonServer;
  private active = new Set<Promise<void>>();
  /** Requests this process is currently executing (for graceful drain). */
  private running = new Map<string, RequestRecord>();
  private syncTimer?: NodeJS.Timeout;
  private snapshotTimer?: NodeJS.Timeout;
  private syncing = false;
  private initialized = false;
  private readonly transportLedger: TransportLedger;

  constructor(
    private readonly name: string,
    private readonly topology: Topology,
    private readonly cluster: KvStore,
    private readonly local: KvStore,
    dataDir: string,
  ) {
    // Coordinator is the preferred source; peer entries are fallbacks used
    // when the preferred source is unreachable. ConfigCache writes the entry's
    // local transport counters, which this process republishes as a complete
    // durable snapshot so its traffic survives going offline.
    this.transportLedger = new TransportLedger(local, openClusterTransportSink(dataDir));
    this.cache = new ConfigCache(
      new LocalConfigCache(local, name),
      configSources(topology, name),
      {
        store: local,
        role: `entry:${name}`,
      },
    );
    this.ledger = new LedgerRepository(cluster);
  }

  static async start(
    name: string,
    topology: Topology,
    dataDir: string,
  ): Promise<EntryGateway> {
    const cluster = openClusterStore(dataDir);
    const local = openStore(localStorePath(dataDir, name));
    const gateway = new EntryGateway(name, topology, cluster, local, dataDir);
    await gateway.boot();
    return gateway;
  }

  private definition(): EntryProcessDef {
    const entry = this.topology.entries.find((candidate) => candidate.name === this.name);
    if (!entry) throw new Error(`entry ${this.name} missing from topology`);
    return entry;
  }

  private async boot(): Promise<void> {
    this.bootNo = new ProcessState(this.local, this.name).boot();
    this.instance = `${this.name}#${this.bootNo}`;
    // Restore the last committed bindings first: if the coordinator is down an
    // entry must still accept against the versions that were effective.
    const saved = this.local.get<BindingState>('bindings:last');
    if (saved) {
      this.binding.epoch = saved.epoch;
      this.binding.versions = { ...saved.versions };
      this.binding.digests = { ...saved.digests };
      for (const [version, digest] of Object.entries(this.binding.digests)) {
        this.cache.setExpectedDigest(version, digest);
      }
      this.initialized = true;
    }
    await this.pullBindings();
    // Pre-warm every version any of our groups may be pinned to. Combined with
    // the durable local cache this is what lets the entry keep accepting if the
    // coordinator is unreachable.
    for (const version of new Set(Object.values(this.binding.versions))) {
      await this.cache.warm(version, 1500).catch(() => false);
    }
    const def = this.definition();
    this.server = await startJsonServer(def.port, (ctx) => this.handle(ctx));
    // Keep bindings fresh without making acceptance depend on the coordinator.
    // This is what lets a stopped coordinator coexist with live traffic.
    this.syncTimer = setInterval(() => void this.sync(), 200);
    this.syncTimer.unref?.();
    // Republish this entry's complete transport counters periodically so the
    // cluster sink stays fresh; the same snapshot is rewritten after a
    // restart (no accumulation) and retained if the entry goes offline.
    this.publishTransportSnapshot();
    this.snapshotTimer = setInterval(() => this.publishTransportSnapshot(), 500);
    this.snapshotTimer.unref?.();
    logEvent(this.name, 'listening', {
      url: this.server.url,
      instance: this.instance,
      epoch: this.binding.epoch,
      groups: def.groups,
      versions: this.binding.versions,
    });
  }

  /** Publish this entry's full local counters as its durable cluster snapshot. */
  private publishTransportSnapshot(): void {
    this.transportLedger.publishSnapshot(`entry:${this.name}`, this.bootNo);
  }

  /** Durably remember committed bindings so acceptance survives restarts. */
  private persistBindings(): void {
    this.local.set('bindings:last', {
      epoch: this.binding.epoch,
      versions: this.binding.versions,
      digests: this.binding.digests,
      pendingEpoch: null,
    } satisfies BindingState);
  }

  /* ------------------------------- bindings -------------------------------- */

  private async sync(): Promise<void> {
    if (this.syncing) return;
    this.syncing = true;
    try {
      await this.pullBindings();
    } catch {
      /* coordinator may be down; bindings stay as last committed */
    } finally {
      this.syncing = false;
    }
  }

  /** Fetch bindings from the coordinator; keep-local on failure. */
  private async pullBindings(): Promise<void> {
    const res = await jsonRequest<BindingsView>(coordinatorUrl(this.topology), {
      path: '/admin/bindings',
      timeoutMs: 800,
    });
    if (!res.ok || !res.body) return;
    const next = res.body;
    // Advance, or initialize for the very first time. A stale read (older
    // epoch) must never rewind the pin map.
    const shouldApply =
      !this.initialized || (typeof next.epoch === 'number' && next.epoch > this.binding.epoch);
    if (shouldApply && typeof next.epoch === 'number') {
      this.binding.epoch = next.epoch;
      this.binding.versions = { ...next.versions };
      this.binding.digests = { ...(next.digests ?? {}) };
      for (const [version, digest] of Object.entries(this.binding.digests)) {
        this.cache.setExpectedDigest(version, digest);
      }
      this.initialized = true;
      this.persistBindings();
    }
  }

  /* --------------------------------- routes -------------------------------- */

  private async doc(version: Version): Promise<ConfigDocument> {
    return this.cache.get(version, 2500);
  }

  /**
   * Execute a route node: run its service, recurse into branch children in
   * parallel and join, then feed the joined value into `next`.
   */
  private async runStep(
    step: RouteStep,
    input: unknown,
    record: RequestRecord,
  ): Promise<unknown> {
    const segments: SegmentResult[] = [];
    const output = await this.runStepInto(step, input, record, [], segments);
    record.segments.push(...segments);
    return output;
  }

  private async runStepInto(
    step: RouteStep,
    input: unknown,
    record: RequestRecord,
    path: ServiceName[],
    segments: SegmentResult[],
  ): Promise<unknown> {
    const slot = [...path, step.service].join('>');
    const segment = await this.callService(step.service, slot, input, record);
    segments.push(segment);

    let value: unknown = segment.output;

    if (step.branch && step.branch.length > 0) {
      const branchResults = await Promise.all(
        step.branch.map(async (child, index) => {
          const childSegments: SegmentResult[] = [];
          const result = await this.runStepInto(
            child,
            value,
            record,
            [...path, `${step.service}[b${index}]`],
            childSegments,
          );
          return { segments: childSegments, value: result };
        }),
      );
      for (const branch of branchResults) segments.push(...branch.segments);
      value = joinValues(step.join, branchResults.map((branch) => branch.value));
    }

    if (step.next) {
      value = await this.runStepInto(step.next, value, record, path, segments);
    }
    return value;
  }

  /**
   * Call one service with retry. Transport failures and version-not-installed
   * errors are retried (a restarting service needs a moment); rule type errors
   * are permanent and bubble up.
   *
   * The retry carries the *same* pinned version and the same slot key, so the
   * service's journal resumes rather than recomputing from scratch.
   */
  private async callService(
    service: ServiceName,
    slot: string,
    input: unknown,
    record: RequestRecord,
  ): Promise<SegmentResult> {
    const url = serviceUrl(this.topology, service);
    const startedAt = Date.now();
    let recovered = false;
    let lastError: { error: string; code?: string; installed?: Version[] } | undefined;

    for (let attempt = 0; attempt < 40; attempt += 1) {
      const res = await jsonRequest<{
        ok: boolean;
        output?: unknown;
        ms?: number;
        version?: Version;
        instance?: string;
        error?: string;
        code?: string;
        installed?: Version[];
      }>(url, {
        path: '/execute',
        body: {
          requestId: record.requestId,
          version: record.version,
          slot,
          service,
          input,
        },
        timeoutMs: 3000,
      });

      if (res.ok && res.body?.ok) {
        this.transport.requests += 1;
        this.transport.requestBytes += res.bytesOut;
        return {
          service,
          instance: res.body.instance ?? service,
          version: res.body.version ?? record.version,
          group: record.group,
          slot,
          input,
          output: res.body.output,
          ms: res.body.ms ?? Date.now() - startedAt,
          recovered,
        };
      }

      const body = res.body as { error?: string; code?: string; installed?: Version[] } | undefined;
      lastError = {
        error: body?.error ?? res.error ?? `status ${res.status}`,
        code: body?.code,
        installed: body?.installed,
      };
      if (body?.code === 'version_not_installed' && body.installed) {
        // Ask the coordinator for the doc and push it directly; the push path
        // is idempotent and keeps the service's journal key stable.
        const installed = await this.ensureServiceVersion(url);
        if (installed) {
          recovered = true;
          continue;
        }
      }
      if (lastError.code === 'wrong_input_type' || lastError.code === 'bad_rule') {
        break;
      }
      recovered = true;
      await sleep(Math.min(40 + attempt * 20, 400));
    }

    throw new RuleError(
      `segment ${service}@${slot} failed after retries: ${lastError?.error ?? 'unknown'}`,
      lastError?.code ?? 'segment_failed',
      { service, slot, installed: lastError?.installed },
      false,
    );
  }

  private async ensureServiceVersion(url: string): Promise<boolean> {
    for (let attempt = 0; attempt < 8; attempt += 1) {
      const info = await this.peers.serviceInfo(url);
      if (info.ok) {
        const missing = [...this.cache.versions()].filter((version) => !info.versions.includes(version));
        if (missing.length === 0) return true;
        for (const version of missing) {
          const doc = this.cache.cached(version);
          if (!doc) continue;
          const meta = this.binding.digests[version];
          // Provenance lets the service attribute the back-fill to this entry,
          // distinct from a coordinator push.
          const pushed = await this.peers.installConfig(url, doc, meta, `entry:${this.name}`);
          if (!pushed.ok) return false;
        }
      }
      await sleep(60);
    }
    return false;
  }

  /* ------------------------------- execution ------------------------------- */

  private versionFor(group: GroupId): Version | undefined {
    return this.binding.versions[group];
  }

  /**
   * Run one request to completion.
   *
   * `entryOverride` re-enters through a different entry after the original
   * entry failed the request; the pinned version travels in the record and is
   * never re-read from bindings.
   */
  private async execute(record: RequestRecord, doc: ConfigDocument): Promise<RequestRecord> {
    const route = doc.routes[record.group];
    if (!route) {
      throw new RuleError(`group ${record.group} has no route in ${record.version}`, 'no_route', {
        group: record.group,
      });
    }
    record.status = 'running';
    record.startedAt = Date.now();
    record.path = routePath(doc, record.group);
    this.ledger.saveRecord(record);
    this.running.set(record.requestId, record);

    try {
      const output = await this.runStep(route, record.input, record);
      record.output = output;
      record.status = 'done';
      record.finishedAt = Date.now();
      record.segments.sort((a, b) => a.slot.localeCompare(b.slot));
      this.ledger.saveRecord(record);
      return record;
    } finally {
      this.running.delete(record.requestId);
    }
  }

  private async fail(record: RequestRecord, err: unknown): Promise<RequestRecord> {
    const ruleError = err instanceof RuleError ? err : undefined;
    record.status = 'failed';
    record.error = err instanceof Error ? err.message : String(err);
    record.errorCode = ruleError?.code ?? 'execution_failed';
    record.finishedAt = Date.now();
    this.ledger.saveRecord(record);
    return record;
  }

  /* --------------------------------- intake -------------------------------- */

  private async accept(group: GroupId, input: unknown): Promise<RequestReceipt | ApiError> {
    const def = this.definition();
    if (!def.groups.includes(group)) {
      return {
        ok: false,
        code: 'group_not_served',
        error: `entry ${this.name} does not serve group ${group}`,
      };
    }

    // Bindings are kept fresh by the background sync loop. If this entry has
    // not observed a binding for the group yet (e.g. it just started), make one
    // blocking attempt so the first request is not spuriously rejected.
    let version = this.versionFor(group);
    if (!version) {
      await this.pullBindings().catch(() => undefined);
      version = this.versionFor(group);
    }
    if (!version) {
      return {
        ok: false,
        code: 'no_binding',
        error: `no effective version for group ${group} (coordinator never reached)`,
      };
    }

    let doc: ConfigDocument;
    try {
      doc = await this.doc(version);
    } catch (err) {
      return {
        ok: false,
        code: 'config_unavailable',
        error: `cannot load config ${version} for group ${group}: ${
          err instanceof Error ? err.message : String(err)
        }`,
      };
    }

    const requestId = newId('req');
    const acceptedAt = Date.now();
    const entry: RequestLedgerEntry = {
      requestId,
      version,
      group,
      acceptedBy: this.instance,
      acceptedAt,
      bindingEpoch: this.binding.epoch,
      path: routePath(doc, group),
      input,
    };
    // Durability before acknowledgement: the pin must survive an entry crash
    // in the microsecond window between reply and first segment.
    this.ledger.putEntry(entry);
    const record: RequestRecord = {
      requestId,
      version,
      group,
      entry: this.name,
      bindingEpoch: this.binding.epoch,
      input,
      segments: [],
      status: 'accepted',
      path: entry.path,
      acceptedBy: this.instance,
      acceptedAt,
      retries: 0,
    };
    this.ledger.saveRecord(record);
    this.inflight += 1;
    const task: Promise<void> = this.execute(record, doc)
      .then(() => undefined)
      .catch(async (err) => {
        await this.fail(record, err);
      })
      .finally(() => {
        this.inflight -= 1;
      });
    this.active.add(task);
    void task.finally(() => this.active.delete(task));

    return {
      ok: true,
      requestId,
      version,
      group,
      acceptedBy: this.instance,
      acceptedAt,
      entry: this.name,
      bindingEpoch: this.binding.epoch,
      retried: false,
    };
  }

  /** Accept synchronously and wait for completion (used by validation). */
  private async acceptAndWait(
    group: GroupId,
    input: unknown,
  ): Promise<{ receipt?: RequestReceipt; record?: RequestRecord; error?: ApiError }> {
    const receipt = await this.accept(group, input);
    if ('ok' in receipt && receipt.ok === false) return { error: receipt };
    const ok = receipt as RequestReceipt;
    const record = await this.awaitRecord(ok.requestId);
    return { receipt: ok, record };
  }

  private async awaitRecord(requestId: string, timeoutMs = 15000): Promise<RequestRecord> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const record = this.ledger.getRecord(requestId);
      if (record && (record.status === 'done' || record.status === 'failed')) return record;
      await sleep(20);
    }
    const record = this.ledger.getRecord(requestId);
    if (record) return record;
    throw new Error(`request ${requestId} never materialized`);
  }

  /* ------------------------------- resume path ----------------------------- */

  /**
   * Resume a pinned request that this entry did not accept (or accepted before
   * a restart). The ledger holds everything required: version, group, input.
   */
  private async resume(requestId: string): Promise<RequestRecord | ApiError> {
    const entry = this.ledger.getEntry(requestId);
    if (!entry) {
      return { ok: false, code: 'unknown_request', error: `request ${requestId} not in ledger` };
    }
    const existing = this.ledger.getRecord(requestId);
    if (existing && existing.status === 'done') return existing;

    let doc: ConfigDocument;
    try {
      doc = await this.doc(entry.version);
    } catch (err) {
      return {
        ok: false,
        code: 'config_unavailable',
        error: `cannot load pinned config ${entry.version}: ${
          err instanceof Error ? err.message : String(err)
        }`,
      };
    }

    const record: RequestRecord =
      existing ?? {
        requestId,
        version: entry.version,
        group: entry.group,
        entry: this.name,
        bindingEpoch: entry.bindingEpoch,
        input: entry.input,
        segments: [],
        status: 'accepted',
        path: entry.path,
        acceptedBy: entry.acceptedBy,
        acceptedAt: entry.acceptedAt,
        retries: 0,
      };
    // The ledger is authoritative for the pin; make sure the record matches
    // even if a stale record was written by another entry.
    record.version = entry.version;
    record.group = entry.group;
    if (record.input === undefined) record.input = entry.input;

    // Reset for a clean re-run: segments are journaled inside the services, so
    // completed units are returned from cache without redoing work.
    record.status = 'accepted';
    record.segments = [];
    record.output = undefined;
    record.error = undefined;
    record.errorCode = undefined;
    record.resumedBy = this.instance;
    record.retries += 1;
    this.ledger.saveRecord(record);

    this.inflight += 1;
    try {
      return await this.execute(record, doc).catch(async (err) => this.fail(record, err));
    } finally {
      this.inflight -= 1;
    }
  }

  /* ---------------------------------- HTTP --------------------------------- */

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
        body: { ok: true, role: 'entry', name: this.name, instance: this.instance },
      };
    }

    if (path === '/admin/bindings' && method === 'GET') {
      const detail = ctx.query.get('detail') === '1';
      return {
        status: 200,
        body: {
          epoch: this.binding.epoch,
          versions: this.binding.versions,
          digests: this.binding.digests,
          pendingEpoch: this.binding.pendingEpoch,
          ...(detail
            ? {
                cachedVersions: this.cache.versions(),
                inflight: this.inflight,
                instance: this.instance,
              }
            : {}),
        },
      };
    }

    // Peer fallback source: serve a config document this entry has durably
    // cached and verified. Execution services use this when their preferred
    // source (the coordinator) is unreachable. Only cached documents are
    // served — an entry never triggers a fresh fetch on a peer's behalf.
    if (path.startsWith('/configs/') && method === 'GET') {
      const version = decodeURIComponent(path.slice('/configs/'.length));
      const doc = this.cache.cached(version);
      if (!doc) {
        return {
          status: 404,
          body: { ok: false, code: 'not_cached', error: `entry ${this.name} has no cached ${version}` },
        };
      }
      return { status: 200, body: doc };
    }

    if (path === '/admin/prepare' && method === 'POST') {
      const epoch = ctx.body?.epoch as number | undefined;
      const versions = ctx.body?.versions as Record<GroupId, Version> | undefined;
      const digests = ctx.body?.digests as Record<Version, string> | undefined;
      if (typeof epoch !== 'number' || !versions) {
        return { status: 400, body: { ok: false, error: 'epoch and versions required' } };
      }
      if (epoch < this.binding.epoch) {
        return {
          status: 409,
          body: {
            ok: false,
            error: `stale epoch ${epoch} < current ${this.binding.epoch}`,
            epoch: this.binding.epoch,
          },
        };
      }
      // Prepare ensures every version this entry may need is locally available,
      // so commit can be a pure in-memory flip even if the coordinator dies.
      for (const version of new Set(Object.values(versions))) {
        if (digests?.[version]) this.cache.setExpectedDigest(version, digests[version]);
        const ok = this.cache.has(version) || (await this.cache.warm(version, 800));
        if (!ok) {
          return {
            status: 409,
            body: { ok: false, error: `config ${version} not available for prepare` },
          };
        }
      }
      this.binding.pendingEpoch = epoch;
      this.staged = { epoch, versions: { ...versions }, digests: { ...(digests ?? {}) } };
      return { status: 200, body: { ok: true, epoch, instance: this.instance } };
    }

    if (path === '/admin/commit' && method === 'POST') {
      const epoch = ctx.body?.epoch as number | undefined;
      if (typeof epoch !== 'number') {
        return { status: 400, body: { ok: false, error: 'epoch required' } };
      }
      if (epoch < this.binding.epoch) {
        return {
          status: 409,
          body: { ok: false, error: `stale epoch ${epoch} < current ${this.binding.epoch}` },
        };
      }
      if (this.staged && this.staged.epoch === epoch) {
        // Normal path: flip exactly the prepared map. Pure in-memory, so it
        // cannot fail for I/O reasons between prepare and commit.
        this.binding.versions = { ...this.staged.versions };
        if (Object.keys(this.staged.digests).length > 0) {
          this.binding.digests = { ...this.staged.digests };
          for (const [version, digest] of Object.entries(this.staged.digests)) {
            this.cache.setExpectedDigest(version, digest);
          }
        }
        this.staged = null;
      } else {
        // Commit without a local prepare (e.g. coordinator recovered and is
        // re-asserting existing bindings): adopt the authoritative map.
        const bindings = await jsonRequest<BindingsView>(coordinatorUrl(this.topology), {
          path: '/admin/bindings',
          timeoutMs: 800,
        });
        if (bindings.ok && bindings.body) {
          this.binding.versions = { ...bindings.body.versions };
          this.binding.digests = { ...(bindings.body.digests ?? {}) };
        } else {
          return { status: 409, body: { ok: false, error: 'no staged bindings and coordinator unreachable' } };
        }
      }
      this.binding.epoch = epoch;
      this.binding.pendingEpoch = null;
      this.persistBindings();
      return { status: 200, body: { ok: true, epoch: this.binding.epoch } };
    }

    if (path === '/accept' && method === 'POST') {
      const group = ctx.body?.group as GroupId | undefined;
      if (!group) {
        return { status: 400, body: { ok: false, code: 'bad_request', error: 'group required' } };
      }
      const receipt = await this.accept(group, ctx.body?.input);
      const status = 'ok' in receipt && receipt.ok === false ? 409 : 200;
      return { status, body: receipt };
    }

    if (path === '/submit' && method === 'POST') {
      const group = ctx.body?.group as GroupId | undefined;
      if (!group) {
        return { status: 400, body: { ok: false, code: 'bad_request', error: 'group required' } };
      }
      const { receipt, record, error } = await this.acceptAndWait(group, ctx.body?.input);
      if (error) return { status: 409, body: error };
      return { status: 200, body: { ok: true, receipt, record } };
    }

    if (path === '/status' && method === 'GET') {
      const inflight = this.inflight;
      const def = this.definition();
      return {
        status: 200,
        body: {
          ok: true,
          name: this.name,
          instance: this.instance,
          epoch: this.binding.epoch,
          groups: def.groups,
          versions: this.binding.versions,
          cachedVersions: this.cache.versions(),
          inflight,
          transport: this.transport,
        },
      };
    }

    if (path === '/admin/inflight' && method === 'GET') {
      return {
        status: 200,
        body: { inflight: this.inflight, accepted: this.running.size, instance: this.instance },
      };
    }

    if (path === '/admin/records' && method === 'GET') {
      const ids = (ctx.query.get('ids') ?? '')
        .split(',')
        .map((id) => id.trim())
        .filter(Boolean);
      const records = ids
        .map((id) => this.ledger.getRecord(id))
        .filter((record): record is RequestRecord => Boolean(record));
      return { status: 200, body: { ok: true, records } };
    }

    if (path.startsWith('/requests/') && method === 'GET') {
      const id = decodeURIComponent(path.slice('/requests/'.length));
      const record = this.ledger.getRecord(id);
      if (!record) {
        return {
          status: 404,
          body: { ok: false, code: 'not_found', error: `unknown request ${id}` },
        };
      }
      return { status: 200, body: { ok: true, record } };
    }

    if (path === '/retry' && method === 'POST') {
      const id = ctx.body?.requestId as string | undefined;
      if (!id) {
        return { status: 400, body: { ok: false, code: 'bad_request', error: 'requestId required' } };
      }
      const result = await this.resume(id);
      if ('ok' in result && result.ok === false) return { status: 409, body: result };
      return { status: 200, body: { ok: true, record: result } };
    }

    if (path === '/admin/transport' && method === 'GET') {
      return {
        status: 200,
        body: {
          ok: true,
          name: this.name,
          transport: {
            ...this.transport,
            configBytes: this.transport.configBytes + this.cache.transport.bytes,
            configFetches: this.transport.configFetches + this.cache.transport.fetches,
            versionsFetched: this.cache.transport.perVersion,
          },
        },
      };
    }

    return { status: 404, body: { ok: false, code: 'not_found', error: `${method} ${path}` } };
  }

  async close(): Promise<void> {
    if (this.syncTimer) clearInterval(this.syncTimer);
    if (this.snapshotTimer) clearInterval(this.snapshotTimer);
    this.publishTransportSnapshot();
    await this.server?.close();
    this.cluster.close();
    this.local.close();
  }

  /** Test hook: wait for all background executions this process started. */
  async drain(timeoutMs = 10000): Promise<boolean> {
    const deadline = Date.now() + timeoutMs;
    while (this.active.size > 0 && Date.now() < deadline) {
      await Promise.race([
        Promise.allSettled([...this.active]),
        sleep(50),
      ]);
    }
    return this.active.size === 0;
  }
}

async function main(): Promise<void> {
  const args = parseArgs();
  const topology = loadTopology(args.topology);
  const name = args.name;
  if (!name) throw new Error('--name is required');
  const gateway = await EntryGateway.start(name, topology, args.dataDir);
  const shutdown = async (): Promise<void> => {
    logEvent(name, 'shutting down');
    await gateway.close();
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown());
  process.on('SIGINT', () => void shutdown());
}

main().catch(fatal);
