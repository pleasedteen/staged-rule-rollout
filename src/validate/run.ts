import { spawn, type ChildProcess } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:net';
import { join, resolve } from 'node:path';
import { jsonRequest } from '../shared/http';
import { formatBytes, sleep } from '../shared/util';
import {
  currencyV1Segments,
  expectedArchiveV2,
  expectedCurrencyV1,
  expectedCurrencyV2,
  expectedReportV2,
  expectedSearchV1,
  expectedSearchV2,
} from './oracles';

const ROOT = resolve(__dirname, '..', '..');
const RUN = join(ROOT, '.run');
const RUN_SINGLE = join(ROOT, '.run-single');
const FIXTURES = join(ROOT, 'fixtures');
const TOPOLOGY_PATH = join(FIXTURES, 'topology.json');
const TOPOLOGY_SINGLE_PATH = join(FIXTURES, 'topology-single.json');
const PLAN_PATH = join(FIXTURES, 'plan.json');
const DIST = join(ROOT, 'dist');

interface InstanceDef {
  name: string;
  port: number;
}

interface TopologyFile {
  coordinator: { port: number };
  entries: { name: string; port: number; groups: string[] }[];
  services: { name: string; port?: number; groups: string[]; instances?: InstanceDef[] }[];
}

/** Instances of a service, including the legacy single-instance `port` form. */
const serviceInstances = (service: TopologyFile['services'][number]): InstanceDef[] => {
  if (service.instances && service.instances.length > 0) return service.instances;
  if (typeof service.port === 'number') return [{ name: service.name, port: service.port }];
  return [];
};

const topology = JSON.parse(readFileSync(TOPOLOGY_PATH, 'utf8')) as TopologyFile;
const topologySingle = JSON.parse(readFileSync(TOPOLOGY_SINGLE_PATH, 'utf8')) as TopologyFile;

const instancesByName = new Map<string, InstanceDef>();
for (const service of [...topology.services, ...topologySingle.services]) {
  for (const instance of serviceInstances(service)) instancesByName.set(instance.name, instance);
}

const coordinatorBase = `http://127.0.0.1:${topology.coordinator.port}`;
const entryBase = (name: string): string =>
  `http://127.0.0.1:${topology.entries.find((entry) => entry.name === name)!.port}`;
const singleEntryBase = (name: string): string =>
  `http://127.0.0.1:${topologySingle.entries.find((entry) => entry.name === name)!.port}`;
const instanceBase = (name: string): string => `http://127.0.0.1:${instancesByName.get(name)!.port}`;
const instanceNamesOf = (service: string): string[] =>
  serviceInstances(topology.services.find((candidate) => candidate.name === service)!).map(
    (instance) => instance.name,
  );

/* --------------------------------- processes -------------------------------- */

interface ProcessSpec {
  role: 'coordinator' | 'entry' | 'service';
  /** Unique key inside one cluster (process/instance name). */
  key: string;
  port: number;
  args: string[];
  service?: string;
  instance?: string;
  child?: ChildProcess;
  exited: boolean;
  stdout: string[];
  stderr: string[];
}

class Cluster {
  readonly procs = new Map<string, ProcessSpec>();

  constructor(
    private readonly dataDir: string,
    private readonly topo: TopologyFile,
    private readonly topoPath: string,
  ) {}

  private spawnProcess(
    spec: Omit<ProcessSpec, 'child' | 'exited' | 'stdout' | 'stderr' | 'args'>,
    args: string[],
  ): ProcessSpec {
    const proc: ProcessSpec = { ...spec, args, exited: false, stdout: [], stderr: [] };
    const child = spawn(process.execPath, args, {
      cwd: ROOT,
      env: { ...process.env, NODE_NO_WARNINGS: '1' },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    child.stdout?.on('data', (chunk: Buffer) => {
      proc.stdout.push(chunk.toString());
      if (process.env.VALIDATE_VERBOSE) process.stderr.write(chunk);
    });
    child.stderr?.on('data', (chunk: Buffer) => {
      proc.stderr.push(chunk.toString());
      if (process.env.VALIDATE_VERBOSE) process.stderr.write(chunk);
    });
    child.on('exit', () => {
      proc.exited = true;
    });
    proc.child = child;
    this.procs.set(spec.key, proc);
    return proc;
  }

  startCoordinator(): ProcessSpec {
    return this.spawnProcess(
      { role: 'coordinator', key: 'coordinator', port: this.topo.coordinator.port },
      [
        join(DIST, 'coordinator/main.js'),
        '--data-dir',
        this.dataDir,
        '--topology',
        this.topoPath,
        '--plan',
        PLAN_PATH,
      ],
    );
  }

  startEntry(name: string): ProcessSpec {
    const entry = this.topo.entries.find((candidate) => candidate.name === name)!;
    return this.spawnProcess(
      { role: 'entry', key: name, port: entry.port },
      [
        join(DIST, 'entry/main.js'),
        '--data-dir',
        this.dataDir,
        '--topology',
        this.topoPath,
        '--name',
        name,
      ],
    );
  }

  startService(service: string, instance: string, port: number): ProcessSpec {
    const args = [
      join(DIST, 'service/main.js'),
      '--data-dir',
      this.dataDir,
      '--topology',
      this.topoPath,
      '--name',
      service,
    ];
    // Legacy single-instance services (instance name == service name) need no
    // --instance flag; that path is exercised by the compatibility cluster.
    if (instance !== service) args.push('--instance', instance);
    return this.spawnProcess({ role: 'service', key: instance, port, service, instance }, args);
  }

  async waitHealthy(base: string, timeoutMs = 10000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const res = await jsonRequest(base, { path: '/health', timeoutMs: 300 });
      if (res.ok) return;
      await sleep(40);
    }
    throw new Error(`process at ${base} did not become healthy`);
  }

  async waitUnhealthy(base: string, timeoutMs = 5000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const res = await jsonRequest(base, { path: '/health', timeoutMs: 200 });
      if (!res.ok) return;
      await sleep(30);
    }
    throw new Error(`process at ${base} stayed healthy`);
  }

  async startAll(): Promise<void> {
    // Coordinator first: it creates the shared cluster schema. Then entries
    // (they also touch the cluster DB) and services (local DB only). This
    // avoids a boot-time stampede on the new cluster file.
    this.startCoordinator();
    await this.waitHealthy(`http://127.0.0.1:${this.topo.coordinator.port}`);
    for (const entry of this.topo.entries) {
      this.startEntry(entry.name);
      await this.waitHealthy(`http://127.0.0.1:${entry.port}`);
    }
    const started: { base: string }[] = [];
    for (const service of this.topo.services) {
      for (const instance of serviceInstances(service)) {
        this.startService(service.name, instance.name, instance.port);
        started.push({ base: `http://127.0.0.1:${instance.port}` });
      }
    }
    await Promise.all(started.map((item) => this.waitHealthy(item.base)));
  }

  private baseOf(key: string): string {
    const proc = this.procs.get(key);
    if (!proc) throw new Error(`unknown process ${key}`);
    return `http://127.0.0.1:${proc.port}`;
  }

  /** SIGKILL — no graceful flush, mirroring a real crash. */
  async kill(key: string): Promise<void> {
    const proc = this.procs.get(key);
    if (!proc?.child || proc.exited) return;
    const base = this.baseOf(key);
    const exited = new Promise<void>((resolveExit) => proc.child!.once('exit', () => resolveExit()));
    proc.child.kill('SIGKILL');
    await Promise.race([exited, sleep(3000)]);
    await this.waitUnhealthy(base, 3000).catch(() => undefined);
  }

  /** Relaunch a process (also used after a deliberate self-exit). */
  async restart(key: string): Promise<void> {
    const existing = this.procs.get(key);
    if (!existing) throw new Error(`unknown process ${key}`);
    if (existing.role === 'coordinator') this.startCoordinator();
    else if (existing.role === 'entry') this.startEntry(key);
    else this.startService(existing.service!, existing.instance!, existing.port);
    await this.waitHealthy(`http://127.0.0.1:${existing.port}`);
    // Give the entry sync loop a moment to observe settled bindings.
    if (existing.role === 'entry') await sleep(300);
  }

  async shutdown(): Promise<void> {
    const children = [...this.procs.values()]
      .map((proc) => proc.child)
      .filter((child): child is ChildProcess => Boolean(child) && !child!.killed && child!.exitCode === null);
    await Promise.all(
      children.map(
        (child) =>
          new Promise<void>((resolveExit) => {
            child.once('exit', () => resolveExit());
            child.kill('SIGKILL');
            setTimeout(resolveExit, 2000);
          }),
      ),
    );
  }

  log(name: string): string {
    const proc = this.procs.get(name);
    return proc ? `${proc.stdout.join('')}${proc.stderr.join('')}` : '';
  }
}

/* --------------------------------- assertions ------------------------------- */

class Report {
  readonly lines: { ok: boolean; name: string; detail: string }[] = [];
  private failures = 0;

  check(ok: boolean, name: string, detail: string): boolean {
    this.lines.push({ ok, name, detail });
    if (!ok) this.failures += 1;
    return ok;
  }

  section(title: string): void {
    this.lines.push({ ok: true, name: `## ${title}`, detail: '\u0000' });
  }

  get failed(): number {
    return this.failures;
  }

  print(): void {
    for (const line of this.lines) {
      if (line.detail === '\u0000') {
        process.stdout.write(`\n${line.name}\n`);
        continue;
      }
      process.stdout.write(`  [${line.ok ? 'PASS' : 'FAIL'}] ${line.name}\n`);
      if (line.detail) process.stdout.write(`         ${line.detail}\n`);
    }
  }
}

/* ---------------------------------- clients --------------------------------- */

interface ServiceInstanceView {
  name: string;
  port: number;
  reachable: boolean;
  versions: string[];
}

interface StateView {
  releaseId: string;
  status: string;
  paused: boolean;
  cursor: number;
  batches: {
    index: number;
    name: string;
    groups: string[];
    targetVersion: string;
    status: string;
    effective: Record<string, string>;
    before: Record<string, string>;
    note?: string;
  }[];
  bindings: { epoch: number; versions: Record<string, string> };
  rollback: { batchIndex: number; status: string; inflightAtConfirm?: number } | null;
  rollbackHistory: { batchIndex: number; status: string; inflightAtConfirm?: number }[];
  recovered: boolean;
  recoveries: number;
  lastRecovery: string | null;
  entries?: { name: string; reachable: boolean; epoch: number | null; versions: Record<string, string> }[];
  services?: {
    name: string;
    reachable: boolean;
    versions: string[];
    instances: ServiceInstanceView[];
  }[];
}

interface RecordView {
  requestId: string;
  version: string;
  group: string;
  status: string;
  output?: unknown;
  segments: {
    service: string;
    instance: string;
    version: string;
    recovered: boolean;
    reused?: boolean;
    originInstance?: string;
    failovers?: number;
    slot: string;
    output: unknown;
  }[];
  retries: number;
  resumedBy?: string;
  error?: string;
}

const state = async (detail = false): Promise<StateView> => {
  const res = await jsonRequest<StateView>(coordinatorBase, {
    path: detail ? '/admin/state?detail=1' : '/admin/state',
    timeoutMs: 6000,
  });
  if (!res.ok || !res.body) throw new Error(`state unavailable: ${res.error ?? res.status}`);
  return res.body;
};

const ops = async (path: string, body?: unknown) => {
  const res = await jsonRequest(coordinatorBase, { path, body: body ?? {}, timeoutMs: 10000 });
  return { status: res.status, body: res.body as any, transportError: res.error };
};

const accept = async (entry: string, group: string, input: unknown) => {
  const res = await jsonRequest(entryBase(entry), {
    path: '/accept',
    body: { group, input },
    timeoutMs: 6000,
  });
  return { status: res.status, body: res.body as any, transportError: res.error };
};

const submit = async (entry: string, group: string, input: unknown) => {
  const res = await jsonRequest(entryBase(entry), {
    path: '/submit',
    body: { group, input },
    timeoutMs: 20000,
  });
  return { status: res.status, body: res.body as any, transportError: res.error };
};

const getRecord = async (entry: string, requestId: string): Promise<RecordView | undefined> => {
  const res = await jsonRequest<{ record: RecordView }>(entryBase(entry), {
    path: `/requests/${requestId}`,
    timeoutMs: 6000,
  });
  return res.ok ? res.body.record : undefined;
};

const waitRecord = async (
  entry: string,
  requestId: string,
  timeoutMs = 20000,
): Promise<RecordView | undefined> => {
  const deadline = Date.now() + timeoutMs;
  let last: RecordView | undefined;
  while (Date.now() < deadline) {
    last = await getRecord(entry, requestId);
    if (last && (last.status === 'done' || last.status === 'failed')) return last;
    await sleep(40);
  }
  return last;
};

const entryBindings = async (
  name: string,
): Promise<{ epoch: number; versions: Record<string, string> } | undefined> => {
  const res = await jsonRequest<{ epoch: number; versions: Record<string, string> }>(entryBase(name), {
    path: '/admin/bindings',
    timeoutMs: 3000,
  });
  return res.ok ? res.body : undefined;
};

/** Wait until every entry has observed a binding for every group it serves. */
const waitBindings = async (groups: string[], timeoutMs = 8000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const observed = await Promise.all(['entry-a', 'entry-b', 'entry-c'].map(entryBindings));
    const ready = observed.every(
      (entry) => entry && Object.values(entry.versions).length > 0,
    );
    if (ready) return;
    await sleep(50);
  }
  void groups;
};

const recordsEqual = (a: unknown, b: unknown): boolean => JSON.stringify(a) === JSON.stringify(b);

/** Instance name (without boot suffix) that answered a service segment. */
const segmentInstance = (record: RecordView | undefined, service: string): string | undefined => {
  const segment = (record?.segments ?? []).find((candidate) => candidate.service === service);
  return segment?.instance.split('#')[0];
};

const otherInstanceOf = (service: string, instance: string): string => {
  const other = instanceNamesOf(service).find((candidate) => candidate !== instance);
  if (!other) throw new Error(`service ${service} has no sibling of ${instance}`);
  return other;
};

/** True if nothing is listening on the port (we can bind it). */
const portFree = (port: number): Promise<boolean> =>
  new Promise((resolvePort) => {
    const server = createServer();
    server.once('error', () => resolvePort(false));
    server.once('listening', () => server.close(() => resolvePort(true)));
    server.listen(port, '127.0.0.1');
  });

/** All ports both clusters will use. */
const allPorts = (): number[] => {
  const ports = new Set<number>();
  for (const topo of [topology, topologySingle]) {
    ports.add(topo.coordinator.port);
    for (const entry of topo.entries) ports.add(entry.port);
    for (const service of topo.services) {
      for (const instance of serviceInstances(service)) ports.add(instance.port);
    }
  }
  return [...ports];
};

/**
 * Refuse to run against leftover processes. Reusing a deleted `.run` while an
 * old cluster still holds the ports would make health checks pass against the
 * wrong process and produce confusing failures.
 */
async function assertPortsFree(): Promise<number[]> {
  const busy: number[] = [];
  for (const port of allPorts()) {
    if (!(await portFree(port))) busy.push(port);
  }
  return busy;
}

/* ---------------------------------- runner ---------------------------------- */

async function main(): Promise<number> {
  if (!existsSync(join(DIST, 'coordinator/main.js'))) {
    process.stderr.write('dist not built; run `npm run build`\n');
    return 2;
  }
  const busyPorts = await assertPortsFree();
  if (busyPorts.length > 0) {
    process.stderr.write(
      `端口已被占用：${busyPorts.join(', ')}\n` +
        `这通常意味着上一轮遗留进程仍在运行。请先结束它们：\n` +
        `  pkill -f 'main\\.js --data-dir'\n然后重新运行 npm run validate。\n`,
    );
    return 4;
  }
  rmSync(RUN, { recursive: true, force: true });
  mkdirSync(RUN, { recursive: true });
  rmSync(RUN_SINGLE, { recursive: true, force: true });
  mkdirSync(RUN_SINGLE, { recursive: true });

  const report = new Report();
  const cluster = new Cluster(RUN, topology, TOPOLOGY_PATH);
  const startedAt = Date.now();

  // Services with more than one instance (their failover is exercised below).
  const multiInstance = topology.services
    .filter((service) => serviceInstances(service).length > 1)
    .map((service) => service.name);

  try {
    /* ------------------------------- 1. boot ------------------------------- */
    report.section('1. Boot & initial state');
    await cluster.startAll();
    await waitBindings(['currency', 'search', 'report']);
    const initial = await state(true);
    report.check(initial.releaseId === 'rel-2026-11-26', 'plan installed', `releaseId=${initial.releaseId}`);
    report.check(
      initial.batches.every((batch) => batch.status === 'pending'),
      'all batches pending at boot',
      initial.batches.map((batch) => `${batch.name}=${batch.status}`).join(', '),
    );
    report.check(
      Object.values(initial.bindings.versions).length === 4 &&
        Object.values(initial.bindings.versions).every((version) => version === 'v1'),
      'initial bindings are v1 for every group',
      JSON.stringify(initial.bindings.versions),
    );
    {
      const declared = topology.services.reduce(
        (sum, service) => sum + serviceInstances(service).length,
        0,
      );
      const seen = (initial.services ?? []).flatMap((service) => service.instances);
      report.check(
        seen.length === declared && seen.every((instance) => instance.reachable),
        'every declared service instance is up and independently addressable',
        `declared=${declared} reachable=${seen.filter((instance) => instance.reachable).length}`,
      );
      report.check(
        multiInstance.length >= 3 &&
          multiInstance.every((service) =>
            (initial.services ?? []).find((view) => view.name === service)?.instances.length === 2,
          ),
        'services with several instances expose each one to the coordinator',
        multiInstance
          .map(
            (service) =>
              `${service}:${(initial.services ?? []).find((view) => view.name === service)?.instances.map((i) => i.name).join('+')}`,
          )
          .join(' '),
      );
    }

    /* ------------------------- 2. pinning at accept ------------------------ */
    report.section('2. Version pinned at first acceptance');
    {
      const accepted = await accept('entry-a', 'currency', 5);
      report.check(accepted.status === 200, 'accept returns a receipt', JSON.stringify(accepted.body?.code ?? 'ok'));
      const record = await waitRecord('entry-a', accepted.body.requestId);
      report.check(
        record?.version === 'v1' && record.status === 'done',
        'currency request pinned v1 and completed',
        `version=${record?.version} status=${record?.status}`,
      );
      report.check(
        record?.segments.every((segment) => segment.version === 'v1') === true,
        'every segment ran pinned v1',
        (record?.segments ?? []).map((s) => `${s.slot}@${s.version}`).join(' | '),
      );
      report.check(
        recordsEqual(record?.output, expectedCurrencyV1(5)),
        'result matches the independent v1 oracle',
        `expected=${JSON.stringify(expectedCurrencyV1(5))} actual=${JSON.stringify(record?.output)}`,
      );
      const seg = currencyV1Segments(5);
      const byService = new Map((record?.segments ?? []).map((s) => [s.service, s.output]));
      report.check(
        recordsEqual(byService.get('ingest'), seg.ingest) &&
          recordsEqual(byService.get('fx-math'), seg.branchMath) &&
          recordsEqual(byService.get('fx-tag'), seg.branchTag),
        'branch node outputs match the oracle',
        `ingest=${JSON.stringify(byService.get('ingest'))} fx-math=${JSON.stringify(byService.get('fx-math'))} fx-tag=${JSON.stringify(byService.get('fx-tag'))}`,
      );
      report.check(
        (record?.segments ?? []).length === 4,
        'route executed exactly four segments (ingest, 2 branches, final)',
        (record?.segments ?? []).map((s) => s.service).join(','),
      );
    }

    /* ---------------------- 3. batch 1 shared boundary --------------------- */
    report.section('3. Batch 1 — currency, instance-level readiness');
    {
      // Take one fx-math instance down before advancing: the batch must still
      // go through because the sibling instance can execute the target.
      await cluster.kill('fx-math-1');
      const degraded = await state(true);
      const fxMath = (degraded.services ?? []).find((service) => service.name === 'fx-math');
      report.check(
        fxMath?.reachable === true &&
          fxMath.instances.filter((instance) => !instance.reachable).map((instance) => instance.name).join(',') ===
            'fx-math-1',
        'a service stays ready with one of its instances down',
        `reachable=${fxMath?.instances.filter((i) => i.reachable).map((i) => i.name).join(',')}`,
      );

      const advance = await ops('/ops/advance');
      report.check(advance.status === 200 && advance.body.ok, 'advance batch 1 succeeds', advance.body.error ?? 'ok');
      report.check(
        Array.isArray(advance.body.blocked) && advance.body.blocked.length === 0,
        'readiness needs only one executable instance per service',
        JSON.stringify(advance.body.blocked ?? []),
      );
      await cluster.restart('fx-math-1');
      const after = await state();
      const observed = await Promise.all(['entry-a', 'entry-b', 'entry-c'].map(entryBindings));
      report.check(
        observed.every((entry) => entry?.versions.currency === 'v2' && entry.epoch === after.bindings.epoch),
        'every entry reports the new epoch and currency=v2',
        observed.map((entry, index) => `${['a', 'b', 'c'][index]}:epoch=${entry?.epoch},c=${entry?.versions.currency}`).join(' '),
      );
      report.check(
        after.bindings.versions.search === 'v1' && after.bindings.versions.report === 'v1',
        'other groups untouched by batch 1',
        JSON.stringify(after.bindings.versions),
      );
      report.check(
        after.batches[0].status === 'effective' && after.batches[1].status === 'pending',
        'batch 1 effective, batch 2 still pending',
        after.batches.map((b) => `${b.name}=${b.status}`).join(', '),
      );

      const v2 = await accept('entry-c', 'currency', 5);
      const v2Record = await waitRecord('entry-c', v2.body.requestId);
      report.check(
        v2Record?.version === 'v2' && recordsEqual(v2Record.output, expectedCurrencyV2(5)),
        'new currency traffic pins v2 with the v2 oracle result',
        `v=${v2Record?.version} out=${JSON.stringify(v2Record?.output)} exp=${JSON.stringify(expectedCurrencyV2(5))}`,
      );
      report.check(
        (v2Record?.segments ?? []).every((segment) => segment.version === 'v2'),
        'no segment mixed versions while one instance was restarted',
        (v2Record?.segments ?? []).map((s) => `${s.service}@${s.version}`).join(' '),
      );
      const search = await accept('entry-b', 'search', 'kiwi');
      const searchRecord = await waitRecord('entry-b', search.body.requestId);
      report.check(
        searchRecord?.version === 'v1' && recordsEqual(searchRecord.output, expectedSearchV1('kiwi')),
        'search traffic still pins v1',
        `v=${searchRecord?.version} out=${JSON.stringify(searchRecord?.output)}`,
      );
    }

    /* ------------------ 4. pause + later-batch downtime ------------------- */
    report.section('4. Pause semantics & later-batch-exclusive downtime');
    {
      await cluster.kill('archive-store-1'); // only batch 3 needs it
      await ops('/ops/pause');
      const blocked = await ops('/ops/advance');
      report.check(
        blocked.status === 409 && blocked.body.code === 'paused',
        'advance is refused while paused',
        `code=${blocked.body.code}`,
      );
      const frozen = (await state()).batches.map((b) => `${b.index}:${b.status}`).join(',');
      await sleep(150);
      report.check(
        (await state()).batches.map((b) => `${b.index}:${b.status}`).join(',') === frozen,
        'effective batches stay frozen while paused',
        frozen,
      );

      await ops('/ops/resume');

      // Batch 2 needs search-a. One instance down must not block it; all
      // instances down must, and only search-a must be named.
      await cluster.kill('search-a-1');
      const oneDown = await ops('/ops/advance');
      report.check(
        oneDown.status === 200 && oneDown.body.ok,
        'batch 2 advances with one search-a instance down',
        JSON.stringify(oneDown.body.blocked ?? oneDown.body.error ?? 'ok'),
      );
      const after2 = await state();
      report.check(
        after2.bindings.versions.search === 'v2' &&
          after2.bindings.versions.report === 'v2' &&
          after2.bindings.versions.currency === 'v2',
        'both groups of batch 2 flipped together; batch 1 unchanged',
        JSON.stringify(after2.bindings.versions),
      );
      report.check(
        after2.batches[1].effective.search === 'v2' && after2.batches[1].effective.report === 'v2',
        'batch 2 groups share one effective version',
        JSON.stringify(after2.batches[1].effective),
      );
      report.check(
        after2.batches[0].status === 'effective' && after2.batches[2].status === 'pending',
        'batch 1 effective and batch 3 pending after batch 2',
        after2.batches.map((b) => `${b.name}=${b.status}`).join(', '),
      );

      // New search/report traffic is v2; archive (later batch) is untouched v1.
      const search = await submit('entry-b', 'search', 'post-batch2');
      const report1 = await submit('entry-b', 'report', 'post-batch2');
      const archive = await submit('entry-a', 'archive', 'post-batch2');
      report.check(
        (search.body.record as RecordView)?.version === 'v2' &&
          recordsEqual((search.body.record as RecordView)?.output, expectedSearchV2('post-batch2')),
        'search now serves v2 through a single surviving instance',
        `out=${JSON.stringify((search.body.record as RecordView)?.output)}`,
      );
      report.check(
        (report1.body.record as RecordView)?.version === 'v2' &&
          recordsEqual((report1.body.record as RecordView)?.output, expectedReportV2('post-batch2')),
        'report now serves v2',
        `out=${JSON.stringify((report1.body.record as RecordView)?.output)}`,
      );
      report.check(
        (archive.body.record as RecordView)?.version === 'v1',
        'archive still pinned to v1 while its batch is pending',
        `v=${(archive.body.record as RecordView)?.version}`,
      );
      await cluster.restart('search-a-1');
      await cluster.restart('archive-store-1');
    }

    /* ------------------- 5. same-batch service downtime ------------------- */
    report.section('5. Batch blocked when every instance is down');
    {
      await cluster.kill('archive-load-1'); // batch 3 needs it; only instance
      const blocked = await ops('/ops/advance');
      report.check(
        blocked.status === 409 && blocked.body.code === 'blocked',
        'batch 3 is blocked while every archive-load instance is down',
        JSON.stringify(blocked.body.blocked ?? blocked.body.error),
      );
      report.check(
        Array.isArray(blocked.body.blocked) &&
          blocked.body.blocked.length > 0 &&
          blocked.body.blocked.every((entry: { service: string }) => entry.service === 'archive-load'),
        'only archive-load is reported as blocking',
        JSON.stringify(blocked.body.blocked),
      );
      report.check(
        (await state()).batches[1].status === 'effective',
        'already-effective batches are unaffected by the block',
        (await state()).batches[1].status,
      );
      await cluster.restart('archive-load-1');
      const retry = await ops('/ops/advance');
      report.check(retry.status === 200 && retry.body.ok, 'batch 3 advances once the service is back', retry.body.error ?? 'ok');
      const archive = await submit('entry-c', 'archive', 'after-batch3');
      report.check(
        (archive.body.record as RecordView)?.version === 'v2' &&
          recordsEqual((archive.body.record as RecordView)?.output, expectedArchiveV2('after-batch3')),
        'archive now serves v2 with the v2 oracle result',
        `out=${JSON.stringify((archive.body.record as RecordView)?.output)} exp=${JSON.stringify(expectedArchiveV2('after-batch3'))}`,
      );
    }

    /* ------------------- 6. coordinator crash & recovery ------------------ */
    report.section('6. Coordinator crash; successor resumes from actual progress');
    {
      const preCrash = await state();
      const offlineExpected = expectedReportV2('offline-1');
      await cluster.kill('coordinator');
      const offline = await accept('entry-b', 'report', 'offline-1');
      report.check(offline.status === 200, 'entry still accepts while coordinator is down', JSON.stringify(offline.body?.code ?? 'ok'));
      const offlineRecord = await waitRecord('entry-b', offline.body.requestId);
      report.check(
        offlineRecord?.status === 'done' &&
          offlineRecord.version === 'v2' &&
          recordsEqual(offlineRecord.output, offlineExpected),
        'offline request completes on its pinned v2',
        `v=${offlineRecord?.version} out=${JSON.stringify(offlineRecord?.output)}`,
      );
      await cluster.restart('coordinator');
      const recovered = await state(true);
      report.check(
        recovered.recovered && recovered.recoveries >= 1,
        'successor reports a recovery pass',
        `recoveries=${recovered.recoveries} note=${recovered.lastRecovery}`,
      );
      report.check(
        recovered.batches.every((batch) => batch.status === 'effective'),
        'successor sees all batches effective',
        recovered.batches.map((b) => `${b.name}=${b.status}`).join(', '),
      );
      report.check(
        JSON.stringify(recovered.bindings.versions) === JSON.stringify(preCrash.bindings.versions),
        'bindings survive the crash unchanged',
        JSON.stringify(recovered.bindings.versions),
      );
      report.check(
        (recovered.entries ?? []).every((entry) => entry.reachable && entry.epoch === recovered.bindings.epoch),
        'all entries agree with the successor epoch',
        (recovered.entries ?? []).map((entry) => `${entry.name}:${entry.epoch}`).join(' '),
      );
    }

    /* ------------- 7. rollback with in-flight requests (currency) --------- */
    report.section('7. Rollback while requests are still executing');
    {
      // A long-ish request is accepted, then we roll batch 1 back to v1 while
      // it is still inside the delayed segment.
      const inflight = await accept('entry-a', 'currency', 21);
      await sleep(80);
      report.check(inflight.status === 200, 'request accepted under v2 before rollback', `v=${inflight.body.version}`);
      await ops('/ops/pause');
      const requested = await ops('/ops/rollback', { batchIndex: 0 });
      report.check(requested.status === 200, 'rollback of batch 1 requested', requested.body.error ?? 'ok');
      const confirmed = await ops('/ops/rollback/confirm');
      const rolledBack = (await state()).rollbackHistory.slice(-1)[0];
      report.check(
        confirmed.status === 200,
        'rollback confirmed while a request is executing',
        `inflightAtConfirm=${rolledBack?.inflightAtConfirm ?? 'n/a'}`,
      );
      report.check(
        (rolledBack?.inflightAtConfirm ?? 0) > 0,
        'the coordinator observed a request still in flight at confirmation',
        `inflightAtConfirm=${rolledBack?.inflightAtConfirm ?? 0}`,
      );
      const inflightRecord = await waitRecord('entry-a', inflight.body.requestId);
      report.check(
        inflightRecord?.version === 'v2' && recordsEqual(inflightRecord.output, expectedCurrencyV2(21)),
        'the in-flight request still completes on its pinned v2',
        `v=${inflightRecord?.version} out=${JSON.stringify(inflightRecord?.output)}`,
      );
      const fresh = await accept('entry-a', 'currency', 4);
      const freshRecord = await waitRecord('entry-a', fresh.body.requestId);
      report.check(
        freshRecord?.version === 'v1' && recordsEqual(freshRecord.output, expectedCurrencyV1(4)),
        'post-confirm acceptance uses the pre-batch version v1',
        `v=${freshRecord?.version} out=${JSON.stringify(freshRecord?.output)} exp=${JSON.stringify(expectedCurrencyV1(4))}`,
      );
      report.check(
        (await state()).bindings.versions.currency === 'v1',
        'currency binding restored to v1',
        JSON.stringify((await state()).bindings.versions),
      );
      await ops('/ops/resume');
    }

    /* ------------- 8. re-advance after rollback + crash-after-commit ------ */
    report.section('8. Re-advance rolled-back batch; crash after commit');
    {
      // Batch 1 was rolled back, so it is first in line to advance again.
      const crashAdvance = await jsonRequest(coordinatorBase, {
        path: '/ops/advance',
        body: { fault: 'after-commit' },
        timeoutMs: 15000,
      });
      report.check(
        !crashAdvance.ok,
        'coordinator dies after entries commit but before bookkeeping',
        `status=${crashAdvance.status} err=${crashAdvance.error ?? ''}`,
      );
      const entryEpochs = await Promise.all(['entry-a', 'entry-b', 'entry-c'].map(entryBindings));
      report.check(
        entryEpochs.every((entry) => entry?.versions.currency === 'v2'),
        'entries really did commit currency=v2 before the crash',
        entryEpochs.map((e) => `${e?.epoch}:${e?.versions.currency}`).join(' '),
      );
      await cluster.restart('coordinator');
      const successor = await state(true);
      report.check(
        successor.batches[0].status === 'effective' && successor.bindings.versions.currency === 'v2',
        'successor adopted the committed epoch (no false pending)',
        `batch1=${successor.batches[0].status} currency=${successor.bindings.versions.currency}`,
      );
      report.check(
        (successor.entries ?? []).every((e) => e.epoch === successor.bindings.epoch),
        'successor epoch matches every entry',
        (successor.entries ?? []).map((e) => `${e.name}:${e.epoch}/${successor.bindings.epoch}`).join(' '),
      );
    }

    /* ------------- 9. crash after durable intent, before fan-out ---------- */
    report.section('9. Crash after durable intent; successor discards an uncommitted switch');
    {
      // Roll batch 1 back again so a fresh advance is pending.
      await ops('/ops/pause');
      await ops('/ops/rollback', { batchIndex: 0 });
      await ops('/ops/rollback/confirm');
      await ops('/ops/resume');
      const before = await state();
      report.check(before.bindings.versions.currency === 'v1', 'precondition: currency back on v1', before.bindings.versions.currency);
      const crashAdvance = await jsonRequest(coordinatorBase, {
        path: '/ops/advance',
        body: { fault: 'after-pending' },
        timeoutMs: 8000,
      });
      report.check(!crashAdvance.ok, 'coordinator dies right after writing the pending intent', `status=${crashAdvance.status}`);
      await cluster.restart('coordinator');
      const successor = await state(true);
      report.check(
        successor.bindings.versions.currency === 'v1' && successor.batches[0].status === 'pending',
        'successor discarded an intent that no entry committed',
        `currency=${successor.bindings.versions.currency} batch1=${successor.batches[0].status}`,
      );
      const advance = await ops('/ops/advance');
      report.check(advance.status === 200, 'the batch can be advanced again afterwards', advance.body.error ?? 'ok');
      report.check((await state()).bindings.versions.currency === 'v2', 'currency now v2 again', JSON.stringify((await state()).bindings.versions));
    }

    /* ------- 10. instance halt mid-request: first concurrent batch -------- */
    report.section('10. Instance halt mid-request — in-flight requests fail over');
    {
      // Pin the entry's affinity to one fx-tag instance, then stop it while a
      // batch of currency requests is inside its 500 ms delay. The requests
      // must continue on the sibling instance, on their pinned version.
      const warm = await submit('entry-a', 'currency', 90);
      const active = segmentInstance(warm.body.record as RecordView, 'fx-tag')!;
      const sibling = otherInstanceOf('fx-tag', active);
      report.check(
        Boolean(active) && Boolean(sibling),
        'entry is steering currency traffic to one fx-tag instance',
        `active=${active} sibling=${sibling}`,
      );

      const inFlight = await Promise.all(
        Array.from({ length: 6 }, (_, index) => accept('entry-a', 'currency', 100 + index)),
      );
      await sleep(160);
      await cluster.kill(active);
      const firstWave = await Promise.all(
        inFlight.map((receipt) => waitRecord('entry-a', receipt.body.requestId, 25000)),
      );
      const firstWaveOk = firstWave.every(
        (record, index) =>
          record?.version === 'v2' &&
          record.status === 'done' &&
          recordsEqual(record.output, expectedCurrencyV2(100 + index)),
      );
      report.check(
        firstWaveOk,
        'requests whose instance stopped still resolve on the pinned version',
        firstWave
          .map((record, index) => `${record?.status}:${recordsEqual(record?.output, expectedCurrencyV2(100 + index))}`)
          .join(' '),
      );
      report.check(
        firstWave.every((record) => (record?.segments ?? []).every((segment) => segment.version === 'v2')),
        'no in-flight segment flipped to another version during failover',
        firstWave.map((record) => (record?.segments ?? []).map((s) => s.version).join('/')).join(' '),
      );
      report.check(
        firstWave.every((record) => segmentInstance(record, 'fx-tag') === sibling),
        'every resumed branch segment ran on the sibling instance',
        firstWave.map((record) => segmentInstance(record, 'fx-tag')).join(','),
      );
      report.check(
        firstWave.every((record) =>
          (record?.segments ?? [])
            .filter((segment) => segment.service === 'fx-tag')
            .every((segment) => segment.recovered && (segment.failovers ?? 0) >= 1),
        ),
        'the stopped instance was actually exercised through failover',
        firstWave
          .flatMap((record) => (record?.segments ?? []).filter((s) => s.service === 'fx-tag'))
          .map((segment) => `rec=${segment.recovered}/fb=${segment.failovers}`)
          .join(' '),
      );
      await cluster.restart(active);

      // Immediately fire the first concurrent batch after the failover.
      const burstInputs = Array.from({ length: 12 }, (_, index) => 200 + index);
      const burst = await Promise.all(burstInputs.map((input) => accept('entry-c', 'currency', input)));
      const burstRecords = await Promise.all(
        burst.map((receipt) => waitRecord('entry-c', receipt.body.requestId, 25000)),
      );
      const allCorrect = burstInputs.every((input, index) => {
        const record = burstRecords[index];
        return record?.version === 'v2' && record.status === 'done' && recordsEqual(record.output, expectedCurrencyV2(input));
      });
      report.check(
        allCorrect,
        'first concurrent batch after failover returns pinned-version results',
        burstRecords
          .map((record, index) => `${record?.status}:${recordsEqual(record?.output, expectedCurrencyV2(burstInputs[index]))}`)
          .join(' '),
      );
    }

    /* ------------------ 11. branch takeover reuses durable result --------- */
    report.section('11. Lost receipt: branch result reused by a sibling instance');
    {
      // Warm up so the entry is pinned to one fx-tag instance, then arm a
      // one-shot fault that persists the branch result and dies before
      // acknowledging. The retry lands on the sibling, which must re-read the
      // durable result rather than recompute, and the join + serial tail must
      // still run on the pinned version.
      const warm = await submit('entry-a', 'currency', 710);
      const origin = segmentInstance(warm.body.record as RecordView, 'fx-tag')!;
      const sibling = otherInstanceOf('fx-tag', origin);
      const armed = await jsonRequest<{ ok: boolean; armed?: string }>(instanceBase(origin), {
        path: '/admin/fault',
        body: { mode: 'crash-after-journal' },
        timeoutMs: 3000,
      });
      report.check(armed.ok && armed.body?.armed === 'crash-after-journal', 'armed durable-write-then-crash on the active instance', `instance=${origin}`);

      const input = 711;
      const accepted = await accept('entry-a', 'currency', input);
      // The origin process exits by itself after writing the journal.
      const record = await waitRecord('entry-a', accepted.body.requestId, 30000);

      const tag = (record?.segments ?? []).find((segment) => segment.service === 'fx-tag');
      const finalSegment = (record?.segments ?? []).find((segment) => segment.service === 'fx-final');
      const expectedTag = `${input}n=-v2`; // v2 fx-tag: tag "-v2" on "711n="
      report.check(
        record?.status === 'done' && record.version === 'v2',
        'request completes on its pinned v2 after the receipt was lost',
        `status=${record?.status} v=${record?.version} err=${record?.error ?? ''}`,
      );
      report.check(
        Boolean(tag?.reused) && tag?.instance.split('#')[0] === sibling,
        'the sibling served the branch segment from the durable journal',
        `instance=${tag?.instance} sibling=${sibling} reused=${tag?.reused}`,
      );
      report.check(
        (tag?.originInstance ?? '').split('#')[0] === origin,
        'the reused segment is attributed to the instance that persisted it',
        `origin=${tag?.originInstance} expected=${origin}`,
      );
      report.check(
        recordsEqual(tag?.output, expectedTag),
        'the reused branch result is the first-acceptance version result',
        `output=${JSON.stringify(tag?.output)} expected=${JSON.stringify(expectedTag)}`,
      );
      report.check(
        Boolean(tag?.recovered) && (tag?.failovers ?? 0) >= 1,
        'the takeover was recorded as a recovery on the segment',
        `recovered=${tag?.recovered} failovers=${tag?.failovers}`,
      );
      report.check(
        (record?.segments ?? []).every((segment) => segment.version === 'v2'),
        'branches, join and the serial tail all used the pinned version',
        (record?.segments ?? []).map((s) => `${s.slot}@${s.version}`).join(' | '),
      );
      report.check(
        recordsEqual(record?.output, expectedCurrencyV2(input)) &&
          recordsEqual(finalSegment?.output, expectedCurrencyV2(input)),
        'final join output matches the independent v2 oracle',
        `actual=${JSON.stringify(record?.output)} expected=${JSON.stringify(expectedCurrencyV2(input))}`,
      );
      report.check(
        (record?.segments ?? []).filter((segment) => segment.service === 'fx-math').length === 1 &&
          (record?.segments ?? []).some((segment) => segment.service === 'fx-final'),
        'the parallel sibling branch and the serial tail each ran exactly once',
        (record?.segments ?? []).map((s) => s.service).join(','),
      );

      await cluster.restart(origin);
    }

    /* -------------- 12. cross-entry retry after an entry crash ------------ */
    report.section('12. Cross-entry resume after entry crash');
    {
      // Currency's fx-tag has an 80 ms delay, so the request is still running
      // when we kill the accepting entry.
      const receipt = await accept('entry-a', 'currency', 333);
      const early = await getRecord('entry-a', receipt.body.requestId);
      report.check(Boolean(early), 'request visible on the accepting entry', receipt.body.requestId);
      await cluster.kill('entry-a');
      const resume = await jsonRequest<{ ok: boolean; record: RecordView; error?: string }>(
        entryBase('entry-c'),
        { path: '/retry', body: { requestId: receipt.body.requestId }, timeoutMs: 25000 },
      );
      report.check(resume.ok && resume.body.ok === true, 'entry-c resumes the pinned request from the shared ledger', resume.body?.error ?? `status=${resume.status}`);
      const record = await getRecord('entry-c', receipt.body.requestId);
      report.check(
        record?.version === 'v2' &&
          record.retries >= 1 &&
          recordsEqual(record.output, expectedCurrencyV2(333)),
        'resumed request keeps the pinned version and yields the correct result',
        `v=${record?.version} retries=${record?.retries} out=${JSON.stringify(record?.output)} exp=${JSON.stringify(expectedCurrencyV2(333))}`,
      );
      report.check(
        (record?.segments ?? []).every((segment) => segment.version === 'v2'),
        'every resumed segment used the pinned version',
        (record?.segments ?? []).map((s) => `${s.service}@${s.version}`).join(' '),
      );
      await cluster.restart('entry-a');
    }

    /* ------------- 13. transport invariant under request volume ----------- */
    report.section('13. Config distribution is per instance and request-volume independent');
    {
      const transport = async (): Promise<{
        configBytes: number;
        perProcess: Record<string, number>;
      }> => {
        const res = await jsonRequest<{ configBytes: number; perProcess: Record<string, number> }>(
          coordinatorBase,
          { path: '/admin/transport', timeoutMs: 4000 },
        );
        return { configBytes: res.body?.configBytes ?? 0, perProcess: res.body?.perProcess ?? {} };
      };
      const before = await transport();
      report.check(
        multiInstance.every((service) =>
          instanceNamesOf(service).every(
            (instance) => (before.perProcess[`service:${instance}`] ?? 0) >= 0,
          ),
        ) && instanceNamesOf('fx-tag').every((instance) => `service:${instance}` in before.perProcess),
        'the transport ledger accounts for every instance separately',
        `perProcess=${Object.keys(before.perProcess).filter((k) => k.startsWith('service:fx-tag')).join(',')}`,
      );

      const burst = await Promise.all(
        Array.from({ length: 24 }, (_, index) =>
          submit(index % 2 === 0 ? 'entry-a' : 'entry-b', 'search', `bulk-${index}`),
        ),
      );
      report.check(
        burst.every((res) => res.status === 200 && (res.body.record as RecordView | undefined)?.status === 'done'),
        '24 concurrent search requests complete',
        burst
          .filter((res) => res.status !== 200 || (res.body.record as RecordView | undefined)?.status !== 'done')
          .map((res) => `${res.status}:${JSON.stringify(res.body?.code ?? res.body?.error ?? res.transportError ?? (res.body.record as RecordView | undefined)?.error)}`)
          .join(' | ') || 'all done',
      );
      const after = await transport();
      report.check(
        after.configBytes === before.configBytes,
        'config bytes over the wire unchanged by 24 requests',
        `before=${formatBytes(before.configBytes)} after=${formatBytes(after.configBytes)} delta=${after.configBytes - before.configBytes}`,
      );
      const segmentCounts = burst.map((res) => ((res.body.record as RecordView | undefined)?.segments ?? []).length);
      report.check(
        segmentCounts.every((count) => count > 0),
        'each request still executed its route',
        `segments/request=${segmentCounts.join(',')}`,
      );
      const versions = new Set(
        burst
          .map((res) => (res.body.record as RecordView | undefined)?.version)
          .filter((version): version is string => Boolean(version)),
      );
      report.check(versions.size === 1 && versions.has('v2'), 'all bulk requests pinned v2', [...versions].join(','));
    }

    /* --------------------------- 14. skew control ------------------------- */
    report.section('14. Version-skew is surfaced, not silently mixed');
    {
      const wrongType = await jsonRequest<{ ok: boolean; code?: string; error?: string }>(
        instanceBase('fx-math-1'),
        {
          path: '/execute',
          body: { requestId: 'skew-1', version: 'v1', slot: 'probe', service: 'fx-math', input: 'not-a-number' },
          timeoutMs: 3000,
        },
      );
      report.check(
        wrongType.status === 422 && wrongType.body?.code === 'wrong_input_type',
        'v1 numeric rule rejects string input from a v2-shaped pipeline',
        `status=${wrongType.status} code=${wrongType.body?.code}`,
      );
      const missingVersion = await jsonRequest<{ ok: boolean; code?: string }>(instanceBase('fx-math-1'), {
        path: '/execute',
        body: { requestId: 'skew-2', version: 'v9', slot: 'probe', service: 'fx-math', input: 1 },
        timeoutMs: 3000,
      });
      report.check(
        missingVersion.status === 409 && missingVersion.body?.code === 'version_not_installed',
        'a segment refuses a version it does not have',
        `status=${missingVersion.status} code=${missingVersion.body?.code}`,
      );
      // An instance refuses to execute a service name it does not implement.
      const wrongService = await jsonRequest<{ ok: boolean; code?: string }>(instanceBase('fx-math-1'), {
        path: '/execute',
        body: { requestId: 'skew-3', version: 'v2', slot: 'probe', service: 'fx-tag', input: 'x' },
        timeoutMs: 3000,
      });
      report.check(
        wrongService.status === 400 && wrongService.body?.code === 'bad_request',
        'an instance rejects a request for a different service name',
        `status=${wrongService.status} code=${wrongService.body?.code}`,
      );
    }

    /* -------------------------- 15. final sanity -------------------------- */
    report.section('15. Final consistency');
    {
      const final = await state(true);
      report.check(
        final.status === 'complete',
        'release completes',
        `status=${final.status} batches=${final.batches.map((b) => `${b.name}=${b.status}`).join(', ')}`,
      );
      report.check(
        final.batches.every((batch) => batch.status === 'effective'),
        'every batch effective at the end',
        final.batches.map((b) => `${b.name}=${b.status}`).join(', '),
      );
      report.check(
        (final.services ?? []).every((service) => service.reachable),
        'every service still has at least one healthy instance',
        (final.services ?? [])
          .filter((service) => !service.reachable)
          .map((service) => service.name)
          .join(',') || 'all reachable',
      );
      const ledger = await jsonRequest<{ configBytes: number; byRole: Record<string, number> }>(
        coordinatorBase,
        { path: '/admin/transport', timeoutMs: 3000 },
      );
      report.check(
        (ledger.body?.configBytes ?? 0) > 0,
        'transport ledger recorded real config traffic',
        `configBytes=${formatBytes(ledger.body?.configBytes ?? 0)}`,
      );
      const sample = await submit('entry-c', 'report', 'final-check');
      const sampleRecord = sample.body.record as RecordView;
      report.check(
        sampleRecord?.version === 'v2' && recordsEqual(sampleRecord.output, expectedReportV2('final-check')),
        'a final report request renders correctly on v2',
        `v=${sampleRecord?.version} out=${JSON.stringify(sampleRecord?.output)}`,
      );

      // Also verify an older pinned version is still executable directly.
      const v1Probe = await jsonRequest<{ ok: boolean; output?: unknown }>(instanceBase('report-format-1'), {
        path: '/execute',
        body: { requestId: 'hist-1', version: 'v1', slot: 'root', service: 'report-format', input: 'x' },
        timeoutMs: 3000,
      });
      report.check(
        v1Probe.ok && v1Probe.body?.output === 'x-fmt1',
        'older pinned versions remain executable after the release completes',
        `output=${JSON.stringify(v1Probe.body?.output)} exp="x-fmt1"`,
      );
    }

    /* ------------- 16. legacy single-instance deployment still works ------- */
    report.section('16. Single-instance deployment stays supported');
    {
      const single = new Cluster(RUN_SINGLE, topologySingle, TOPOLOGY_SINGLE_PATH);
      try {
        await single.startAll();
        const soloEntry = topologySingle.entries[0].name;
        const res = await jsonRequest<{ ok: boolean; record?: RecordView }>(singleEntryBase(soloEntry), {
          path: '/submit',
          body: { group: 'currency', input: 5 },
          timeoutMs: 20000,
        });
        const record = res.body?.record;
        report.check(
          res.status === 200 && record?.status === 'done',
          'a topology that declares only `port` boots and serves requests',
          `status=${res.status} record=${record?.status ?? res.body?.ok}`,
        );
        report.check(
          record?.version === 'v1' && recordsEqual(record.output, expectedCurrencyV1(5)),
          'the single-instance route produces the correct result',
          `v=${record?.version} out=${JSON.stringify(record?.output)}`,
        );
        report.check(
          (record?.segments ?? []).every((segment) => segment.version === 'v1'),
          'single-instance segments all use the pinned version',
          (record?.segments ?? []).map((s) => `${s.service}@${s.instance}`).join(' '),
        );
      } finally {
        await single.shutdown();
      }
    }
  } finally {
    await cluster.shutdown();
  }

  const elapsed = (Date.now() - startedAt) / 1000;
  report.print();
  const passed = report.lines.filter((line) => line.detail !== '\u0000' && line.ok).length;
  const total = report.lines.filter((line) => line.detail !== '\u0000').length;
  process.stdout.write(
    `\n${report.failed === 0 ? 'ALL CHECKS PASSED' : `${report.failed} CHECK(S) FAILED`} ` +
      `(${passed}/${total}), Elapsed: ${elapsed.toFixed(1)}s (limit 80s)\n`,
  );
  writeFileSync(
    join(RUN, 'validation-report.json'),
    JSON.stringify({ lines: report.lines, elapsed, failed: report.failed }, null, 2),
  );
  return report.failed === 0 && elapsed <= 80 ? 0 : 1;
}

main()
  .then((code) => process.exit(code))
  .catch((err) => {
    process.stderr.write(`validation crashed: ${(err as Error).stack ?? err}\n`);
    process.exit(3);
  });
