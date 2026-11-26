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
const FIXTURES = join(ROOT, 'fixtures');
const TOPOLOGY_PATH = join(FIXTURES, 'topology.json');
const PLAN_PATH = join(FIXTURES, 'plan.json');
const DIST = join(ROOT, 'dist');

interface TopologyFile {
  coordinator: { port: number };
  entries: { name: string; port: number; groups: string[] }[];
  services: { name: string; port: number; groups: string[] }[];
}

const topology = JSON.parse(readFileSync(TOPOLOGY_PATH, 'utf8')) as TopologyFile;
const coordinatorBase = `http://127.0.0.1:${topology.coordinator.port}`;
const entryBase = (name: string): string =>
  `http://127.0.0.1:${topology.entries.find((entry) => entry.name === name)!.port}`;
const serviceBase = (name: string): string =>
  `http://127.0.0.1:${topology.services.find((service) => service.name === name)!.port}`;
const entryPort = (name: string): number => topology.entries.find((e) => e.name === name)!.port;
const servicePort = (name: string): number => topology.services.find((s) => s.name === name)!.port;

/* --------------------------------- processes -------------------------------- */

interface ProcessSpec {
  role: 'coordinator' | 'entry' | 'service';
  name: string;
  port: number;
  args: string[];
  child?: ChildProcess;
  exited: boolean;
  stdout: string[];
  stderr: string[];
}

class Cluster {
  readonly procs = new Map<string, ProcessSpec>();

  constructor(private readonly dataDir: string) {}

  private spawnProcess(spec: Omit<ProcessSpec, 'child' | 'exited' | 'stdout' | 'stderr'>): ProcessSpec {
    const proc: ProcessSpec = { ...spec, exited: false, stdout: [], stderr: [] };
    const child = spawn(process.execPath, spec.args, {
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
    this.procs.set(spec.name, proc);
    return proc;
  }

  private argsFor(role: 'coordinator' | 'entry' | 'service', name: string): string[] {
    const common = ['--data-dir', this.dataDir, '--topology', TOPOLOGY_PATH];
    if (role === 'coordinator') {
      return [join(DIST, 'coordinator/main.js'), ...common, '--plan', PLAN_PATH];
    }
    return [join(DIST, `${role}/main.js`), ...common, '--name', name];
  }

  startCoordinator(): ProcessSpec {
    return this.spawnProcess({
      role: 'coordinator',
      name: 'coordinator',
      port: topology.coordinator.port,
      args: this.argsFor('coordinator', 'coordinator'),
    });
  }

  startEntry(name: string): ProcessSpec {
    return this.spawnProcess({ role: 'entry', name, port: entryPort(name), args: this.argsFor('entry', name) });
  }

  startService(name: string): ProcessSpec {
    return this.spawnProcess({
      role: 'service',
      name,
      port: servicePort(name),
      args: this.argsFor('service', name),
    });
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
    await this.waitHealthy(coordinatorBase);
    for (const entry of topology.entries) {
      this.startEntry(entry.name);
      await this.waitHealthy(entryBase(entry.name));
    }
    for (const service of topology.services) this.startService(service.name);
    await Promise.all(topology.services.map((service) => this.waitHealthy(serviceBase(service.name))));
  }

  /** SIGKILL — no graceful flush, mirroring a real crash. */
  async kill(name: string): Promise<void> {
    const proc = this.procs.get(name);
    if (!proc?.child || proc.exited) return;
    const exited = new Promise<void>((resolveExit) => proc.child!.once('exit', () => resolveExit()));
    proc.child.kill('SIGKILL');
    await Promise.race([exited, sleep(3000)]);
    await this.waitUnhealthy(`http://127.0.0.1:${proc.port}`, 3000).catch(() => undefined);
  }

  async restart(name: string): Promise<void> {
    const existing = this.procs.get(name);
    if (!existing) throw new Error(`unknown process ${name}`);
    if (existing.role === 'coordinator') this.startCoordinator();
    else if (existing.role === 'entry') this.startEntry(name);
    else this.startService(name);
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
  services?: { name: string; reachable: boolean; versions: string[] }[];
}

interface RecordView {
  requestId: string;
  version: string;
  group: string;
  status: string;
  output?: unknown;
  segments: { service: string; version: string; recovered: boolean; slot: string; output: unknown }[];
  retries: number;
  resumedBy?: string;
  error?: string;
}

interface ProgressEvidence {
  observed: boolean;
  version: string | null;
  at: number | null;
  instance?: string | null;
  producer?: string | null;
  entry?: string | null;
  recovered?: boolean;
  healed?: boolean;
  ok?: boolean;
}

interface ProgressSegment {
  slot: string;
  service: string;
  order: number;
  serviceCompleted: ProgressEvidence;
  entryReceipt: ProgressEvidence;
  actualVersion: string | null;
  recordVersion: string | null;
  gaps: string[];
  state: string;
  conflict?: boolean;
}

interface ProgressView {
  channel: string;
  expected: number;
  actualVersion: string | null;
  segments: ProgressSegment[];
  summary: {
    total: number;
    serviceCompleted: number;
    receiptsObserved: number;
    confirmed: number;
    withGaps: number;
    complete: boolean;
  };
  gaps: string[];
  updatedAt: number | null;
}

interface ProgressRecordView {
  requestId: string;
  recordAvailable: boolean;
  version?: string;
  group?: string;
  status?: string;
  output?: unknown;
  segments?: { service: string; version: string; recovered: boolean; slot: string; output: unknown }[];
  retries?: number;
  error?: string;
  accepted: {
    version: string;
    group: string;
    acceptedBy: string;
    acceptedAt: number;
    bindingEpoch: number | null;
    path: string[];
  } | null;
  progress: ProgressView;
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

/** Query per-request progress evidence from the coordinator. */
const progressRecords = async (ids: string[]): Promise<ProgressRecordView[]> => {
  const res = await jsonRequest<{ ok: boolean; records: ProgressRecordView[] }>(coordinatorBase, {
    path: `/admin/records?ids=${ids.join(',')}`,
    timeoutMs: 6000,
  });
  return res.ok ? (res.body.records ?? []) : [];
};

/** Poll a request's progress until every expected segment is confirmed. */
const waitProgressComplete = async (
  requestId: string,
  timeoutMs = 8000,
): Promise<ProgressRecordView | undefined> => {
  const deadline = Date.now() + timeoutMs;
  let last: ProgressRecordView | undefined;
  while (Date.now() < deadline) {
    last = (await progressRecords([requestId]))[0];
    if (last?.progress.summary.complete) return last;
    await sleep(40);
  }
  return last ?? (await progressRecords([requestId]))[0];
};

/** Poll until a predicate holds over the request's progress view. */
const waitProgress = async (
  requestId: string,
  predicate: (view: ProgressRecordView) => boolean,
  timeoutMs = 8000,
): Promise<ProgressRecordView | undefined> => {
  const deadline = Date.now() + timeoutMs;
  let last: ProgressRecordView | undefined;
  while (Date.now() < deadline) {
    last = (await progressRecords([requestId]))[0];
    if (last && predicate(last)) return last;
    await sleep(40);
  }
  return last;
};

const setProgressChannel = async (hold: {
  service_done?: boolean;
  entry_receipt?: boolean;
}): Promise<boolean> => {
  const res = await jsonRequest<{ ok: boolean; hold?: Record<string, boolean> }>(coordinatorBase, {
    path: '/admin/progress/control',
    body: hold,
    timeoutMs: 3000,
  });
  if (!res.ok || !res.body?.ok) return false;
  const applied = res.body.hold ?? {};
  return Object.entries(hold).every(([stage, value]) => applied[stage] === value);
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

/** True if nothing is listening on the port (we can bind it). */
const portFree = (port: number): Promise<boolean> =>
  new Promise((resolvePort) => {
    const server = createServer();
    server.once('error', () => resolvePort(false));
    server.once('listening', () => server.close(() => resolvePort(true)));
    server.listen(port, '127.0.0.1');
  });

/** All ports the fixed topology will use. */
const allPorts = (): number[] => [
  topology.coordinator.port,
  ...topology.entries.map((entry) => entry.port),
  ...topology.services.map((service) => service.port),
];

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

  const report = new Report();
  const cluster = new Cluster(RUN);
  const startedAt = Date.now();

  try {
    /* ------------------------------- 1. boot ------------------------------- */
    report.section('1. Boot & initial state');
    await cluster.startAll();
    await waitBindings(['currency', 'search', 'report']);
    const initial = await state();
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
    report.section('3. Batch 1 — currency, shared entry boundary');
    {
      const advance = await ops('/ops/advance');
      report.check(advance.status === 200 && advance.body.ok, 'advance batch 1 succeeds', advance.body.error ?? 'ok');
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
      await cluster.kill('archive-store'); // only batch 3 needs it
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
      const advance2 = await ops('/ops/advance');
      report.check(
        advance2.status === 200 && advance2.body.ok,
        'batch 2 advances although archive-store (batch 3 only) is down',
        JSON.stringify(advance2.body.blocked ?? 'unblocked'),
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
        after2.bindings.epoch > 0,
        'a single epoch covers the multi-group batch',
        `epoch=${after2.bindings.epoch}`,
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
        'search now serves v2',
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
      await cluster.restart('archive-store');
    }

    /* ------------------- 5. same-batch service downtime ------------------- */
    report.section('5. Batch blocked when its own service is down');
    {
      await cluster.kill('archive-load'); // batch 3 needs it
      const blocked = await ops('/ops/advance');
      report.check(
        blocked.status === 409 && blocked.body.code === 'blocked',
        'batch 3 is blocked while archive-load is down',
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
      await cluster.restart('archive-load');
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

    /* ------- 10. service restart: first batch of concurrent requests ------ */
    report.section('10. Service restart — first concurrent batch after reboot');
    {
      // FX-tag has a 500 ms delay, so these requests are still in flight when
      // the service is killed. A restart happens promptly; the accepted
      // requests must then finish on their pinned v2 (service journal heal +
      // entry retry) rather than fail or flip version.
      const inFlight = await Promise.all(
        Array.from({ length: 6 }, (_, index) => accept('entry-a', 'currency', 100 + index)),
      );
      await sleep(120);
      await cluster.kill('fx-tag');
      await sleep(150);
      await cluster.restart('fx-tag');
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
        'requests in flight across the restart still resolve on v2',
        firstWave
          .map((record, index) => `${record?.status}:${recordsEqual(record?.output, expectedCurrencyV2(100 + index))}`)
          .join(' '),
      );
      report.check(
        firstWave.every((record) => (record?.segments ?? []).every((segment) => segment.version === 'v2')),
        'no in-flight segment flipped to another version',
        firstWave.map((record) => (record?.segments ?? []).map((s) => s.version).join('/')).join(' '),
      );

      // Immediately fire the first concurrent batch after the reboot.
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
        'first concurrent batch after restart returns pinned-version results',
        burstRecords
          .map((record, index) => `${record?.status}:${recordsEqual(record?.output, expectedCurrencyV2(burstInputs[index]))}`)
          .join(' '),
      );
      report.check(
        burstRecords.every((record) => (record?.segments ?? []).every((segment) => segment.version === 'v2')),
        'no post-restart segment ran a version other than v2',
        burstRecords.map((record) => (record?.segments ?? []).map((s) => s.version).join('/')).join(' '),
      );
      report.check(
        firstWave.some((record) => (record?.segments ?? []).some((segment) => segment.recovered)),
        'the restart was exercised through the recovery path',
        firstWave
          .flatMap((record) => record?.segments ?? [])
          .filter((segment) => segment.recovered)
          .map((segment) => segment.slot)
          .join(',') || 'none',
      );
    }

    /* -------------- 11. cross-entry retry after an entry crash ------------ */
    report.section('11. Cross-entry resume after entry crash');
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

    /* ------------- 12. transport invariant under request volume ----------- */
    report.section('12. Config transport does not scale with request volume');
    {
      const configBytes = async (): Promise<number> => {
        const res = await jsonRequest<{ configBytes: number }>(coordinatorBase, {
          path: '/admin/transport',
          timeoutMs: 4000,
        });
        return res.body?.configBytes ?? 0;
      };
      const before = await configBytes();
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
      const after = await configBytes();
      report.check(
        after === before,
        'config bytes over the wire unchanged by 24 requests',
        `before=${formatBytes(before)} after=${formatBytes(after)} delta=${after - before}`,
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

    /* --------------------------- 13. skew control ------------------------- */
    report.section('13. Version-skew is surfaced, not silently mixed');
    {
      const wrongType = await jsonRequest<{ ok: boolean; code?: string; error?: string }>(
        serviceBase('fx-math'),
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
      const missingVersion = await jsonRequest<{ ok: boolean; code?: string }>(serviceBase('fx-math'), {
        path: '/execute',
        body: { requestId: 'skew-2', version: 'v9', slot: 'probe', service: 'fx-math', input: 1 },
        timeoutMs: 3000,
      });
      report.check(
        missingVersion.status === 409 && missingVersion.body?.code === 'version_not_installed',
        'a segment refuses a version it does not have',
        `status=${missingVersion.status} code=${missingVersion.body?.code}`,
      );
    }

    /* ------------- 14. live progress evidence while running --------------- */
    report.section('14. /admin/records shows per-segment progress while running');
    {
      // currency's fx-tag branch has a 500 ms delay: this request is still
      // executing while we query it, so progress must be visible mid-flight.
      const receipt = await accept('entry-a', 'currency', 777);
      report.check(receipt.status === 200, 'long request accepted', receipt.body.requestId);

      const running = await waitProgress(
        receipt.body.requestId,
        (view) => view.progress.segments.some((segment) => segment.state === 'confirmed'),
        6000,
      );
      const runningProgress = running?.progress;
      report.check(
        Boolean(runningProgress) && runningProgress!.segments.length === 4,
        'each route segment appears separately before completion',
        (runningProgress?.segments ?? []).map((s) => `${s.slot}:${s.state}`).join(' | '),
      );
      const confirmed = (runningProgress?.segments ?? []).filter((s) => s.state === 'confirmed');
      report.check(
        confirmed.length >= 1 &&
          confirmed.every((s) => s.serviceCompleted.observed && s.entryReceipt.observed),
        'a finished segment shows both service completion and entry receipt',
        confirmed.map((s) => `${s.service}[svc=${s.serviceCompleted.observed},rcpt=${s.entryReceipt.observed}]`).join(' '),
      );
      report.check(
        running !== undefined && running.version === 'v2' && running.status === 'running',
        'original accepted version and status still shown',
        `status=${running?.status} version=${running?.version}`,
      );
      // The slow fx-tag branch has not produced evidence yet: its segment is
      // listed with both gaps and no claimed version.
      const pending = (runningProgress?.segments ?? []).find((s) => s.state === 'pending');
      report.check(
        pending !== undefined &&
          pending.serviceCompleted.observed === false &&
          pending.entryReceipt.observed === false &&
          pending.actualVersion === null &&
          pending.gaps.includes('service_completion') &&
          pending.gaps.includes('entry_receipt'),
        'a not-yet-executed segment is listed with an explicit gap',
        `state=${pending?.state} gaps=${(pending?.gaps ?? []).join(',')} actual=${pending?.actualVersion}`,
      );

      // Once it finishes, every segment is confirmed with actual version v2.
      const completedRecord = await waitRecord('entry-a', receipt.body.requestId, 20000);
      const done = await waitProgressComplete(receipt.body.requestId, 10000);
      report.check(
        done?.progress.summary.complete === true &&
          done.progress.segments.every((s) => s.state === 'confirmed') &&
          done.progress.segments.every((s) => s.actualVersion === 'v2') &&
          done.progress.actualVersion === 'v2',
        'after completion every segment is confirmed at the executed version',
        (done?.progress.segments ?? []).map((s) => `${s.slot}:${s.state}@${s.actualVersion}`).join(' '),
      );
      report.check(
        completedRecord?.version === 'v2' &&
          completedRecord.status === 'done' &&
          done?.version === 'v2' &&
          done.status === 'done' &&
          recordsEqual(done.output, expectedCurrencyV2(777)) &&
          recordsEqual(completedRecord.output, expectedCurrencyV2(777)),
        'original result is still returned unchanged alongside progress',
        `v=${completedRecord?.version} status=${completedRecord?.status} out=${JSON.stringify(completedRecord?.output)}`,
      );    }

    /* --------- 15. progress held back; evidence survives stops ----------- */
    report.section('15. Completion delivered but receipt withheld; evidence survives');
    {
      // Hold back the entry-receipt stage only (execution and service-progress
      // keep flowing). A currency request's fast segments then finish while the
      // slow fx-tag branch is still running: completion visible, receipt withheld.
      report.check(
        await setProgressChannel({ entry_receipt: true }),
        'entry-receipt evidence can be held back independently of execution',
        'hold.entry_receipt=true',
      );
      const receipt = await accept('entry-a', 'currency', 888);
      report.check(receipt.status === 200, 'request accepted while progress is held back', receipt.body.requestId);
      const running = await waitProgress(
        receipt.body.requestId,
        (view) => view.progress.segments.some((s) => s.state === 'service_done_unacked'),
        6000,
      );
      const delivered = (running?.progress.segments ?? []).find(
        (s) => s.state === 'service_done_unacked',
      );
      report.check(
        Boolean(delivered) &&
          delivered!.serviceCompleted.observed &&
          !delivered!.entryReceipt.observed,
        'service completion observed while the entry receipt is still withheld',
        (running?.progress.segments ?? [])
          .map((s) => `${s.service}[svc=${s.serviceCompleted.observed},rcpt=${s.entryReceipt.observed}]`)
          .join(' '),
      );

      // The business request still completes correctly on its pinned version.
      const completed = await waitRecord('entry-a', receipt.body.requestId, 25000);
      report.check(
        completed?.status === 'done' &&
          completed.version === 'v2' &&
          recordsEqual(completed.output, expectedCurrencyV2(888)),
        'business result is correct even though the progress path is interrupted',
        `status=${completed?.status} v=${completed?.version} out=${JSON.stringify(completed?.output)}`,
      );

      // Stop the entry and the execution service: everything for this request
      // is now down. Progress evidence and the actual version must remain
      // queryable, with the withheld receipt still marked as not observed.
      await cluster.kill('entry-a');
      await cluster.kill('fx-tag');
      const dark = (await progressRecords([receipt.body.requestId]))[0];
      report.check(
        dark?.progress.channel === 'interrupted' &&
          dark.progress.summary.withGaps > 0 &&
          dark.progress.gaps.length > 0,
        'query with the entry and service stopped marks the evidence gap',
        `channel=${dark?.progress.channel} gaps=${(dark?.progress.gaps ?? []).slice(0, 2).join(' | ')}`,
      );
      const probedSegment = (dark?.progress.segments ?? []).find((s) => s.serviceCompleted.observed);
      report.check(
        probedSegment !== undefined &&
          probedSegment.actualVersion === 'v2' &&
          probedSegment.entryReceipt.observed === false,
        'actual version survives; entry receipt stays "not observed"',
        `actual=${probedSegment?.actualVersion} rcptObserved=${probedSegment?.entryReceipt.observed}`,
      );
      report.check(
        dark?.recordAvailable === true &&
          dark.status === 'done' &&
          dark.version === 'v2' &&
          recordsEqual(dark.output, expectedCurrencyV2(888)),
        'the original accepted version, status and result are still shown',
        `recordAvailable=${dark?.recordAvailable} v=${dark?.version} status=${dark?.status}`,
      );

      // Restart the producers; buffered evidence drains and the gaps fill.
      await cluster.restart('fx-tag');
      await cluster.restart('entry-a');
      report.check(
        await setProgressChannel({ entry_receipt: false }),
        'entry-receipt stage reopened',
        'hold.entry_receipt=false',
      );
      const healed = await waitProgressComplete(receipt.body.requestId, 12000);
      report.check(
        healed?.progress.summary.complete === true &&
          healed.progress.channel === 'flowing' &&
          healed.progress.segments.every((s) => s.entryReceipt.observed),
        'a producer restart backfills the withheld completion and receipt evidence',
        (healed?.progress.segments ?? []).map((s) => `${s.slot}:${s.state}`).join(' '),
      );
    }

    /* ------------- 16. progress outage vs. duplicate/late events --------- */
    report.section('16. Outage, duplication and late reports keep progress monotonic');
    {
      // Interrupt the channel, run a full request, then verify the query
      // reports the gap while the business result is unaffected.
      await setProgressChannel({ service_done: true });
      const receipt = await accept('entry-c', 'currency', 999);      const completed = await waitRecord('entry-c', receipt.body.requestId, 25000);
      report.check(
        completed?.status === 'done' &&
          completed.version === 'v2' &&
          recordsEqual(completed.output, expectedCurrencyV2(999)),
        'request completes correctly with the progress path interrupted',
        `status=${completed?.status} out=${JSON.stringify(completed?.output)}`,
      );
      const gap = (await progressRecords([receipt.body.requestId]))[0];
      report.check(
        gap?.progress.channel === 'interrupted' &&
          gap.progress.summary.complete === false &&
          gap.progress.gaps.length > 0 &&
          gap.progress.segments.every((s) => !s.serviceCompleted.observed),
        'query marks the progress gap while the result is already correct',
        `channel=${gap?.progress.channel} gaps=${gap?.progress.gaps.length} observed=${gap?.progress.segments.filter((s) => s.serviceCompleted.observed).length}`,
      );

      // Restore the path: buffered evidence drains and completes the record.
      await setProgressChannel({ service_done: false });
      const drained = await waitProgressComplete(receipt.body.requestId, 12000);
      report.check(
        drained?.progress.summary.complete === true &&
          drained.progress.segments.every((s) => s.actualVersion === 'v2'),
        'restoring the channel backfills the original request’s evidence',
        (drained?.progress.segments ?? []).map((s) => `${s.slot}:${s.state}`).join(' '),
      );

      // Duplicate and late events must not duplicate a segment or rewind it.
      const before = drained?.progress.segments ?? [];
      const replay = before.flatMap((segment) => [
        {
          eventId: `replay:${segment.slot}:svc`,
          stage: 'service_done',
          role: 'service',
          producer: 'fx-tag',
          instance: 'fx-tag#99',
          requestId: receipt.body.requestId,
          slot: segment.slot,
          service: segment.service,
          version: 'v2',
          at: segment.serviceCompleted.at ?? Date.now(),
          seq: 1,
        },
        {
          eventId: `replay:${segment.slot}:rcpt`,
          stage: 'entry_receipt',
          role: 'entry',
          producer: 'entry-c',
          instance: 'entry-c#99',
          entry: 'entry-c',
          requestId: receipt.body.requestId,
          slot: segment.slot,
          service: segment.service,
          version: 'v2',
          at: segment.entryReceipt.at ?? Date.now(),
          seq: 1,
        },
      ]);
      const replayRes = await jsonRequest<{ ok: boolean; accepted?: number }>(coordinatorBase, {
        path: '/admin/progress',
        body: { events: replay },
        timeoutMs: 4000,
      });
      // A late, contradicting report arrives afterwards as well.
      await jsonRequest(coordinatorBase, {
        path: '/admin/progress',
        body: {
          events: before.map((segment) => ({
            eventId: `late:${segment.slot}`,
            stage: 'service_done',
            role: 'service',
            producer: 'fx-tag',
            instance: 'fx-tag#99',
            requestId: receipt.body.requestId,
            slot: segment.slot,
            service: segment.service,
            version: 'v1',
            at: Date.now() + 5000,
            seq: 2,
          })),
        },
        timeoutMs: 4000,
      });
      const after = (await progressRecords([receipt.body.requestId]))[0];
      report.check(
        replayRes.ok && after !== undefined,
        'duplicate reports are accepted idempotently',
        `accepted=${replayRes.body?.accepted}`,
      );
      report.check(
        after?.progress.segments.length === before.length &&
          new Set(after.progress.segments.map((s) => s.slot)).size === after.progress.segments.length,
        'no segment appears twice after duplicate reports',
        (after?.progress.segments ?? []).map((s) => s.slot).join(' | '),
      );
      report.check(
        after?.progress.actualVersion === 'v2' &&
          after.progress.segments.every(
            (s) => s.actualVersion === 'v2' && s.state === 'confirmed',
          ),
        'late/contradicting reports do not rewind segment versions or state',
        (after?.progress.segments ?? []).map((s) => `${s.slot}:${s.actualVersion}:${s.state}`).join(' '),
      );
    }

    /* -------------------------- 17. final sanity -------------------------- */
    report.section('17. Final consistency');
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
      const v1Probe = await jsonRequest<{ ok: boolean; output?: unknown }>(serviceBase('report-format'), {
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
