import type {
  AdvanceOutcome,
  BatchState,
  BindingsView,
  BlockedReason,
  ConfigDocument,
  CoordinatorState,
  EntryAck,
  EntryBindingView,
  EntryProcessDef,
  GroupId,
  PendingSwitch,
  ReleasePlan,
  ReleaseStateView,
  RollbackState,
  ServiceName,
  ServiceReadinessView,
  Topology,
  Version,
} from '../shared/types';
import type { ConfigRepository, CoordinatorRepository } from '../shared/store';
import { TransportLedger } from '../shared/store';
import { requiredServices, validateConfigDocument, walkRoute } from '../shared/engine';
import { jsonRequest } from '../shared/http';
import { PeerClient } from '../shared/peer';
import { errorMessage, sleep } from '../shared/util';

export interface DeployResult {
  ok: boolean;
  version: Version;
  installed: string[];
  failed: { service: string; error: string }[];
}

/**
 * Release controller: the single writer of coordinator state.
 *
 * Model
 * -----
 * A plan is a list of batches. Batch N covers a set of groups; every group in
 * the batch flips to `targetVersion` at one shared boundary (`epoch`). Batches
 * advance strictly in order. A batch switches only when every service its
 * groups' routes actually need can execute the target version — services used
 * exclusively by later batches are irrelevant, so their downtime cannot stall
 * the current batch.
 *
 * Two-phase cut-over
 * ------------------
 * Before touching any entry the coordinator durably writes `pending` with the
 * complete desired bindings. Entries run prepare → commit: prepare verifies
 * config availability and records the incoming epoch; commit flips the
 * in-memory binding map atomically. After the fan-out the coordinator re-reads
 * every entry and declares the batch effective only when all reachable entries
 * report the new epoch. A crash at any point is therefore recoverable: the
 * successor inspects the entries to learn whether the switch actually landed
 * and never guesses.
 */
export class ReleaseController {
  private state: CoordinatorState;
  private recoveryPerformed = false;

  constructor(
    private readonly topology: Topology,
    private readonly repo: CoordinatorRepository,
    private readonly configs: ConfigRepository,
    private readonly peers = new PeerClient(),
  ) {
    this.state = this.repo.loadState() ?? this.blankState();
  }

  private blankState(): CoordinatorState {
    return {
      plan: { releaseId: 'unset', batches: [] },
      batches: [],
      cursor: -1,
      paused: false,
      rollback: null,
      rollbackHistory: [],
      initialVersions: {},
      pending: null,
      epochSeq: 0,
      seq: 0,
      updatedAt: Date.now(),
      recoveries: 0,
      lastRecovery: null,
    };
  }

  private saveState(): void {
    this.repo.saveState(this.state);
  }

  /* ------------------------------ construction ----------------------------- */

  /** First boot only: install a plan, its configs and the initial bindings. */
  initialize(input: {
    plan: ReleasePlan;
    initialVersions: Record<GroupId, Version>;
    configs: ConfigDocument[];
  }): void {
    if (this.repo.loadState()) return;
    for (const doc of input.configs) this.configs.publish(doc);
    for (const version of new Set(Object.values(input.initialVersions))) {
      if (!this.configs.get(version)) {
        throw new Error(`plan references ${version} but no config document was supplied for it`);
      }
    }
    const batches: BatchState[] = input.plan.batches.map((batch, index) => {
      const before: Record<GroupId, Version> = {};
      for (const group of batch.groups) before[group] = input.initialVersions[group];
      return {
        index,
        name: batch.name,
        groups: [...batch.groups],
        targetVersion: batch.targetVersion,
        effective: { ...before },
        before,
        status: 'pending',
        updatedAt: Date.now(),
      };
    });
    this.state = {
      ...this.blankState(),
      plan: input.plan,
      batches,
      initialVersions: { ...input.initialVersions },
    };
    this.saveState();
    this.repo.saveBindings({
      versions: { ...input.initialVersions },
      digests: this.digestsFor(input.initialVersions),
      epoch: 0,
      updatedAt: Date.now(),
    });
  }

  /* --------------------------------- boot ---------------------------------- */

  /**
   * Reconcile persisted state with what entries actually run. Called on every
   * boot of an existing state; a no-op when there is nothing to reconcile.
   */
  async recover(): Promise<void> {
    if (this.state.plan.releaseId === 'unset') return;
    const notes: string[] = [];
    const entries = await this.probeEntries();

    if (this.state.pending) {
      const pending = this.state.pending;
      const committed = await this.resolvePending(pending, entries);
      if (committed) {
        this.repo.saveBindings({
          versions: pending.versions,
          digests: pending.digests,
          epoch: pending.epoch,
          updatedAt: Date.now(),
        });
        this.finishPending(pending);
        notes.push(`pending ${pending.kind} of batch ${pending.batchIndex} had committed; adopted`);
      } else {
        this.state.pending = null;
        this.saveState();
        notes.push(
          `pending ${pending.kind} of batch ${pending.batchIndex} had not committed; discarded`,
        );
      }
    }

    for (const batch of this.state.batches) {
      if (batch.status === 'switching') {
        const observed = this.observedBatchView(batch, entries);
        batch.effective = observed.effective;
        batch.status = observed.settled ? 'effective' : 'pending';
        batch.updatedAt = Date.now();
        notes.push(`batch ${batch.index} was mid-switch; reconciled to ${batch.status} from entries`);
      }
    }

    if (this.state.rollback?.status === 'confirming') {
      notes.push(`rollback of batch ${this.state.rollback.batchIndex} awaiting confirmation`);
    }

    const bindings = this.repo.loadBindings();
    if (bindings) {
      const failures = await this.pushBindings(bindings);
      if (failures.length > 0) notes.push(`bindings not yet acknowledged by ${failures.join(', ')}`);
    }

    this.recoveryPerformed = true;
    this.state.recoveries += 1;
    this.state.lastRecovery = notes.join('; ') || 'state loaded; nothing to reconcile';
    this.saveState();
  }

  private async resolvePending(
    pending: PendingSwitch,
    entries: EntryBindingView[],
  ): Promise<boolean> {
    const reachable = entries.filter((entry) => entry.reachable && entry.epoch !== null);
    if (reachable.length === 0) {
      return this.repo.loadBindings()?.epoch === pending.epoch;
    }
    const committed = reachable.filter((entry) => (entry.epoch ?? -1) >= pending.epoch);
    if (committed.length === reachable.length) return true;
    if (committed.length === 0) return false;
    // Mixed: predecessor died mid-fan-out. Re-push to converge on the durable
    // intent; entries already committed are idempotent for the same epoch.
    const bindings: BindingsView = {
      versions: pending.versions,
      digests: pending.digests,
      epoch: pending.epoch,
      updatedAt: Date.now(),
    };
    const failures = await this.pushBindings(bindings);
    return failures.length === 0;
  }

  private finishPending(pending: PendingSwitch): void {
    const batch = this.state.batches[pending.batchIndex];
    if (pending.kind === 'advance' && batch) {
      const observedAtCommit: Record<GroupId, Version> = {};
      for (const group of batch.groups) {
        observedAtCommit[group] = pending.versions[group];
      }
      batch.effective = observedAtCommit;
      batch.status = 'effective';
      batch.note = undefined;
      batch.updatedAt = Date.now();
      this.state.cursor = Math.max(this.state.cursor, pending.batchIndex);
    }
    if (pending.kind === 'rollback' && batch) {
      const restored: Record<GroupId, Version> = {};
      for (const group of batch.groups) restored[group] = pending.versions[group];
      batch.effective = restored;
      batch.status = 'rolled_back';
      batch.note = 'rolled back by operator';
      batch.updatedAt = Date.now();
      const rollback = this.state.rollback;
      if (rollback) {
        rollback.status = 'confirmed';
        rollback.confirmedAt = rollback.confirmedAt ?? Date.now();
        this.state.rollbackHistory.push(rollback);
        this.state.rollback = null;
      }
    }
    this.state.pending = null;
    this.saveState();
  }

  /* ------------------------------- queries -------------------------------- */

  view(detail: false): ReleaseStateView;
  view(
    detail: true,
    store?: { kind: 'sqlite' | 'file'; location: string; cluster?: 'file' },
  ): Promise<ReleaseStateView>;
  view(
    detail: boolean,
    store?: { kind: 'sqlite' | 'file'; location: string; cluster?: 'file' },
  ): ReleaseStateView | Promise<ReleaseStateView>;
  view(
    detail: boolean,
    store?: { kind: 'sqlite' | 'file'; location: string },
  ): ReleaseStateView | Promise<ReleaseStateView> {
    const base = this.viewBase();
    return detail ? this.viewDetailed(base, store) : base;
  }

  private viewBase(): ReleaseStateView {
    const bindings = this.repo.loadBindings() ?? {
      versions: {},
      digests: {},
      epoch: 0,
      updatedAt: 0,
    };
    const allEffective =
      this.state.batches.length > 0 &&
      this.state.batches.every((batch) => batch.status === 'effective');
    const anyRolledBack = this.state.batches.some((batch) => batch.status === 'rolled_back');
    const pendingBatches = this.state.batches.some(
      (batch) => batch.status === 'pending' || batch.status === 'switching',
    );
    const status = this.state.paused
      ? 'paused'
      : allEffective
        ? 'complete'
        : !pendingBatches && anyRolledBack
          ? 'rolled_back'
          : 'releasing';
    return {
      releaseId: this.state.plan.releaseId,
      status,
      paused: this.state.paused,
      cursor: this.state.cursor,
      seq: this.state.seq,
      batches: this.state.batches.map((batch) => ({
        index: batch.index,
        name: batch.name,
        groups: [...batch.groups],
        targetVersion: batch.targetVersion,
        status: batch.status,
        effective: { ...batch.effective },
        before: { ...batch.before },
        note: batch.note,
      })),
      bindings: { ...bindings, versions: { ...bindings.versions }, digests: { ...bindings.digests } },
      rollback: this.state.rollback ? { ...this.state.rollback } : null,
      rollbackHistory: this.state.rollbackHistory.map((entry) => ({ ...entry })),
      recovered: this.recoveryPerformed,
      recoveries: this.state.recoveries,
      lastRecovery: this.state.lastRecovery,
      updatedAt: this.state.updatedAt,
    };
  }

  private async viewDetailed(
    base: ReleaseStateView,
    store?: { kind: 'sqlite' | 'file'; location: string; cluster?: 'file' },
  ): Promise<ReleaseStateView> {
    const [entries, services] = await Promise.all([this.probeEntries(), this.probeServices()]);
    return { ...base, entries, services, store };
  }

  bindingsView(): BindingsView {
    return (
      this.repo.loadBindings() ?? { versions: {}, digests: {}, epoch: 0, updatedAt: 0 }
    );
  }

  planSnapshot(): ReleasePlan {
    return this.state.plan;
  }

  configList(): { version: Version; digest: string; size: number }[] {
    return this.configs
      .versions()
      .map((version) => {
        const meta = this.configs.meta(version);
        return { version, digest: meta?.digest ?? '', size: meta?.size ?? 0 };
      })
      .filter((entry) => entry.digest !== '');
  }

  config(version: Version): ConfigDocument | undefined {
    return this.configs.get(version);
  }

  stateSnapshot(): CoordinatorState {
    return JSON.parse(JSON.stringify(this.state)) as CoordinatorState;
  }

  /* --------------------------------- ops ----------------------------------- */

  async deploy(version: Version, only?: ServiceName[]): Promise<DeployResult> {
    const doc = this.configs.get(version);
    if (!doc) {
      return {
        ok: false,
        version,
        installed: [],
        failed: [{ service: '*', error: `config ${version} not published` }],
      };
    }
    const meta = this.configs.meta(version);
    const targets = this.topology.services.filter(
      (service) => !only || only.includes(service.name),
    );
    const installed: string[] = [];
    const failed: { service: string; error: string }[] = [];
    await Promise.all(
      targets.map(async (service) => {
        const url = `http://127.0.0.1:${service.port}`;
        const result = await this.peers.installConfig(url, doc, meta?.digest);
        if (result.ok) installed.push(service.name);
        else failed.push({ service: service.name, error: result.error ?? 'install failed' });
      }),
    );
    installed.sort();
    failed.sort((a, b) => a.service.localeCompare(b.service));
    return { ok: failed.length === 0, version, installed, failed };
  }

  /**
   * Advance to the first non-effective batch. Refuses while paused or while a
   * rollback awaits confirmation. Idempotent when a switch is already durable.
   *
   * `fault` exists only for the crash-recovery fixtures: it makes the process
   * die at a precise point so a successor must reconcile from durable state.
   */
  async advance(
    options: { fault?: 'after-pending' | 'after-commit' } = {},
  ): Promise<AdvanceOutcome> {
    if (this.state.plan.releaseId === 'unset') {
      return { ok: false, code: 'not_initialized', error: 'no plan installed' };
    }
    if (this.state.rollback && this.state.rollback.status !== 'confirmed') {
      return {
        ok: false,
        code: 'rollback_pending',
        error: 'a rollback is pending; confirm it before advancing',
      };
    }
    if (this.state.paused) {
      return { ok: false, code: 'paused', error: 'release is paused; resume before advancing' };
    }
    if (this.state.pending) await this.recover();

    const next = this.nextAdvanceIndex();
    if (next === null) {
      return { ok: true, code: 'complete', blocked: [], state: this.viewBase() };
    }
    const batch = this.state.batches[next];
    if (!this.configs.get(batch.targetVersion)) {
      return {
        ok: false,
        code: 'missing_config',
        error: `config ${batch.targetVersion} is not published`,
        batchIndex: next,
      };
    }
    const versions: Record<GroupId, Version> = {
      ...(this.repo.loadBindings()?.versions ?? this.state.initialVersions),
    };
    for (const [group, version] of Object.entries(this.state.initialVersions)) {
      versions[group] = versions[group] ?? version;
    }
    // Record what each of this batch's groups runs *right now*; that is the
    // rollback target if this release is later reverted.
    batch.before = {};
    for (const group of batch.groups) batch.before[group] = versions[group] ?? batch.before[group];
    for (const group of batch.groups) versions[group] = batch.targetVersion;

    const missing = [...new Set(Object.values(versions))].filter(
      (version) => !this.configs.get(version),
    );
    if (missing.length > 0) {
      return {
        ok: false,
        code: 'missing_config',
        error: `configs not published: ${missing.join(', ')}`,
        batchIndex: next,
      };
    }

    // Preflight: install the target on the services this batch needs, then
    // verify. A service that is down or cannot execute the target blocks this
    // batch; later batches' services are never consulted.
    const blocked = await this.ensureServicesForBatch(batch, batch.targetVersion);
    if (blocked.length > 0) {
      batch.status = 'pending';
      this.saveState();
      return {
        ok: false,
        code: 'blocked',
        error: 'not every service required by this batch can execute the target version',
        batchIndex: next,
        blocked,
        state: this.viewBase(),
      };
    }

    const epoch = this.state.epochSeq + 1;
    const digests = this.digestsFor(versions);
    const previous = this.bindingsView();
    const pending: PendingSwitch = {
      kind: 'advance',
      batchIndex: next,
      epoch,
      versions,
      digests,
      previous,
    };

    // Durable intent BEFORE touching any entry.
    this.state.pending = pending;
    this.state.epochSeq = epoch;
    this.state.cursor = next;
    batch.status = 'switching';
    this.saveState();
    if (options.fault === 'after-pending') process.exit(97);

    const outcome = await this.pushSwitch(pending);
    if (!outcome.ok) {
      this.state.pending = null;
      batch.status = 'pending';
      this.saveState();
      return {
        ok: false,
        code: outcome.code,
        error: outcome.error,
        batchIndex: next,
        blocked: outcome.blocked,
        acks: outcome.acks,
        state: this.viewBase(),
      };
    }
    // Entries have committed; crash before the coordinator records it.
    if (options.fault === 'after-commit') process.exit(98);

    this.repo.saveBindings({ versions, digests, epoch, updatedAt: Date.now() });
    this.finishPending(pending);
    return {
      ok: true,
      batchIndex: next,
      blocked: [],
      acks: outcome.acks,
      state: this.viewBase(),
    };
  }

  pause(): ReleaseStateView {
    this.state.paused = true;
    this.saveState();
    return this.viewBase();
  }

  async resume(): Promise<ReleaseStateView> {
    this.state.paused = false;
    this.saveState();
    return this.viewBase();
  }

  requestRollback(batchIndex?: number): AdvanceOutcome {
    const target =
      batchIndex ??
      [...this.state.batches].reverse().find((batch) => batch.status === 'effective')?.index;
    if (target === undefined) {
      return { ok: false, code: 'nothing_to_rollback', error: 'no effective batch' };
    }
    const batch = this.state.batches[target];
    if (!batch || batch.status !== 'effective') {
      return { ok: false, code: 'batch_not_effective', error: `batch ${target} is not effective` };
    }
    if (this.state.rollback && this.state.rollback.status !== 'confirmed') {
      return { ok: false, code: 'rollback_exists', error: 'a rollback is already in progress' };
    }
    this.state.rollback = {
      batchIndex: target,
      status: 'requested',
      requestedAt: Date.now(),
    };
    this.saveState();
    return { ok: true, batchIndex: target, state: this.viewBase() };
  }

  /**
   * Confirm a requested rollback.
   *
   * Permitted while paused and while requests accepted under the batch are
   * still executing: those requests keep their pinned version. Only *new*
   * acceptances use the pre-batch (rollback) bindings.
   */
  async confirmRollback(): Promise<AdvanceOutcome> {
    const rollback = this.state.rollback;
    if (!rollback || rollback.status === 'confirmed') {
      return { ok: false, code: 'no_rollback', error: 'no rollback requested' };
    }
    const batch = this.state.batches[rollback.batchIndex];
    if (!batch) {
      return { ok: false, code: 'bad_batch', error: `unknown batch ${rollback.batchIndex}` };
    }

    rollback.status = 'confirming';
    rollback.inflightAtConfirm = await this.inflightCount();
    this.saveState();

    const versions: Record<GroupId, Version> = {
      ...(this.repo.loadBindings()?.versions ?? this.state.initialVersions),
    };
    for (const group of batch.groups) versions[group] = batch.before[group];

    const epoch = this.state.epochSeq + 1;
    const digests = this.digestsFor(versions);
    const pending: PendingSwitch = {
      kind: 'rollback',
      batchIndex: rollback.batchIndex,
      epoch,
      versions,
      digests,
      previous: this.bindingsView(),
    };
    this.state.pending = pending;
    this.state.epochSeq = epoch;
    this.saveState();

    // Restore the pre-batch rule set on exactly the services these groups use.
    for (const version of new Set(batch.groups.map((group) => batch.before[group]))) {
      const doc = this.configs.get(version);
      if (!doc) {
        this.state.pending = null;
        rollback.status = 'requested';
        this.saveState();
        return { ok: false, code: 'missing_config', error: `config ${version} is not published` };
      }
      const blocked = await this.installOnServices(doc, requiredServices(doc, batch.groups));
      if (blocked.length > 0) {
        this.state.pending = null;
        rollback.status = 'requested';
        this.saveState();
        return {
          ok: false,
          code: 'config_push_failed',
          error: `could not install ${version} on every service the rollback needs`,
          blocked,
        };
      }
    }

    const outcome = await this.pushSwitch(pending);
    if (!outcome.ok) {
      this.state.pending = null;
      rollback.status = 'requested';
      this.saveState();
      return {
        ok: false,
        code: outcome.code,
        error: outcome.error,
        blocked: outcome.blocked,
        acks: outcome.acks,
      };
    }

    this.repo.saveBindings({ versions, digests, epoch, updatedAt: Date.now() });
    this.finishPending(pending);
    // Later effective batches must be explicitly re-advanced after a rollback.
    for (const other of this.state.batches) {
      if (other.index > rollback.batchIndex && other.status === 'effective') {
        other.note = 'must be explicitly re-advanced after rollback of an earlier batch';
      }
    }
    this.saveState();
    return {
      ok: true,
      batchIndex: rollback.batchIndex,
      blocked: [],
      acks: outcome.acks,
      state: this.viewBase(),
    };
  }

  /* -------------------------------- internals ------------------------------ */

  /**
   * The batch advance() would act on: the first batch that is not yet effective
   * in plan order. A rolled-back batch becomes eligible again, which is how an
   * operator re-releases a batch after a rollback.
   */
  private nextAdvanceIndex(): number | null {
    const index = this.state.batches.findIndex((batch) => batch.status !== 'effective');
    return index === -1 ? null : index;
  }

  private digestsFor(versions: Record<GroupId, Version>): Record<Version, string> {
    const digests: Record<Version, string> = {};
    for (const version of new Set(Object.values(versions))) {
      const meta = this.configs.meta(version);
      if (meta) digests[version] = meta.digest;
    }
    return digests;
  }

  private serviceUrl(name: string): string | undefined {
    const service = this.topology.services.find((candidate) => candidate.name === name);
    return service ? `http://127.0.0.1:${service.port}` : undefined;
  }

  /** Install one doc on the given services; returns the failures. */
  private async installOnServices(
    doc: ConfigDocument,
    names: ServiceName[],
  ): Promise<BlockedReason[]> {
    const meta = this.configs.meta(doc.version);
    const blocked: BlockedReason[] = [];
    await Promise.all(
      names.map(async (name) => {
        const url = this.serviceUrl(name);
        if (!url) {
          blocked.push({ service: name, reason: 'not present in topology' });
          return;
        }
        const installed = await this.peers.installConfig(url, doc, meta?.digest);
        if (!installed.ok) {
          blocked.push({ service: name, reason: installed.error ?? 'install failed' });
        }
      }),
    );
    blocked.sort((a, b) => a.service.localeCompare(b.service));
    return blocked;
  }

  /**
   * Ensure every service this batch's groups need is reachable and can execute
   * `version`: install the doc, then re-verify. Services used only by other
   * batches are never touched, so their downtime cannot stall this batch.
   */
  async ensureServicesForBatch(batch: BatchState, version: Version): Promise<BlockedReason[]> {
    const doc = this.configs.get(version);
    if (!doc) return [{ service: '*', reason: `config ${version} is not published` }];
    const names = requiredServices(doc, batch.groups);
    const meta = this.configs.meta(version);
    const blocked: BlockedReason[] = [];
    await Promise.all(
      names.map(async (name) => {
        const url = this.serviceUrl(name);
        if (!url) {
          blocked.push({ service: name, reason: 'not present in topology' });
          return;
        }
        const before = await this.peers.serviceInfo(url);
        if (!before.ok) {
          blocked.push({ service: name, reason: `unreachable: ${before.error ?? 'no response'}` });
          return;
        }
        const installed = await this.peers.installConfig(url, doc, meta?.digest);
        if (!installed.ok) {
          blocked.push({ service: name, reason: `install failed: ${installed.error}` });
          return;
        }
        const after = await this.peers.serviceInfo(url);
        if (!after.ok) {
          blocked.push({ service: name, reason: `unreachable after install: ${after.error}` });
          return;
        }
        if (!after.versions.includes(version)) {
          blocked.push({
            service: name,
            reason: `cannot execute ${version} (has ${after.versions.join(', ') || 'none'})`,
          });
        }
      }),
    );
    blocked.sort((a, b) => a.service.localeCompare(b.service));
    return blocked;
  }

  /**
   * Push bindings to reachable entries via prepare → commit.
   *
   * Service-level config installation is done by the caller (it differs
   * between advance and rollback); this step only flips entry bindings, which
   * is the shared batch boundary.
   */
  private async pushSwitch(pending: PendingSwitch): Promise<{
    ok: boolean;
    code?: string;
    error?: string;
    blocked?: BlockedReason[];
    acks: EntryAck[];
  }> {

    const reachable = await this.reachableEntries();
    if (reachable.length === 0) {
      return { ok: false, code: 'entries_unreachable', error: 'no entry is reachable', acks: [] };
    }

    const payload = {
      epoch: pending.epoch,
      versions: pending.versions,
      digests: pending.digests,
    };
    const acks: EntryAck[] = [];
    const prepareFailures: string[] = [];
    await Promise.all(
      reachable.map(async (entry) => {
        const url = `http://127.0.0.1:${entry.port}`;
        let lastError = 'prepare failed';
        for (let attempt = 0; attempt < 4; attempt += 1) {
          const result = await this.peers.prepare(url, payload);
          if (result.ok) {
            const committed = await this.peers.commit(url, pending.epoch);
            if (committed.ok) {
              const observed = await this.peers.bindings(url);
              acks.push({ entry: entry.name, ok: true, epoch: observed.epoch });
              return;
            }
            lastError = committed.error ?? 'commit failed';
          } else {
            lastError = result.error ?? 'prepare failed';
          }
          await sleep(80);
        }
        prepareFailures.push(`${entry.name} (${lastError})`);
        acks.push({ entry: entry.name, ok: false, error: lastError });
      }),
    );

    if (prepareFailures.length === reachable.length) {
      return {
        ok: false,
        code: 'entries_failed',
        error: `no entry accepted epoch ${pending.epoch}: ${prepareFailures.join(', ')}`,
        acks,
      };
    }
    if (prepareFailures.length > 0) {
      return {
        ok: false,
        code: 'entry_prepare_failed',
        error: `entries failed to switch: ${prepareFailures.join(', ')}`,
        acks,
      };
    }
    return { ok: true, acks };
  }

  /** Re-assert bindings (recovery / entry catch-up); best-effort. */
  private async pushBindings(bindings: BindingsView): Promise<string[]> {
    const reachable = await this.reachableEntries();
    const failures: string[] = [];
    await Promise.all(
      reachable.map(async (entry) => {
        const url = `http://127.0.0.1:${entry.port}`;
        const prepared = await this.peers.prepare(url, {
          epoch: bindings.epoch,
          versions: bindings.versions,
          digests: bindings.digests,
        });
        if (!prepared.ok) {
          failures.push(entry.name);
          return;
        }
        const committed = await this.peers.commit(url, bindings.epoch);
        if (!committed.ok) failures.push(entry.name);
      }),
    );
    return failures.sort();
  }

  private async reachableEntries(): Promise<EntryProcessDef[]> {
    const results = await Promise.all(
      this.topology.entries.map(async (entry) => {
        const res = await jsonRequest(`http://127.0.0.1:${entry.port}`, {
          path: '/health',
          timeoutMs: 500,
        });
        return res.ok ? entry : undefined;
      }),
    );
    return results.filter((entry): entry is EntryProcessDef => Boolean(entry));
  }

  private async inflightCount(): Promise<number> {
    let total = 0;
    await Promise.all(
      this.topology.entries.map(async (entry) => {
        const res = await this.peers.inflight(`http://127.0.0.1:${entry.port}`);
        if (res.ok) total += res.inflight;
      }),
    );
    return total;
  }

  /* -------------------------------- probing -------------------------------- */

  async probeEntries(): Promise<EntryBindingView[]> {
    return Promise.all(
      this.topology.entries.map(async (entry) => {
        const url = `http://127.0.0.1:${entry.port}`;
        const res = await jsonRequest<{
          epoch: number;
          versions: Record<GroupId, Version>;
          cachedVersions: Version[];
          inflight: number;
        }>(url, { path: '/admin/bindings?detail=1', timeoutMs: 800 });
        if (!res.ok || !res.body) {
          return {
            name: entry.name,
            reachable: false,
            epoch: null,
            versions: {},
            cachedVersions: [],
            error: res.error ?? `status ${res.status}`,
          };
        }
        return {
          name: entry.name,
          reachable: true,
          epoch: res.body.epoch,
          versions: res.body.versions ?? {},
          cachedVersions: res.body.cachedVersions ?? [],
          inflight: res.body.inflight ?? 0,
        };
      }),
    );
  }

  async probeServices(): Promise<ServiceReadinessView[]> {
    return Promise.all(
      this.topology.services.map(async (service) => {
        const url = `http://127.0.0.1:${service.port}`;
        const res = await this.peers.serviceInfo(url);
        return { name: service.name, reachable: res.ok, versions: res.versions, error: res.error };
      }),
    );
  }

  /** Entry-reported state for the groups of one batch, plus a settled flag. */
  observedBatchView(
    batch: BatchState,
    entries: EntryBindingView[],
  ): { effective: Record<GroupId, Version>; settled: boolean } {
    const effective: Record<GroupId, Version> = { ...batch.effective };
    let targetVotes = 0;
    let otherVotes = 0;
    for (const entry of entries) {
      if (!entry.reachable) continue;
      let allTarget = true;
      let sawAny = false;
      for (const group of batch.groups) {
        const version = entry.versions[group];
        if (version === undefined) continue;
        sawAny = true;
        effective[group] = version;
        if (version !== batch.targetVersion) allTarget = false;
      }
      if (!sawAny) continue;
      if (allTarget) targetVotes += 1;
      else otherVotes += 1;
    }
    return { effective, settled: targetVotes > 0 && otherVotes === 0 };
  }

  putConfig(doc: ConfigDocument): { ok: boolean; errors?: string[]; digest?: string } {
    const errors = validateConfigDocument(doc);
    if (errors.length > 0) return { ok: false, errors };
    const meta = this.configs.publish(doc);
    return { ok: true, digest: meta.digest };
  }

  /** Services referenced by every group's current route — used by status. */
  referencedServices(): ServiceName[] {
    const names = new Set<ServiceName>();
    const bindings = this.bindingsView();
    for (const [group, version] of Object.entries(bindings.versions)) {
      const doc = this.configs.get(version);
      const route = doc?.routes[group];
      if (!route) continue;
      walkRoute(route, (step) => names.add(step.service));
    }
    return [...names].sort();
  }

  lastError(err: unknown): string {
    return errorMessage(err);
  }

  currentRollback(): RollbackState | null {
    return this.state.rollback;
  }

  /**
   * Aggregate durable transport accounting from every process. Each entry and
   * service accounts the full-config bytes that crossed into it; the sum is
   * the cluster-wide total the invariant is stated over.
   */
  async transportSummary(): Promise<{
    configBytes: number;
    perProcess: Record<string, number>;
    versions: Record<string, number>;
  }> {
    const perProcess: Record<string, number> = {};
    const versions: Record<string, number> = {};

    const own = new TransportLedger(this.repo.store).summary();
    perProcess.coordinator = own.configBytes;
    for (const [version, bytes] of Object.entries(own.byVersion)) {
      versions[version] = (versions[version] ?? 0) + bytes;
    }

    const squeezes = [
      ...this.topology.entries.map((entry) => ({
        name: `entry:${entry.name}`,
        url: `http://127.0.0.1:${entry.port}`,
      })),
      ...this.topology.services.map((service) => ({
        name: `service:${service.name}`,
        url: `http://127.0.0.1:${service.port}`,
      })),
    ];
    await Promise.all(
      squeezes.map(async ({ name, url }) => {
        const res = await jsonRequest<{
          transport: { configBytes: number; versionsFetched?: Record<string, number> };
        }>(url, { path: '/admin/transport', timeoutMs: 1000 });
        if (!res.ok || !res.body?.transport) return;
        perProcess[name] = res.body.transport.configBytes;
        for (const [version, bytes] of Object.entries(res.body.transport.versionsFetched ?? {})) {
          versions[version] = (versions[version] ?? 0) + bytes;
        }
      }),
    );

    const configBytes = Object.values(perProcess).reduce((sum, value) => sum + value, 0);
    return { configBytes, perProcess, versions };
  }

  /**
   * Best-effort boot step: make sure every service can execute the versions
   * currently bound for the groups whose routes use it. Idempotent and safe to
   * call when some services are still starting.
   */
  async primeServices(): Promise<void> {
    const bindings = this.bindingsView();
    const seen = new Set<string>();
    for (const [group, version] of Object.entries(bindings.versions)) {
      const doc = this.configs.get(version);
      if (!doc) continue;
      const key = `${group}:${version}`;
      if (seen.has(key)) continue;
      seen.add(key);
      const names = requiredServices(doc, [group]);
      await this.installOnServices(doc, names);
    }
  }
}
