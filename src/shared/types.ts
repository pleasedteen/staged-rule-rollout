/**
 * Shared protocol types for the local rule-release middleware.
 *
 * Core invariant: a request is bound to exactly one configuration version at
 * first acceptance. That version travels with every hop, branch and retry, so
 * every execution segment interprets its input with the same rule set.
 */

export type GroupId = string;
export type ServiceName = string;
export type Version = string;

export interface ServiceProcessDef {
  name: ServiceName;
  port: number;
  /** Informational: groups whose routes reference this service. */
  groups: GroupId[];
}

export interface EntryProcessDef {
  name: string;
  port: number;
  /** Traffic groups this entry accepts requests for. */
  groups: GroupId[];
}

export interface Topology {
  coordinator: { port: number };
  entries: EntryProcessDef[];
  services: ServiceProcessDef[];
}

/** One batch of a release plan. All groups in a batch cut over together. */
export interface BatchPlan {
  name: string;
  groups: GroupId[];
  targetVersion: Version;
}

export interface ReleasePlan {
  releaseId: string;
  batches: BatchPlan[];
}

/** Plan fixture: plan + initial binding + directory of config documents. */
export interface PlanFile extends ReleasePlan {
  initialVersions: Record<GroupId, Version>;
  configsDir?: string;
}

export type BatchStatus = 'pending' | 'switching' | 'effective' | 'rolled_back';

export interface BatchState {
  index: number;
  name: string;
  groups: GroupId[];
  targetVersion: Version;
  /** Version per group as actually effective. */
  effective: Record<GroupId, Version>;
  /** Version effective immediately before this batch's commit (rollback target). */
  before: Record<GroupId, Version>;
  status: BatchStatus;
  note?: string;
  updatedAt: number;
}

/**
 * A durable release operation that may have partially landed.
 *
 * `advance` switches a batch forward; `rollback` restores `before` versions.
 * Both carry the complete post-switch binding map so recovery can verify —
 * not guess — what the entries ended up running.
 */
export type ReleaseOpKind = 'advance' | 'rollback';

export interface ReleaseOp {
  kind: ReleaseOpKind;
  batchIndex: number;
  epoch: number;
  versions: Record<GroupId, Version>;
  digests: Record<Version, string>;
}

export type ReleaseStatus = 'releasing' | 'paused' | 'complete' | 'rolled_back';

export type RollbackStatus = 'requested' | 'confirming' | 'confirmed';

export interface RollbackState {
  batchIndex: number;
  status: RollbackStatus;
  requestedAt: number;
  confirmedAt?: number;
  /** Requests still executing when confirmation landed (they keep their pin). */
  inflightAtConfirm?: number;
}

/** Durable two-phase switch intent, written before any entry is touched. */
export interface PendingSwitch {
  kind: 'advance' | 'rollback';
  batchIndex: number;
  epoch: number;
  /** Complete group→version map entries must run once committed. */
  versions: Record<GroupId, Version>;
  digests: Record<Version, string>;
  /** Bindings in force before this switch; used to abort cleanly. */
  previous: BindingsView;
}

export interface CoordinatorState {
  plan: ReleasePlan;
  batches: BatchState[];
  /** Last batch index the coordinator acted on. */
  cursor: number;
  paused: boolean;
  rollback: RollbackState | null;
  rollbackHistory: RollbackState[];
  initialVersions: Record<GroupId, Version>;
  pending: PendingSwitch | null;
  /** Last epoch handed out; monotonic across restarts. */
  epochSeq: number;
  seq: number;
  updatedAt: number;
  recoveries: number;
  lastRecovery: string | null;
}

export interface BindingsView {
  versions: Record<GroupId, Version>;
  digests: Record<Version, string>;
  /** Monotonic boundary id; all groups in a batch flip at one epoch. */
  epoch: number;
  updatedAt: number;
}

export type ServiceOp = 'double' | 'add' | 'concat' | 'tag' | 'identity';
export type JoinOp = 'sum' | 'concat' | 'list';

export interface ServiceConfig {
  op: ServiceOp;
  operand?: number | string;
  /** Simulated processing time in ms. */
  delayMs?: number;
}

/**
 * Route node: run `service` on the payload, then (optionally) run every branch
 * child in parallel on that output and join their final outputs, then
 * (optionally) feed the joined value into `next`.
 */
export interface RouteStep {
  service: ServiceName;
  branch?: RouteStep[];
  join?: JoinOp;
  next?: RouteStep;
}

export interface ConfigDocument {
  version: Version;
  services: Record<ServiceName, ServiceConfig>;
  routes: Record<GroupId, RouteStep>;
  notes?: string;
}

/* ------------------------------- Request path ------------------------------ */

export interface SegmentResult {
  service: ServiceName;
  instance: string;
  /** Version actually used by this segment; must equal the pinned version. */
  version: Version;
  group: GroupId;
  slot: string;
  input: unknown;
  output: unknown;
  ms: number;
  /** True when the segment needed a transport retry (e.g. service restart). */
  recovered: boolean;
}

export type RequestStatus = 'accepted' | 'running' | 'done' | 'failed';

export interface RequestRecord {
  requestId: string;
  /** Version pinned at first acceptance; never changes. */
  version: Version;
  group: GroupId;
  entry: string;
  /** Binding epoch observed at first acceptance (shared batch boundary). */
  bindingEpoch: number | null;
  input: unknown;
  output?: unknown;
  segments: SegmentResult[];
  status: RequestStatus;
  path: ServiceName[];
  acceptedBy: string;
  acceptedAt: number;
  startedAt?: number;
  finishedAt?: number;
  /** Count of cross-entry resume attempts. */
  retries: number;
  /** Entry that actually completed the record, if different from acceptor. */
  resumedBy?: string;
  /** True if any segment executed under a version other than the pinned one. */
  skewDetected?: boolean;
  error?: string;
  errorCode?: string;
}

export interface RequestReceipt {
  ok: true;
  requestId: string;
  version: Version;
  group: GroupId;
  acceptedBy: string;
  acceptedAt: number;
  entry: string;
  bindingEpoch: number | null;
  retried: boolean;
}

export interface ApiError {
  ok: false;
  error: string;
  code: string;
  details?: unknown;
}

export interface RequestLedgerEntry {
  requestId: string;
  version: Version;
  group: GroupId;
  acceptedBy: string;
  acceptedAt: number;
  bindingEpoch: number | null;
  path: ServiceName[];
  /** The original input, so any entry can resume without the request record. */
  input: unknown;
}

/* ------------------------------- Execution -------------------------------- */

export interface ExecuteResponse {
  ok: boolean;
  service: ServiceName;
  instance: string;
  version: Version;
  output?: unknown;
  ms: number;
  recovered: boolean;
  error?: string;
  code?: string;
}

/* -------------------------------- Progress -------------------------------- */

/**
 * Segment progress travels on a side channel, separate from execution.
 *
 * The execution service remains the sole owner of its write-ahead journal; it
 * only *publishes* completion events. The entry publishes the matching receipt
 * once it has received a segment result. Both producers keep a durable local
 * outbox so a temporarily unreachable coordinator cannot lose evidence.
 */
export type ProgressStage = 'service_done' | 'entry_receipt';
export type ProgressRole = 'service' | 'entry';

export interface ProgressEvent {
  /** Stable id of this report; re-delivery of the same id is idempotent. */
  eventId: string;
  stage: ProgressStage;
  role: ProgressRole;
  /** Process name that produced the event (e.g. `fx-tag`, `entry-a`). */
  producer: string;
  /** Boot instance of the producer. */
  instance: string;
  requestId: string;
  slot: string;
  service: ServiceName;
  /** Version the evidence proves was actually executed / received. */
  version: Version;
  at: number;
  /** Per-producer monotonic sequence (tie-breaker for late reports). */
  seq: number;
  /** Entry that received the segment (receipt stage). */
  entry?: string;
  /** Service answered from a journaled result after a restart/retry. */
  recovered?: boolean;
  /** Result was produced by boot-time journal healing. */
  healed?: boolean;
  /** Completion succeeded (false means the rule itself errored). */
  ok?: boolean;
  ms?: number;
}

export interface StoredServiceEvidence {
  version: Version;
  instance: string;
  producer: string;
  at: number;
  recovered: boolean;
  healed: boolean;
  ok: boolean;
  ms?: number;
}

export interface StoredReceiptEvidence {
  version: Version;
  entry: string;
  instance: string;
  at: number;
}

export interface StoredProgressSegment {
  slot: string;
  service: ServiceName;
  /** First-seen display order (expected route order takes precedence). */
  order: number;
  firstSeenAt: number;
  serviceCompleted?: StoredServiceEvidence;
  entryReceipt?: StoredReceiptEvidence;
  /** Evidence for one slot disagreed on version; first version is kept. */
  conflict?: boolean;
}

export interface StoredProgressRecord {
  requestId: string;
  segments: Record<string, StoredProgressSegment>;
  nextOrder: number;
  updatedAt: number;
  events: number;
}

export interface ProgressEvidenceView {
  observed: boolean;
  version: Version | null;
  at: number | null;
  instance?: string | null;
  producer?: string | null;
  entry?: string | null;
  recovered?: boolean;
  healed?: boolean;
  ok?: boolean;
}

export type ProgressSegmentState =
  | 'pending'
  | 'service_done_unacked'
  | 'receipt_only'
  | 'confirmed';

export interface ProgressSegmentView {
  slot: string;
  service: ServiceName;
  order: number;
  /** Service-side completion evidence (execution service). */
  serviceCompleted: ProgressEvidenceView;
  /** Entry-side acknowledgement that it received the segment result. */
  entryReceipt: ProgressEvidenceView;
  /** Version proven by evidence (service first, then receipt); null on a gap. */
  actualVersion: Version | null;
  /** Version the authoritative request record reports for this slot, if any. */
  recordVersion: Version | null;
  gaps: ('service_completion' | 'entry_receipt')[];
  state: ProgressSegmentState;
  conflict?: boolean;
}

export interface ProgressView {
  /** `interrupted` means the side channel rejected reports at query time. */
  channel: 'flowing' | 'interrupted';
  expected: number;
  /** Version that the observed evidence proves was actually executed. */
  actualVersion: Version | null;
  segments: ProgressSegmentView[];
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

/** Accepted-pin metadata straight from the shared ledger. */
export interface AcceptedPinView {
  version: Version;
  group: GroupId;
  acceptedBy: string;
  acceptedAt: number;
  bindingEpoch: number | null;
  path: ServiceName[];
}

/**
 * `/admin/records` item: the original record fields (when available) plus
 * separately queryable per-segment progress evidence.
 */
export interface RecordProgressView {
  requestId: string;
  recordAvailable: boolean;
  /** True when neither the entry, cluster record nor ledger knows the id. */
  notFound?: boolean;
  version?: Version;
  group?: GroupId;
  status?: RequestStatus | 'unknown';
  output?: unknown;
  segments?: SegmentResult[];
  retries?: number;
  error?: string;
  accepted: AcceptedPinView | null;
  progress: ProgressView;
}

/* --------------------------------- Release -------------------------------- */

export interface BlockedReason {
  service: string;
  reason: string;
}

export interface EntryAck {
  entry: string;
  ok: boolean;
  epoch?: number;
  error?: string;
}

export interface BatchView {
  index: number;
  name: string;
  groups: GroupId[];
  targetVersion: Version;
  status: BatchStatus;
  effective: Record<GroupId, Version>;
  before: Record<GroupId, Version>;
  note?: string;
}

export interface EntryBindingView {
  name: string;
  reachable: boolean;
  epoch: number | null;
  versions: Record<GroupId, Version>;
  cachedVersions: Version[];
  inflight?: number;
  error?: string;
}

export interface ServiceReadinessView {
  name: string;
  reachable: boolean;
  versions: Version[];
  error?: string;
}

export interface ReleaseStateView {
  releaseId: string;
  status: ReleaseStatus;
  paused: boolean;
  cursor: number;
  seq: number;
  batches: BatchView[];
  bindings: BindingsView;
  rollback: RollbackState | null;
  /** Most recent completed rollbacks (newest last). */
  rollbackHistory: RollbackState[];
  recovered: boolean;
  recoveries: number;
  lastRecovery: string | null;
  updatedAt: number;
  entries?: EntryBindingView[];
  services?: ServiceReadinessView[];
  store?: { kind: 'sqlite' | 'file'; location: string; cluster?: 'file' };
}

export interface AdvanceOutcome {
  ok: boolean;
  code?: string;
  error?: string;
  batchIndex?: number;
  blocked?: BlockedReason[];
  acks?: EntryAck[];
  state?: ReleaseStateView;
}
