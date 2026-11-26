import type {
  ConfigDocument,
  GroupId,
  ProgressEvent,
  ProgressEvidenceView,
  ProgressRole,
  ProgressSegmentState,
  ProgressSegmentView,
  ProgressView,
  SegmentResult,
  StoredProgressRecord,
} from './types';
import type { KvStore } from './store';
import { routeSlots, type RouteSlot } from './engine';
import { jsonRequest } from './http';

/**
 * Segment-progress side channel.
 *
 * Execution never depends on this path. Producers (entries and execution
 * services) durably enqueue events in a local outbox and drain it best-effort;
 * the coordinator is the single aggregate. If the coordinator cannot accept
 * reports, business results still flow and the outbox survives a producer
 * restart so evidence can be backfilled later.
 */

const OUTBOX_KEY = 'progress:outbox';
const SEQ_KEY = 'progress:seq';
const MAX_OUTBOX = 4000;

interface EmitFields {
  stage: ProgressEvent['stage'];
  instance: string;
  requestId: string;
  slot: string;
  service: string;
  version: string;
  at?: number;
  entry?: string;
  recovered?: boolean;
  healed?: boolean;
  ok?: boolean;
  ms?: number;
}

/**
 * Durable local outbox held by one producer process.
 *
 * `emit` persists before returning, so a `kill -9` right afterwards cannot
 * lose the evidence. `flush` removes only events the sink acknowledged;
 * re-delivery is harmless because the coordinator is idempotent per
 * (requestId, slot).
 */
export class ProgressOutbox {
  private flushing = false;
  private timer?: NodeJS.Timeout;
  private stopped = false;

  constructor(
    private readonly local: KvStore,
    private readonly coordinatorUrl: string,
    private readonly producer: string,
    private readonly role: ProgressRole,
  ) {}

  private read(): ProgressEvent[] {
    return this.local.get<ProgressEvent[]>(OUTBOX_KEY) ?? [];
  }

  emit(fields: EmitFields): ProgressEvent {
    const seq = (this.local.get<number>(SEQ_KEY) ?? 0) + 1;
    this.local.set(SEQ_KEY, seq);
    const event: ProgressEvent = {
      ...fields,
      at: fields.at ?? Date.now(),
      eventId: `${this.producer}:${seq}`,
      seq,
      producer: this.producer,
      role: this.role,
    };
    const outbox = this.read();
    outbox.push(event);
    if (outbox.length > MAX_OUTBOX) outbox.splice(0, outbox.length - MAX_OUTBOX);
    this.local.set(OUTBOX_KEY, outbox);
    void this.flush();
    return event;
  }

  pending(): number {
    return this.read().length;
  }

  start(intervalMs = 250): void {
    if (this.timer) return;
    this.stopped = false;
    this.timer = setInterval(() => void this.flush(), intervalMs);
    this.timer.unref?.();
    void this.flush();
  }

  stop(): void {
    this.stopped = true;
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
  }

  /** Deliver a batch; returns true when the sink accepted it. */
  async flush(): Promise<boolean> {
    if (this.flushing || this.stopped) return false;
    const snapshot = this.read();
    if (snapshot.length === 0) return true;
    this.flushing = true;
    try {
      const batch = snapshot.slice(0, 200);
      const res = await jsonRequest(this.coordinatorUrl, {
        path: '/admin/progress',
        body: { events: batch },
        timeoutMs: 1200,
      });
      if (!res.ok) return false;
      const delivered = new Set(batch.map((event) => event.eventId));
      const remaining = this.read().filter((event) => !delivered.has(event.eventId));
      this.local.set(OUTBOX_KEY, remaining);
      return true;
    } finally {
      this.flushing = false;
    }
  }
}

/** Route slots for the pinned route, used to know which segments to expect. */
export function expectedRouteSlots(
  doc: ConfigDocument | undefined,
  group: GroupId,
): RouteSlot[] {
  const route = doc?.routes[group];
  return route ? routeSlots(route) : [];
}

const missingEvidence = (version: string | null): ProgressEvidenceView => ({
  observed: false,
  version,
  at: null,
});

/**
 * Assemble the per-request progress view from the durable aggregate.
 *
 * Every expected route slot is listed even before any evidence arrives, so a
 * running request shows each segment separately: service completion, entry
 * receipt, the version the evidence proves, and the gaps that remain.
 */
export function buildProgressView(args: {
  stored: StoredProgressRecord | undefined;
  expected: RouteSlot[];
  recordSegments?: SegmentResult[];
  /** Coordinator is refusing reports, so the evidence path is interrupted. */
  ingestPaused?: boolean;
}): ProgressView {
  const { stored, expected, recordSegments, ingestPaused = false } = args;

  const order: string[] = [];
  const serviceBySlot = new Map<string, string>();
  const expectedSet = new Set<string>();
  for (const slot of expected) {
    if (serviceBySlot.has(slot.slot)) continue;
    order.push(slot.slot);
    serviceBySlot.set(slot.slot, slot.service);
    expectedSet.add(slot.slot);
  }
  const storedSegments = stored
    ? Object.values(stored.segments).sort((a, b) => a.order - b.order)
    : [];
  for (const segment of storedSegments) {
    if (serviceBySlot.has(segment.slot)) continue;
    order.push(segment.slot);
    serviceBySlot.set(segment.slot, segment.service);
  }
  const recordBySlot = new Map<string, SegmentResult>();
  for (const segment of recordSegments ?? []) {
    recordBySlot.set(segment.slot, segment);
    if (!serviceBySlot.has(segment.slot)) {
      order.push(segment.slot);
      serviceBySlot.set(segment.slot, segment.service);
    }
  }

  const segments: ProgressSegmentView[] = [];
  const gaps: string[] = [];
  const observedVersions = new Set<string>();
  let serviceCompletedCount = 0;
  let receiptCount = 0;
  let confirmedCount = 0;
  let withGaps = 0;

  for (const slot of order) {
    const storedSegment = stored?.segments[slot];
    const recordSegment = recordBySlot.get(slot);
    const service = serviceBySlot.get(slot) ?? 'unknown';
    const completion = storedSegment?.serviceCompleted;
    const receipt = storedSegment?.entryReceipt;

    const serviceCompleted: ProgressEvidenceView = completion
      ? {
          observed: true,
          version: completion.version,
          at: completion.at,
          instance: completion.instance,
          producer: completion.producer,
          recovered: completion.recovered,
          healed: completion.healed,
          ok: completion.ok,
        }
      : missingEvidence(null);
    const entryReceipt: ProgressEvidenceView = receipt
      ? {
          observed: true,
          version: receipt.version,
          at: receipt.at,
          instance: receipt.instance,
          entry: receipt.entry,
        }
      : missingEvidence(null);

    const segGaps: ('service_completion' | 'entry_receipt')[] = [];
    if (!completion) {
      segGaps.push('service_completion');
      gaps.push(`${slot}: service completion not observed`);
    }
    if (!receipt) {
      segGaps.push('entry_receipt');
      gaps.push(`${slot}: entry receipt not observed`);
    }
    if (segGaps.length > 0) withGaps += 1;

    const state: ProgressSegmentState =
      completion && receipt
        ? 'confirmed'
        : completion
          ? 'service_done_unacked'
          : receipt
            ? 'receipt_only'
            : 'pending';

    const evidenceVersion = completion?.version ?? receipt?.version ?? null;
    if (evidenceVersion) observedVersions.add(evidenceVersion);
    if (completion) serviceCompletedCount += 1;
    if (receipt) receiptCount += 1;
    if (state === 'confirmed') confirmedCount += 1;

    segments.push({
      slot,
      service,
      order: storedSegment?.order ?? segments.length,
      serviceCompleted,
      entryReceipt,
      actualVersion: evidenceVersion,
      recordVersion: recordSegment?.version ?? null,
      gaps: segGaps,
      state,
      conflict: storedSegment?.conflict,
    });
  }

  const channelInterrupted = ingestPaused;
  if (channelInterrupted) {
    gaps.push('progress channel interrupted; reports are buffered by producers');
  }

  // The version execution evidence actually proves. Only meaningful when it is
  // unambiguous; mixed/unproven evidence yields null rather than a guess.
  const actualVersion =
    observedVersions.size === 1 ? [...observedVersions][0] : null;

  return {
    channel: channelInterrupted ? 'interrupted' : 'flowing',
    expected: expected.length,
    actualVersion,
    segments,
    summary: {
      total: segments.length,
      serviceCompleted: serviceCompletedCount,
      receiptsObserved: receiptCount,
      confirmed: confirmedCount,
      withGaps,
      complete:
        segments.length > 0 &&
        segments.length >= expectedSet.size &&
        confirmedCount === segments.length,
    },
    gaps: [...new Set(gaps)],
    updatedAt: stored?.updatedAt ?? null,
  };
}
