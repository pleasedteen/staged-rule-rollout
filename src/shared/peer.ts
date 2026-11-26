import type {
  BindingsView,
  ConfigDocument,
  GroupId,
  ReleaseStateView,
  Version,
} from './types';
import { jsonRequest } from './http';

export interface BindingsResponse {
  epoch: number;
  versions: Record<GroupId, Version>;
  digests?: Record<Version, string>;
  cachedVersions?: Version[];
}

export interface InflightResponse {
  inflight: number;
  accepted: number;
}

/**
 * Coordinator → entry / service control-plane client.
 *
 * Plain JSON calls with short timeouts; no hidden retries. Retry policy lives
 * in the coordinator so every attempt is visible in release state.
 */
export class PeerClient {
  constructor(private readonly timeoutMs = 1500) {}

  async bindings(url: string): Promise<{ ok: boolean; epoch?: number; versions?: Record<GroupId, Version>; cachedVersions?: Version[]; error?: string }> {
    const res = await jsonRequest<BindingsResponse>(url, {
      path: '/admin/bindings?detail=1',
      timeoutMs: this.timeoutMs,
    });
    if (!res.ok || !res.body) return { ok: false, error: res.error ?? `status ${res.status}` };
    return {
      ok: true,
      epoch: res.body.epoch,
      versions: res.body.versions ?? {},
      cachedVersions: res.body.cachedVersions ?? [],
    };
  }

  async prepare(
    url: string,
    payload: { epoch: number; versions: Record<GroupId, Version>; digests: Record<Version, string> },
  ): Promise<{ ok: boolean; error?: string }> {
    const res = await jsonRequest<{ ok: boolean; error?: string }>(url, {
      path: '/admin/prepare',
      body: payload,
      timeoutMs: this.timeoutMs,
    });
    if (!res.ok || !res.body?.ok) {
      return { ok: false, error: res.body?.error ?? res.error ?? `status ${res.status}` };
    }
    return { ok: true };
  }

  async commit(url: string, epoch: number): Promise<{ ok: boolean; error?: string }> {
    const res = await jsonRequest<{ ok: boolean; error?: string }>(url, {
      path: '/admin/commit',
      body: { epoch },
      timeoutMs: this.timeoutMs,
    });
    if (!res.ok || !res.body?.ok) {
      return { ok: false, error: res.body?.error ?? res.error ?? `status ${res.status}` };
    }
    return { ok: true };
  }

  async installConfig(
    url: string,
    doc: ConfigDocument,
    digest?: string,
    source?: string,
  ): Promise<{ ok: boolean; cached?: boolean; error?: string }> {
    const res = await jsonRequest<{ ok: boolean; cached?: boolean; error?: string }>(url, {
      path: '/admin/config',
      body: { doc, digest, source },
      timeoutMs: this.timeoutMs,
    });
    if (!res.ok || !res.body?.ok) {
      return { ok: false, error: res.body?.error ?? res.error ?? `status ${res.status}` };
    }
    return { ok: true, cached: res.body.cached };
  }

  async serviceInfo(url: string): Promise<{ ok: boolean; versions: Version[]; error?: string }> {
    const res = await jsonRequest<{ versions: Version[] }>(url, {
      path: '/admin/versions',
      timeoutMs: this.timeoutMs,
    });
    if (!res.ok || !res.body) {
      return { ok: false, versions: [], error: res.error ?? `status ${res.status}` };
    }
    return { ok: true, versions: res.body.versions ?? [] };
  }

  async inflight(url: string): Promise<{ ok: boolean; inflight: number; error?: string }> {
    const res = await jsonRequest<InflightResponse>(url, {
      path: '/admin/inflight',
      timeoutMs: this.timeoutMs,
    });
    if (!res.ok || !res.body) return { ok: false, inflight: 0, error: res.error };
    return { ok: true, inflight: res.body.inflight ?? 0 };
  }

  async state(url: string, detail = false): Promise<ReleaseStateView | undefined> {
    const res = await jsonRequest<ReleaseStateView>(url, {
      path: detail ? '/admin/state?detail=1' : '/admin/state',
      timeoutMs: this.timeoutMs,
    });
    return res.ok ? res.body : undefined;
  }

  bindingsView(epoch: number, versions: Record<GroupId, Version>): BindingsView {
    return { epoch, versions, digests: {}, updatedAt: Date.now() };
  }
}
