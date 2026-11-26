import type { ConfigDocument, Version } from '../shared/types';
import { jsonRequest } from '../shared/http';
import { ReleaseController } from './controller';

export interface CoordinatorServerOptions {
  controller: ReleaseController;
  storeInfo: { kind: 'sqlite' | 'file'; location: string; cluster?: 'file' };
  /** Entry urls the coordinator fans request-record lookups out to. */
  entryUrls: string[];
}

export interface RouteResult {
  status: number;
  body: unknown;
}

/**
 * Control-plane HTTP surface. Each route is a thin translation into the
 * controller; all durability and state transitions live there.
 */
export async function handleCoordinatorRequest(
  ctx: { method: string; path: string; query: URLSearchParams; body: any },
  options: CoordinatorServerOptions,
): Promise<RouteResult> {
  const { controller } = options;
  const path = ctx.path;

  if (path === '/health' && ctx.method === 'GET') {
    return { status: 200, body: { ok: true, role: 'coordinator' } };
  }

  if (path === '/admin/state' && ctx.method === 'GET') {
    if (ctx.query.get('detail') === '1') {
      return { status: 200, body: await controller.view(true, options.storeInfo) };
    }
    return { status: 200, body: controller.view(false) };
  }

  if (path === '/admin/state/raw' && ctx.method === 'GET') {
    return { status: 200, body: controller.stateSnapshot() };
  }

  if (path === '/admin/plan' && ctx.method === 'GET') {
    return {
      status: 200,
      body: { ok: true, plan: controller.planSnapshot(), state: controller.view(false) },
    };
  }

  if (path === '/admin/bindings' && ctx.method === 'GET') {
    return { status: 200, body: controller.bindingsView() };
  }

  if ((path === '/configs' || path === '/admin/configs') && ctx.method === 'GET') {
    return { status: 200, body: { ok: true, configs: controller.configList() } };
  }

  if (path.startsWith('/configs/') && ctx.method === 'GET') {
    const version = decodeURIComponent(path.slice('/configs/'.length)) as Version;
    const doc = controller.config(version);
    if (!doc) {
      return {
        status: 404,
        body: { ok: false, code: 'not_found', error: `config ${version} not found` },
      };
    }
    return { status: 200, body: doc };
  }

  if ((path === '/configs' || path === '/admin/configs') && ctx.method === 'POST') {
    const doc = (ctx.body?.doc ?? ctx.body) as ConfigDocument;
    const result = controller.putConfig(doc);
    if (!result.ok) {
      return { status: 400, body: { ok: false, code: 'invalid_config', errors: result.errors } };
    }
    return { status: 200, body: { ok: true, digest: result.digest } };
  }

  if (path === '/ops/advance' && ctx.method === 'POST') {
    const fault = ctx.body?.fault as 'after-pending' | 'after-commit' | undefined;
    const outcome = await controller.advance({ fault });
    return { status: outcome.ok ? 200 : 409, body: outcome };
  }

  if (path === '/ops/pause' && ctx.method === 'POST') {
    return { status: 200, body: { ok: true, state: controller.pause() } };
  }

  if (path === '/ops/resume' && ctx.method === 'POST') {
    return { status: 200, body: { ok: true, state: await controller.resume() } };
  }

  if (path === '/ops/rollback' && ctx.method === 'POST') {
    const outcome = controller.requestRollback(ctx.body?.batchIndex as number | undefined);
    return { status: outcome.ok ? 200 : 409, body: outcome };
  }

  if (path === '/ops/rollback/confirm' && ctx.method === 'POST') {
    const outcome = await controller.confirmRollback();
    return { status: outcome.ok ? 200 : 409, body: outcome };
  }

  if (path === '/ops/recover' && ctx.method === 'POST') {
    await controller.recover();
    return { status: 200, body: { ok: true, state: controller.view(false) } };
  }

  if (path === '/ops/deploy' && ctx.method === 'POST') {
    const version = ctx.body?.version as Version | undefined;
    if (!version) {
      return { status: 400, body: { ok: false, code: 'bad_request', error: 'version is required' } };
    }
    const result = await controller.deploy(version, ctx.body?.services);
    return { status: result.ok ? 200 : 409, body: result };
  }

  if (path === '/admin/entries' && ctx.method === 'GET') {
    return { status: 200, body: { ok: true, entries: await controller.probeEntries() } };
  }

  if (path === '/admin/services' && ctx.method === 'GET') {
    return { status: 200, body: { ok: true, services: await controller.probeServices() } };
  }

  if (path === '/admin/records' && ctx.method === 'GET') {
    const ids = (ctx.query.get('ids') ?? '')
      .split(',')
      .map((id) => id.trim())
      .filter(Boolean);
    return { status: 200, body: { ok: true, records: await fanOutRecords(options.entryUrls, ids) } };
  }

  if (path === '/admin/transport' && ctx.method === 'GET') {
    const summary = await controller.transportSummary();
    return { status: 200, body: { ok: true, ...summary } };
  }

  return { status: 404, body: { ok: false, code: 'not_found', error: `${ctx.method} ${path}` } };
}

/**
 * Records live with the entries. The coordinator fans lookups out so operators
 * get a single endpoint even when the accepting entry has restarted.
 */
async function fanOutRecords(entryUrls: string[], ids: string[]): Promise<unknown[]> {
  if (ids.length === 0) return [];
  const found = new Map<string, unknown>();
  await Promise.all(
    entryUrls.map(async (url) => {
      const res = await jsonRequest<{ records: { requestId: string }[] }>(url, {
        path: `/admin/records?ids=${ids.join(',')}`,
        timeoutMs: 3000,
      });
      if (res.ok && res.body?.records) {
        for (const record of res.body.records) {
          if (record && typeof record === 'object' && !found.has(record.requestId)) {
            found.set(record.requestId, record);
          }
        }
      }
    }),
  );
  return ids.map((id) => found.get(id) ?? { requestId: id, error: 'not found' });
}
