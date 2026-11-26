import * as http from 'node:http';
import type { AddressInfo } from 'node:net';

export interface JsonRequestInit {
  method?: string;
  path: string;
  body?: unknown;
  timeoutMs?: number;
  headers?: Record<string, string>;
}

export interface HttpResult<T = unknown> {
  ok: boolean;
  status: number;
  body: T;
  bytesIn: number;
  bytesOut: number;
  error?: string;
}

/**
 * JSON client with byte accounting.
 *
 * `Connection: close` is deliberate. A pooled keep-alive socket that survives
 * into a restarted process can silently swallow the next request; closing per
 * call keeps fail/restart semantics deterministic.
 */
export async function jsonRequest<T = unknown>(
  baseUrl: string,
  init: JsonRequestInit,
): Promise<HttpResult<T>> {
  const url = new URL(init.path, baseUrl);
  const method = init.method ?? (init.body === undefined ? 'GET' : 'POST');
  const payload =
    init.body === undefined ? undefined : Buffer.from(JSON.stringify(init.body), 'utf8');
  const timeoutMs = init.timeoutMs ?? 5000;

  return new Promise<HttpResult<T>>((resolve) => {
    let settled = false;
    const finish = (value: HttpResult<T>): void => {
      if (settled) return;
      settled = true;
      resolve(value);
    };

    const request = http.request(
      {
        hostname: url.hostname,
        port: url.port,
        path: `${url.pathname}${url.search}`,
        method,
        headers: {
          'content-type': 'application/json',
          connection: 'close',
          ...(payload ? { 'content-length': String(payload.length) } : {}),
          ...(init.headers ?? {}),
        },
      },
      (res) => {
        const chunks: Buffer[] = [];
        let bytesIn = 0;
        res.on('data', (chunk: Buffer) => {
          bytesIn += chunk.length;
          chunks.push(chunk);
        });
        res.on('end', () => {
          const text = Buffer.concat(chunks).toString('utf8');
          let body: unknown;
          try {
            body = text ? JSON.parse(text) : undefined;
          } catch {
            body = text;
          }
          finish({
            ok: (res.statusCode ?? 500) < 400,
            status: res.statusCode ?? 500,
            body: body as T,
            bytesIn,
            bytesOut: payload ? payload.length : 0,
          });
        });
      },
    );
    request.setTimeout(timeoutMs, () => request.destroy(new Error('timeout')));
    request.on('error', (err) => {
      finish({
        ok: false,
        status: 0,
        body: undefined as T,
        bytesIn: 0,
        bytesOut: payload ? payload.length : 0,
        error: err.message,
      });
    });
    if (payload) request.write(payload);
    request.end();
  });
}

export interface HandlerContext {
  method: string;
  path: string;
  query: URLSearchParams;
  body: any;
  bytes: number;
}

export interface HandlerResult {
  status?: number;
  body?: unknown;
  /**
   * Test-only fault hook: close the socket without responding, simulating a
   * lost execution receipt. The work is already journaled, so the client's
   * retry exercises the dedup path.
   */
  dropConnection?: boolean;
}

export type Handler = (
  ctx: HandlerContext,
) => Promise<HandlerResult | void> | HandlerResult | void;

export interface JsonServer {
  url: string;
  port: number;
  close(): Promise<void>;
}

export async function startJsonServer(port: number, handler: Handler): Promise<JsonServer> {
  const server = http.createServer((req, res) => {
    void (async () => {
      const url = new URL(req.url ?? '/', 'http://local');
      const chunks: Buffer[] = [];
      let bytes = 0;
      for await (const chunk of req) {
        bytes += (chunk as Buffer).length;
        chunks.push(chunk as Buffer);
      }
      let body: unknown;
      try {
        const text = Buffer.concat(chunks).toString('utf8');
        body = text ? JSON.parse(text) : undefined;
      } catch {
        res.writeHead(400, { 'content-type': 'application/json', connection: 'close' });
        res.end(JSON.stringify({ ok: false, code: 'bad_json', error: 'invalid JSON body' }));
        return;
      }
      try {
        const result =
          (await handler({
            method: req.method ?? 'GET',
            path: url.pathname,
            query: url.searchParams,
            body,
            bytes,
          })) ?? {};
        if (result.dropConnection) {
          // Deliberately lose the receipt: destroy the socket mid-conversation
          // so the caller sees a transport error and retries.
          req.socket.destroy();
          return;
        }
        const status = result.status ?? 200;
        const payload = result.body === undefined ? '' : JSON.stringify(result.body);
        res.writeHead(status, {
          'content-type': 'application/json',
          'content-length': Buffer.byteLength(payload),
          connection: 'close',
        });
        res.end(payload);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        res.writeHead(500, { 'content-type': 'application/json', connection: 'close' });
        res.end(JSON.stringify({ ok: false, code: 'internal', error: message }));
      }
    })();
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', () => resolve());
  });
  const actual = (server.address() as AddressInfo).port;
  return {
    url: `http://127.0.0.1:${actual}`,
    port: actual,
    close: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections?.();
        server.close(() => resolve());
      }),
  };
}

export async function waitForHttp(
  baseUrl: string,
  path: string,
  timeoutMs = 5000,
  intervalMs = 40,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const res = await jsonRequest(baseUrl, { path, timeoutMs: 250 });
    if (res.ok) return true;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  return false;
}
