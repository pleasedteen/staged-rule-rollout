#!/usr/bin/env node
/**
 * Deterministic unit checks for ConfigCache source fail-over and byte
 * accounting, run against in-process HTTP sources (no full cluster needed).
 *
 * Covers:
 *   1. preferred source that drops mid-transfer is skipped and a healthy
 *      fallback source completes the document;
 *   2. a truncated/corrupt body is neither cached nor counted as bytes;
 *   3. bytes are recorded only for the complete, verified body, exactly once;
 *   4. concurrent callers share one delivery (single flight).
 *
 * The script writes everything under an OS temp directory and only uses Node
 * built-ins. Run with: node scripts/test-config-cache.js
 */
'use strict';

const http = require('node:http');
const os = require('node:os');
const fs = require('node:fs');
const path = require('node:path');

const { ConfigCache } = require('../dist/shared/configCache.js');
const { openStore } = require('../dist/shared/store.js');
const { canonicalJson, sha256 } = require('../dist/shared/util.js');

const doc = {
  version: 'vX',
  services: { svc: { op: 'identity' } },
  routes: { g: { service: 'svc' } },
};
const fullBody = JSON.stringify(doc);
const digest = sha256(canonicalJson(doc));

function startHealthySource() {
  return new Promise((resolveStart) => {
    const server = http.createServer((req, res) => {
      res.writeHead(200, { 'content-type': 'application/json', connection: 'close' });
      res.end(fullBody);
    });
    server.listen(0, '127.0.0.1', () => {
      resolveStart({ server, url: `http://127.0.0.1:${server.address().port}` });
    });
  });
}

/** Source that writes a partial body then destroys the socket (mid-transfer drop). */
function startTruncatingSource() {
  return new Promise((resolveStart) => {
    const server = http.createServer((req, res) => {
      res.writeHead(200, { 'content-type': 'application/json', connection: 'close' });
      res.write(fullBody.slice(0, 40));
      res.socket?.destroy();
    });
    server.listen(0, '127.0.0.1', () => {
      resolveStart({ server, url: `http://127.0.0.1:${server.address().port}` });
    });
  });
}

/** Source that returns a well-formed but wrong-digest document. */
function startCorruptSource() {
  return new Promise((resolveStart) => {
    const bad = { ...doc, notes: 'tampered' };
    const server = http.createServer((req, res) => {
      res.writeHead(200, { 'content-type': 'application/json', connection: 'close' });
      res.end(JSON.stringify(bad));
    });
    server.listen(0, '127.0.0.1', () => {
      resolveStart({ server, url: `http://127.0.0.1:${server.address().port}` });
    });
  });
}

let failures = 0;
function check(name, ok, detail) {
  process.stdout.write(`  [${ok ? 'PASS' : 'FAIL'}] ${name}${detail ? `\n         ${detail}` : ''}\n`);
  if (!ok) failures += 1;
}

async function main() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cfg-cache-'));
  const healthy = await startHealthySource();
  const truncating = await startTruncatingSource();
  const corrupt = await startCorruptSource();

  process.stdout.write('ConfigCache fail-over & accounting\n');

  // 1-3: truncating first, corrupt second, healthy last.
  {
    const store = openStore(path.join(dir, 'one.sqlite'));
    const cache = new ConfigCache(
      {
        get: () => undefined,
        put: () => {},
        versions: () => [],
      },
      [
        { name: 'truncating', baseUrl: truncating.url },
        { name: 'corrupt', baseUrl: corrupt.url },
        { name: 'healthy', baseUrl: healthy.url },
      ],
      { store, role: 'test:one' },
    );
    cache.setExpectedDigest('vX', digest);
    const got = await cache.get('vX', 1500);
    check('fail-over reaches the healthy source after truncation + corruption', got.version === 'vX', `source=${cache.sourceByVersion.get('vX')}`);
    check('only the complete body is counted as bytes', cache.transport.bytes === Buffer.byteLength(fullBody), `bytes=${cache.transport.bytes} expected=${Buffer.byteLength(fullBody)}`);
    check('exactly one fetch recorded despite two failed sources', cache.transport.fetches === 1, `fetches=${cache.transport.fetches}`);
    check('the winning source is named correctly', cache.sourceByVersion.get('vX') === 'healthy', cache.sourceByVersion.get('vX'));
    store.close();
  }

  // 4: single flight — many concurrent getters share one network delivery.
  {
    const hits = { count: 0 };
    const counted = http.createServer((req, res) => {
      hits.count += 1;
      res.writeHead(200, { 'content-type': 'application/json', connection: 'close' });
      res.end(fullBody);
    });
    await new Promise((r) => counted.listen(0, '127.0.0.1', r));
    const countedUrl = `http://127.0.0.1:${counted.address().port}`;
    const store = openStore(path.join(dir, 'two.sqlite'));
    const cache = new ConfigCache(
      { get: () => undefined, put: () => {}, versions: () => [] },
      [{ name: 'only', baseUrl: countedUrl }],
      { store, role: 'test:two' },
    );
    cache.setExpectedDigest('vX', digest);
    const results = await Promise.all(Array.from({ length: 8 }, () => cache.get('vX', 1500)));
    check('all concurrent callers receive the document', results.every((d) => d.version === 'vX'), `n=${results.length}`);
    check('concurrent callers share a single network fetch', hits.count === 1, `server hits=${hits.count}`);
    check('single flight records the body once', cache.transport.bytes === Buffer.byteLength(fullBody), `bytes=${cache.transport.bytes}`);
    counted.close();
    store.close();
  }

  for (const s of [healthy, truncating, corrupt]) s.server.close();
  fs.rmSync(dir, { recursive: true, force: true });

  process.stdout.write(failures === 0 ? '\nCONFIG-CACHE CHECKS PASSED\n' : `\n${failures} CHECK(S) FAILED\n`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((err) => {
  process.stderr.write(`${err.stack ?? err}\n`);
  process.exit(2);
});
