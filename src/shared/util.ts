import { createHash, randomUUID } from 'node:crypto';
import type { ConfigDocument, Version } from './types';

export function sha256(data: string): string {
  return createHash('sha256').update(data).digest('hex');
}

export function newId(prefix: string): string {
  return `${prefix}_${randomUUID()}`;
}

/** Deterministic serialization (object keys sorted) — stable across processes. */
export function canonicalJson(value: unknown): string {
  return JSON.stringify(value, (_k, v) => {
    if (v && typeof v === 'object' && !Array.isArray(v)) {
      const sorted: Record<string, unknown> = {};
      for (const key of Object.keys(v as Record<string, unknown>).sort()) {
        sorted[key] = (v as Record<string, unknown>)[key];
      }
      return sorted;
    }
    return v;
  });
}

export function bundleMeta(doc: ConfigDocument): { digest: string; size: number } {
  const payload = canonicalJson(doc);
  return { digest: sha256(payload), size: Buffer.byteLength(payload, 'utf8') };
}

export function configDigest(doc: ConfigDocument): string {
  return sha256(canonicalJson(doc));
}

export const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

/** Deterministic float in [0,1) from a string seed. */
export function seededUnit(seed: string): number {
  const digest = createHash('sha256').update(seed).digest();
  return digest.readUInt32BE(0) / 0x1_0000_0000;
}

export function parseVersion(version: Version): number {
  const match = /^v(\d+)$/.exec(version);
  return match ? Number(match[1]) : NaN;
}

export function renderVersion(n: number): Version {
  return `v${n}`;
}

export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KiB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MiB`;
}

export function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

export function pct(numerator: number, denominator: number): string {
  if (denominator === 0) return 'n/a';
  return `${((numerator / denominator) * 100).toFixed(1)}%`;
}
