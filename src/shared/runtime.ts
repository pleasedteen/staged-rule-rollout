import { readFileSync } from 'node:fs';
import type { ConfigDocument, ServiceInstanceDef, ServiceProcessDef, Topology } from './types';
import { validateConfigDocument } from './engine';

export const DEFAULT_TOPOLOGY = 'fixtures/topology.json';

export interface CommonArgs {
  dataDir: string;
  topology: string;
  name?: string;
  /** Service instance name; defaults to the service name for single-instance services. */
  instance?: string;
  coordinator?: string;
  plan?: string;
  port?: number;
  host?: string;
  seedConfig?: string;
}

const FIELD_BY_FLAG: Record<string, keyof CommonArgs> = {
  'data-dir': 'dataDir',
  topology: 'topology',
  name: 'name',
  instance: 'instance',
  coordinator: 'coordinator',
  plan: 'plan',
  port: 'port',
  host: 'host',
  'seed-config': 'seedConfig',
};

/** Minimal `--flag value` / `--flag=value` parser; unknown flags are fatal. */
export function parseArgs(argv = process.argv.slice(2)): CommonArgs {
  const out: CommonArgs = { dataDir: '.run', topology: DEFAULT_TOPOLOGY };
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) throw new Error(`unexpected argument ${token}`);
    const eq = token.indexOf('=');
    const flag = eq === -1 ? token.slice(2) : token.slice(2, eq);
    const field = FIELD_BY_FLAG[flag];
    if (!field) throw new Error(`unknown flag --${flag}`);
    let value: string;
    if (eq !== -1) {
      value = token.slice(eq + 1);
    } else {
      value = argv[i + 1];
      if (value === undefined) throw new Error(`missing value for --${flag}`);
      i += 1;
    }
    if (field === 'port') {
      const port = Number(value);
      if (!Number.isInteger(port) || port <= 0) throw new Error(`bad port ${value}`);
      out.port = port;
    } else {
      (out as unknown as Record<string, string>)[field] = value;
    }
  }
  return out;
}

export function readJson<T>(path: string): T {
  return JSON.parse(readFileSync(path, 'utf8')) as T;
}

export function loadTopology(path: string): Topology {
  const topology = readJson<Topology>(path);
  if (!topology.coordinator || !Number.isInteger(topology.coordinator.port)) {
    throw new Error(`${path}: missing coordinator.port`);
  }
  if (!Array.isArray(topology.entries) || !Array.isArray(topology.services)) {
    throw new Error(`${path}: entries and services must be arrays`);
  }
  const seen = new Set<string>();
  for (const service of topology.services) {
    if (!service.name) throw new Error(`${path}: service without a name`);
    const instances = serviceInstances(service);
    if (instances.length === 0) {
      throw new Error(`${path}: service ${service.name} declares no instances`);
    }
    for (const instance of instances) {
      if (seen.has(instance.name)) {
        throw new Error(`${path}: duplicate instance name ${instance.name}`);
      }
      seen.add(instance.name);
      if (!Number.isInteger(instance.port) || instance.port <= 0) {
        throw new Error(`${path}: instance ${instance.name} has a bad port`);
      }
    }
  }
  return topology;
}

/**
 * Instances of one service. A topology that only declares `port` is a
 * single-instance service whose instance name is the service name, so existing
 * single-instance deployments keep working without changes.
 */
export function serviceInstances(service: ServiceProcessDef): ServiceInstanceDef[] {
  if (service.instances && service.instances.length > 0) return service.instances;
  if (typeof service.port === 'number') return [{ name: service.name, port: service.port }];
  return [];
}

export function instancesOf(topology: Topology, serviceName: string): ServiceInstanceDef[] {
  const service = topology.services.find((candidate) => candidate.name === serviceName);
  return service ? serviceInstances(service) : [];
}

export function instancePort(topology: Topology, serviceName: string, instanceName: string): number {
  const instance = instancesOf(topology, serviceName).find(
    (candidate) => candidate.name === instanceName,
  );
  if (!instance) throw new Error(`unknown instance ${serviceName}/${instanceName}`);
  return instance.port;
}

export const coordinatorUrl = (topology: Topology): string =>
  `http://127.0.0.1:${topology.coordinator.port}`;

export const entryUrl = (topology: Topology, name: string): string => {
  const entry = topology.entries.find((candidate) => candidate.name === name);
  if (!entry) throw new Error(`unknown entry ${name}`);
  return `http://127.0.0.1:${entry.port}`;
};

/**
 * URL of one instance; without a name, the service's first instance.
 */
export const instanceUrl = (
  topology: Topology,
  serviceName: string,
  instanceName?: string,
): string => {
  const instances = instancesOf(topology, serviceName);
  const instance = instanceName
    ? instances.find((candidate) => candidate.name === instanceName)
    : instances[0];
  if (!instance) throw new Error(`unknown instance ${serviceName}/${instanceName ?? '*'}`);
  return `http://127.0.0.1:${instance.port}`;
};

/** Backwards-compatible alias: the first instance of a service. */
export const serviceUrl = (topology: Topology, name: string): string =>
  instanceUrl(topology, name);

export function loadConfig(path: string): ConfigDocument {
  const doc = readJson<ConfigDocument>(path);
  const errors = validateConfigDocument(doc);
  if (errors.length > 0) throw new Error(`${path}: ${errors.join('; ')}`);
  return doc;
}

export function logEvent(role: string, message: string, extra?: Record<string, unknown>): void {
  const suffix = extra ? ` ${JSON.stringify(extra)}` : '';
  process.stdout.write(`[${new Date().toISOString()}] [${role}] ${message}${suffix}\n`);
}

export function fatal(err: unknown): never {
  const message = err instanceof Error ? (err.stack ?? err.message) : String(err);
  process.stderr.write(`[fatal] ${message}\n`);
  process.exit(1);
}

export function installSignalHandlers(onShutdown: () => void | Promise<void>): void {
  let shuttingDown = false;
  const shutdown = (signal: string) => {
    if (shuttingDown) return;
    shuttingDown = true;
    process.stdout.write(`[signal] ${signal}, shutting down\n`);
    Promise.resolve(onShutdown()).finally(() => process.exit(0));
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}
