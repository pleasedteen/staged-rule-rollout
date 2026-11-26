import { readdirSync } from 'node:fs';
import { join, resolve } from 'node:path';
import type { ConfigDocument, PlanFile } from '../shared/types';
import {
  openClusterStore,
  clusterStorePath,
  storeEngine,
  CoordinatorRepository,
  ConfigRepository,
} from '../shared/store';
import { startJsonServer } from '../shared/http';
import { fatal, loadTopology, logEvent, parseArgs, readJson } from '../shared/runtime';
import { validateConfigDocument } from '../shared/engine';
import { ReleaseController } from './controller';
import { handleCoordinatorRequest } from './server';

/**
 * Coordinator process.
 *
 * Boot sequence:
 *   1. open the shared cluster store;
 *   2. on a virgin store, install the plan + configs + initial bindings;
 *   3. on an existing store, reconcile with the entries (`recover`);
 *   4. serve the control plane.
 *
 * Killing and restarting the process therefore preserves everything an
 * operator needs: per-batch effective versions and release status are read
 * from durable state and re-verified against the entries.
 */
async function main(): Promise<void> {
  const args = parseArgs();
  const topology = loadTopology(args.topology);
  const store = openClusterStore(args.dataDir);
  const repo = new CoordinatorRepository(store);
  const configs = new ConfigRepository(store);
  const controller = new ReleaseController(topology, repo, configs);

  const hasState = Boolean(repo.loadState());
  if (!hasState && args.plan) {
    const planPath = resolve(args.plan);
    const planFile = readJson<PlanFile>(planPath);
    const configsDir = planFile.configsDir
      ? resolve(join(planPath, '..', planFile.configsDir))
      : resolve(join(planPath, '..'));
    const docs: ConfigDocument[] = [];
    for (const name of readdirSync(configsDir)) {
      if (!name.endsWith('.json')) continue;
      const doc = readJson<ConfigDocument>(join(configsDir, name));
      const errors = validateConfigDocument(doc);
      if (errors.length > 0) throw new Error(`${name}: ${errors.join('; ')}`);
      docs.push(doc);
    }
    controller.initialize({
      plan: { releaseId: planFile.releaseId, batches: planFile.batches },
      initialVersions: planFile.initialVersions,
      configs: docs,
    });
    logEvent('coordinator', 'initialized release plan', {
      releaseId: planFile.releaseId,
      batches: planFile.batches.length,
      configs: docs.length,
    });
  } else if (!hasState && args.seedConfig) {
    const doc = readJson<ConfigDocument>(args.seedConfig);
    controller.putConfig(doc);
    logEvent('coordinator', 'seeded config', { version: doc.version });
  }

  await controller.recover();
  await controller.primeServices().catch(() => undefined);
  const recovered = controller.view(false);
  if (recovered.recoveries > 0) {
    logEvent('coordinator', 'recovered durable state', {
      recoveries: recovered.recoveries,
      cursor: recovered.cursor,
      note: recovered.lastRecovery,
    });
  }

  const entryUrls = topology.entries.map((entry) => `http://127.0.0.1:${entry.port}`);
  // The cluster store is always the file engine; local stores use SQLite when
  // available. Report the local engine as the SQLite capability indicator.
  const storeInfo = {
    kind: storeEngine(),
    location: clusterStorePath(args.dataDir),
    cluster: 'file' as const,
  };
  const server = await startJsonServer(topology.coordinator.port, (ctx) =>
    handleCoordinatorRequest(ctx, { controller, storeInfo, entryUrls }),
  );

  logEvent('coordinator', 'listening', {
    url: server.url,
    store: storeInfo.kind,
    state: recovered.status,
    batches: recovered.batches.map((batch) => `${batch.name}:${batch.status}`),
  });

  const shutdown = async (signal: string): Promise<void> => {
    logEvent('coordinator', `shutting down (${signal})`);
    await server.close();
    store.close();
  };
  process.on('SIGTERM', () => void shutdown('SIGTERM').then(() => process.exit(0)));
  process.on('SIGINT', () => void shutdown('SIGINT').then(() => process.exit(0)));
}

main().catch(fatal);
