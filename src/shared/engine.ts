import type {
  ConfigDocument,
  JoinOp,
  RouteStep,
  ServiceConfig,
  ServiceName,
  Version,
} from './types';

export class RuleError extends Error {
  constructor(
    message: string,
    readonly code: string,
    readonly details?: Record<string, unknown>,
    readonly permanent = true,
  ) {
    super(message);
  }
}

/** Apply one service rule. Types are strict, so mixed versions fail loudly. */
export function applyRule(service: ServiceName, config: ServiceConfig, input: unknown): unknown {
  switch (config.op) {
    case 'double': {
      if (typeof input !== 'number') {
        throw new RuleError(
          `service ${service} expected a number for op=double, got ${typeof input}`,
          'wrong_input_type',
          { service, input },
        );
      }
      return input * 2;
    }
    case 'add': {
      const operand = config.operand ?? 0;
      if (typeof input !== 'number' || typeof operand !== 'number') {
        throw new RuleError(
          `service ${service} expected a number for op=add, got ${typeof input}`,
          'wrong_input_type',
          { service, input, operand },
        );
      }
      return input + operand;
    }
    case 'concat': {
      const operand = config.operand ?? '';
      if (typeof input !== 'string') {
        throw new RuleError(
          `service ${service} expected a string for op=concat, got ${typeof input}`,
          'wrong_input_type',
          { service, input },
        );
      }
      return `${input}${operand}`;
    }
    case 'tag': {
      const operand = config.operand ?? '';
      return `${String(input)}${operand}`;
    }
    case 'identity':
      return input;
    default:
      throw new RuleError(`service ${service} has unknown op ${String(config.op)}`, 'bad_rule', {
        service,
        op: config.op,
      });
  }
}

export function joinValues(join: JoinOp | undefined, values: unknown[]): unknown {
  switch (join ?? 'list') {
    case 'list':
      return values;
    case 'sum': {
      let total = 0;
      for (const value of values) {
        if (typeof value !== 'number') {
          throw new RuleError('join=sum requires numeric branch outputs', 'wrong_join_type', {
            value,
          });
        }
        total += value;
      }
      return total;
    }
    case 'concat': {
      let text = '';
      for (const value of values) {
        if (typeof value !== 'string') {
          throw new RuleError('join=concat requires string branch outputs', 'wrong_join_type', {
            value,
          });
        }
        text += value;
      }
      return text;
    }
    default:
      throw new RuleError(`unknown join ${String(join)}`, 'bad_join');
  }
}

const isRouteStep = (value: unknown): value is RouteStep =>
  typeof value === 'object' && value !== null && typeof (value as RouteStep).service === 'string';

export function walkRoute(step: RouteStep, visit: (step: RouteStep, slot: string) => void, slot = 'root'): void {
  visit(step, slot);
  (step.branch ?? []).forEach((child, index) => walkRoute(child, visit, `${slot}.branch[${index}]`));
  if (step.next) walkRoute(step.next, visit, `${slot}.next`);
}

export function routePath(doc: ConfigDocument, group: string): ServiceName[] {
  const route = doc.routes[group];
  if (!route) {
    throw new RuleError(`group ${group} has no route in ${doc.version}`, 'no_route', { group });
  }
  const path: ServiceName[] = [];
  walkRoute(route, (step) => path.push(step.service));
  return path;
}

export function requiredServices(doc: ConfigDocument, groups: string[]): ServiceName[] {
  const set = new Set<ServiceName>();
  for (const group of groups) {
    const route = doc.routes[group];
    if (!route) continue;
    walkRoute(route, (step) => set.add(step.service));
  }
  return [...set].sort();
}

export function assertVersion(actual: Version, expected: Version, where: string): void {
  if (actual !== expected) {
    throw new RuleError(
      `version mismatch at ${where}: request pinned ${expected} but service uses ${actual}`,
      'version_mismatch',
      { expected, actual, where },
    );
  }
}

/** Static validation used when publishing a config version. */
export function validateConfigDocument(doc: ConfigDocument): string[] {
  const errors: string[] = [];
  if (!doc || typeof doc !== 'object') return ['document is not an object'];
  if (!/^v\d+$/.test(doc.version ?? '')) errors.push(`invalid version "${doc.version}"`);
  if (!doc.services || Object.keys(doc.services).length === 0) errors.push('no services declared');
  if (!doc.routes || Object.keys(doc.routes).length === 0) errors.push('no routes declared');
  for (const [name, config] of Object.entries(doc.services ?? {})) {
    if (!['double', 'add', 'concat', 'tag', 'identity'].includes(config.op)) {
      errors.push(`service ${name} has unknown op ${String(config.op)}`);
    }
    if (config.delayMs !== undefined && typeof config.delayMs !== 'number') {
      errors.push(`service ${name} has a non-numeric delayMs`);
    }
  }
  for (const [group, route] of Object.entries(doc.routes ?? {})) {
    if (!isRouteStep(route)) {
      errors.push(`group ${group} route is malformed`);
      continue;
    }
    walkRoute(route, (step, slot) => {
      if (!doc.services[step.service]) errors.push(`${group}@${slot}: unknown service ${step.service}`);
      if (step.branch && step.join === undefined) {
        // Defaults to 'list'; allowed but worth flagging in notes only.
      }
    });
  }
  return errors;
}
