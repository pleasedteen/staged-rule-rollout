/**
 * Independent oracles for the fixtures.
 *
 * These re-derive expected results from the config fixtures *without*
 * importing the middleware engine, so an engine bug cannot be masked by a
 * matching bug in the test.
 *
 * Value semantics (see fixtures/configs/*.json):
 *   add      x + operand           (numbers)
 *   double   x * 2                 (numbers)
 *   concat   x + operand           (x must be a string)
 *   tag      String(x) + operand   (any input)
 *
 * Route semantics: a node runs its service on the payload; if it has `branch`
 * children they each run on that output in parallel and their final outputs
 * are combined by `join`; if it has `next`, the combined value is its input.
 * A `list` join renders as a comma-joined array when later stringified.
 */

const listToString = (items: unknown[]): string => items.map((item) => String(item)).join(',');

/* ------------------------------- currency v1 ------------------------------- */

export function expectedCurrencyV1(input: number): string {
  const ingest = input + 10; // add 10
  const branchMath = ingest * 2; // fx-math: double
  const branchTag = `${ingest}-v1`; // fx-tag: tag "-v1"
  const joined = [branchMath, branchTag]; // join: list
  return `${listToString(joined)}-v1`; // fx-final: tag "-v1"
}

export function currencyV1Segments(input: number): {
  ingest: number;
  branchMath: number;
  branchTag: string;
  finalInput: unknown[];
} {
  const ingest = input + 10;
  const branchMath = ingest * 2;
  const branchTag = `${ingest}-v1`;
  return { ingest, branchMath, branchTag, finalInput: [branchMath, branchTag] };
}

/* ------------------------------- currency v2 ------------------------------- */

export function expectedCurrencyV2(input: number): string {
  const ingest = `${input}n=`; // tag "n=" (String(x) + operand)
  const branchMath = `${ingest}*2`; // fx-math: concat "*2"
  const branchTag = `${ingest}-v2`; // fx-tag: tag "-v2"
  const joined = [branchMath, branchTag]; // join: list
  return `${listToString(joined)}-v2`; // fx-final: tag "-v2"
}

/* -------------------------------- search v1 -------------------------------- */

export function expectedSearchV1(input: string): string {
  const idx = `${input}-i1`; // concat "-i1"
  const a = `${idx}-a1`; // search-a: concat "-a1"
  const b = `${idx}-b1`; // search-b: tag "-b1"
  const joined = `${a}${b}`; // join: concat
  return `${joined}-f1`; // search-final: concat "-f1"
}

/* -------------------------------- search v2 -------------------------------- */

export function expectedSearchV2(input: string): string {
  const idx = `${input}-i2`; // tag "-i2"
  const a = `${idx}-a2`; // search-a: concat "-a2"
  const b = `${idx}-b2`; // search-b: tag "-b2"
  const joined = `${a}${b}`; // join: concat
  return `${joined}-f2`; // search-final: tag "-f2"
}

/* -------------------------------- report v1 -------------------------------- */

export function expectedReportV1(input: string): string {
  const agg = `${input}-agg1`; // tag "-agg1"
  const fmt = `${agg}-fmt1`; // concat "-fmt1"
  return `${fmt}-fin1`; // tag "-fin1"
}

/* -------------------------------- report v2 -------------------------------- */

export function expectedReportV2(input: string): string {
  const agg = `${input}-agg2`; // concat "-agg2"
  const fmt = `${agg}-fmt2`; // tag "-fmt2"
  return `${fmt}-fin2`; // concat "-fin2"
}

/* -------------------------------- archive v1 ------------------------------- */

export function expectedArchiveV1(input: string): string {
  const load = `${input}-l1`; // tag "-l1"
  return `${load}-s1`; // concat "-s1"
}

/* -------------------------------- archive v2 ------------------------------- */

export function expectedArchiveV2(input: string): string {
  const load = `${input}-l2`; // concat "-l2"
  return `${load}-s2`; // tag "-s2"
}

/* -------------------------------- gateway ---------------------------------- */
/*
 * The gateway group reuses the currency services but versions its route:
 *   v1: ingest → branch[fx-math, fx-tag] join list → fx-final   (branched)
 *   v2: ingest → fx-final                                       (no fan-out)
 * Because the v2 route never references fx-math/fx-tag, batch 4 may switch
 * while those branch services are down; parked v1 requests keep their pin.
 */

export function expectedGatewayV1(input: number): string {
  const ingest = input + 10; // add 10
  const branchMath = ingest * 2; // fx-math: double
  const branchTag = `${ingest}-v1`; // fx-tag: tag "-v1"
  const joined = [branchMath, branchTag]; // join: list
  return `${listToString(joined)}-v1`; // fx-final: tag "-v1"
}

export function gatewayV1Segments(input: number): {
  ingest: number;
  branchMath: number;
  branchTag: string;
} {
  const ingest = input + 10;
  return { ingest, branchMath: ingest * 2, branchTag: `${ingest}-v1` };
}

export function expectedGatewayV2(input: number): string {
  const ingest = `${input}n=`; // tag "n="
  return `${ingest}-v2`; // fx-final: tag "-v2"
}
