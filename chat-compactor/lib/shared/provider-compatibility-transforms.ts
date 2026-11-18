import { createHash } from "node:crypto";

// Provider-equivalent SEMANTIC subtrees: digest-pinned before/after every
// transform, never mutated. Only non-semantic policy fields may move.
const SEMANTIC_KEYS = ["system", "messages", "images", "tools"] as const;
// Digest sentinel for an absent subtree, so add/remove is caught like a change.
const ABSENT = " <absent>";

export class TransformCapabilityError extends Error {
  override readonly name = "TransformCapabilityError";
}
const fail = (message: string): never => {
  throw new TransformCapabilityError(message);
};
const sha256 = (value: string): string =>
  createHash("sha256").update(value).digest("hex");
// Canonical JSON: object keys sorted recursively so a digest is stable under
// key-order churn. Array order is preserved — it is semantic.
function sortKeys(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortKeys);
  if (value && typeof value === "object") {
    const src = value as Record<string, unknown>;
    const out: Record<string, unknown> = {};
    for (const k of Object.keys(src).sort()) out[k] = sortKeys(src[k]);
    return out;
  }
  return value;
}
export function canonicalJson(value: unknown): string {
  return JSON.stringify(sortKeys(value)) ?? "null";
}
function fieldDigest(payload: Record<string, unknown>, key: string): string {
  return key in payload ? sha256(canonicalJson(payload[key])) : ABSENT;
}
/** A RESOLVED, ready-to-apply registry entry. */
export interface ProviderTransform {
  readonly id: string;
  readonly digest: string; // source digest, recomputed from the definition
  readonly schema: string; // input/output schema tag
  readonly mutableFields: readonly string[]; // non-semantic keys it may write
  apply(payload: Record<string, unknown>): Record<string, unknown>; // pure
}
/** What config resolves against the registry (mirrors config.ts). */
export interface TransformSelector {
  readonly transformId: string;
  readonly expectedDigest: string;
  readonly schema: string;
  readonly order: number;
}
type RegisteredTransform = Omit<ProviderTransform, "digest">;

// MiniMax needs its configured request priority preserved on the provider
// request (plan §143/§191); nothing semantic is touched.
const MINIMAX_PRIORITY: RegisteredTransform = {
  id: "minimax-priority",
  schema: "api-payload/v1",
  mutableFields: ["priority"],
  apply: (payload) => ({ ...payload, priority: "high" }),
};

// Finite task-owned registry. Code is the source of truth; rich.json only
// selects an id + expected source digest/schema/order against these entries.
export const PROVIDER_TRANSFORM_REGISTRY: readonly RegisteredTransform[] =
  Object.freeze([MINIMAX_PRIORITY]);

// Source digest recomputed from the registered definition so ANY code change
// (id, schema, allowlist or apply body) flips it; config pins this exact value.
export function transformSourceDigest(id: string): string {
  const def =
    PROVIDER_TRANSFORM_REGISTRY.find((t) => t.id === id) ??
    fail(`Unknown transform id: ${id}`);
  return sha256(
    canonicalJson({
      id: def.id,
      schema: def.schema,
      mutableFields: def.mutableFields,
      applySource: def.apply.toString(),
    }),
  );
}

// Resolve config selectors against the registry. Rejects unknown id, digest or
// schema mismatch and duplicate order; returns the list sorted ascending.
export function resolveTransforms(
  selectors: readonly TransformSelector[],
): ProviderTransform[] {
  const orders = new Set<number>();
  const resolved = selectors.map((sel) => {
    const def =
      PROVIDER_TRANSFORM_REGISTRY.find((t) => t.id === sel.transformId) ??
      fail(`Unknown transform id: ${sel.transformId}`);
    const digest = transformSourceDigest(def.id);
    if (sel.expectedDigest !== digest)
      fail(`Transform digest mismatch: ${def.id}`);
    if (sel.schema !== def.schema) fail(`Transform schema mismatch: ${def.id}`);
    if (orders.has(sel.order)) fail(`Duplicate transform order: ${sel.order}`);
    orders.add(sel.order);
    return { ...def, digest, order: sel.order };
  });
  resolved.sort((a, b) => a.order - b.order);
  return resolved.map(({ order: _order, ...t }) => Object.freeze(t));
}

function deepFreeze(value: unknown): void {
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const child of Object.values(value)) deepFreeze(child);
  }
}
// Reject any changed non-semantic field outside the transform's allowlist.
function assertOnlyAllowed(
  t: ProviderTransform,
  before: Record<string, unknown>,
  after: Record<string, unknown>,
): void {
  const allowed = new Set(t.mutableFields);
  for (const key of new Set([...Object.keys(before), ...Object.keys(after)])) {
    if ((SEMANTIC_KEYS as readonly string[]).includes(key)) continue;
    if (
      fieldDigest(before, key) !== fieldDigest(after, key) &&
      !allowed.has(key)
    )
      fail(`Transform ${t.id} wrote disallowed field '${key}'`);
  }
}

// The security boundary. Each transform receives a deep clone whose semantic
// subtrees are frozen; every semantic digest must be byte-identical afterwards
// and every changed non-semantic key must be allowlisted, else fail closed.
export function applyProviderTransforms(
  payload: Record<string, unknown>,
  transforms: readonly ProviderTransform[],
): Record<string, unknown> {
  const baseline = SEMANTIC_KEYS.map((key) => fieldDigest(payload, key));
  let current = payload;
  for (const t of transforms) {
    const input = structuredClone(current);
    for (const key of SEMANTIC_KEYS) if (key in input) deepFreeze(input[key]);
    const output = t.apply(input);
    if (!output || typeof output !== "object" || Array.isArray(output))
      fail(`Transform ${t.id} returned a non-object`);
    SEMANTIC_KEYS.forEach((key, i) => {
      if (fieldDigest(output, key) !== baseline[i])
        fail(`Transform ${t.id} mutated semantic subtree '${key}'`);
    });
    assertOnlyAllowed(t, current, output);
    current = output;
  }
  return current;
}
