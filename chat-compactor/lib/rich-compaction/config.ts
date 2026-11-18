import { readFileSync } from "node:fs";
import type {
  RichConfig,
  RichRole,
  RoleSettings,
  TransformSelector,
} from "./rich-types.js";
import {
  HEX64,
  LEGACY_FIELDS,
  RICH_ROLES,
  RichConfigParseError,
  TOP_LEVEL,
  fail,
  isObject,
  parseRole,
  rejectUnknown,
} from "./config-parse.js";

export { RichConfigParseError } from "./config-parse.js";

// Registry cross-check (transformId ↔ digest) is slice 4b; here the id is an
// opaque validated string.
function parseTransforms(raw: unknown): TransformSelector[] {
  if (!Array.isArray(raw))
    fail("providerCompatibilityTransforms", "must-be-array");
  const orders = new Set<number>();
  return (raw as unknown[]).map((entry, i) => {
    const f = `providerCompatibilityTransforms[${i}]`;
    if (!isObject(entry)) fail(f, "not-object");
    const t = entry as Record<string, unknown>;
    // Rejects module/export/exec and any other non-descriptor field (§122).
    rejectUnknown(f, t, [
      "provider",
      "model",
      "transformId",
      "expectedDigest",
      "schema",
      "order",
    ]);
    for (const s of [
      "provider",
      "model",
      "transformId",
      "expectedDigest",
      "schema",
    ])
      if (typeof t[s] !== "string" || !(t[s] as string).trim())
        fail(`${f}.${s}`, "not-string");
    if (!HEX64.test(t.expectedDigest as string))
      fail(`${f}.expectedDigest`, "not-64-hex");
    if (!Number.isInteger(t.order) || (t.order as number) < 0)
      fail(`${f}.order`, "not-non-negative-integer");
    if (orders.has(t.order as number)) fail(`${f}.order`, "duplicate-order");
    orders.add(t.order as number);
    return {
      provider: t.provider as string,
      model: t.model as string,
      transformId: t.transformId as string,
      expectedDigest: t.expectedDigest as string,
      schema: t.schema as string,
      order: t.order as number,
    };
  });
}

/** Strict validator for a parsed rich.json object. No model id is accepted
 * anywhere (invariant 13); every legacy field and unknown key is rejected. */
export function parseRichConfig(json: unknown): RichConfig {
  if (!isObject(json)) fail("<root>", "not-object");
  const o = json as Record<string, unknown>;
  for (const legacy of LEGACY_FIELDS)
    if (legacy in o) fail(legacy, "legacy-field-forbidden");
  for (const k of Object.keys(o))
    if (!TOP_LEVEL.has(k)) fail(k, "unknown-field");
  if (o.enabled !== undefined && typeof o.enabled !== "boolean")
    fail("enabled", "not-boolean");
  if (!isObject(o.roles)) fail("roles", "not-object");
  const roleObj = o.roles as Record<string, unknown>;
  for (const k of Object.keys(roleObj))
    if (!RICH_ROLES.includes(k as RichRole)) fail(`roles.${k}`, "unknown-role");
  const roles = {} as Record<RichRole, RoleSettings>;
  for (const role of RICH_ROLES) {
    if (!(role in roleObj)) fail(`roles.${role}`, "missing-role");
    roles[role] = parseRole(role, roleObj[role]);
  }
  // Invariant 7 (rev 20): verifier independence is a distinct thinking level.
  if (roles.verifier.thinking === roles.summarizer.thinking)
    fail("roles.verifier.thinking", "must-differ-from-summarizer");
  return {
    enabled: (o.enabled as boolean | undefined) ?? true,
    roles,
    providerCompatibilityTransforms: parseTransforms(
      o.providerCompatibilityTransforms ?? [],
    ),
  };
}

/** Thin IO: read + JSON.parse + strict parse; every failure is a RichConfigParseError. */
export function loadRichConfig(
  path: string,
  onError?: (error: RichConfigParseError) => void,
): RichConfig {
  let json: unknown;
  try {
    json = JSON.parse(readFileSync(path, "utf8"));
  } catch (error) {
    const wrapped = new RichConfigParseError(
      path,
      `load-failed: ${(error as Error).message}`,
    );
    onError?.(wrapped);
    throw wrapped;
  }
  try {
    return parseRichConfig(json);
  } catch (error) {
    if (error instanceof RichConfigParseError) onError?.(error);
    throw error;
  }
}
