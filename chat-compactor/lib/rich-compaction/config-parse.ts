import type { ModelThinkingLevel } from "@earendil-works/pi-ai";
import type { RichRole, RoleSettings } from "./rich-types.js";

// Shared strict-parse primitives for rich.json. config.ts is the public surface
// (parseRichConfig + loadRichConfig); the verbose per-node validators live here
// so each file stays ≤150 lines, mirroring the auditor config/config-load split.

/** Strict parse failure for rich.json. Mirrors auditor `ConfigParseError`. */
export class RichConfigParseError extends Error {
  constructor(
    public readonly field: string,
    public readonly reason: string,
  ) {
    super(field + ": " + reason);
    this.name = "RichConfigParseError";
  }
}

export const isObject = (v: unknown): v is Record<string, unknown> =>
  v !== null && typeof v === "object" && !Array.isArray(v);
export const isPosInt = (v: unknown): v is number =>
  Number.isInteger(v) && (v as number) > 0;
export const fail = (field: string, reason: string): never => {
  throw new RichConfigParseError(field, reason);
};
/** Fail-closed: any key outside `allowed` is rejected (invariant 13 — no model
 * id or path/export/executable field may hide in a nested object). */
export const rejectUnknown = (field: string, o: Record<string, unknown>, allowed: string[]): void => {
  for (const k of Object.keys(o)) if (!allowed.includes(k)) fail(`${field}.${k}`, "unknown-field");
};

const THINKING = new Set("off minimal low medium high xhigh max".split(" "));
export const RICH_ROLES = "summarizer verifier repair fallback".split(" ") as RichRole[];
export const TOP_LEVEL = new Set("name enabled roles providerCompatibilityTransforms".split(" "));
// Legacy/model fields banned by invariant 13; each rejected with its own reason.
// context_fit_profiles / approved_effective_digests are banned too: context
// sizing and provider provenance approval are not this extension's business.
// `roots` belonged to the desktop host analysis this tree does not carry.
export const LEGACY_FIELDS = "model prompt_file max_context_chars max_restore_skill_chars continuation_text disabled provider_compatibility_extensions context_fit_profiles approved_effective_digests roots".split(" ");
export const HEX64 = /^[0-9a-f]{64}$/;

export function parseRole(role: RichRole, raw: unknown): RoleSettings {
  if (!isObject(raw)) fail(`roles.${role}`, "not-object");
  const r = raw as Record<string, unknown>;
  rejectUnknown(`roles.${role}`, r, ["thinking", "timeout_min"]);
  if (typeof r.thinking !== "string" || !THINKING.has(r.thinking))
    fail(`roles.${role}.thinking`, "invalid-thinking-level");
  if (!isPosInt(r.timeout_min))
    fail(`roles.${role}.timeout_min`, "not-positive-integer");
  return {
    thinking: r.thinking as ModelThinkingLevel,
    timeoutMs: (r.timeout_min as number) * 60000,
  };
}
