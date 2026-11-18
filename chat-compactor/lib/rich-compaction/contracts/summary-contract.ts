import type { GenerationContract } from "../generation-contract.js";
import type { DirectContent } from "../../shared/isolated-pi-types.js";
import { loadRichPrompt } from "../rich-prompt.js";
import {
  CHAT_SECTIONS,
  EVENT_ITEM_PREFIX,
  MAX_ITEM_CHARS,
  MAX_SECTION_ITEMS,
  chatSection,
  chatSectionOrder,
  isChatSectionKey,
  normalizeItem,
} from "./chat-sections.js";
import type {
  RootSectionsV1,
  SourceInterval,
  SummaryNodeRole,
  SummaryNodeV1,
} from "./summary-types.js";

/** Host-owned sentinel; the model must never echo it back inside a summary. */
export const RESERVED_HOST_MARKER = "<<<RICH:";
const REQUEST_MARKER = `${RESERVED_HOST_MARKER}REQUEST`;

// Model-authored Files/Skills headings are forbidden — host-owned (invariant 9).
const FILE_SKILL_HEADING = /^ {0,3}#{1,6}[ \t]*(files|skills)\b/im;

/** The single semantic system prompt; loaded once, byte-exact (invariant 15). */
const RICH_PROMPT = loadRichPrompt();

/** One request to summarize an interval as a node of `role`; the model echoes the
 *  host digests (sourceDigest/coverageDigest) we re-verify (rich.md). */
export interface SummaryRequestV1 {
  readonly role: SummaryNodeRole;
  readonly interval: SourceInterval;
  readonly frames: readonly DirectContent[];
  readonly childDigests?: readonly string[];
  readonly sourceDigest?: string;
  readonly coverageDigest?: string;
  /** Concrete issues fed back on a repair request. */
  readonly issues?: readonly string[];
}

function renderHeader(input: SummaryRequestV1): string {
  const { startOrdinal, endOrdinal } = input.interval;
  const lines = [
    `${REQUEST_MARKER} role=${input.role} interval=${startOrdinal}..${endOrdinal}`,
  ];
  if (input.childDigests?.length)
    lines.push(`childDigests=${input.childDigests.join(",")}`);
  if (input.sourceDigest) lines.push(`sourceDigest=${input.sourceDigest}`);
  if (input.coverageDigest)
    lines.push(`coverageDigest=${input.coverageDigest}`);
  if (input.issues?.length) lines.push(`issues=${input.issues.join(" | ")}`);
  lines.push(
    "Summarize ONLY the source frames below, per the system contract.",
  );
  return lines.join("\n");
}

/** Ordered content: one typed header frame, then the raw source frames. */
function encodeInput(input: SummaryRequestV1): readonly DirectContent[] {
  const blocks: DirectContent[] = [{ type: "text", text: renderHeader(input) }];
  for (const frame of input.frames) blocks.push(frame);
  return blocks;
}

/** Normalize a root section set: one line per value, empty items dropped, so a
 *  model that wraps a bullet or pads an item still lands on the rendered shape.
 *  Unknown keys are re-emitted as-is and rejected by validate. */
function normalizeRootSections(raw: unknown): RootSectionsV1 {
  if (raw === null || typeof raw !== "object" || Array.isArray(raw))
    throw new Error("root: sections object required");
  const out: Record<string, string | readonly string[]> = {};
  for (const [key, value] of Object.entries(raw as Record<string, unknown>)) {
    const spec = isChatSectionKey(key) ? chatSection(key) : undefined;
    if (!spec) {
      out[key] = typeof value === "string" ? value : JSON.stringify(value);
      continue;
    }
    if (spec.kind === "prose") {
      if (typeof value !== "string")
        throw new Error(`root.${key}: must be a string`);
      out[key] = normalizeItem(value);
      continue;
    }
    if (!Array.isArray(value) || value.some((i) => typeof i !== "string"))
      throw new Error(`root.${key}: must be an array of strings`);
    const items = (value as string[]).map(normalizeItem).filter((i) => i.length > 0);
    if (items.length > 0) out[key] = items;
  }
  return out as RootSectionsV1;
}

function parseFinal(text: string): SummaryNodeV1 {
  const obj = JSON.parse(text.trim()) as {
    kind?: unknown;
    sections?: unknown;
  };
  if (obj.kind === "leaf" || obj.kind === "merge") return obj as unknown as SummaryNodeV1;
  if (obj.kind === "root")
    return {
      ...(obj as unknown as { coverageDigest: string }),
      sections: normalizeRootSections(obj.sections),
    };
  throw new Error(`Unknown summary node kind: ${String(obj.kind)}`);
}

/** Field gate: no host marker or Files/Skills heading; mustFill forbids "". */
function gateField(value: unknown, label: string, mustFill: boolean): void {
  if (typeof value !== "string") throw new Error(`${label}: non-string field`);
  if (mustFill && !value.trim()) throw new Error(`${label}: field is empty`);
  if (value.includes(RESERVED_HOST_MARKER))
    throw new Error(`${label}: reserved host marker leaked`);
  if (FILE_SKILL_HEADING.test(value))
    throw new Error(`${label}: model-authored Files/Skills section forbidden`);
}

function validateInterval(
  interval: SourceInterval | undefined,
  label: string,
): void {
  const start = interval?.startOrdinal;
  const end = interval?.endOrdinal;
  if (
    !Number.isInteger(start) ||
    !Number.isInteger(end) ||
    (start as number) < 0 ||
    (end as number) < (start as number)
  )
    throw new Error(`${label}: invalid source interval`);
}

function requireNonEmpty(value: unknown, message: string): void {
  if (typeof value !== "string" || !value.trim()) throw new Error(message);
}

function validateDigests(
  digests: readonly string[] | undefined,
  label: string,
): void {
  if (!Array.isArray(digests) || digests.length === 0)
    throw new Error(`${label}: at least one child digest required`);
  if (digests.some((d) => typeof d !== "string" || !d.trim()))
    throw new Error(`${label}: child digests must be non-empty strings`);
}

/** The chat structure gate: keys must be registry keys, reported in registry
 *  order, with the declared kind, inside the item/length ceilings; required
 *  sections must be present and non-empty. A failure here is a `model-output`
 *  failure and goes through repair before the fallback profile. */
function validateRootSections(sections: RootSectionsV1): void {
  if (sections === null || typeof sections !== "object")
    throw new Error("root: sections object required");
  let lastOrder = -1;
  for (const [key, value] of Object.entries(sections)) {
    const spec = isChatSectionKey(key) ? chatSection(key) : undefined;
    if (!spec) throw new Error(`root: unknown section "${key}"`);
    const order = chatSectionOrder(key);
    if (order <= lastOrder)
      throw new Error(`root: sections must follow the registry order (${key})`);
    lastOrder = order;
    if (spec.kind === "prose") {
      gateField(value, `root.${key}`, spec.required);
      continue;
    }
    if (!Array.isArray(value) || value.length === 0)
      throw new Error(`root.${key}: must be a non-empty array of items`);
    if (value.length > MAX_SECTION_ITEMS)
      throw new Error(
        `root.${key}: ${value.length} items exceeds the ${MAX_SECTION_ITEMS}-item ceiling`,
      );
    value.forEach((item, i) => {
      gateField(item, `root.${key}[${i}]`, true);
      if ((item as string).length > MAX_ITEM_CHARS)
        throw new Error(
          `root.${key}[${i}]: item exceeds ${MAX_ITEM_CHARS} chars`,
        );
      if (key === "events" && !EVENT_ITEM_PREFIX.test(item as string))
        throw new Error(
          `root.events[${i}]: item must start with "YYYY-MM-DD — " or "— — "`,
        );
    });
  }
  for (const spec of CHAT_SECTIONS) {
    if (!spec.required) continue;
    if (!(spec.key in sections))
      throw new Error(`root: required section "${spec.key}" is missing`);
  }
}

function validate(role: SummaryNodeRole, output: SummaryNodeV1): void {
  if (role !== "repair" && output.kind !== role)
    throw new Error(`Expected a ${role} node, got ${output.kind}`);
  switch (output.kind) {
    case "leaf":
      validateInterval(output.interval, "leaf");
      requireNonEmpty(output.sourceDigest, "leaf: sourceDigest required");
      gateField(output.summary, "leaf", true);
      return;
    case "merge":
      validateInterval(output.interval, "merge");
      validateDigests(output.childDigests, "merge");
      gateField(output.summary, "merge", true);
      return;
    case "root":
      validateRootSections(output.sections);
      requireNonEmpty(output.coverageDigest, "root: coverageDigest required");
      return;
  }
}

/** One factory per node role; the generic ContractGenerator injects the profile. */
export function createSummaryContract(
  role: SummaryNodeRole,
): GenerationContract<SummaryRequestV1, SummaryNodeV1> {
  return {
    id: `rich-summary:${role}`,
    systemPrompt: RICH_PROMPT.bytes,
    encodeInput,
    parseFinal,
    validate: (output) => validate(role, output),
  };
}
