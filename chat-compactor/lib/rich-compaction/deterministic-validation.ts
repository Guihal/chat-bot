import { RESERVED_HOST_MARKER } from "./contracts/summary-contract.js";
import {
  CHAT_SECTIONS,
  chatSectionOrder,
  type ChatSectionSpec,
} from "./contracts/chat-sections.js";
import type { RootSummaryV1 } from "./contracts/summary-types.js";
import { RichFailure } from "./rich-types.js";

const SECTION_HEADER = /^## (.+)$/gm;

/**
 * Envelope gate: the model text must be exactly one JSON object — no prose
 * preamble, no ``` code fence, no `<tool_call>` marker. This catches the recorded
 * incidents (a fenced leaf with a preamble; a "let me understand… <tool_call>"
 * continuation) before the typed contract validator runs. A failure is
 * `model-output`, routed to repair by the recovery policy.
 */
export function gateEnvelope(text: string): void {
  const trimmed = text.trim();
  if (!trimmed) throw new RichFailure("model-output", "envelope", "empty");
  if (trimmed.includes("<tool_call>"))
    throw new RichFailure("model-output", "envelope", "tool-call-marker");
  if (trimmed.startsWith("```"))
    throw new RichFailure("model-output", "envelope", "code-fence");
  if (!trimmed.startsWith("{") || !trimmed.endsWith("}"))
    throw new RichFailure("model-output", "envelope", "not-single-json-object");
  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {
    throw new RichFailure("model-output", "envelope", "json-parse");
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed))
    throw new RichFailure("model-output", "envelope", "not-object");
}

function renderSection(spec: ChatSectionSpec, value: string | readonly string[]): string {
  const body = Array.isArray(value)
    ? (value as readonly string[]).map((item) => `- ${item}`).join("\n")
    : (value as string);
  return `## ${spec.heading}\n\n${body}`.trimEnd();
}

/** Render the present root sections to markdown in registry order. Empty item
 *  sections are skipped, so the persisted summary carries no empty headings. */
export function renderRootMarkdown(root: RootSummaryV1): string {
  const rendered: string[] = [];
  for (const spec of CHAT_SECTIONS) {
    const value = root.sections[spec.key];
    if (value === undefined) continue;
    if (Array.isArray(value) && value.length === 0) continue;
    rendered.push(renderSection(spec, value as string | readonly string[]));
  }
  return rendered.join("\n\n");
}

/**
 * Final gate on the rendered summary: only registry headings, in registry order,
 * no repeats, no content before the first heading, a non-empty body under every
 * heading, every required section present, and no host `<<<RICH:` sentinel.
 */
export function assertChatSections(markdown: string): void {
  if (markdown.includes(RESERVED_HOST_MARKER))
    throw new RichFailure("model-output", "assembly", "host-marker-in-summary");
  const headers = [...markdown.matchAll(SECTION_HEADER)];
  if (headers.length === 0)
    throw new RichFailure("model-output", "assembly", "section-count");
  if (markdown.slice(0, headers[0].index ?? 0).trim() !== "")
    throw new RichFailure(
      "model-output",
      "assembly",
      "content-before-first-section",
    );
  const seen = new Set<string>();
  let lastOrder = -1;
  headers.forEach((match, i) => {
    const heading = match[1].trim();
    const spec = CHAT_SECTIONS.find((s) => s.heading === heading);
    if (!spec)
      throw new RichFailure("model-output", "assembly", "section-unknown");
    const order = chatSectionOrder(spec.key);
    if (order <= lastOrder)
      throw new RichFailure("model-output", "assembly", "section-order");
    if (seen.has(spec.key))
      throw new RichFailure("model-output", "assembly", "section-duplicate");
    seen.add(spec.key);
    lastOrder = order;
    const bodyStart = (match.index ?? 0) + match[0].length;
    const bodyEnd = headers[i + 1]?.index ?? markdown.length;
    if (markdown.slice(bodyStart, bodyEnd).trim() === "")
      throw new RichFailure("model-output", "assembly", "section-empty");
  });
  for (const spec of CHAT_SECTIONS)
    if (spec.required && !seen.has(spec.key))
      throw new RichFailure("model-output", "assembly", "section-missing");
}
