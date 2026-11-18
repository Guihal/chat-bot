import type { AgentMessage } from "@earendil-works/pi-agent-core";

export type SourceKind =
  | "prior-state"
  | "user"
  | "assistant"
  | "toolResult"
  | "custom"
  | "branchSummary";

/** One immutable source record at a record-level ordinal. The original message
 *  is carried unchanged; rendering (source-renderer) derives its byte frames. */
export interface SourceRecord {
  readonly ordinal: number;
  readonly kind: SourceKind;
  readonly message: AgentMessage;
}

export interface SourceManifestInput {
  readonly semanticPreviousSummary?: string;
  readonly messagesToSummarize: readonly AgentMessage[];
  readonly turnPrefixMessages: readonly AgentMessage[];
}

function classify(message: AgentMessage): SourceKind {
  const role = (message as { role?: string }).role;
  if (role === "user") return "user";
  if (role === "assistant") return "assistant";
  if (role === "toolResult") return "toolResult";
  return "custom";
}

/** Synthetic carrier for the semantic remainder of a prior compaction. A neutral
 *  user text message; `kind` marks it and it appears at most once, first. */
function priorStateMessage(text: string): AgentMessage {
  return { role: "user", content: [{ type: "text", text }], timestamp: 0 };
}

/**
 * Builds the ordered immutable source manifest: an optional single `prior-state`
 * record (only when the semantic remainder is non-empty), then every
 * `messagesToSummarize`, then every `turnPrefixMessages`. Record ordinals are
 * dense and 0-based; there is exactly one prior-state and it is first.
 */
export function buildSourceManifest(
  input: SourceManifestInput,
): readonly SourceRecord[] {
  const records: SourceRecord[] = [];
  const prior = input.semanticPreviousSummary?.trim();
  if (prior)
    records.push({
      ordinal: 0,
      kind: "prior-state",
      message: priorStateMessage(input.semanticPreviousSummary as string),
    });
  for (const message of [
    ...input.messagesToSummarize,
    ...input.turnPrefixMessages,
  ])
    records.push({
      ordinal: records.length,
      kind: classify(message),
      message,
    });
  return records;
}
