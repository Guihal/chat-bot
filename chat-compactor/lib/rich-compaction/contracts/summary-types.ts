// Envelope types for the direct-Agent summarization contract.
// The model emits ONE JSON envelope per node; all chat content lives in the
// root section values. The chat structure itself is owned by chat-sections.ts.
import type { ChatSectionKey } from "./chat-sections.js";

/** A node role selects which envelope the model must emit. `repair` reruns the
 *  same source and yields one of the three node kinds. */
export type SummaryNodeRole = "leaf" | "merge" | "root" | "repair";

/** A contiguous half-open-free ordinal span of the immutable semantic source. */
export interface SourceInterval {
  readonly startOrdinal: number;
  readonly endOrdinal: number;
}

/** One leaf summarizes a contiguous source interval. */
export interface LeafNodeV1 {
  readonly kind: "leaf";
  readonly interval: SourceInterval;
  readonly sourceDigest: string;
  readonly summary: string;
}

/** One merge fans in adjacent child summaries. */
export interface MergeNodeV1 {
  readonly kind: "merge";
  readonly interval: SourceInterval;
  readonly childDigests: readonly string[];
  readonly summary: string;
}

/** A prose section is one paragraph; an item section is a list of facts. Which
 *  one a key carries is fixed by its registry entry. */
export type ChatSectionValue = string | readonly string[];

/** The root sections that are present in this summary; absent keys are simply
 *  not reported. Required keys are enforced by the contract, not by the type. */
export type RootSectionsV1 = Readonly<
  Partial<Record<ChatSectionKey, ChatSectionValue>>
>;

export interface RootSummaryV1 {
  readonly kind: "root";
  readonly sections: RootSectionsV1;
  readonly coverageDigest: string;
}

export type SummaryNodeV1 = LeafNodeV1 | MergeNodeV1 | RootSummaryV1;

export interface VerifierVerdictV1 {
  readonly ok: boolean;
  readonly issues?: readonly string[];
}
