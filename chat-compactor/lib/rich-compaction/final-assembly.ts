import { assertChatSections } from "./deterministic-validation.js";

export interface AssemblyInput {
  /** The chat sections, already gated by executeSemanticTree. */
  readonly rootMarkdown: string;
}

export interface AssemblyResult {
  readonly summary: string;
}

/**
 * Compose the persisted summary. The bot's compactor owns no host inventories
 * (no file, skill or guide lists), so assembly is the semantic body plus one
 * final structure gate — assembly never emits a malformed summary.
 */
export function assemble(input: AssemblyInput): AssemblyResult {
  assertChatSections(input.rootMarkdown);
  return { summary: input.rootMarkdown };
}
