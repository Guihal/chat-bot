import type { GenerationContract } from "../generation-contract.js";
import type { DirectContent } from "../../shared/isolated-pi-types.js";
import { loadRichPrompt } from "../rich-prompt.js";
import { renderRootMarkdown } from "../deterministic-validation.js";
import type { RootSummaryV1, VerifierVerdictV1 } from "./summary-types.js";

/** The single semantic system prompt; the verifier reads the same rich.md
 *  contract as the summarizer (invariant 5), differing only by its request. */
const RICH_PROMPT = loadRichPrompt();

/** One root-summary verification request. Root-only fields (latest request,
 *  continuation, custom instructions) let the verifier judge continuation fit. */
export interface VerificationRequestV1 {
  readonly candidate: RootSummaryV1;
  readonly latestRequest: string;
  readonly continuation: string;
  readonly customInstructions?: string;
}

function renderVerification(input: VerificationRequestV1): string {
  const lines = [
    "<<<RICH:VERIFY candidate chat summary follows>>>",
    renderRootMarkdown(input.candidate),
    `## latestRequest\n${input.latestRequest}`,
    `## continuation\n${input.continuation}`,
  ];
  if (input.customInstructions)
    lines.push(`## customInstructions\n${input.customInstructions}`);
  lines.push('Return ONE JSON verdict {"ok":boolean,"issues"?:string[]}.');
  return lines.join("\n\n");
}

function encodeInput(input: VerificationRequestV1): readonly DirectContent[] {
  return [{ type: "text", text: renderVerification(input) }];
}

function parseFinal(text: string): VerifierVerdictV1 {
  const obj = JSON.parse(text.trim()) as { ok?: unknown; issues?: unknown };
  if (typeof obj.ok !== "boolean")
    throw new Error("verifier: `ok` must be a boolean");
  return {
    ok: obj.ok,
    issues: Array.isArray(obj.issues) ? (obj.issues as string[]) : undefined,
  };
}

/** A rejecting verdict must carry at least one concrete issue; an accepting
 *  verdict needs none. */
function validate(output: VerifierVerdictV1): void {
  if (typeof output.ok !== "boolean")
    throw new Error("verifier: `ok` must be a boolean");
  if (output.ok) return;
  if (
    !Array.isArray(output.issues) ||
    output.issues.length === 0 ||
    output.issues.some((i) => typeof i !== "string" || !i.trim())
  )
    throw new Error("verifier: ok:false requires non-empty string issues");
}

/** A separate plain contract for a second instance of the SAME generic
 *  generator — a distinct injected contract + verifier model profile
 *  (inherited model at a distinct thinking level; invariant 7). */
export function createVerifierContract(): GenerationContract<
  VerificationRequestV1,
  VerifierVerdictV1
> {
  return {
    id: "rich-verifier:root",
    systemPrompt: RICH_PROMPT.bytes,
    encodeInput,
    parseFinal,
    validate,
  };
}
