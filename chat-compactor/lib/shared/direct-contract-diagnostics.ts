import { createHash } from "node:crypto";
import type { Context } from "@earendil-works/pi-ai";
import type {
  DirectContractRequest,
  InvocationDiagnostics,
} from "./isolated-pi-types.js";
import { canonicalJson } from "./provider-compatibility-transforms.js";

/** Bounded structural diagnostics for the direct-contract path. Prompt/response
 *  bodies stay out; only digests and counts are exposed. */
export const sha256 = (value: string): string =>
  createHash("sha256").update(value).digest("hex");

export function evidence(
  request: DirectContractRequest,
  context: Context | undefined,
  providerCalls: number,
  postTransform?: Record<string, unknown>,
): InvocationDiagnostics {
  const postTransformSha256 = postTransform
    ? sha256(canonicalJson(postTransform))
    : undefined;
  // pi 0.87+ TranscriptContext: the system prompt is the leading system
  // message's string content; the user message follows it.
  const messages = context?.messages ?? [];
  const system = messages[0]?.role === "system" ? messages[0] : undefined;
  const systemPrompt =
    typeof system?.content === "string"
      ? system.content
      : (context?.systemPrompt ?? request.systemPrompt);
  const user = [...messages].reverse().find((item) => item.role === "user");
  const blocks = Array.isArray(user?.content) ? user.content : [];
  const terminal = blocks.at(-1);
  return {
    providerCalls,
    systemPromptSha256: sha256(systemPrompt),
    userMessages: messages.filter((item) => item.role === "user").length,
    tools: context?.tools?.length ?? 0,
    resources: 0,
    extensions: 0,
    terminalTextSha256:
      terminal?.type === "text" ? sha256(terminal.text) : undefined,
    ...(postTransformSha256 ? { postTransformSha256 } : {}),
  };
}

export function emptyDiagnostics(
  request: DirectContractRequest,
): InvocationDiagnostics {
  return {
    providerCalls: 0,
    systemPromptSha256: sha256(request.systemPrompt),
    userMessages: 0,
    tools: 0,
    resources: 0,
    extensions: 0,
  };
}
