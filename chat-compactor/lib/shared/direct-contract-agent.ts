import { Agent, type AgentMessage } from "@earendil-works/pi-agent-core";
import type { Api, AssistantMessage, Context, Model, ModelsSimpleStreamOptions } from "@earendil-works/pi-ai";
import type {
  BuiltInStream,
  DirectAgentDependencies,
  DirectContractRequest,
  DirectFailureKind,
  InvocationDiagnostics,
  IsolatedPiInvocation,
  IsolatedPiSessionResult,
} from "./isolated-pi-types.js";
import { emptyDiagnostics, evidence } from "./direct-contract-diagnostics.js";
import { applyProviderTransforms } from "./provider-compatibility-transforms.js";
/** Execute one contract turn through public Agent and the sealed built-in seam. */
export function executeDirectContract(
  request: DirectContractRequest,
  dependencies: DirectAgentDependencies,
): IsolatedPiInvocation {
  let agent: Agent | undefined;
  let aborted = false;
  let timedOut = false;
  let providerCalls = 0;
  let captured: Context | undefined;
  let postTransform: Record<string, unknown> | undefined;
  let streamFailure: { kind: "capability" | "transport"; message: string } | undefined;
  // onPayload observes ONLY the API-native serialised payload — never Context.
  const onPayload = (payload: unknown): Record<string, unknown> => {
    try { return (postTransform = applyProviderTransforms(payload as Record<string, unknown>, dependencies.transforms!)); }
    catch (error) { streamFailure = { kind: "capability", message: message(error) }; throw error; }
  };
  let cancellationSettled = false;
  let cancelResult: (result: IsolatedPiSessionResult) => void = () => {};
  const cancellation = new Promise<IsolatedPiSessionResult>((resolve) => {
    cancelResult = resolve;
  });
  const abort = (): void => {
    aborted = true;
    agent?.abort();
    if (!cancellationSettled) {
      cancellationSettled = true;
      cancelResult(failed(timedOut ? "timeout" : "aborted", timedOut ? "Direct invocation timed out" : "Direct invocation aborted", evidence(request, captured, providerCalls)));
    }
  };
  const execution = execute().catch((error) =>
    failed(classify(error), message(error), evidence(request, captured, providerCalls)));
  const result = Promise.race([execution, cancellation]).finally(() => cleanup());
  const timer = setTimeout(() => {
    timedOut = true;
    abort();
  }, request.timeoutMs);
  const onAbort = (): void => abort();
  request.signal?.addEventListener("abort", onAbort, { once: true });
  function cleanup(): void {
    clearTimeout(timer);
    request.signal?.removeEventListener("abort", onAbort);
  }
  async function execute(): Promise<IsolatedPiSessionResult> {
    if (request.signal?.aborted)
      return failed("aborted", "Direct invocation aborted before start", emptyDiagnostics(request));
    const { stream } = dependencies;
    let model: Model<Api> | undefined;
    try {
      model = stream.findModel(request.provider, request.model);
    } catch (error) {
      return failed("capability", message(error), emptyDiagnostics(request));
    }
    if (!model)
      return failed("capability", "Unknown compaction model for provider", emptyDiagnostics(request));
    const streamFn = (_model: Model<Api>, context: Context, options?: ModelsSimpleStreamOptions) => {
      providerCalls += 1;
      captured = context;
      try {
        assertContext(request, context, providerCalls);
      } catch (error) {
        streamFailure = { kind: "capability", message: message(error) };
        throw error;
      }
      const streamOptions = dependencies.transforms?.length ? { ...options, onPayload } : options;
      try {
        return stream.streamSimple(model, context, streamOptions);
      } catch (error) {
        streamFailure ??= { kind: "transport", message: message(error) };
        throw error;
      }
    };
    agent = new Agent({
      initialState: { systemPrompt: request.systemPrompt, model, thinkingLevel: request.thinkingLevel, tools: [] },
      convertToLlm: async (messages) => {
        try {
          return await dependencies.convertToLlm(messages);
        } catch (error) {
          streamFailure = { kind: "capability", message: message(error) };
          throw error;
        }
      },
      streamFn,
      // pi 0.86+ removed shouldStopAfterTurn; FinishTurn {action:"end"} is the
      // equivalent hard stop after one completed turn.
      finishTurn: () => ({ action: "end" }),
    });
    if (aborted) return failed("aborted", "Direct invocation aborted before prompt", emptyDiagnostics(request));
    try {
      await agent.prompt(request.userMessage);
    } catch (error) {
      return failed(streamFailure?.kind ?? "transport", streamFailure?.message ?? message(error), evidence(request, captured, providerCalls));
    }
    const diagnostics = evidence(request, captured, providerCalls, postTransform);
    if (timedOut) return failed("timeout", "Direct invocation timed out", diagnostics);
    if (aborted) return failed("aborted", "Direct invocation aborted", diagnostics);
    if (streamFailure) return failed(streamFailure.kind, streamFailure.message, diagnostics);
    const assistant = lastAssistant(agent.state.messages);
    if (!assistant) return failed("model-output", "Direct invocation returned no assistant", diagnostics);
    if (providerCalls !== 1) return failed("capability", "Direct invocation made multiple provider calls", diagnostics);
    if (assistant.content.some((block) => block.type === "toolCall"))
      return failed("model-output", "Tool calls are forbidden in contract output", diagnostics);
    if (assistant.stopReason !== "stop")
      return failed(assistant.stopReason === "error" || assistant.stopReason === "aborted" ? "transport" : "model-output", `Rejected stop reason: ${assistant.stopReason}${stopDetail(assistant)}`, diagnostics);
    const text = assistant.content.filter((block) => block.type === "text").map((block) => block.text).join("");
    if (!text) return failed("model-output", "Direct invocation returned no final text", diagnostics);
    return { ok: true, profile: "direct-contract", text, assistant, usage: assistant.usage, diagnostics };
  }
  return Object.freeze({ result, abort });
}
function assertContext(request: DirectContractRequest, context: Context, calls: number): void {
  if (calls !== 1 || (context.tools?.length ?? 0) !== 0)
    throw new CapabilityViolation("Direct Agent context contract violated");
  // pi 0.87+ TranscriptContext: system prompt and tool declarations live in the
  // leading system message; exact string content proves none were injected.
  const messages = context.messages;
  const system = messages[0];
  const user = messages[1];
  if (messages.length !== 2 || system?.role !== "system" || system.content !== request.systemPrompt)
    throw new CapabilityViolation("Direct Agent context contract violated");
  if (user === undefined || JSON.stringify(user) !== JSON.stringify(request.userMessage))
    throw new CapabilityViolation("Direct Agent user message contract violated");
}
function lastAssistant(messages: readonly AgentMessage[]): AssistantMessage | undefined {
  return [...messages].reverse().find((item) => item?.role === "assistant");
}

/** The provider's own reason for a failed turn. Without it the operator sees only
 *  "stop reason: error" and cannot tell a usage limit from a rejected request. */
function stopDetail(assistant: AssistantMessage): string {
  const detail = assistant.errorMessage?.trim();
  if (detail) return `: ${detail}`;
  if (assistant.errorCause) return `: ${JSON.stringify(assistant.errorCause)}`;
  return "";
}
function failed(kind: DirectFailureKind, text: string, diagnostics: InvocationDiagnostics): IsolatedPiSessionResult {
  return { ok: false, profile: "direct-contract", error: { kind, message: text }, diagnostics };
}
const message = (error: unknown): string => error instanceof Error ? error.message : String(error);
const classify = (error: unknown): "capability" | "transport" =>
  error instanceof CapabilityViolation ? "capability" : "transport";
class CapabilityViolation extends Error {
  override readonly name = "CapabilityViolation";
}
