/**
 * Request-local output retry for Alibaba inspection errors and hy4 content filters.
 *
 * Behavior contract: the same request object is re-sent without replaying tools,
 * editing messages, restarting an agent or changing the session. Partial events
 * from rejected attempts stay inside this wrapper; exactly one assistant `start`
 * is emitted for the accepted answer. Abort and unrelated errors pass through.
 * `qwen-token-plan-individual` keeps the original 5 retries; opencode-go's
 * `hy4-flash-free` gets up to 20 retries because the runtime classifier treats
 * `Provider finish_reason: content_filter` as unknown and does not retry it.
 *
 * Provider factories come from the runtime's virtual module so this file can be
 * copied unchanged into any agent profile.
 */

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import type {
  AssistantMessage,
  AssistantMessageEventStream,
} from "@earendil-works/pi-ai";

export const ALIBABA_PROVIDER = "qwen-token-plan-individual";
export const OPENCODE_GO_PROVIDER = "opencode-go";
export const HY4_MODEL_ID = "hy4-flash-free";
export const ALIBABA_OUTPUT_RETRIES = 5;
export const HY4_OUTPUT_RETRIES = 20;

type ModelRef = { provider?: string; id?: string };
type StreamFn = (
  model: any,
  context: any,
  options?: any,
) => AssistantMessageEventStream;
type Wait = (ms: number, signal?: AbortSignal) => Promise<void>;
type RetryPolicy = {
  maxRetries: number;
  reason: string;
  isRetryable(message: AssistantMessage): boolean;
};

const wait: Wait = async (ms, signal) => {
  await sleep(ms, undefined, { signal });
};

function agentDir(): string {
  const value =
    process.env.OMO_CODING_AGENT_DIR ||
    process.env.SENPI_CODING_AGENT_DIR ||
    path.join(os.homedir(), ".omo", "agent");
  return path.resolve(value);
}

function logAttempt(data: Record<string, unknown>): void {
  try {
    const dir = agentDir();
    const logs = path.join(dir, "logs");
    fs.mkdirSync(logs, { recursive: true, mode: 0o700 });
    fs.appendFileSync(
      path.join(logs, "alibaba-output-retry.log"),
      JSON.stringify({ ts: new Date().toISOString(), ...data }) + "\n",
      { mode: 0o600 },
    );
  } catch {
    /* diagnostics must never fail a request */
  }
}

/** Alibaba inspection errors arrive as plain text or JSON/SSE-wrapped 400s. */
export function isInspectionError(text: string): boolean {
  return (
    /\b(?:data_inspection_failed|DataInspectionFailed)\b/i.test(text) ||
    /\b(?:input(?:\s+or\s+output)?|output)(?:\s+text)?\s+data\s+may\s+contain\s+inappropriate\s+content\b/i.test(
      text,
    )
  );
}

export function isInspectionRejection(message: AssistantMessage): boolean {
  return (
    message.stopReason === "error" &&
    isInspectionError(message.errorMessage ?? "")
  );
}

export function isHy4Rejection(message: AssistantMessage): boolean {
  if (message.stopReason !== "error") return false;
  const text = message.errorMessage ?? "";
  return /\bcontent_filter\b/i.test(text) || isInspectionError(text);
}

export function isHy4Model(model: ModelRef | undefined): boolean {
  if (!model) return false;
  const id = (model.id ?? "").toLowerCase();
  return id === HY4_MODEL_ID || id.endsWith(`/${HY4_MODEL_ID}`);
}

/** First matching policy wins; unrelated providers/models are never wrapped. */
export function retryPolicy(model: ModelRef | undefined): RetryPolicy | null {
  if (model?.provider === ALIBABA_PROVIDER) {
    return {
      maxRetries: ALIBABA_OUTPUT_RETRIES,
      reason: "inspection",
      isRetryable: isInspectionRejection,
    };
  }
  if (model?.provider === OPENCODE_GO_PROVIDER && isHy4Model(model)) {
    return {
      maxRetries: HY4_OUTPUT_RETRIES,
      reason: "content_filter",
      isRetryable: isHy4Rejection,
    };
  }
  return null;
}

export function withOutputRetry(
  send: StreamFn,
  createStream: () => AssistantMessageEventStream,
  onRetry: (attempt: number, model: any, reason: string) => void = () => {},
  pause: Wait = wait,
): StreamFn {
  return (model, context, options) => {
    const policy = retryPolicy(model);
    if (!policy) return send(model, context, options);

    const output = createStream();
    let started = false;
    const cancelled = (message: AssistantMessage) => {
      const error = {
        ...message,
        stopReason: "aborted" as const,
        errorMessage: "Request aborted",
      };
      output.push({ type: "error", reason: "aborted", error });
      output.end();
    };

    async function run() {
      for (let attempt = 0; ; attempt++) {
        let rejected: AssistantMessage | undefined;
        const input = send(model, context, options);
        for await (const event of input) {
          if (event.type === "error") {
            if (
              attempt < policy.maxRetries &&
              !options?.signal?.aborted &&
              policy.isRetryable(event.error)
            ) {
              rejected = event.error;
              break;
            }
            output.push(event);
            output.end();
            return;
          }
          if (event.type === "done") {
            output.push(event);
            output.end();
            return;
          }
          if (event.type === "start") {
            if (!started) {
              started = true;
              output.push(event);
            }
          } else {
            output.push(event);
          }
        }
        if (!rejected)
          throw Error(
            "Retryable provider stream ended without a terminal event",
          );
        try {
          onRetry(attempt + 1, model, policy.reason);
        } catch {
          /* diagnostics cannot fail a request */
        }
        try {
          await pause(Math.min(500 * 2 ** attempt, 4000), options?.signal);
        } catch (error) {
          if (!options?.signal?.aborted) throw error;
          cancelled(rejected);
          return;
        }
        if (options?.signal?.aborted) {
          cancelled(rejected);
          return;
        }
      }
    }

    void run().catch((error) => output.fail(error));
    return output;
  };
}

function register(
  pi: any,
  provider: any,
  createStream: () => AssistantMessageEventStream,
): void {
  const log = (attempt: number, model: any, reason: string) => {
    logAttempt({
      provider: provider.id,
      model: model?.id,
      attempt,
      maxRetries: retryPolicy(model)?.maxRetries ?? 0,
      reason,
    });
  };
  pi.registerProvider({
    ...provider,
    stream: withOutputRetry(provider.stream.bind(provider), createStream, log),
    streamSimple: withOutputRetry(
      provider.streamSimple.bind(provider),
      createStream,
      log,
    ),
  });
}

export default async function alibabaOutputRetryPort(pi: any): Promise<void> {
  const [{ createAssistantMessageEventStream }, { builtinProviders }] =
    await Promise.all([
      import("@earendil-works/pi-ai"),
      import("@earendil-works/pi-ai/providers/all"),
    ]);
  const providers = builtinProviders();
  for (const id of [ALIBABA_PROVIDER, OPENCODE_GO_PROVIDER]) {
    const provider = providers.find((candidate: any) => candidate.id === id);
    if (!provider) throw Error(`Missing provider factory: ${id}`);
    register(pi, provider, createAssistantMessageEventStream);
  }
}
