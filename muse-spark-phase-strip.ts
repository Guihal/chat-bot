// muse-spark-phase-strip — drop the terminal `phase` marker the provider rejects.
//
// Muse Spark through opencode-go (OpenAI Responses API) emits assistant
// messages carrying `phase: "commentary"`. The gateway rejects a replayed
// history whose turn-ending assistant message carries that marker with a
// generic 400 invalid_request_error; the host replays the whole history on
// every turn, so the session dead-ends. Live probes on 2026-11-02: the exact
// recorded input → 400; the same input with `phase` removed from the last
// assistant message of the turn → 200; keeping only that phase → 400.
//
// Scope is deliberately narrow: model id `muse-spark-1.3-contributor` and the
// Responses wire shape (`payload.input`) only. Non-terminal `phase` values are
// accepted by the provider and stay untouched; every other field is preserved.
//
// Performance: one pass over the already-built in-memory payload, with an
// in-place `delete` on the few terminal messages. No clone, no JSON.stringify,
// no per-request allocation of the message array — a 4 MB history costs one
// scan and a handful of property deletions.
//
// Port note (2026-11-03): this is the hosted copy for the isolated chat bot.
// The extension dir there is a bare state directory staged from the launch
// spec, so the file is self-contained on purpose: node builtins only, no
// `jsonc-parser`, no `@code-yeongyu/senpi` import — the host passes extension
// API objects at runtime, and types are declared locally. The logic below is
// identical to ~/.omo/agent/extensions/lib/muse-spark-phase-strip.ts.

import { appendFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const TARGET_MODEL_ID = "muse-spark-1.3-contributor";

type ModelRef = { id?: string };

/** Minimal surface this extension uses; the host supplies the real object. */
interface ExtensionApi {
  on(event: string, handler: (event: any, ctx: any) => unknown): void;
}

/**
 * Agent directory used for the observability log. The bot's host exports
 * OMO_CODING_AGENT_DIR; the same fallbacks as the local extension dir apply so
 * the file behaves the same in both places.
 */
function agentDir(): string {
  const fromEnv =
    process.env.OMO_CODING_AGENT_DIR ||
    process.env.SENPI_CODING_AGENT_DIR ||
    process.env.PI_CODING_AGENT_DIR;
  return fromEnv && fromEnv.length > 0 ? fromEnv : join(homedir(), ".omo", "agent");
}

/** Target model only: exact id or a catalog-prefixed form. */
export function isTargetModel(model: ModelRef | undefined): boolean {
  const id = (model?.id ?? "").toLowerCase();
  return id === TARGET_MODEL_ID || id.endsWith(`/${TARGET_MODEL_ID}`);
}

/**
 * A turn ends at a `role === "user"` item or at the end of the array; the
 * terminal assistant message is the last one before that boundary. It stays
 * terminal when a `function_call`/`function_call_output` follows it — that is
 * exactly the recorded shape that 400s while carrying `phase`.
 *
 * Mutates the existing item objects in place and returns the number of removed
 * markers. Malformed payloads are a no-op, never a throw.
 */
export function stripTerminalPhase(payload: unknown, model?: ModelRef): number {
  if (!isTargetModel(model)) return 0;
  if (payload === null || typeof payload !== "object") return 0;
  const input = (payload as { input?: unknown }).input;
  if (!Array.isArray(input)) return 0;

  let stripped = 0;
  let terminal: Record<string, unknown> | undefined;
  const flush = () => {
    if (terminal !== undefined && "phase" in terminal) {
      delete terminal.phase;
      stripped += 1;
    }
    terminal = undefined;
  };

  for (const item of input) {
    if (item === null || typeof item !== "object") continue;
    const record = item as Record<string, unknown>;
    if (record.role === "user") {
      flush();
      continue;
    }
    if (record.role === "assistant") terminal = record;
  }
  flush();
  return stripped;
}

function logApplied(model: ModelRef | undefined, inputLength: number, stripped: number): void {
  try {
    appendFileSync(
      join(agentDir(), "logs/muse-spark-phase-strip.log"),
      `${JSON.stringify({
        ts: new Date().toISOString(),
        model: model?.id,
        input_length: inputLength,
        stripped,
      })}\n`,
      { mode: 0o600 },
    );
  } catch {
    // Observability must never break the provider request.
  }
}

export default function museSparkPhaseStrip(pi: ExtensionApi): void {
  pi.on("before_provider_request", (event: { payload: unknown }, ctx: any) => {
    const model = ctx?.model as ModelRef | undefined;
    try {
      const stripped = stripTerminalPhase(event.payload, model);
      if (stripped === 0) return undefined;
      const length = Array.isArray((event.payload as { input?: unknown }).input)
        ? (event.payload as { input: unknown[] }).input.length
        : 0;
      logApplied(model, length, stripped);
      return event.payload;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (message.includes("stale after session replacement")) return undefined;
      throw error;
    }
  });
}
