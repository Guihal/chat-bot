import { appendFileSync, mkdirSync } from "node:fs";
import { agentDir } from "../agent-dir.js";
import { dirname, join } from "node:path";
import type { InvocationDiagnostics } from "../shared/isolated-pi-types.js";

export type GenerationDiagnostic = InvocationDiagnostics & {
  readonly contractId: string;
};

const RICH_LOG_PATH = join(agentDir(), "logs/rich-compaction.log");
let logDirReady = false;

/**
 * Append one JSONL diagnostic line: metadata only (stage, reason, counts,
 * digests, reasonCode) — never prompt, response or skill bodies. A failed write
 * must never abort compaction (invariant 4), so this is the one sanctioned
 * catch-without-rethrow: the log is best-effort observability, not durable state.
 * The directory is created on the first write because a fresh agent profile has
 * no `logs/` yet, and the load/fallback lines are the operator's only evidence.
 */
export function logRichEvent(
  stage: string,
  fields: Record<string, unknown> = {},
): void {
  const line = JSON.stringify({
    ts: new Date().toISOString(),
    stage,
    ...fields,
  });
  try {
    if (!logDirReady) {
      mkdirSync(dirname(RICH_LOG_PATH), { recursive: true });
      logDirReady = true;
    }
    appendFileSync(RICH_LOG_PATH, `${line}\n`);
  } catch {
    // Observability is best-effort; a logging failure cannot break compaction.
  }
}

/** Diagnostics are bounded structural metadata; prompt/response bodies stay out. */
export function attachContractId(
  contractId: string,
  diagnostics: InvocationDiagnostics,
): GenerationDiagnostic {
  return Object.freeze({ ...diagnostics, contractId });
}
