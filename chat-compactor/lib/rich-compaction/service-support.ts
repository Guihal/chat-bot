import type { AgentMessage } from "@earendil-works/pi-agent-core";
import type {
  CompactionResult,
  ExtensionContext,
  SessionBeforeCompactEvent,
} from "@code-yeongyu/senpi";
import type { ContractGenerator } from "./contract-generator.js";
import type { VerificationRequestV1 } from "./contracts/verifier-contract.js";
import type { VerifierVerdictV1 } from "./contracts/summary-types.js";
import type { AssemblyResult } from "./final-assembly.js";
import {
  resolveRoleProfile,
  type SessionModel,
} from "./model-profiles.js";
import type { RichConfig } from "./rich-types.js";
import { RichFailure } from "./rich-types.js";
import type { RecoveryOptions } from "./semantic-tree.js";
import { sha256Hex } from "./source-renderer.js";

/** Walk the Error `cause` chain into one string — the deepest cause is the
 *  specific gate (e.g. a contract validate message), which the top-level
 *  "validation failed" wrapper hides. Metadata only, no prompt/response bodies. */
export function describeCauses(error: unknown): string | undefined {
  const parts: string[] = [];
  let cur: unknown = error;
  for (let i = 0; i < 5 && cur instanceof Error; i++) {
    parts.push(cur.message);
    cur = (cur as { cause?: unknown }).cause;
  }
  return parts.length ? parts.join(" <= ") : undefined;
}

/** Read the last user-authored text — the "latest ask" the verifier judges
 *  next-steps against. Empty when the branch opens on a non-user turn. */
export function latestUserRequest(messages: readonly AgentMessage[]): string {
  for (let i = messages.length - 1; i >= 0; i--) {
    const m = messages[i] as { role?: string; content?: unknown };
    if (m.role !== "user") continue;
    if (typeof m.content === "string") return m.content;
    if (Array.isArray(m.content)) {
      const text = m.content
        .filter((b) => b && (b as { type?: string }).type === "text")
        .map((b) => (b as { text: string }).text)
        .join("\n");
      if (text) return text;
    }
  }
  return "";
}

/** The inherited session model; absence is a capability failure, never a swap
 *  (invariant 13 — the compactor never picks a model, it inherits ctx.model). */
export function identityOf(ctx: ExtensionContext): SessionModel {
  const model = ctx.model as { id?: string; provider?: string } | undefined;
  if (
    !model ||
    typeof model.id !== "string" ||
    typeof model.provider !== "string"
  )
    throw new RichFailure("capability-schema", "identity", "no-active-model");
  return { provider: model.provider, id: model.id };
}

/** Recovery wiring for one run: repair/fallback always; the root verifier and
 *  its latest-ask context only when a verifier generator is available. */
export function buildRecovery(args: {
  readonly config: RichConfig;
  readonly sessionModel: SessionModel;
  readonly event: SessionBeforeCompactEvent;
  readonly verifier?: ContractGenerator<VerificationRequestV1, VerifierVerdictV1>;
}): RecoveryOptions {
  const { config, sessionModel, event, verifier } = args;
  const base: RecoveryOptions = {
    repairProfile: resolveRoleProfile("repair", config, sessionModel),
    fallbackProfile: resolveRoleProfile("fallback", config, sessionModel),
  };
  if (!verifier) return base;
  return {
    ...base,
    verifier,
    verifierProfile: resolveRoleProfile("verifier", config, sessionModel),
    verification: {
      latestRequest: latestUserRequest([
        ...event.preparation.messagesToSummarize,
        ...event.preparation.turnPrefixMessages,
      ]),
      continuation: "",
      customInstructions: event.customInstructions,
    },
  };
}

/** Shape the CompactionResult pi persists: the assembled summary, the branch
 *  cut point echoed back verbatim, and chat-compactor details. The bot's
 *  compactor owns no file or skill inventories, so those stay empty while the
 *  pi-side shape is preserved. */
export function toCompactionResult(args: {
  readonly preparation: SessionBeforeCompactEvent["preparation"];
  readonly assembled: AssemblyResult;
  readonly summaryUsage: CompactionResult["usage"];
  readonly coverageDigest: string;
  readonly calls: number;
  readonly unitCount: number;
}): CompactionResult {
  const { preparation: p, assembled } = args;
  return {
    summary: assembled.summary,
    firstKeptEntryId: p.firstKeptEntryId,
    tokensBefore: p.tokensBefore,
    usage: args.summaryUsage,
    details: {
      schema: "chat-compactor/v1",
      readFiles: [],
      modifiedFiles: [],
      skills: [],
      coverageDigest: args.coverageDigest,
      sourceIdentity: sha256Hex(
        `${p.firstKeptEntryId}:${args.unitCount}:${args.coverageDigest}`,
      ),
      calls: args.calls,
    },
  };
}
