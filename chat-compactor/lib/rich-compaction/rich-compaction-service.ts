import type {
  CompactionResult,
  ExtensionContext,
  SessionBeforeCompactEvent,
} from "@code-yeongyu/senpi";
import type { ContractGenerator } from "./contract-generator.js";
import type { VerificationRequestV1 } from "./contracts/verifier-contract.js";
import type { VerifierVerdictV1 } from "./contracts/summary-types.js";
import { logRichEvent } from "./diagnostics.js";
import { assemble } from "./final-assembly.js";
import { decodePreviousSummary } from "./host-codec.js";
import { resolveRoleProfile, type SessionModel } from "./model-profiles.js";
import type { RichConfig } from "./rich-types.js";
import { RichFailure } from "./rich-types.js";
import type { SummaryGenerators } from "./semantic-node.js";
import { buildSemanticTree } from "./semantic-plan.js";
import { executeSemanticTree } from "./semantic-tree.js";
import { buildRecovery, describeCauses, identityOf, toCompactionResult } from "./service-support.js";
import { buildSourceManifest } from "./source-manifest.js";
import { renderSemanticRecord } from "./source-renderer.js";
import { buildUnits } from "./utf8-chunker.js";

/** Structural shape of SessionBeforeCompactResult (not re-exported by the root). */
type CompactResult = { cancel?: boolean; compaction?: CompactionResult };

/** Per-role generators, built from the session's runtime `ctx.modelRegistry`
 *  (find + streamSimple), hence constructed lazily at the first compact. */
export interface RichGenerators {
  readonly summaryGenerators: SummaryGenerators;
  readonly verifierGenerator?: ContractGenerator<
    VerificationRequestV1,
    VerifierVerdictV1
  >;
}

export interface RichServiceDeps {
  readonly buildGenerators: (ctx: ExtensionContext) => RichGenerators;
  readonly config: RichConfig;
  readonly now: () => number;
}

/**
 * Owns one compaction: guard → identity → semantic source → hierarchy →
 * verification/recovery → assembly → return. Every failure is logged and becomes
 * a local log and native Pi fallback; explicit user abort still cancels.
 */
export class RichCompactionService {
  private generators?: RichGenerators;

  constructor(private readonly deps: RichServiceDeps) {}

  async compact(
    event: SessionBeforeCompactEvent,
    ctx: ExtensionContext,
  ): Promise<CompactResult | undefined> {
    if (event.signal.aborted) {
      logRichEvent("cancelled", { reason: event.reason, reasonCode: "aborted" });
      return { cancel: true };
    }
    try {
      return { compaction: await this.run(event, ctx) };
    } catch (error) {
      if (error instanceof RichFailure)
        logRichEvent("fallback", {
          reason: event.reason,
          kind: error.kind,
          stage: error.stage,
          reasonCode: error.reasonCode,
          cause: describeCauses(error.cause),
        });
      else
        logRichEvent("unexpected", {
          reason: event.reason,
          message: describeCauses(error),
        });
      return event.signal.aborted ? { cancel: true } : undefined;
    }
  }

  private async run(
    event: SessionBeforeCompactEvent,
    ctx: ExtensionContext,
  ): Promise<CompactionResult> {
    const { config } = this.deps;
    const sessionModel = identityOf(ctx);
    logRichEvent("identity", { provider: sessionModel.provider, model: sessionModel.id });
    // Capability is runtime-only, so generators are built lazily and cached.
    this.generators ??= this.deps.buildGenerators(ctx);
    const p = event.preparation;
    // Prior summaries re-enter the manifest; any host sections a desktop summary
    // carried are stripped, so only the chat body is honoured as history.
    const decoded = decodePreviousSummary(p.previousSummary);
    const rendered = buildSourceManifest({
      semanticPreviousSummary: decoded.semanticPreviousSummary,
      messagesToSummarize: p.messagesToSummarize,
      turnPrefixMessages: p.turnPrefixMessages,
    }).map(renderSemanticRecord);

    const units = buildUnits(rendered);
    const plan = buildSemanticTree(units);

    const result = await executeSemanticTree(
      plan,
      this.generators.summaryGenerators,
      resolveRoleProfile("summarizer", config, sessionModel),
      event.signal,
      buildRecovery({
        config,
        sessionModel,
        event,
        verifier: this.generators.verifierGenerator,
      }),
    );

    const assembled = assemble({ rootMarkdown: result.rootMarkdown });

    logRichEvent("accepted", {
      reason: event.reason,
      calls: result.calls,
      coverageDigest: result.coverageDigest,
      reasonCode: "",
    });
    return toCompactionResult({
      preparation: p,
      assembled,
      summaryUsage: result.usage,
      coverageDigest: result.coverageDigest,
      calls: result.calls,
      unitCount: units.length,
    });
  }
}
