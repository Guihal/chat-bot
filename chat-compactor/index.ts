// Chat compactor — composition root. Registers one `session_before_compact`
// handler that replaces Pi's native summary with the chat-shaped structure from
// `contracts/chat-sections.ts`. The model is inherited from the session: no
// model id is ever configured here, and a failure of configuration or generation
// leaves native compaction in charge (the chat must never lose compaction).
import { join } from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@code-yeongyu/senpi";
import { convertToLlm } from "@code-yeongyu/senpi";
import { agentDir } from "./lib/agent-dir.js";
import { loadRichConfig, RichConfigParseError } from "./lib/rich-compaction/config.js";
import { ContractGenerator } from "./lib/rich-compaction/contract-generator.js";
import { createSummaryContract } from "./lib/rich-compaction/contracts/summary-contract.js";
import { createVerifierContract } from "./lib/rich-compaction/contracts/verifier-contract.js";
import { logRichEvent } from "./lib/rich-compaction/diagnostics.js";
import {
  RichCompactionService,
  type RichGenerators,
} from "./lib/rich-compaction/rich-compaction-service.js";
import type { BuiltInStream } from "./lib/shared/isolated-pi-types.js";
import { IsolatedPiSessionRunner } from "./lib/shared/isolated-pi-session.js";
import { resolveTransforms } from "./lib/shared/provider-compatibility-transforms.js";

const RICH_CONFIG_PATH = join(agentDir(), "compactors/rich.json");

/**
 * The session id the provider needs for routing. opencode-go rejects a request
 * without `x-opencode-session` (400 `MissingSessionID`), and the native summary
 * path passes the same value through its stream options; a direct-contract call
 * that omits it would make every compaction fail on that provider.
 */
function sessionIdOf(ctx: ExtensionContext): string | undefined {
  const manager = (ctx as unknown as { sessionManager?: { getSessionId?: () => string } })
    .sessionManager;
  const id = manager?.getSessionId?.();
  return typeof id === "string" && id.trim() ? id : undefined;
}

export default function (pi: ExtensionAPI): void {
  let config;
  try {
    config = loadRichConfig(RICH_CONFIG_PATH);
  } catch (error) {
    const reasonCode =
      error instanceof RichConfigParseError ? error.message : "config-load";
    logRichEvent("config-error", { reasonCode });
    return;
  }

  if (!config.enabled) {
    logRichEvent("disabled", { reasonCode: "config-enabled-false" });
    return;
  }

  // The isolated runner streams compaction turns through the session's own
  // provider via the stock public `ctx.modelRegistry` (find + streamSimple).
  const transforms = resolveTransforms(config.providerCompatibilityTransforms);
  const buildGenerators = (ctx: ExtensionContext): RichGenerators => {
    const registry = ctx.modelRegistry;
    const stream: BuiltInStream = {
      findModel: (provider, modelId) => registry.find(provider, modelId),
      streamSimple: (model, context, options) => {
        const sessionId = options?.sessionId ?? sessionIdOf(ctx);
        return registry.streamSimple(
          model,
          context,
          sessionId ? { ...options, sessionId } : options,
        );
      },
    };
    const runner = new IsolatedPiSessionRunner(undefined, {
      stream,
      convertToLlm,
      transforms,
    });
    const summary = (role: "leaf" | "merge" | "root") =>
      new ContractGenerator(runner, createSummaryContract(role));
    return {
      summaryGenerators: {
        leaf: summary("leaf"),
        merge: summary("merge"),
        root: summary("root"),
      },
      verifierGenerator: new ContractGenerator(runner, createVerifierContract()),
    };
  };
  const service = new RichCompactionService({
    buildGenerators,
    config,
    now: Date.now,
  });
  logRichEvent("loaded", { config: RICH_CONFIG_PATH });
  pi.on("session_before_compact", (e, ctx) => service.compact(e, ctx));
}
