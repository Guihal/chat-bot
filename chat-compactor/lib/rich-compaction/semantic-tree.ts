import type { Usage } from "@earendil-works/pi-ai";
import type {
  RootSummaryV1,
  SourceInterval,
} from "./contracts/summary-types.js";
import { assertExactOnce } from "./coverage-tree.js";
import {
  assertChatSections,
  renderRootMarkdown,
} from "./deterministic-validation.js";
import { DEFAULT_BUDGET, newRecoveryState, type RecoveryState } from "./recovery-policy.js";
import type { RecoveryDeps, RecoveryOptions } from "./recovery-types.js";
import { RichFailure, type ModelProfile } from "./rich-types.js";
import type { SemanticNodePlan, SemanticTreePlan } from "./semantic-plan.js";
import { resolveNode } from "./semantic-recovery.js";
import type { SummaryGenerators } from "./semantic-node.js";
import { verifyRoot } from "./root-verify.js";
import { UsageLedger } from "./usage-ledger.js";

export type { SummaryGenerators, RecoveryOptions };

export interface SemanticTreeResult {
  readonly root: RootSummaryV1;
  readonly rootMarkdown: string;
  readonly coverageDigest: string;
  readonly usage: Usage;
  readonly calls: number;
}

/**
 * Execute the plan bottom-up with recovery: each node resolves via resolveNode
 * (repair/fallback), the root is re-verified when recovery is wired,
 * coverage is asserted exactly-once, the rendered root passes the chat
 * section gate, and the returned usage is the exact sum of every call
 * (invariant 11).
 */
export async function executeSemanticTree(
  plan: SemanticTreePlan,
  generators: SummaryGenerators,
  profile: ModelProfile,
  signal?: AbortSignal,
  recovery?: RecoveryOptions,
): Promise<SemanticTreeResult> {
  const ledger = new UsageLedger();
  const deps: RecoveryDeps = {
    generators,
    profiles: {
      summary: profile,
      repair: recovery?.repairProfile ?? profile,
      fallback: recovery?.fallbackProfile ?? profile,
    },
    budget: recovery
      ? (recovery.budget ?? DEFAULT_BUDGET)
      : { globalExtra: 0, perNodeRepair: 0 },
    ledger,
    extraCalls: { used: 0 },
    signal,
  };
  const summaries = new Map<number, string>();
  const leafIntervals: SourceInterval[] = [];
  let root: RootSummaryV1 | undefined;
  let rootNode: SemanticNodePlan | undefined;
  let rootState: RecoveryState | undefined;
  for (let idx = 0; idx < plan.nodes.length; idx++) {
    const node = plan.nodes[idx];
    if (signal?.aborted)
      throw new RichFailure("model-output", "execute", "aborted");
    const state = newRecoveryState();
    const output = await resolveNode(plan, node, summaries, deps, state);
    if (output.kind === "root") {
      root = output;
      rootNode = node;
      rootState = state;
    } else summaries.set(idx, output.summary);
    if (output.kind === "leaf") leafIntervals.push(node.interval);
  }
  if (!root || !rootNode)
    throw new RichFailure("model-output", "execute", "no-root");
  if (recovery?.verifier)
    root = await verifyRoot(plan, rootNode, summaries, root, recovery, deps, rootState);
  assertExactOnce(
    leafIntervals.length > 0
      ? leafIntervals
      : [{ startOrdinal: 0, endOrdinal: plan.units.length - 1 }],
    plan.units.length,
  );
  const rootMarkdown = renderRootMarkdown(root);
  assertChatSections(rootMarkdown);
  return {
    root,
    rootMarkdown,
    coverageDigest: plan.coverageDigest,
    usage: ledger.total(),
    calls: ledger.calls(),
  };
}
