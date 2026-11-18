import type { SummaryRequestV1 } from "./contracts/summary-contract.js";
import type { RootSummaryV1 } from "./contracts/summary-types.js";
import { consumeExtraCall, newRecoveryState, route, type RecoveryState } from "./recovery-policy.js";
import type { RecoveryDeps, RecoveryOptions } from "./recovery-types.js";
import { RichFailure } from "./rich-types.js";
import type { SemanticNodePlan, SemanticTreePlan } from "./semantic-plan.js";
import { requestFor, verify } from "./semantic-node.js";

/**
 * Re-verify the root and repair it while a rejecting verdict has budget. Each
 * repair re-runs the root generator with the verifier's issues fed back, then the
 * verifier judges again. Exhausted budget or an abort cancels.
 */
export async function verifyRoot(
  plan: SemanticTreePlan,
  rootNode: SemanticNodePlan,
  summaries: ReadonlyMap<number, string>,
  root: RootSummaryV1,
  recovery: RecoveryOptions,
  deps: RecoveryDeps,
  state: RecoveryState = newRecoveryState(),
): Promise<RootSummaryV1> {
  const { verifier, verifierProfile, verification } = recovery;
  if (!verifier || !verifierProfile || !verification) return root;
  let current = root;
  let firstVerification = true;
  for (;;) {
    if (deps.signal?.aborted)
      throw new RichFailure("model-output", "verify", "aborted");
    if (!firstVerification) consumeExtraCall(deps.extraCalls, deps.budget, "verify");
    firstVerification = false;
    const verdict = await verifier.generate(
      { candidate: current, ...verification },
      verifierProfile,
      deps.signal,
    );
    deps.ledger.add(verdict.usage);
    if (verdict.output.ok) return current;
    state.globalUsed = deps.extraCalls.used;
    const action = route(
      new RichFailure("model-output", "verify", "verifier-rejected"),
      state,
      deps.budget,
    );
    if (action.action !== "repair" && action.action !== "fallback")
      throw new RichFailure("model-output", "verify", "verifier-exhausted");
    // A root repair must still be independently verified. Reserve enough room
    // for both calls before starting a repair that cannot be accepted otherwise.
    if (deps.budget.globalExtra - deps.extraCalls.used < 2)
      throw new RichFailure("model-output", "verify", "global-budget-exhausted");
    consumeExtraCall(deps.extraCalls, deps.budget, "verify");
    const useFallback = action.action === "fallback";
    if (useFallback) state.fallbackUsed = true;
    else state.nodeRepairUsed += 1;
    const req: SummaryRequestV1 = {
      ...requestFor(plan, rootNode, summaries),
      issues: verdict.output.issues ?? ["verifier rejected"],
    };
    const result = await deps.generators.root.generate(
      req,
      useFallback ? deps.profiles.fallback : deps.profiles.repair,
      deps.signal,
    );
    deps.ledger.add(result.usage);
    verify(plan, rootNode, result.output);
    if (result.output.kind !== "root")
      throw new RichFailure("model-output", "verify", "root-kind");
    current = result.output;
  }
}
