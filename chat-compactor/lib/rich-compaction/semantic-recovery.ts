import type { SummaryRequestV1 } from "./contracts/summary-contract.js";
import type { SummaryNodeV1 } from "./contracts/summary-types.js";
import { intervalDigest } from "./coverage-tree.js";
import {
  ContractGenerationError,
  RichFailure,
  type ModelProfile,
} from "./rich-types.js";
import {
  classifyError,
  consumeExtraCall,
  isAbort,
  newRecoveryState,
  route,
  type RecoveryState,
} from "./recovery-policy.js";
import type { RecoveryDeps } from "./recovery-types.js";
import { describeCauses } from "./service-support.js";
import { byteLen, frameToken } from "./source-renderer.js";
import {
  coverageUnits,
  type SemanticNodePlan,
  type SemanticTreePlan,
} from "./semantic-plan.js";
import { requestFor, verify } from "./semantic-node.js";

const requestBytes = (request: SummaryRequestV1): number =>
  request.frames.reduce((sum, f) => sum + byteLen(frameToken(f)), 0);

const aborted = (stage: string): RichFailure =>
  new RichFailure("model-output", stage, "aborted");
async function generateVerified(
  plan: SemanticTreePlan,
  node: SemanticNodePlan,
  request: SummaryRequestV1,
  profile: ModelProfile,
  deps: RecoveryDeps,
): Promise<SummaryNodeV1> {
  let result;
  try {
    result = await deps.generators[node.role].generate(
      request,
      profile,
      deps.signal,
    );
  } catch (error) {
    // A rejected model response is still a billed call — count its usage (inv. 11).
    if (error instanceof ContractGenerationError && error.usage)
      deps.ledger.add(error.usage);
    throw error;
  }
  deps.ledger.add(result.usage);
  verify(plan, node, result.output);
  return result.output;
}

/** Resolve one node under the recovery policy: model-output → repair → fallback
 *  → cancel; host-race → one reread; abort cancels. */
export async function resolveNode(
  plan: SemanticTreePlan,
  node: SemanticNodePlan,
  summaries: ReadonlyMap<number, string>,
  deps: RecoveryDeps,
  state: RecoveryState = newRecoveryState(),
): Promise<SummaryNodeV1> {
  const base = requestFor(plan, node, summaries);
  let request = base;
  let profile = deps.profiles.summary;
  for (;;) {
    if (deps.signal?.aborted) throw aborted(node.role);
    try {
      return await generateVerified(plan, node, request, profile, deps);
    } catch (error) {
      if (isAbort(error) || deps.signal?.aborted) throw aborted(node.role);
      const failure = classifyError(error, node.role);
      state.globalUsed = deps.extraCalls.used;
      const action = route(failure, state, deps.budget);
      if (action.action === "cancel") throw failure;
      consumeExtraCall(deps.extraCalls, deps.budget, node.role);
      // The repair request carries the concrete gate that rejected the answer —
      // "model-output/root: validation" alone would ask the model to guess.
      request = {
        ...base,
        issues: [describeCauses(error) ?? failure.message],
      };
      if (action.action === "repair") {
        state.nodeRepairUsed += 1;
        profile = deps.profiles.repair;
      } else if (action.action === "fallback") {
        state.fallbackUsed = true;
        profile = deps.profiles.fallback;
      }
    }
  }
}
