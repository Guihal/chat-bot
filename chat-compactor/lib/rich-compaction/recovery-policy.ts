import { ContractGenerationError, RichFailure } from "./rich-types.js";
import type { SemanticTreePlan } from "./semantic-plan.js";

/** Recovery budget (rev 20): extra model calls across the whole tree, and repair
 *  attempts per node. Constants with a ceiling — raising them trades cost for a
 *  higher chance of salvaging a flaky model, never correctness. */
export interface RecoveryBudget {
  readonly globalExtra: number;
  readonly perNodeRepair: number;
}
export const DEFAULT_BUDGET: RecoveryBudget = {
  globalExtra: 3,
  perNodeRepair: 1,
};

/** Per-node recovery state. globalUsed is refreshed from the run's authoritative
 * extra-call counter before routing; repair/fallback remain owned by this node,
 * including root repair after verification. */
export interface RecoveryState {
  globalUsed: number;
  nodeRepairUsed: number;
  fallbackUsed: boolean;
}

export type RecoveryAction =
  | { readonly action: "repair" }
  | { readonly action: "fallback" }
  | { readonly action: "reread" }
  | { readonly action: "cancel"; readonly reason: string };

export const newRecoveryState = (): RecoveryState => ({
  globalUsed: 0,
  nodeRepairUsed: 0,
  fallbackUsed: false,
});

/** One counter is shared by every node and verification phase of a run. Initial
 * node calls and the first root verifier are baseline; every retry is extra. */
export interface ExtraCallCounter { used: number }
export function consumeExtraCall(counter: ExtraCallCounter, budget: RecoveryBudget, stage: string): void {
  if (counter.used >= budget.globalExtra)
    throw new RichFailure("model-output", stage, "global-budget-exhausted");
  counter.used += 1;
}

export const planBaseline = (plan: SemanticTreePlan): number =>
  plan.nodes.length;

/** True for an abort — recovery never retries an abort (invariant 10). */
export const isAbort = (error: unknown): boolean =>
  error instanceof ContractGenerationError && error.kind === "aborted";

/** Map any thrown error to the rev-20 failure taxonomy. Host-side RichFailures
 *  pass through; generator errors classify by kind. */
export function classifyError(error: unknown, stage: string): RichFailure {
  if (error instanceof RichFailure) return error;
  if (error instanceof ContractGenerationError) {
    if (error.kind === "capability")
      return new RichFailure("capability-schema", stage, "capability", {
        cause: error,
      });
    if (error.kind === "transport" || error.kind === "timeout")
      return new RichFailure("host-race", stage, error.kind, { cause: error });
    return new RichFailure("model-output", stage, error.kind, { cause: error });
  }
  return new RichFailure("model-output", stage, "unknown", { cause: error });
}

/**
 * Exhaustive routing over the taxonomy. `capability-schema`/`trust-durable-state`
 * cancel at once (no model call). `host-race` gets one reread within the global
 * budget. `model-output` gets per-node repair, then one fallback, then cancel —
 * each bounded by the global budget. (`context-size` no longer arises: the
 * source is pi-trimmed, never budgeted here.)
 */
export function route(
  failure: RichFailure,
  state: RecoveryState,
  budget: RecoveryBudget,
): RecoveryAction {
  if (
    failure.kind === "capability-schema" ||
    failure.kind === "trust-durable-state"
  )
    return {
      action: "cancel",
      reason: `${failure.kind}:${failure.reasonCode}`,
    };
  if (state.globalUsed >= budget.globalExtra)
    return { action: "cancel", reason: "global-budget-exhausted" };
  if (failure.kind === "host-race") return { action: "reread" };
  if (state.nodeRepairUsed < budget.perNodeRepair) return { action: "repair" };
  if (!state.fallbackUsed) return { action: "fallback" };
  return { action: "cancel", reason: "model-output-exhausted" };
}
