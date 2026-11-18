import type { ContractGenerator } from "./contract-generator.js";
import type { SummaryRequestV1 } from "./contracts/summary-contract.js";
import type {
  SummaryNodeV1,
  VerifierVerdictV1,
} from "./contracts/summary-types.js";
import type { VerificationRequestV1 } from "./contracts/verifier-contract.js";
import type { ExtraCallCounter, RecoveryBudget } from "./recovery-policy.js";
import type { ModelProfile } from "./rich-types.js";
import type { SummaryGenerators } from "./semantic-node.js";
import type { UsageLedger } from "./usage-ledger.js";

/** Everything a node needs to resolve itself with recovery. Profiles are all the
 *  inherited model at role-specific thinking levels (invariant 7). */
export interface RecoveryDeps {
  readonly generators: SummaryGenerators;
  readonly profiles: {
    readonly summary: ModelProfile;
    readonly repair: ModelProfile;
    readonly fallback: ModelProfile;
  };
  readonly budget: RecoveryBudget;
  readonly extraCalls: ExtraCallCounter;
  readonly ledger: UsageLedger;
  readonly signal?: AbortSignal;
}

/** Optional recovery wiring for a full compaction run; absent = single attempt
 *  per node and no verifier (the Ф1 lossless-hierarchy path). */
export interface RecoveryOptions {
  readonly repairProfile: ModelProfile;
  readonly fallbackProfile: ModelProfile;
  readonly budget?: RecoveryBudget;
  /** Root verification is optional; when a verifier is set, so are the other two. */
  readonly verifier?: ContractGenerator<
    VerificationRequestV1,
    VerifierVerdictV1
  >;
  readonly verifierProfile?: ModelProfile;
  readonly verification?: {
    readonly latestRequest: string;
    readonly continuation: string;
    readonly customInstructions?: string;
  };
}

export type { SummaryNodeV1, SummaryRequestV1 };
