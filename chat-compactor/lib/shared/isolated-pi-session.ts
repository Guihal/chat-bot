import { executeDirectContract } from "./direct-contract-agent.js";
import type {
  DirectAgentDependencies,
  DirectContractRequest,
  IsolatedPiInvocation,
} from "./isolated-pi-types.js";

/** Rich compaction uses one tool-free Native Agent turn, never a Pi subprocess. */
export class IsolatedPiSessionRunner {
  constructor(
    _unused: undefined,
    private readonly dependencies: DirectAgentDependencies,
  ) {}

  start(request: DirectContractRequest): IsolatedPiInvocation {
    return executeDirectContract(request, this.dependencies);
  }
}
