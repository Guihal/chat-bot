import { DirectContractRequest } from "../shared/isolated-pi-types.js";
import type { IsolatedPiSessionRunner } from "../shared/isolated-pi-session.js";
import { attachContractId } from "./diagnostics.js";
import type { GenerationContract } from "./generation-contract.js";
import { ContractGenerationError } from "./rich-types.js";
import type { GenerationResult, ModelProfile } from "./rich-types.js";
import { copyUsage } from "./usage-ledger.js";

/** Single execution class for every constructor-injected generation contract. */
export class ContractGenerator<TInput, TOutput> {
  constructor(
    private readonly runner: IsolatedPiSessionRunner,
    private readonly contract: GenerationContract<TInput, TOutput>,
    private readonly now: () => number = Date.now,
  ) {
    if (!contract.id || !contract.systemPrompt)
      throw new Error("Generation contract identity is incomplete");
  }

  async generate(
    input: TInput,
    profile: ModelProfile,
    signal?: AbortSignal,
  ): Promise<GenerationResult<TOutput>> {
    const encoded = this.contract
      .encodeInput(input)
      .map((block) => ({ ...block }));
    encoded.push({ type: "text", text: this.contract.systemPrompt });
    const request = new DirectContractRequest({
      systemPrompt: this.contract.systemPrompt,
      userMessage: { role: "user", content: encoded, timestamp: this.now() },
      provider: profile.provider,
      model: profile.model,
      thinkingLevel: profile.thinkingLevel,
      timeoutMs: profile.timeoutMs,
      signal,
    });
    const result = await this.runner.start(request).result;
    if (!result.ok)
      throw new ContractGenerationError(
        result.error.kind,
        result.error.message,
      );
    if (result.profile !== "direct-contract")
      throw new ContractGenerationError("transport", "Wrong isolated profile");
    let output: TOutput;
    try {
      output = this.contract.parseFinal(result.text);
    } catch (error) {
      throw new ContractGenerationError(
        "parse",
        "Contract output parse failed",
        { cause: error },
        copyUsage(result.usage),
      );
    }
    try {
      this.contract.validate(output);
    } catch (error) {
      throw new ContractGenerationError(
        "validation",
        "Contract output validation failed",
        { cause: error },
        copyUsage(result.usage),
      );
    }
    return Object.freeze({
      output,
      usage: copyUsage(result.usage),
      diagnostics: attachContractId(this.contract.id, result.diagnostics),
    });
  }
}
