import type { ModelThinkingLevel, Usage } from "@earendil-works/pi-ai";
import type { InvocationDiagnostics } from "../shared/isolated-pi-types.js";
import type { DirectFailureKind } from "../shared/isolated-pi-types.js";

export interface ModelProfile {
  readonly provider: string;
  readonly model: string;
  readonly thinkingLevel: ModelThinkingLevel;
  readonly timeoutMs: number;
}

// ---------- config/profile domain (validated by config.ts) ----------

export type RichRole = "summarizer" | "verifier" | "repair" | "fallback";

/** Per-role tunables. The provider+model are inherited from ctx.model at
 * runtime (invariant 13); config never names a model, only its thinking/timeout. */
export interface RoleSettings {
  readonly thinking: ModelThinkingLevel;
  readonly timeoutMs: number;
}

/** Selects a finite task-owned registry transform by id + expected source
 * digest/schema/order. Never a module path/export or executable config. */
export interface TransformSelector {
  readonly provider: string;
  readonly model: string;
  readonly transformId: string;
  readonly expectedDigest: string;
  readonly schema: string;
  readonly order: number;
}

export interface RichConfig {
  readonly enabled: boolean;
  readonly roles: Readonly<Record<RichRole, RoleSettings>>;
  readonly providerCompatibilityTransforms: readonly TransformSelector[];
}

export interface GenerationResult<TOutput> {
  readonly output: TOutput;
  readonly usage: Usage;
  readonly diagnostics: InvocationDiagnostics & { readonly contractId: string };
}

export class ContractGenerationError extends Error {
  constructor(
    public readonly kind: DirectFailureKind | "parse" | "validation",
    message: string,
    options?: ErrorOptions,
    /** Usage of the rejected model response, when one was produced (parse or
     *  validation failure). A rejected attempt is still a billed call — recovery
     *  adds it to the ledger so returned usage sums every call (invariant 11). */
    public readonly usage?: Usage,
  ) {
    super(message, options);
    this.name = "ContractGenerationError";
  }
}

/**
 * The rev-20 failure taxonomy for the semantic hierarchy. `stage` names the
 * pipeline step (e.g. "leaf", "merge", "root", "verify", "assembly"); `reasonCode`
 * is a short machine tag. Recovery routing (Ф2) is exhaustive over `kind`:
 * `model-output` → repair/fallback, `host-race`
 * → single reread, `capability-schema`/`trust-durable-state` → immediate cancel.
 */
export type RichFailureKind =
  | "model-output"
  | "context-size"
  | "host-race"
  | "capability-schema"
  | "trust-durable-state";

export class RichFailure extends Error {
  constructor(
    public readonly kind: RichFailureKind,
    public readonly stage: string,
    public readonly reasonCode: string,
    options?: ErrorOptions,
  ) {
    super(`${kind}/${stage}: ${reasonCode}`, options);
    this.name = "RichFailure";
  }
}
