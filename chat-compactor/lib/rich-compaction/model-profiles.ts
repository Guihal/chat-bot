import type { ModelProfile, RichConfig, RichRole } from "./rich-types.js";

/** The inherited active session model (invariant 13). Provider+model come from
 * `ctx.model`; rich.json never names a model. */
export interface SessionModel {
  readonly provider: string;
  readonly id: string;
}

/**
 * Builds a per-role `ModelProfile` from the INHERITED session model plus the
 * role's configured thinking/timeout. No model id is ever taken from config —
 * verifier independence is a distinct thinking level on the same model, never a
 * separate model (invariant 7, rev 20).
 */
export function resolveRoleProfile(
  role: RichRole,
  config: RichConfig,
  sessionModel: SessionModel,
): ModelProfile {
  const settings = config.roles[role];
  return {
    provider: sessionModel.provider,
    model: sessionModel.id,
    thinkingLevel: settings.thinking,
    timeoutMs: settings.timeoutMs,
  };
}
