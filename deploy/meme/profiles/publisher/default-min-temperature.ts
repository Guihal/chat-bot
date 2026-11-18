/**
 * Global default sampling parameters, applied to every provider request.
 *
 * Ported from ~/.pi/agent/extensions/default-min-temperature.ts (omo is a Pi
 * fork). Behaviour preserved, with two omo-specific changes:
 *
 *   1. The MiniMax M3.1 Flash family profile now covers BOTH
 *      `MiniMax-M3.1-Flash` and `MiniMax-M3.1-Flash-Preview` — same values
 *      for both, so the two catalog entries cannot drift apart.
 *   2. Temperature and the repetition penalty follow the operator's local
 *      tuning request: temperature 0.8 (down from 1.0) plus a small
 *      repetition penalty 1.05.
 *
 * Model-specific sampling profiles, selected per model (first match wins,
 * generic fallback last):
 *   - muse-spark — any model id containing `muse-spark` (all providers):
 *     nothing injected. Meta tunes Muse Spark for temperature 1.0 /
 *     top_p 1.0 and recommends leaving both unset; the empty profile lets
 *     the API defaults apply.
 *   - minimax-m3.1-flash — MiniMax M3.1 Flash family, exact model id
 *     `minimax-m3.1-flash` or `minimax-m3.1-flash-preview`, including
 *     catalog-prefixed forms: temperature 0.8, top_p 0.95,
 *     repetition_penalty 1.05.
 *   - minimax — remaining MiniMax models (any provider whose id starts with
 *     `minimax`, or any model.id containing `minimax`): temperature 0.8,
 *     top_p 0.7, top_k 12, presence_penalty 0.2, frequency_penalty 0.1.
 *   - glm-5.3-flash — GLM-5.3-Flash only (exact model id `glm-5.3-flash`,
 *     including catalog-prefixed forms like `z-ai/glm-5.3-flash`):
 *     temperature 0.7, top_p 0.95.
 *   - qwen3.8-flash — exact model id, including catalog-prefixed forms
 *     like `qwen/qwen3.8-flash`: temperature 1.0, top_p 0.98, top_k 80,
 *     presence_penalty 0.0, repetition_penalty 1.08.
 *   - deepseek-v4.1-flash — exact model id, including catalog-prefixed forms:
 *     temperature 0.8, top_p 1.0, repetition_penalty 1.05.
 *   - mistral-large-4-1t-a52b — exact model id, including catalog-prefixed
 *     forms (e.g. mistralai/mistral-large-4-1t-a52b): temperature 1.0,
 *     top_p 0.95 (Mistral recommended), plus mild repetition_penalty 1.05
 *     and frequency/presence_penalty 0.2 as an experimental anti-loop tweak.
 *   - iceman-contributor-free — exact model id, including catalog-prefixed
 *     forms: only top_p 0.95 (operator request 2026-11-16). No temperature,
 *     no top_k, no penalties: the gateway serves it text-only and the operator
 *     wants nothing but the nucleus default.
 *   - hy4-flash-free — exact model id, including catalog-prefixed forms:
 *     only top_p 0.7 (operator request 2026-11-16: stray Chinese tokens);
 *     keeps the free vision model out of the generic 0.9/0.85 profile.
 *   - generic — everything else: temperature 0.9, top_p 0.85, top_k 12,
 *     presence_penalty 0.2, frequency_penalty 0.1.
 *
 * Defaults are projected into each supported API's native request shape.
 * Unsupported fields, model/thinking restrictions and intentionally omitted
 * native OpenAI OAuth fields take precedence. Unknown APIs are unchanged.
 *
 * temperature always wins — it is overridden unconditionally (no
 * `=== undefined` check), because pi and other extensions set their own
 * temperature in buildParams before this hook runs, and our default must
 * prevail for stable sampling. Every other default (top_p, top_k,
 * presence_penalty, frequency_penalty, repetition_penalty) is filled only
 * when the payload doesn't already have that key (`=== undefined` check).
 * Keys a profile does not list are never injected. Unsupported explicit keys
 * are not removed; their owner remains the caller. The MiniMax configured
 * temperature floor of 0.8 is applied on top.
 */

import { appendFileSync } from "node:fs";
import { join } from "node:path";

import type {
  ExtensionAPI,
  ExtensionContext,
} from "@code-yeongyu/senpi";

import { projectSampling } from "./sampling-payload.ts";

type SamplingParams = {
  temperature?: number;
  top_p?: number;
  top_k?: number;
  presence_penalty?: number;
  frequency_penalty?: number;
  repetition_penalty?: number;
};

type ModelRef = { provider?: string; id?: string };

const GENERIC_SAMPLING_DEFAULTS: Readonly<SamplingParams> = {
  temperature: 0.9,
  top_p: 0.85,
  top_k: 12,
  presence_penalty: 0.2,
  frequency_penalty: 0.1,
};

/**
 * Muse Spark family: vendor-tuned sampling. Meta documents temperature 1.0 /
 * top_p 1.0 as the tuned operating point and recommends leaving both unset
 * (dev.meta.ai/docs/protocols/chat-completions; dev.meta.ai/docs/muse-spark-open/settings).
 * Empty defaults inject nothing, so the API defaults apply: projectSampling
 * finds no keys to write, returns undefined, and the payload passes through
 * untouched. Do not add temperature/top_p/penalties here without a Meta-side
 * recommendation change.
 */
const MUSE_SPARK_SAMPLING_DEFAULTS: Readonly<SamplingParams> = {};

const MUSE_SPARK_ID_PART = "muse-spark";

/** Muse Spark family: any model id containing `muse-spark`, any provider. */
function isMuseSpark(model: ModelRef | undefined): boolean {
  if (!model) return false;
  const id = (model.id ?? "").toLowerCase();
  return id.includes(MUSE_SPARK_ID_PART);
}

/** MiniMax M3.1 Flash family: reduced temperature plus a mild repetition penalty. */
const MINIMAX_M3_1_FLASH_SAMPLING_DEFAULTS: Readonly<SamplingParams> = {
  temperature: 0.8,
  top_p: 0.95,
  repetition_penalty: 1.05,
};

const MINIMAX_SAMPLING_DEFAULTS: Readonly<SamplingParams> = {
  temperature: 0.8,
  top_p: 0.7,
  top_k: 12,
  presence_penalty: 0.2,
  frequency_penalty: 0.1,
};

/** Coding/agentic tuning: nucleus sampling only — no top_k, no penalties. */
const GLM_5_3_FLASH_SAMPLING_DEFAULTS: Readonly<SamplingParams> = {
  temperature: 0.7,
  top_p: 0.95,
};

/** Qwen3.8-Flash tuning with mild repetition control. */
const QWEN_3_8_FLASH_SAMPLING_DEFAULTS: Readonly<SamplingParams> = {
  temperature: 1.0,
  top_p: 0.98,
  top_k: 80,
  presence_penalty: 0.0,
  repetition_penalty: 1.08,
};

/**
 * DeepSeek V4.1 Flash, operator-tuned (2026-11-07): temperature 1.0, top_p 0.7
 * for a tighter, more stable distribution with fewer outliers. Repetition
 * penalty kept as a mild anti-loop hint (gateway accepts it; effect may vary).
 */
const DEEPSEEK_V4_1_FLASH_SAMPLING_DEFAULTS: Readonly<SamplingParams> = {
  temperature: 1.0,
  top_p: 0.7,
  repetition_penalty: 1.05,
};

/**
 * Mistral Large 4 (1T A52B), vendor-recommended sampling for all reasoning
 * efforts: temperature 1.0, top_p 0.95.
 * Source: https://huggingface.co/mistralai/Mistral-Large-4-1T-A52B
 * Plus mild anti-loop penalties (experimental, provider-dependent).
 */
const MISTRAL_LARGE_4_1T_A52B_SAMPLING_DEFAULTS: Readonly<SamplingParams> = {
  temperature: 1.0,
  top_p: 0.95,
  repetition_penalty: 1.05,
  frequency_penalty: 0.2,
  presence_penalty: 0.2,
};

/** Configured MiniMax temperature floor. */
const MINIMAX_TEMPERATURE_FLOOR = 0.8;

/** Roles whose function is to check / falsify / issue a verdict. */
const CHECKING_ROLES: ReadonlySet<string> = new Set([
  "architect",
  "auditor",
  "constitution-reviewer",
  "critic",
  "crystal-reviewer",
  "implementation-critic",
  "plan-verifier",
  "task-researcher",
  "teamlead",
  "product-validator",
]);

/** Wide-search sampling: calibrated logits over the full distribution. */
const CHECKER_SAMPLING = { temperature: 1.0, top_p: 1.0 } as const;

/**
 * Extract the semantic role from a child session name of the form
 * `subagent-<role>-<runId>-<N>` (runId shape varies — do not assume it).
 * Prefix-matches each known role; unknown / unparseable names yield
 * undefined → root behavior.
 */
export function getCurrentRole(
  sessionName: string | undefined,
): string | undefined {
  if (!sessionName) return undefined;
  for (const role of CHECKING_ROLES) {
    if (sessionName.startsWith(`subagent-${role}-`)) return role;
  }
  return undefined;
}

const MINIMAX_M3_1_FLASH_ID = "minimax-m3.1-flash";
const MINIMAX_M3_1_FLASH_PREVIEW_ID = "minimax-m3.1-flash-preview";

type SamplingProfile = {
  name: string;
  matches(model: ModelRef | undefined): boolean;
  defaults: Readonly<SamplingParams>;
  finalize?(next: Record<string, unknown>): Record<string, unknown>;
};

/** MiniMax M3.1 Flash family: both the release and the preview catalog entry. */
function isMinimaxM31Flash(model: ModelRef | undefined): boolean {
  if (!model) return false;
  const id = (model.id ?? "").toLowerCase();
  return (
    id === MINIMAX_M3_1_FLASH_ID ||
    id.endsWith(`/${MINIMAX_M3_1_FLASH_ID}`) ||
    id === MINIMAX_M3_1_FLASH_PREVIEW_ID ||
    id.endsWith(`/${MINIMAX_M3_1_FLASH_PREVIEW_ID}`)
  );
}

/** All MiniMax flavors: built-in `minimax`, `minimax-litellm*`, clean providers, any model id containing `minimax`. */
function isMinimax(model: ModelRef | undefined): boolean {
  if (!model) return false;
  const provider = (model.provider ?? "").toLowerCase();
  const id = (model.id ?? "").toLowerCase();
  return provider.startsWith("minimax") || id.includes("minimax");
}

const GLM_5_3_FLASH_ID = "glm-5.3-flash";

/** GLM-5.3-Flash only: exact model id or a catalog-prefixed form. */
function isGlm53Flash(model: ModelRef | undefined): boolean {
  if (!model) return false;
  const id = (model.id ?? "").toLowerCase();
  return id === GLM_5_3_FLASH_ID || id.endsWith(`/${GLM_5_3_FLASH_ID}`);
}

/**
 * GLM-5.3-Flash-only role override, applied to the canonical profile before
 * API projection. Checking roles get temperature 1.0 AND top_p 1.0 — top_p is
 * written unconditionally here (deliberate departure from fill-on-undefined).
 */
function applyRoleSampling(
  next: Record<string, unknown>,
  model: ModelRef | undefined,
  role: string | undefined,
): boolean {
  if (isGlm53Flash(model) && role && CHECKING_ROLES.has(role)) {
    next.temperature = CHECKER_SAMPLING.temperature;
    next.top_p = CHECKER_SAMPLING.top_p;
    return true;
  }
  return false;
}

const QWEN_3_8_FLASH_ID = "qwen3.8-flash";

/** Qwen3.8-Flash only, including catalog-prefixed model ids. */
function isQwen38Flash(model: ModelRef | undefined): boolean {
  if (!model) return false;
  const id = (model.id ?? "").toLowerCase();
  return id === QWEN_3_8_FLASH_ID || id.endsWith(`/${QWEN_3_8_FLASH_ID}`);
}

const DEEPSEEK_V4_1_FLASH_ID = "deepseek-v4.1-flash";

/** DeepSeek V4.1 Flash only, including catalog-prefixed model ids. */
function isDeepseekV41Flash(model: ModelRef | undefined): boolean {
  if (!model) return false;
  const id = (model.id ?? "").toLowerCase();
  return id === DEEPSEEK_V4_1_FLASH_ID || id.endsWith(`/${DEEPSEEK_V4_1_FLASH_ID}`);
}

const MISTRAL_LARGE_4_1T_A52B_ID = "mistral-large-4-1t-a52b";

/** Mistral Large 4 (1T A52B) only, including catalog-prefixed model ids. */
function isMistralLarge41tA52b(model: ModelRef | undefined): boolean {
  if (!model) return false;
  const id = (model.id ?? "").toLowerCase();
  return (
    id === MISTRAL_LARGE_4_1T_A52B_ID ||
    id.endsWith(`/${MISTRAL_LARGE_4_1T_A52B_ID}`)
  );
}

const ICEMAN_CONTRIBUTOR_FREE_ID = "iceman-contributor-free";

/**
 * Iceman Contributor Free only, including catalog-prefixed model ids.
 * Operator request (2026-11-16): `top_p` 0.95 and nothing else — no
 * temperature, no top_k, no penalties.
 */
function isIcemanContributorFree(model: ModelRef | undefined): boolean {
  if (!model) return false;
  const id = (model.id ?? "").toLowerCase();
  return (
    id === ICEMAN_CONTRIBUTOR_FREE_ID ||
    id.endsWith(`/${ICEMAN_CONTRIBUTOR_FREE_ID}`)
  );
}

/** Iceman Contributor Free: nucleus sampling only — `top_p` 0.95, nothing else. */
const ICEMAN_CONTRIBUTOR_FREE_SAMPLING_DEFAULTS: Readonly<SamplingParams> = {
  top_p: 0.95,
};

const HY4_FLASH_FREE_ID = "hy4-flash-free";

/**
 * Hy4 flash Free only, including catalog-prefixed model ids. Zhenya runs on it
 * (vision-capable, free). The operator set `top_p` 0.7 after seeing stray
 * Chinese tokens; the narrow nucleus keeps it out of the generic 0.9/0.85 profile.
 */
function isHy4FlashFree(model: ModelRef | undefined): boolean {
  if (!model) return false;
  const id = (model.id ?? "").toLowerCase();
  return id === HY4_FLASH_FREE_ID || id.endsWith(`/${HY4_FLASH_FREE_ID}`);
}

/** Hy4 flash Free: nucleus sampling only — `top_p` 0.7, nothing else. */
const HY4_FLASH_FREE_SAMPLING_DEFAULTS: Readonly<SamplingParams> = {
  top_p: 0.7,
};

const SAMPLING_PROFILES: readonly SamplingProfile[] = [
  {
    name: "muse-spark",
    matches: isMuseSpark,
    defaults: MUSE_SPARK_SAMPLING_DEFAULTS,
  },
  {
    name: "minimax-m3.1-flash",
    matches: isMinimaxM31Flash,
    defaults: MINIMAX_M3_1_FLASH_SAMPLING_DEFAULTS,
    finalize: clampMinimaxTemperature,
  },
  {
    name: "minimax",
    matches: isMinimax,
    defaults: MINIMAX_SAMPLING_DEFAULTS,
    finalize: clampMinimaxTemperature,
  },
  {
    name: "glm-5.3-flash",
    matches: isGlm53Flash,
    defaults: GLM_5_3_FLASH_SAMPLING_DEFAULTS,
  },
  {
    name: "qwen3.8-flash",
    matches: isQwen38Flash,
    defaults: QWEN_3_8_FLASH_SAMPLING_DEFAULTS,
  },
  {
    name: "deepseek-v4.1-flash",
    matches: isDeepseekV41Flash,
    defaults: DEEPSEEK_V4_1_FLASH_SAMPLING_DEFAULTS,
  },
  {
    name: "mistral-large-4-1t-a52b",
    matches: isMistralLarge41tA52b,
    defaults: MISTRAL_LARGE_4_1T_A52B_SAMPLING_DEFAULTS,
  },
  {
    name: "iceman-contributor-free",
    matches: isIcemanContributorFree,
    defaults: ICEMAN_CONTRIBUTOR_FREE_SAMPLING_DEFAULTS,
  },
  {
    name: "hy4-flash-free",
    matches: isHy4FlashFree,
    defaults: HY4_FLASH_FREE_SAMPLING_DEFAULTS,
  },
];

/** Fallback for every model no profile matched. */
const GENERIC_PROFILE: SamplingProfile = {
  name: "generic",
  matches: () => true,
  defaults: GENERIC_SAMPLING_DEFAULTS,
};

/** Floor-clamp on the shallow copy only — the event payload is never mutated. */
function clampMinimaxTemperature(
  next: Record<string, unknown>,
): Record<string, unknown> {
  const temperature = next.temperature;
  if (
    typeof temperature === "number" &&
    temperature < MINIMAX_TEMPERATURE_FLOOR
  ) {
    next.temperature = MINIMAX_TEMPERATURE_FLOOR;
  }
  return next;
}

/**
 * Where the debug log lands. The bot profile has no `omo.jsonc` and no
 * `jsonc-parser`, so the omo `runtime-identity` helper is not available here:
 * the agent dir comes from the environment the host already sets.
 */
function logDir(): string {
  return (
    process.env.OMO_CODING_AGENT_DIR ||
    process.env.SENPI_CODING_AGENT_DIR ||
    process.env.OMO_CHATBOT_STATE_DIR ||
    `${process.env.HOME || "."}/.omo/agent`
  );
}

function logApplied(data: Record<string, unknown>): void {
  try {
    appendFileSync(
      join(logDir(), "logs/default-min-temperature.log"),
      `${JSON.stringify({ ts: new Date().toISOString(), ...data })}\n`,
      { mode: 0o600 },
    );
  } catch {
    /* logging must never break the extension */
  }
}

export default function defaultMinTemperature(pi: ExtensionAPI): void {
  pi.on(
    "before_provider_request",
    (event: { payload: unknown }, ctx: ExtensionContext) => {
      const payload = event.payload;
      if (typeof payload !== "object" || payload === null) return undefined;

      try {
        const model = ctx.model as ModelRef | undefined;
        const profile =
          SAMPLING_PROFILES.find((p) => p.matches(model)) ?? GENERIC_PROFILE;
        const defaults = { ...profile.defaults };
        const role = getCurrentRole(
          ctx.sessionManager?.getSessionName?.() as string | undefined,
        );
        const overridden = applyRoleSampling(defaults, model, role);
        profile.finalize?.(defaults);
        const usingOAuth =
          model &&
          typeof ctx.modelRegistry?.isUsingOAuth === "function"
            ? ctx.modelRegistry.isUsingOAuth(model as never)
            : undefined;
        const projection = projectSampling(
          payload,
          model,
          defaults,
          overridden,
          usingOAuth,
        );
        if (!projection) return undefined;
        logApplied({
          profile: profile.name,
          model: model?.id,
          provider: model?.provider,
          role: role ?? "root",
          roleOverride: overridden,
          applied: projection.applied,
        });
        return projection.payload;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        if (message.includes("stale after session replacement")) return undefined;
        throw error;
      }
    },
  );
}
