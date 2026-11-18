/** API-native request projection. Unknown APIs receive no guessed fields. */
export type SamplingValues = Partial<Record<
  "temperature" | "top_p" | "top_k" | "presence_penalty" | "frequency_penalty" | "repetition_penalty", number>>;
export type SamplingModel = {
  provider?: string; id?: string; api?: string; baseUrl?: string; reasoning?: boolean;
  compat?: unknown;
};
type Payload = Record<string, unknown>;
const record = (value: unknown): value is Payload =>
  value !== null && typeof value === "object" && !Array.isArray(value);
const FLAT = { temperature: "temperature", top_p: "top_p", top_k: "top_k",
  presence_penalty: "presence_penalty", frequency_penalty: "frequency_penalty",
  repetition_penalty: "repetition_penalty" } as const;
type Key = keyof typeof FLAT;
const CAMEL = { ...FLAT, top_p: "topP", top_k: "topK", presence_penalty: "presencePenalty",
  frequency_penalty: "frequencyPenalty" };
const legacyClaude = (id = ""): boolean =>
  /(?:^|[./])claude-(?:[123](?:[.-]|$)|(?:opus|sonnet|haiku)[.-]4(?:[.-][0-6](?:[.-]|$)|-\d{8}(?:[-:]|$)|$))/i.test(id);

export interface SamplingProjection { payload: Payload; applied: SamplingValues }

export function projectSampling(payload: unknown, model: SamplingModel | undefined,
  defaults: SamplingValues, overrideTopP = false, usingOAuth?: boolean): SamplingProjection | undefined {
  if (!record(payload) || !model?.api) return undefined;
  const compat = record(model.compat) ? model.compat : {};
  if (compat.supportsTemperature === false) return undefined;
  let container: string | undefined;
  let names: Record<Key, string> = FLAT;
  let keys: Key[];
  const mini = model.provider?.toLowerCase().startsWith("minimax") || model.id?.toLowerCase().includes("minimax");
  switch (model.api) {
    case "google-generative-ai": case "google-vertex":
      if (!record(payload.config) || !Array.isArray(payload.contents)) return undefined;
      container = "config"; names = CAMEL;
      keys = ["temperature", "top_p", "top_k", "presence_penalty", "frequency_penalty"];
      break;
    case "bedrock-converse-stream":
      if (!record(payload.inferenceConfig) || !Array.isArray(payload.messages)) return undefined;
      if (record(payload.additionalModelRequestFields) && record(payload.additionalModelRequestFields.thinking) &&
        payload.additionalModelRequestFields.thinking.type !== "disabled") return undefined;
      container = "inferenceConfig"; names = CAMEL; keys = ["temperature", "top_p"];
      if (/claude/i.test(model.id ?? "")) {
        if (!legacyClaude(model.id)) return undefined;
        keys = payload.inferenceConfig.topP !== undefined ? [] : ["temperature"];
      }
      break;
    case "pi-messages":
      if (!record(payload.options)) return undefined;
      container = "options"; keys = ["temperature"];
      break;
    case "mistral-conversations":
      if (!Array.isArray(payload.messages)) return undefined;
      names = CAMEL; keys = ["temperature", "top_p", "presence_penalty", "frequency_penalty"];
      break;
    case "anthropic-messages": {
      if (!Array.isArray(payload.messages)) return undefined;
      // MiniMax accepts sampling fields next to adaptive thinking (its
      // Anthropic-compatible endpoint rejects no sampling key), so the
      // thinking guard below must not skip MiniMax models.
      if (mini) { keys = ["temperature", "top_p", "repetition_penalty"]; break; }
      if (compat.supportsMidConvoEffort ||
        (record(payload.thinking) && payload.thinking.type !== "disabled")) return undefined;
      // Only known legacy Claude models permit tuning. Newer Claude models reject
      // sampling fields, even with thinking off. Do not infer future support.
      if (!legacyClaude(model.id)) return undefined;
      // Claude also rejects temperature+top_p together. Preserve explicit top_p.
      keys = payload.top_p !== undefined ? [] : ["temperature"];
      break;
    }
    case "openai-completions":
      if (!Array.isArray(payload.messages)) return undefined;
      keys = mini ? ["temperature", "top_p", "repetition_penalty"] : ["temperature", "top_p", "presence_penalty", "frequency_penalty"];
      // Alibaba's documented extras are not OpenAI-standard request fields.
      if (model.provider?.startsWith("qwen-token-plan") || /https:\/\/[^/]*\.aliyuncs\.com(?:\/|$)/i.test(model.baseUrl ?? "")) {
        keys = ["temperature", "top_p", "presence_penalty"];
        if (/(?:^|\/)qwen/i.test(model.id ?? "")) keys.push("top_k", "repetition_penalty");
      }
      // Together's official Chat Completions schema also supports these extras.
      if (model.provider === "together" || /^https:\/\/api\.together\.ai(?::443)?(?:\/|$)/i.test(model.baseUrl ?? ""))
        keys.push("top_k", "repetition_penalty");
      // opencode-go's OpenAI-compatible gateway accepts sampling extras: measured 2026-10-28,
      // bodies carrying repetition_penalty / frequency_penalty / presence_penalty answered 200.
      // The served DeepSeek model does not act on them (see default-min-temperature.ts), and
      // plain OpenAI rejects unknown fields, so this stays scoped to that gateway's provider id.
      if (model.provider === "opencode-go") keys.push("repetition_penalty");
      break;
    case "openai-responses": case "azure-openai-responses": case "openai-codex-responses":
      if (!Array.isArray(payload.input)) return undefined;
      keys = ["temperature", "top_p"];
      // Native OpenAI ChatGPT OAuth omits sampling. The caller supplies the
      // public registry's auth-kind boolean, never a key or token. API-key
      // requests still receive defaults when the serializer omitted options.
      if (model.api === "openai-codex-responses" || (model.provider === "openai" && usingOAuth !== false))
        return undefined;
      break;
    default: return undefined;
  }
  // OpenAI reasoning families do not accept profile sampling. Other providers'
  // reasoning models (GLM/Qwen/MiniMax) retain their documented sampling modes.
  const openAIWire = model.api.startsWith("openai-") || model.api === "azure-openai-responses";
  if (openAIWire && (/(?:^|\/)(?:o[1-9](?:[.-]|$)|gpt-5)/i.test(model.id ?? "") ||
    ((model.provider === "openai" || model.provider === "openai-codex") && model.reasoning))) return undefined;
  const target = container ? payload[container] as Payload : payload;
  const next: Payload = { ...target };
  const applied: SamplingValues = {};
  for (const key of keys) {
    if (defaults[key] === undefined) continue;
    const native = names[key];
    if (key === "temperature" || (key === "top_p" && overrideTopP) || next[native] === undefined) {
      next[native] = defaults[key];
      applied[key] = defaults[key];
    }
  }
  if (Object.keys(applied).length === 0) return undefined;
  return { payload: container ? { ...payload, [container]: next } : next, applied };
}
