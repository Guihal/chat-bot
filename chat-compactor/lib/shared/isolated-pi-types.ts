import type {
  Api,
  AssistantMessage,
  AssistantMessageEventStream,
  Context,
  ImageContent,
  Message,
  Model,
  ModelThinkingLevel,
  ModelsSimpleStreamOptions,
  TextContent,
  Usage,
  UserMessage,
} from "@earendil-works/pi-ai";
import type { AgentMessage } from "@earendil-works/pi-agent-core";
import type { ProviderTransform } from "./provider-compatibility-transforms.js";

export type DirectContent = TextContent | ImageContent;

export interface AuditorSessionRequest {
  readonly kind: "auditor-process";
  readonly command: string;
  readonly args: readonly string[];
  readonly cwd: string;
  readonly env: Readonly<NodeJS.ProcessEnv>;
  readonly timeoutMs: number;
  readonly signal?: AbortSignal;
}

/** Public built-in stream view: model lookup + simple streaming, sourced from
 *  the session's stock `ctx.modelRegistry`. No provenance seals — the only
 *  correctness gate is compact-content validation, not provider attestation. */
export interface BuiltInStream {
  findModel(provider: string, modelId: string): Model<Api> | undefined;
  streamSimple(
    model: Model<Api>,
    context: Context,
    options?: ModelsSimpleStreamOptions,
  ): AssistantMessageEventStream;
}

export interface DirectAgentDependencies {
  readonly stream: BuiltInStream;
  readonly convertToLlm: (messages: AgentMessage[]) => Message[] | Promise<Message[]>;
  /** Ordered pure API-payload transforms applied inside `onPayload` only. */
  readonly transforms?: readonly ProviderTransform[];
}

export class DirectContractRequest {
  readonly kind = "direct-contract" as const;
  readonly systemPrompt: string;
  readonly userMessage: UserMessage & { content: DirectContent[] };
  readonly provider: string;
  readonly model: string;
  readonly thinkingLevel: ModelThinkingLevel;
  readonly timeoutMs: number;
  readonly signal?: AbortSignal;

  constructor(input: Omit<DirectContractRequest, "kind" | "userMessage"> & {
    userMessage: UserMessage;
  }) {
    if (!input.systemPrompt || !input.provider || !input.model)
      throw new Error("Direct contract identity is incomplete");
    if (input.userMessage.role !== "user" || !Array.isArray(input.userMessage.content))
      throw new Error("Direct contract requires one explicit user message");
    if (!Number.isFinite(input.userMessage.timestamp))
      throw new Error("Direct contract requires a finite timestamp");
    if (!Number.isFinite(input.timeoutMs) || input.timeoutMs <= 0)
      throw new Error("Direct contract timeout must be positive");
    const content = input.userMessage.content.map(copyContent);
    this.systemPrompt = input.systemPrompt;
    this.userMessage = Object.freeze({ ...input.userMessage, content: Object.freeze(content) }) as typeof this.userMessage;
    this.provider = input.provider;
    this.model = input.model;
    this.thinkingLevel = input.thinkingLevel;
    this.timeoutMs = input.timeoutMs;
    this.signal = input.signal;
    Object.freeze(this);
  }
}

function copyContent(block: DirectContent): DirectContent {
  if (block.type === "text" && typeof block.text === "string")
    return Object.freeze({ ...block });
  if (block.type === "image" && typeof block.data === "string" && typeof block.mimeType === "string")
    return Object.freeze({ ...block });
  throw new Error("Unsupported direct contract content block");
}

export interface InvocationDiagnostics {
  readonly providerCalls: number;
  readonly systemPromptSha256: string;
  readonly userMessages: number;
  readonly tools: number;
  readonly resources: 0;
  readonly extensions: 0;
  readonly terminalTextSha256?: string;
  /** sha256 of canonical post-transform payload; set only when a transform ran. */
  readonly postTransformSha256?: string;
}

export type DirectFailureKind =
  | "aborted"
  | "timeout"
  | "capability"
  | "transport"
  | "model-output";

export type IsolatedPiSessionResult =
  | { ok: true; profile: "auditor-process"; text: string; stderr: string; diagnostics: Record<string, unknown> }
  | { ok: true; profile: "direct-contract"; text: string; assistant: AssistantMessage; usage: Usage; diagnostics: InvocationDiagnostics }
  | { ok: false; profile: "auditor-process"; error: { kind: "aborted" | "timeout" | "transport"; message: string }; diagnostics: Record<string, unknown> }
  | { ok: false; profile: "direct-contract"; error: { kind: DirectFailureKind; message: string }; diagnostics: InvocationDiagnostics };

export interface IsolatedPiInvocation {
  readonly result: Promise<IsolatedPiSessionResult>;
  abort(): void;
}

export type IsolatedPiRequest = AuditorSessionRequest | DirectContractRequest;
