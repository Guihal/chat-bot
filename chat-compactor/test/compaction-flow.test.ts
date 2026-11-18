// End-to-end compaction flow with a scripted generator: the shape that reaches
// Pi, the repair path, the exhausted-budget fallback to native compaction, the
// abort path, and the source manifest the model actually receives.
//
// What this file cannot prove: that the model folds repeated windows into one
// fact — that is the model's job under the prompt contract (prompt-contract.test
// pins the instruction) and is checked on a live compaction. What it does prove
// is that the host delivers every window to the request and then accepts only a
// registry-shaped answer.
import { beforeAll, expect, test } from "bun:test";
import { cpSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const agent = mkdtempSync(join(tmpdir(), "chat-compactor-flow-"));
cpSync(join(import.meta.dir, "../../bot-home/compactors"), join(agent, "compactors"), {
  recursive: true,
});
process.env.OMO_CODING_AGENT_DIR = agent;

let Service: any;
let loadRichConfig: (path: string) => any;
let createSummaryContract: any;
let ContractGenerationError: any;

beforeAll(async () => {
  ({ RichCompactionService: Service } = (await import(
    "../lib/rich-compaction/rich-compaction-service.ts"
  )) as any);
  ({ loadRichConfig } = (await import("../lib/rich-compaction/config.ts")) as any);
  ({ createSummaryContract } = (await import(
    "../lib/rich-compaction/contracts/summary-contract.ts"
  )) as any);
  ({ ContractGenerationError } = (await import(
    "../lib/rich-compaction/rich-types.ts"
  )) as any);
});

const config = () => loadRichConfig(join(agent, "compactors/rich.json"));

const usage = () => ({
  input: 10,
  output: 5,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 15,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
});

const CHAT = {
  narrative: "Чат про кота и бота. Обсуждали имя, спорили, к согласию не пришли.",
  people: [
    "@masha — заводила, шутит первой",
    "@dima — владелец, ставит задачи",
  ],
  chatRules: [
    "Технические ошибки в чат не пересказываются",
    "На задание владельца нужен отчёт, а не «делаю»",
  ],
  toneMemes: [
    "«блядский» — обращение к боту",
    "мем про Барсика",
    "присказка «и всё»",
  ],
  topics: ["имя кота", "переезд бота"],
  events: [
    "2026-11-04 — Дима перенёс бота на новый сервер",
    "2026-11-05 — Маша предложила имя Барсик",
  ],
  promises: ["бот обещал прислать логи"],
  lastConversation: [
    "Маша предложила Барсика",
    "Дима сказал, что подумает и ответит утром",
  ],
  openThreads: ["имя кота не выбрано", "бот не прислал логи"],
  operational: ["history.db лежит в /opt/omo-chat/data/chat-state"],
};

type Fake = { generate: (request: any) => Promise<any> };

function rootGenerator(
  captured: any[],
  sections: () => any = () => CHAT,
): Fake {
  const contract = createSummaryContract("root");
  return {
    generate: async (request: any) => {
      captured.push(request);
      const text = JSON.stringify({
        kind: "root",
        coverageDigest: request.coverageDigest,
        sections: sections(),
      });
      let output;
      try {
        output = contract.parseFinal(text);
        contract.validate(output);
      } catch (error) {
        throw new ContractGenerationError("validation", "Contract output validation failed", {
          cause: error,
        });
      }
      return {
        output,
        usage: usage(),
        diagnostics: { contractId: contract.id },
      };
    },
  };
}

const leafGenerator = (): Fake => ({
  generate: async () => {
    throw new Error("leaf is not used by the single-root plan");
  },
});

function generators(args: {
  captured?: any[];
  sections?: () => any;
  verdict?: () => any;
  verifier?: boolean;
}): any {
  const captured = args.captured ?? [];
  const root = rootGenerator(captured, args.sections);
  return {
    summaryGenerators: { leaf: leafGenerator(), merge: leafGenerator(), root },
    ...(args.verifier === false
      ? {}
      : {
          verifierGenerator: {
            generate: async () => ({
              output: args.verdict ? args.verdict() : { ok: true },
              usage: usage(),
              diagnostics: { contractId: "rich-verifier:root" },
            }),
          },
        }),
  };
}

const message = (role: "user" | "assistant", text: string, timestamp: number) =>
  role === "user"
    ? { role, content: text, timestamp }
    : { role, content: [{ type: "text", text }], timestamp };

function eventOf(args: {
  messages: any[];
  previousSummary?: string;
  signal?: AbortSignal;
}): any {
  return {
    type: "session_before_compact",
    reason: "manual",
    willRetry: false,
    requestId: "req-1",
    preparation: {
      firstKeptEntryId: "entry-1",
      messagesToSummarize: args.messages,
      turnPrefixMessages: [],
      isSplitTurn: false,
      tokensBefore: 1000,
      ...(args.previousSummary ? { previousSummary: args.previousSummary } : {}),
      settings: {},
    },
    signal: args.signal ?? new AbortController().signal,
  };
}

const ctx = { model: { provider: "opencode-go", id: "muse-spark-1.3" } };

function service(buildGenerators: () => any, extra: any = {}) {
  return new Service({
    buildGenerators,
    config: config(),
    now: () => 1_700_000_000_000,
    ...extra,
  });
}

const MESSAGES = [
  message("user", "[1] @masha: привет", 1),
  message("assistant", "привет", 2),
  message("user", "[2] @dima: давай назовём кота Барсиком", 3),
];

test("a real compaction returns the chat structure and chat-compactor details", async () => {
  const captured: any[] = [];
  const result = await service(() => generators({ captured })).compact(
    eventOf({ messages: MESSAGES }),
    ctx,
  );
  expect(result?.cancel).toBeUndefined();
  const details = result?.compaction?.details;
  expect(details?.schema).toBe("chat-compactor/v1");
  expect(details?.readFiles).toEqual([]);
  expect(details?.modifiedFiles).toEqual([]);
  expect(details?.skills).toEqual([]);
  expect(details?.sourceIdentity).toMatch(/^[0-9a-f]{64}$/);
  expect(details?.calls).toBe(2);

  const summary: string = result.compaction.summary;
  const headings = [...summary.matchAll(/^## (.+)$/gm)].map((m) => m[1]);
  expect(headings).toEqual([
    "Narrative",
    "People",
    "Chat Rules",
    "Tone & Memes",
    "Topics",
    "Events",
    "Promises",
    "Last Conversation",
    "Open Threads",
    "Operational",
  ]);
  const items = summary.split("\n").filter((line) => line.startsWith("- "));
  expect(items.length).toBeGreaterThanOrEqual(15);
  expect(summary).toContain("2026-11-04 — Дима перенёс бота на новый сервер");
  expect(summary).not.toContain("<<<RICH:");

  const request = captured[0];
  expect(request.role).toBe("root");
  expect(request.coverageDigest).toMatch(/^[0-9a-f]{64}$/);
  expect(request.frames.length).toBe(MESSAGES.length);
});

test("a rejected first answer is repaired with the failure fed back", async () => {
  const captured: any[] = [];
  let attempt = 0;
  const sections = () => (++attempt === 1 ? { narrative: "Чат.", people: [] } : CHAT);
  const result = await service(() =>
    generators({ captured, sections: sections }),
  ).compact(eventOf({ messages: MESSAGES }), ctx);

  expect(result?.compaction?.details?.schema).toBe("chat-compactor/v1");
  expect(captured.length).toBe(2);
  expect(captured[1].issues?.length).toBe(1);
  expect(String(captured[1].issues[0])).toContain("lastConversation");
});

test("a verifier that keeps rejecting ends in native compaction, not a bad summary", async () => {
  const result = await service(() =>
    generators({ verdict: () => ({ ok: false, issues: ["повторы скопированы"] }) }),
  ).compact(eventOf({ messages: MESSAGES }), ctx);
  expect(result).toBeUndefined();
});

test("without a verifier the single attempt still passes the structure gate", async () => {
  const result = await service(() =>
    generators({ verifier: false }),
  ).compact(eventOf({ messages: MESSAGES }), ctx);
  expect(result?.compaction?.details?.schema).toBe("chat-compactor/v1");
  expect(result?.compaction?.details?.calls).toBe(1);
});

test("an aborted signal cancels instead of compacting", async () => {
  const controller = new AbortController();
  controller.abort();
  const result = await service(() => generators({})).compact(
    eventOf({ messages: MESSAGES, signal: controller.signal }),
    ctx,
  );
  expect(result).toEqual({ cancel: true });
});

test("a prior desktop summary is stripped to its chat body before it is fed back", async () => {
  const captured: any[] = [];
  const { encodeHostSections } = (await import(
    "../lib/rich-compaction/host-codec.ts"
  )) as any;
  const { filesBlock } = encodeHostSections("- a.ts", "");
  await service(() => generators({ captured })).compact(
    eventOf({
      messages: MESSAGES,
      previousSummary: `## Narrative\n\nЧат про кота.\n\n${filesBlock}`,
    }),
    ctx,
  );
  const text = captured[0].frames
    .map((frame: any) => (frame.type === "text" ? frame.text : "[image]"))
    .join("\n");
  expect(text).toContain("Чат про кота.");
  expect(text).not.toContain("<<<RICH:HOST");
  expect(text).not.toContain("- a.ts");
});

test("overlapping windows all reach the request — the host drops no source", async () => {
  const captured: any[] = [];
  const window = (lines: string[]) => message("user", lines.join("\n"), 1);
  const messages = [
    window(["[1] @masha: привет", "[2] @dima: здорово"]),
    window(["[2] @dima: здорово", "[3] @masha: выбери имя коту"]),
    window(["[3] @masha: выбери имя коту", "[4] @dima: пусть будет Барсик"]),
  ];
  await service(() => generators({ captured })).compact(
    eventOf({ messages }),
    ctx,
  );
  expect(captured[0].frames.length).toBe(3);
  const text = captured[0].frames
    .map((frame: any) => (frame.type === "text" ? frame.text : ""))
    .join("\n");
  for (const line of ["@masha: привет", "@dima: здорово", "выбери имя коту", "пусть будет Барсик"])
    expect(text).toContain(line);
  expect(text.split("выбери имя коту").length - 1).toBe(2);
});
