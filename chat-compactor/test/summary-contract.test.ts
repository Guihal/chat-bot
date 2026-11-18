// The root envelope gate: what a chat summary must and must not look like. The
// contract has to fail loudly here, because a rejected envelope is what routes
// into repair and, on exhaustion, into native compaction.
import { beforeAll, expect, test } from "bun:test";
import { cpSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const agent = mkdtempSync(join(tmpdir(), "chat-compactor-contract-"));
cpSync(join(import.meta.dir, "../../bot-home/compactors"), join(agent, "compactors"), {
  recursive: true,
});
process.env.OMO_CODING_AGENT_DIR = agent;

type Contract = {
  parseFinal(text: string): any;
  validate(output: any): void;
};

let make: (role: "root" | "leaf" | "repair") => Contract;
let render: (root: any) => string;
let assertSectionGate: (markdown: string) => void;

beforeAll(async () => {
  const summary = (await import(
    "../lib/rich-compaction/contracts/summary-contract.ts"
  )) as any;
  const validation = (await import(
    "../lib/rich-compaction/deterministic-validation.ts"
  )) as any;
  make = summary.createSummaryContract;
  render = validation.renderRootMarkdown;
  assertSectionGate = validation.assertChatSections;
});

const DIGEST = "a".repeat(64);

function envelope(sections: unknown): string {
  return JSON.stringify({ kind: "root", coverageDigest: DIGEST, sections });
}

const CHAT_SECTIONS = {
  narrative: "Чат про бота и повседневную болтовню.\nРазговор шёл два дня.",
  people: ["@masha — заводила, шутит первой", "@dima — владелец, ставит задачи"],
  chatRules: ["Про политику здесь не говорят"],
  toneMemes: ["«блядский» — обращение к боту, мем чата"],
  topics: ["переезд бота на VPS"],
  events: ["2026-11-04 — Дима перенёс бота на новый сервер"],
  promises: ["бот обещал прислать логи вечером"],
  lastConversation: ["Маша спросила про кота, Дима ответил, что кот теперь Барсик"],
  openThreads: ["имя кота так и не выбрали окончательно"],
  operational: ["history.db лежит в /opt/omo-chat/data/chat-state"],
};

function rootOf(text: string) {
  const contract = make("root");
  const parsed = contract.parseFinal(text);
  contract.validate(parsed);
  return parsed;
}

test("a full ten-section chat envelope is accepted and renders in registry order", () => {
  const root = rootOf(envelope(CHAT_SECTIONS));
  const markdown = render(root);
  const headings = [...markdown.matchAll(/^## (.+)$/gm)].map((m) => m[1]);
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
  expect(markdown).toContain("- @masha — заводила, шутит первой");
  expect(markdown).toContain("2026-11-04 — Дима перенёс бота на новый сервер");
  expect(markdown.split("\n").filter((line) => line.startsWith("- ")).length).toBe(10);
  assertSectionGate(markdown);
});

test("multi-line values collapse to one line instead of breaking the layout", () => {
  const root = rootOf(
    envelope({
      narrative: "Первая строка.\nВторая   строка.",
      lastConversation: ["  Маша\nспросила про кота  "],
    }),
  );
  expect(root.sections.narrative).toBe("Первая строка. Вторая строка.");
  expect(root.sections.lastConversation).toEqual(["Маша спросила про кота"]);
  assertSectionGate(render(root));
});

test("empty item sections are dropped, and a section with nothing is simply absent", () => {
  const root = rootOf(
    envelope({
      narrative: "Короткий чат.",
      people: [],
      lastConversation: ["Ни о чём не договорились"],
      operational: ["  "],
    }),
  );
  const markdown = render(root);
  expect(markdown).not.toContain("## People");
  expect(markdown).not.toContain("## Operational");
  expect(markdown).toContain("## Last Conversation");
  assertSectionGate(markdown);
});

test("the required sections can never be empty or missing", () => {
  expect(() => rootOf(envelope({ narrative: "Чат.", lastConversation: [] }))).toThrow();
  expect(() => rootOf(envelope({ narrative: "", lastConversation: ["x"] }))).toThrow();
  expect(() => rootOf(envelope({ lastConversation: ["x"] }))).toThrow();
  expect(() => rootOf(envelope({ narrative: "Чат." }))).toThrow();
});

test("an unknown key, a shifted order and a wrong kind are rejected", () => {
  expect(() =>
    rootOf(envelope({ ...CHAT_SECTIONS, files: ["a.ts"] })),
  ).toThrow(/unknown section/);
  expect(() =>
    rootOf(envelope({ lastConversation: ["x"], narrative: "Чат." })),
  ).toThrow(/registry order/);
  expect(() =>
    rootOf(envelope({ narrative: ["Чат."], lastConversation: ["x"] })),
  ).toThrow(/must be a string/);
  expect(() =>
    rootOf(envelope({ narrative: "Чат.", people: "просто строка", lastConversation: ["x"] })),
  ).toThrow(/array of strings/);
});

test("item ceilings and the events date prefix are enforced", () => {
  const tooMany = Array.from({ length: 13 }, (_, i) => `факт ${i}`);
  expect(() =>
    rootOf(envelope({ narrative: "Чат.", people: tooMany, lastConversation: ["x"] })),
  ).toThrow(/ceiling/);
  expect(() =>
    rootOf(
      envelope({
        narrative: "Чат.",
        people: ["x".repeat(401)],
        lastConversation: ["x"],
      }),
    ),
  ).toThrow(/exceeds/);
  expect(() =>
    rootOf(
      envelope({
        narrative: "Чат.",
        events: ["Дима перенёс бота"],
        lastConversation: ["x"],
      }),
    ),
  ).toThrow(/YYYY-MM-DD/);
  expect(() =>
    rootOf(
      envelope({
        narrative: "Чат.",
        events: ["— — был какой-то скандал"],
        lastConversation: ["x"],
      }),
    ),
  ).not.toThrow();
  expect(() =>
    rootOf(
      envelope({
        narrative: "Чат.",
        events: ["2026-11-04 — Дима перенёс бота"],
        lastConversation: ["x"],
      }),
    ),
  ).not.toThrow();
});

test("a leaked host marker or a model-authored Files heading is rejected", () => {
  expect(() =>
    rootOf(
      envelope({
        narrative: "Чат с маркером <<<RICH:HOST files v3 >>>",
        lastConversation: ["x"],
      }),
    ),
  ).toThrow(/host marker/);
  expect(() =>
    rootOf(
      envelope({
        narrative: "Чат.",
        people: ["## Files\n- a.ts"],
        lastConversation: ["x"],
      }),
    ),
  ).toThrow(/Files\/Skills/);
});

test("leaf and merge nodes keep their prototype shape", () => {
  const leaf = make("leaf");
  const parsed = leaf.parseFinal(
    JSON.stringify({
      kind: "leaf",
      interval: { startOrdinal: 0, endOrdinal: 1 },
      sourceDigest: DIGEST,
      summary: "Маша шутила про кота.",
    }),
  );
  leaf.validate(parsed);
  expect(parsed.kind).toBe("leaf");
  const inverted = leaf.parseFinal(
    JSON.stringify({
      kind: "leaf",
      interval: { startOrdinal: 1, endOrdinal: 0 },
      sourceDigest: DIGEST,
      summary: "x",
    }),
  );
  expect(() => leaf.validate(inverted)).toThrow(/invalid source interval/);
  expect(() => leaf.validate(leaf.parseFinal(
    JSON.stringify({ kind: "root", sections: {} }),
  ))).toThrow(/Expected a leaf node/);
});

test("the assembly gate rejects content outside the registry shape", () => {
  expect(() => assertSectionGate("## Narrative\n\nЧат.\n\n## Last Conversation\n\n- x")).not.toThrow();
  expect(() => assertSectionGate("ведущий абзац\n\n## Narrative\n\nЧат.")).toThrow();
  expect(() =>
    assertSectionGate("## Narrative\n\nЧат.\n\n## Last Conversation\n\n- x\n\n## Unknown\n\n- y"),
  ).toThrow(/section-unknown/);
  expect(() =>
    assertSectionGate("## Last Conversation\n\n- x\n\n## Narrative\n\nЧат."),
  ).toThrow(/section-order/);
  expect(() => assertSectionGate("## Narrative\n\nЧат.")).toThrow(/section-missing/);
  expect(() => assertSectionGate("## Narrative\n\n## Last Conversation\n\n- x")).toThrow(
    /section-empty/,
  );
});
