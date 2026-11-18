// Contract tests for chat-control-task: the record, its limits, the rendered
// anchor and the injection into each provider wire shape. `node --test`.
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import chatControlTask, {
  MARKER,
  bumpTurn,
  commitAdd,
  commitSet,
  docPath,
  injectAnchor,
  loadStore,
  logPath,
  parseDoc,
  parseStore,
  renderAnchor,
  resolveStateDir,
} from "./chat-control-task.mjs";

function workdir() {
  const dir = mkdtempSync(join(tmpdir(), "control-task-"));
  return {
    dir,
    cleanup: () => rmSync(dir, { recursive: true, force: true }),
  };
}

function doc(overrides = {}) {
  return {
    goal: "вести беседу про переезд сервера",
    chat: {
      topic: "переезд сервера",
      participants: ["Димоня", "Женя"],
      discussed: ["диск почти полон"],
      conclusions: ["чистим кэш"],
    },
    explicit: ["не отправлять тестовые сообщения в чат"],
    implicit: ["прод не дёргать промптами (ранее сказано)"],
    constraints: [],
    sources: ["/opt/omo-chat"],
    ...overrides,
  };
}

// ── document parsing and limits ─────────────────────────────────────────────

test("parseDoc: normalizes and requires goal and chat.topic", () => {
  const ok = parseDoc(doc());
  assert.equal(ok.ok, true);
  assert.equal(ok.value.chat.topic, "переезд сервера");
  assert.deepEqual(ok.value.chat.participants, ["Димоня", "Женя"]);

  assert.equal(parseDoc({ chat: { topic: "t" } }).ok, false, "goal required");
  assert.equal(parseDoc({ goal: "g" }).ok, false, "chat required");
  assert.equal(parseDoc({ goal: "g", chat: { topic: "  " } }).ok, false, "topic required");
  assert.equal(parseDoc({ goal: "   ", chat: { topic: "t" } }).ok, false, "blank goal rejected");
});

test("parseDoc: every limit refuses, never truncates silently", () => {
  assert.equal(parseDoc(doc({ goal: "x".repeat(2001) })).ok, false, "goal");
  assert.equal(
    parseDoc(doc({ chat: { topic: "t", discussed: ["x".repeat(241)] } })).ok,
    false,
    "discussed item",
  );
  assert.equal(
    parseDoc(doc({ chat: { topic: "t", participants: Array.from({ length: 21 }, () => "p") } })).ok,
    false,
    "participants count",
  );
  assert.equal(
    parseDoc(doc({ explicit: Array.from({ length: 401 }, () => "") })).ok,
    false,
    "more than 200 section entries",
  );
  const oversized = {
    ...doc(),
    explicit: Array.from({ length: 25 }, () => "x".repeat(1960)),
  };
  assert.match(parseDoc(oversized).error ?? "", /объедини/u);
  assert.equal(parseDoc(doc({ explicit: ["x".repeat(49000)] })).ok, false, "whole doc");
});

// ── store: append, absent, poison ───────────────────────────────────────────

test("store: missing file is uninitialized; set creates it; add dedupes and replaces the topic", () => {
  const { dir, cleanup } = workdir();
  try {
    assert.equal(loadStore(dir).status, "uninitialized");

    const first = commitSet(dir, doc());
    assert.equal(first.ok, true);
    assert.equal(first.outcome.committed, true);
    assert.equal(loadStore(dir).status, "current");

    const noChange = commitAdd(dir, {
      chat: { topic: "переезд сервера", discussed: ["диск почти полон"] },
      explicit: ["не отправлять тестовые сообщения в чат"],
    });
    assert.equal(noChange.ok, true);
    assert.match(noChange.outcome.text, /уже записаны/u);

    const grown = commitAdd(dir, {
      chat: { topic: "переезд сервера и бэкап", discussed: ["диск почти полон", "бэкап старый"] },
    });
    assert.equal(grown.ok, true);
    assert.match(grown.outcome.text, /добавлено 1, пропущено дублей 1/u);
    const store = loadStore(dir);
    assert.equal(store.doc.chat.topic, "переезд сервера и бэкап", "topic is replaced");
    assert.deepEqual(store.doc.chat.discussed, ["диск почти полон", "бэкап старый"]);
  } finally {
    cleanup();
  }
});

test("store: add without a record is refused and points at control_task_set", () => {
  const { dir, cleanup } = workdir();
  try {
    const res = commitAdd(dir, { chat: { topic: "x" } });
    assert.equal(res.ok, false);
    assert.match(res.error, /control_task_set/u);
  } finally {
    cleanup();
  }
});

test("store: a poisoned file is unavailable, tool writes are refused, it is not self-healed", () => {
  const { dir, cleanup } = workdir();
  try {
    mkdirSync(join(dir, "control-task"), { recursive: true });
    writeFileSync(docPath(dir), "{not json");
    assert.equal(loadStore(dir).status, "unavailable");
    const res = commitSet(dir, doc());
    assert.equal(res.ok, false);
    assert.match(res.error, /JSON/u);
    assert.equal(readFileSync(docPath(dir), "utf8"), "{not json", "file must stay untouched");

    writeFileSync(docPath(dir), JSON.stringify({ schemaVersion: 9, revisions: [] }));
    assert.equal(loadStore(dir).status, "unavailable");
    assert.match(parseStore({ schemaVersion: 9, revisions: [] }).error, /schemaVersion/u);
  } finally {
    cleanup();
  }
});

test("store: writing clears the obligation, a later turn recreates it", () => {
  const { dir, cleanup } = workdir();
  try {
    assert.equal(commitSet(dir, doc()).ok, true);
    assert.equal(loadStore(dir).writtenAtTurn, loadStore(dir).turnCount, "write covers this turn");
    bumpTurn(dir);
    const store = loadStore(dir);
    assert.ok(store.turnCount > store.writtenAtTurn, "a new turn is unreviewed");
    assert.equal(commitSet(dir, doc()).outcome.committed, false, "same doc is not rewritten");
    assert.equal(loadStore(dir).writtenAtTurn, loadStore(dir).turnCount, "no-op write clears it");
  } finally {
    cleanup();
  }
});

// ── the anchor text ─────────────────────────────────────────────────────────

test("render: the whole chat context is present in every state", () => {
  const current = renderAnchor({ status: "current", doc: doc(), turnCount: 2, writtenAtTurn: 2 });
  for (const needle of ["переезд сервера", "Димоня", "Женя", "диск почти полон", "чистим кэш"]) {
    assert.ok(current.includes(needle), `chat field missing: ${needle}`);
  }
  assert.match(current, new RegExp(MARKER.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&"), "u"));
  assert.match(current, /не сообщение пользователя/u);

  const empty = renderAnchor({ status: "uninitialized", doc: null });
  assert.match(empty, /НЕ ЗАПИСАН/u);
  assert.match(empty, /control_task_set/u);

  const broken = renderAnchor({ status: "unavailable", doc: null, error: "битый файл", statePath: "/x" });
  assert.match(broken, /НЕДОСТУПЕН/u);
  assert.match(broken, /битый файл/u);
  assert.match(broken, /\/x/u);
});

test("render: overflow degrades requirements but keeps the chat context verbatim", () => {
  const big = doc({
    explicit: Array.from({ length: 9 }, (_, i) => `требование ${i} `.repeat(230).trim()),
  });
  const text = renderAnchor({ status: "current", doc: big, turnCount: 1, writtenAtTurn: 0 });
  assert.ok(text.length < 20500, `overflow branch not taken: ${text.length}`);
  for (const needle of ["переезд сервера", "Димоня", "диск почти полон", "чистим кэш"]) {
    assert.ok(text.includes(needle), `chat field lost on overflow: ${needle}`);
  }
  assert.match(text, /превышает/u);
  assert.match(text, /ОБЯЗАТЕЛЬСТВО/u, "the obligation survives the degraded branch");
});

test("render: no obligation line when the record covers the turn", () => {
  const text = renderAnchor({ status: "current", doc: doc(), turnCount: 3, writtenAtTurn: 3 });
  assert.doesNotMatch(text, /ОБЯЗАТЕЛЬСТВО/u);
});

// ── injection across wire shapes ────────────────────────────────────────────

const ANCHOR = `${MARKER}\nЦЕЛЬ: тест`;

test("inject: OpenAI Responses — leading instruction item, string and block content", () => {
  const stringHead = {
    input: [
      { role: "developer", content: "system prompt" },
      { role: "user", content: [{ type: "input_text", text: "привет" }] },
    ],
  };
  const first = injectAnchor(stringHead, ANCHOR);
  assert.equal(first.changed, true);
  assert.equal(stringHead.input.length, 3);
  assert.equal(stringHead.input[0].content, ANCHOR);
  assert.equal(injectAnchor(stringHead, ANCHOR).changed, false, "second pass must not duplicate");

  const blockHead = {
    input: [{ role: "system", content: [{ type: "input_text", text: "system prompt" }] }],
  };
  assert.equal(injectAnchor(blockHead, ANCHOR).changed, true);
  assert.equal(blockHead.input.length, 2);

  const emptyInput = { input: [] };
  assert.equal(injectAnchor(emptyInput, ANCHOR).changed, true);
  assert.equal(emptyInput.input[0].role, "system");
});

test("inject: Anthropic system text and blocks", () => {
  const asText = { system: "ты бот", messages: [{ role: "user", content: "hi" }] };
  assert.equal(injectAnchor(asText, ANCHOR).changed, true);
  assert.match(asText.system, /ты бот[\s\S]*omo-control-task:v2/u);
  assert.equal(injectAnchor(asText, ANCHOR).changed, false);

  const asBlocks = { system: [{ type: "text", text: "ты бот" }], messages: [] };
  assert.equal(injectAnchor(asBlocks, ANCHOR).changed, true);
  assert.equal(asBlocks.system.length, 2);
  assert.equal(asBlocks.system[1].text, ANCHOR);
});

test("inject: Chat Completions leading system message", () => {
  const withSystem = { messages: [{ role: "system", content: "rules" }, { role: "user", content: "hi" }] };
  assert.equal(injectAnchor(withSystem, ANCHOR).changed, true);
  assert.match(withSystem.messages[0].content, /rules[\s\S]*omo-control-task:v2/u);
  assert.equal(injectAnchor(withSystem, ANCHOR).changed, false);

  const withoutSystem = { messages: [{ role: "user", content: "hi" }] };
  assert.equal(injectAnchor(withoutSystem, ANCHOR).changed, true);
  assert.equal(withoutSystem.messages[0].role, "system");
  assert.equal(withoutSystem.messages.length, 2);
});

test("inject: unknown payload shape is a no-op, never a throw", () => {
  for (const payload of [null, undefined, 42, "text", { foo: "bar" }, { input: "not-an-array" }]) {
    const res = injectAnchor(payload, ANCHOR);
    assert.equal(res.changed, false);
  }
  assert.equal(injectAnchor({ foo: "bar" }, ANCHOR).unknownShape, true);
  assert.equal(injectAnchor(null, ANCHOR).unknownShape, undefined);
});

// ── wiring and containment ──────────────────────────────────────────────────

test("resolveStateDir: OMO_ wins, CHATBOT_ fallback, otherwise null (no ./state writes)", () => {
  assert.equal(resolveStateDir({ OMO_CHATBOT_STATE_DIR: "/a", CHATBOT_STATE_DIR: "/b" }), "/a");
  assert.equal(resolveStateDir({ CHATBOT_STATE_DIR: "/b" }), "/b");
  assert.equal(resolveStateDir({}), null);
});

test("factory: registers both tools and the two hooks; a thrown storage call never escapes", async () => {
  const { dir, cleanup } = workdir();
  const saved = process.env.OMO_CHATBOT_STATE_DIR;
  process.env.OMO_CHATBOT_STATE_DIR = dir;
  const tools = new Map();
  const hooks = new Map();
  const pi = {
    registerTool: (tool) => tools.set(tool.name, tool),
    on: (event, handler) => hooks.set(event, handler),
  };
  try {
    chatControlTask(pi);
    assert.deepEqual([...tools.keys()], ["control_task_set", "control_task_add"]);
    assert.equal(typeof hooks.get("before_provider_request"), "function");
    assert.equal(typeof hooks.get("turn_start"), "function");
    assert.match(tools.get("control_task_set").description, /контекст/u);
    assert.match(tools.get("control_task_add").description, /Дополняет/u);

    const okSet = await tools.get("control_task_set").execute("c1", doc());
    assert.match(okSet.content[0].text, /^control_task_set: записано/u);

    // A poisoned record must produce text, not an exception.
    writeFileSync(docPath(dir), "{broken");
    const refused = await tools.get("control_task_add").execute("c2", { chat: { topic: "x" } });
    assert.match(refused.content[0].text, /отклонено/u);

    hooks.get("turn_start")();
    const payload = { input: [{ role: "developer", content: "sys" }] };
    const poisoned = hooks.get("before_provider_request")({ payload });
    assert.match(String(poisoned.input[0].content), /НЕДОСТУПЕН/u, "poison renders as text");

    writeFileSync(docPath(dir), JSON.stringify({ schemaVersion: 1, turnCount: 0, writtenAtTurn: 0, revisions: [] }));
    const injected = hooks.get("before_provider_request")({ payload: { input: [{ role: "developer", content: "sys" }] } });
    assert.equal(injected.input.length, 2);
    assert.match(String(injected.input[0].content), /НЕ ЗАПИСАН/u);

    assert.ok(readFileSync(logPath(dir), "utf8").includes('"event":"loaded"'), "loaded line is logged");
  } finally {
    if (saved === undefined) delete process.env.OMO_CHATBOT_STATE_DIR;
    else process.env.OMO_CHATBOT_STATE_DIR = saved;
    cleanup();
  }
});

test("containment: the extension holds no Bot API surface", () => {
  const source = readFileSync(new URL("./chat-control-task.mjs", import.meta.url), "utf8");
  for (const needle of ["api.telegram.org", "tg-bot", "fetch("]) {
    assert.equal(source.includes(needle), false, `must not contain ${needle}`);
  }
});
