/**
 * Unit tests for the md-inject prompt plan.
 *
 * The runtime hook is not started here: the knowledge directory, the session
 * name and the arrival counter are plain arguments, so the contract (what lands
 * in the system prompt of one turn) is checkable without a host.
 *
 * The checks are on properties — which rules and which documents ride which
 * arrival, and what that costs — never on prompt wording: a test that pins a
 * sentence forbids improving the sentence. Rule ids are the machine-handle the
 * owner's rules already carry.
 *
 * Run: node --test md-inject.test.mjs
 */
import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  LIGHT_MAX_BYTES,
  gatherKnowledge,
  isSummarySession,
  newState,
  plan,
  resetState,
  resolveKnowledgeDir,
  rulesBlock,
  rulesIds,
} from "./md-inject.ts";

const BASE = "БАЗОВЫЙ ПРОМПТ ПЕРСОНЫ";
const CHAT = { sessionName: "chat-participant", summarySession: "chat-summary" };

function tmpDir(prefix = "md-inject-") {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function writeKnowledge(dir, files) {
  fs.mkdirSync(dir, { recursive: true });
  for (const [name, body] of Object.entries(files)) {
    fs.writeFileSync(path.join(dir, name), body);
  }
}

function bytesOf(entries, name) {
  return Buffer.byteLength(entries[name] ?? "", "utf8");
}

/** One light document and three heavy ones with distinguishable sizes. Sorted
 * by name the heavy documents are `01`, `02`, `03` — the rotation order. */
function mixedFixture() {
  return {
    "00-лёгкий.md": "лёгкая запись",
    "01-тяжёлый.md": "a".repeat(LIGHT_MAX_BYTES + 500),
    "02-тяжёлый.md": "b".repeat(LIGHT_MAX_BYTES + 790),
    "03-тяжёлый.md": "c".repeat(LIGHT_MAX_BYTES + 970),
  };
}

const TIER_IDS = {
  1: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
  2: [1, 2, 3, 4, 5, 6, 10],
  3: [5, 7, 8, 9, 10, 11],
  4: [1, 2, 3, 4, 5, 6, 10],
  5: [5, 10],
  6: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
};

test("the first arrival carries everything: all paths, both halves of the rules, every rule id", () => {
  const root = tmpDir();
  const dir = path.join(root, "knowledge");
  const files = {
    "Дискурс.md": "# Дискурс\n\nлиния беседы",
    "Важные персоны вне канала.md": "# Персоны\n\nвне канала",
    "Словарь.md": "# Словарь\n\nтермины",
    "Список легенд(донатеров).md": "# Легенды\n\nBront @Oleg3573 - 200 рублей",
    "Список важных событий.md": "# События\n\nчто-то было",
  };
  writeKnowledge(dir, files);

  const result = plan(BASE, newState("s1"), { ...CHAT, knowledgeDir: dir, cwd: root });
  assert.ok(result.systemPrompt, "the turn must get a prompt");
  assert.ok(
    result.systemPrompt.startsWith(BASE),
    "the block must be appended to the base prompt, never replace it",
  );
  for (const name of Object.keys(files)) {
    const absolute = path.join(dir, name);
    assert.ok(result.systemPrompt.includes(absolute), `missing path ${absolute}`);
  }
  assert.ok(result.systemPrompt.includes("Bront @Oleg3573 - 200 рублей"));
  assert.equal(result.paths.length, 5);
  assert.deepEqual(result.rulesIds, TIER_IDS[1]);
  assert.equal(result.rules, true);
  const total = Object.keys(files).reduce((sum, name) => sum + bytesOf(files, name), 0);
  assert.equal(result.knowledgeBytes, total);
});

test("after the first arrival exactly one rotation document rides, cycling by period", () => {
  const root = tmpDir();
  const dir = path.join(root, "knowledge");
  const files = mixedFixture();
  writeKnowledge(dir, files);
  const rotationDocs = ["01-тяжёлый.md", "02-тяжёлый.md", "03-тяжёлый.md"].map((n) =>
    path.join(dir, n),
  );
  const light = path.join(dir, "00-лёгкий.md");
  const state = newState("s1");
  const ctx = { ...CHAT, knowledgeDir: dir, cwd: root };

  const first = plan(BASE, state, ctx);
  for (const heavyPath of rotationDocs) {
    assert.ok(first.systemPrompt.includes(heavyPath), `first arrival misses ${heavyPath}`);
  }

  for (let arrival = 2; arrival <= 7; arrival += 1) {
    const result = plan(BASE, state, ctx);
    assert.equal(state.arrivals, arrival);
    const carried = rotationDocs.filter((heavyPath) => result.systemPrompt.includes(heavyPath));
    assert.equal(carried.length, 1, `arrival ${arrival} carried ${carried.length} rotation docs`);
    assert.equal(
      carried[0],
      rotationDocs[arrival % rotationDocs.length],
      `arrival ${arrival} rotated wrong`,
    );
    assert.ok(result.systemPrompt.includes(light), `arrival ${arrival} lost the light document`);
    assert.equal(result.heavy, carried[0]);
  }
});

test("the knowledge budget per arrival is at most one rotation document plus the light ones", () => {
  const root = tmpDir();
  const dir = path.join(root, "knowledge");
  const files = mixedFixture();
  writeKnowledge(dir, files);
  const state = newState("s1");
  const ctx = { ...CHAT, knowledgeDir: dir, cwd: root };

  const lights = bytesOf(files, "00-лёгкий.md");
  const maxHeavy = Math.max(...Object.keys(files).map((n) => bytesOf(files, n)));
  const total = Object.keys(files).reduce((sum, name) => sum + bytesOf(files, name), 0);
  const ceiling = maxHeavy + lights;

  const first = plan(BASE, state, ctx);
  assert.equal(first.knowledgeBytes, total, "the first arrival carries the whole catalog");

  let sum = 0;
  for (let arrival = 2; arrival <= 7; arrival += 1) {
    const result = plan(BASE, state, ctx);
    assert.ok(result.knowledgeBytes > 0);
    assert.ok(
      result.knowledgeBytes <= ceiling,
      `arrival ${arrival} paid ${result.knowledgeBytes} B, ceiling ${ceiling} B`,
    );
    assert.ok(result.knowledgeBytes < total, `arrival ${arrival} paid the whole catalog`);
    sum += result.knowledgeBytes;
  }
  assert.ok(sum / 6 < total, "rotation must be cheaper than shipping everything");
});

test("rules ride their own priority tiers: high-impact rules every turn, stable ones less often", () => {
  const root = tmpDir();
  const dir = path.join(root, "knowledge");
  writeKnowledge(dir, { "Дискурс.md": "тема" });
  const state = newState("s1");
  const ctx = { ...CHAT, knowledgeDir: dir, cwd: root };

  for (let arrival = 1; arrival <= 6; arrival += 1) {
    const expected = TIER_IDS[arrival];
    const result = plan(BASE, state, ctx);
    assert.deepEqual(result.rulesIds, expected, `arrival ${arrival} ids`);
    assert.deepEqual(result.rulesIds, rulesIds(arrival), `arrival ${arrival} vs rulesIds()`);
    assert.equal(result.rules, true);
    assert.ok(
      result.systemPrompt.includes(rulesBlock(arrival)),
      `arrival ${arrival} prompt misses its rules block`,
    );
  }
  for (const id of [10]) {
    for (let arrival = 1; arrival <= 6; arrival += 1) {
      assert.ok(TIER_IDS[arrival].includes(id), `rule ${id} must ride arrival ${arrival}`);
    }
  }
  assert.ok(!TIER_IDS[2].includes(11), "rule 11 must be demoted, not every turn");
  assert.ok(!TIER_IDS[2].includes(7), "a stable rule must be able to sit out a turn");
});

test("a reset (session start or compact) brings every rule and the full knowledge set back", () => {
  const root = tmpDir();
  const dir = path.join(root, "knowledge");
  const files = mixedFixture();
  writeKnowledge(dir, files);
  const state = newState("s1");
  const ctx = { ...CHAT, knowledgeDir: dir, cwd: root };

  plan(BASE, state, ctx);
  plan(BASE, state, ctx);
  resetState(state);
  const afterReset = plan(BASE, state, ctx);
  assert.equal(afterReset.paths.length, 4, "a reset must restore the whole knowledge set");
  assert.deepEqual(afterReset.rulesIds, TIER_IDS[1]);
});

test("the cwd notes file rotates even when it is smaller than the light threshold", () => {
  const root = tmpDir();
  const dir = path.join(root, "knowledge");
  writeKnowledge(dir, {
    "00-лёгкий.md": "лёгкая запись",
    "01-тяжёлый.md": "a".repeat(LIGHT_MAX_BYTES + 200),
  });
  const notes = path.join(root, "agents.md");
  fs.writeFileSync(notes, "заметки владельца");
  const state = newState("s1");
  const ctx = { ...CHAT, knowledgeDir: dir, cwd: root };

  const rotationLen = 2; // one heavy document + the notes file
  let notesCarried = 0;
  for (let arrival = 1; arrival <= 1 + rotationLen * 2; arrival += 1) {
    const result = plan(BASE, state, ctx);
    const carriesNotes = result.paths.includes(notes);
    if (carriesNotes && arrival > 1) {
      notesCarried += 1;
      assert.equal(result.heavy, notes, "the notes file must be the rotation document it rides as");
    }
    if (arrival > 1) {
      assert.notEqual(result.heavy, null, `arrival ${arrival} carried no rotation document`);
    }
  }
  assert.ok(notesCarried >= 2, "the notes file must reach the model within the rotation cycle");
  assert.ok(notesCarried < 1 + rotationLen * 2 - 1, "the notes file must not ride every arrival");
});

test("the summarizing session gets nothing at all", () => {
  const root = tmpDir();
  const dir = path.join(root, "knowledge");
  writeKnowledge(dir, { "Дискурс.md": "тема" });
  const state = newState("sum");

  const result = plan(BASE, state, {
    knowledgeDir: dir,
    cwd: root,
    sessionName: "chat-summary",
    summarySession: "chat-summary",
  });
  assert.equal(result.systemPrompt, undefined);
  assert.equal(result.rules, false);
  assert.deepEqual(result.rulesIds, []);
  assert.equal(result.knowledgeBytes, 0);
  assert.equal(result.paths.length, 0);
  assert.equal(state.arrivals, 0, "the summary host must not consume the cadence");
  assert.equal(isSummarySession("chat-summary", "chat-summary"), true);
  assert.equal(isSummarySession("chat-participant", "chat-summary"), false);
  assert.equal(isSummarySession(undefined, "chat-summary"), false);
});

test("a new *.md enters without a code change: light at once, heavy within the period", () => {
  const root = tmpDir();
  const dir = path.join(root, "knowledge");
  const files = mixedFixture();
  writeKnowledge(dir, files);
  const state = newState("s1");
  const ctx = { ...CHAT, knowledgeDir: dir, cwd: root };
  plan(BASE, state, ctx);

  fs.writeFileSync(path.join(dir, "04-мемы.md"), "мем про кота");
  const lightArrival = plan(BASE, state, ctx);
  assert.ok(lightArrival.systemPrompt.includes(path.join(dir, "04-мемы.md")));
  assert.ok(lightArrival.systemPrompt.includes("мем про кота"));

  fs.writeFileSync(path.join(dir, "05-архив.md"), "z".repeat(LIGHT_MAX_BYTES + 2110));
  const heavyPath = path.join(dir, "05-архив.md");
  let seenAt = 0;
  for (let i = 2; i <= 7 && seenAt === 0; i += 1) {
    const result = plan(BASE, state, ctx);
    if (result.systemPrompt.includes(heavyPath)) seenAt = state.arrivals;
  }
  assert.ok(seenAt > 0, "a new heavy document never entered the rotation");
  assert.ok(seenAt - 1 <= 5, `the new heavy waited ${seenAt - 1} arrivals`);
});

test("the nonce is honest: a marker on a rotated-out document is not reported", () => {
  const root = tmpDir();
  const dir = path.join(root, "knowledge");
  const files = mixedFixture();
  // Sorted rotation is 01, 02, 03; arrivals 2..7 carry 03, 01, 02, 03, 01, 02.
  files["01-тяжёлый.md"] = `начало nonce-A1b2C3d4 ${"a".repeat(LIGHT_MAX_BYTES)}`;
  writeKnowledge(dir, files);
  const state = newState("s1");
  const ctx = { ...CHAT, knowledgeDir: dir, cwd: root };

  const full = plan(BASE, state, ctx);
  assert.equal(full.nonce, "nonce-A1b2C3d4", "the first arrival ships the marker document");
  assert.ok(full.systemPrompt.includes("маркер поставки: nonce-A1b2C3d4"));

  const rotatedOut = plan(BASE, state, ctx);
  assert.equal(rotatedOut.paths.some((p) => p.endsWith("01-тяжёлый.md")), false);
  assert.equal(rotatedOut.nonce, null, "a marker outside the delivered set must not be reported");
  assert.equal(rotatedOut.systemPrompt.includes("маркер поставки"), false);
  assert.equal(rotatedOut.systemPrompt.includes("nonce-A1b2C3d4"), false);
});

test("an empty file contributes only its path", () => {
  const root = tmpDir();
  const dir = path.join(root, "knowledge");
  writeKnowledge(dir, { "Словарь.md": "" });
  const emptyPath = path.join(dir, "Словарь.md");

  const result = plan(BASE, newState("s1"), { ...CHAT, knowledgeDir: dir, cwd: root });
  assert.ok(result.systemPrompt.includes(emptyPath));
  const after = result.systemPrompt.slice(result.systemPrompt.indexOf(emptyPath) + emptyPath.length);
  assert.ok(
    after.startsWith("\n\n["),
    `no body may follow an empty file, got ${JSON.stringify(after.slice(0, 40))}`,
  );
});

test("service files stay out, the cwd agents.md goes in with its own path", () => {
  const root = tmpDir();
  const dir = path.join(root, "knowledge");
  writeKnowledge(dir, {
    "Дискурс.md": "тема",
    "system-prompt.md": "персона",
    "chat-summary.md": "сводка",
    "knowledge-prompt.md": "промпт крона",
    "AGENTS.md": "верхний регистр",
    "agents.md": "не тот файл",
  });
  fs.writeFileSync(path.join(root, "agents.md"), "важное по cwd");

  const { entries } = gatherKnowledge(dir, { cwd: root });
  const paths = entries.map((entry) => entry.path);
  assert.deepEqual(paths, [path.join(dir, "Дискурс.md"), path.join(root, "agents.md")]);
  assert.equal(entries[1].body, "важное по cwd");
});

test("the knowledge dir is the absolute env value first, cwd only as fallback", () => {
  assert.equal(
    resolveKnowledgeDir({
      env: {
        OMO_CHATBOT_KNOWLEDGE_DIR: "/abs/knowledge",
        CHATBOT_KNOWLEDGE_DIR: "/other",
        CHATBOT_CWD: "/moved",
      },
      cwd: "/session",
    }),
    "/abs/knowledge",
  );
  assert.equal(
    resolveKnowledgeDir({ env: { CHATBOT_KNOWLEDGE_DIR: "/other" }, cwd: "/session" }),
    "/other",
  );
  assert.equal(resolveKnowledgeDir({ env: {}, cwd: "/session" }), "/session/knowledge");
  assert.equal(resolveKnowledgeDir({ env: { CHATBOT_CWD: "/moved" } }), "/moved/knowledge");
  assert.equal(resolveKnowledgeDir({ env: {} }), null);
});

test("a missing directory is reported, never silently treated as empty knowledge", () => {
  const root = tmpDir();
  const dir = path.join(root, "knowledge");
  const result = plan(BASE, newState("s1"), { ...CHAT, knowledgeDir: dir, cwd: root });
  assert.equal(result.missingDir, dir);
  assert.equal(result.rules, true, "rules still ride the first arrival");
});
