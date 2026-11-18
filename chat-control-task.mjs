/**
 * chat-control-task: a durable CHAT-CONTEXT record for the hosted bot plus the
 * anchor that puts it back in front of the model on EVERY provider request.
 *
 * Why this exists: the bot's conversation is compacted once the context window
 * fills up, and after a compaction the model no longer knows what the talk was
 * about — the transcript that carried it is gone. A file outside the transcript
 * is unaffected by compaction, so the model writes the conversation context
 * (topic, participants, what was discussed, what was concluded) with
 * `control_task_set` / `control_task_add`, and this extension re-attaches that
 * record as a system block on each request. Because the block is rebuilt from
 * the file at request time — never stored as a message — a compaction cannot
 * drop it.
 *
 * Invariants kept here:
 *  - The anchor is delivered on EVERY provider request, including the
 *    uninitialized state: silence is how a model forgets mid-conversation.
 *  - The block is instruction text, never a user message, and it says so.
 *  - Storage failures become visible text (`НЕДОСТУПНА` in the anchor, a
 *    `отклонено` reply from a tool) and never throw: a broken file must not
 *    stop the bot from answering.
 *  - Writes are one read → mutate → tmp → rename pass with no `await` in
 *    between, so a mutation is atomic without a queue.
 *
 * Port note (2026-11-06): the hosted bot's agent dir is a bare staged state
 * directory, so this file is self-contained on purpose — `node:*` builtins
 * only, no typebox, no runtime package import. The extension API object is
 * handed in by the host at load time. The desktop original
 * (`~/.omo/agent/extensions/lib/control-task/*`) is the reference for the store
 * shape and tool contract, not a dependency.
 */

import { appendFileSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { join } from "node:path";

/** Marks our block inside a provider request; detection is idempotence, not decoration. */
export const MARKER = "<!-- omo-control-task:v2 -->";

/** Requirements sections, separate from the chat context on purpose. */
const SECTIONS = ["explicit", "implicit", "constraints", "sources"];

/** Chat-context lists; each is rendered in full, so each stays small. */
const CHAT_LISTS = ["participants", "discussed", "conclusions"];

const LIMITS = {
  goal: 2000,
  topic: 1000,
  participants: { items: 20, item: 120 },
  discussed: { items: 25, item: 240 },
  conclusions: { items: 15, item: 240 },
  section: { items: 200, item: 2000 },
  doc: 48000,
};

/** Above this the requirements degrade to counters; the chat context never does. */
export const MAX_INLINE_CHARS = 20000;

/** Revisions kept: the LAST one is canonical, older ones are forensics only. */
export const MAX_STORED_REVISIONS = 50;

const PROVENANCE =
  "Внутренний якорь беседы, не сообщение пользователя. Свежие сообщения пользователя важнее этого блока. " +
  "Здесь только устойчивый контекст беседы: без прогресса, статусов, плана и «сделано/не сделано».";

// ── small helpers ───────────────────────────────────────────────────────────

function isObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function reason(error) {
  return error instanceof Error ? error.message : String(error);
}

/** Comparison key: a line re-sent with other spacing or case is the same line. */
function itemKey(item) {
  return item.trim().replace(/\s+/gu, " ").toLowerCase();
}

function mergeList(existing, incoming, maxItems, maxItemLen, name) {
  const merged = [...existing];
  const seen = new Set(existing.map(itemKey));
  let added = 0;
  let skipped = 0;
  for (const raw of incoming ?? []) {
    if (typeof raw !== "string") return { ok: false, error: `${name}: ожидается список строк` };
    const item = raw.trim();
    if (item === "") continue;
    if (item.length > maxItemLen) {
      return { ok: false, error: `${name}: пункт длиннее ${maxItemLen} символов — сократи` };
    }
    const key = itemKey(item);
    if (seen.has(key)) {
      skipped += 1;
      continue;
    }
    if (merged.length >= maxItems) {
      return {
        ok: false,
        error: `${name}: уже ${maxItems} пунктов — объедини формулировки через control_task_set`,
      };
    }
    seen.add(key);
    merged.push(item);
    added += 1;
  }
  return { ok: true, merged, added, skipped };
}

// ── document parsing (the single validator) ─────────────────────────────────

/**
 * Every document, stored or submitted, passes through here so a malformed field
 * can never reach the renderer. Normalization is trimming only: dropping or
 * rewriting content silently would make the anchor lie about the conversation.
 */
export function parseDoc(raw) {
  if (!isObject(raw)) return { ok: false, error: "документ должен быть объектом" };
  if (typeof raw.goal !== "string" || raw.goal.trim() === "") {
    return { ok: false, error: "goal обязателен: что бот делает в этой беседе" };
  }
  const goal = raw.goal.trim();
  if (goal.length > LIMITS.goal) {
    return { ok: false, error: `goal длиннее ${LIMITS.goal} символов — сократи` };
  }
  if (!isObject(raw.chat)) {
    return { ok: false, error: "chat обязателен: тема, участники, что обсуждали, к чему пришли" };
  }
  const topic = typeof raw.chat.topic === "string" ? raw.chat.topic.trim() : "";
  if (topic === "") return { ok: false, error: "chat.topic обязателен: о чём идёт беседа" };
  if (topic.length > LIMITS.topic) {
    return { ok: false, error: `chat.topic длиннее ${LIMITS.topic} символов — сократи` };
  }
  const chat = { topic };
  for (const key of CHAT_LISTS) {
    const parsed = parseStrings(raw.chat[key], `chat.${key}`, LIMITS[key].items, LIMITS[key].item);
    if (!parsed.ok) return parsed;
    chat[key] = parsed.value;
  }
  const doc = { goal, chat };
  for (const section of SECTIONS) {
    const parsed = parseStrings(raw[section], section, LIMITS.section.items, LIMITS.section.item);
    if (!parsed.ok) return parsed;
    doc[section] = parsed.value;
  }
  if (JSON.stringify(doc).length > LIMITS.doc) {
    return {
      ok: false,
      error: `документ длиннее ${LIMITS.doc} символов — объедини пункты через control_task_set`,
    };
  }
  return { ok: true, value: doc };
}

function parseStrings(raw, name, maxItems, maxItemLen) {
  if (raw === undefined || raw === null) return { ok: true, value: [] };
  if (!Array.isArray(raw)) return { ok: false, error: `${name}: ожидается список строк` };
  if (raw.length > maxItems) {
    return { ok: false, error: `${name}: больше ${maxItems} пунктов — объедини через control_task_set` };
  }
  const value = [];
  for (const entry of raw) {
    if (typeof entry !== "string") return { ok: false, error: `${name}: ожидаются только строки` };
    const item = entry.trim();
    if (item === "") continue;
    if (item.length > maxItemLen) {
      return { ok: false, error: `${name}: пункт длиннее ${maxItemLen} символов — сократи` };
    }
    value.push(item);
  }
  return { ok: true, value };
}

// ── store format ────────────────────────────────────────────────────────────

/**
 * `<state>/control-task/task.json` holds ONE document for the whole bot (the
 * daemon serves one chat), as an append-only revision list whose last entry is
 * canonical. `turnCount` counts turns seen; `writtenAtTurn` is the turn the last
 * write covered — while the first exceeds the second, the anchor states the
 * obligation to record the conversation context after a new batch.
 */
export function parseStore(raw) {
  if (!isObject(raw)) return { ok: false, error: "файл состояния не объект" };
  if (raw.schemaVersion !== 1) {
    return { ok: false, error: `неподдерживаемый schemaVersion ${JSON.stringify(raw.schemaVersion)}` };
  }
  if (!Array.isArray(raw.revisions)) return { ok: false, error: "revisions должен быть массивом" };
  const revisions = [];
  for (const entry of raw.revisions) {
    if (!isObject(entry)) return { ok: false, error: "ревизия не объект" };
    if (typeof entry.mutationId !== "string" || entry.mutationId === "") {
      return { ok: false, error: "у ревизии нет mutationId" };
    }
    if (typeof entry.at !== "string" || entry.at === "") {
      return { ok: false, error: "у ревизии нет at" };
    }
    const doc = parseDoc(entry.doc);
    if (!doc.ok) return { ok: false, error: `невалидная ревизия: ${doc.error}` };
    revisions.push({ mutationId: entry.mutationId, at: entry.at, doc: doc.value });
  }
  return {
    ok: true,
    turnCount: nonNegativeInt(raw.turnCount),
    writtenAtTurn: nonNegativeInt(raw.writtenAtTurn),
    revisions,
  };
}

function nonNegativeInt(raw) {
  return Number.isInteger(raw) && raw >= 0 ? raw : 0;
}

/** Three states, mirroring the durable record's own honesty. */
export function loadStore(dir) {
  const path = docPath(dir);
  let raw;
  try {
    raw = readFileSync(path, "utf8");
  } catch (error) {
    if (error && error.code === "ENOENT") {
      return { status: "uninitialized", doc: null, turnCount: 0, writtenAtTurn: 0 };
    }
    return unavailable(`файл не читается (${reason(error)})`, dir);
  }
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return unavailable("файл состояния — не валидный JSON", dir);
  }
  const store = parseStore(parsed);
  if (!store.ok) return unavailable(store.error, dir);
  const last = store.revisions[store.revisions.length - 1];
  if (last === undefined) {
    return {
      status: "uninitialized",
      doc: null,
      turnCount: store.turnCount,
      writtenAtTurn: store.writtenAtTurn,
    };
  }
  return {
    status: "current",
    doc: last.doc,
    turnCount: store.turnCount,
    writtenAtTurn: store.writtenAtTurn,
  };
}

function unavailable(error, dir) {
  return { status: "unavailable", doc: null, turnCount: 0, writtenAtTurn: 0, error, path: docPath(dir) };
}

export function docPath(dir) {
  return join(dir, "control-task", "task.json");
}

export function logPath(dir) {
  return join(dir, "control-task.log");
}

/**
 * The ONE writer. Read → mutate the whole in-memory copy → tmp in the same dir →
 * rename. Synchronous, so two calls in this process cannot interleave; a present
 * but invalid file is NEVER silently healed — a poison is rejected instead.
 */
export function mutateStore(dir, mutate) {
  const target = docPath(dir);
  let base = { schemaVersion: 1, turnCount: 0, writtenAtTurn: 0, revisions: [] };
  try {
    const raw = readFileSync(target, "utf8");
    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch {
      return { ok: false, error: "файл состояния — не валидный JSON" };
    }
    const store = parseStore(parsed);
    if (!store.ok) return { ok: false, error: store.error };
    base = {
      schemaVersion: 1,
      turnCount: store.turnCount,
      writtenAtTurn: store.writtenAtTurn,
      revisions: store.revisions,
    };
  } catch (error) {
    if (!error || error.code !== "ENOENT") {
      return { ok: false, error: `файл не читается (${reason(error)})` };
    }
  }
  const mutated = mutate(base);
  if (!mutated.ok) return mutated;
  try {
    mkdirSync(join(dir, "control-task"), { recursive: true });
    const tmp = `${target}.tmp-${process.pid}-${randomUUID()}`;
    writeFileSync(tmp, JSON.stringify(base, null, 2), "utf8");
    renameSync(tmp, target); // atomic within the same directory
  } catch (error) {
    return { ok: false, error: `запись не удалась (${reason(error)})` };
  }
  return { ok: true, outcome: mutated.outcome };
}

/** Count one bot turn; the anchor uses the difference to state its obligation. */
export function bumpTurn(dir) {
  return mutateStore(dir, (store) => {
    store.turnCount += 1;
    return { ok: true };
  });
}

function pushRevision(store, doc) {
  store.revisions.push({ mutationId: randomUUID(), at: new Date().toISOString(), doc });
  if (store.revisions.length > MAX_STORED_REVISIONS) {
    store.revisions.splice(0, store.revisions.length - MAX_STORED_REVISIONS);
  }
  store.writtenAtTurn = store.turnCount;
}

function docSummary(doc) {
  return (
    `явных=${doc.explicit.length}, неявных=${doc.implicit.length}, ` +
    `ограничений=${doc.constraints.length}, источников=${doc.sources.length}, ` +
    `участников=${doc.chat.participants.length}, обсуждений=${doc.chat.discussed.length}, ` +
    `выводов=${doc.chat.conclusions.length}`
  );
}

/** Replace the whole record (`control_task_set`). */
export function commitSet(dir, params) {
  const parsed = parseDoc(params);
  if (!parsed.ok) return { ok: false, error: parsed.error };
  const outcome = { text: "", committed: false };
  const written = mutateStore(dir, (store) => {
    const last = store.revisions[store.revisions.length - 1];
    if (last !== undefined && JSON.stringify(last.doc) === JSON.stringify(parsed.value)) {
      // Identical document: no new revision, but the call still proves the model
      // read the current conversation, so the obligation is cleared.
      store.writtenAtTurn = store.turnCount;
      outcome.text = `документ не изменился, писать нечего (${docSummary(parsed.value)})`;
      return { ok: true, outcome };
    }
    pushRevision(store, parsed.value);
    outcome.committed = true;
    outcome.text = `записано (тема: ${parsed.value.chat.topic}; ${docSummary(parsed.value)})`;
    return { ok: true, outcome };
  });
  return written;
}

/** Extend only with genuinely new lines (`control_task_add`); the chat topic is replaced. */
export function commitAdd(dir, params) {
  const outcome = { text: "", committed: false };
  const written = mutateStore(dir, (store) => {
    const last = store.revisions[store.revisions.length - 1];
    if (last === undefined) {
      return {
        ok: false,
        error: "запись ещё не создана; сначала control_task_set{goal, chat:{topic}, …}",
      };
    }
    const next = structuredClone(last.doc);
    if (Array.isArray(params)) return { ok: false, error: "параметры должны быть объектом" };
    const p = isObject(params) ? params : {};
    let added = 0;
    let skipped = 0;
    if (isObject(p.chat)) {
      if (p.chat.topic !== undefined) {
        const topic = typeof p.chat.topic === "string" ? p.chat.topic.trim() : "";
        if (typeof p.chat.topic !== "string") return { ok: false, error: "chat.topic — строка" };
        if (topic === "") return { ok: false, error: "chat.topic не может быть пустым" };
        if (topic.length > LIMITS.topic) {
          return { ok: false, error: `chat.topic длиннее ${LIMITS.topic} символов — сократи` };
        }
        next.chat.topic = topic;
      }
      for (const key of CHAT_LISTS) {
        const merged = mergeList(
          next.chat[key],
          p.chat[key],
          LIMITS[key].items,
          LIMITS[key].item,
          `chat.${key}`,
        );
        if (!merged.ok) return merged;
        next.chat[key] = merged.merged;
        added += merged.added;
        skipped += merged.skipped;
      }
    }
    for (const section of SECTIONS) {
      const merged = mergeList(
        next[section],
        p[section],
        LIMITS.section.items,
        LIMITS.section.item,
        section,
      );
      if (!merged.ok) return merged;
      next[section] = merged.merged;
      added += merged.added;
      skipped += merged.skipped;
    }
    if (JSON.stringify(next).length > LIMITS.doc) {
      return {
        ok: false,
        error: `документ длиннее ${LIMITS.doc} символов — объедини пункты через control_task_set`,
      };
    }
    if (added === 0 && next.chat.topic === last.doc.chat.topic) {
      // A no-op call still proves the model looked at the conversation: it
      // clears the obligation without growing the record.
      store.writtenAtTurn = store.turnCount;
      outcome.text = `все пункты уже записаны, ничего не добавлено (${docSummary(last.doc)})`;
      return { ok: true, outcome };
    }
    pushRevision(store, next);
    outcome.committed = true;
    outcome.text =
      `добавлено ${added}` +
      (skipped > 0 ? `, пропущено дублей ${skipped}` : "") +
      `; тема: ${next.chat.topic}; ${docSummary(next)}`;
    return { ok: true, outcome };
  });
  return written;
}

// ── the anchor text ─────────────────────────────────────────────────────────

function lines(items) {
  return items.length === 0 ? ["— не записано —"] : items.map((item) => `- ${item}`);
}

function chatBody(doc) {
  return [
    "КОНТЕКСТ БЕСЕДЫ:",
    `Тема: ${doc.chat.topic}`,
    "",
    "Участники:",
    ...lines(doc.chat.participants),
    "",
    "Что обсуждали:",
    ...lines(doc.chat.discussed),
    "",
    "К чему пришли:",
    ...lines(doc.chat.conclusions),
  ].join("\n");
}

function requirementsBody(doc) {
  const out = [];
  const labels = [
    ["explicit", "ЯВНЫЕ ТРЕБОВАНИЯ (сказано пользователем прямо)"],
    ["implicit", "НЕЯВНЫЕ ТРЕБОВАНИЯ (следуют из сказанного; в скобках основание)"],
    ["constraints", "ОГРАНИЧЕНИЯ И ПРАВИЛА"],
    ["sources", "ИСТОЧНИКИ ИСТИНЫ"],
  ];
  for (const [key, label] of labels) {
    out.push("", `${label}:`, ...lines(doc[key]));
  }
  return out.join("\n");
}

function obligationLine(store) {
  const missed = store.turnCount - store.writtenAtTurn;
  const scope =
    missed === 1
      ? "с прошлой записи прошёл новый ход беседы"
      : `с прошлой записи прошло ${missed} ходов беседы`;
  return [
    `ОБЯЗАТЕЛЬСТВО: ${scope}, а записанный контекст беседы не обновлён.`,
    "Если в беседе появились новые темы, участники, обсуждения или выводы — допиши их в этом ходу: control_task_add{chat:{topic, participants, discussed, conclusions}}.",
    "Если контекст изменился целиком — control_task_set с полным документом.",
    "Если нового нет — не вызывай инструмент.",
  ].join(" ");
}

export function renderAnchor(input) {
  const { status, doc, turnCount = 0, writtenAtTurn = 0, error, statePath } = input;
  if (status === "unavailable") {
    return [
      MARKER,
      "КОНТЕКСТ БЕСЕДЫ — НЕДОСТУПЕН.",
      `Хранилище не читается (${error ?? "unknown"}). Из этой ошибки нельзя выводить ни содержание беседы, ни его отсутствие.`,
      statePath ? `Файл: ${statePath}` : "",
    ]
      .filter(Boolean)
      .join("\n");
  }
  if (status !== "current" || !doc) {
    return [
      MARKER,
      PROVENANCE,
      "",
      "КОНТЕКСТ БЕСЕДЫ — НЕ ЗАПИСАН.",
      "После сжатия контекста модель потеряет, о чём шла речь. Первое действие: control_task_set{goal, chat:{topic, participants, discussed, conclusions}} — тема обязательна, остальное по мере того, как беседа это дала.",
    ].join("\n");
  }

  const chat = chatBody(doc);
  const full = [`${MARKER}\n${PROVENANCE}`, "", `ЦЕЛЬ: ${doc.goal}`, "", chat, requirementsBody(doc)].join("\n");
  const store = { turnCount, writtenAtTurn };
  const duty = turnCount > writtenAtTurn ? `\n\n${obligationLine(store)}` : "";

  if (full.length + duty.length <= MAX_INLINE_CHARS) return `${full}${duty}`;

  // Requirements may degrade to counters; the chat context is exactly what the
  // model must not lose after a compaction, so it stays verbatim.
  const counts = SECTIONS.map((key) => `${key}=${doc[key].length}`).join(", ");
  return [
    MARKER,
    PROVENANCE,
    "",
    `ЦЕЛЬ: ${doc.goal}`,
    "",
    chat,
    `Требования записаны (${counts}); их полный текст вместе с этим блоком превышает ${MAX_INLINE_CHARS} символов.`,
    statePath ? `Полный документ: ${statePath}` : "",
    duty ? obligationLine(store) : "",
  ]
    .filter(Boolean)
    .join("\n");
}

// ── injecting the anchor into a provider request ────────────────────────────

function contentText(content) {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content
    .map((block) => (isObject(block) && typeof block.text === "string" ? block.text : ""))
    .join("\n");
}

function appendToContent(content, text) {
  if (typeof content === "string") return `${content}\n\n${text}`;
  if (Array.isArray(content)) return [...content, { type: "text", text }];
  return [{ type: "text", text }];
}

/**
 * Put the anchor where the provider actually reads instructions, for the three
 * wire shapes the runtime can produce:
 *   - OpenAI Responses: `payload.input`, instruction item first (the bot's shape);
 *   - Anthropic Messages: `payload.system` as text or text blocks;
 *   - Chat Completions: `payload.messages`, leading system message.
 * An already-present marker makes this a no-op, so a request can be re-entered
 * without stacking anchors. An unknown shape is left untouched and reported.
 */
export function injectAnchor(payload, text) {
  if (!isObject(payload)) return { changed: false, payload };

  if (Array.isArray(payload.input)) {
    const head = payload.input[0];
    if (isObject(head) && (head.role === "system" || head.role === "developer")) {
      if (contentText(head.content).includes(MARKER)) return { changed: false, payload };
      payload.input.unshift({ role: "system", content: text });
      return { changed: true, payload };
    }
    if (payload.input.some((item) => isObject(item) && contentText(item.content).includes(MARKER))) {
      return { changed: false, payload };
    }
    payload.input.unshift({ role: "system", content: text });
    return { changed: true, payload };
  }

  if (typeof payload.system === "string" || Array.isArray(payload.system)) {
    if (contentText(payload.system).includes(MARKER)) return { changed: false, payload };
    payload.system = appendToContent(payload.system, text);
    return { changed: true, payload };
  }

  if (Array.isArray(payload.messages)) {
    const head = payload.messages[0];
    if (isObject(head) && (head.role === "system" || head.role === "developer")) {
      if (contentText(head.content).includes(MARKER)) return { changed: false, payload };
      head.content = appendToContent(head.content, text);
      return { changed: true, payload };
    }
    if (payload.messages.some((item) => isObject(item) && contentText(item.content).includes(MARKER))) {
      return { changed: false, payload };
    }
    payload.messages.unshift({ role: "system", content: text });
    return { changed: true, payload };
  }

  return { changed: false, payload, unknownShape: true };
}

// ── wiring ──────────────────────────────────────────────────────────────────

/** Host launch-spec env only allows (SENPI|OMO|PI)_[A-Z0-9_]+ keys. */
export function resolveStateDir(env = process.env) {
  for (const key of ["OMO_CHATBOT_STATE_DIR", "CHATBOT_STATE_DIR"]) {
    const value = env[key];
    if (typeof value === "string" && value !== "") return value;
  }
  return null;
}

function logLine(dir, fields) {
  if (dir === null) return;
  try {
    mkdirSync(dir, { recursive: true });
    appendFileSync(logPath(dir), `${JSON.stringify({ ts: new Date().toISOString(), ...fields })}\n`, {
      mode: 0o600,
    });
  } catch {
    // Observability must never break a turn or a write.
  }
}

function reply(tool, text) {
  return { content: [{ type: "text", text: `${tool}: ${text}` }] };
}

const CHAT_SCHEMA = {
  type: "object",
  description:
    "Текущий контекст беседы: о чём речь, кто участвовал, что обсуждали, к чему пришли.",
  properties: {
    topic: { type: "string", description: "Тема беседы одним предложением." },
    participants: {
      type: "array",
      items: { type: "string" },
      description: "Кто участвует: имена/роли из беседы.",
    },
    discussed: {
      type: "array",
      items: { type: "string" },
      description: "Что обсуждали — по пункту на тему.",
    },
    conclusions: {
      type: "array",
      items: { type: "string" },
      description: "К чему пришли: решения, договорённости, итоги.",
    },
  },
  required: ["topic"],
  additionalProperties: false,
};

const SECTION_ARRAY = { type: "array", items: { type: "string" } };

const SET_DESCRIPTION =
  "Полностью заменяет durable-запись беседы, которая переживает сжатие контекста: после компакта " +
  "модель теряет, о чём шла речь, а эта запись возвращается в каждый запрос. " +
  "chat — текущий контекст: topic (о чём беседа), participants (кто участвовал), " +
  "discussed (что обсуждали), conclusions (к чему пришли). goal — что бот делает в этой беседе. " +
  "explicit/implicit — требования, сказанные прямо и выведенные (с основанием в скобках); " +
  "constraints — правила и ограничения; sources — пути и источники истины. " +
  "Не пиши прогресс, статусы, план, результаты тулов и «сделано/не сделано» — это живёт только в разговоре. " +
  "Вызывай, когда контекст беседы сменился целиком; для дополнения дешевле control_task_add.";

const ADD_DESCRIPTION =
  "Дополняет durable-запись беседы ТОЛЬКО новым: то, что уже записано (в том же смысле и написании), " +
  "пропускается и возвращается в отчёте, поэтому повторный вызов безопасен. " +
  "chat.topic заменяет тему, а participants/discussed/conclusions и секции требований дополняются. " +
  "Вызывай после новой пачки сообщений: сначала обнови chat (что обсуждали, к чему пришли), затем требования. " +
  "Для переписывания целиком или отмены — control_task_set с полным документом.";

/**
 * Register the durable-record tools and the per-request anchor.
 * `pi` is the host's extension API; everything below degrades to text, never to
 * a thrown error, because a storage fault must not silence the bot.
 */
export default function chatControlTask(pi) {
  const dir = resolveStateDir();
  logLine(dir, { event: "loaded", state_dir: dir });

  pi.registerTool({
    name: "control_task_set",
    label: "Record the chat context",
    description: SET_DESCRIPTION,
    parameters: {
      type: "object",
      properties: {
        goal: { type: "string" },
        chat: CHAT_SCHEMA,
        explicit: SECTION_ARRAY,
        implicit: SECTION_ARRAY,
        constraints: SECTION_ARRAY,
        sources: SECTION_ARRAY,
      },
      required: ["goal", "chat"],
      additionalProperties: false,
    },
    execute: async (_toolCallId, params) => {
      if (dir === null) {
        return reply(
          "control_task_set",
          "отклонено — каталог состояния бота не задан (OMO_CHATBOT_STATE_DIR), хранилище недоступно",
        );
      }
      try {
        const written = commitSet(dir, params);
        if (!written.ok) return reply("control_task_set", `отклонено — ${written.error}`);
        logLine(dir, {
          event: "write",
          tool: "set",
          committed: written.outcome.committed,
          state: loadStore(dir).status,
        });
        return reply("control_task_set", written.outcome.text);
      } catch (error) {
        logLine(dir, { event: "error", tool: "set", error: reason(error) });
        return reply("control_task_set", "внутренняя ошибка записи; запись не изменена");
      }
    },
  });

  pi.registerTool({
    name: "control_task_add",
    label: "Add to the chat context",
    description: ADD_DESCRIPTION,
    parameters: {
      type: "object",
      properties: {
        chat: CHAT_SCHEMA,
        explicit: SECTION_ARRAY,
        implicit: SECTION_ARRAY,
        constraints: SECTION_ARRAY,
        sources: SECTION_ARRAY,
      },
      additionalProperties: false,
    },
    execute: async (_toolCallId, params) => {
      if (dir === null) {
        return reply(
          "control_task_add",
          "отклонено — каталог состояния бота не задан (OMO_CHATBOT_STATE_DIR), хранилище недоступно",
        );
      }
      try {
        const written = commitAdd(dir, params);
        if (!written.ok) return reply("control_task_add", `отклонено — ${written.error}`);
        logLine(dir, {
          event: "write",
          tool: "add",
          committed: written.outcome.committed,
          state: loadStore(dir).status,
        });
        return reply("control_task_add", written.outcome.text);
      } catch (error) {
        logLine(dir, { event: "error", tool: "add", error: reason(error) });
        return reply("control_task_add", "внутренняя ошибка записи; запись не изменена");
      }
    },
  });

  // The bot's inbox is a custom `session_control_delivery` message, not an
  // `input` event, so "a new batch arrived" is counted here: every turn start is
  // one more conversation step the record has not been read against yet.
  pi.on("turn_start", () => {
    if (dir === null) return;
    try {
      const bumped = bumpTurn(dir);
      if (!bumped.ok) logLine(dir, { event: "turn-count-failed", error: bumped.error });
    } catch (error) {
      logLine(dir, { event: "turn-count-failed", error: reason(error) });
    }
  });

  // The anchor rides on EVERY request: it is rebuilt from the file, so a
  // compaction cannot take it away with the transcript.
  pi.on("before_provider_request", (event) => {
    if (dir === null) return undefined;
    try {
      const store = loadStore(dir);
      const text = renderAnchor({
        status: store.status,
        doc: store.doc,
        turnCount: store.turnCount,
        writtenAtTurn: store.writtenAtTurn,
        error: store.error,
        statePath: docPath(dir),
      });
      const injected = injectAnchor(event && event.payload, text);
      if (injected.unknownShape) {
        logLine(dir, { event: "inject-unknown-shape" });
        return undefined;
      }
      if (!injected.changed) return undefined;
      if (process.env.OMO_CONTROL_TASK_DEBUG === "1") {
        logLine(dir, {
          event: "inject",
          state: store.status,
          chars: text.length,
          unreviewed: store.turnCount - store.writtenAtTurn,
        });
      }
      return injected.payload;
    } catch (error) {
      logLine(dir, { event: "inject-failed", error: reason(error) });
      return undefined;
    }
  });
}
