// md-inject — put the bot's own knowledge files and its operating rules into the
// system prompt of a chat turn.
//
// Why `before_agent_start` + `systemPrompt` and never `message`:
// - `message` returned from this hook is STORED in the session and replayed in
//   every later request, so a per-turn block would accumulate a full copy each
//   time; the whole point of moving the knowledge out of history is the context
//   budget.
// - `systemPrompt` REPLACES the prompt for the current turn (chained across
//   extensions). Replacing it alone would silently drop the persona, so the
//   block is always APPENDED to `event.systemPrompt`; when there is nothing to
//   add the handler returns `undefined` and the base prompt stays untouched.
// - Inside one run the override survives compaction (`_systemPromptOverride` in
//   the installed engine is not cleared by it), so no re-injection on
//   `session_compact` is needed — only the cadence counter resets there.
//
// Rules ride per-rule cadences, not as one block and not as two coarse bands
// (owner, 2026-11-17: «правила по каденциям распределить … по приоритету правил
// … проанализировать по поведению агента»). Priority is derived from the live
// corpus measured on 2026-11-17 (2 663 replies, plus the owner's own decrees):
// - every arrival — 10 (Russian only) and 11 (no quoting/re-telling; broaden
//   the discourse): the corpus shows language leaks still happening while the
//   rule rode every turn (Latin 1.2 %, CJK 0.2 %, spiking on 2026-11-16 to
//   Latin 2.9 % and six CJK replies), and rule 11 is the owner's newest decree
//   born from the bot re-telling posts instead of adding its own point;
// - every 2nd arrival — 5, 6 (visible style: length, one line, lowercase, no
//   list; and the quoting mechanism) and 1–4 (task work). Style compliance is
//   already high (81.4 % ≤ 40 chars, 100 % one line, 94.1 % lowercase start,
//   3.7 % final period, quote punctuation 1.09 %), so this tier needs pressure
//   but not every turn; task rules are rare-trigger/high-impact and ride on the
//   first arrival plus every 2nd;
// - every 3rd arrival — 7, 8, 9 (no help-desk offer, do not audit other
//   people's numbers, the snark tone): stable in the corpus, lowest drift.
// The first arrival of a run and every reset carry ALL rules: a fresh session
// has no context at all.
//
// Knowledge cadence (measured, the old behaviour was ~68 KB every turn):
// light documents (`≤ LIGHT_MAX_BYTES`) ride every arrival; heavy ones rotate —
// exactly ONE per arrival after the first, cycling through the list. The cwd
// notes file (`agents.md`) is ALWAYS rotation material regardless of size
// (owner, 2026-11-17: it must not ride every turn), so a squeezed notes file
// cannot regain always-on delivery. No arrival after the first carries two
// heavy documents, and every rotation document lands at least once per
// `rotation.length` arrivals.
//
// Scope: only the chat-participant session. The summarizing host boots with the
// same agent dir and therefore auto-loads this file too; there the knowledge
// block would feed `chat-summary.md` back into the summary of itself. The
// summary session name is `CHATBOT_SUMMARY_SESSION` (default `chat-summary`).
//
// Knowledge dir: one absolute authority, the same `CHATBOT_KNOWLEDGE_DIR` the
// knowledge cron writes to (host daemons see only `OMO_`/`SENPI_`/`PI_` names, so
// the `OMO_CHATBOT_` form is checked too). Fallbacks: `<cwd>/knowledge`, then
// `<CHATBOT_CWD>/knowledge`. The live session keeps the cwd it was OPENED with
// (`ensure_session` reuses the recorded session), so cwd alone is not enough.
//
// This extension only builds text: no network, no database, no writes except its
// own diagnostic log.

import {
  appendFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

/** A knowledge document at or below this size ships every arrival; a heavier
 * one enters the rotation. Byte count is measured on the trimmed body, so the
 * threshold means what the prompt pays. Sized against the live catalog of
 * 2026-11-17: discourse ≈32 KB, persons ≈15 KB, dictionary ≈10.5 KB are heavy;
 * the event and donor lists (<1 KB) are light. Recompute if the catalog's shape
 * changes — the property "at most one rotation document per arrival" does not.
 * The cwd notes file ignores this threshold on purpose: it is rotation material
 * even when it is small. */
export const LIGHT_MAX_BYTES = 3072;

export const SUMMARY_SESSION_DEFAULT = "chat-summary";

/**
 * Service files that must never be dragged into the block: the persona and the
 * chat summary are already part of the prompt, the knowledge cron's prompt is
 * machinery, `AGENTS.md` is loaded by the runtime itself, and the lowercase
 * `agents.md` is added from the cwd separately with its own path.
 */
const EXCLUDED_FILES = new Set([
  "system-prompt.md",
  "chat-summary.md",
  "knowledge-prompt.md",
  "AGENTS.md",
  "agents.md",
]);

type EnvLike = Record<string, string | undefined>;

/** Minimal surface this extension uses; the host supplies the real object. */
interface ExtensionApi {
  on(event: string, handler: (event: any) => unknown): void;
  getSessionName?: () => string | undefined;
}

export interface KnowledgeEntry {
  /** Absolute path of the file the model may edit. */
  path: string;
  body: string;
}

export interface PlanState {
  /** Deliveries seen in this session; 0 means "session just started". */
  arrivals: number;
  sessionId: string;
}

export interface PlanContext {
  knowledgeDir?: string | null;
  cwd?: string;
  sessionName?: string;
  summarySession?: string;
  env?: EnvLike;
  /** Test seam: read one file. */
  readFile?: (path: string) => string;
}

export interface PlanResult {
  /** Undefined when this session must not be touched at all (summary host). */
  systemPrompt?: string;
  /** True when this turn carries at least one rule. */
  rules: boolean;
  /** Ids of the rules this turn carried, ascending. */
  rulesIds: number[];
  /** Bytes of knowledge bodies this turn pays for. */
  knowledgeBytes: number;
  /** The single rotation document this turn carried, if any. */
  heavy: string | null;
  nonce: string | null;
  /** Paths actually delivered this turn. */
  paths: string[];
  missingDir: string | null;
}

export function newState(sessionId = ""): PlanState {
  return { arrivals: 0, sessionId };
}

export function resetState(state: PlanState): void {
  state.arrivals = 0;
}

function envValue(env: EnvLike, name: string): string {
  for (const key of [`OMO_${name}`, name]) {
    const value = env[key];
    if (value !== undefined && value !== "") return value;
  }
  return "";
}

export function summarySessionName(env: EnvLike = process.env): string {
  return envValue(env, "CHATBOT_SUMMARY_SESSION") || SUMMARY_SESSION_DEFAULT;
}

/** True only for the summarizing host session; an unnamed session is the chat. */
export function isSummarySession(sessionName: string | undefined, summarySession: string): boolean {
  return (sessionName ?? "") === summarySession;
}

/** Absolute knowledge directory, or null when neither the env nor a cwd is known. */
export function resolveKnowledgeDir(opts: {
  env?: EnvLike;
  cwd?: string;
}): string | null {
  const env = opts.env ?? process.env;
  const fromEnv = envValue(env, "CHATBOT_KNOWLEDGE_DIR");
  if (fromEnv) return fromEnv;
  if (opts.cwd) return join(opts.cwd, "knowledge");
  const fallbackCwd = envValue(env, "CHATBOT_CWD");
  return fallbackCwd ? join(fallbackCwd, "knowledge") : null;
}

/** Every `*.md` of the knowledge dir, sorted, service files excluded. */
export function listKnowledge(dir: string): string[] {
  let names: string[];
  try {
    names = readdirSync(dir);
  } catch {
    return [];
  }
  return names
    .filter((name) => name.endsWith(".md") && !EXCLUDED_FILES.has(name))
    .sort()
    .map((name) => join(dir, name));
}

function defaultReadFile(path: string): string {
  return readFileSync(path, "utf8");
}

/**
 * Knowledge entries plus the cwd's lowercase `agents.md`.
 *
 * The runtime loads only `AGENTS.md`, so the rule "write important things into
 * agents.md by cwd" would otherwise name a file nothing ever reads.
 */
export function gatherKnowledge(
  dir: string | null,
  opts: {
    cwd?: string;
    readFile?: (path: string) => string;
  } = {},
): { entries: KnowledgeEntry[]; missingDir: string | null } {
  const read = opts.readFile ?? defaultReadFile;
  const entries: KnowledgeEntry[] = [];
  let missingDir: string | null = null;

  if (dir) {
    if (!existsSync(dir)) {
      missingDir = dir;
    } else {
      for (const path of listKnowledge(dir)) {
        try {
          entries.push({ path, body: read(path).trim() });
        } catch {
          // One unreadable file must not cost the whole turn; the next turn
          // rebuilds the block from scratch.
        }
      }
      if (entries.length === 0) missingDir = dir;
    }
  }

  const agentsMd = opts.cwd ? join(opts.cwd, "agents.md") : "";
  if (agentsMd && agentsMd !== dir && existsSync(agentsMd)) {
    try {
      const body = read(agentsMd).trim();
      // An empty agents.md still earns its path: it is the file the model is
      // told to write to, and its location is the useful part.
      entries.push({ path: agentsMd, body });
    } catch {
      // Same failure semantics as a knowledge file.
    }
  }

  return { entries, missingDir };
}

const NONCE_RE = /\bnonce-[A-Za-z0-9]{6,}\b/;

/** First `nonce-…` marker found among the DELIVERED entries, for proof in the
 * log. Computed after rotation on purpose: reporting a marker of a document
 * this turn did not carry would be a delivery lie. */
export function findNonce(entries: KnowledgeEntry[]): string | null {
  for (const entry of entries) {
    const match = NONCE_RE.exec(entry.body);
    if (match) return match[0];
  }
  return null;
}

/** The knowledge part of the prompt. An empty file contributes only its path. */
export function buildKnowledgeBlock(entries: KnowledgeEntry[], nonce: string | null = null): string {
  if (entries.length === 0) return "";
  const header = [
    "[файлы знаний бота] Ниже лежат твои рабочие документы.",
    "Перед содержимым каждого — абсолютный путь: правка идёт в этот файл, а не в копию.",
  ];
  if (nonce) header.push(`маркер поставки: ${nonce}`);
  const lines = [header.join(" "), ""];
  for (const entry of entries) {
    lines.push(`## ${entry.path}`);
    if (entry.body) lines.push(entry.body);
  }
  return lines.join("\n");
}

/** One governing rule and its priority tier.
 *
 * `every: 1` rides every arrival, `2` every second, `3` every third; the first
 * arrival of a run always carries all of them. Priority is behaviour-derived —
 * see the file header for the corpus numbers and the owner decree behind each
 * tier. Numbering follows the owner's live rules so rules 10 and 11 keep the
 * numbers the owner himself uses. */
interface Rule {
  id: number;
  every: 1 | 2 | 3;
  text: string;
}

const RULES: Rule[] = [
  {
    id: 1,
    every: 2,
    text: "Не понял — иди в интернет или уточняй; после уточнения запиши вывод в файл знаний.",
  },
  {
    id: 2,
    every: 2,
    text: "Твои ответы вне Telegram никто не видит: дали задачу — обязательно отпишись о результате.",
  },
  { id: 3, every: 2, text: "Задачу доделывай до конца." },
  { id: 4, every: 2, text: "Важное — в agents.md по cwd, файл дискурса, словарь и так далее." },
  {
    id: 5,
    every: 1,
    text:
      "База — одна короткая фраза 20–40 знаков (у людей медиана 22); длиннее — только по прямой " +
      "просьбе, не по привычке и не по своей инициативе. С маленькой буквы, без точки в конце, без " +
      "длинных тире, скобок, эмодзи и канцелярита. Без вступлений, разборов, заголовков и списков. " +
      "Свои прошлые реплики не повторяй теми же словами.",
  },
  {
    id: 6,
    every: 2,
    text: "Своими словами. Чужое сообщение НЕ цитируешь: ни реплаем, ни в кавычках, ни пересказом «ты сказал, что…».",
  },
  {
    id: 7,
    every: 3,
    text: "Ты не справочная: помощь не предлагаешь и не учишь, как с тобой говорить («скажи прямо», «чем помочь»). Не поддакиваешь.",
  },
  {
    id: 8,
    every: 3,
    text: "Чужие цифры не пересчитываешь, пруфов не требуешь, чужие мнения не проверяешь — они бывают относительными.",
  },
  { id: 9, every: 3, text: "Колкая: подъёбываешь коротко и по делу." },
  {
    id: 10,
    every: 1,
    text: "Только русский. Никакой англичанки, китайщины и прочего мусора в ответах, даже в шутку или ради точности. Не можешь сказать по-русски — промолчи.",
  },
  {
    id: 11,
    every: 3,
    text: "Посты не цитировать и не пересказывать даже своими словами. Если есть что сказать — коротко, своим мнением; факт или ссылку добавляй только когда это уточняет, без лонгрида. Вода и пересказ хуже молчания.",
  },
];

/** Ids of the rules that ride a given arrival. Arrival 1 (and after a reset)
 * carries every rule; later turns follow each rule's own tier. */
export function rulesIds(arrival: number = 1): number[] {
  const first = arrival <= 1;
  return RULES.filter((rule) => first || arrival % rule.every === 0).map((rule) => rule.id);
}

/** The rules part of the prompt for a given arrival. */
export function rulesBlock(arrival: number = 1): string {
  const ids = new Set(rulesIds(arrival));
  const lines = ["[правила поведения]"];
  for (const rule of RULES) {
    if (ids.has(rule.id)) lines.push(`${rule.id}. ${rule.text}`);
  }
  return lines.join("\n");
}

function bodyBytes(entry: KnowledgeEntry): number {
  return Buffer.byteLength(entry.body, "utf8");
}

/**
 * Compose one turn. Pure except for reading the knowledge files.
 *
 * Cadence (see the file header): per-rule priority tiers, light documents every
 * arrival, exactly one rotation document (heavy or the cwd notes file) after
 * the first. The first arrival carries everything.
 */
export function plan(basePrompt: string, state: PlanState, ctx: PlanContext = {}): PlanResult {
  const summarySession = ctx.summarySession ?? summarySessionName(ctx.env);
  if (isSummarySession(ctx.sessionName, summarySession)) {
    return {
      rules: false,
      rulesIds: [],
      knowledgeBytes: 0,
      heavy: null,
      nonce: null,
      paths: [],
      missingDir: null,
    };
  }

  state.arrivals += 1;
  const arrival = state.arrivals;

  const { entries, missingDir } = gatherKnowledge(ctx.knowledgeDir ?? null, {
    cwd: ctx.cwd,
    readFile: ctx.readFile,
  });

  // The cwd notes file is the owner's live instruction channel, and the owner
  // ruled it must NOT ride every turn (2026-11-17): it joins the rotation
  // regardless of size. Degenerate catalogue: a single-document rotation list
  // delivers that document every arrival — the live catalogue holds three heavy
  // documents plus these notes.
  const notesPath = ctx.cwd ? join(ctx.cwd, "agents.md") : null;
  const isNotes = (entry: KnowledgeEntry) => entry.path === notesPath;
  const rotation = entries.filter(
    (entry) => isNotes(entry) || bodyBytes(entry) > LIGHT_MAX_BYTES,
  );
  // The first arrival of a run has no context at all: ship everything.
  const fullSet = arrival === 1;
  const rotated = !fullSet && rotation.length > 0 ? rotation[arrival % rotation.length] : null;
  const delivered =
    fullSet || rotated === null
      ? entries
      : entries.filter(
          (entry) =>
            (!isNotes(entry) && bodyBytes(entry) <= LIGHT_MAX_BYTES) || entry === rotated,
        );
  const heavy = rotated === null ? null : rotated.path;

  const nonce = findNonce(delivered);
  const knowledgeBytes = delivered.reduce((sum, entry) => sum + bodyBytes(entry), 0);
  const ids = rulesIds(arrival);
  const parts: string[] = [];
  const knowledge = buildKnowledgeBlock(delivered, nonce);
  if (knowledge) parts.push(knowledge);
  parts.push(rulesBlock(arrival));

  const base = {
    rules: ids.length > 0,
    rulesIds: ids,
    knowledgeBytes,
    heavy,
    nonce,
    paths: delivered.map((entry) => entry.path),
    missingDir,
  };
  if (parts.length === 0) return base;
  return { ...base, systemPrompt: `${basePrompt}\n\n${parts.join("\n\n")}` };
}

const LOG_MAX_BYTES = 1024 * 1024;

/** Agent dir for the diagnostic log, mirroring muse-spark-phase-strip. */
function agentDir(): string {
  const fromEnv =
    process.env.OMO_CODING_AGENT_DIR ||
    process.env.SENPI_CODING_AGENT_DIR ||
    process.env.PI_CODING_AGENT_DIR;
  return fromEnv && fromEnv.length > 0 ? fromEnv : join(homedir(), ".omo", "agent");
}

function logEvent(record: Record<string, unknown>): void {
  try {
    const dir = join(agentDir(), "logs");
    mkdirSync(dir, { recursive: true });
    const path = join(dir, "md-inject.log");
    try {
      if (statSync(path).size > LOG_MAX_BYTES) writeFileSync(path, "", { mode: 0o600 });
    } catch {
      // Missing file: the append creates it.
    }
    appendFileSync(path, `${JSON.stringify({ ts: new Date().toISOString(), ...record })}\n`, {
      mode: 0o600,
    });
  } catch {
    // Observability must never break a turn.
  }
}

export default function mdInject(pi: ExtensionApi): void {
  const state = newState();
  let rulesSent = false;
  let ruleIds: number[] = [];
  let nonce: string | null = null;

  const reset = () => {
    resetState(state);
    rulesSent = false;
    ruleIds = [];
    nonce = null;
  };

  // A new session or a compaction starts a fresh cadence: the first arrival of
  // the new run must carry every rule and the whole knowledge set again.
  pi.on("session_start", reset);
  pi.on("session_compact", reset);

  pi.on("before_agent_start", (event: any) => {
    try {
      const sessionName = pi.getSessionName?.() ?? "";
      const summarySession = summarySessionName(process.env);
      if (isSummarySession(sessionName, summarySession)) return undefined;

      const cwd = event?.systemPromptOptions?.cwd;
      const knowledgeDir = resolveKnowledgeDir({ env: process.env, cwd });
      const result = plan(event?.systemPrompt ?? "", state, {
        knowledgeDir,
        cwd,
        sessionName,
        summarySession,
      });

      rulesSent = result.rules;
      ruleIds = result.rulesIds;
      nonce = result.nonce;
      if (result.missingDir) logEvent({ event: "knowledge_dir_missing", path: result.missingDir });
      logEvent({
        event: "before_agent_start",
        session: sessionName,
        arrival: state.arrivals,
        dir: knowledgeDir,
        paths: result.paths,
        rules: result.rules,
        rules_ids: result.rulesIds,
        knowledge_bytes: result.knowledgeBytes,
        heavy: result.heavy,
        nonce: result.nonce,
      });
      return result.systemPrompt === undefined ? undefined : { systemPrompt: result.systemPrompt };
    } catch (error) {
      logEvent({
        event: "before_agent_start_failed",
        message: error instanceof Error ? error.message : String(error),
      });
      return undefined;
    }
  });

  // Observation only: the prompt is owned by `before_agent_start`. Logged so a
  // live acceptance can tell a delivered block from a stale file on disk.
  pi.on("before_provider_request", (event: any) => {
    try {
      const sessionName = pi.getSessionName?.() ?? "";
      if (isSummarySession(sessionName, summarySessionName(process.env))) return undefined;
      const input = event?.payload?.input;
      let nonceInPayload: boolean | null = null;
      if (nonce) {
        try {
          nonceInPayload = JSON.stringify(event?.payload ?? "").includes(nonce);
        } catch {
          nonceInPayload = null;
        }
      }
      logEvent({
        event: "before_provider_request",
        items: Array.isArray(input) ? input.length : null,
        nonce,
        nonce_in_payload: nonceInPayload,
        rules_sent: rulesSent,
        rules_ids: ruleIds,
      });
    } catch {
      // Never interfere with the request.
    }
    return undefined;
  });
}
