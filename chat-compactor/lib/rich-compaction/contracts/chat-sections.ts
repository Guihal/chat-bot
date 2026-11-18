/**
 * The chat-shaped summary structure. This registry is the ONE authority for it:
 * key, heading, kind and whether the section must be present. Rendering,
 * validation and the verifier contract all read it, so a new section is added
 * here and nowhere else.
 *
 * Order is the rendering order. `prose` sections are one paragraph of text;
 * `items` sections are a list of concrete one-line facts. Empty item sections
 * are omitted from the persisted summary, while `narrative` and
 * `lastConversation` are always required — a summary that cannot say what the
 * chat was about, or what the latest conversation was, has lost the thread.
 */
export const CHAT_SECTION_KEYS = [
  "narrative",
  "people",
  "chatRules",
  "toneMemes",
  "topics",
  "events",
  "promises",
  "lastConversation",
  "openThreads",
  "operational",
] as const;

export type ChatSectionKey = (typeof CHAT_SECTION_KEYS)[number];
export type ChatSectionKind = "prose" | "items";

export interface ChatSectionSpec {
  readonly key: ChatSectionKey;
  readonly heading: string;
  readonly kind: ChatSectionKind;
  readonly required: boolean;
}

export const CHAT_SECTIONS: readonly ChatSectionSpec[] = [
  { key: "narrative", heading: "Narrative", kind: "prose", required: true },
  { key: "people", heading: "People", kind: "items", required: false },
  { key: "chatRules", heading: "Chat Rules", kind: "items", required: false },
  { key: "toneMemes", heading: "Tone & Memes", kind: "items", required: false },
  { key: "topics", heading: "Topics", kind: "items", required: false },
  { key: "events", heading: "Events", kind: "items", required: false },
  { key: "promises", heading: "Promises", kind: "items", required: false },
  { key: "lastConversation", heading: "Last Conversation", kind: "items", required: true },
  { key: "openThreads", heading: "Open Threads", kind: "items", required: false },
  { key: "operational", heading: "Operational", kind: "items", required: false },
];

/** Ceiling per item section and per item, so one chatty section cannot crowd
 *  the others out of the persisted summary. */
export const MAX_SECTION_ITEMS = 12;
export const MAX_ITEM_CHARS = 400;

/** `events` starts with the day it happened, or with `—` when the source has no
 *  date: `2026-11-04 — Дима поставил бота на VPS`. */
export const EVENT_ITEM_PREFIX = /^(?:\d{4}-\d{2}-\d{2}|—) — /;

const BY_KEY = new Map<string, ChatSectionSpec>(
  CHAT_SECTIONS.map((spec) => [spec.key, spec]),
);

const ORDER = new Map<string, number>(
  CHAT_SECTIONS.map((spec, index) => [spec.key, index]),
);

export function chatSection(key: string): ChatSectionSpec | undefined {
  return BY_KEY.get(key);
}

/** Rendering position of a section, or -1 for a key outside the registry. */
export const chatSectionOrder = (key: string): number =>
  ORDER.get(key) ?? -1;

export function isChatSectionKey(key: string): key is ChatSectionKey {
  return BY_KEY.has(key);
}

/** One physical line per item: the source (and the model) uses newlines freely,
 *  but a rendered bullet must stay one line so the persisted summary parses
 *  back section by section. */
export const normalizeItem = (text: string): string =>
  text.replace(/\s+/g, " ").trim();
