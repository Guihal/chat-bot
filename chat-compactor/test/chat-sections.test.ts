// Structure of the chat summary: the registry is the single author, so these
// tests pin the properties every other module depends on — order, kinds,
// required sections, ceilings — and the prompt/registry drift.
import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  CHAT_SECTIONS,
  CHAT_SECTION_KEYS,
  MAX_ITEM_CHARS,
  MAX_SECTION_ITEMS,
  chatSection,
  chatSectionOrder,
  isChatSectionKey,
  normalizeItem,
} from "../lib/rich-compaction/contracts/chat-sections.ts";

const RICH_MD = readFileSync(
  join(import.meta.dir, "../../bot-home/compactors/rich.md"),
  "utf8",
);

test("registry order is the rendering order the spec fixes", () => {
  expect(CHAT_SECTIONS.map((s) => s.key)).toEqual([
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
  ]);
  expect(CHAT_SECTIONS.map((s) => s.heading)).toEqual([
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
});

test("exactly Narrative and Last Conversation are required, and are prose/items", () => {
  expect(CHAT_SECTIONS.filter((s) => s.required).map((s) => s.key)).toEqual([
    "narrative",
    "lastConversation",
  ]);
  expect(chatSection("narrative")?.kind).toBe("prose");
  expect(chatSection("lastConversation")?.kind).toBe("items");
  expect(CHAT_SECTIONS.filter((s) => s.kind === "prose").length).toBe(1);
});

test("keys and headings are unique and lookups agree with the order", () => {
  expect(new Set(CHAT_SECTION_KEYS).size).toBe(CHAT_SECTIONS.length);
  expect(new Set(CHAT_SECTIONS.map((s) => s.heading)).size).toBe(
    CHAT_SECTIONS.length,
  );
  CHAT_SECTIONS.forEach((spec, index) => {
    expect(chatSection(spec.key)).toBe(spec);
    expect(chatSectionOrder(spec.key)).toBe(index);
    expect(isChatSectionKey(spec.key)).toBe(true);
  });
  expect(chatSection("files")).toBeUndefined();
  expect(chatSectionOrder("files")).toBe(-1);
  expect(isChatSectionKey("skills")).toBe(false);
});

test("ceilings are positive and an item is one line", () => {
  expect(MAX_SECTION_ITEMS).toBeGreaterThan(0);
  expect(MAX_ITEM_CHARS).toBeGreaterThan(0);
  expect(normalizeItem("  two\n\nlines\there ")).toBe("two lines here");
  expect(normalizeItem("\n \t ")).toBe("");
});

test("rich.md documents every registry key and heading — no prompt drift", () => {
  for (const spec of CHAT_SECTIONS) {
    expect(RICH_MD).toContain(`\`${spec.key}\``);
    expect(RICH_MD).toContain(spec.heading);
  }
});
