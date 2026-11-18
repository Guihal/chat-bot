// rich.md is both the model's system prompt and the loader's fail-closed
// contract: it must start with '#', be byte-identical to its own UTF-8 trim, and
// carry the instruction that matters most for this chat — the source repeats
// itself, so repeats are folded instead of copied.
import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";

const PROMPT_BYTES = readFileSync(
  join(import.meta.dir, "../../bot-home/compactors/rich.md"),
);
const PROMPT = PROMPT_BYTES.toString("utf8");

test("the prompt satisfies the loader's boundary contract byte-for-byte", () => {
  expect(PROMPT_BYTES.length).toBeGreaterThan(0);
  expect(PROMPT_BYTES.equals(Buffer.from(PROMPT.trim(), "utf8"))).toBe(true);
  expect(PROMPT[0]).toBe("#");
  expect(/\s$/.test(PROMPT)).toBe(false);
});

test("the prompt tells the model to fold repeated windows into one account", () => {
  const lower = PROMPT.toLowerCase();
  expect(lower).toContain("re-sends the last 50 chat messages");
  expect(lower).toContain("fold those repeats into one chronological account");
  expect(lower).toContain("each fact appears once");
  expect(lower).toContain("never produce a copy per window");
});

test("the prompt pins the two required sections and the events date prefix", () => {
  expect(PROMPT).toContain("`narrative` (prose, REQUIRED, never empty)");
  expect(PROMPT).toContain("`lastConversation` (items, REQUIRED, never empty)");
  expect(PROMPT).toContain("YYYY-MM-DD — <event>");
  expect(PROMPT).toContain("`operational` (items, LAST)");
});

test("the prompt carries the envelope, the ceilings and the verifier contract", () => {
  expect(PROMPT).toContain('"kind":"root"');
  expect(PROMPT).toContain('"coverageDigest"');
  expect(PROMPT).toContain("at most 400 characters");
  expect(PROMPT).toContain("at most 12 per section");
  expect(PROMPT).toContain("<<<RICH:VERIFY");
  expect(PROMPT).toContain('{"ok":true}');
  expect(PROMPT).toContain("Never invent a participant, event, date or fact");
});
