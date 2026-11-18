import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { join } from "node:path";
import { agentDir } from "../agent-dir.js";

/** Default location of the single semantic contract, relative to this module. */
export const RICH_PROMPT_PATH = join(agentDir(), "compactors/rich.md");

export interface RichPrompt {
  readonly bytes: string;
  readonly sha256: string;
}

/**
 * Loads the canonical rich.md contract fail-closed (invariant 15).
 *
 * The raw file bytes must be non-empty and byte-identical to their own UTF-8
 * trim round-trip — no leading/trailing whitespace, no BOM, no terminal newline
 * — and start with `#`. These exact bytes become both the direct-Agent system
 * prompt and the terminal user-suffix duplicate, so any boundary drift here
 * would silently break the provider-bound digest identity.
 */
export function loadRichPrompt(path: string = RICH_PROMPT_PATH): RichPrompt {
  const raw = readFileSync(path);
  const text = raw.toString("utf8");
  const trimmed = Buffer.from(text.trim(), "utf8");
  if (raw.length === 0) throw new Error(`rich.md is empty: ${path}`);
  if (!raw.equals(trimmed))
    throw new Error(
      `rich.md has boundary whitespace/BOM; must equal its UTF-8 trim: ${path}`,
    );
  if (text[0] !== "#") throw new Error(`rich.md must start with '#': ${path}`);
  if (/\s$/.test(text))
    throw new Error(`rich.md must end on a non-whitespace byte: ${path}`);
  return {
    bytes: text,
    sha256: createHash("sha256").update(raw).digest("hex"),
  };
}
