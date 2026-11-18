import { createHash } from "node:crypto";
import { RichFailure } from "./rich-types.js";

/** The host-owned inventories carried in a persisted summary, decoded back as
 *  raw section bodies (the text between the open marker line and its close).
 *  `guides` is optional by contract: an empty agent-state ledger omits the
 *  section entirely (zero-or-exactly-one), so v3 summaries without it — and
 *  every older summary — decode to a prior without guides. */
export interface PriorHost {
  readonly files?: string;
  readonly skills?: string;
  readonly guides?: string;
}

export interface DecodedSummary {
  /** The seven semantic sections with the host sections stripped off. */
  readonly semanticPreviousSummary?: string;
  readonly prior: PriorHost;
}

const sha256 = (text: string): string =>
  createHash("sha256").update(text, "utf8").digest("hex");

/** Host sentinel; the model can never emit it (gateField), so splitting on it is
 *  unambiguous. v1/v2 wrote a `## Skills to Restore` heading — decoded as inert
 *  metadata (stripped), never rebuilt into an inventory (invariant 9). */
const HOST_OPEN =
  /<<<RICH:HOST (files|skills|guides) v3 sha256=([0-9a-f]{64})>>>/g;
const LEGACY_SKILLS = /\n?## Skills to Restore[\s\S]*$/;

type HostKind = "files" | "skills" | "guides";

/** Decode complete outer sections. Literal marker text inside a verified body
 * stays data; only markers outside that body identify additional sections. */
export function decodePreviousSummary(text: string | undefined): DecodedSummary {
  if (!text) return { prior: {} };
  const prior: { files?: string; skills?: string; guides?: string } = {};
  const semantic: string[] = [];
  const opens = new RegExp(HOST_OPEN.source, "g");
  let cursor = 0;
  let match: RegExpExecArray | null;
  while ((match = opens.exec(text))) {
    const kind = match[1] as HostKind;
    const digest = match[2];
    if (prior[kind] !== undefined)
      throw new RichFailure("trust-durable-state", "codec", `${kind}-duplicate`);
    const start = match.index + match[0].length;
    if (text[start] !== "\n")
      throw new RichFailure("trust-durable-state", "codec", `${kind}-open`);
    const bodyStart = start + 1;
    const close = `\n<<<RICH:/HOST ${kind}>>>`;
    const hash = createHash("sha256");
    let hashedUntil = bodyStart;
    let end = text.indexOf(close, bodyStart);
    while (end >= 0) {
      hash.update(text.slice(hashedUntil, end), "utf8");
      hashedUntil = end;
      if (hash.copy().digest("hex") === digest) break;
      end = text.indexOf(close, end + close.length);
    }
    if (end < 0)
      throw new RichFailure("trust-durable-state", "codec", `${kind}-digest-or-close`);
    prior[kind] = text.slice(bodyStart, end);
    semantic.push(text.slice(cursor, match.index));
    cursor = end + close.length;
    opens.lastIndex = cursor;
  }
  semantic.push(text.slice(cursor));
  const body = semantic.join("").replace(LEGACY_SKILLS, "").trimEnd();
  return { semanticPreviousSummary: body || undefined, prior };
}

/** Wrap the host-computed section bodies in their digest-stamped markers. The
 *  digest is over the body exactly as embedded, so decode re-verifies it.
 *  `guidesBody` is undefined when the ledger is empty — the section is then
 *  omitted outright (zero-or-exactly-one), never emitted with an empty body. */
export function encodeHostSections(
  filesBody: string,
  skillsBody: string,
  guidesBody?: string,
): { filesBlock: string; skillsBlock: string; guidesBlock?: string } {
  const wrap = (kind: HostKind, body: string): string =>
    `<<<RICH:HOST ${kind} v3 sha256=${sha256(body)}>>>\n${body}\n<<<RICH:/HOST ${kind}>>>`;
  return {
    filesBlock: wrap("files", filesBody),
    skillsBlock: wrap("skills", skillsBody),
    ...(guidesBody === undefined
      ? {}
      : { guidesBlock: wrap("guides", guidesBody) }),
  };
}
