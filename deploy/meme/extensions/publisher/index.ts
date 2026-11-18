import fs from "node:fs";
import path from "node:path";
import type { ExtensionAPI } from "@code-yeongyu/senpi";
import {
	CANDIDATES_DIR,
	TARGET_CHAT_ID,
 alertOnce,
 env,
 isCandidateId,
 mediaLimit,
 sha256File,
 sniffMime,
} from "../lib/common.js";
import { sendMedia } from "../lib/botapi.js";
import {
 claimCandidate,
 listCandidates,
 markFailed,
 markPublished,
 readCandidate,
} from "../lib/storage.js";

function text(value: unknown): { content: Array<{ type: "text"; text: string }> } {
	return {
	  content: [
	    {
	      type: "text",
	      text:
	        typeof value === "string"
	          ? value
 	          : JSON.stringify(value, null, 2),
	    },
	  ],
  };
}

function fail(message: string): {
  content: Array<{ type: "text"; text: string }>;
  isError: true;
} {
  return { content: [{ type: "text", text: `error: ${message}` }], isError: true };
}

export default function memePublisher(pi: ExtensionAPI): void {
  pi.registerTool({
    name: "meme_candidates_list",
    label: "List meme candidates",
    description:
      "List unpublished meme candidates with their metadata and visual notes. Use it to choose one.",
    parameters: {
      type: "object",
      properties: {
        limit: {
          type: "integer",
          description: "How many candidates to return (default 20, max 50).",
        },
      },
      additionalProperties: false,
    },
    execute: async (_toolCallId, params) => {
      try {
        const limit = Number((params as Record<string, unknown>).limit || 20);
        return text({ candidates: listCandidates(limit) });
      } catch (error) {
        return fail((error as Error).message);
      }
    },
  });

  pi.registerTool({
    name: "meme_publish",
    label: "Publish one meme",
    description:
      `Publish exactly one candidate to the fixed target channel ${TARGET_CHAT_ID}. ` +
      "Accepts only a candidate id; claims it before the single send and never retries blindly.",
    parameters: {
      type: "object",
      properties: {
        candidate_id: {
          type: "string",
          description:
            "Candidate id returned by meme_candidates_list (source__post_id).",
        },
      },
      required: ["candidate_id"],
      additionalProperties: false,
    },
    execute: async (_toolCallId, params) => {
      const id = String((params as Record<string, unknown>).candidate_id || "");
      if (env("MEME_ENABLED", "true") === "false") return fail("memes are disabled by MEME_ENABLED=false");
      if (!isCandidateId(id)) return fail("candidate_id has an invalid shape");
      let claimed = null;
      try {
        const row = readCandidate(id);
        const base = path.resolve(CANDIDATES_DIR);
        const mediaPath = path.resolve(row.media.path);
        if (!mediaPath.startsWith(`${base}${path.sep}`)) throw new Error("candidate media escapes storage");
        const stat = fs.lstatSync(mediaPath);
        if (!stat.isFile() || stat.isSymbolicLink()) throw new Error("candidate media is not a regular file");
        if (stat.size !== row.media.bytes) throw new Error("candidate media size changed");
        if (stat.size > mediaLimit(row.media.kind)) throw new Error("candidate media exceeds its size cap");
        if (sha256File(mediaPath) !== row.media.sha256) throw new Error("candidate media hash changed");
        if (sniffMime(fs.readFileSync(mediaPath)) !== row.media.mime) throw new Error("candidate media MIME changed");
        claimed = claimCandidate(id);
        const sent = await sendMedia(claimed);
        const published = markPublished(id, sent.message_id);
        return text({
          status: published.status,
          candidate_id: id,
          target_chat_id: TARGET_CHAT_ID,
          method: sent.method,
          telegram_message_id: sent.message_id,
        });
      } catch (error) {
        const message = (error as Error).message;
        if (claimed) {
          try {
            markFailed(id, message);
          } catch {}
          alertOnce("publish-failed", `meme publish failed for ${id}: ${message}`);
        }
        return fail(message);
      }
    },
  });
}
