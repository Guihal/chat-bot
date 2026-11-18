import type { ExtensionAPI } from "@code-yeongyu/senpi";
import {
	assertAllowedMediaUrl,
	downloadBounded,
	mediaLimit,
	sniffMime,
} from "../lib/common.js";
import { fetchPreview, readSources, thumbnailsFor } from "../lib/preview.js";
import { saveCandidate } from "../lib/storage.js";
import type { MediaKind } from "../lib/types.js";

function text(value: unknown): { content: Array<{ type: "text"; text: string }> } {
	return {
		content: [
			{
				type: "text",
				text: typeof value === "string" ? value : JSON.stringify(value, null, 2),
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

function metrics(value: unknown): Record<string, string> {
	if (!value || typeof value !== "object") return {};
	const out: Record<string, string> = {};
	for (const key of ["views", "reactions", "datetime"]) {
		const raw = (value as Record<string, unknown>)[key];
		if (raw !== undefined && raw !== null) out[key] = String(raw);
	}
	return out;
}

export default function memeDiscovery(pi: ExtensionAPI): void {
	pi.registerTool({
		name: "meme_sources_read",
		label: "Read meme sources",
		description:
			"Read the agent-editable meme source allowlist from its md file. " +
			"Returns the parsed usernames and weights. Missing or empty file is an error.",
		parameters: { type: "object", properties: {}, additionalProperties: false },
		execute: async () => {
			try {
				const sources = readSources();
				return text({ path: sources.path, entries: sources.entries });
			} catch (error) {
				return fail((error as Error).message);
			}
		},
	});

	pi.registerTool({
		name: "meme_preview_fetch",
		label: "Fetch Telegram preview",
		description:
			"Fetch recent public posts of one allowed source via its Telegram preview page. " +
			"Returns compact post metadata plus up to three bounded thumbnails/frames as images.",
		parameters: {
			type: "object",
			properties: {
				username: {
					type: "string",
					description: "Source username exactly as listed by meme_sources_read.",
				},
			},
			required: ["username"],
			additionalProperties: false,
		},
		execute: async (_toolCallId, params) => {
			try {
				const username = String((params as Record<string, unknown>).username || "")
					.replace(/^@/, "")
					.toLowerCase();
				const allowed = readSources().entries.some((entry) => entry.username === username);
				if (!allowed) throw new Error(`source is not in the allowlist: ${username}`);
				const posts = await fetchPreview(username);
				const images = await thumbnailsFor(posts);
				return {
					content: [
						{ type: "text" as const, text: JSON.stringify({ source: username, posts }, null, 2) },
						...images,
					],
				};
			} catch (error) {
				return fail((error as Error).message);
			}
		},
	});

	pi.registerTool({
		name: "meme_candidate_save",
		label: "Save meme candidate",
		description:
			"Save one selected meme candidate with its media and a required visual note. " +
			"Revalidates the source allowlist and media host/size/MIME; duplicate ids or hashes are skipped.",
		parameters: {
			type: "object",
			properties: {
				source: { type: "string", description: "Allowed source username." },
				post_id: { type: "string", description: "Numeric Telegram post id." },
				text: { type: "string", description: "Original post text or caption." },
				media_url: { type: "string", description: "Allowed Telegram CDN media URL." },
				media_kind: {
					type: "string",
					enum: ["photo", "video", "animation"],
					description: "Media kind.",
				},
				metrics: {
					type: "object",
					description: "Optional views/reactions/datetime metadata.",
					additionalProperties: true,
				},
				why_funny: {
					type: "string",
					description:
						"What is visible only in the picture/frame that makes this funny (1..500 chars).",
				},
			},
			required: ["source", "post_id", "text", "media_url", "media_kind", "why_funny"],
			additionalProperties: false,
		},
		execute: async (_toolCallId, params) => {
			try {
				const p = params as Record<string, unknown>;
				const source = String(p.source || "").replace(/^@/, "").toLowerCase();
				const allowed = readSources().entries.find((entry) => entry.username === source);
				if (!allowed) throw new Error(`source is not in the allowlist: ${source}`);
				const postId = String(p.post_id || "").trim();
				if (!/^[0-9]{1,20}$/.test(postId)) throw new Error("post_id must be numeric");
				const why = String(p.why_funny || "").trim();
				if (!why || why.length > 500) throw new Error("why_funny must be 1..500 characters");
				const kind = String(p.media_kind || "") as MediaKind;
				if (!["photo", "video", "animation"].includes(kind)) {
					throw new Error("media_kind must be photo, video or animation");
				}
				const mediaUrl = String(p.media_url || "");
				assertAllowedMediaUrl(mediaUrl);
				const buffer = await downloadBounded(mediaUrl, mediaLimit(kind));
				const mime = sniffMime(buffer);
				if (!mime) throw new Error("media bytes are not a supported image/video");
				const result = saveCandidate({
					source,
					post_id: postId,
					source_url: `https://t.me/${source}/${postId}`,
					text: String(p.text || ""),
					why_funny: why,
					metrics: metrics(p.metrics),
					media: { buffer, mime, kind },
				});
				return text(result);
			} catch (error) {
				return fail((error as Error).message);
			}
		},
	});
}
