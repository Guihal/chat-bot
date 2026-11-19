import fs from "node:fs";
import {
	assertAllowedMediaUrl,
	downloadBounded,
	env,
	sniffMime,
	SOURCES_PATH,
} from "./common.js";
import type { PreviewMedia, PreviewPost, SourceEntry } from "./types.js";

function isLoopbackHost(hostname: string): boolean {
	return hostname === "127.0.0.1" || hostname === "::1" || hostname === "[::1]" || hostname === "localhost";
}

export function parseSources(markdown: string): SourceEntry[] {
	const seen = new Set<string>();
	const out: SourceEntry[] = [];
	for (const rawLine of markdown.split(/\r?\n/)) {
		let line = rawLine.replace(/\s+#.*$/, "").trim();
		if (!line || line.startsWith("#")) continue;
		line = line.replace(/^[-*]\s+/, "").trim();
		if (!line) continue;
		const parts = line.split(/\s+/);
		let token = parts.shift() || "";
		if (/^https?:\/\/t\.me\//i.test(token)) {
			try {
				token = new URL(token).pathname.replace(/^\/+|\/+$/g, "").split("/")[0];
			} catch {
				continue;
			}
		}
		token = token.replace(/^@/, "").toLowerCase();
		if (!/^[a-z0-9_]{3,64}$/.test(token)) continue;
		if (seen.has(token)) continue;
		let weight: SourceEntry["weight"] = "normal";
		for (const part of parts) {
			const match = /^weight=(high|normal|low)$/i.exec(part);
			if (match) weight = match[1].toLowerCase() as SourceEntry["weight"];
		}
		seen.add(token);
		out.push({ username: token, weight });
	}
	return out;
}

export function readSourcesFrom(file: string): { path: string; entries: SourceEntry[] } {
	let markdown = "";
	try {
		markdown = fs.readFileSync(file, "utf8");
	} catch {
		throw new Error(`sources file is missing or unreadable: ${file}`);
	}
	const entries = parseSources(markdown);
	if (entries.length === 0) throw new Error(`sources file has no valid entries: ${file}`);
	return { path: file, entries };
}

export function readSources(): { path: string; entries: SourceEntry[] } {
	return readSourcesFrom(SOURCES_PATH);
}

export function previewBase(): string {
	const base = env("MEME_PREVIEW_BASE", "https://t.me");
	let parsed: URL;
	try {
		parsed = new URL(base);
	} catch {
		throw new Error("MEME_PREVIEW_BASE is not a valid URL");
	}
	if (parsed.protocol !== "https:" && !(parsed.protocol === "http:" && isLoopbackHost(parsed.hostname))) {
		throw new Error("MEME_PREVIEW_BASE must be https or loopback http");
	}
	return parsed.toString().replace(/\/+$/, "");
}

function decodeEntities(value: string): string {
	return value
		.replace(/<br\s*\/?>/gi, "\n")
		.replace(/<[^>]+>/g, "")
		.replace(/&#x([0-9a-f]+);/gi, (_m, hex) => String.fromCodePoint(Number.parseInt(hex, 16)))
		.replace(/&#(\d+);/g, (_m, dec) => String.fromCodePoint(Number.parseInt(dec, 10)))
		.replace(/&nbsp;/gi, " ")
		.replace(/&amp;/gi, "&")
		.replace(/&lt;/gi, "<")
		.replace(/&gt;/gi, ">")
		.replace(/&quot;/gi, '"')
		.replace(/&#39;/g, "'")
		.trim();
}

function resolveMedia(raw: string | undefined, base: string): string | undefined {
	if (!raw) return undefined;
	try {
		const absolute = new URL(raw, `${base}/`).toString();
		assertAllowedMediaUrl(absolute);
		return absolute;
	} catch {
		return undefined;
	}
}

export function parsePreview(html: string, username: string, base = previewBase()): PreviewPost[] {
	const posts: PreviewPost[] = [];
	const blocks = html.split('<div class="tgme_widget_message ').slice(1);
	for (const block of blocks) {
		const dataPost = /data-post="([^"]+)"/.exec(block)?.[1];
		if (!dataPost) continue;
		const postId = dataPost.split("/").pop() || "";
		if (!/^\d{1,20}$/.test(postId)) continue;
		const textMatch = /class="[^"]*js-message_text[^"]*"[^>]*>([\s\S]*?)<\/div>/.exec(block);
		const text = textMatch ? decodeEntities(textMatch[1]) : "";
		const datetime = /<time[^>]+datetime="([^"]+)"/.exec(block)?.[1];
		const views = /class="tgme_widget_message_views"[^>]*>([^<]+)</.exec(block)?.[1]?.trim();
		const reactionsMatch =
			/class="tgme_widget_message_reactions"[^>]*>([\s\S]*?)<\/span>/.exec(block)?.[1] ||
			/class="tgme_widget_message_reactions"[^>]*>([\s\S]*?)<\/div>/.exec(block)?.[1];
		const reactions = reactionsMatch ? decodeEntities(reactionsMatch) : undefined;

		let media: PreviewMedia | undefined;
		const videoSrc = /<video[^>]*\ssrc="([^"]+)"/.exec(block)?.[1];
		const poster = /<video[^>]*\sposter="([^"]+)"/.exec(block)?.[1];
		const thumbStyle =
			/<i[^>]*class="[^"]*tgme_widget_message_video_thumb[^"]*"[^>]*style="([^"]*)"/i.exec(block)?.[1] ||
			/<i[^>]*style="([^"]*)"[^>]*class="[^"]*tgme_widget_message_video_thumb[^"]*"/i.exec(block)?.[1] ||
			"";
		const thumbUrl = /background-image:\s*url\((['"]?)([^'")]+)\1\)/i.exec(thumbStyle)?.[2];
		if (videoSrc) {
			const url = resolveMedia(videoSrc, base);
			if (url) {
				const kind = /\.gif($|\?)/i.test(url) ? "animation" : "video";
				const thumbnail = resolveMedia(poster || thumbUrl, base);
				media = { kind, url, ...(thumbnail ? { thumbnail } : {}) };
			}
		} else {
			const bg =
				/background-image:\s*url\((['"]?)([^'")]+)\1\)/i.exec(block)?.[2] ||
				/style="[^"]*background-image:\s*url\((['"]?)([^'")]+)\1\)/i.exec(block)?.[2];
			const url = resolveMedia(bg, base);
			if (url) media = { kind: "photo", url };
		}

		if (!text && !media) continue;
		const post: PreviewPost = {
			post_id: postId,
			url: `https://t.me/${username}/${postId}`,
			text,
			...(datetime ? { datetime } : {}),
			...(views ? { views } : {}),
			...(reactions ? { reactions } : {}),
			...(media ? { media } : {}),
		};
		posts.push(post);
	}
	return posts;
}

async function fetchTextBounded(url: string, maxBytes: number): Promise<string> {
	const response = await fetch(url, { signal: AbortSignal.timeout(20_000) });
	if (!response.ok) throw new Error(`preview fetch failed with HTTP ${response.status}`);
	const declared = Number(response.headers.get("content-length") || "0");
	if (declared > maxBytes) throw new Error("preview page exceeds size cap");
	const text = await response.text();
	if (Buffer.byteLength(text) > maxBytes) throw new Error("preview page exceeds size cap");
	return text;
}

export async function fetchPreview(username: string): Promise<PreviewPost[]> {
	const clean = String(username).replace(/^@/, "").toLowerCase();
	if (!/^[a-z0-9_]{3,64}$/.test(clean)) throw new Error("invalid source username");
	const base = previewBase();
	const html = await fetchTextBounded(`${base}/s/${clean}`, 2 * 1024 * 1024);
	const posts = parsePreview(html, clean, base);
	if (posts.length === 0) throw new Error(`preview parser found no posts for ${clean}`);
	return posts;
}

export async function thumbnailsFor(
	posts: PreviewPost[],
	maxImages = 3,
	maxTotalBytes = 512 * 1024,
): Promise<Array<{ type: "image"; data: string; mimeType: string }>> {
	const out: Array<{ type: "image"; data: string; mimeType: string }> = [];
	let total = 0;
	for (const post of posts) {
		if (out.length >= maxImages) break;
		const media = post.media;
		if (!media) continue;
		 const url =
		   media.kind === "photo" ? media.url : media.thumbnail || "";
	  if (!url) continue;
	  const remaining = maxTotalBytes - total;
	  if (remaining <= 0) break;
	  try {
	    const bytes = await downloadBounded(url, remaining);
	    const mimeType = sniffMime(bytes);
	    if (!mimeType || !mimeType.startsWith("image/")) continue;
	    total += bytes.length;
	    out.push({ type: "image", data: bytes.toString("base64"), mimeType });
	  } catch {}
  }
  return out;
}
