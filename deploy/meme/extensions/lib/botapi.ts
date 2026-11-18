import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import {
	TARGET_CHAT_ID,
	captionFor,
	env,
	mediaLimit,
	sha256File,
	sniffMime,
} from "./common.js";
import type { Candidate } from "./types.js";

function isLoopbackHost(hostname: string): boolean {
	return hostname === "127.0.0.1" || hostname === "::1" || hostname === "[::1]" || hostname === "localhost";
}

function tokenPath(): string {
	const explicit = env("MEME_BOT_TOKEN_FILE", "");
	if (explicit) return explicit;
	const fallback = env("CHATBOT_TOKEN_FILE", "");
	if (fallback) return fallback;
	throw new Error("MEME_BOT_TOKEN_FILE or CHATBOT_TOKEN_FILE is required");
}

function readToken(): string {
	const file = tokenPath();
	let stat: fs.Stats;
	try {
		stat = fs.statSync(file);
	} catch {
		throw new Error(`token file is missing or unreadable: ${file}`);
	}
	if ((stat.mode & 0o077) !== 0) throw new Error("token file must be owner-only (mode 0600)");
	const token = fs.readFileSync(file, "utf8").trim();
	if (!token) throw new Error("token file is empty");
	return token;
}

export function apiBase(): string {
	const raw = env("MEME_BOT_API_BASE", "");
	if (!raw) return "https://api.telegram.org";
	let url: URL;
	try {
		url = new URL(raw);
	} catch {
		throw new Error("MEME_BOT_API_BASE is not a valid URL");
	}
	if (!isLoopbackHost(url.hostname)) {
		throw new Error("MEME_BOT_API_BASE must point at loopback");
	}
	return raw.replace(/\/+$/, "");
}

async function apiCall<T>(method: string, params: Record<string, unknown>): Promise<T> {
	const token = readToken();
	let response: Response;
	try {
		response = await fetch(`${apiBase()}/bot${token}/${method}`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify(params),
			signal: AbortSignal.timeout(20_000),
		});
	} catch (error) {
		const code = (error as { cause?: { code?: string } }).cause?.code || (error as Error).name;
		throw new Error(`Bot API ${method} network error (${code})`);
	}
	let payload: { ok?: boolean; result?: T; description?: string };
	try {
		payload = (await response.json()) as typeof payload;
	} catch {
		throw new Error(`Bot API ${method} returned non-JSON HTTP ${response.status}`);
	}
	if (!response.ok || payload.ok !== true) {
		throw new Error(`Bot API ${method}: ${payload.description || `HTTP ${response.status}`}`);
	}
	return payload.result as T;
}

function buildMultipart(
	fields: Record<string, string>,
	fileField: string,
	filePath: string,
	mimeType: string,
): { body: Buffer; boundary: string } {
	const boundary = `----meme${crypto.randomBytes(16).toString("hex")}`;
	const chunks: Buffer[] = [];
	for (const [key, value] of Object.entries(fields)) {
		chunks.push(
			Buffer.from(
				`--${boundary}\r\nContent-Disposition: form-data; name="${key}"\r\n\r\n${value}\r\n`,
				"utf8",
			),
		);
	}
	const filename = path.basename(filePath);
	chunks.push(
		Buffer.from(
			`--${boundary}\r\nContent-Disposition: form-data; name="${fileField}"; filename="${filename}"\r\nContent-Type: ${mimeType}\r\n\r\n`,
			"utf8",
		),
	);
	chunks.push(fs.readFileSync(filePath));
	chunks.push(Buffer.from(`\r\n--${boundary}--\r\n`, "utf8"));
	return { body: Buffer.concat(chunks), boundary };
}

export interface SendResult {
	method: string;
	message_id: number;
}

export async function sendMedia(candidate: Candidate): Promise<SendResult> {
	const method =
		candidate.media.kind === "photo"
			? "sendPhoto"
			: candidate.media.kind === "animation"
				? "sendAnimation"
				: "sendVideo";
	const field =
		candidate.media.kind === "photo"
			? "photo"
			: candidate.media.kind === "animation"
				? "animation"
				: "video";
	const stat = fs.statSync(candidate.media.path);
	if (!stat.isFile()) throw new Error("candidate media is not a regular file");
	if (stat.size !== candidate.media.bytes) throw new Error("candidate media size changed");
	if (stat.size > mediaLimit(candidate.media.kind)) throw new Error("candidate media exceeds its size cap");
	if (sha256File(candidate.media.path) !== candidate.media.sha256) {
		throw new Error("candidate media hash changed");
	}
	if (sniffMime(fs.readFileSync(candidate.media.path)) !== candidate.media.mime) {
		throw new Error("candidate media MIME changed");
	}
	const caption = captionFor(candidate);
	if (caption.length > 1024) throw new Error("caption exceeds 1024 characters");
	const multipart = buildMultipart(
		{ chat_id: String(TARGET_CHAT_ID), caption },
		field,
		candidate.media.path,
		candidate.media.mime,
	);
	const token = readToken();
	let response: Response;
	try {
		response = await fetch(`${apiBase()}/bot${token}/${method}`, {
			method: "POST",
			headers: {
				"content-type": `multipart/form-data; boundary=${multipart.boundary}`,
				"content-length": String(multipart.body.length),
			},
			body: multipart.body,
			signal: AbortSignal.timeout(45_000),
		});
	} catch (error) {
		const code = (error as { cause?: { code?: string } }).cause?.code || (error as Error).name;
		throw new Error(`Bot API ${method} network error (${code})`);
	}
	let payload: { ok?: boolean; result?: { message_id?: number }; description?: string };
	try {
		payload = (await response.json()) as typeof payload;
	} catch {
		throw new Error(`Bot API ${method} returned non-JSON HTTP ${response.status}`);
	}
	if (!response.ok || payload.ok !== true) {
		 throw new Error(`Bot API ${method}: ${payload.description || `HTTP ${response.status}`}`);
	  }
	const messageId = Number(payload.result?.message_id || 0);
	if (!Number.isInteger(messageId) || messageId <= 0) {
		 throw new Error(`Bot API ${method} returned no message_id`);
	  }
	return { method, message_id: messageId };
}

export async function checkChannelRights(): Promise<Record<string, unknown>> {
	const me = await apiCall<{ id?: number; username?: string }>("getMe", {});
	if (!me.id) throw new Error("getMe returned no bot id");
	const chat = await apiCall<{ type?: string }>("getChat", { chat_id: TARGET_CHAT_ID });
	const member = await apiCall<{
		 status?: string;
		 can_post_messages?: boolean;
	  }>("getChatMember", { chat_id: TARGET_CHAT_ID, user_id: me.id });
	const status = String(member.status || "");
	if (!["administrator", "creator"].includes(status)) {
		 throw new Error(`bot is not an administrator of the target channel (status=${status})`);
	  }
	if (member.can_post_messages === false) {
		 throw new Error("bot administrator rights do not include posting messages");
	  }
	return {
		 bot_username: me.username || "",
		 chat_type: chat.type || "",
		 status,
		 can_post_messages: member.can_post_messages !== false,
	  };
}
