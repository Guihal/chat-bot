import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import type { Candidate, LedgerRecord, MediaKind } from "./types.js";

export const TARGET_CHAT_ID = -1004421941006;

export function env(name: string, fallback = ""): string {
  const value = process.env[name];
  return value === undefined || value === "" ? fallback : value;
}

export const DATA_DIR = env("MEME_DATA_DIR", "/opt/omo-chat/data/memes");
export const CANDIDATES_DIR = path.join(DATA_DIR, "candidates");
export const PUBLISHED_DIR = path.join(DATA_DIR, "published");
export const FAILED_DIR = path.join(DATA_DIR, "failed");
export const LOCKS_DIR = path.join(DATA_DIR, "locks");
export const ALERTS_DIR = path.join(DATA_DIR, "alerts");
export const LEDGER_PATH = path.join(DATA_DIR, "dedupe.jsonl");
export const SOURCES_PATH = env(
  "MEME_SOURCES_PATH",
  path.join(DATA_DIR, "meme-sources.md"),
);

export function ensureLayout(): void {
  for (const dir of [
    DATA_DIR,
    CANDIDATES_DIR,
    PUBLISHED_DIR,
    FAILED_DIR,
    LOCKS_DIR,
    ALERTS_DIR,
  ]) {
    fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  }
}

export function nowIso(): string {
  return new Date().toISOString();
}

export function readJson<T>(file: string): T {
  return JSON.parse(fs.readFileSync(file, "utf8")) as T;
}

export function writeJsonAtomic(file: string, value: unknown): void {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  const tmp = `${file}.tmp-${process.pid}-${crypto.randomBytes(6).toString("hex")}`;
  fs.writeFileSync(tmp, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
  fs.renameSync(tmp, file);
}

export function candidateId(source: string, postId: string): string {
  const cleanSource = String(source)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 64);
  const cleanPost = String(postId).trim();
  if (!cleanSource || !/^[0-9]{1,20}$/.test(cleanPost)) {
    throw new Error("invalid candidate identity");
  }
  return `${cleanSource}__${cleanPost}`;
}

export function isCandidateId(id: string): boolean {
  return /^[a-z0-9_]{1,64}__[0-9]{1,20}$/.test(id);
}

export function sha256Bytes(data: Buffer): string {
  return crypto.createHash("sha256").update(data).digest("hex");
}

export function sha256File(file: string): string {
  return sha256Bytes(fs.readFileSync(file));
}

export function sniffMime(data: Buffer): string | null {
  if (data.length >= 3 && data[0] === 0xff && data[1] === 0xd8 && data[2] === 0xff) {
    return "image/jpeg";
  }
  if (
    data.length >= 8 &&
    data.subarray(0, 8).equals(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]))
  ) {
    return "image/png";
  }
  if (data.length >= 6 && /^GIF8[79]a$/.test(data.subarray(0, 6).toString("ascii"))) {
    return "image/gif";
  }
  if (
    data.length >= 12 &&
    data.subarray(0, 4).toString("ascii") === "RIFF" &&
    data.subarray(8, 12).toString("ascii") === "WEBP"
  ) {
    return "image/webp";
  }
  if (data.length >= 12 && data.subarray(4, 8).toString("ascii") === "ftyp") {
    return "video/mp4";
  }
  if (data.length >= 4 && data[0] === 0x1a && data[1] === 0x45 && data[2] === 0xdf && data[3] === 0xa3) {
    return "video/webm";
  }
  return null;
}

export function extensionForMime(mime: string): string {
  switch (mime) {
    case "image/jpeg":
      return "jpg";
    case "image/png":
      return "png";
    case "image/gif":
      return "gif";
    case "image/webp":
      return "webp";
    case "video/mp4":
      return "mp4";
    case "video/webm":
      return "webm";
    default:
      throw new Error(`unsupported media mime ${mime}`);
  }
}

function isLoopbackHost(hostname: string): boolean {
  return hostname === "127.0.0.1" || hostname === "::1" || hostname === "[::1]" || hostname === "localhost";
}

export function assertAllowedMediaUrl(raw: string): URL {
  let url: URL;
  try {
    url = new URL(String(raw));
  } catch {
    throw new Error("invalid media url");
  }
  const fixture = env("MEME_ALLOW_FIXTURE_MEDIA", "") === "1";
  if (fixture && url.protocol === "http:" && isLoopbackHost(url.hostname)) return url;
  if (url.protocol !== "https:") throw new Error("media url must be https");
  	const host = url.hostname.toLowerCase();
	if (!(host === "telesco.pe" || host.endsWith(".telesco.pe"))) {
		throw new Error("media host is not an allowed Telegram CDN host");
	}
	return url;
}

export async function downloadBounded(rawUrl: string, maxBytes: number): Promise<Buffer> {
	const url = assertAllowedMediaUrl(rawUrl);
	const response = await fetch(url, { signal: AbortSignal.timeout(20_000) });
	if (!response.ok) throw new Error(`media fetch failed with HTTP ${response.status}`);
	const declared = Number(response.headers.get("content-length") || "0");
	if (declared > maxBytes) throw new Error(`media exceeds ${maxBytes} bytes`);
	if (!response.body) throw new Error("media response has no body");
	const reader = response.body.getReader();
	const chunks: Buffer[] = [];
	let total = 0;
	for (;;) {
		const { done, value } = await reader.read();
		if (done) break;
		const chunk = Buffer.from(value);
		total += chunk.length;
		if (total > maxBytes) throw new Error(`media exceeds ${maxBytes} bytes`);
		chunks.push(chunk);
	}
	return Buffer.concat(chunks);
}

export function captionFor(candidate: Candidate): string {
	const text = String(candidate.text || "").trim();
	if (!text) return "";
	return text.length > 1024 ? `${text.slice(0, 1023)}…` : text;
}

export function appendLedger(record: LedgerRecord): void {
	ensureLayout();
	fs.appendFileSync(LEDGER_PATH, `${JSON.stringify(record)}\n`, { mode: 0o600 });
}

export function ledgerRecords(): LedgerRecord[] {
	try {
		const raw = fs.readFileSync(LEDGER_PATH, "utf8");
		return raw
			.split("\n")
			.map((line) => line.trim())
			.filter(Boolean)
			.flatMap((line) => {
				try {
					return [JSON.parse(line) as LedgerRecord];
				} catch {
					return [];
				}
			});
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
		throw error;
	}
}

export function ledgerHas(idOrHash: { id?: string; sha256?: string }): boolean {
	return ledgerRecords().some(
		(record) =>
			(idOrHash.id !== undefined && record.id === idOrHash.id) ||
			(idOrHash.sha256 !== undefined && record.sha256 === idOrHash.sha256),
	);
}

export function ledgerPublished(idOrHash: { id?: string; sha256?: string }): boolean {
	return ledgerRecords().some(
		(record) =>
			["published", "failed", "duplicate"].includes(record.status) &&
			((idOrHash.id !== undefined && record.id === idOrHash.id) ||
				(idOrHash.sha256 !== undefined && record.sha256 === idOrHash.sha256)),
	);
}

export function alertOnce(kind: string, message: string): boolean {
	try {
		ensureLayout();
		const day = new Date().toISOString().slice(0, 10);
		const marker = path.join(ALERTS_DIR, `${day}-${kind}`);
		if (fs.existsSync(marker)) return false;
		const bin = env("MEME_TG_NOTIFY_BIN", "/root/bin/tg-notify");
		const result = spawnSync(bin, [message], {
			env: process.env,
			stdio: "ignore",
			timeout: 5000,
		});
		if (result.error || result.status !== 0) return false;
		fs.writeFileSync(marker, `${nowIso()} ${message}\n`, { mode: 0o600 });
		return true;
	} catch {
		return false;
	}
}

export function withLock<T>(name: string, fn: () => T): T {
	ensureLayout();
	const lockPath = path.join(LOCKS_DIR, `${name}.lock`);
	let fd: number | null = null;
	for (let attempt = 0; attempt < 2 && fd === null; attempt += 1) {
		try {
			fd = fs.openSync(lockPath, "wx", 0o600);
			fs.writeFileSync(fd, `${process.pid} ${nowIso()}\n`);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
			try {
				const ageMs = Date.now() - fs.statSync(lockPath).mtimeMs;
				if (ageMs > 30 * 60_000) fs.unlinkSync(lockPath);
				else throw new Error(`lock busy: ${name}`);
			} catch (statError) {
				if ((statError as NodeJS.ErrnoException).code === "ENOENT") continue;
				throw statError instanceof Error ? statError : new Error(String(statError));
			}
		}
	}
	if (fd === null) throw new Error(`lock busy: ${name}`);
	try {
		return fn();
	} finally {
		try {
			fs.closeSync(fd);
		} catch {}
		try {
			fs.unlinkSync(lockPath);
		} catch {}
	}
}

export function mediaLimit(kind: MediaKind): number {
	return kind === "photo" ? Math.floor(10 * Math.pow(1024,2)) : Math.floor(50 * Math.pow(1024,2));
}
