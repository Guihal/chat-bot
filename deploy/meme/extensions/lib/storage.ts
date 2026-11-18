import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import {
	CANDIDATES_DIR,
	TARGET_CHAT_ID,
	appendLedger,
	candidateId,
	ensureLayout,
	env,
	extensionForMime,
	isCandidateId,
	ledgerHas,
	ledgerPublished,
	nowIso,
	readJson,
	sha256Bytes,
	sniffMime,
	writeJsonAtomic,
} from "./common.js";
import type { Candidate, MediaKind } from "./types.js";

export interface SaveInput {
	source: string;
	post_id: string;
	source_url?: string;
	text: string;
	why_funny: string;
	metrics?: Candidate["metrics"];
	media: { buffer: Buffer; mime: string; kind: MediaKind };
}

export interface SaveResult {
	status: "saved" | "duplicate";
	candidate?: Candidate;
}

function candidatePath(id: string): string {
	if (!isCandidateId(id)) throw new Error("invalid candidate id");
	const base = path.resolve(CANDIDATES_DIR);
	const file = path.resolve(base, `${id}.json`);
	if (!file.startsWith(`${base}${path.sep}`)) throw new Error("candidate path escapes storage");
	return file;
}

export function candidateExists(id: string): boolean {
	return fs.existsSync(candidatePath(id));
}

export function readCandidate(id: string): Candidate {
	const file = candidatePath(id);
	const stat = fs.lstatSync(file);
	if (!stat.isFile() || stat.isSymbolicLink()) throw new Error("candidate is not a regular file");
	return readJson<Candidate>(file);
}

function mimeAllowed(kind: MediaKind, mime: string): boolean {
	if (kind === "photo") return ["image/jpeg", "image/png", "image/webp"].includes(mime);
	if (kind === "animation") return ["image/gif", "video/mp4", "video/webm"].includes(mime);
	return ["video/mp4", "video/webm"].includes(mime);
}

export function saveCandidate(input: SaveInput): SaveResult {
	const why = String(input.why_funny || "").trim();
	if (!why || why.length > 500) throw new Error("why_funny must be 1..500 characters");
	const id = candidateId(input.source, input.post_id);
	if (candidateExists(id) || ledgerHas({ id })) return { status: "duplicate" };
	const mime = sniffMime(input.media.buffer);
	if (!mime || mime !== input.media.mime) throw new Error("media MIME does not match its bytes");
	if (!mimeAllowed(input.media.kind, mime)) throw new Error("media kind does not match its MIME");
	const sha256 = sha256Bytes(input.media.buffer);
	if (ledgerHas({ sha256 })) return { status: "duplicate" };

	ensureLayout();
	const extension = extensionForMime(mime);
	const mediaPath = path.join(CANDIDATES_DIR, `${id}.${extension}`);
	const tempPath = path.join(
		CANDIDATES_DIR,
		`.${id}.${process.pid}.${crypto.randomBytes(6).toString("hex")}.tmp`,
	);
	fs.writeFileSync(tempPath, input.media.buffer, { mode: 0o600 });
	try {
		fs.linkSync(tempPath, mediaPath);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "EEXIST") return { status: "duplicate" };
		throw error;
	} finally {
		try {
			fs.unlinkSync(tempPath);
		} catch {}
	}

	const candidate: Candidate = {
		id,
		source: String(input.source).toLowerCase(),
		source_post_id: String(input.post_id),
		source_url:
			input.source_url ||
			`https://t.me/${String(input.source).toLowerCase()}/${String(input.post_id)}`,
		text: String(input.text || "").trim(),
		why_funny: why,
		media: {
			kind: input.media.kind,
			path: mediaPath,
			sha256,
			bytes: input.media.buffer.length,
			mime,
		},
		metrics: input.metrics || {},
		discovered_at: nowIso(),
		status: "candidate",
		published_at: null,
		target_chat_id: null,
		telegram_message_id: null,
	};
	try {
		const fd = fs.openSync(candidatePath(id), "wx", 0o600);
		try {
			fs.writeFileSync(fd, `${JSON.stringify(candidate, null, 2)}\n`);
		} finally {
			fs.closeSync(fd);
		}
	} catch (error) {
		try {
			fs.unlinkSync(mediaPath);
		} catch {}
		if ((error as NodeJS.ErrnoException).code === "EEXIST") return { status: "duplicate" };
		throw error;
	}
	appendLedger({
		id,
		source: candidate.source,
		source_post_id: candidate.source_post_id,
		sha256,
		status: "candidate",
		published_at: null,
		first_seen: candidate.discovered_at,
	});
	return { status: "saved", candidate };
}

export function listCandidates(limit = 20): Candidate[] {
	const bound = Math.max(1, Math.min(50, Number(limit) || 20));
	let names: string[] = [];
	try {
		names = fs.readdirSync(CANDIDATES_DIR).filter((name) => name.endsWith(".json"));
	} catch {
		return [];
	}
	const rows: Candidate[] = [];
	for (const name of names) {
		try {
			const row = readJson<Candidate>(path.join(CANDIDATES_DIR, name));
			if (row.status === "candidate") rows.push(row);
		} catch {}
	}
	return rows
		.sort((a, b) => String(b.discovered_at).localeCompare(String(a.discovered_at)))
		.slice(0, bound);
}

function allCandidates(): Candidate[] {
	let names: string[] = [];
	try {
		names = fs.readdirSync(CANDIDATES_DIR).filter((name) => name.endsWith(".json"));
	} catch {
		return [];
	}
	const rows: Candidate[] = [];
	for (const name of names) {
		 try {
		   rows.push(readJson<Candidate>(path.join(CANDIDATES_DIR, name)));
		 } catch {}
	  }
	  return rows;
}

function dailyPublishedCount(): number {
	const today = new Date().toISOString().slice(0, 10);
	return allCandidates().filter(
		 (row) => row.status === "published" && String(row.published_at || "").startsWith(today),
	  ).length;
}

export function claimCandidate(id: string): Candidate {
	const row = readCandidate(id);
	if (row.status !== "candidate") throw new Error(`candidate is not available (${row.status})`);
	if (ledgerPublished({ id }) || ledgerPublished({ sha256: row.media.sha256 })) {
		 throw new Error("candidate is already known in the permanent dedupe ledger");
	  }
	const cap = Number(env("MEME_MAX_PUBLISH_PER_DAY", "3")) || 3;
	if (dailyPublishedCount() >= cap) throw new Error("daily publish cap reached");
	const claimed = { ...row, status: "publishing" as const, claimed_at: nowIso() };
	  writeJsonAtomic(candidatePath(id), claimed);
	  return claimed;
}

export function markPublished(id: string, messageId: number): Candidate {
	const row = readCandidate(id);
	if (row.status !== "publishing") throw new Error(`candidate is not claimed (${row.status})`);
	const publishedAt = nowIso();
	const updated: Candidate = {
		 ...row,
		 status: "published",
		 published_at: publishedAt,
		 target_chat_id: TARGET_CHAT_ID,
		 telegram_message_id: messageId,
	  };
	  writeJsonAtomic(candidatePath(id), updated);
	  appendLedger({
	    id,
	    source: row.source,
	    source_post_id: row.source_post_id,
	    sha256: row.media.sha256,
	    status: "published",
	    published_at: publishedAt,
	    first_seen: row.discovered_at,
	  });
	  return updated;
}

export function markFailed(id: string, errorText: string): void {
	const row = readCandidate(id);
	const updated: Candidate = { ...row, status: "failed", error: String(errorText).slice(0, 500) };
	  writeJsonAtomic(candidatePath(id), updated);
	  appendLedger({
	    id,
	    source: row.source,
	    source_post_id: row.source_post_id,
	    sha256: row.media.sha256,
	    status: "failed",
	    published_at: null,
	    first_seen: row.discovered_at,
	  });
}

export function prune(): void {
	const rows = allCandidates();
	let totalBytes = 0;
	for (const row of rows) {
		 try {
		   totalBytes += fs.statSync(row.media.path).size;
		 } catch {}
	  }
	let entries = rows.length;
	if (totalBytes <= 200 * 1024 * 1024 && entries <= 500) return;
	const ordered = [...rows].sort((a, b) => String(a.discovered_at).localeCompare(String(b.discovered_at)));
	for (const row of ordered) {
		 if (totalBytes <= 200 * 1024 * 1024 && entries <= 500) break;
		 if (!["published", "failed", "candidate"].includes(row.status)) continue;
		 let size = 0;
		 try {
		   size = fs.statSync(row.media.path).size;
		 } catch {}
		 try {
		   fs.unlinkSync(row.media.path);
		 } catch {}
		 try {
		   fs.unlinkSync(candidatePath(row.id));
		 } catch {}
		 totalBytes -= size;
		 entries -= 1;
	  }
}
