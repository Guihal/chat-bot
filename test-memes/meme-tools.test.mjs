import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import crypto from "node:crypto";
import { afterAll, beforeAll, expect, test } from "bun:test";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "meme-tools-"));
const fixtures = path.join(import.meta.dir, "fixtures");
const tokenPath = path.join(root, "token");
fs.writeFileSync(tokenPath, "123456:TESTTOKEN\n", { mode: 0o600 });
fs.chmodSync(tokenPath, 0o600);
const notifyPath = path.join(root, "tg-notify");
fs.writeFileSync(notifyPath, "#!/bin/sh\nexit 0\n", { mode: 0o755 });
fs.chmodSync(notifyPath, 0o755);
process.env.MEME_TG_NOTIFY_BIN = notifyPath;
fs.writeFileSync(
	path.join(root, "meme-sources.md"),
	"# sources\nhttps://t.me/catmodernews weight=high\nhttps://t.me/second\n",
);

process.env.MEME_DATA_DIR = root;
process.env.MEME_SOURCES_PATH = path.join(root, "meme-sources.md");
process.env.MEME_ALLOW_FIXTURE_MEDIA = "1";
process.env.MEME_BOT_TOKEN_FILE = tokenPath;
process.env.MEME_MAX_PUBLISH_PER_DAY = "3";

let server;
let port = 0;
let lastMultipart = Buffer.alloc(0);
let sendCount = 0;

beforeAll(async () => {
	server = http.createServer((req, res) => {
		const chunks = [];
		req.on("data", (chunk) => chunks.push(chunk));
		req.on("end", () => {
			const body = Buffer.concat(chunks);
			const url = req.url || "";
			const sendJson = (value) => {
				res.writeHead(200, { "content-type": "application/json" });
				res.end(JSON.stringify(value));
			};
			if (url === "/s/catmodernews") {
				res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
				res.end(fs.readFileSync(path.join(fixtures, "preview-catmodernews.html")));
				return;
			}
			if (url === "/s/catmodernewsvideo") {
				res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
				res.end(fs.readFileSync(path.join(fixtures, "preview-catmodernews-video.html")));
				return;
			}
			if (url === "/s/second") {
				res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
				res.end(fs.readFileSync(path.join(fixtures, "preview-second.html")));
				return;
			}
			if (url === "/media/cat-quartz.png") {
				res.writeHead(200, { "content-type": "image/png" });
				res.end(fs.readFileSync(path.join(fixtures, "cat-quartz.png")));
				return;
			}
			if (url === "/media/video-frame-42.png") {
				res.writeHead(200, { "content-type": "image/png" });
				res.end(fs.readFileSync(path.join(fixtures, "video-frame-42.png")));
				return;
			}
			if (url === "/media/video-frame-42.mp4") {
				res.writeHead(200, { "content-type": "video/mp4" });
				res.end(fs.readFileSync(path.join(fixtures, "video-frame-42.mp4")));
				return;
			}
			if (url.includes("/sendPhoto") || url.includes("/sendVideo") || url.includes("/sendAnimation")) {
				sendCount += 1;
				lastMultipart = body;
				sendJson({ ok: true, result: { message_id: 123 } });
				return;
			}
			if (url.includes("/getMe")) {
				sendJson({ ok: true, result: { id: 999, username: "test_bot" } });
				return;
			}
			if (url.includes("/getChatMember")) {
				sendJson({
					ok: true,
					result: { status: "administrator", can_post_messages: true },
				});
				return;
			}
			if (url.includes("/getChat")) {
				sendJson({ ok: true, result: { type: "channel" } });
				return;
			}
			res.writeHead(404);
			res.end("not found");
		});
	});
	await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
	port = server.address().port;
	process.env.MEME_PREVIEW_BASE = `http://127.0.0.1:${port}`;
	process.env.MEME_BOT_API_BASE = `http://127.0.0.1:${port}`;
});

afterAll(() => {
	server?.close();
	fs.rmSync(root, { recursive: true, force: true });
});

const common = await import("../deploy/meme/extensions/lib/common.ts");
const preview = await import("../deploy/meme/extensions/lib/preview.ts");
const storage = await import("../deploy/meme/extensions/lib/storage.ts");
const botapi = await import("../deploy/meme/extensions/lib/botapi.ts");
const discoveryModule = await import("../deploy/meme/extensions/discovery/index.ts");
const publisherModule = await import("../deploy/meme/extensions/publisher/index.ts");

function toolsOf(factory) {
	const tools = new Map();
	factory({ registerTool: (tool) => tools.set(tool.name, tool) });
	return tools;
}

test("allowlist parses urls weights and dedupes", () => {
	const entries = preview.parseSources(
		"# comment\nhttps://t.me/CatModerNews weight=high\ncatmodernews\nsecond\n",
	);
	expect(entries).toEqual([
		{ username: "catmodernews", weight: "high" },
		{ username: "second", weight: "normal" },
	]);
});

test("missing sources file fails closed", () => {
	expect(() => preview.readSourcesFrom(path.join(root, "missing.md"))).toThrow(/missing or unreadable/);
});

test("preview parser extracts posts and image content", async () => {
	const posts = await preview.fetchPreview("catmodernews");
	expect(posts.length).toBe(2);
	expect(posts[0].post_id).toBe("101");
	expect(posts[0].text).not.toContain("42");
	const images = await preview.thumbnailsFor(posts);
	expect(images.length).toBeGreaterThanOrEqual(1);
	expect(images[0].type).toBe("image");
	expect(images[0].mimeType).toBe("image/png");
});

test("video thumbnail falls back to live video_thumb markup", async () => {
	const posts = await preview.fetchPreview("catmodernewsvideo");
	expect(posts.length).toBe(1);
	expect(posts[0].media?.kind).toBe("video");
	expect(posts[0].media?.thumbnail || "").toContain("video-frame-42.png");
});

test("discovery tool errors raise a once-per-day alert", async () => {
	const tools = toolsOf(discoveryModule.default);
	const marker = path.join(common.ALERTS_DIR, `${new Date().toISOString().slice(0, 10)}-discovery-tool`);
	fs.rmSync(marker, { force: true });
	const result = await tools.get("meme_preview_fetch").execute("alert", { source: "not_allowed" });
	expect(result.isError).toBe(true);
	expect(fs.existsSync(marker)).toBe(true);
});

test("discovery tools have no send capability and save once", async () => {
	const tools = toolsOf(discoveryModule.default);
	expect([...tools.keys()].sort()).toEqual([
		"meme_candidate_save",
		"meme_preview_fetch",
		"meme_sources_read",
	]);
	const saved = await tools.get("meme_candidate_save").execute("t1", {
		source: "catmodernews",
		post_id: "101",
		text: "кот и кварц",
		media_url: `http://127.0.0.1:${port}/media/cat-quartz.png`,
		media_kind: "photo",
		why_funny: "три жёлтых круга и число 42",
	});
	const payload = JSON.parse(saved.content[0].text);
	expect(payload.status).toBe("saved");
	const duplicate = await tools.get("meme_candidate_save").execute("t2", {
		source: "catmodernews",
		post_id: "101",
		text: "кот и кварц",
		media_url: `http://127.0.0.1:${port}/media/cat-quartz.png`,
		media_kind: "photo",
		why_funny: "три жёлтых круга и число 42",
	});
	expect(JSON.parse(duplicate.content[0].text).status).toBe("duplicate");
});

test("caption stays within budget and has no source attribution", () => {
	const candidate = storage.readCandidate("catmodernews__101");
	const caption = common.captionFor({ ...candidate, text: "я".repeat(3000) });
	expect(caption.length).toBeLessThanOrEqual(1024);
	expect(caption).not.toContain("источник:");
	expect(common.captionFor({ ...candidate, text: "   " })).toBe("");
});

test("publisher sends exactly one multipart to the fixed channel", async () => {
	const tools = toolsOf(publisherModule.default);
	const listed = await tools.get("meme_candidates_list").execute("t3", { limit: 10 });
	const candidates = JSON.parse(listed.content[0].text).candidates;
	const id = candidates.find((row) => row.id === "catmodernews__101").id;
	const published = await tools.get("meme_publish").execute("t4", { candidate_id: id });
	const payload = JSON.parse(published.content[0].text);
	expect(payload.status).toBe("published");
	expect(payload.target_chat_id).toBe(-1004421941006);
	const bodyText = lastMultipart.toString("utf8");
	const png = fs.readFileSync(path.join(fixtures, "cat-quartz.png"));
	const pngHash = crypto.createHash("sha256").update(png).digest("hex");
	const bodyHash = crypto.createHash("sha256").update(lastMultipart).digest("hex");
	console.log(`MULTIPART_HASH ${bodyHash} PNG_HASH ${pngHash}`);
	expect(bodyText).toContain('name="chat_id"');
	expect(bodyText).toContain("-1004421941006");
	expect(bodyText).toContain('name="caption"');
	expect(bodyText).toContain("кот и кварц");
	expect(bodyText).not.toContain("источник:");
	expect(lastMultipart.includes(png)).toBe(true);
	const again = await tools.get("meme_publish").execute("t5", { candidate_id: id });
	if (!again.isError) throw new Error("second publish must be refused");
});

test("empty caption omits the multipart caption field", async () => {
	const discovery = toolsOf(discoveryModule.default);
	const saved = await discovery.get("meme_candidate_save").execute("t-empty", {
		source: "catmodernews",
		post_id: "103",
		text: "   ",
		media_url: `http://127.0.0.1:${port}/media/video-frame-42.png`,
		media_kind: "photo",
		why_funny: "число 42 на кадре",
	});
	const payload = JSON.parse(saved.content[0].text);
	if (payload.status !== "saved") throw new Error(`empty-caption candidate not saved`);
	const publisher = toolsOf(publisherModule.default);
	const published = await publisher
		.get("meme_publish")
		.execute("t-empty-pub", { candidate_id: payload.candidate.id });
	expect(JSON.parse(published.content[0].text).status).toBe("published");
	const bodyText = lastMultipart.toString("utf8");
	expect(bodyText).not.toContain('name="caption"');
});

test("daily cap blocks a second publish", async () => {
	const tools = toolsOf(discoveryModule.default);
	const saved = await tools.get("meme_candidate_save").execute("t6", {
		source: "catmodernews",
		post_id: "102",
		text: "видео постмодерн",
		media_url: `http://127.0.0.1:${port}/media/video-frame-42.mp4`,
		media_kind: "video",
		why_funny: "число 42 на кадре",
	});
	const payload = JSON.parse(saved.content[0].text);
	if (payload.status !== "saved") throw new Error(`second candidate not saved`);
	const previousCap = process.env.MEME_MAX_PUBLISH_PER_DAY;
	try {
	  process.env.MEME_MAX_PUBLISH_PER_DAY = "1";
	  expect(() => storage.claimCandidate(payload.candidate.id)).toThrow(/cap/);
	} finally {
	  process.env.MEME_MAX_PUBLISH_PER_DAY = previousCap || "3";
	}
});

test("non-cdn media url is rejected outside fixtures", () => {
	const previous = process.env.MEME_ALLOW_FIXTURE_MEDIA;
	delete process.env.MEME_ALLOW_FIXTURE_MEDIA;
	try {
		 expect(() => common.assertAllowedMediaUrl("https://evil.example/x.png")).toThrow(/allowed Telegram CDN/);
	  } finally {
	    process.env.MEME_ALLOW_FIXTURE_MEDIA = previous || "";
	  }
});

test("bot api base must stay loopback when set", () => {
	const previous = process.env.MEME_BOT_API_BASE;
	try {
		 process.env.MEME_BOT_API_BASE = "https://api.telegram.org";
	  expect(() => botapi.apiBase()).toThrow(/loopback/);
	  } finally {
	    process.env.MEME_BOT_API_BASE = previous || "";
	  }
});

function uniquePng(tag) {
	return Buffer.concat([fs.readFileSync(path.join(fixtures, "cat-quartz.png")), Buffer.from(tag)]);
}

function saveUnique(source, postId, tag) {
	return storage.saveCandidate({
		source,
		post_id: postId,
		text: "тест",
		why_funny: "тестовая причина",
		media: { buffer: uniquePng(tag), mime: "image/png", kind: "photo" },
	});
}

function candidateJson(id) {
	return path.join(common.CANDIDATES_DIR, `${id}.json`);
}

test("cross-source repost with the same sha256 is rejected", () => {
	const first = saveUnique("second", "910", "cross-source");
	expect(first.status).toBe("saved");
	const second = saveUnique("second", "911", "cross-source");
	expect(second.status).toBe("duplicate");
});

test("media above its kind cap is rejected at save time", () => {
	const header = fs.readFileSync(path.join(fixtures, "cat-quartz.png")).subarray(0, 8);
	const oversized = Buffer.concat([header, Buffer.alloc(10 * 1024 * 1024 + 1)]);
	expect(() =>
		storage.saveCandidate({
			source: "second",
			post_id: "912",
			text: "x",
			why_funny: "y",
			media: { buffer: oversized, mime: "image/png", kind: "photo" },
		}),
	).toThrow(/size cap/);
});

test("token file with group or other permissions is refused", () => {
	const badToken = path.join(root, "bad-token");
	fs.writeFileSync(badToken, "123456:BADTOKEN\n", { mode: 0o644 });
	fs.chmodSync(badToken, 0o644);
	const previous = process.env.MEME_BOT_TOKEN_FILE;
	try {
		process.env.MEME_BOT_TOKEN_FILE = badToken;
		expect(() => botapi.sendMedia(storage.readCandidate("catmodernews__101"))).toThrow(/owner-only|permission/);
	} finally {
		process.env.MEME_BOT_TOKEN_FILE = previous || tokenPath;
	}
});

for (const [label, mutate] of [
	["traversal", (row) => ({ ...row, media: { ...row.media, path: "/etc/passwd" } })],
	["symlink", (row) => row],
]) {
	test(`publish rejects ${label} media paths`, async () => {
		const id = `second__${label === "traversal" ? "913" : "914"}`;
		expect(saveUnique("second", id.split("__")[1], label).status).toBe("saved");
		const file = candidateJson(id);
		const row = JSON.parse(fs.readFileSync(file, "utf8"));
		if (label === "symlink") {
			fs.rmSync(row.media.path);
			fs.symlinkSync("/etc/passwd", row.media.path);
		}
		fs.writeFileSync(file, JSON.stringify(mutate(row)));
		const result = await toolsOf(publisherModule.default)
			.get("meme_publish")
			.execute(`neg-${label}`, { candidate_id: id });
		expect(result.isError).toBe(true);
	});
}

test("stale publishing claim is failed instead of re-sent", () => {
	const id = "second__915";
	expect(saveUnique("second", "915", "stale").status).toBe("saved");
	const file = candidateJson(id);
	const row = JSON.parse(fs.readFileSync(file, "utf8"));
	row.status = "publishing";
	row.claimed_at = new Date(Date.now() - 31 * 60_000).toISOString();
	fs.writeFileSync(file, JSON.stringify(row));
	expect(() => storage.claimCandidate(id)).toThrow(/stale/);
});

test("concurrent publishers send exactly once", async () => {
	const id = "second__916";
	expect(saveUnique("second", "916", "concurrent").status).toBe("saved");
	const publish = toolsOf(publisherModule.default).get("meme_publish");
	const before = sendCount;
	const results = await Promise.allSettled([
		publish.execute("c1", { candidate_id: id }),
		publish.execute("c2", { candidate_id: id }),
	]);
	const succeeded = results.filter((r) => r.status === "fulfilled" && !r.value.isError).length;
	expect(succeeded).toBe(1);
	expect(sendCount - before).toBe(1);
});

test("prune removes media but keeps the permanent ledger entry", () => {
	const id = "second__917";
	expect(saveUnique("second", "917", "prune-ledger").status).toBe("saved");
	const previousBytes = process.env.MEME_PRUNE_MAX_BYTES;
	const previousEntries = process.env.MEME_PRUNE_MAX_ENTRIES;
	try {
		process.env.MEME_PRUNE_MAX_BYTES = "1";
		process.env.MEME_PRUNE_MAX_ENTRIES = "1";
		storage.prune();
	} finally {
		process.env.MEME_PRUNE_MAX_BYTES = previousBytes || String(200 * 1024 * 1024);
		process.env.MEME_PRUNE_MAX_ENTRIES = previousEntries || "500";
	}
	expect(fs.existsSync(candidateJson(id))).toBe(false);
	expect(common.ledgerHas({ id })).toBe(true);
});

test("alerts are rate limited once per day and kind", () => {
	const kind = `negative-${Date.now()}`;
	expect(common.alertOnce(kind, "first" )).toBe(true);
	expect(common.alertOnce(kind, "second" )).toBe(false);
});

test("alert marker is written only after tg-notify succeeds", () => {
	const failing = path.join(root, "tg-notify-fail");
	fs.writeFileSync(failing, "#!/bin/sh\nexit 1\n", { mode: 0o755 });
	fs.chmodSync(failing, 0o755);
	const previous = process.env.MEME_TG_NOTIFY_BIN;
	process.env.MEME_TG_NOTIFY_BIN = failing;
	const kind = `failed-${Date.now()}`;
	try {
		expect(common.alertOnce(kind, "must not mark" )).toBe(false);
		const marker = path.join(common.ALERTS_DIR, `${new Date().toISOString().slice(0, 10)}-${kind}`);
		expect(fs.existsSync(marker)).toBe(false);
	} finally {
		process.env.MEME_TG_NOTIFY_BIN = previous || notifyPath;
	}
});

test("save fails closed above the storage hard limit", () => {
	const previous = process.env.MEME_STORAGE_HARD_BYTES;
	process.env.MEME_STORAGE_HARD_BYTES = "1";
	try {
		expect(() => saveUnique("second", "918", "hard-limit")).toThrow(/hard limit/);
	} finally {
		process.env.MEME_STORAGE_HARD_BYTES = previous || String(256 * 1024 * 1024);
	}
});
