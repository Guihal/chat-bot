import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const repo = path.resolve(here, "..");
const fixtures = path.join(here, "fixtures");
const creds = process.env.CHATBOT_STAND_CREDS || "";
if (!creds || !fs.existsSync(path.join(creds, "auth.json"))) {
	console.error("STAND_MEME_FAIL missing CHATBOT_STAND_CREDS/auth.json");
	process.exit(2);
}

const root = fs.mkdtempSync(path.join(process.env.TMPDIR || os.tmpdir(), "meme-stand-"));
const dataDir = path.join(root, "data");
const sourcesPath = path.join(dataDir, "meme-sources.md");
const tokenPath = path.join(root, "token");
const discoveryProfile = path.join(root, "discovery-profile");
const publisherProfile = path.join(root, "publisher-profile");
let server;
let cleaned = false;

function cleanup() {
	if (cleaned) return;
	cleaned = true;
	try {
		server?.close();
	} catch {}
	try {
		fs.rmSync(root, { recursive: true, force: true });
	} catch {}
}

process.on("SIGINT", () => {
	cleanup();
	process.exit(130);
});
process.on("SIGTERM", () => {
	cleanup();
	process.exit(143);
});

function copyFile(from, to) {
	fs.mkdirSync(path.dirname(to), { recursive: true });
	fs.copyFileSync(from, to);
}

function prepareProfile(role) {
	const profile = role === "discovery" ? discoveryProfile : publisherProfile;
	const source = path.join(repo, "deploy/meme");
	fs.mkdirSync(path.join(profile, "extensions/lib"), { recursive: true });
	fs.mkdirSync(path.join(profile, "extensions", role), { recursive: true });
	fs.mkdirSync(path.join(profile, "skills/task"), { recursive: true });
	copyFile(path.join(source, "profiles", role, "settings.json"), path.join(profile, "settings.json"));
	copyFile(path.join(source, "profiles", role, "models.json"), path.join(profile, "models.json"));
	copyFile(path.join(creds, "auth.json"), path.join(profile, "auth.json"));
	fs.chmodSync(path.join(profile, "auth.json"), 0o600);
	for (const name of fs.readdirSync(path.join(source, "extensions/lib"))) {
		if (name.endsWith(".ts")) {
			copyFile(path.join(source, "extensions/lib", name), path.join(profile, "extensions/lib", name));
		}
	}
	copyFile(path.join(source, "extensions", role, "index.ts"), path.join(profile, "extensions", role, "index.ts"));
	copyFile(
		path.join(source, "extensions", role, "package.json"),
		path.join(profile, "extensions", role, "package.json"),
	);
	copyFile(
		path.join(source, "profiles", role, "default-min-temperature.ts"),
		path.join(profile, "extensions/default-min-temperature.ts"),
	);
	copyFile(
		path.join(source, "profiles", role, "sampling-payload.ts"),
		path.join(profile, "extensions/lib/sampling-payload.ts"),
	);
	copyFile(path.join(source, "skills/task/SKILL.md"), path.join(profile, "skills/task/SKILL.md"));
	return profile;
}

function readCandidates() {
	const dir = path.join(dataDir, "candidates");
	if (!fs.existsSync(dir)) return [];
	return fs
		.readdirSync(dir)
		.filter((name) => name.endsWith(".json"))
		.map((name) => JSON.parse(fs.readFileSync(path.join(dir, name), "utf8")));
}

function runProcess(command, args, env) {
	return new Promise((resolve) => {
		const child = spawn(command, args, {
			cwd: repo,
			env,
			stdio: ["ignore", "pipe", "pipe"],
		});
		let out = "";
		let err = "";
		const timer = setTimeout(() => child.kill("SIGKILL"), 420_000);
		child.stdout.on("data", (chunk) => {
			out = (out + chunk).slice(-200_000);
		});
		child.stderr.on("data", (chunk) => {
			err = (err + chunk).slice(-200_000);
		});
		child.on("close", (code) => {
			clearTimeout(timer);
			resolve({ code: code ?? -1, out, err });
		});
	});
}

function assert(condition, message) {
	if (!condition) throw new Error(message);
}

const state = { previewRequests: [], sends: [], lastMultipart: Buffer.alloc(0) };

function startServer() {
	return new Promise((resolve) => {
		server = http.createServer((req, res) => {
			const chunks = [];
			req.on("data", (chunk) => chunks.push(chunk));
			req.on("end", () => {
				const body = Buffer.concat(chunks);
				const url = req.url || "";
				const json = (value) => {
					res.writeHead(200, { "content-type": "application/json" });
					res.end(JSON.stringify(value));
				};
				if (url.startsWith("/s/")) {
					state.previewRequests.push(url);
					const name = url.slice(3);
					const file =
						name === "catmodernews"
							? path.join(fixtures, "preview-catmodernews-video.html")
							: name === "second"
								? path.join(fixtures, "preview-second.html")
								: "";
					if (!file || !fs.existsSync(file)) {
						res.writeHead(404);
						res.end("not found");
						return;
					}
					res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
					res.end(fs.readFileSync(file));
					return;
				}
				if (url.startsWith("/media/")) {
					const file = path.join(fixtures, path.basename(url));
					if (!fs.existsSync(file)) {
						res.writeHead(404);
						res.end("not found");
						return;
					}
					res.writeHead(200);
					res.end(fs.readFileSync(file));
					return;
				}
				if (/\/send(Photo|Video|Animation)$/.test(url)) {
					state.sends.push({ method: url.split("/").pop() });
					state.lastMultipart = body;
					json({ ok: true, result: { message_id: 9001 } });
					return;
				}
				if (url.includes("/getMe")) {
					json({ ok: true, result: { id: 999 } });
					return;
				}
				if (url.includes("/getChatMember")) {
					json({ ok: true, result: { status: "administrator", can_post_messages: true } });
					return;
				}
				if (url.includes("/getChat")) {
					json({ ok: true, result: { type: "channel" } });
					return;
				}
				res.writeHead(404);
				res.end("not found");
			});
		});
		server.listen(0, "127.0.0.1", () => resolve(server.address().port));
	});
}

async function main() {
	for (const role of ["discovery", "publisher"]) prepareProfile(role);
	if (process.env.CHATBOT_STAND_CREDS_DELETE === "1") {
		fs.rmSync(creds, { recursive: true, force: true });
		assert(!fs.existsSync(creds), "stand credentials directory still exists");
	}
	fs.mkdirSync(dataDir, { recursive: true });
	fs.writeFileSync(tokenPath, "123456:STANDTOKEN\n", { mode: 0o600 });
	fs.chmodSync(tokenPath, 0o600);
	fs.writeFileSync(sourcesPath, "# stand\nhttps://t.me/catmodernews weight=high\n");

	const port = await startServer();
	const env = {
		 ...process.env,
		 MEME_ROOT: dataDir,
		 MEME_LOG_DIR: path.join(dataDir, "logs"),
		 MEME_DATA_DIR: dataDir,
		 MEME_SOURCES_PATH: sourcesPath,
		 MEME_ALLOW_FIXTURE_MEDIA: "1",
		 MEME_PREVIEW_BASE: `http://127.0.0.1:${port}`,
		 MEME_BOT_API_BASE: `http://127.0.0.1:${port}`,
		 MEME_BOT_TOKEN_FILE: tokenPath,
		 MEME_DISCOVERY_PROFILE: discoveryProfile,
		 MEME_PUBLISHER_PROFILE: publisherProfile,
		 MEME_NO_SYSTEMD_RUN: "1",
		 MEME_ENABLED: "true",
		 OMO_BIN: process.env.OMO_BIN || "omo",
	  };

	const discovery = await runProcess("bash", [path.join(repo, "deploy/meme/discovery-run.sh")], env);
	assert(
		 discovery.code === 0,
		 `discovery exited ${discovery.code}\n${discovery.err.slice(-4000)}\n${discovery.out.slice(-2000)}`,
	  );
	const first = readCandidates();
	console.log(`CANDIDATES_AFTER_DISCOVERY ${first.map((row) => `${row.id}:${row.status}`).join(",")}`);
	assert(first.length >= 1, `discovery saved no candidates\n${discovery.out.slice(-3000)}`);
	const candidate = first[0];
	assert(candidate.media.kind === "video", `expected a video candidate, got ${candidate.media.kind}`);
	assert(!/42|круг/i.test(candidate.text), `fixture post text must not contain the image marker`);
	assert(
		 /42|круг/i.test(candidate.why_funny),
		 `why_funny does not name the frame-only marker: ${candidate.why_funny}`,
	  );
	assert(state.sends.length === 0, `discovery sent ${state.sends.length} channel messages`);
	const countBefore = first.length;
	const secondPreviewBefore = state.previewRequests.filter((url) => url.includes("/s/second")).length;

	fs.appendFileSync(sourcesPath, "https://t.me/second\n");
	const discovery2 = await runProcess("bash", [path.join(repo, "deploy/meme/discovery-run.sh")], env);
	assert(
		 discovery2.code === 0,
		 `second discovery exited ${discovery2.code}\n${discovery2.err.slice(-4000)}`,
	  );
	assert(
		 state.previewRequests.filter((url) => url.includes("/s/second")).length > secondPreviewBefore,
		 `mutated sources md was not picked up by the next run`,
	  );
	assert(readCandidates().length === countBefore, `duplicate candidate was written on the second run`);

	const sendsBefore = state.sends.length;
	const publisher = await runProcess("bash", [path.join(repo, "deploy/meme/publisher-run.sh")], env);
	assert(
		 publisher.code === 0,
		 `publisher exited ${publisher.code}\n${publisher.err.slice(-4000)}\n${publisher.out.slice(-2000)}`,
	  );
	assert(state.sends.length === sendsBefore + 1, `publisher made ${state.sends.length - sendsBefore} sends`);
	const bodyText = state.lastMultipart.toString("utf8");
	const mp4 = fs.readFileSync(path.join(fixtures, "video-frame-42.mp4"));
	assert(state.sends.at(-1)?.method === "sendVideo", `publisher used ${state.sends.at(-1)?.method}, not sendVideo`);
	assert(bodyText.includes("-1004421941006"), `multipart does not target the fixed channel`);
	assert(bodyText.includes('name="caption"'), `multipart has no caption`);
	assert(!bodyText.includes("источник:"), `caption still has source attribution`);
	assert(state.lastMultipart.includes(mp4), `multipart does not contain the saved MP4 bytes`);
	const published = readCandidates().find((row) => row.id === candidate.id);
	assert(published?.status === "published", `candidate was not marked published`);

	const publisher2 = await runProcess("bash", [path.join(repo, "deploy/meme/publisher-run.sh")], env);
	assert(publisher2.code === 0, `second publisher exited ${publisher2.code}`);
	if (state.sends.length !== sendsBefore + 1) {
	  const rows = readCandidates().map((row) => `${row.id}:${row.status}`).join(",");
	  throw new Error(
	    `second publisher sent again: candidates=${rows} sends=${state.sends.map((send) => send.method).join(",")}`,
	  );
	}

	const summary = {
		whyFunny: candidate.why_funny,
		publishedId: candidate.id,
		multipartLength: state.lastMultipart.length,
	};
	cleanup();
	assert(!fs.existsSync(root), "stand temp directory still exists");
	console.log(`DISCOVERY_WHY_FUNNY ${summary.whyFunny}`);
	console.log(`PUBLISHED_ID ${summary.publishedId}`);
	console.log(`MULTIPART_HASH ${summary.multipartLength}`);
	console.log("STAND_MEME_DISCOVERY_OK");
	console.log("STAND_MEME_PUBLISHER_OK");
	console.log("STAND_MEME_DEDUPE_OK");
}

main()
  .catch((error) => {
    console.error(`STAND_MEME_FAIL ${error.message}`);
    cleanup();
    process.exit(1);
  })
  .finally(() => cleanup());
