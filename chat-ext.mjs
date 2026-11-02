/**
 * chat-ext: native omo extension driving the chat-bot host session.
 *
 * - Registers the SOLE send path `chat_send` (Bot API sendMessage/sendAnimation).
 *   The daemon never posts assistant text; a turn with zero `chat_send` calls
 *   sends nothing (silence, equivalent to `[skip]`).
 * - Transport: the daemon hosts a unix socket at `<state>/control.sock` and this
 *   extension connects to it (no files). The daemon writes one line per batch,
 *   `{"push":{"delivery_id","text","deliverAs"}}`; the extension admits it via
 *   `admitExternalMessage` (idempotent per delivery_id) and answers
 *   `{"result":{"delivery_id","kind"}}`. A steer carries the current
 *   `turn_epoch`, so it lands in a RUNNING turn at the agent's next model step;
 *   `turn_conflict`/`held_draft` mean the runtime enqueued nothing, so the result
 *   says so and the daemon retries instead of counting it delivered.
 * - Delivery proof: `listAdmittedDeliveries()` is the only honest source for
 *   "the agent has it in context" (`emitted`) or "the session file refused it"
 *   (`failed`); both are mirrored into events as `delivery_state`.
 * - Events for the daemon land in `<state>/events.jsonl` (`turn_start`,
 *   `turn_end` with the turn's chat_send receipts, `admitted`, `admit_error`,
 *   `agent_end_aborted`, `compacted` with deliveries still active at compact
 *   time). The daemon waits on these instead of polling
 *   `omo state/read/wait`.
 * - Exactly-once sends across session restarts: receipts are cached in
 *   `<state>/sent-receipts.json` keyed by delivery_id; a re-admitted batch
 *   replays cached receipts without re-sending.
 * - `chat_send` sends one message per call and at most ONE media attachment
 *   per turn: a chat GIF by `gif_file_id` (sendAnimation), a sticker by
 *   `sticker_file_id` (sendSticker, no caption possible), or a local file by
 *   `media_path` (`.gif` -> sendAnimation, image/* -> sendPhoto, video/* ->
 *   sendVideo, anything else -> sendDocument). The three are mutually
 *   exclusive; a second media call in the same turn is refused.
 * - `gif_list` reads the FULL deduplicated GIF registry through the daemon
 *   (`{"call":{"op":"gif_list"}}` / `{"return":...}` on the same control
 *   socket) — only rows Telegram delivered as `animation`, never videos.
 *   The extension still never opens sqlite itself.
 * - `subagent` is the ONLY dispatch path and it is structurally gated: the
 *   daemon marks owner-authored message ids (`owner_msgs`) in each push, and
 *   the tool refuses unless the caller names one of them (`owner_message_id`).
 *   Owner = `CHATBOT_OWNER_USERNAME` / `CHATBOT_OWNER_ID` per crystal M1:
 *   subagents only on the owner's own request. Both may be unset — then nobody
 *   is the owner and dispatch stays locked (see env.example). The child is a
 *   one-shot `omo -p` process carrying the `general` persona (the native `task`
 *   tool is not part of this session's set and pulling its plugin in would drag
 *   memory/teams/rule injections into the character).
 *
 * No database access: GIF file_ids arrive from the daemon as `gif_file_id`
 * params (the daemon prints a `[gif:ID] -> file_id` catalog into the batch);
 * the full registry comes back through the daemon's control-socket handle.
 */

import { spawn } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";

const SKIP_TOKEN = "[skip]";

// ---- send classification (pure, unit-tested) ----
// One chat_send carries at most ONE media attachment: a GIF from the catalog,
// a sticker by file_id, or a local media file. Text-only calls and these three
// are the only shapes; anything else is refused instead of guessing which
// argument the model meant.
export const MAX_MEDIA_PER_TURN = 1;
export const TURN_MEDIA_ERROR =
  "error: за ход можно отправить только одно медиа (гифку, стикер или файл) — второе не отправлено; захочешь ещё — следующим ходом";
export const STICKER_TEXT_ERROR =
  "error: стикер не принимает подпись — отправь текст отдельным chat_send без sticker_file_id";

export function classifyChatSend(params) {
  const p = params || {};
  const gif = p.gif_file_id ? String(p.gif_file_id) : "";
  const sticker = p.sticker_file_id ? String(p.sticker_file_id) : "";
  const media = p.media_path ? String(p.media_path) : "";
  const chosen = [gif ? "gif" : "", sticker ? "sticker" : "", media ? "media" : ""].filter(
    Boolean,
  );
  if (chosen.length > 1)
    return {
      error: "error: gif_file_id, sticker_file_id и media_path взаимоисключающие",
    };
  if (!chosen.length) return { target: "text" };
  // Telegram's sendSticker takes no caption; silently dropping the text would
  // lose it, so the caller must send it as its own message.
  if (chosen[0] === "sticker" && String(p.text || "").trim())
    return { error: STICKER_TEXT_ERROR };
  return { target: chosen[0] };
}

// One media attachment per turn: the hard, machine-checkable half of
// "кидать активнее, но не перебарщивать" (the persona carries the judgement).
export function allowsAnotherMedia(count) {
  return Number(count) < MAX_MEDIA_PER_TURN;
}

// 45 MB stays under the Bot API upload ceiling; larger files are refused
// before reading them so a huge batch file cannot stall the turn.
const MEDIA_MAX_BYTES = 45 * 1024 * 1024;
const MIME_BY_EXT = {
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".png": "image/png",
  ".webp": "image/webp",
  ".gif": "image/gif",
  ".mp4": "video/mp4",
  ".mov": "video/quicktime",
  ".webm": "video/webm",
  ".mkv": "video/x-matroska",
  ".mp3": "audio/mpeg",
  ".ogg": "audio/ogg",
  ".pdf": "application/pdf",
};

// Upload target by file type. A `.gif` goes out as an ANIMATION (Telegram
// shows it as a looping gif), not as a still photo; only other images use
// sendPhoto.
export function mediaSpec(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  const mime = MIME_BY_EXT[ext] || "application/octet-stream";
  if (ext === ".gif")
    return { method: "sendAnimation", field: "animation", mime };
  if (mime.startsWith("image/"))
    return { method: "sendPhoto", field: "photo", mime };
  if (mime.startsWith("video/"))
    return { method: "sendVideo", field: "video", mime };
  return { method: "sendDocument", field: "document", mime };
}

function env(name, fallback) {
  // Host launch-spec env only allows (SENPI|OMO|PI)_[A-Z0-9_]+ keys, so the
  // daemon passes OMO_CHATBOT_* and local harnesses use CHATBOT_*.
  for (const k of [`OMO_${name}`, name]) {
    const v = process.env[k];
    if (v !== undefined && v !== "") return v;
  }
  return fallback;
}

export default function (pi) {
  const stateDir = env("CHATBOT_STATE_DIR", "./state");
  const controlSockPath = path.join(stateDir, "control.sock");
  const eventsPath = path.join(stateDir, "events.jsonl");
  const receiptsPath = path.join(stateDir, "sent-receipts.json");
  const apiBase = env("CHATBOT_API_BASE", "https://api.telegram.org");
  const tokenFile = env("CHATBOT_TOKEN_FILE", "");
  const chatId = env("CHATBOT_CHAT_ID", "");
  // A bot without a token or a target chat can only misbehave; refuse to load
  // instead of falling back to some inherited chat id.
  if (!tokenFile) throw new Error("CHATBOT_TOKEN_FILE is not set");
  if (!chatId) throw new Error("CHATBOT_CHAT_ID is not set");

  // Owner-authored message ids of the CURRENT turn: filled from every pushed
  // batch (`owner_msgs`), cleared on turn_end. The `subagent` tool demands one
  // of them, so a permission can never outlive the message that granted it and
  // nobody else's message can open dispatch.
  let ownerMsgs = new Set();

  // Incoming message ids of the CURRENT turn: filled from every pushed batch
  // (`batch_ids`), cleared on turn_end. `chat_send` accepts `reply_to` only from
  // this set, so a model can never quote the 50-message history window, the GIF
  // catalog or a message from an earlier turn. An empty set (old daemon) keeps
  // the previous unchecked behaviour.
  let batchIds = new Set();


  // Delivery ids admitted by drain and served by the current turn. The daemon
  // serializes batches (one turn at a time), so this normally holds exactly one.
  let activeDeliveries = [];
  // (reply_to, text, gif, sticker, media) -> receipt for the current turn
  // (cross-turn replay is served from the receipts file below).
  let turnSends = new Map();
  // Media attachments already sent in this turn: the hard "не перебарщивать"
  // bound (one per turn), reset on both turn boundaries.
  let turnMediaCount = 0;

  function emit(ev) {
    try {
      fs.appendFileSync(
        eventsPath,
        JSON.stringify({ t: Date.now(), ...ev }) + "\n",
      );
    } catch {
      // Events are observability only; never break the turn for them.
    }
  }

  function readToken() {
    if (!tokenFile) throw new Error("CHATBOT_TOKEN_FILE is not set");
    return fs.readFileSync(tokenFile, "utf8").trim();
  }

  async function botApi(method, params) {
    const url = `${apiBase}/bot${readToken()}/${method}`;
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params),
    });
    let data = null;
    try {
      data = await res.json();
    } catch {
      throw new Error(
        `Bot API ${method}: HTTP ${res.status} (unparseable body)`,
      );
    }
    if (!data || data.ok !== true) {
      throw new Error(
        `Bot API ${method}: ${JSON.stringify(data).slice(0, 300)}`,
      );
    }
    return data.result;
  }

  function loadReceipts() {
    try {
      const raw = JSON.parse(fs.readFileSync(receiptsPath, "utf8"));
      if (raw && typeof raw === "object") return raw;
    } catch {
      // Missing or corrupt: start empty (a corrupt cache must not block sends).
    }
    return {};
  }

  function saveReceipt(key, receipt) {
    const all = loadReceipts();
    all[key] = receipt;
    try {
      fs.writeFileSync(receiptsPath, JSON.stringify(all));
    } catch {
      // Cache write failure must not fail the send that already happened.
    }
  }

  function receiptKey(
    deliveryId,
    replyTo,
    text,
    gifFileId,
    stickerFileId,
    mediaPath,
  ) {
    return `${deliveryId}::${replyTo === null || replyTo === undefined ? "" : replyTo}::${text}::${gifFileId || ""}::${stickerFileId || ""}::${mediaPath || ""}`;
  }

  // ---- media upload (Attachment 3) ----
  async function botApiForm(method, form) {
    const url = `${apiBase}/bot${readToken()}/${method}`;
    const res = await fetch(url, { method: "POST", body: form });
    let data = null;
    try {
      data = await res.json();
    } catch {
      throw new Error(
        `Bot API ${method}: HTTP ${res.status} (unparseable body)`,
      );
    }
    if (!data || data.ok !== true) {
      throw new Error(
        `Bot API ${method}: ${JSON.stringify(data).slice(0, 300)}`,
      );
    }
    return data.result;
  }

  async function sendMediaFile(filePath, caption, replyTo) {
    const spec = mediaSpec(filePath);
    const buf = fs.readFileSync(filePath);
    const form = new FormData();
    form.append("chat_id", chatId);
    form.append(spec.field, new Blob([buf], { type: spec.mime }), path.basename(filePath));
    const cap = String(caption || "").slice(0, 1024);
    if (cap.trim()) form.append("caption", cap);
    if (replyTo !== null) form.append("reply_to_message_id", String(replyTo));
    return botApiForm(spec.method, form);
  }

  // Typing ownership: shown only while the agent is actually working, never
  // blindly during daemon waits. Stopped on every turn/agent/session end so
  // no timer outlives the work it signals.
  let typingTimer = null;
  function startTyping() {
    if (typingTimer) return;
    const tick = () => {
      try {
        botApi("sendChatAction", { chat_id: chatId, action: "typing" }).catch(() => {});
      } catch {
        // Token missing/unreadable: typing is best-effort only, never fail a turn.
      }
    };
    tick();
    typingTimer = setInterval(tick, 4000);
  }
  function stopTyping() {
    if (typingTimer) {
      clearInterval(typingTimer);
      typingTimer = null;
    }
  }

  pi.registerTool({
    name: "chat_send",
    label: "Send chat message",
    description:
      "The ONLY way to send a message to the Telegram chat. Call once per message " +
      "with the reply target and AT MOST ONE media argument per turn: a chat GIF " +
      "(gif_file_id), a sticker by file_id (sticker_file_id, no caption), or a " +
      "local media file (media_path from a batch [file] marker). A turn that " +
      "calls no chat_send sends nothing (silence). Never emit chat text outside " +
      "this tool.",
    parameters: {
      type: "object",
      properties: {
        text: {
          type: "string",
          description:
            "Message text (caption when gif_file_id or media_path is set; a " +
            "sticker cannot carry a caption — send the text separately).",
        },
        reply_to: {
          type: "integer",
          description:
            "message_id to reply to — MUST be one of this batch's [id] markers; " +
            "ids from the history block or the GIF catalog are not reply targets.",
        },
        gif_file_id: {
          type: "string",
          description: "file_id of a chat GIF from the batch GIF catalog.",
        },
        sticker_file_id: {
          type: "string",
          description:
            "file_id of a sticker from a batch [sticker] line; sent with " +
            "sendSticker, so it takes no caption.",
        },
        media_path: {
          type: "string",
          description:
            "Absolute path of a local media file from a batch [file] marker " +
            "(.gif -> sendAnimation, photo -> sendPhoto, video -> sendVideo, " +
            "otherwise sendDocument).",
        },
      },
      additionalProperties: false,
    },
    execute: async (toolCallId, params) => {
      // The daemon no longer parses markers: strip any stray `[reply:N]` /
      // `[gif:N]` markers so they never leak into the chat verbatim.
      let text = String(params.text || "")
        .replace(/^\s*\[reply:\d+\]\s*/, "")
        .replace(/\[gif:\d+\]/g, "")
        .trim();
      const requestedReplyTo =
        params.reply_to === undefined || params.reply_to === null
          ? null
          : Number(params.reply_to);
      let replyTo = requestedReplyTo;
      let replyDropped = false;
      if (
        replyTo !== null &&
        batchIds.size > 0 &&
        !batchIds.has(replyTo)
      ) {
        // A quote outside this batch is exactly the "ancient message" failure:
        // drop the quote but keep the answer (exactly-once send is preserved and
        // the model is told what happened).
        emit({
          kind: "chat_send_reply_dropped",
          toolCallId,
          reply_to: replyTo,
          valid: [...batchIds],
        });
        replyDropped = true;
        replyTo = null;
      }
      const dropNote = replyDropped
        ? `\nsent without quote: reply_to ${requestedReplyTo} is not in this batch (valid: ${[
            ...batchIds,
          ].join(", ")})`
        : "";
      const gifFileId = params.gif_file_id ? String(params.gif_file_id) : "";
      const stickerFileId = params.sticker_file_id
        ? String(params.sticker_file_id)
        : "";
      const mediaPath = params.media_path ? String(params.media_path) : "";
      const classified = classifyChatSend({ ...params, text });
      if (classified.error) {
        return {
          content: [{ type: "text", text: classified.error }],
          isError: true,
        };
      }
      if (!text.trim() && !gifFileId && !stickerFileId && !mediaPath) {
        return {
          content: [{ type: "text", text: "error: empty body, nothing sent" }],
          isError: true,
        };
      }
      if (text === SKIP_TOKEN) {
        return {
          content: [
            { type: "text", text: "silence acknowledged, nothing sent" },
          ],
        };
      }
      const turnKey = `${replyTo === null ? "" : replyTo}::${text}::${gifFileId}::${stickerFileId}::${mediaPath}`;
      if (turnSends.has(turnKey)) {
        return {
          content: [
            { type: "text", text: JSON.stringify(turnSends.get(turnKey)) },
          ],
        };
      }
      // Cross-turn replay: same delivery re-admitted after a session restart
      // must not re-send. The daemon serializes batches, so activeDeliveries
      // identifies this turn's batch.
      const cached = loadReceipts();
      for (const id of activeDeliveries) {
        const hit = cached[
          receiptKey(id, replyTo, text, gifFileId, stickerFileId, mediaPath)
        ];
        if (hit) {
          turnSends.set(turnKey, hit);
          emit({
            kind: "chat_send_replay",
            toolCallId,
            delivery_id: id,
            receipt: hit,
          });
          return {
            content: [{ type: "text", text: JSON.stringify(hit) + dropNote }],
          };
        }
      }
      const isMediaSend = Boolean(gifFileId || stickerFileId || mediaPath);
      if (isMediaSend && !allowsAnotherMedia(turnMediaCount)) {
        return {
          content: [{ type: "text", text: TURN_MEDIA_ERROR }],
          isError: true,
        };
      }
      let sent;
      if (gifFileId) {
        const p = { chat_id: chatId, animation: gifFileId };
        if (text.trim()) p.caption = text.slice(0, 1024);
        if (replyTo !== null) p.reply_to_message_id = replyTo;
        sent = await botApi("sendAnimation", p);
      } else if (stickerFileId) {
        // sendSticker takes no caption: classifyChatSend already refused a
        // sticker call that carried text, so nothing is dropped here.
        const p = { chat_id: chatId, sticker: stickerFileId };
        if (replyTo !== null) p.reply_to_message_id = replyTo;
        sent = await botApi("sendSticker", p);
      } else if (mediaPath) {
        let stat = null;
        try {
          stat = fs.statSync(mediaPath);
        } catch {
          return {
            content: [
              { type: "text", text: `error: media file not found: ${mediaPath}` },
            ],
            isError: true,
          };
        }
        if (!stat.isFile()) {
          return {
            content: [
              { type: "text", text: `error: not a regular file: ${mediaPath}` },
            ],
            isError: true,
          };
        }
        if (stat.size > MEDIA_MAX_BYTES) {
          return {
            content: [
              {
                type: "text",
                text: `error: file is ${Math.round(stat.size / 1048576)} MB, over the 45 MB send limit`,
              },
            ],
            isError: true,
          };
        }
        sent = await sendMediaFile(mediaPath, text, replyTo);
      } else {
        const p = { chat_id: chatId, text: text.slice(0, 4000) };
        if (replyTo !== null) p.reply_to_message_id = replyTo;
        sent = await botApi("sendMessage", p);
      }
      const receipt = { message_id: sent.message_id, reply_to: replyTo, text };
      if (isMediaSend) turnMediaCount += 1;
      turnSends.set(turnKey, receipt);
      for (const id of activeDeliveries)
        saveReceipt(
          receiptKey(id, replyTo, text, gifFileId, stickerFileId, mediaPath),
          receipt,
        );
      emit({ kind: "chat_send_result", toolCallId, receipt });
      return {
        content: [{ type: "text", text: JSON.stringify(receipt) + dropNote }],
      };
    },
  });

  pi.registerTool({
    name: "gif_list",
    label: "List chat GIFs",
    description:
      "Full deduplicated list of GIFs seen in this chat (file_id, caption, and " +
      "the moment it was used), ordered newest first, served by the bot daemon. " +
      "Only real animations — videos from the chat are not listed. The batch " +
      "catalog is capped; use this when the needed GIF is not in it. Send one " +
      "with chat_send(gif_file_id=...).",
    parameters: {
      type: "object",
      properties: {
        offset: {
          type: "integer",
          description: "Skip this many GIFs (default 0).",
        },
        limit: {
          type: "integer",
          description: "How many to return (default 200, max 500).",
        },
      },
      additionalProperties: false,
    },
    execute: async (toolCallId, params) => {
      const offset = Number.isFinite(Number(params.offset))
        ? Math.max(0, Math.trunc(Number(params.offset)))
        : 0;
      const limit = Number.isFinite(Number(params.limit))
        ? Math.min(500, Math.max(1, Math.trunc(Number(params.limit))))
        : 200;
      try {
        const text = await controlCall("gif_list", { offset, limit }, 5000);
        return { content: [{ type: "text", text }] };
      } catch (e) {
        emit({ kind: "gif_list_error", error: String(e).slice(0, 200) });
        return {
          content: [
            {
              type: "text",
              text: `error: ручка демона недоступна: ${String(e).slice(0, 200)}`,
            },
          ],
          isError: true,
        };
      }
    },
  });

  // ---- subagents (crystal M1) ----
  // ONE generic child role. The native `task` tool is absent from this session
  // and loading its plugin would pull memory/teams/rule injections into the
  // character, so a dispatch is a one-shot `omo -p` process with the `general`
  // persona appended. Hard bounds: 2 concurrent children, 210 s each (under the
  // daemon's 420 s turn budget), stdout capped, killed on session_shutdown.
  const MAX_SUBAGENTS = 2;
  const SUBAGENT_TIMEOUT_MS = 210_000;
  const SUBAGENT_OUT_CAP = 6800;
  const GENERAL_PERSONA = [
    "Ты — general-субагент бота-участника Telegram-чата.",
    "Тебе приходит одна задача от владельца бота: выполни её по существу и верни результат текстом.",
    "Пиши напрямую и по делу, без служебных преамбул. Если чего-то не смог — скажи честно, что именно",
    "и что проверил. Не отправляй сообщений в чат, не зови своих субагентов и не задавай вопросов через тулы.",
  ].join("\n");
  const subagentProcs = new Set();

  // The owner's name is configuration, never a literal: a clone sets its own
  // CHATBOT_OWNER_USERNAME and the shipped text follows it.
  const ownerLabel = (() => {
    const name = env("CHATBOT_OWNER_USERNAME", "");
    return name ? `@${name}` : "[владелец]";
  })();

  pi.registerTool({
    name: "subagent",
    label: "Run a subagent",
    description:
      "Delegate one self-contained task to a general subagent and get its text " +
      "result back. ALLOWED ONLY when the owner (" +
      ownerLabel +
      ") asked for it: " +
      "pass owner_message_id = the [id] of the owner's message marked [владелец]. " +
      "Without such a message the call is refused. Never call it on your own " +
      "initiative and never for ordinary chat replies.",
    parameters: {
      type: "object",
      properties: {
        task: {
          type: "string",
          description: "What the subagent must do, with all needed context.",
        },
        owner_message_id: {
          type: "integer",
          description:
            "message_id of the owner's message (marked [владелец]) that asked for this.",
        },
        label: {
          type: "string",
          description: "Short label for logs.",
        },
      },
      required: ["task", "owner_message_id"],
      additionalProperties: false,
    },
    execute: async (toolCallId, params) => {
      const denied = (why) => ({
        content: [{ type: "text", text: `error: ${why}` }],
        isError: true,
      });
      const wanted = Number(params.owner_message_id);
      if (!Number.isInteger(wanted) || !ownerMsgs.has(wanted)) {
        return denied(
          "subagent denied: pass owner_message_id of a message from the owner " +
            "([владелец]) in this batch. Dispatching is allowed only when " +
            ownerLabel +
            " asked for it.",
        );
      }
      const task = String(params.task || "").trim();
      if (!task) return denied("empty task, nothing dispatched");
      if (subagentProcs.size >= MAX_SUBAGENTS) {
        return denied(
          `too many subagents running (max ${MAX_SUBAGENTS}); wait for one to finish`,
        );
      }
      const model = env("CHATBOT_SUBAGENT_MODEL", "");
      const args = ["-p", "--no-session"];
      if (model) args.push("--model", model);
      // `--` so a task that looks like an @file or a flag stays a prompt.
      args.push("--append-system-prompt", GENERAL_PERSONA, "--", task);
      const started = Date.now();
      emit({
        kind: "subagent_start",
        toolCallId,
        label: String(params.label || "").slice(0, 80),
        owner_message_id: wanted,
      });
      const child = spawn("omo", args, {
        cwd: process.cwd(),
        env: process.env,
        stdio: ["ignore", "pipe", "pipe"],
      });
      subagentProcs.add(child);
      const out = [];
      let outLen = 0;
      let errTail = "";
      const collectErr = (chunk) => {
        errTail = (errTail + chunk).slice(-400);
      };
      return await new Promise((resolve) => {
        let done = false;
        let timer = null;
        const finish = (payload, isError) => {
          if (done) return;
          done = true;
          clearTimeout(timer);
          subagentProcs.delete(child);
          emit({
            kind: "subagent_end",
            toolCallId,
            ms: Date.now() - started,
            ok: !isError,
            chars: outLen,
          });
          if (!child.killed) {
            try {
              child.kill("SIGKILL");
            } catch {
              // Already gone: nothing to release.
            }
          }
          resolve({
            content: [{ type: "text", text: payload }],
            ...(isError ? { isError: true } : {}),
          });
        };
        timer = setTimeout(() => {
          finish(
            `error: subagent timed out after ${Math.round(SUBAGENT_TIMEOUT_MS / 1000)}s`,
            true,
          );
        }, SUBAGENT_TIMEOUT_MS);
        child.stdout.on("data", (chunk) => {
          if (outLen >= SUBAGENT_OUT_CAP) return;
          const s = String(chunk);
          outLen += s.length;
          out.push(s);
        });
        child.stderr.on("data", (chunk) => collectErr(String(chunk)));
        child.on("error", (e) => {
          finish(`error: cannot start omo: ${String(e).slice(0, 200)}`, true);
        });
        child.on("close", (code) => {
          const text = out.join("").trim();
          if (code === 0 && text) {
            finish(text.slice(0, 6500), false);
          } else if (code === 0) {
            finish("error: subagent returned no output", true);
          } else {
            finish(
              `error: subagent failed (rc=${code}): ${errTail.trim().slice(0, 280)}`,
              true,
            );
          }
        });
      });
    },
  });

  pi.on("turn_start", (ev) => {
    turnSends = new Map();
    turnMediaCount = 0;
    startTyping();
    emit({ kind: "turn_start", turnIndex: ev.turnIndex });
    emit({ kind: "agent_activity", phase: "working" });
  });

  pi.on("turn_end", (ev) => {
    stopTyping();
    emit({
      kind: "turn_end",
      turnIndex: ev.turnIndex,
      chat_sends: [...turnSends.values()],
    });
    activeDeliveries = [];
    turnMediaCount = 0;
    // A dispatch permission lives only for the turn whose batch carried the
    // owner's message: never leave it standing for a later turn.
    ownerMsgs = new Set();
    // Reply targets are likewise per-turn: the next turn's batch re-fills them.
    batchIds = new Set();
    emit({ kind: "agent_activity", phase: "stopped" });
  });

  // A run fully settled: the only signal the daemon accepts as "the agent
  // finished with this turn and chose silence".
  pi.on("agent_settled", () => {
    emit({ kind: "agent_settled" });
    reportDeliveryStates();
  });

  pi.on("agent_end", (ev) => {
    if (ev.aborted || ev.deadline) {
      stopTyping();
      emit({
        kind: "agent_end_aborted",
        aborted: Boolean(ev.aborted),
        abortSource: ev.abortSource || null,
        deadline: ev.deadline || null,
        willRetry: Boolean(ev.willRetry),
      });
      emit({ kind: "agent_activity", phase: "stopped" });
    }
  });

  // Agent-cycle liveness the daemon can see: every tool call/result proves
  // the agent is working (thoughts/actions in flight). The daemon extends
  // the turn budget on these and never murders a working session.
  pi.on("tool_call", (event) => {
    try {
      const name = event && event.toolName ? String(event.toolName) : "";
      emit({ kind: "agent_activity", phase: "working", tool: name || null });
    } catch {
      // Observability only; never break the agent loop.
    }
  });
  pi.on("tool_result", () => {
    try {
      emit({ kind: "agent_activity", phase: "working" });
    } catch {
      // Observability only; never break the agent loop.
    }
  });
  pi.on("session_shutdown", () => {
    stopTyping();
    for (const child of subagentProcs) {
      try {
        child.kill("SIGKILL");
      } catch {
        // Already exited: nothing to release.
      }
    }
    subagentProcs.clear();
    closeControl();
  });

  // Compaction rewrites the transcript: an admitted-but-unprocessed steer
  // vanishes with it, so the daemon must re-deliver. Rejected compactions
  // leave the context intact and need no re-delivery.
  pi.on("session_compact", (ev) => {
    try {
      if (ev && ev.accepted === false) return;
      if (activeDeliveries.length) {
        emit({
          kind: "compacted",
          delivery_ids: [...activeDeliveries],
          requestId: (ev && ev.requestId) || null,
          reason: (ev && ev.reason) || null,
        });
      }
    } catch {
      // Observability only; the daemon's turn_end timeout is the fallback.
    }
  });

  // Kinds that mean the runtime took the text. Everything else enqueued
  // nothing and must be retried, never reported as delivered.
  const ACCEPTED_KINDS = new Set([
    "started",
    "steered",
    "queued",
    "already_admitted",
  ]);
  // Deliveries this process admitted, and the (id,state) pairs already mirrored
  // into events (the runtime ledger is process-lifetime, so repeats are noise).
  const trackedDeliveries = new Set();
  const reportedStates = new Set();

  function currentTurnEpoch() {
    try {
      const gate = pi.session.admissionGate();
      if (gate && gate.turn_epoch !== undefined && gate.turn_epoch !== null) {
        return gate.turn_epoch;
      }
    } catch {
      // Runtime not bound yet; the admit call reports its own refusal.
    }
    return undefined;
  }

  // Delivery proof the daemon closes on: `emitted` means the transcript entry
  // (custom `session_control_delivery` with details.delivery_id) exists, so the
  // agent really has the text in context. `failed` means the session file
  // refused it and it must be retried.
  function reportDeliveryStates() {
    if (!trackedDeliveries.size) return;
    let ledger = null;
    try {
      ledger = pi.session.listAdmittedDeliveries();
    } catch {
      return;
    }
    if (!ledger) return;
    const mark = (id, state, extra) => {
      const key = `${id}:${state}`;
      if (reportedStates.has(key)) return;
      reportedStates.add(key);
      emit({
        kind: "delivery_state",
        delivery_id: String(id),
        state,
        ...(extra || {}),
      });
    };
    for (const id of ledger.emitted || []) {
      if (trackedDeliveries.has(String(id))) mark(id, "emitted");
    }
    for (const f of ledger.failed || []) {
      const id = String((f && f.delivery_id) || "");
      if (id && trackedDeliveries.has(id)) {
        mark(id, "failed", {
          error: String((f && f.error) || "").slice(0, 200),
        });
      }
    }
  }

  // Transport: a unix socket owned by the daemon — no files anywhere. The
  // daemon writes one JSON object per line; every push is answered with a
  // result line so the daemon can close accepted batches and retry refusals.
  let controlSock = null;
  let controlBuf = "";
  let controlRetry = null;
  let controlAttempt = 0;
  // A session replacement/reload leaves this extension instance with a stale
  // ctx: every admit then throws StaleExtensionContextError. Such an instance
  // is dead for delivery, so it must STOP answering and release the socket —
  // the live instance (it gets its own session_start) then becomes the one the
  // daemon serves. Re-armed only by a fresh session_start.
  let controlDead = false;

  function connectControl() {
    if (controlSock || controlRetry || controlDead) return;
    const sock = net.connect({ path: controlSockPath });
    sock.setEncoding("utf8");
    sock.on("connect", () => {
      controlSock = sock;
      controlBuf = "";
      controlAttempt = 0;
      sock.write(
        JSON.stringify({
          hello: { session: env("CHATBOT_SESSION", ""), pid: process.pid },
        }) + "\n",
      );
      emit({ kind: "control_connected", path: controlSockPath });
    });
    sock.on("data", (chunk) => {
      controlBuf += chunk;
      let i = controlBuf.indexOf("\n");
      while (i >= 0) {
        const line = controlBuf.slice(0, i);
        controlBuf = controlBuf.slice(i + 1);
        if (line.trim()) {
          try {
            handleControlLine(JSON.parse(line));
          } catch {
            emit({ kind: "control_parse_error", line: line.slice(0, 200) });
          }
        }
        i = controlBuf.indexOf("\n");
      }
    });
    sock.on("error", () => {
      try {
        sock.destroy();
      } catch {
        // The close handler schedules the reconnect.
      }
    });
    sock.on("close", () => {
      if (controlSock === sock) controlSock = null;
      scheduleControlReconnect();
    });
  }

  function scheduleControlReconnect() {
    if (controlRetry) return;
    const delay = Math.min(2000, 100 * Math.pow(2, controlAttempt));
    controlAttempt = Math.min(controlAttempt + 1, 5);
    controlRetry = setTimeout(() => {
      controlRetry = null;
      connectControl();
    }, delay);
  }

  function closeControl() {
    if (controlRetry) {
      clearTimeout(controlRetry);
      controlRetry = null;
    }
    if (controlSock) {
      const s = controlSock;
      controlSock = null;
      try {
        s.destroy();
      } catch {
        // Teardown must not throw.
      }
    }
  }

  // Request/response on the daemon's control socket: the extension asks for
  // data it must not read itself (sqlite). One outstanding entry per call id;
  // a missing answer fails the call instead of hanging the turn.
  const pendingCalls = new Map();
  let callSeq = 0;

  function controlCall(op, args, timeoutMs) {
    return new Promise((resolve, reject) => {
      if (!controlSock) {
        reject(new Error("нет связи с демоном (control.sock)"));
        return;
      }
      const id = `c${++callSeq}-${Date.now()}`;
      const timer = setTimeout(() => {
        pendingCalls.delete(id);
        reject(
          new Error(
            `ручка демона не ответила за ${Math.max(1, Math.round(timeoutMs / 1000))} с`,
          ),
        );
      }, timeoutMs);
      pendingCalls.set(id, { resolve, reject, timer });
      try {
        controlSock.write(JSON.stringify({ call: { id, op, args } }) + "\n");
      } catch (e) {
        clearTimeout(timer);
        pendingCalls.delete(id);
        reject(new Error(`control write failed: ${String(e).slice(0, 160)}`));
      }
    });
  }

  function handleControlLine(msg) {
    if (!msg) return;
    if (msg.return) {
      const r = msg.return;
      const wait = pendingCalls.get(String(r.id || ""));
      if (!wait) return;
      pendingCalls.delete(String(r.id || ""));
      clearTimeout(wait.timer);
      if (r.ok) wait.resolve(String(r.text || ""));
      else wait.reject(new Error(String(r.error || "control call failed")));
      return;
    }
    const push = msg.push;
    if (!push) return;
    const deliveryId = String(push.delivery_id || "");
    const text = String(push.text || "");
    if (!deliveryId || !text) return;
    // Owner-authored message ids travel with the batch. They are the ONLY
    // proof the `subagent` tool accepts; a batch without them opens nothing.
    for (const id of Array.isArray(push.owner_msgs) ? push.owner_msgs : []) {
      const n = Number(id);
      if (Number.isInteger(n) && n > 0) ownerMsgs.add(n);
    }
    // Incoming ids of this batch are the only legal `reply_to` targets for the
    // turn. A steer batch in the same turn extends them; turn_end clears them.
    for (const id of Array.isArray(push.batch_ids) ? push.batch_ids : []) {
      const n = Number(id);
      if (Number.isInteger(n) && n > 0) batchIds.add(n);
    }
    const kind = admitDelivery(deliveryId, text, push.deliverAs || "steer");
    if (!controlSock) return;
    try {
      controlSock.write(
        JSON.stringify({ result: { delivery_id: deliveryId, kind } }) + "\n",
      );
    } catch {
      // A lost answer is recovered by the daemon's retry clock.
    }
  }

  // Returns the admission kind: the daemon closes a batch only on acceptance and
  // retries on a refusal. The epoch is what lets a steer land in a running turn
  // instead of being refused with `turn_conflict`.
  function admitDelivery(deliveryId, text, deliverAs) {
    const params = {
      delivery_id: deliveryId,
      text,
      deliverAs,
      sender: { kind: "external", platform: "telegram" },
    };
    if (deliverAs === "steer") {
      const epoch = currentTurnEpoch();
      if (epoch !== undefined) params.expected_turn_id = epoch;
    }
    try {
      const res = pi.session.admitExternalMessage(params);
      trackedDeliveries.add(deliveryId);
      if (ACCEPTED_KINDS.has(res.kind)) {
        if (!activeDeliveries.includes(deliveryId)) {
          activeDeliveries.push(deliveryId);
        }
        emit({ kind: "admitted", delivery_id: deliveryId, admission: res.kind });
      } else {
        emit({
          kind: "admit_deferred",
          delivery_id: deliveryId,
          admission: res.kind,
        });
      }
      reportDeliveryStates();
      return res.kind;
    } catch (e) {
      const text = String(e);
      const stale =
        text.includes("StaleExtensionContextError") ||
        text.includes("stale after session replacement");
      emit({
        kind: "admit_error",
        delivery_id: deliveryId,
        stale,
        error: text.slice(0, 200),
      });
      if (stale) {
        // This instance can never admit again: disconnect and stay quiet so a
        // live instance can take the socket. The daemon retries the batch.
        controlDead = true;
        emit({ kind: "control_stale", delivery_id: deliveryId });
        closeControl();
      }
      return "error";
    }
  }

  connectControl();
  pi.on("session_start", () => {
    // A fresh session may re-bind this instance: it is usable again.
    controlDead = false;
    connectControl();
  });
}
