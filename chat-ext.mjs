/**
 * chat-ext: native omo extension driving the chat-bot host session.
 *
 * - Registers the SOLE send path `chat_send`; a turn that calls no chat_send
 *   sends nothing (silence, equivalent to `[skip]`).
 * - Transport: the daemon hosts a unix socket at `<state>/control.sock`. The
 *   daemon owns the Telegram token and every Bot API call. This file never
 *   reads the token, never builds a Bot API URL and never calls fetch.
 * - `chat_send`, `typing` and `gif_list` are RPC ops on that socket. The daemon
 *   decides message parts (sticker + text is two messages), mints the answer in
 *   its journal, writes the durable send intent before HTTP, and replays the
 *   stored receipt for a repeated call with the same arguments.
 * - A push carries the batch text, incoming ids and forum topic. The
 *   extension admits it via `admitExternalMessage` and answers
 *   `{"result":{"delivery_id","kind"}}`; the daemon retries a refusal.
 * - Delivery proof: `listAdmittedDeliveries()` is the only honest source for
 *   "the agent has it in context" (`emitted`) or "the session file refused it"
 *   (`failed`); both are mirrored into events as `delivery_state`.
 * - Events for the daemon land in `<state>/events.jsonl` (`turn_start`,
 *   `turn_end`, `admitted`, `admit_error`, `agent_end_aborted`, `compacted`).
 * - `subagent` dispatches a one-shot `omo -p` process carrying the `general`
 *   persona, allowed as the persona text describes.
 */

import { spawn } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";

const SKIP_TOKEN = "[skip]";

// Pure argument shape for the daemon RPC. Kept exported so the send contract is
// testable without an omo runtime; it contains no URL and no token.
export function sendArgs(params, batchKey, threadId) {
  const p = params || {};
  const out = { batch_key: String(batchKey || "") };
  if (p.text !== undefined && p.text !== null) out.text = String(p.text);
  if (p.reply_to !== undefined && p.reply_to !== null)
    out.reply_to = Number(p.reply_to);
  if (p.gif_file_id) out.gif_file_id = String(p.gif_file_id);
  if (p.sticker_file_id) out.sticker_file_id = String(p.sticker_file_id);
  if (p.media_path) out.media_path = String(p.media_path);
  if (threadId !== undefined && threadId !== null)
    out.thread_id = Number(threadId);
  return out;
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

// The handshake names the durable session and its generation, never the
// configured session name. Exported so the frame shape is testable without a
// running runtime.
export function helloFrame(uid, generation, pid) {
  return {
    hello: {
      session: String(uid || ""),
      generation: Number(generation || 0),
      pid: Number(pid || 0),
    },
  };
}

// A stale extension instance is dead for delivery: it must not reconnect and
// re-arms only through a fresh session start.
export function staleGate() {
  let dead = false;
  return {
    isDead: () => dead,
    markDead: () => {
      dead = true;
    },
    rearm: () => {
      dead = false;
    },
  };
}

/**
 * Media the OpenAI-shaped gateway refuses inside a tool result.
 *
 * The installed runtime serializes a tool result as
 *   {type:"function_call_output", call_id, output:[{type:"input_image",
 *    detail:"auto", image_url:"data:<mime>;base64,..."}]}
 * and that position accepts raster images only. The builtin `read_video` tool
 * (registered whenever the model declares the `video` modality) puts an mp4
 * there as `image_url`, the gateway answers 400 `unsupported media type`, and
 * because the block stays in the transcript EVERY later request fails on the
 * same history index. Rewriting those parts to text keeps the video modality
 * useful everywhere the gateway does accept it and cannot wedge the session.
 */
const BLOCKED_MEDIA_RE = /^(video|audio)\//i;
const MEDIA_ERROR_RE =
  /unsupported media type|image_parse_error|invalid_image|does not represent a valid image/i;
const MAX_WALK_DEPTH = 32;

function mediaMime(part) {
  if (!part || typeof part !== "object") return "";
  const source = part.source && typeof part.source === "object" ? part.source : null;
  const raw =
    part.media_type ||
    part.mediaType ||
    part.mimeType ||
    (source && (source.media_type || source.mediaType || source.mimeType)) ||
    part.image_url ||
    "";
  const text = typeof raw === "string" ? raw : "";
  const dataUrl = /^data:([^;,]+)[;,]/.exec(text);
  if (dataUrl) return dataUrl[1].trim().toLowerCase();
  return /^[a-z]+\/[a-z0-9.+-]+$/i.test(text) ? text.trim().toLowerCase() : "";
}

function blockedPlaceholder(part) {
  const mime = mediaMime(part) || "media";
  const text = `[медиа ${mime} опущено: гейтвей принимает здесь только JPEG/PNG/GIF/WebP/ICO]`;
  // Anthropic-shaped parts carry `{type:"image", source:{media_type}}`; the
  // OpenAI-shaped ones carry `{type:"input_image"}`. Each family keeps its own
  // text-part spelling so the payload stays a valid request.
  const anthropic = part && part.type === "image";
  return anthropic ? { type: "text", text } : { type: "input_text", text };
}

function collectsToolResults(node) {
  if (!node || typeof node !== "object") return false;
  if (node.type === "function_call_output" || node.type === "tool_result") return true;
  return node.role === "tool" || node.role === "function";
}

function rewriteParts(node, inToolResult, depth, stats) {
  if (depth > MAX_WALK_DEPTH || node === null || typeof node !== "object") return node;
  if (Array.isArray(node)) {
    let changed = false;
    const next = node.map((item) => {
      const replaced = rewriteParts(item, inToolResult, depth + 1, stats);
      if (replaced !== item) changed = true;
      return replaced;
    });
    return changed ? next : node;
  }
  if (inToolResult && BLOCKED_MEDIA_RE.test(mediaMime(node))) {
    stats.removed += 1;
    stats.mimes.add(mediaMime(node));
    return blockedPlaceholder(node);
  }
  const childInTool = inToolResult || collectsToolResults(node);
  let changed = false;
  const next = {};
  for (const [key, value] of Object.entries(node)) {
    const replaced = rewriteParts(value, childInTool, depth + 1, stats);
    if (replaced !== value) changed = true;
    next[key] = replaced;
  }
  return changed ? next : node;
}

/**
 * Strip video/audio parts that sit inside a tool result, leaving everything
 * else (in particular user-message media) untouched. Returns the original
 * payload object when nothing had to change, so an untouched request is sent
 * byte-identically.
 */
export function sanitizePayload(payload) {
  const stats = { removed: 0, mimes: new Set() };
  const rewritten = rewriteParts(payload, false, 0, stats);
  return {
    changed: rewritten !== payload,
    payload: rewritten,
    removed: stats.removed,
    mimes: [...stats.mimes],
  };
}

/**
 * A run that ended because the provider failed is NOT the model choosing
 * silence: the batch has to be retried, never closed as `skipped`. Mirrors the
 * runtime's own terminal predicate: an assistant message with `stopReason:
 * "error"`, or an abort the provider/system caused (a user abort is not a
 * failure). Retryable ends (`willRetry`) are excluded by the caller.
 */
export function terminalProviderError(ev) {
  if (!ev || typeof ev !== "object") return "";
  const messages = Array.isArray(ev.messages) ? ev.messages : [];
  for (let i = messages.length - 1; i >= 0; i -= 1) {
    const raw = messages[i];
    const msg = raw && raw.message && typeof raw.message === "object" ? raw.message : raw;
    if (!msg || typeof msg !== "object") continue;
    if (msg.role && msg.role !== "assistant") continue;
    if (msg.stopReason === "error") return String(msg.errorMessage || "provider error");
    if (msg.stopReason === "aborted") {
      const source = msg.abortSource || ev.abortSource;
      if (source && source !== "user") return `aborted by ${source}`;
    }
  }
  if (ev.aborted && ev.abortSource && ev.abortSource !== "user") return `aborted by ${ev.abortSource}`;
  return "";
}

export default function (pi) {
  const stateDir = env("CHATBOT_STATE_DIR", "./state");  const controlSockPath = path.join(stateDir, "control.sock");
  const eventsPath = path.join(stateDir, "events.jsonl");

  // Batches admitted in this turn: key, incoming ids and forum topic. The
  // daemon validates reply targets and owns message_thread_id, but choosing
  // WHICH batch an answer belongs to needs the ticket the push carried.
  let activeBatches = [];

  // Delivery ids admitted by drain and served by the current turn. The daemon
  // serializes batches (one turn at a time), so this normally holds exactly one.
  let activeDeliveries = [];
  // argument fingerprint -> raw daemon response for the current turn.
  let turnSends = new Map();

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

  function chooseBatch(replyTo) {
    if (!activeBatches.length) return null;
    const wanted = Number(replyTo);
    if (Number.isInteger(wanted)) {
      for (let i = activeBatches.length - 1; i >= 0; i--) {
        if (activeBatches[i].ids.includes(wanted)) return activeBatches[i];
      }
    }
    return activeBatches[activeBatches.length - 1];
  }

  pi.registerTool({
    name: "chat_send",
    label: "Send chat message",
    description:
      "The ONLY way to send a message to the Telegram chat. Call once per message. " +
      "The daemon owns the Bot API. A turn that calls no chat_send sends nothing. " +
      "A repeat with identical arguments returns the same receipt without sending " +
      "again; a different second answer in the same turn is refused. A sticker " +
      "plus text is delivered as TWO messages. At most one media argument per call.",
    parameters: {
      type: "object",
      properties: {
        text: {
          type: "string",
          description:
            "Message text (caption when gif_file_id or media_path is set). With " +
            "sticker_file_id the text is sent as its own message.",
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
          description: "file_id of a sticker from a batch [sticker] line.",
        },
        media_path: {
          type: "string",
          description:
            "Absolute path of a local media file from a batch [file] marker " +
            "(.gif -> animation, image -> photo, video -> video, else document).",
        },
      },
      additionalProperties: false,
    },
    execute: async (toolCallId, params) => {
      const deny = (why) => ({
        content: [{ type: "text", text: `error: ${why}` }],
        isError: true,
      });
      const text = String(params.text || "")
        .replace(/^\s*\[reply:\d+\]\s*/, "")
        .replace(/\[gif:\d+\]/g, "")
        .trim();
      const batch = chooseBatch(params.reply_to);
      if (!batch) {
        return deny("нет активной пачки — chat_send вызывается только в ходе с доставленной пачкой");
      }
      const args = sendArgs({ ...params, text }, batch.key, batch.thread);
      if (!text && !args.gif_file_id && !args.sticker_file_id && !args.media_path) {
        return deny("empty body, nothing sent");
      }
      if (text === SKIP_TOKEN) {
        return {
          content: [{ type: "text", text: "silence acknowledged, nothing sent" }],
        };
      }
      const key = JSON.stringify(args);
      if (turnSends.has(key)) {
        return { content: [{ type: "text", text: turnSends.get(key) }] };
      }
      try {
        const raw = await controlCall("chat_send", args, 90000);
        turnSends.set(key, raw);
        let receipt = null;
        try {
          receipt = JSON.parse(raw);
        } catch {
          // The daemon's text is the contract; a non-JSON note is still valid.
        }
        emit({ kind: "chat_send_result", toolCallId, receipt });
        return { content: [{ type: "text", text: raw }] };
      } catch (e) {
        const msg = String(e).slice(0, 400);
        emit({ kind: "chat_send_error", toolCallId, error: msg });
        return deny(msg);
      }
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
          description: "Skip this many GIFs (default 0). Negative is an error.",
        },
        limit: {
          type: "integer",
          description: "How many to return (default 30, max 200). Out-of-range is an error.",
        },
      },
      additionalProperties: false,
    },
    execute: async (toolCallId, params) => {
      // Bounds are the daemon's contract: pass them through unchanged so a bad
      // offset/limit comes back as `bad offset` / `bad limit`, never clamped.
      const args = {};
      if (params.offset !== undefined && params.offset !== null)
        args.offset = params.offset;
      if (params.limit !== undefined && params.limit !== null)
        args.limit = params.limit;
      try {
        const text = await controlCall("gif_list", args, 8000);
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

  // ---- subagents ----
  // ONE generic child role. The native `task` tool is absent from this session
  // and loading its plugin would pull memory/teams/rule injections into the
  // character, so a dispatch is a one-shot `omo -p` process with the `general`
  // persona appended. The owner-only rule lives in the persona text; this tool
  // enforces no behavioural gate of its own.
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
      ") asked for it, as the persona text describes. Never call it on your own " +
      "initiative and never for ordinary chat replies.",
    parameters: {
      type: "object",
      properties: {
        task: {
          type: "string",
          description: "What the subagent must do, with all needed context.",
        },
        label: {
          type: "string",
          description: "Short label for logs.",
        },
      },
      required: ["task"],
      additionalProperties: false,
    },
    execute: async (toolCallId, params) => {
      const denied = (why) => ({
        content: [{ type: "text", text: `error: ${why}` }],
        isError: true,
      });
      const task = String(params.task || "").trim();
      if (!task) return denied("empty task, nothing dispatched");
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
    emit({ kind: "turn_start", turnIndex: ev.turnIndex });
    emit({ kind: "agent_activity", phase: "working" });
  });

  pi.on("turn_end", (ev) => {
    // The indicator belongs to the RUN, not the turn: a run is a chain of turns
    // and stopping here is what made it vanish while the agent still worked.
    emit({
      kind: "turn_end",
      turnIndex: ev.turnIndex,
      chat_sends: [...turnSends.values()].map((raw) => {
        try {
          return JSON.parse(raw);
        } catch {
          return null;
        }
      }),
    });
    activeDeliveries = [];
    activeBatches = [];
  });

  // A run fully settled: the only signal the daemon accepts as "the agent
  // finished with this turn and chose silence".
  pi.on("agent_settled", () => {
    emit({ kind: "agent_settled" });
    // The run is over: this is the "agent stopped" hook the daemon polls for.
    emit({ kind: "agent_activity", phase: "stopped" });
    reportDeliveryStates();
  });

  // Last chance to keep an unwedgeable request: the provider payload is built
  // by now, so a media part the gateway rejects inside a tool result is removed
  // here instead of failing the turn and poisoning the session for good.
  pi.on("before_provider_request", (event) => {
    try {
      const result = sanitizePayload(event && event.payload);
      if (result.removed) {
        emit({ kind: "payload_sanitized", removed: result.removed, mimes: result.mimes });
      }
      return result.changed ? result.payload : undefined;
    } catch (e) {
      emit({ kind: "payload_sanitize_error", error: String(e).slice(0, 200) });
      return undefined;
    }
  });

  pi.on("agent_end", (ev) => {
    // A run the provider terminated is a failed delivery, not the model picking
    // silence. `willRetry` means the runtime will re-ask by itself; only the
    // terminal end carries the decision the daemon needs.
    try {
      if (ev && !ev.willRetry) {
        const error = terminalProviderError(ev);
        if (error) {
          emit({
            kind: "run_failed",
            error: error.slice(0, 400),
            media_rejected: MEDIA_ERROR_RE.test(error),
            abort_source: (ev.abortSource || null) && String(ev.abortSource),
          });
        }
      }
    } catch {
      // Observability only; never break the agent loop.
    }
    if (ev.aborted || ev.deadline) {
      // A retry is a continuation of the same run; only a terminal abort ends it.
      emit({
        kind: "agent_end_aborted",
        aborted: Boolean(ev.aborted),
        abortSource: ev.abortSource || null,
        deadline: ev.deadline || null,
        willRetry: Boolean(ev.willRetry),
      });
      // Only a terminal end means "the agent stopped"; a retry is the same run
      // still working, and the indicator must keep polling through it.
      if (!ev.willRetry) emit({ kind: "agent_activity", phase: "stopped" });
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
  const gate = staleGate();
  // Durable identity learned from the daemon's `session` frame. Until the
  // daemon answers, hello carries an empty uid and the daemon fills it in.
  let sessionIdentity = { uid: "", generation: 0 };

  function sendHello() {
    if (!controlSock) return;
    try {
      controlSock.write(
        JSON.stringify(
          helloFrame(sessionIdentity.uid, sessionIdentity.generation, process.pid),
        ) + "\n",
      );
    } catch {
      // The close handler owns reconnection; a write failure is not fatal.
    }
  }

  function connectControl() {
    if (controlSock || controlRetry || gate.isDead()) return;
    const sock = net.connect({ path: controlSockPath });
    sock.setEncoding("utf8");
    sock.on("connect", () => {
      controlSock = sock;
      controlBuf = "";
      controlAttempt = 0;
      sendHello();
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
    if (msg.session) {
      const uid = String(msg.session.uid || "");
      const generation = Number(msg.session.generation || 0);
      if (uid && sessionIdentity.uid && uid !== sessionIdentity.uid) {
        // The daemon owns a different durable session: this runtime is stale.
        gate.markDead();
        emit({
          kind: "control_stale_session",
          claimed: sessionIdentity.uid,
          live: uid,
        });
        closeControl();
        return;
      }
      const changed =
        uid && (uid !== sessionIdentity.uid || generation !== sessionIdentity.generation);
      if (uid) sessionIdentity = { uid, generation };
      if (changed) sendHello();
      return;
    }
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
    // The batch ticket: which incoming ids it carried and which forum topic it
    // came from. A steer in the same turn adds a second batch; turn_end clears
    // the list. The daemon validates reply targets itself.
    const batchKey = deliveryId.split("#")[0];
    const ids = (Array.isArray(push.batch_ids) ? push.batch_ids : [])
      .map(Number)
      .filter((n) => Number.isInteger(n) && n > 0);
    activeBatches = activeBatches.filter((b) => b.key !== batchKey);
    activeBatches.push({
      key: batchKey,
      ids,
      thread: Number.isInteger(push.thread_id) ? push.thread_id : null,
    });
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
        gate.markDead();
        emit({ kind: "control_stale", delivery_id: deliveryId });
        closeControl();
      }
      return "error";
    }
  }

  connectControl();
  pi.on("session_start", () => {
    // A fresh session may re-bind this instance: it is usable again. Its old
    // durable identity is gone, so hello starts empty and the daemon answers
    // with the live uid + generation.
    gate.rearm();
    sessionIdentity = { uid: "", generation: 0 };
    closeControl();
    connectControl();
  });
}
