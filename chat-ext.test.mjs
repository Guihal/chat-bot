/**
 * Unit tests for the pure RPC argument shape of chat-ext.mjs.
 *
 * The tool registration needs the omo runtime, so the send contract is factored
 * into sendArgs(): a batch ticket, the optional text/media fields and the forum
 * topic. No URL and no token are constructed here; the daemon owns them.
 *
 * Run: node --test chat-ext.test.mjs
 */
import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import chatExt, {
  compactEvents,
  helloFrame,
  sanitizePayload,
  sendArgs,
  staleGate,
  terminalProviderError,
} from "./chat-ext.mjs";

test("a compaction always reports the lost context, delivery ids or not", () => {
  // The daemon needs the unconditional fact: the next turn must carry the
  // history window even when no delivery was in flight.
  assert.deepEqual(compactEvents({ requestId: "r1", reason: "auto" }, []), [
    { kind: "context_compacted", requestId: "r1", reason: "auto" },
  ]);
  assert.deepEqual(compactEvents(null, []), [
    { kind: "context_compacted", requestId: null, reason: null },
  ]);
});

test("a compaction with in-flight deliveries reports both facts", () => {
  const evs = compactEvents({ requestId: "r2" }, ["sig-a#1", "sig-b#1"]);
  assert.deepEqual(evs, [
    {
      kind: "compacted",
      delivery_ids: ["sig-a#1", "sig-b#1"],
      requestId: "r2",
      reason: null,
    },
    { kind: "context_compacted", requestId: "r2", reason: null },
  ]);
});

test("a rejected compaction leaves the context intact and reports nothing", () => {
  assert.deepEqual(compactEvents({ accepted: false }, ["sig-a#1"]), []);
});

test("hello carries the durable session id and generation, never the name", () => {
  assert.deepEqual(helloFrame("dur-7f3a", 4, 1234), {
    hello: { session: "dur-7f3a", generation: 4, pid: 1234 },
  });
  assert.equal(helloFrame("", 0, 1).hello.session, "");
});

test("a stale instance refuses reconnect until a fresh session start", () => {
  const gate = staleGate();
  assert.equal(gate.isDead(), false);
  gate.markDead();
  assert.equal(gate.isDead(), true);
  gate.rearm();
  assert.equal(gate.isDead(), false);
});

test("sendArgs carries the text and nothing about batches", () => {
  assert.deepEqual(sendArgs({ text: "привет" }), { text: "привет" });
});

test("sendArgs keeps every media argument", () => {
  assert.deepEqual(
    sendArgs({
      text: "подпись",
      reply_to: 501,
      gif_file_id: "gif_1",
      media_path: "/state/media/a.gif",
    }),
    {
      text: "подпись",
      reply_to: 501,
      gif_file_id: "gif_1",
      media_path: "/state/media/a.gif",
    },
  );
});

test("a sticker plus text stays one RPC call and two messages", () => {
  const args = sendArgs({ sticker_file_id: "stick_1", text: "подпись" });
  assert.equal(args.sticker_file_id, "stick_1");
  assert.equal(args.text, "подпись");
});

test("a send never carries a batch ticket", () => {
  const args = sendArgs({ text: "x" });
  assert.equal("batch_key" in args, false);
  assert.equal("thread_id" in args, false);
});

test("the extension contains no Bot API transport or token access", () => {
  const src = fs.readFileSync(new URL("./chat-ext.mjs", import.meta.url), "utf8");
  for (const forbidden of [
    "api.telegram.org",
    "CHATBOT_TOKEN_FILE",
    "readFileSync(token",
    "fetch(",
    "botApi(",
    "sendChatAction",
  ]) {
    assert.equal(src.includes(forbidden), false, `found ${forbidden}`);
  }
});

test("subagent spawns with no owner message id", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "chat-ext-subagent-"));
  const bin = path.join(dir, "bin");
  fs.mkdirSync(bin);
  const omo = path.join(bin, "omo");
  fs.writeFileSync(omo, "#!/bin/sh\necho SUBAGENT-OK\n");
  fs.chmodSync(omo, 0o755);
  const oldPath = process.env.PATH;
  const oldState = process.env.CHATBOT_STATE_DIR;
  process.env.PATH = `${bin}:${oldPath || ""}`;
  process.env.CHATBOT_STATE_DIR = dir;
  const tools = new Map();
  const handlers = new Map();
  const pi = {
    registerTool: (t) => tools.set(t.name, t),
    on: (name, fn) => handlers.set(name, fn),
    session: {},
  };
  try {
    chatExt(pi);
    const tool = tools.get("subagent");
    assert.ok(tool, "subagent tool was not registered");
    assert.deepEqual(tool.parameters.required, ["task"]);
    assert.equal(
      Object.hasOwn(tool.parameters.properties, "owner_message_id"),
      false,
    );
    const res = await tool.execute("call-1", { task: "do the thing" });
    assert.equal(res.isError, undefined, JSON.stringify(res));
    assert.match(res.content[0].text, /SUBAGENT-OK/);
  } finally {
    const shutdown = handlers.get("session_shutdown");
    if (shutdown) shutdown();
    process.env.PATH = oldPath;
    if (oldState === undefined) delete process.env.CHATBOT_STATE_DIR;
    else process.env.CHATBOT_STATE_DIR = oldState;
    fs.rmSync(dir, { recursive: true, force: true });
  }
});


// ---------------------------------------------------------------------------
// Typing indicator: it belongs to the RUN, not to a single turn.
//
// Measured on live events before this change: 1281 turn joints inside one run,
// 251 of them with the indicator dark for 3s+ (worst case 4515226 ms) while the
// agent kept working. These tests fail on the turn-scoped implementation.
// ---------------------------------------------------------------------------
import net from "node:net";

const THREAD = 6043;
const BATCH = "sig-run";

function fakeRuntime(admitKind, gate) {
  const sentUserMessages = [];
  const handlers = new Map();
  const tools = new Map();
  const pi = {
    on(name, fn) {
      if (!handlers.has(name)) handlers.set(name, []);
      handlers.get(name).push(fn);
    },
    registerTool(tool) {
      tools.set(tool.name, tool);
    },
    // Top-level, like the real `pi`: a user message is a runtime API, not a
    // session-control action.
    sendUserMessage: (text, options) => {
      sentUserMessages.push({ text, options });
    },
    session: {
      admissionGate: () => gate || { can_admit: true, turn_epoch: 1 },
      listAdmittedDeliveries: () => ({ pending: [], emitted: [], failed: [] }),
    },
  };
  const fire = async (name, ev) => {
    for (const fn of handlers.get(name) || []) await fn(ev || {});
  };
  return { pi, fire, tools, sentUserMessages };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function withControlSock(t, admitKind, gate) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "chat-ext-typing-"));
  const sockPath = path.join(dir, "control.sock");
  const calls = [];
  const sockets = [];
  const server = net.createServer((sock) => {
    // The test process must be able to exit: an unref'd handle never keeps the
    // event loop alive, so a leaked socket cannot hang the runner.
    sock.unref();
    sockets.push(sock);
    let buf = "";
    sock.setEncoding("utf8");
    sock.on("data", (chunk) => {
      buf += chunk;
      let i = buf.indexOf("\n");
      while (i >= 0) {
        const line = buf.slice(0, i).trim();
        buf = buf.slice(i + 1);
        if (line) {
          const msg = JSON.parse(line);
          if (msg.call) {
            calls.push(msg.call);
            sock.write(
              JSON.stringify({ return: { id: msg.call.id, ok: true, text: "" } }) + "\n",
            );
          }
        }
        i = buf.indexOf("\n");
      }
    });
  });
  await new Promise((resolve) => server.listen(sockPath, resolve));
  server.unref();

  const prevState = process.env.CHATBOT_STATE_DIR;
  process.env.CHATBOT_STATE_DIR = dir;
  const rt = fakeRuntime(admitKind, gate);
  chatExt(rt.pi);
  await rt.fire("session_start", {});

  t.after(async () => {
    // Release the extension's own socket and timers, otherwise the test runner
    // waits forever on a live handle.
    try {
      await rt.fire("session_shutdown", {});
    } catch {
      // the extension may already be closed
    }
    for (const s of sockets) {
      try {
        s.destroy();
      } catch {
        // already gone
      }
    }
    if (prevState === undefined) delete process.env.CHATBOT_STATE_DIR;
    else process.env.CHATBOT_STATE_DIR = prevState;
    try {
      server.close();
    } catch {
      // already closed
    }
    fs.rmSync(dir, { recursive: true, force: true });
  });

  const waitConnected = async () => {
    const deadline = Date.now() + 8000;
    while (Date.now() < deadline && sockets.length === 0) {
      await sleep(600);
    }
    assert.equal(sockets.length > 0, true, "the extension attaches to the control socket");
    return sockets[sockets.length - 1];
  };
  const typingCalls = () => calls.filter((c) => c.op === "typing");
  return { rt, calls, typingCalls, waitConnected };
}

function pushFrame(sock, deliveryId) {
  sock.write(
    JSON.stringify({
      push: {
        delivery_id: deliveryId,
        text: "сделай задачу",
        deliverAs: "steer",
        batch_ids: [42],
        thread_id: THREAD,
      },
    }) + "\n",
  );
}

test("the extension reports activity and never drives the indicator", async (t) => {
  const h = await withControlSock(t, "started");
  const sock = await h.waitConnected();
  pushFrame(sock, `${BATCH}#1`);
  await sleep(300);

  await h.rt.fire("turn_start", { turnIndex: 0 });
  await h.rt.fire("turn_end", { turnIndex: 0 });
  await h.rt.fire("agent_settled", {});

  // Long enough for several of the old ticks: the daemon owns the indicator now.
  await sleep(2500);
  assert.equal(h.typingCalls().length, 0, "the extension never calls the typing op");

  const events = fs
    .readFileSync(path.join(process.env.CHATBOT_STATE_DIR, "events.jsonl"), "utf8")
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line))
    .filter((ev) => ev.kind === "agent_activity");
  assert.deepEqual(
    events.map((ev) => ev.phase),
    ["working", "stopped"],
    "the two owner hooks publish exactly the state the daemon polls",
  );
});

// The daemon closes a batch on the emitted proof. A user message leaves no
// entry in the engine's external-admission ledger, so the extension must report
// that proof itself - without it the daemon waits out its 60s delivery clock and
// the history window is never marked served (it would ride every later turn).
test("a user-prompt delivery reports the emitted proof", async (t) => {
  const h = await withControlSock(t, "started");
  const sock = await h.waitConnected();
  pushFrame(sock, `${BATCH}#2`);
  await sleep(600);

  assert.equal(h.rt.sentUserMessages.length, 1, "the push reaches the model as a user message");
  assert.ok(
    h.rt.sentUserMessages[0].text.includes("сделай задачу"),
    "the batch text is the user message",
  );
  const readEvents = () =>
    fs
      .readFileSync(path.join(process.env.CHATBOT_STATE_DIR, "events.jsonl"), "utf8")
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
  const emitted = () =>
    readEvents().filter((ev) => ev.kind === "delivery_state" && ev.state === "emitted");

  // The send alone proves nothing: the engine can park the text as a held wake
  // while the session is paused, and a proof here would mark the batch delivered
  // (and the history window served) for text the model never saw.
  assert.equal(emitted().length, 0, "no proof before the turn starts");

  await h.rt.fire("turn_start", { turnIndex: 0 });
  await sleep(150);
  const proof = emitted();
  assert.equal(proof.length, 1, "exactly one emitted proof per started turn");
  assert.equal(proof[0].delivery_id, `${BATCH}#2`);
});

// A held session parks an extension user message as a notice instead of writing it
// to the transcript, and only the engine's own resume replays those notices. The
// extension must refuse such a delivery, or the daemon closes the batch (and marks
// the history window served) for text the model never saw.
test("a held session refuses the delivery instead of parking it", async (t) => {
  const h = await withControlSock(t, "started", { can_admit: false, turn_epoch: 1 });
  const sock = await h.waitConnected();
  pushFrame(sock, `${BATCH}#3`);
  await sleep(400);

  assert.equal(h.rt.sentUserMessages.length, 0, "nothing is handed to a held session");
  const events = fs
    .readFileSync(path.join(process.env.CHATBOT_STATE_DIR, "events.jsonl"), "utf8")
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line));
  assert.equal(
    events.filter((ev) => ev.kind === "delivery_state").length,
    0,
    "a refusal must not report a proof",
  );
  assert.equal(
    events.filter((ev) => ev.kind === "admit_deferred").length,
    1,
    "the deferral is reported so the daemon retries later",
  );
});

// ---- gateway-rejected media in tool results -------------------------------

// The installed runtime serializes a tool result as
// {type:"function_call_output", call_id, output:[{type:"input_image",
//  detail:"auto", image_url:"data:<mime>;base64,..."}]} and the gateway accepts
// only raster images there. A video part left in that position makes EVERY
// later request fail on the same history index, so the sanitizer must find the
// part in exactly this shape.
function responsesPayload(outputParts) {
  return {
    model: "muse-spark-1.3-contributor",
    input: [
      { role: "user", content: [{ type: "input_text", text: "смотри" }] },
      { type: "function_call_output", call_id: "call_1", output: outputParts },
    ],
  };
}

test("sanitizePayload replaces video inside a tool result", () => {
  const payload = responsesPayload([
    { type: "input_text", text: 'Read video file "clip.mp4" [video/mp4]' },
    { type: "input_image", detail: "auto", image_url: "data:video/mp4;base64,AAAAIGZ0eXA" },
  ]);
  const result = sanitizePayload(payload);
  assert.equal(result.changed, true);
  assert.equal(result.removed, 1);
  assert.deepEqual(result.mimes, ["video/mp4"]);
  const parts = result.payload.input[1].output;
  assert.equal(parts.length, 2);
  assert.equal(parts[0].text, 'Read video file "clip.mp4" [video/mp4]', "the name of the file stays");
  assert.equal(parts[1].type, "input_text");
  assert.match(parts[1].text, /video\/mp4/);
  assert.equal("image_url" in parts[1], false);
});

test("sanitizePayload leaves an image inside a tool result untouched", () => {
  const payload = responsesPayload([
    { type: "input_image", detail: "auto", image_url: "data:image/png;base64,iVBORw0KG" },
  ]);
  const result = sanitizePayload(payload);
  assert.equal(result.changed, false);
  assert.equal(result.removed, 0);
  assert.equal(result.payload, payload, "an unchanged request must be sent identically");
});

test("sanitizePayload replaces an oversized raster inside a tool result", () => {
  const oversize = "A".repeat(Math.ceil(((256 * 1024 + 1) * 4) / 3));
  const payload = responsesPayload([
    { type: "input_image", detail: "auto", image_url: `data:image/png;base64,${oversize}` },
  ]);
  const result = sanitizePayload(payload);
  assert.equal(result.changed, true);
  assert.equal(result.removed, 1);
  const parts = result.payload.input[1].output;
  assert.equal(parts.length, 1);
  assert.equal(parts[0].type, "input_text");
  assert.match(parts[0].text, /изображение image\/png опущено/);
  assert.equal("image_url" in parts[0], false);
});

test("sanitizePayload leaves user-message media untouched", () => {
  const payload = {
    input: [
      { role: "user", content: [{ type: "input_image", image_url: "data:video/mp4;base64,AAAA" }] },
    ],
  };
  const result = sanitizePayload(payload);
  assert.equal(result.changed, false);
  assert.equal(result.payload, payload);
});

test("sanitizePayload is a no-op for string outputs and media-free payloads", () => {
  const stringOut = responsesPayload("plain text output");
  assert.equal(sanitizePayload(stringOut).changed, false);
  const plain = { model: "m", input: "Say hi" };
  assert.equal(sanitizePayload(plain).payload, plain);
});

test("sanitizePayload handles the anthropic tool_result shape", () => {
  const payload = {
    messages: [
      {
        role: "user",
        content: [
          {
            type: "tool_result",
            tool_use_id: "tu_1",
            content: [{ type: "image", source: { type: "base64", media_type: "video/webm", data: "AAAA" } }],
          },
        ],
      },
    ],
  };
  const result = sanitizePayload(payload);
  assert.equal(result.changed, true);
  const part = result.payload.messages[0].content[0].content[0];
  assert.equal(part.type, "text");
  assert.match(part.text, /video\/webm/);
});

// ---- terminal provider failure vs model silence ---------------------------

function assistantEnd(extra) {
  return {
    messages: [
      { message: { role: "user" } },
      {
        message: {
          role: "assistant",
          stopReason: "error",
          errorMessage:
            'opencode-go API error (400): {"message":"unsupported media type at input[43].output[1]: `video/mp4`"}',
        },
      },
    ],
    ...extra,
  };
}

test("terminalProviderError names a provider error", () => {
  const text = terminalProviderError(assistantEnd());
  assert.match(text, /unsupported media type/);
});

test("terminalProviderError treats a provider abort as terminal, a user abort not", () => {
  const providerAbort = {
    aborted: true,
    abortSource: "provider",
    messages: [{ message: { role: "assistant", stopReason: "aborted" } }],
  };
  assert.match(terminalProviderError(providerAbort), /aborted by provider/);
  const userAbort = {
    aborted: true,
    abortSource: "user",
    messages: [{ message: { role: "assistant", stopReason: "aborted" } }],
  };
  assert.equal(terminalProviderError(userAbort), "");
});

test("terminalProviderError stays empty for a clean run", () => {
  const clean = { messages: [{ message: { role: "assistant", stopReason: "stop" } }] };
  assert.equal(terminalProviderError(clean), "");
});
