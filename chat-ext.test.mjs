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

import chatExt, { helloFrame, sendArgs, staleGate } from "./chat-ext.mjs";

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

test("sendArgs carries the batch ticket and text", () => {
  assert.deepEqual(sendArgs({ text: "привет" }, "sig-b", null), {
    batch_key: "sig-b",
    text: "привет",
  });
});

test("sendArgs keeps every media argument and the topic", () => {
  assert.deepEqual(
    sendArgs(
      {
        text: "подпись",
        reply_to: 501,
        gif_file_id: "gif_1",
        media_path: "/state/media/a.gif",
      },
      "sig-c",
      4113,
    ),
    {
      batch_key: "sig-c",
      text: "подпись",
      reply_to: 501,
      gif_file_id: "gif_1",
      media_path: "/state/media/a.gif",
      thread_id: 4113,
    },
  );
});

test("a sticker plus text stays one RPC call and two messages", () => {
  const args = sendArgs(
    { sticker_file_id: "stick_1", text: "подпись" },
    "sig-d",
    77,
  );
  assert.equal(args.sticker_file_id, "stick_1");
  assert.equal(args.text, "подпись");
  assert.equal(args.thread_id, 77);
});

test("blank batch ticket is represented, never guessed", () => {
  assert.equal(sendArgs({ text: "x" }, "", null).batch_key, "");
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

function fakeRuntime(admitKind) {
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
    session: {
      admissionGate: () => ({ can_admit: true, turn_epoch: 1 }),
      admitExternalMessage: () => ({ kind: admitKind }),
      listAdmittedDeliveries: () => ({ pending: [], emitted: [], failed: [] }),
    },
  };
  const fire = async (name, ev) => {
    for (const fn of handlers.get(name) || []) await fn(ev || {});
  };
  return { pi, fire, tools };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function withControlSock(t, admitKind) {
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
  const rt = fakeRuntime(admitKind);
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

test("typing survives turn joints and stops only when the run ends", async (t) => {
  const h = await withControlSock(t, "started");
  const sock = await h.waitConnected();
  pushFrame(sock, `${BATCH}#1`);
  await sleep(1000);

  const first = h.typingCalls();
  assert.equal(first.length >= 1, true, "a run lights the indicator before the first turn");
  assert.equal(first[0].args.thread_id, THREAD, "topic must not be dropped");
  assert.equal(first[0].args.batch_key, BATCH);

  await h.rt.fire("turn_start", { turnIndex: 1 });
  await h.rt.fire("turn_end", { turnIndex: 1 });
  const afterJoint = h.typingCalls().length;
  await sleep(4600);
  const afterGap = h.typingCalls().length;
  assert.equal(
    afterGap > afterJoint,
    true,
    `the turn joint must not silence the indicator (was ${afterJoint}, now ${afterGap})`,
  );
  for (const c of h.typingCalls().slice(first.length)) {
    assert.equal(c.args.thread_id, THREAD, "later ticks keep the run topic");
  }

  await h.rt.fire("agent_settled", {});
  await sleep(1000);
  const afterSettle = h.typingCalls().length;
  await sleep(5200);
  assert.equal(
    h.typingCalls().length,
    afterSettle,
    "nothing is sent after the run settled",
  );
});

test("an already admitted re-push does not light the indicator", async (t) => {
  const h = await withControlSock(t, "already_admitted");
  const sock = await h.waitConnected();
  pushFrame(sock, `${BATCH}#2`);
  await sleep(4600);
  assert.equal(
    h.typingCalls().length,
    0,
    "a lost-answer re-push proves no run started, so no indicator",
  );
});

test("a retrying agent_end keeps the indicator, a terminal one stops it", async (t) => {
  const h = await withControlSock(t, "started");
  const sock = await h.waitConnected();
  pushFrame(sock, `${BATCH}#3`);
  await sleep(1000);
  const lit = h.typingCalls().length;
  assert.equal(lit >= 1, true, "the run starts the indicator");

  await h.rt.fire("agent_end", { aborted: true, willRetry: true, abortSource: "budget" });
  await sleep(4600);
  assert.equal(
    h.typingCalls().length > lit,
    true,
    "a retry is a continuation of the same run, so the indicator keeps ticking",
  );

  await h.rt.fire("agent_end", { aborted: true, willRetry: false, abortSource: "user" });
  await sleep(1000);
  const stopped = h.typingCalls().length;
  await sleep(5200);
  assert.equal(h.typingCalls().length, stopped, "a terminal abort ends the run");
});

test("a push that never becomes a turn stops the indicator", async (t) => {
  process.env.CHATBOT_TYPING_ARM_TIMEOUT_MS = "900";
  t.after(() => {
    delete process.env.CHATBOT_TYPING_ARM_TIMEOUT_MS;
  });
  const h = await withControlSock(t, "started");
  const sock = await h.waitConnected();
  pushFrame(sock, `${BATCH}#4`);
  await sleep(700);
  const lit = h.typingCalls().length;
  assert.equal(lit >= 1, true, "the push arms the indicator");
  await sleep(5200);
  assert.equal(
    h.typingCalls().length,
    lit,
    "with no turn_start the indicator stops instead of burning",
  );
});

test("a steer inside a live run never reopens the no-turn window", async (t) => {
  process.env.CHATBOT_TYPING_ARM_TIMEOUT_MS = "900";
  t.after(() => {
    delete process.env.CHATBOT_TYPING_ARM_TIMEOUT_MS;
  });
  const h = await withControlSock(t, "steered");
  const sock = await h.waitConnected();
  pushFrame(sock, `${BATCH}#5`);
  await sleep(500);
  await h.rt.fire("turn_start", { turnIndex: 1 });
  await sleep(700);
  const beforeSteer = h.typingCalls().length;
  pushFrame(sock, `${BATCH}#6`);
  await sleep(5200);
  assert.equal(
    h.typingCalls().length > beforeSteer,
    true,
    "a steer during a run must not silence the indicator",
  );
});
