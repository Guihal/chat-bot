import { test } from "node:test";
import assert from "node:assert/strict";

import defaultMinTemperature from "./default-min-temperature.ts";
import { projectSampling } from "./sampling-payload.ts";

const OPENCODE_DEEPSEEK = {
  provider: "opencode-go",
  id: "deepseek-v4.1-flash",
  api: "openai-completions",
  baseUrl: "https://opencode.ai/inference/go/v1",
  reasoning: true,
};
const MUSE = { provider: "opencode-go", id: "muse-spark-1.3-contributor", api: "openai-completions" };
const PLAIN = { provider: "openai", id: "gpt-4.1", api: "openai-completions" };

function handler() {
  const handlers = [];
  defaultMinTemperature({
    on: (_name, fn) => handlers.push(fn),
  });
  assert.equal(handlers.length, 1);
  return handlers[0];
}

const request = (extra = {}) => ({
  model: "deepseek-v4.1-flash",
  messages: [{ role: "user", content: "hi" }],
  ...extra,
});

test("the deepseek profile lands in the opencode-go payload", () => {
  const out = handler()({ payload: request() }, { model: OPENCODE_DEEPSEEK });
  assert.equal(out.temperature, 1.0);
  assert.equal(out.top_p, 0.7);
});

test("temperature is overridden even when the caller set one", () => {
  const out = handler()({ payload: request({ temperature: 0.2 }) }, { model: OPENCODE_DEEPSEEK });
  assert.equal(out.temperature, 1.0);
});

test("an existing top_p is kept", () => {
  const out = handler()({ payload: request({ top_p: 0.33 }) }, { model: OPENCODE_DEEPSEEK });
  assert.equal(out.top_p, 0.33);
});

test("muse-spark leaves the payload alone", () => {
  const out = handler()({ payload: request() }, { model: MUSE });
  assert.equal(out, undefined);
});

test("an unknown api is untouched", () => {
  const out = handler()({ payload: request() }, { model: { provider: "x", id: "y", api: "mystery" } });
  assert.equal(out, undefined);
});

test("the generic profile is 0.9 / 0.85", () => {
  const out = handler()({ payload: request() }, { model: PLAIN });
  assert.equal(out.temperature, 0.9);
  assert.equal(out.top_p, 0.85);
});

const ICEMAN = { provider: "opencode-go", id: "iceman-contributor-free", api: "openai-completions" };

test("the iceman profile adds only top_p 0.95", () => {
  const out = handler()({ payload: request({ temperature: 0.8 }) }, { model: ICEMAN });
  assert.equal(out.top_p, 0.95);
  assert.equal(out.temperature, 0.8);
  assert.equal("top_k" in out, false);
  assert.equal("presence_penalty" in out, false);
  assert.equal("frequency_penalty" in out, false);
  assert.equal("repetition_penalty" in out, false);
});

test("the iceman profile matches the catalog-prefixed model id", () => {
  const out = handler()(
    { payload: request({ temperature: 0.8 }) },
    { model: { ...ICEMAN, id: "opencode-go/iceman-contributor-free" } },
  );
  assert.equal(out.top_p, 0.95);
  assert.equal("top_k" in out, false);
});

const HY4 = { provider: "opencode-go", id: "hy4-flash-free", api: "openai-completions" };

test("the hy4-flash-free profile adds only top_p 0.7", () => {
  const out = handler()({ payload: request({ temperature: 0.8 }) }, { model: HY4 });
  assert.equal(out.top_p, 0.7);
  assert.equal(out.temperature, 0.8);
  assert.equal("top_k" in out, false);
  assert.equal("presence_penalty" in out, false);
  assert.equal("frequency_penalty" in out, false);
  assert.equal("repetition_penalty" in out, false);
});

test("projectSampling gives the opencode-go gateway the repetition penalty", () => {
  const projection = projectSampling(request(), OPENCODE_DEEPSEEK, { temperature: 1.0, top_p: 0.7, repetition_penalty: 1.05 });
  assert.equal(projection.payload.repetition_penalty, 1.05);
});
