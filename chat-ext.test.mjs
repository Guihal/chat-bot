/**
 * Unit tests for the pure send-selection helpers of chat-ext.mjs.
 *
 * The tool registration itself needs the omo runtime (`pi`), so the interesting
 * decisions are factored out and asserted here: which Telegram method a call
 * would use (gif / sticker / .gif / photo / video / document / text), that the
 * three media arguments are mutually exclusive, and that "не перебарщивать" is
 * a real limit (one media per turn) instead of a wish.
 *
 * Run: node --test chat-ext.test.mjs
 */
import test from "node:test";
import assert from "node:assert/strict";

import {
  MAX_MEDIA_PER_TURN,
  STICKER_TEXT_ERROR,
  TURN_MEDIA_ERROR,
  allowsAnotherMedia,
  classifyChatSend,
  mediaSpec,
} from "./chat-ext.mjs";

test("classifyChatSend picks the single media target", () => {
  assert.deepEqual(classifyChatSend({}), { target: "text" });
  assert.deepEqual(classifyChatSend({ text: "привет" }), { target: "text" });
  assert.deepEqual(classifyChatSend({ gif_file_id: "gif_1" }), {
    target: "gif",
  });
  assert.deepEqual(classifyChatSend({ sticker_file_id: "sticker_1" }), {
    target: "sticker",
  });
  assert.deepEqual(classifyChatSend({ media_path: "/state/media/a.gif" }), {
    target: "media",
  });
  // A GIF caption is text next to the media, not a separate body.
  assert.deepEqual(classifyChatSend({ gif_file_id: "gif_1", text: "ах" }), {
    target: "gif",
  });
});

test("classifyChatSend refuses more than one media argument", () => {
  for (const params of [
    { gif_file_id: "g", media_path: "/a.gif" },
    { gif_file_id: "g", sticker_file_id: "s" },
    { sticker_file_id: "s", media_path: "/a.mp4" },
    { gif_file_id: "g", sticker_file_id: "s", media_path: "/a.mp4" },
  ]) {
    const got = classifyChatSend(params);
    assert.ok(got.error, `expected a refusal for ${JSON.stringify(params)}`);
    assert.match(got.error, /взаимоисключающие/);
  }
});

test("a sticker cannot carry a caption", () => {
  const withText = classifyChatSend({ sticker_file_id: "s", text: "подпись" });
  assert.equal(withText.error, STICKER_TEXT_ERROR);
  assert.equal(classifyChatSend({ sticker_file_id: "s", text: "   " }).target, "sticker");
});

test("one media attachment per turn is the hard limit", () => {
  assert.equal(MAX_MEDIA_PER_TURN, 1);
  assert.equal(allowsAnotherMedia(0), true, "first media of a turn is allowed");
  assert.equal(allowsAnotherMedia(1), false, "second media is refused");
  assert.equal(allowsAnotherMedia(2), false);
  assert.match(TURN_MEDIA_ERROR, /только одно медиа/);
});

test("mediaSpec sends by file type", () => {
  assert.deepEqual(mediaSpec("/s/media/loop.gif"), {
    method: "sendAnimation",
    field: "animation",
    mime: "image/gif",
  });
  assert.equal(mediaSpec("/s/media/a.jpg").method, "sendPhoto");
  assert.equal(mediaSpec("/s/media/a.png").method, "sendPhoto");
  assert.equal(mediaSpec("/s/media/a.mp4").method, "sendVideo");
  assert.equal(mediaSpec("/s/media/a.webm").method, "sendVideo");
  assert.deepEqual(mediaSpec("/s/media/a.pdf"), {
    method: "sendDocument",
    field: "document",
    mime: "application/pdf",
  });
  assert.equal(mediaSpec("/s/media/blob.bin").method, "sendDocument");
});
