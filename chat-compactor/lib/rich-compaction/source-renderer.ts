import { createHash } from "node:crypto";
import type { ImageContent, TextContent } from "@earendil-works/pi-ai";
import type { DirectContent } from "../shared/isolated-pi-types.js";
import type { SourceKind, SourceRecord } from "./source-manifest.js";

const RECORD_OPEN = "<<<RICH:RECORD";
const RECORD_CLOSE = "<<<RICH:/RECORD>>>";
const encoder = new TextEncoder();

export const byteLen = (text: string): number => encoder.encode(text).length;
export const sha256Hex = (text: string): string =>
  createHash("sha256").update(text, "utf8").digest("hex");

/** Stable per-frame hash token: text frames hash their text, image frames hash
 *  their mime+base64 payload. The record digest folds these in frame order. */
export const frameToken = (frame: DirectContent): string =>
  frame.type === "image" ? `IMG:${frame.mimeType}:${frame.data}` : frame.text;

export interface RenderedRecord {
  readonly recordOrdinal: number;
  readonly kind: SourceKind;
  readonly frames: readonly DirectContent[];
  /** Combined non-image JSON payload, present when the record has non-image
   *  content; the only part the chunker may code-point split. */
  readonly textPayload: string | null;
  readonly hasImage: boolean;
  readonly bytes: number;
  readonly sha256: string;
}

/** One wrapped text RECORD frame. `chunk` is `i/n` when the payload was
 *  code-point split; the header carries byte length + sha256 of the payload. */
export function buildTextFrame(
  recordOrdinal: number,
  kind: SourceKind,
  type: string,
  payload: string,
  chunk?: { index: number; total: number },
): TextContent {
  const chunkAttr = chunk ? ` chunk=${chunk.index}/${chunk.total}` : "";
  const header =
    `${RECORD_OPEN} ordinal=${recordOrdinal} role=${kind} type=${type}` +
    ` bytes=${byteLen(payload)} sha256=${sha256Hex(payload)}${chunkAttr}>>>`;
  return { type: "text", text: `${header}\n${payload}\n${RECORD_CLOSE}` };
}

function imageHeaderFrame(
  recordOrdinal: number,
  kind: SourceKind,
  image: ImageContent,
): TextContent {
  const decodedBytes = Buffer.from(image.data, "base64").length;
  const header =
    `${RECORD_OPEN} ordinal=${recordOrdinal} role=${kind} type=image` +
    ` bytes=${decodedBytes} sha256=${sha256Hex(image.data)}` +
    ` mime=${image.mimeType} image=raw>>>`;
  return { type: "text", text: `${header}\n${RECORD_CLOSE}` };
}

const isImage = (block: unknown): block is ImageContent =>
  !!block &&
  typeof block === "object" &&
  (block as { type?: unknown }).type === "image";

/** Split a record's content into non-image blocks (serialised together as one
 *  JSON payload) and raw image blocks (passed through byte-identically). */
function partition(message: unknown): {
  nonImage: unknown[];
  images: ImageContent[];
} {
  const content = (message as { content?: unknown }).content;
  if (typeof content === "string")
    return { nonImage: [{ type: "text", text: content }], images: [] };
  if (!Array.isArray(content)) return { nonImage: [message], images: [] };
  const nonImage: unknown[] = [];
  const images: ImageContent[] = [];
  for (const block of content) (isImage(block) ? images : nonImage).push(block);
  return { nonImage, images };
}

/**
 * Render one record to its byte-preserving frames: at most one combined text
 * frame for all non-image content, then a header+raw pair per image. The record
 * digest folds every frame in order; images pass through untouched — the raw
 * frame reuses the original `ImageContent` object so no field is lost.
 */
export function renderSemanticRecord(record: SourceRecord): RenderedRecord {
  const { nonImage, images } = partition(record.message);
  const frames: DirectContent[] = [];
  let textPayload: string | null = null;
  if (nonImage.length > 0) {
    textPayload = JSON.stringify(nonImage);
    frames.push(
      buildTextFrame(record.ordinal, record.kind, "message", textPayload),
    );
  }
  for (const image of images) {
    frames.push(imageHeaderFrame(record.ordinal, record.kind, image));
    frames.push(image);
  }
  const bytes = frames.reduce((sum, f) => sum + byteLen(frameToken(f)), 0);
  const sha256 = sha256Hex(frames.map(frameToken).join("\n"));
  return {
    recordOrdinal: record.ordinal,
    kind: record.kind,
    frames,
    textPayload,
    hasImage: images.length > 0,
    bytes,
    sha256,
  };
}
