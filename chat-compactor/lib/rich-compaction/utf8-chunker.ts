import type { DirectContent } from "../shared/isolated-pi-types.js";
import type { SourceKind } from "./source-manifest.js";
import {
  buildTextFrame,
  byteLen,
  frameToken,
  sha256Hex,
  type RenderedRecord,
} from "./source-renderer.js";

export interface RenderedUnit {
  readonly ordinal: number;
  readonly recordOrdinal: number;
  readonly chunkIndex: number;
  readonly kind: SourceKind;
  readonly frames: readonly DirectContent[];
  readonly bytes: number;
  readonly sha256: string;
}

function unitOf(
  recordOrdinal: number,
  kind: SourceKind,
  frames: readonly DirectContent[],
): RenderedUnit {
  const bytes = frames.reduce((sum, f) => sum + byteLen(frameToken(f)), 0);
  const sha256 = sha256Hex(frames.map(frameToken).join("\n"));
  return { ordinal: recordOrdinal, recordOrdinal, chunkIndex: 0, kind, frames, bytes, sha256 };
}

/**
 * Flatten every rendered record into one coverage unit (1:1, no splitting).
 * Context sizing is pi's job: the session arrives already trimmed to fit the
 * inherited model, so the source is never budgeted here.
 */
export function buildUnits(
  records: readonly RenderedRecord[],
): RenderedUnit[] {
  return records.map((record) => unitOf(record.recordOrdinal, record.kind, record.frames));
}
