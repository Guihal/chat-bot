import { createHash } from "node:crypto";
import type { SourceInterval } from "./contracts/summary-types.js";

/** One coverage leaf: an ordinal position and the sha256 of its byte-exact
 *  rendered unit. Ordinals are dense 0-based over the post-chunk unit stream. */
export interface CoverageUnit {
  readonly ordinal: number;
  readonly sha256: string;
}

const sha256Hex = (data: string): string =>
  createHash("sha256").update(data, "utf8").digest("hex");

/**
 * Merkle root over the ordered unit digests. A binary fold: at each level an
 * odd tail node is paired with itself, so the root always transitively covers
 * every unit and any reordering, insertion or drop changes it. Empty source
 * hashes to a fixed sentinel so a zero-record compaction still has a stable
 * coverage identity.
 */
export function coverageDigest(units: readonly CoverageUnit[]): string {
  if (units.length === 0) return sha256Hex("RICH:COVERAGE:EMPTY");
  let level = units.map((u) => u.sha256);
  while (level.length > 1) {
    const next: string[] = [];
    for (let i = 0; i < level.length; i += 2) {
      const left = level[i];
      const right = i + 1 < level.length ? level[i + 1] : left;
      next.push(sha256Hex(`${left}:${right}`));
    }
    level = next;
  }
  return level[0];
}

/** The binding digest of a node: the coverage Merkle root of exactly the units
 *  in its inclusive ordinal interval. Used as a leaf's `sourceDigest`, as a
 *  child entry in a parent's `childDigests`, and (whole source) as the root
 *  `coverageDigest`. */
export function intervalDigest(
  units: readonly CoverageUnit[],
  interval: SourceInterval,
): string {
  if (
    interval.startOrdinal < 0 ||
    interval.endOrdinal >= units.length ||
    interval.endOrdinal < interval.startOrdinal
  )
    throw new Error(
      `interval ${interval.startOrdinal}..${interval.endOrdinal} out of coverage 0..${units.length - 1}`,
    );
  return coverageDigest(
    units.slice(interval.startOrdinal, interval.endOrdinal + 1),
  );
}

/**
 * Assert a set of inclusive intervals tiles `[0, total)` exactly once: sorted by
 * start they must be adjacent with no gap, overlap or duplicate, and end exactly
 * at `total - 1`. This is the structural coverage guarantee — no "drop oldest"
 * branch can pass it. `total` is the number of units.
 */
export function assertExactOnce(
  intervals: readonly SourceInterval[],
  total: number,
): void {
  if (total === 0) {
    if (intervals.length !== 0)
      throw new Error("coverage: intervals present for empty source");
    return;
  }
  const sorted = [...intervals].sort((a, b) => a.startOrdinal - b.startOrdinal);
  let expected = 0;
  for (const { startOrdinal, endOrdinal } of sorted) {
    if (endOrdinal < startOrdinal)
      throw new Error(
        `coverage: inverted interval ${startOrdinal}..${endOrdinal}`,
      );
    if (startOrdinal !== expected)
      throw new Error(
        `coverage: gap or overlap at ordinal ${expected} (next start ${startOrdinal})`,
      );
    expected = endOrdinal + 1;
  }
  if (expected !== total)
    throw new Error(`coverage: tiled up to ${expected}, expected ${total}`);
}
