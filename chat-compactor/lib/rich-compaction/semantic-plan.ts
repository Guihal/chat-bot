import type { SourceInterval } from "./contracts/summary-types.js";
import { coverageDigest, type CoverageUnit } from "./coverage-tree.js";
import { RichFailure } from "./rich-types.js";
import type { RenderedUnit } from "./utf8-chunker.js";

export interface SemanticNodePlan {
  readonly role: "leaf" | "merge" | "root";
  readonly interval: SourceInterval;
  /** Plan indices of the child nodes this node folds (empty for a leaf and for a
   * single-root plan that folds raw source directly). */
  readonly childIndices: readonly number[];
}

export interface SemanticTreePlan {
  readonly nodes: readonly SemanticNodePlan[];
  readonly units: readonly RenderedUnit[];
  readonly coverageDigest: string;
}

export const coverageUnits = (units: readonly RenderedUnit[]): CoverageUnit[] =>
  units.map((u) => ({ ordinal: u.ordinal, sha256: u.sha256 }));

/**
 * Plan the single-root reduction. Context sizing is pi's job — the session
 * arrives already trimmed to fit the inherited model — so the whole source is
 * always one root request folding the raw frames directly. The plan carries the
 * host-computed coverage digest the root must echo.
 */
export function buildSemanticTree(
  units: readonly RenderedUnit[],
): SemanticTreePlan {
  if (units.length === 0)
    throw new RichFailure("model-output", "plan", "empty-source");
  const digest = coverageDigest(coverageUnits(units));
  return {
    nodes: [
      { role: "root", interval: { startOrdinal: 0, endOrdinal: units.length - 1 }, childIndices: [] },
    ],
    units,
    coverageDigest: digest,
  };
}
