import type { DirectContent } from "../shared/isolated-pi-types.js";
import type { ContractGenerator } from "./contract-generator.js";
import type { SummaryRequestV1 } from "./contracts/summary-contract.js";
import type { SummaryNodeV1 } from "./contracts/summary-types.js";
import { intervalDigest } from "./coverage-tree.js";
import { RichFailure } from "./rich-types.js";
import {
  coverageUnits,
  type SemanticNodePlan,
  type SemanticTreePlan,
} from "./semantic-plan.js";

/** One generator per node role; every role runs the same generic contract engine
 *  with a role-bound contract (createSummaryContract). */
export type SummaryGenerators = Record<
  "leaf" | "merge" | "root",
  ContractGenerator<SummaryRequestV1, SummaryNodeV1>
>;

/** Fold a node's request frames: a folding node (merge/root with children) folds
 *  the child summaries; a leaf and a single root fold the raw unit frames of
 *  their interval, byte-for-byte. */
function framesFor(
  plan: SemanticTreePlan,
  node: SemanticNodePlan,
  summaries: ReadonlyMap<number, string>,
): DirectContent[] {
  if (node.childIndices.length > 0)
    return node.childIndices.map((i) => ({
      type: "text",
      text: summaries.get(i) ?? "",
    }));
  const out: DirectContent[] = [];
  for (let i = node.interval.startOrdinal; i <= node.interval.endOrdinal; i++)
    out.push(...plan.units[i].frames);
  return out;
}

/** Build the summary request for a node, embedding the host-computed digests the
 *  model must echo back (leaf sourceDigest, merge childDigests, root coverage). */
export function requestFor(
  plan: SemanticTreePlan,
  node: SemanticNodePlan,
  summaries: ReadonlyMap<number, string>,
): SummaryRequestV1 {
  const units = coverageUnits(plan.units);
  const base = {
    interval: node.interval,
    frames: framesFor(plan, node, summaries),
  };
  if (node.role === "leaf")
    return {
      role: "leaf",
      ...base,
      sourceDigest: intervalDigest(units, node.interval),
    };
  if (node.role === "merge")
    return {
      role: "merge",
      ...base,
      childDigests: node.childIndices.map((i) =>
        intervalDigest(units, plan.nodes[i].interval),
      ),
    };
  return { role: "root", ...base, coverageDigest: plan.coverageDigest };
}

/** Host-side verification that the model folded exactly the requested source: the
 *  node kind, its interval, and the echoed coverage digests must all match what
 *  the host computed. Any mismatch is a `model-output` failure (recovery in Ф2). */
export function verify(
  plan: SemanticTreePlan,
  node: SemanticNodePlan,
  out: SummaryNodeV1,
): void {
  if (out.kind !== node.role)
    throw new RichFailure("model-output", node.role, "kind-mismatch");
  const units = coverageUnits(plan.units);
  if (out.kind !== "root") {
    if (
      out.interval.startOrdinal !== node.interval.startOrdinal ||
      out.interval.endOrdinal !== node.interval.endOrdinal
    )
      throw new RichFailure("model-output", node.role, "interval-mismatch");
  }
  if (
    out.kind === "leaf" &&
    out.sourceDigest !== intervalDigest(units, node.interval)
  )
    throw new RichFailure("model-output", "leaf", "source-digest-mismatch");
  if (out.kind === "merge") {
    const expected = node.childIndices.map((i) =>
      intervalDigest(units, plan.nodes[i].interval),
    );
    if (
      out.childDigests.length !== expected.length ||
      out.childDigests.some((d, i) => d !== expected[i])
    )
      throw new RichFailure("model-output", "merge", "child-digests-mismatch");
  }
  if (out.kind === "root" && out.coverageDigest !== plan.coverageDigest)
    throw new RichFailure("model-output", "root", "coverage-digest-mismatch");
}
