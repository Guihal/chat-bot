import type { Usage } from "@earendil-works/pi-ai";

/** Return a detached typed usage value; generation results never share provider state. */
export function copyUsage(usage: Usage): Usage {
  return Object.freeze({
    ...usage,
    cost: Object.freeze({ ...usage.cost }),
  });
}

export function zeroUsage(): Usage {
  return copyUsage({
    input: 0,
    output: 0,
    cacheRead: 0,
    cacheWrite: 0,
    totalTokens: 0,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  });
}

/** Field-wise sum of two usage values; the running total of a multi-call tree. */
export function addUsage(a: Usage, b: Usage): Usage {
  return {
    input: a.input + b.input,
    output: a.output + b.output,
    cacheRead: a.cacheRead + b.cacheRead,
    cacheWrite: a.cacheWrite + b.cacheWrite,
    totalTokens: a.totalTokens + b.totalTokens,
    cost: {
      input: a.cost.input + b.cost.input,
      output: a.cost.output + b.cost.output,
      cacheRead: a.cost.cacheRead + b.cost.cacheRead,
      cacheWrite: a.cost.cacheWrite + b.cost.cacheWrite,
      total: a.cost.total + b.cost.total,
    },
  };
}

/**
 * Accumulates every model call's usage so the returned compaction usage is the
 * exact sum of the calls it made (invariant 11) — no call is silently dropped or
 * double-counted. `assertEquals` is the self-check the service runs before return.
 */
export class UsageLedger {
  private running = zeroUsage();
  private count = 0;

  add(usage: Usage): void {
    this.running = addUsage(this.running, usage);
    this.count += 1;
  }

  total(): Usage {
    return copyUsage(this.running);
  }

  calls(): number {
    return this.count;
  }

  assertEquals(returned: Usage): void {
    if (
      returned.input !== this.running.input ||
      returned.output !== this.running.output ||
      returned.totalTokens !== this.running.totalTokens ||
      returned.cost.total !== this.running.cost.total
    )
      throw new Error("usage-ledger: returned usage != sum of calls");
  }
}
