import { homedir } from "node:os";
import { join, resolve } from "node:path";

/**
 * Agent directory whose `compactors/rich.{json,md}` the bot's compactor reads.
 *
 * The daemon exports `OMO_CODING_AGENT_DIR` for its own profile, so a session
 * that runs with no override at all is a configuration bug, not a case to
 * paper over: the `~/.omo/agent` fallback only keeps a hand-started local host
 * usable. There is no desktop-specific guard here — this tree ships inside the
 * bot, where the pi agent directory is never the target.
 */
export function agentDir(): string {
  const value =
    process.env.OMO_CODING_AGENT_DIR ||
    process.env.SENPI_CODING_AGENT_DIR ||
    process.env.PI_CODING_AGENT_DIR ||
    join(homedir(), ".omo", "agent");
  return resolve(value);
}
