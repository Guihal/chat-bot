#!/bin/sh
# Seed an isolated bot agent dir from bot-home/.
#
# Idempotent: bot-home/ supplies the MCP registration, settings, compactors and
# code extensions; the global AGENTS.md is maintained by hand and never written
# here. auth.json/models.json stay manual (mode 600, never committed).
#
# Usage: provision-agent.sh AGENT_DIR [CODE_DIR]
#   AGENT_DIR  target agent dir (created if missing, e.g. ./state/agent)
#   CODE_DIR   where the chat-bot code of THIS checkout lives; defaults to the
#              directory holding this script
#
# Required environment (no defaults — a wrong value would point the bot at
# somebody else's chat):
#   CHATBOT_CHAT_ID     Telegram chat id the bot serves
# Optional:
#   CHATBOT_STATE_DIR   absolute state dir of the daemon (default: CODE_DIR/state)
#
# The agent profile never receives the Bot API token: tg-chat talks to the
# daemon's tool socket for media, and the daemon owns the token.
#
# The global AGENTS.md of the profile is NOT written here: it belongs to the
# agent and is maintained by hand.
set -eu

AGENT_DIR="${1:?usage: provision-agent.sh AGENT_DIR [CODE_DIR]}"
HERE="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
CODE_DIR="${2:-$HERE}"
STATE_DIR="${CHATBOT_STATE_DIR:-$CODE_DIR/state}"
: "${CHATBOT_CHAT_ID:?CHATBOT_CHAT_ID is required (Telegram chat id, e.g. -1001234567890)}"

mkdir -p "$AGENT_DIR"
if [ -f "$AGENT_DIR/settings.json" ] && ! cmp -s "$HERE/bot-home/settings.json" "$AGENT_DIR/settings.json"; then
  cp -a "$AGENT_DIR/settings.json" "$AGENT_DIR/settings.json.bak-$(date +%Y%m%d-%H%M%S)"
fi
cp "$HERE/bot-home/settings.json" "$AGENT_DIR/settings.json"
sed -e "s|__CODE_DIR__|$CODE_DIR|g" \
    -e "s|__STATE_DIR__|$STATE_DIR|g" \
    -e "s|__CHAT_ID__|$CHATBOT_CHAT_ID|g" \
    "$HERE/bot-home/mcp.json" > "$AGENT_DIR/mcp.json"
chmod 600 "$AGENT_DIR/mcp.json"
if grep -q '__[A-Z_]*__' "$AGENT_DIR/mcp.json"; then
    echo "provision-agent.sh: unsubstituted placeholder left in $AGENT_DIR/mcp.json" >&2
    exit 1
fi
# The hosted runtime discovers <agentDir>/extensions/*.js by itself (no
# launch-spec entry), so the durable chat-context extension is staged there. The
# repo source is an ESM .mjs; only .js/.ts names are discovered.
mkdir -p "$AGENT_DIR/extensions"
cp "$HERE/chat-control-task.mjs" "$AGENT_DIR/extensions/chat-control-task.js"
chmod 644 "$AGENT_DIR/extensions/chat-control-task.js"
# md-inject stages the knowledge docs and the operating rules into the chat
# turn's system prompt. The live file is agent-authored state: seed it only when
# the profile has no file, and never overwrite an existing rule block. A
# checkout used as a release fixture may have no source next to this script:
# warn loudly instead of failing the provision, and verify the installed file
# separately by `file`/`sha256sum`.
if [ -e "$AGENT_DIR/extensions/md-inject.ts" ]; then
  echo "keep existing md-inject.ts: agent-authored rule block is not overwritten"
elif [ -f "$HERE/md-inject.ts" ]; then
  cp "$HERE/md-inject.ts" "$AGENT_DIR/extensions/md-inject.ts"
  chmod 644 "$AGENT_DIR/extensions/md-inject.ts"
else
  echo "WARN md-inject.ts missing next to provision-agent.sh: extension NOT installed" >&2
fi
# The sampling defaults (ported from the omo profile) are discovered from the
# same directory. Both files travel together: the entry imports the projection
# module by a flat relative path. The request-local retry extension is flat too.
if [ -f "$HERE/bot-home/extensions/default-min-temperature.ts" ] && [ -f "$HERE/bot-home/extensions/sampling-payload.ts" ] && [ -f "$HERE/bot-home/extensions/alibaba-output-retry-port.ts" ]; then
  cp "$HERE/bot-home/extensions/default-min-temperature.ts" "$AGENT_DIR/extensions/default-min-temperature.ts"
  cp "$HERE/bot-home/extensions/sampling-payload.ts" "$AGENT_DIR/extensions/sampling-payload.ts"
  cp "$HERE/bot-home/extensions/alibaba-output-retry-port.ts" "$AGENT_DIR/extensions/alibaba-output-retry-port.ts"
  chmod 644 "$AGENT_DIR/extensions/default-min-temperature.ts" "$AGENT_DIR/extensions/sampling-payload.ts" "$AGENT_DIR/extensions/alibaba-output-retry-port.ts"
else
  echo "WARN bot-home/extensions profile files missing next to provision-agent.sh: sampling/retry defaults NOT installed" >&2
fi
# The chat compactor reads its contract from <agentDir>/compactors/. Both files are
# shipped verbatim: rich.json is the strict role config, rich.md is the model
# prompt and must keep its exact bytes (no trailing newline).
mkdir -p "$AGENT_DIR/compactors"
cp "$HERE/bot-home/compactors/rich.json" "$AGENT_DIR/compactors/rich.json"
cp "$HERE/bot-home/compactors/rich.md" "$AGENT_DIR/compactors/rich.md"
chmod 644 "$AGENT_DIR/compactors/rich.json" "$AGENT_DIR/compactors/rich.md"
echo "provisioned $AGENT_DIR from $HERE/bot-home (code at $CODE_DIR)"
