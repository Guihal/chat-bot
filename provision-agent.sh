#!/bin/sh
# Seed an isolated bot agent dir from bot-home/.
#
# Idempotent: bot-home/ is the source of truth for the persona and the MCP
# registration; auth.json/models.json stay manual (mode 600, never committed).
#
# Usage: provision-agent.sh AGENT_DIR [CODE_DIR]
#   AGENT_DIR  target agent dir (created if missing, e.g. ./state/agent)
#   CODE_DIR   where the chat-bot code of THIS checkout lives; defaults to the
#              directory holding this script
#
# Required environment (no defaults — a wrong value would point the bot at
# somebody else's chat):
#   CHATBOT_TOKEN_FILE  absolute path of the token file (mode 600)
#   CHATBOT_CHAT_ID     Telegram chat id the bot serves
# Optional:
#   CHATBOT_STATE_DIR   absolute state dir of the daemon (default: CODE_DIR/state)
set -eu

AGENT_DIR="${1:?usage: provision-agent.sh AGENT_DIR [CODE_DIR]}"
HERE="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
CODE_DIR="${2:-$HERE}"
STATE_DIR="${CHATBOT_STATE_DIR:-$CODE_DIR/state}"
: "${CHATBOT_TOKEN_FILE:?CHATBOT_TOKEN_FILE is required (absolute path of the token file, mode 600)}"
: "${CHATBOT_CHAT_ID:?CHATBOT_CHAT_ID is required (Telegram chat id, e.g. -1001234567890)}"

mkdir -p "$AGENT_DIR"
cp "$HERE/bot-home/AGENTS.md" "$AGENT_DIR/AGENTS.md"
cp "$HERE/bot-home/settings.json" "$AGENT_DIR/settings.json"
sed -e "s|__CODE_DIR__|$CODE_DIR|g" \
    -e "s|__STATE_DIR__|$STATE_DIR|g" \
    -e "s|__TOKEN_FILE__|$CHATBOT_TOKEN_FILE|g" \
    -e "s|__CHAT_ID__|$CHATBOT_CHAT_ID|g" \
    "$HERE/bot-home/mcp.json" > "$AGENT_DIR/mcp.json"
chmod 600 "$AGENT_DIR/mcp.json"
if grep -q '__[A-Z_]*__' "$AGENT_DIR/mcp.json"; then
    echo "provision-agent.sh: unsubstituted placeholder left in $AGENT_DIR/mcp.json" >&2
    exit 1
fi
echo "provisioned $AGENT_DIR from $HERE/bot-home (code at $CODE_DIR)"
