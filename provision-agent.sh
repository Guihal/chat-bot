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
#   CHATBOT_CHAT_ID     Telegram chat id the bot serves
# Optional:
#   CHATBOT_STATE_DIR   absolute state dir of the daemon (default: CODE_DIR/state)
#
# The agent profile never receives the Bot API token: tg-chat talks to the
# daemon's tool socket for media, and the daemon owns the token.
set -eu

# Merge the repo profile into an existing one instead of overwriting it.
# The repo part lives inside the begin/end markers and is replaced on every
# provision; everything outside them is accumulated in the live profile by the
# bot itself and is preserved verbatim.
merge_profile() {
  src="$1"; dst="$2"
  if [ ! -f "$dst" ]; then cp "$src" "$dst"; return; fi
  if cmp -s "$src" "$dst"; then return; fi
  tmp="$dst.merge.$$"
  {
    printf '<!-- repo-profile:begin (managed: replaced on every provision) -->\n'
    cat "$src"
    printf '\n<!-- repo-profile:end -->\n\n'
    awk 'BEGIN{skip=0; started=0}
         /<!-- repo-profile:begin/{skip=1;next}
         /<!-- repo-profile:end/{skip=0;next}
         skip==1{next}
         started==0 && $0==""{next}
         {started=1; print}' "$dst"
  } > "$tmp"
  if cmp -s "$tmp" "$dst"; then
    rm -f "$tmp"
    return
  fi
  cp -a "$dst" "$dst.bak-$(date +%Y%m%d-%H%M%S)"
  mv "$tmp" "$dst"
}


AGENT_DIR="${1:?usage: provision-agent.sh AGENT_DIR [CODE_DIR]}"
HERE="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
CODE_DIR="${2:-$HERE}"
STATE_DIR="${CHATBOT_STATE_DIR:-$CODE_DIR/state}"
: "${CHATBOT_CHAT_ID:?CHATBOT_CHAT_ID is required (Telegram chat id, e.g. -1001234567890)}"

mkdir -p "$AGENT_DIR"
merge_profile "$HERE/bot-home/AGENTS.md" "$AGENT_DIR/AGENTS.md"
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
echo "provisioned $AGENT_DIR from $HERE/bot-home (code at $CODE_DIR)"
