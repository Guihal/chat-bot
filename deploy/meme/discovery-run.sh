#!/bin/sh
set -eu

HERE=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
. "$HERE/common.sh"

meme_preflight discovery

unset CHATBOT_TOKEN_FILE TG_BOT_TOKEN TELEGRAM_BOT_TOKEN TG_NOTIFY_ENV MEME_BOT_TOKEN_FILE

PROFILE=${MEME_DISCOVERY_PROFILE:-/opt/omo-chat/state/meme-discovery-agent}
export OMO_CODING_AGENT_DIR="$PROFILE"
export MEME_DATA_DIR="$MEME_ROOT"
export MEME_SOURCES_PATH="${MEME_SOURCES_PATH:-$MEME_ROOT/meme-sources.md}"

PROMPT=$(cat "$HERE/discovery-prompt.md")

meme_run "${MEME_DISCOVERY_TIMEOUT:-300}" \
  "$OMO_BIN" -p "$PROMPT" \
  --provider opencode-go \
  --model hy4-flash-free \
  --thinking low \
  --no-session \
  --no-context-files \
  --no-builtin-tools \
  --extension "$PROFILE/extensions/discovery/index.ts" \
  --tools meme_sources_read,meme_preview_fetch,meme_candidate_save \
  --skill "$PROFILE/skills/task/SKILL.md"
