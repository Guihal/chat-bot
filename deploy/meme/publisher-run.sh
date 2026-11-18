#!/bin/sh
set -eu

HERE=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
CALLER_MEME_ENABLED=${MEME_ENABLED:-}
if [ -f /etc/omo-chat/meme.env ]; then
  set -a
  . /etc/omo-chat/meme.env
  set +a
fi
if [ -n "$CALLER_MEME_ENABLED" ]; then
  export MEME_ENABLED="$CALLER_MEME_ENABLED"
fi

. "$HERE/common.sh"

meme_preflight publisher

PROFILE=${MEME_PUBLISHER_PROFILE:-/opt/omo-chat/state/meme-publisher-agent}
export OMO_CODING_AGENT_DIR="$PROFILE"
export MEME_DATA_DIR="$MEME_ROOT"
export MEME_SOURCES_PATH="${MEME_SOURCES_PATH:-$MEME_ROOT/meme-sources.md}"

PROMPT=$(cat "$HERE/publisher-prompt.md")

meme_run "${MEME_PUBLISHER_TIMEOUT:-240}" \
  "$OMO_BIN" -p "$PROMPT" \
  --provider opencode-go \
  --model hy4-flash-free \
  --thinking low \
  --no-session \
  --no-context-files \
  --no-builtin-tools \
  --extension "$PROFILE/extensions/publisher/index.ts" \
  --tools meme_candidates_list,meme_publish \
  --skill "$PROFILE/skills/task/SKILL.md"
