#!/bin/sh
set -eu

HERE=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)

# shellcheck source=common.sh
. "$HERE/common.sh"

meme_preflight publisher

PROFILE=${MEME_PUBLISHER_PROFILE:-/opt/omo-chat/state/meme-publisher-agent}
export OMO_CODING_AGENT_DIR="$PROFILE"
export MEME_DATA_DIR="$MEME_ROOT"
export MEME_SOURCES_PATH="${MEME_SOURCES_PATH:-$MEME_ROOT/meme-sources.md}"

PROMPT=$(cat "$HERE/publisher-prompt.md")

set +e
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
status=$?
set -e
if [ "$status" -ne 0 ]; then
  meme_alert_once run-failed-publisher "meme publisher exited $status"
  exit "$status"
fi
