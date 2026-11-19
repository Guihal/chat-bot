#!/bin/sh
set -eu

HERE=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
# shellcheck source=common.sh
. "$HERE/common.sh"

meme_preflight discovery

PROFILE=${MEME_DISCOVERY_PROFILE:-/opt/omo-chat/state/meme-discovery-agent}
export OMO_CODING_AGENT_DIR="$PROFILE"
export MEME_DATA_DIR="$MEME_ROOT"
export MEME_SOURCES_PATH="${MEME_SOURCES_PATH:-$MEME_ROOT/meme-sources.md}"

PROMPT=$(cat "$HERE/discovery-prompt.md")

set +e
meme_run "${MEME_DISCOVERY_TIMEOUT:-300}" \
  env -u CHATBOT_TOKEN_FILE -u TG_BOT_TOKEN -u TELEGRAM_BOT_TOKEN -u MEME_BOT_TOKEN_FILE \
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
status=$?
set -e
if [ "$status" -ne 0 ]; then
  meme_alert_once run-failed-discovery "meme discovery exited $status"
  exit "$status"
fi
