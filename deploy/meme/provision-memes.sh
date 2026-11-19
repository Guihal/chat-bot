#!/bin/sh
set -eu

HERE=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
MEME_ROOT=${MEME_ROOT:-/opt/omo-chat/data/memes}
BOT_PROFILE=${BOT_PROFILE:-/opt/omo-chat/state/agent}
DISCOVERY_PROFILE=${MEME_DISCOVERY_PROFILE:-/opt/omo-chat/state/meme-discovery-agent}
PUBLISHER_PROFILE=${MEME_PUBLISHER_PROFILE:-/opt/omo-chat/state/meme-publisher-agent}

[ -f "$BOT_PROFILE/auth.json" ] || { echo "provision-memes: missing bot auth.json" >&2; exit 1; }

for role in discovery publisher; do
  case "$role" in
    discovery) profile=$DISCOVERY_PROFILE ;;
    publisher) profile=$PUBLISHER_PROFILE ;;
  esac
  mkdir -p "$profile/extensions/lib" "$profile/extensions/$role" "$profile/skills/task" "$profile/logs"
  cp "$HERE/profiles/$role/settings.json" "$profile/settings.json"
  cp "$HERE/profiles/$role/models.json" "$profile/models.json"
  cp "$BOT_PROFILE/auth.json" "$profile/auth.json"
  chmod 600 "$profile/auth.json"
  cp "$HERE/extensions/lib/"*.ts "$profile/extensions/lib/"
  cp "$HERE/extensions/$role/index.ts" "$profile/extensions/$role/index.ts"
  cp "$HERE/extensions/$role/package.json" "$profile/extensions/$role/package.json"
  cp "$HERE/profiles/$role/default-min-temperature.ts" "$profile/extensions/default-min-temperature.ts"
  # The projection helper is imported by default-min-temperature.ts, not an
  # extension entry. Keep it under lib/ so the loader does not try to call a
  # named-only export as a factory; remove the legacy flat copy if present.
  rm -f "$profile/extensions/sampling-payload.ts"
  cp "$HERE/profiles/$role/sampling-payload.ts" "$profile/extensions/lib/sampling-payload.ts"
  cp "$HERE/skills/task/SKILL.md" "$profile/skills/task/SKILL.md"
done

mkdir -p "$MEME_ROOT/candidates" "$MEME_ROOT/published" "$MEME_ROOT/failed" \
  "$MEME_ROOT/locks" "$MEME_ROOT/alerts"
chmod 700 "$MEME_ROOT"

if [ ! -e "$MEME_ROOT/meme-sources.md" ]; then
  cp "$HERE/meme-sources.md" "$MEME_ROOT/meme-sources.md"
fi

echo "PROVISION_MEMES_OK root=$MEME_ROOT discovery=$DISCOVERY_PROFILE publisher=$PUBLISHER_PROFILE"
