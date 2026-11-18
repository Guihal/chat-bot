#!/bin/sh
set -eu

HERE=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
REPO=$(CDPATH='' cd -- "$HERE/.." && pwd)
ROOT=$(mktemp -d "${TMPDIR:-/tmp}/meme-preserve.XXXXXX")
trap 'rm -rf "$ROOT"' EXIT INT TERM

BOT="$ROOT/bot-profile"
WORK="$ROOT/workspace"
KNOW="$ROOT/knowledge"
mkdir -p "$BOT/extensions" "$WORK" "$KNOW"

printf 'auth-sentinel\n' >"$BOT/auth.json"
chmod 600 "$BOT/auth.json"
printf 'system-sentinel\n' >"$WORK/system-prompt.md"
printf 'agents-sentinel\n' >"$WORK/agents.md"
printf 'profile-agents-sentinel\n' >"$BOT/AGENTS.md"
printf 'md-inject-sentinel\n' >"$BOT/extensions/md-inject.ts"
printf 'knowledge-sentinel\n' >"$KNOW/Дискурс.md"

assert_file() {
  [ "$(cat "$1")" = "$2" ] || {
    printf 'FAIL preserved file changed: %s\n' "$1" >&2
    exit 1
  }
}

MEME_ROOT="$ROOT/memes" \
BOT_PROFILE="$BOT" \
MEME_DISCOVERY_PROFILE="$ROOT/discovery-profile" \
MEME_PUBLISHER_PROFILE="$ROOT/publisher-profile" \
sh "$REPO/deploy/meme/provision-memes.sh"

assert_file "$WORK/system-prompt.md" "system-sentinel"
assert_file "$WORK/agents.md" "agents-sentinel"
assert_file "$BOT/AGENTS.md" "profile-agents-sentinel"
assert_file "$BOT/extensions/md-inject.ts" "md-inject-sentinel"
assert_file "$KNOW/Дискурс.md" "knowledge-sentinel"

[ -f "$ROOT/memes/meme-sources.md" ] || {
  echo "FAIL meme-sources was not seeded on a fresh root" >&2
  exit 1
}
printf '\n# agent-added\nhttps://t.me/new_source\n' >>"$ROOT/memes/meme-sources.md"

MEME_ROOT="$ROOT/memes" \
BOT_PROFILE="$BOT" \
MEME_DISCOVERY_PROFILE="$ROOT/discovery-profile" \
MEME_PUBLISHER_PROFILE="$ROOT/publisher-profile" \
sh "$REPO/deploy/meme/provision-memes.sh"

grep -q 'agent-added' "$ROOT/memes/meme-sources.md" || {
  echo "FAIL provisioning overwrote the agent-edited meme sources" >&2
  exit 1
}

if grep -Eq 'deploy/meme|meme-sources' "$REPO/deploy/release.sh" "$REPO/provision-agent.sh"; then
  echo "FAIL release/provision still references meme mutable paths" >&2
  exit 1
fi

echo "REHEARSE_PRESERVE_OK"
