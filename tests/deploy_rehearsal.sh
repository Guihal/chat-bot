#!/bin/sh
# Proves a deploy leaves agent-authored files byte-identical.
#
# Temp dirs only: no /opt, no server, no service. The release list is copied as
# a deploy would copy code, then provision-agent.sh runs; system-prompt.md,
# agents.md, workspace md-inject.ts, the global AGENTS.md and knowledge
# documents must not change. A fresh profile still receives the md-inject seed.
set -eu

HERE=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
die() { echo "deploy rehearsal: $*" >&2; exit 1; }
same() { cmp -s "$1" "$2" || die "$1 changed during the deploy"; }

root=$(mktemp -d "${TMPDIR:-/tmp}/deploy-rehearsal.XXXXXX")
trap 'rm -rf "$root"' EXIT INT TERM
LIVE="$root/live"
AGENT="$root/agent"
FRESH="$root/fresh-agent"
KNOW="$root/knowledge"
mkdir -p "$LIVE" "$AGENT/extensions" "$FRESH/extensions" "$KNOW"

printf 'AGENT PROMPT\n' > "$LIVE/system-prompt.md"
printf 'AGENT WORKSPACE NOTES\n' > "$LIVE/agents.md"
printf 'AGENT WORKSPACE RULES\n' > "$LIVE/md-inject.ts"
printf 'AGENT GLOBAL NOTES\n' > "$AGENT/AGENTS.md"
printf 'AGENT RULES\n' > "$AGENT/extensions/md-inject.ts"
printf 'agent discourse\n' > "$KNOW/Дискурс.md"
printf 'agent persons\n' > "$KNOW/Важные персоны вне канала.md"
printf 'agent dictionary\n' > "$KNOW/Словарь.md"
printf 'agent legends\n' > "$KNOW/Список легенд(донатеров).md"
printf 'agent events\n' > "$KNOW/Список важных событий.md"
mkdir -p "$root/before"
cp -a "$LIVE/system-prompt.md" "$LIVE/agents.md" "$AGENT/AGENTS.md" "$root/before/"
cp -a "$LIVE/md-inject.ts" "$root/before/live-md-inject.ts"
cp -a "$AGENT/extensions/md-inject.ts" "$root/before/agent-md-inject.ts"
cp -a "$KNOW/." "$root/before/knowledge/"

LIST="$root/release.list"
sh "$HERE/deploy/release.sh" list --root "$HERE" > "$LIST" || die "release list failed"

for owned in 'system-prompt.md' 'bot-home/AGENTS.md' 'agents.md' 'md-inject.ts' 'knowledge/'; do
  if awk -F '\t' -v o="$owned" 'index($2, o) == 1 {found=1} END {exit !found}' "$LIST"; then
    die "release list still contains agent-owned $owned"
  fi
done

TAB=$(printf '\t')
while IFS="$TAB" read -r _label rel; do
  case "$rel" in /*) continue ;; esac
  src="$HERE/$rel"
  [ -e "$src" ] || die "release list names a missing file: $rel"
  mkdir -p "$LIVE/$(dirname -- "$rel")"
  cp -a "$src" "$LIVE/$rel"
done < "$LIST"

CHATBOT_CHAT_ID=-100TEST CHATBOT_STATE_DIR="$root/state" \
  sh "$HERE/provision-agent.sh" "$AGENT" "$HERE" > "$root/provision.out" 2>&1 ||
  die "provision failed: $(cat "$root/provision.out")"

CHATBOT_CHAT_ID=-100TEST CHATBOT_STATE_DIR="$root/state" \
  sh "$HERE/provision-agent.sh" "$FRESH" "$HERE" > "$root/provision-fresh.out" 2>&1 ||
  die "fresh provision failed: $(cat "$root/provision-fresh.out")"

same "$LIVE/system-prompt.md" "$root/before/system-prompt.md"
same "$LIVE/agents.md" "$root/before/agents.md"
same "$LIVE/md-inject.ts" "$root/before/live-md-inject.ts"
same "$AGENT/AGENTS.md" "$root/before/AGENTS.md"
same "$AGENT/extensions/md-inject.ts" "$root/before/agent-md-inject.ts"
for f in Дискурс.md 'Важные персоны вне канала.md' Словарь.md 'Список легенд(донатеров).md' 'Список важных событий.md'; do
  same "$KNOW/$f" "$root/before/knowledge/$f"
done
[ -f "$AGENT/mcp.json" ] || die "provision did not write mcp.json"
[ -f "$FRESH/extensions/md-inject.ts" ] || die "fresh profile did not receive the md-inject seed"
cmp -s "$HERE/md-inject.ts" "$FRESH/extensions/md-inject.ts" || die "fresh seed differs from source"
grep -q 'AGENT GLOBAL NOTES' "$AGENT/AGENTS.md" || die "global notes lost"

CRON="$root/cron"
sh "$HERE/deploy/install-knowledge-cron.sh" --root "$CRON" --now '2026-11-18T04:06:00Z' > "$root/cron-install.out" 2>&1 ||
  die "knowledge cron install failed: $(cat "$root/cron-install.out")"
python3 - "$CRON" <<'PY' || die "knowledge cron contract failed"
import datetime
import json
import os
import sys

root = sys.argv[1]
slugs = ["discourse", "persons", "dictionary", "donors", "events"]
minutes = set()
for slug in slugs:
    job_path = os.path.join(root, "jobs", f"knowledge-{slug}.json")
    with open(job_path, encoding="utf-8") as handle:
        job = json.load(handle)
    assert isinstance(job.get("schedule"), dict), f"{slug}: schedule must be an object"
    assert job["schedule"].get("type") == "interval", f"{slug}: schedule type"
    assert job["schedule"].get("minutes") == 60, f"{slug}: interval minutes"
    assert isinstance(job.get("command"), list) and job["command"], f"{slug}: command array"
    assert "--doc" in " ".join(job["command"]), f"{slug}: command must pass --doc"
    assert job.get("cwd") == "/opt/omo-server/workspace/chat-bot", f"{slug}: cwd"
    assert job.get("timeout") == 660, f"{slug}: timeout"
    assert job.get("deliver") == "none", f"{slug}: deliver"
    assert job.get("chat") is None, f"{slug}: chat"
    assert job.get("enabled") is True, f"{slug}: enabled"
    state_path = os.path.join(root, "state", f"knowledge-{slug}.json")
    with open(state_path, encoding="utf-8") as handle:
        state = json.load(handle)
    last = datetime.datetime.fromisoformat(state["last_run"])
    assert last.tzinfo is not None, f"{slug}: last_run must be aware"
    minutes.add(last.minute)
    assert state.get("last_status") == 0, f"{slug}: last_status"
    assert state.get("last_log", "").endswith(f"knowledge-{slug}/latest.log"), f"{slug}: last_log"
assert minutes == {5, 17, 29, 41, 53}, f"stagger minutes: {sorted(minutes)}"
PY
before_state=$(sha256sum "$CRON"/state/*.json | sha256sum)
sh "$HERE/deploy/install-knowledge-cron.sh" --root "$CRON" --now '2026-11-18T04:06:00Z' >/dev/null 2>&1 ||
  die "knowledge cron second install failed"
after_state=$(sha256sum "$CRON"/state/*.json | sha256sum)
[ "$before_state" = "$after_state" ] || die "knowledge cron install is not idempotent"
for f in Дискурс.md 'Важные персоны вне канала.md' Словарь.md 'Список легенд(донатеров).md' 'Список важных событий.md'; do
  same "$KNOW/$f" "$root/before/knowledge/$f"
done

echo "DEPLOY_REHEARSAL_OK live_files_untouched=9 fresh_seed=ok cron_install=ok code_copied=$(wc -l < "$LIST")"
