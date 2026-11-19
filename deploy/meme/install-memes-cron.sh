#!/bin/sh
set -eu

HERE=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
ROOT=
NOW=
RESEED=0
ENABLE=0
TOKEN_FILE=
NO_ENV=0

while [ $# -gt 0 ]; do
  case "$1" in
    --root) ROOT=$2; shift 2 ;;
    --now) NOW=$2; shift 2 ;;
    --reseed) RESEED=1; shift ;;
    --enable) ENABLE=1; shift ;;
    --token-file) TOKEN_FILE=$2; shift 2 ;;
    --no-env) NO_ENV=1; shift ;;
    *) echo "install-memes-cron: unknown argument: $1" >&2; exit 2 ;;
  esac
done

[ -n "$ROOT" ] || { echo "install-memes-cron: --root is required" >&2; exit 2; }
[ -n "$NOW" ] || NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)

mkdir -p "$ROOT/jobs" "$ROOT/state" "$ROOT/logs/meme-discovery" "$ROOT/logs/meme-publisher"

python3 - "$ROOT" "$HERE" "$NOW" "$RESEED" "$ENABLE" <<'PY'
import datetime
import json
import os
import shlex
import sys

root, here, raw_now, reseed_raw, enable_raw = sys.argv[1:6]
reseed = reseed_raw == "1"
enabled = enable_raw == "1"
try:
    now = datetime.datetime.fromisoformat(raw_now.replace("Z", "+00:00"))
except ValueError as exc:
    raise SystemExit(f"install-memes-cron: bad --now: {exc}")
if now.tzinfo is None:
    raise SystemExit("install-memes-cron: --now must be timezone-aware")
now = now.astimezone(datetime.timezone.utc)

roles = [
    ("discovery", 60, 600, 47),
    ("publisher", 120, 420, 7),
]
for role, minutes, timeout, minute in roles:
    log_dir = os.path.join(root, "logs", f"meme-{role}")
    os.makedirs(log_dir, exist_ok=True)
    runner = os.path.join(here, f"{role}-run.sh")
    command = (
        "set -o pipefail; "
        + shlex.quote(runner)
        + " 2>&1 | tee -a "
        + shlex.quote(os.path.join(log_dir, "run.log"))
    )
    job = {
        "schedule": {"type": "interval", "minutes": minutes},
        "command": ["/bin/bash", "-c", command],
        "cwd": here,
        "timeout": timeout,
        "deliver": "none",
        "chat": None,
        "enabled": enabled,
    }
    with open(os.path.join(root, "jobs", f"meme-{role}.json"), "w", encoding="utf-8") as out:
        json.dump(job, out, ensure_ascii=False, indent=2)
        out.write("\n")

    state_path = os.path.join(root, "state", f"meme-{role}.json")
    if os.path.exists(state_path) and not reseed:
        continue
    candidate = now.replace(minute=minute, second=0, microsecond=0)
    if candidate > now:
        candidate -= datetime.timedelta(hours=1)
    state = {
        "last_run": candidate.isoformat(),
        "last_status": 0,
        "last_log": os.path.join(log_dir, "latest.log"),
    }
    with open(state_path, "w", encoding="utf-8") as out:
        json.dump(state, out, ensure_ascii=False, indent=2)
        out.write("\n")
PY

if [ "$NO_ENV" = "0" ]; then
  if [ -z "$TOKEN_FILE" ]; then
    TOKEN_FILE=$(python3 - <<'PY'
import re
import subprocess

env = subprocess.run(
    ["systemctl", "show", "omo-chat.service", "-p", "Environment"],
    capture_output=True,
    text=True,
).stdout
match = re.search(r"CHATBOT_TOKEN_FILE=([^\s]+)", env)
print(match.group(1).strip("\"'") if match else "")
PY
)
  fi
  [ -n "$TOKEN_FILE" ] || { echo "install-memes-cron: cannot resolve CHATBOT_TOKEN_FILE; pass --token-file or --no-env" >&2; exit 1; }
  mkdir -p /etc/omo-chat
  umask 077
  {
    printf 'MEME_BOT_TOKEN_FILE=%s\n' "$TOKEN_FILE"
    printf 'MEME_ENABLED=true\n'
    printf 'MEME_MAX_PUBLISH_PER_DAY=3\n'
  } > /etc/omo-chat/meme.env.tmp
  chmod 600 /etc/omo-chat/meme.env.tmp
  mv /etc/omo-chat/meme.env.tmp /etc/omo-chat/meme.env
fi

echo "INSTALL_MEMES_CRON_OK root=$ROOT now=$NOW reseed=$RESEED enable=$ENABLE"
