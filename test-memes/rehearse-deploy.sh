#!/bin/sh
set -eu

HERE=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
REPO=$(CDPATH='' cd -- "$HERE/.." && pwd)
ROOT=$(mktemp -d "${TMPDIR:-/tmp}/meme-deploy.XXXXXX")
trap 'rm -rf "$ROOT"' EXIT INT TERM

NOW=2026-11-18T22:00:00Z
sh "$REPO/deploy/meme/install-memes-cron.sh" --root "$ROOT/cron" --now "$NOW" --no-env

python3 - "$ROOT/cron" <<'PY'
import datetime
import json
import os
import sys

root = sys.argv[1]
expected = {"discovery": (60, 600, 47), "publisher": (120, 420, 7)}
for role, (minutes, timeout, minute) in expected.items():
    job = json.load(open(os.path.join(root, "jobs", f"meme-{role}.json"), encoding="utf-8"))
    assert job["schedule"] == {"type": "interval", "minutes": minutes}, job["schedule"]
    assert job["timeout"] == timeout, job["timeout"]
    assert job["enabled"] is False, job["enabled"]
    command = " ".join(job["command"])
    assert f"{role}-run.sh" in command, command

    state = json.load(open(os.path.join(root, "state", f"meme-{role}.json"), encoding="utf-8"))
    last = datetime.datetime.fromisoformat(state["last_run"])
    assert last.minute == minute, state["last_run"]
print("CRON_JOBS_OK")
PY

before=$(cat "$ROOT/cron/state/meme-discovery.json")
sh "$REPO/deploy/meme/install-memes-cron.sh" --root "$ROOT/cron" --now "$NOW" --no-env --enable

python3 - "$ROOT/cron" <<'PY'
import json
import os
import sys

root = sys.argv[1]
for role in ("discovery", "publisher"):
    job = json.load(open(os.path.join(root, "jobs", f"meme-{role}.json"), encoding="utf-8"))
    assert job["enabled"] is True, job["enabled"]
print("CRON_ENABLE_OK")
PY

after=$(cat "$ROOT/cron/state/meme-discovery.json")
[ "$before" = "$after" ] || {
  echo "FAIL state was reseeded without --reseed" >&2
  exit 1
}

echo "REHEARSE_DEPLOY_OK"
