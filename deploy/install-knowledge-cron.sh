#!/bin/sh
# Install the five per-document knowledge jobs into an omo-cron root.
#
# The installed omo-cron has interval and daily schedules only; there is no
# minute anchor. The hourly stagger therefore lives in the job's state file:
# each `last_run` is seeded at a distinct minute (05/17/29/41/53) and the next
# interval run inherits that phase. State files are kept on re-run unless
# `--reseed` is explicitly given.
#
# Usage: install-knowledge-cron.sh --root DIR [--now ISO] [--reseed]
set -eu

HERE=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
ROOT=
NOW=
RESEED=0
while [ $# -gt 0 ]; do
  case "$1" in
    --root) ROOT=$2; shift 2 ;;
    --now) NOW=$2; shift 2 ;;
    --reseed) RESEED=1; shift ;;
    *) echo "install-knowledge-cron: unknown argument: $1" >&2; exit 2 ;;
  esac
done
[ -n "$ROOT" ] || { echo "install-knowledge-cron: --root is required" >&2; exit 2; }
[ -n "$NOW" ] || NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)

case "$RESEED" in 1) RESEED_ENV=1 ;; *) RESEED_ENV=0 ;; esac

mkdir -p "$ROOT/jobs" "$ROOT/state" "$ROOT/logs"
for slug in discourse persons dictionary donors events; do
  src="$HERE/omo-cron-knowledge/knowledge-$slug.json"
  [ -f "$src" ] || { echo "install-knowledge-cron: missing $src" >&2; exit 1; }
  cp "$src" "$ROOT/jobs/knowledge-$slug.json"
  mkdir -p "$ROOT/logs/knowledge-$slug"
done

python3 - "$ROOT" "$NOW" "$RESEED_ENV" <<'PY'
import datetime
import json
import os
import sys

root, raw_now, reseed = sys.argv[1], sys.argv[2], sys.argv[3] == "1"
try:
    now = datetime.datetime.fromisoformat(raw_now.replace("Z", "+00:00"))
except ValueError as exc:
    raise SystemExit(f"install-knowledge-cron: bad --now: {exc}")
if now.tzinfo is None:
    raise SystemExit("install-knowledge-cron: --now must be timezone-aware")
now = now.astimezone(datetime.timezone.utc)

slots = [("discourse", 5), ("persons", 17), ("dictionary", 29), ("donors", 41), ("events", 53)]
for slug, minute in slots:
    path = os.path.join(root, "state", f"knowledge-{slug}.json")
    if os.path.exists(path) and not reseed:
        continue
    candidate = now.replace(minute=minute, second=0, microsecond=0)
    if candidate > now:
        candidate -= datetime.timedelta(hours=1)
    state = {
        "last_run": candidate.isoformat(),
        "last_status": 0,
        "last_log": os.path.join(root, "logs", f"knowledge-{slug}", "latest.log"),
    }
    with open(path, "w", encoding="utf-8") as out:
        json.dump(state, out, ensure_ascii=False, indent=2)
        out.write("\n")
PY

echo "INSTALL_KNOWLEDGE_CRON_OK root=$ROOT now=$NOW reseed=$RESEED_ENV"
