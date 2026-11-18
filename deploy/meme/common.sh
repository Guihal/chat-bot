#!/bin/sh
set -eu

MEME_ROOT=${MEME_ROOT:-/opt/omo-chat/data/memes}
MEME_LOG_DIR=${MEME_LOG_DIR:-$MEME_ROOT/logs}
MEME_ENABLED=${MEME_ENABLED:-true}
MEME_MIN_MEM_MIB=${MEME_MIN_MEM_MIB:-1500}
MEME_MIN_DISK_MIB=${MEME_MIN_DISK_MIB:-500}
OMO_BIN=${OMO_BIN:-$(command -v omo || true)}
MEME_DISCOVERY_PROFILE=${MEME_DISCOVERY_PROFILE:-/opt/omo-chat/state/meme-discovery-agent}
MEME_PUBLISHER_PROFILE=${MEME_PUBLISHER_PROFILE:-/opt/omo-chat/state/meme-publisher-agent}

mkdir -p "$MEME_ROOT" "$MEME_LOG_DIR" "$MEME_ROOT/locks" "$MEME_ROOT/alerts"

meme_log() {
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >&2
}

meme_alert_once() {
  kind=$1
  shift
  day=$(date -u +%Y-%m-%d)
  marker="$MEME_ROOT/alerts/$day-$kind"
  [ -e "$marker" ] && return 0
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >"$marker"
  if [ -x /root/bin/tg-notify ]; then
    /root/bin/tg-notify "$*" >/dev/null 2>&1 || true
  fi
}

meme_mem_available_mib() {
  if [ -n "${MEME_TEST_MEM_MIB:-}" ]; then
    printf '%s\n' "$MEME_TEST_MEM_MIB"
    return 0
  fi
  awk '/^MemAvailable:/ { print int($2 / 1024); exit }' /proc/meminfo
}

meme_disk_free_mib() {
  if [ -n "${MEME_TEST_DISK_MIB:-}" ]; then
    printf '%s\n' "$MEME_TEST_DISK_MIB"
    return 0
  fi
  df -Pk "$MEME_ROOT" | awk 'NR == 2 { print int($4 / 1024) }'
}

meme_active_job() {
  pgrep -af 'chatd knowledge|meme-discovery-agent|meme-publisher-agent' >/dev/null 2>&1
}

meme_check_resources() {
  mem=$(meme_mem_available_mib)
  disk=$(meme_disk_free_mib)
  if [ "${mem:-0}" -lt "$MEME_MIN_MEM_MIB" ]; then
    meme_alert_once resource "meme run skipped: MemAvailable ${mem} MiB < ${MEME_MIN_MEM_MIB} MiB"
    meme_log "preflight memory ${mem} MiB below ${MEME_MIN_MEM_MIB} MiB"
    exit 75
  fi
  if [ "${disk:-0}" -lt "$MEME_MIN_DISK_MIB" ]; then
    meme_alert_once resource "meme run skipped: disk free ${disk} MiB < ${MEME_MIN_DISK_MIB} MiB"
    meme_log "preflight disk ${disk} MiB below ${MEME_MIN_DISK_MIB} MiB"
    exit 75
  fi
}

meme_preflight() {
  role=$1
  if [ "$MEME_ENABLED" = "false" ]; then
    printf 'NO_NEW\n'
    exit 0
  fi
  if [ -z "${OMO_BIN:-}" ] || [ ! -x "$OMO_BIN" ]; then
    meme_log "omo binary not found"
    exit 127
  fi
  if meme_active_job; then
    meme_alert_once busy "meme $role skipped: another knowledge/meme run is active"
    meme_log "preflight active job detected"
    exit 75
  fi
  meme_check_resources
  sleep $(( ($$ % 3) + 2 ))
  if meme_active_job; then
    meme_alert_once busy "meme $role skipped after double-check: another run started"
    meme_log "preflight active job detected on second check"
    exit 75
  fi
  meme_check_resources

  lock="$MEME_ROOT/locks/$role.lock"
  exec 9>"$lock"
  if command -v flock >/dev/null 2>&1; then
    flock -n 9 || { meme_log "lock busy: $lock"; exit 75; }
  fi

  export MEME_DATA_DIR="$MEME_ROOT"
  export MEME_SOURCES_PATH="${MEME_SOURCES_PATH:-$MEME_ROOT/meme-sources.md}"
}

meme_run() {
  seconds=$1
  shift
  if [ "${MEME_NO_SYSTEMD_RUN:-0}" != "1" ] && command -v systemd-run >/dev/null 2>&1; then
    nice -n 19 systemd-run --scope --quiet \
      -p MemoryMax=512M \
      -p CPUQuota=50% \
      -- timeout "$seconds" "$@"
    return $?
  fi
  if command -v ionice >/dev/null 2>&1; then
    nice -n 19 ionice -c3 timeout "$seconds" "$@"
    return $?
  fi
  nice -n 19 timeout "$seconds" "$@"
}
