#!/bin/sh
set -eu

HERE=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
COMMON_DIR=$(CDPATH='' cd -- "$HERE/../deploy/meme" && pwd)
ROOT=$(mktemp -d "${TMPDIR:-/tmp}/meme-common-test.XXXXXX")
trap 'rm -rf "$ROOT"' EXIT INT TERM

export COMMON_DIR
export MEME_ROOT="$ROOT/data"
export MEME_LOG_DIR="$ROOT/logs"
export OMO_BIN=/bin/true
export MEME_MIN_MEM_MIB=1500
export MEME_MIN_DISK_MIB=500

run_case() {
  description=$1
  expected=$2
  shift 2
  set +e
  output=$(env "$@" sh -c ". \"\$COMMON_DIR/common.sh\"; meme_preflight discovery" 2>&1)
  code=$?
  set -e
  if [ "$code" != "$expected" ]; then
    printf 'FAIL %s: expected exit %s, got %s\n%s\n' "$description" "$expected" "$code" "$output" >&2
    exit 1
  fi
  printf 'PASS %s (exit %s)\n' "$description" "$code"
}

run_case "kill switch exits NO_NEW" 0 MEME_ENABLED=false

run_case "low memory aborts" 75 \
  MEME_ENABLED=true MEME_TEST_MEM_MIB=10

run_case "low disk aborts" 75 \
  MEME_ENABLED=true MEME_TEST_DISK_MIB=10

mkdir -p "$ROOT/bin"
cat >"$ROOT/bin/pgrep" <<'SH'
#!/bin/sh
exit 0
SH
chmod +x "$ROOT/bin/pgrep"

run_case "active knowledge/meme job aborts" 75 \
  MEME_ENABLED=true PATH="$ROOT/bin:$PATH"

run_case "missing omo binary aborts" 127 \
  MEME_ENABLED=true OMO_BIN=/nonexistent

printf 'COMMON_TESTS_OK\n'
