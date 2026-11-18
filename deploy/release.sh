#!/bin/sh
# Release manifest, verification, backup and rollback for the chat-bot set.
#
# One hash covers every CODE file of the release. Agent-authored state is not
# release content: the workspace `agents.md`, the global `AGENTS.md`, the
# `system-prompt.md` the bot rewrites itself, the `md-inject.ts` rule block and
# every `knowledge/*.md` document are deliberately NOT in this set and are never
# synced by the deploy. A necessary content change to them is transferred by
# hand into the live file.
#
# Usage:
#   release.sh list     --root DIR
#   release.sh manifest --root DIR --env-file PATH [--manifest PATH]
#   release.sh verify   --root DIR --env-file PATH --manifest PATH
#   release.sh backup   --root DIR --env-file PATH --dest DIR [--agent-dir DIR]
#   release.sh rollback --root DIR --backup DIR [--agent-dir DIR]
set -eu

HERE=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
REPO=$(dirname "$HERE")
ROOT=${CHATBOT_RELEASE_ROOT:-$REPO}
ENV_FILE=${CHATBOT_ENV_FILE:-/etc/chat-bot/chat-bot.env}
MANIFEST=${CHATBOT_MANIFEST:-$HERE/release-manifest.txt}
DEST=
BACKUP=
AGENT_DIR=
TAB=$(printf '\t')

die() { echo "release.sh: $*" >&2; exit 1; }

usage() {
    echo "usage: release.sh manifest|verify|backup|rollback [options]" >&2
    exit 2
}

CMD=${1:-}
[ -n "$CMD" ] || usage
shift
while [ $# -gt 0 ]; do
    case "$1" in
        --root) ROOT=$2; shift 2 ;;
        --env-file) ENV_FILE=$2; shift 2 ;;
        --manifest) MANIFEST=$2; shift 2 ;;
        --dest) DEST=$2; shift 2 ;;
        --backup) BACKUP=$2; shift 2 ;;
        --agent-dir) AGENT_DIR=$2; shift 2 ;;
        *) usage ;;
    esac
done

entries() {
    printf 'bin\t%s\n' 'target/release/chatd'
    printf 'bin\t%s\n' 'target/release/botwatch'
    printf 'ext\t%s\n' 'chat-ext.mjs'
    printf 'phase\t%s\n' 'muse-spark-phase-strip.ts'
    printf 'compactor\t%s\n' 'chat-compactor'
    printf 'compactor_cfg\t%s\n' 'bot-home/compactors/rich.json'
    printf 'compactor_cfg\t%s\n' 'bot-home/compactors/rich.md'
    printf 'control_task\t%s\n' 'chat-control-task.mjs'
    printf 'agent_settings\t%s\n' 'bot-home/settings.json'
    printf 'agent_ext\t%s\n' 'bot-home/extensions'
    printf 'agent_mcp\t%s\n' 'bot-home/mcp.json'
    printf 'provision\t%s\n' 'provision-agent.sh'
    printf 'unit\t%s\n' 'deploy/chat-bot.service'
    printf 'env\t%s\n' "$ENV_FILE"
}

# Expand a directory row into one row per file, sorted, so the manifest, the
# backup and the rollback all address individual files of the compactor tree.
expand_entries() {
    while IFS="$TAB" read -r label rel; do
        file=$(source_path "$rel")
        if [ -d "$file" ]; then
            ( cd "$ROOT" && find "$rel" -type f -print | LC_ALL=C sort ) |
            while IFS= read -r sub; do
                printf '%s\t%s\n' "$label" "$sub"
            done
        else
            printf '%s\t%s\n' "$label" "$rel"
        fi
    done
}

source_path() {
    case "$1" in
        /*) printf '%s' "$1" ;;
        *) printf '%s/%s' "$ROOT" "$1" ;;
    esac
}

manifest_write() {
    out=$1
    mkdir -p "$(dirname -- "$out")"
    list="$out.entries.$$"
    lines="$out.lines.$$"
    canon="$out.canon.$$"
    entries | expand_entries > "$list"
    : > "$lines"
    : > "$canon"
    while IFS="$TAB" read -r label rel; do
        file=$(source_path "$rel")
        [ -f "$file" ] || die "missing release file: $file"
        sha=$(sha256sum "$file" | awk '{print $1}')
        printf 'file\t%s\t%s\t%s\n' "$label" "$rel" "$sha" >> "$lines"
        printf '%s\t%s\t%s\n' "$label" "$rel" "$sha" >> "$canon"
    done < "$list"
    set_hash=$(LC_ALL=C sort "$canon" | sha256sum | awk '{print $1}')
    {
        echo 'chat-bot-release-manifest v1'
        echo "set_hash $set_hash"
        cat "$lines"
    } > "$out"
    rm -f "$list" "$lines" "$canon"
}

manifest_hash() {
    awk '$1 == "set_hash" { print $2; exit }' "$1"
}

case "$CMD" in
    manifest)
        manifest_write "$MANIFEST"
        echo "MANIFEST_OK $(manifest_hash "$MANIFEST")"
        ;;
    list)
        entries | expand_entries
        ;;
    verify)
        [ -f "$MANIFEST" ] || die "no manifest at $MANIFEST"
        tmp="$MANIFEST.recheck.$$"
        manifest_write "$tmp"
        got=$(manifest_hash "$tmp")
        want=$(manifest_hash "$MANIFEST")
        rm -f "$tmp"
        [ "$got" = "$want" ] || die "manifest mismatch: current $got, manifest $want"
        echo "MANIFEST_OK $got"
        ;;
    backup)
        [ -n "$DEST" ] || die "backup needs --dest"
        manifest_write "$DEST/before.manifest"
        entries | expand_entries > "$DEST/entries.$$"
        while IFS="$TAB" read -r label rel; do
            src=$(source_path "$rel")
            case "$rel" in
                /*) dst="$DEST/files/env/$(basename -- "$rel")" ;;
                *) dst="$DEST/files/$rel" ;;
            esac
            mkdir -p "$(dirname -- "$dst")"
            cp -p "$src" "$dst"
        done < "$DEST/entries.$$"
        rm -f "$DEST/entries.$$"
        echo "BACKUP_OK $(manifest_hash "$DEST/before.manifest")"
        ;;
    rollback)
        [ -n "$BACKUP" ] || die "rollback needs --backup"
        MAN="$BACKUP/before.manifest"
        [ -f "$MAN" ] || die "no backup manifest at $MAN"
        restored_env=
        while IFS="$TAB" read -r tag label rel sha; do
            [ "$tag" = "file" ] || continue
            case "$rel" in
                /*) src="$BACKUP/files/env/$(basename -- "$rel")"; dst="$rel" ;;
                *) src="$BACKUP/files/$rel"; dst="$ROOT/$rel" ;;
            esac
            [ -f "$src" ] || die "backup is missing $src"
            mkdir -p "$(dirname -- "$dst")"
            cp -p "$src" "$dst"
            if [ "$label" = "env" ]; then
                restored_env="$dst"
            fi
        done < "$MAN"
        [ -n "$restored_env" ] && ENV_FILE="$restored_env"
        if [ -n "$AGENT_DIR" ]; then
            [ -f "$ROOT/provision-agent.sh" ] || die "provision-agent.sh missing in $ROOT"
            # The restored env file is the authority for the profile values.
            set -a
            # shellcheck disable=SC1090
            . "$ENV_FILE"
            set +a
            CHATBOT_STATE_DIR="${CHATBOT_STATE_DIR:-$ROOT/state}" \
                sh "$ROOT/provision-agent.sh" "$AGENT_DIR" "$ROOT"
        fi
        tmp="$BACKUP/postcheck.manifest"
        manifest_write "$tmp"
        got=$(manifest_hash "$tmp")
        want=$(manifest_hash "$MAN")
        rm -f "$tmp"
        [ "$got" = "$want" ] || die "rollback post-check mismatch: current $got, backup $want"
        echo "MANIFEST_OK $got"
        ;;
    *) usage ;;
esac
