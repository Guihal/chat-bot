#!/bin/sh
set -eu

HERE=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
REPO=$(CDPATH='' cd -- "$HERE/.." && pwd)
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT=${1:-"/home/penis/tmp/meme-deploy-$STAMP.tgz"}
mkdir -p "$(dirname -- "$OUT")"

tar -czf "$OUT" -C "$REPO" deploy/meme

echo "STAGE_DEPLOY_OK $OUT"
sha256sum "$OUT"
