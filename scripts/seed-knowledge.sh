#!/bin/sh
# Create the bot's knowledge documents once, never overwrite them.
#
# The knowledge cron fills the files through its `<!-- knowledge:begin -->` /
# `<!-- knowledge:end -->` managed zone and preserves every byte outside it, so
# a header written here survives. A second run is a no-op: only a MISSING file is
# created, and the donors list starts with the entry the owner asked for.
#
# Usage: seed-knowledge.sh [KNOWLEDGE_DIR]
#   Defaults to $CHATBOT_KNOWLEDGE_DIR, then <this script's parent>/knowledge.
set -eu

HERE="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
DIR="${1:-${CHATBOT_KNOWLEDGE_DIR:-$HERE/../knowledge}}"

mkdir -p "$DIR"

create() {
  name="$1"
  body="$2"
  if [ -e "$DIR/$name" ]; then
    echo "keep   $DIR/$name"
    return 0
  fi
  printf '%s\n' "$body" > "$DIR/$name"
  echo "create $DIR/$name"
}

create "Дискурс.md" '# Дискурс

Что обсуждали в канале: темы, позиции, к чему пришли.'
create "Важные персоны вне канала.md" '# Важные персоны вне канала

Люди, которых обсуждают в канале, но которые в нём не пишут.'
create "Словарь.md" '# Словарь

Сленг, сокращения, прозвища и внутренние мемы канала.'
create "Список легенд(донатеров).md" '# Список легенд (донатеры)

Bront @Oleg3573 - 200 рублей'
create "Список важных событий.md" '# Список важных событий

События канала и вокруг него, о которых стоит помнить.'
