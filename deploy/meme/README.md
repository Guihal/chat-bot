# Meme discovery and publisher

Two separate agent runs own the meme rubric:

- discovery runs hourly and only reads sources plus saves candidates;
- publisher runs every two hours and may publish exactly one candidate to the fixed channel `-1004421941006`.

The model does the work through its tools. The shell scripts only preflight, lock, wrap resources and start the headless omo session.

## Schedules

| Job | Interval | Timeout | Seeded phase |
|---|---|---|---|
| `meme-discovery` | 60 min | 300 s | first run near `:47Z` |
| `meme-publisher` | 120 min | 240 s | first run near `:07Z` |

The installer seeds each job state's `last_run` once. omo-cron computes the next run as `last_run + minutes*60`, so later wall-clock drift is possible while the hourly / two-hourly spacing holds.

## Environment

| Variable | Default | Meaning |
|---|---|---|
| `MEME_ENABLED` | `true` | `false` exits both runners with `NO_NEW` before network/model |
| `MEME_MAX_PUBLISH_PER_DAY` | `3` | published records counted per UTC day |
| `MEME_MIN_MEM_MIB` | `1500` | preflight MemAvailable floor |
| `MEME_MIN_DISK_MIB` | `500` | preflight disk-free floor |
| `MEME_BOT_TOKEN_FILE` | unset | explicit root-only token path |
| `CHATBOT_TOKEN_FILE` | unset | fallback token path |
| `MEME_BOT_API_BASE` | unset | loopback-only fake Bot API for stand/dry-run |
| `MEME_PREVIEW_BASE` | `https://t.me` | preview origin; loopback http allowed for fixtures |
| `MEME_SOURCES_PATH` | `<data>/meme-sources.md` | agent-editable allowlist |
| `MEME_DATA_DIR` | `/opt/omo-chat/data/memes` | candidates, ledger, locks, alerts |

Publisher token resolution is fail-closed: `MEME_BOT_TOKEN_FILE`, then `CHATBOT_TOKEN_FILE`, otherwise an actionable error. Discovery unsets every token variable before it starts.

## Caps and dedupe

- photo ≤10 MB; video/animation ≤50 MB;
- one total caption budget ≤1024 characters including automatic source attribution; stored text is truncated first;
- no `parse_mode`;
- candidate files are created with O_EXCL as `<source>__<post_id>.json`;
- permanent append-only `<data>/dedupe.jsonl` keeps source+post id, sha256, status and publication time;
- exact reposts are rejected by id or sha256;
- pruning keeps media/metadata under 200 MiB / 500 entries and never removes ledger rows;
- publisher claims a candidate before the single send; a crash leaves a visible claimed/failed record instead of a blind retry.

## Install

```sh
deploy/meme/provision-memes.sh
deploy/meme/install-memes-cron.sh --root /root/omo-cron --no-env
# after dry-run inspection:
deploy/meme/install-memes-cron.sh --root /root/omo-cron --enable
```

The installer writes `/etc/omo-chat/meme.env` with the token path (mode 0600, no secret value) unless `--no-env` is given. Jobs are installed disabled by default; enable only after the dry-run checks pass.

## Dry-run checks

```sh
MEME_ENABLED=false deploy/meme/discovery-run.sh   # prints NO_NEW, no network/model
MEME_ENABLED=false deploy/meme/publisher-run.sh   # prints NO_NEW, no network/model
```

With a fake Bot API on loopback:

```sh
MEME_BOT_API_BASE=http://127.0.0.1:<port> MEME_BOT_TOKEN_FILE=<0600-file> \
  bun deploy/meme/check-channel.mjs
```

The read-only check calls `getMe`, `getChat`, and `getChatMember`, requires administrator status and posting permission, and fails closed with an actionable alert.

## Rollback

1. Disable both cron jobs and set `MEME_ENABLED=false`.
2. Restore the backed-up profile/job files outside watched extension directories.
3. Verify hashes and that `omo-chat.service` is still active without restarting it.

## Preservation

- `<data>/meme-sources.md` belongs to the agent: provisioning seeds it only when absent and never overwrites it.
- Live bot files (`system-prompt.md`, workspace `agents.md`, profile `AGENTS.md`, `md-inject.ts`, knowledge docs) are outside this deploy path and must remain byte-identical.
- No release sync is used for memes; deployment is a manual component pass with backups outside watched directories.
