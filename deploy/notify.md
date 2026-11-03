# Wiring the owner's Telegram to the bot's own alerts

The daemon can only speak through `CHATBOT_NOTIFY_CMD`: it runs that command with
ONE line as its single argument and reads nothing back. Credentials are not part
of the daemon's environment, so a drop-in has to provide both the command and the
environment file the command itself needs.

Live facts on the host (verified 2026-11-03):

- `/root/bin/tg-notify` requires `TG_NOTIFY_ENV` and defaults to
  `/data/secrets/tg-notify.env`, which DOES NOT exist on this box.
- The real credentials sit in `/root/.config/tg-notify.env` (mode 600) and are
  exported today only for `omo-cron-tick.service`.
- `omo-chat.service` sets no `CHATBOT_NOTIFY_CMD` at all, and
  `/etc/systemd/system/omo-chat.service.d/` does not exist yet.

## Install

```sh
install -d -m 755 /etc/systemd/system/omo-chat.service.d
cat > /etc/systemd/system/omo-chat.service.d/notify.conf <<'EOF'
[Service]
Environment=CHATBOT_NOTIFY_CMD=/root/bin/tg-notify
Environment=TG_NOTIFY_ENV=/root/.config/tg-notify.env
EOF
chmod 644 /etc/systemd/system/omo-chat.service.d/notify.conf
systemctl daemon-reload
systemctl restart omo-chat
```

## Prove it, do not assume it

Configuration alone proves nothing: the point of this cycle is that a failed
notifier is now visible. After the restart:

```sh
grep -c 'notify_failed' /opt/omo-chat/data/chat-state/chatd.log
```

- `0` and no fresh `ALERT` lines: nothing to send yet — force one real alert by
  restarting nothing and instead watching for an organic one, or send a test line
  through the same command by hand:
  `TG_NOTIFY_ENV=/root/.config/tg-notify.env /root/bin/tg-notify 'ALERT test: wiring check'`
  and confirm it arrives in the owner's chat.
- a growing `notify_failed: <kind> rc=...` means the command ran and refused:
  read the rc — `2` is "no credentials" (wrong `TG_NOTIFY_ENV`), other values are
  the HTTP client's exit.

Undelivered lines are kept in `<state>/notify-state.json` under `pending` and are
retried on the daemon's next health tick, at most 5 times; after that the log
records `notify_failed: <kind> giving up after N attempts` — silence beyond that
is a bug, not a policy.

## What the owner receives

- `ALERT <kind>: <detail>` — one line per cause, JSON bodies stripped (the `db`
  error text is useless in Telegram; the cause and the context are not).
- `<kind>:recovered: back to normal` — once, when a persistent cause closes.
- Nothing at all while a cause keeps firing: the repeated ticks stay in
  `chatd.log` only, so a long outage does not become a stream of messages.
