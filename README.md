# chat-bot — демон-участник Telegram-чата

Бот, который живёт в вашем Telegram-чате как обычный собеседник: читает сообщения группы,
отвечает от своего имени и, если вы разрешите, выполняет поручения владельца как агент `omo`
в изолированной сессии.

Всё написано на Rust (`chatd`, `tg-chat`, `botwatch`) плюс один JS-файл расширения
(`chat-ext.mjs`). Python в проекте нет намеренно.

## Как это устроено

```text
Telegram Bot API  ⇄  chatd (Rust)  ⇅ unix-сокет <state>/control.sock  ⇅  chat-ext.mjs  ⇅  host-сессия omo
                              │
                              ├─ history.db (зеркало чата, sqlite, один поток-владелец)
                              ├─ events.jsonl (события доставки и активности)
                              └─ chatd.log (диагностика + строки ALERT)
```

- Демон держит локальный сервер на unix-сокете; расширение само подключается к нему, получает
  пачки строками JSON и отвечает исходом приёма (идемпотентно по `delivery_id`). Файлового
  inbox нет вообще.
- Единственный способ что-то отправить в чат — тул `chat_send` этого расширения. Ход без его
  вызовов означает молчание; это допустимый исход, а не сбой.
- Бинарь `tg-chat` — только чтение (история, поиск, медиа, статистика).
- Бинарь `botwatch` — наблюдатель: режим `watchdog` следит за сервисом изнутри хоста,
  режим `--mode liveness` проверяет доступность хоста снаружи.

## Что понадобится

- Rust (stable, `cargo`) и Node.js — сборка и `node --check`.
- `omo` в `PATH` (пакет `omo-ai`): демон поднимает им host-сессию бота.
- `ffmpeg` в `PATH` — из него делаются кадры-превью для гифок и видео; без него в контексте
  останутся пометки «preview unavailable».
- Токен бота от [@BotFather](https://t.me/BotFather).
- Групповой чат, куда добавлен бот, с правом писать.
- Отдельный Telegram-аккаунт «владельца» — по его прямой просьбе бот запускает субагентов.

## Быстрый старт: от клона до работающего бота

1. **Собрать и проверить.**

   ```bash
   git clone <этот-репозиторий> chat-bot && cd chat-bot
   cargo build --release
   cargo test --lib --bin chatd
   node --check chat-ext.mjs
   ./target/release/botwatch --selftest
   ```

2. **Завести своего бота.** В @BotFather: `/newbot`, имя, username. Токен положить в файл
   с правами 600 — одной строкой, без кавычек (демон отказывается читать файл, доступный
   группе или всем):

   ```bash
   install -m 600 /dev/null ~/.config/chat-bot/bot_token
   printf '%s\n' '123456789:AA...ваш-токен' > ~/.config/chat-bot/bot_token
   ```

3. **Завести группу.** Создайте группу, добавьте бота и обязательно выключите privacy mode:
   @BotFather → `/setprivacy` → выберите бота → `Disable`. Иначе бот увидит только упоминания.

4. **Узнать chat id.** Напишите что-нибудь в группе и спросите Bot API:

   ```bash
   TOKEN="$(cat ~/.config/chat-bot/bot_token)"
   curl -s "https://api.telegram.org/bot$TOKEN/getUpdates" | head -c 2000
   ```

   Идентификатор группы — `result[].message.chat.id`, отрицательный, обычно с `-100`.

5. **Заполнить окружение.** Скопируйте `env.example` в `.env` и подставьте свои значения:

   ```bash
   cp env.example .env
   $EDITOR .env
   set -a; . ./.env; set +a
   ```

   Обязательных переменных ровно две — `CHATBOT_TOKEN_FILE` и `CHATBOT_CHAT_ID`; без них
   демон завершается с ошибкой, называющей недостающую переменную.

6. **Запустить.**

   ```bash
   ./target/release/chatd sync       # опционально: выгрузить историю чата
   ./target/release/chatd summarize  # опционально: chat-summary.md
   ./target/release/chatd run        # основной цикл
   ```

Состояние (история, offset, квитанции, медиа) лежит в `CHATBOT_STATE_DIR` и переживает
перезапуск.

## Переменные окружения

Обязательные:

| Переменная | Значение |
|---|---|
| `CHATBOT_TOKEN_FILE` | путь к файлу с токеном, mode 600 |
| `CHATBOT_CHAT_ID` | id обслуживаемого чата, например `-1001234567890` |

Основные (у остальных — портативный дефолт):

| Переменная | По умолчанию | Значение |
|---|---|---|
| `CHATBOT_OWNER_USERNAME` | пусто | username владельца без `@`; пусто = субагенты закрыты всем |
| `CHATBOT_OWNER_ID` | `0` | числовой id владельца — запасной признак |
| `CHATBOT_USERNAME` | пусто | username бота: по нему определяется обращение `@бот` |
| `CHATBOT_STATE_DIR` | `./state` | `history.db`, `media/`, `events.jsonl`, `chatd.log`, `control.sock` |
| `CHATBOT_SESSION` | `chat-participant` | имя host-сессии, которую ведёт демон |
| `CHATBOT_HOST_SOCKET` | `<state>/host/rpc.sock` | сокет выделенного хоста бота |
| `CHATBOT_CWD` | `.` | рабочий каталог агентской сессии |
| `CHATBOT_AGENT_DIR` | пусто | изолированный agent dir бота |
| `CHATBOT_SYSTEM` | `system-prompt.md` | системный промт сессии |
| `CHATBOT_BATCH_SEC` | `10` | окно батчинга сообщений перед пушем |
| `CHATBOT_API_BASE` | `https://api.telegram.org` | адрес Bot API (меняется только для тестов) |
| `CHATBOT_ADMIT_FIRST_SEC` / `CHATBOT_ADMIT_SEC` | `8` / `10` | повторы пуша при отказе приёма |
| `CHATBOT_TURN_SEC` / `CHATBOT_TURN_EXTRA_SEC` | `420` / `420` | бюджет хода и продление, пока агент работает |
| `CHATBOT_RETRY_SEC` / `CHATBOT_EMIT_SEC` | `60` / `60` | повтор доставки / ожидание следа доставки |
| `CHATBOT_STALE_SEC` | `90` | порог свежести поллинга (период наблюдателя = `stale/2`) |
| `CHATBOT_SYSTEMD_UNIT` | `chat-bot.service` | юнит, чьё состояние решает вердикт `down` |
| `CHATBOT_NOTIFY_CMD` | пусто | внешний notifier: получает строку `ALERT …` одним аргументом |

Наблюдатель `botwatch` читает свои переменные (`BOTWATCH_*`, `LIVENESS_*`) — они описаны
в разделе «Наблюдатель».

## Запуск как сервис (systemd)

```bash
sudo mkdir -p /etc/chat-bot
sudo cp deploy/chat-bot.env.example /etc/chat-bot/chat-bot.env
sudo chmod 600 /etc/chat-bot/chat-bot.env
sudo $EDITOR /etc/chat-bot/chat-bot.env            # токен, chat id, owner, пути
sudo cp deploy/chat-bot.service /etc/systemd/system/chat-bot.service
sudo $EDITOR /etc/systemd/system/chat-bot.service  # WorkingDirectory/ExecStart под свой путь
sudo systemctl daemon-reload
sudo systemctl enable --now chat-bot.service
```

Шаблон подставляет значения только из `EnvironmentFile` — наших путей в юните нет.

## Сделать своим

- **Токен и чат** — `CHATBOT_TOKEN_FILE` и `CHATBOT_CHAT_ID`. Дефолтов в коде нет намеренно:
  незаполненная конфигурация должна падать, а не писать в чужой чат.
- **Владелец** — `CHATBOT_OWNER_USERNAME` (и/или `CHATBOT_OWNER_ID`). Пока он пуст, диспетч
  субагентов закрыт и демон пишет предупреждение в `chatd.log`.
- **Персона** — `bot-home/AGENTS.md`: как бот говорит, кого слушается, что ему запрещено.
  Это единственное место, где живёт «команда владельца vs болтовня». `system-prompt.md` —
  системный промт сессии.
- **Изолированный agent dir** (свой `mcp.json`, свои настройки, без общего профиля):

  ```bash
  export CHATBOT_STATE_DIR=$PWD/state
  ./provision-agent.sh ./state/agent "$PWD"
  export CHATBOT_AGENT_DIR=$PWD/state/agent
  ```

  Провижининг **сливает** профиль: управляемая часть лежит между маркерами
  `<!-- repo-profile:begin -->` / `<!-- repo-profile:end -->` в `bot-home/AGENTS.md`,
  всё, что бот дописал сам, сохраняется; перед изменением делается бэкап, а идентичное
  содержимое не перезаписывается. `settings.json` и `mcp.json` кладутся с бэкапом,
  файлы авторизации (`auth.json`, `models.json`, mode 600) скрипт не создаёт.
- **Медиа и профили.** Картинки/гифки складываются в `state/media/`. Каталог `portraits/`
  (профили собеседников для тулов `chatter_*`) в репозитории отсутствует намеренно — он
  собирается из реальной переписки.

## Как устроен демон

`chatd run` — независимые задачи на одном потоке (`tokio` current_thread + `LocalSet`:
соединения sqlite не `Send`):

- **ingest** — `getUpdates` крутится непрерывно; запись сообщения и скачивание медиа уходят
  в отдельную задачу, поэтому медленная загрузка не тормозит опрос. Окно батча — `CHATBOT_BATCH_SEC`.
- **admit** — единственный владелец control-сокета и жизненного цикла сессии
  (`ensure`/`settle`/`recreate`). Пушит батч строкой `{"push":{"delivery_id","text","deliverAs":"steer"}}`
  и ждёт `result`: `started`, `steered`, `queued`, `already_admitted`. Отказ (`turn_conflict`,
  `held_draft`) доставкой не считается: отметка не ставится, пуш повторяется.
- **track** — читает `events.jsonl`, зеркалит квитанции, применяет `compacted`-переотправку,
  закрывает батч и просит `admit` о восстановлении зависшей сессии.
- **health** — раз в `CHATBOT_STALE_SEC/2` публикует `health.json` и прогоняет алерты.

Батч закрывается только по ответу на все его `message_id`; единственный неотвеченный выход —
`settled` после `emitted` (агент видел и промолчал).

Хост бота — отдельный: демон сам поднимает его командой
`omo host ensure --launch-spec <state>/launch-spec.json --socket <sock>`. Расширение грузится
на весь хост, поэтому общий хост с другим ботом недопустим — `chat_send` утёк бы в чужие
сессии. Отсюда и дефолтный сокет внутри state-каталога.

### Видео и медиа

Модели объявляют модальность `video` в `models.json`, но у OpenAI-совместимого шлюза
видео нет представления внутри результата инструмента: `unsupported media type at
input[N].output[M]: video/mp4` валит **все** последующие запросы, потому что блок остаётся
в транскрипте. Поэтому расширение ставит хук `before_provider_request` и вычищает
видео/аудио **только из результатов тулов** (`function_call_output`, `role:"tool"`,
`tool_result`), заменяя их текстовой пометкой; медиа в сообщениях пользователя не трогается,
так что объявленная модальность остаётся рабочей.

Параллельно расширение различает терминальный отказ провайдера (`stopReason:"error"`, либо
`aborted` с `abortSource != "user"`, при `willRetry:false`) и шлёт демону `run_failed`, чтобы
тот лечил сессию, а не считал это молчанием модели.

### База

Одно соединение, один выделенный поток `chatdb`, FIFO-очередь (`src/db.rs`): блокирующая
работа sqlite не останавливает рантайм, который одновременно опрашивает Telegram и пишет
кадры в сокет. Многошаговые операции (дедуп-вставка, `mark_done`, перегенерация текста) идут
одним замыканием и потому атомарны относительно других пользователей базы. Индексы создаются
идемпотентно при старте, старую базу не ломают.

`src/bin/dbbench.rs` — воспроизводимый замер старых и новых запросов на одном файле в одном
прогоне (фаза «до» — старый SQL без индексов, «после» — новый вызов с индексами; расхождение
результатов = баг, а не ускорение).

## Алерты: что и когда приходит владельцу

Алерт — одна строка `ALERT <kind>: <detail>` в `chatd.log` и, если задан
`CHATBOT_NOTIFY_CMD`, одно сообщение в Telegram. JSON-тела из текста вырезаются, длина
обрезается по границе слова: владельцу нужна суть, а не дамп.

У причины есть ровно один владелец закрытия:

- **постоянные причины** (`host_socket_gone`, `free_below_reserve`, `session_mismatch`,
  `session_held`, `poll_stale`, `run_failed:<sig>`) объявляются один раз на переходе
  и закрываются только обратным переходом — `ALERT <kind>:recovered`;
- **обычные причины** (`db_failure` и подобные) живут по самоистечению: пока повторяются,
  молчат, а по прекращению дают одну строку `recovered`;
- **события** (`send_unknown`) не глушатся и не «восстанавливаются»: каждое появление — своя новость.

Подавление хранится в `<state>/notify-state.json` и переживает `Restart=always`; иначе
краш-луп присылал бы одно и то же каждые 15 секунд. Если нотифаер вернул ненулевой код,
строка попадает в `pending` и повторяется следующим тиком (до пяти попыток), а в лог
пишется `notify_failed: <kind> rc=N` — молчаливой потери алерта не бывает.

Пример проводки (drop-in systemd):

```ini
[Service]
Environment=CHATBOT_NOTIFY_CMD=/root/bin/tg-notify
Environment=TG_NOTIFY_ENV=/root/.config/tg-notify.env
```

`tg-notify` берёт креды из файла, на который указывает `TG_NOTIFY_ENV`. Проверять доставку
надо сообщением, а не фактом конфигурации:

```bash
TG_NOTIFY_ENV=/root/.config/tg-notify.env /root/bin/tg-notify 'ALERT test: wiring check'
```

## Наблюдатель `botwatch`

Два режима одного бинаря:

**`botwatch`** (на сервере, из `omo-cron` раз в час или из собственного таймера). Печатает
`NO_NEW`, когда всё хорошо, иначе — по строке на новую причину:

- мёртвый/упавший юнит: одна попытка `systemctl restart`, затем `HEALED: …` или `DOWN PERSISTS: …`;
- ошибки в `chatd.log` (по списку шаблонов) и в `journalctl`;
- `provider_error` в логе агента;
- причины из `events.jsonl`: `admit_error`, `control_stale`, `control_stale_session`,
  `chat_send_error`, `run_failed`, и `delivery_state:failed` — по нагрузке, потому что
  `delivery_state` пишется на каждую доставку;
- `NOTIFIER BROKEN`, если демон не смог доставить алерт;
- `STALLED`, если в чат писали, а ответов не было.

Личность причины — это **присутствие**, а не счётчик: пока причина жива, повторных
сообщений нет; исчезновение печатается как `CLEARED:` только для того, что наблюдатель
считает состоянием (застой диалога). Истечение окна сканирования — не восстановление.

Строки `ALERT` вотчдог не пересылает: их уже отправил демон.

**`botwatch --mode liveness`** (на другой машине, user-timer). Проверяет только
доступность хоста по ssh: два одинаковых наблюдения подряд, затем одно сообщение
`ХОСТ НЕДОСТУПЕН` / `ХОСТ СНОВА НА СВЯЗИ`. Недоставленная строка остаётся в состоянии
и повторяется следующим прогоном — потерять тревогу из-за сбоя нотифаера нельзя.

Для режима `liveness` обязателен `LIVENESS_SERVER` (например `root@your-host`) — адрес
намеренно не зашит в код. Все внешние зависимости подменяемы переменными
(`BOTWATCH_UNIT_CMD`, `BOTWATCH_JOURNAL_CMD`, `LIVENESS_PROBE_CMD`), поэтому `botwatch --selftest` гоняет весь контракт на временных
файлах и стабах: без root, без целевого хоста и без настоящего нотифаера.

## Проверки

```bash
cargo build --release
cargo test --lib                       # ядро и актор базы
cargo test --bin chatd                 # матрица решений демона
cargo test --test alert_format         # формат, глушение, восстановление, pending
cargo test --test notify_smoke         # контракт нотифаера (rc=2 без кредов и т.п.)
cargo test --test alert_stand          # живой прогон бинаря: фатальный старт → одно сообщение
node --test --test-force-exit chat-ext.test.mjs
./target/release/botwatch --selftest
```

`cargo test --workspace` дополнительно запускает `tests/daemon_smoke.rs` — живой прогон
против фейкового Bot API со стаб-`omo` в `PATH`. Этому тесту нужен собственный host-сокет
и он может ждать его долго; если стенд недоступен, гоняйте таргеты по отдельности.

## Релиз и откат

```bash
deploy/release.sh manifest --root . --env-file /etc/chat-bot/chat-bot.env --manifest /var/lib/chat-bot/release.manifest
deploy/release.sh backup   --root . --env-file /etc/chat-bot/chat-bot.env --dest /var/lib/chat-bot/release-backup --agent-dir /var/lib/chat-bot/agent
deploy/release.sh verify   --root . --env-file /etc/chat-bot/chat-bot.env --manifest /var/lib/chat-bot/release.manifest
deploy/release.sh rollback --root . --backup /var/lib/chat-bot/release-backup --agent-dir /var/lib/chat-bot/agent
```

Манифест содержит один `set_hash` по бинарям (`chatd`, `botwatch`), `chat-ext.mjs`, фазовому
стрипу, промпту, персоне, agent-профилю, `provision-agent.sh`, env-файлу и systemd-юниту.
`rollback` возвращает файлы побайтово, перепровижинит agent dir из восстановленной персоны
и сверяет `set_hash`.

Рабочий бинарь нельзя перезаписать на месте (`ETXTBSY`): заливайте как `chatd.new`, затем
`mv` + `systemctl restart`. Расширение кладётся в рабочий каталог хоста **до** рестарта.
После выкатки сверяйте `sha256` и убеждайтесь, что сессия с настроенным именем ровно одна.

## Чего здесь нет и как это получить

| Нет | Почему | Как получить |
|---|---|---|
| `state/` | живая база, медиа, квитанции — данные конкретного чата | создаётся сама; историю забирает `chatd sync` |
| `portraits/` | профили реальных людей | генерируются в вашем чате |
| `chat-summary.md` | построен по реальной переписке | `chatd summarize` |
| `*.env`, токены | секреты | свои, mode 600, вне git (см. `.gitignore`) |
| systemd-юнит и docker-сборка нашего хоста | привязаны к чужой инфраструктуре | `deploy/` — шаблон |

При копировании rsync/scp исключайте `.git`.

## Если что-то пошло не так

- **Демон сразу завершается** — почти всегда не задана обязательная переменная; текст ошибки
  называет её. `.env` сам не читается.
- **`refusing token file with group/other permissions`** — `chmod 600`.
- **Бот молчит** — проверьте: он в группе, privacy mode выключен, `CHATBOT_CHAT_ID` совпадает,
  в `chatd.log` нет отказов приёма, а в `<agent>/logs/session.log` нет `provider_error`.
  Пустой ответ ассистента при быстром ходе — это не «модель решила молчать», пока не проверен
  `provider_error`.
- **Сообщения «пропадают» при двух процессах** — `getUpdates` подтверждает offset глобально
  для бота: один поллер на токен.
- **После рестарта бот принимает доставки и не отвечает** — сессия могла встать в удержание:
  `omo host session state <durable> --socket <sock>` покажет `holds`; лечится
  `omo host stop --force` (демон поднимет свежий хост и переотправит батч).
- **Субагенты не запускаются** — не задан `CHATBOT_OWNER_USERNAME`/`CHATBOT_OWNER_ID`.
- **Бинарь не стартует на другой машине** (`GLIBC_* not found`) — соберите его на целевой
  системе.

## Лицензия

MIT, см. `LICENSE`.
