//! chat-bot shared core: port of chatd.py (run/sync/summarize helpers) +
//! tg-chat helpers. Single-threaded async: all awaits run on a
//! `current_thread` tokio runtime (see bins); sqlite work is inline on the
//! same thread; typing indicator is a same-thread spawned task.

use anyhow::{anyhow, Context, Result};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// The database actor: one connection, one owning thread, FIFO queue. The
/// blocking sqlite work of the daemon runs there instead of on the
/// `current_thread` runtime that also polls Telegram, admits batches and
/// writes chat pushes.
pub mod catalog;
pub mod context;
pub mod db;
pub mod ledger;
pub mod media;
pub mod ops;
pub mod send;
pub mod session;

pub use catalog::GIF_PAGE_SQL;
pub use catalog::{Caption, FitsWhen, GifPage, GifRecord, GifSource, MediaKind};

use crate::media::MediaState;

pub const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS messages(
  update_id INTEGER PRIMARY KEY,
  message_id INTEGER,
  date INTEGER,
  user_id INTEGER,
  username TEXT,
  first_name TEXT,
  text TEXT,
  reply_to INTEGER,
  media_path TEXT,
  file_id TEXT,
  file_unique_id TEXT,
  media_kind TEXT
);
CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
  text, username, content='messages', content_rowid='update_id'
);
CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
  INSERT INTO messages_fts(rowid, text, username)
  VALUES (new.update_id, new.text, new.username);
END;
CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT);
";

pub fn cfg(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

/// A value the process cannot invent a safe default for. The error names the
/// variable, so an unconfigured clone stops with an actionable message instead
/// of silently posting into somebody else's chat (the failure mode that made
/// the old hardcoded chat id dangerous).
pub fn required_cfg(name: &str) -> Result<String> {
    match std::env::var(name) {
        Ok(v) if !v.trim().is_empty() => Ok(v),
        _ => Err(anyhow!(
            "{name} is not set: export it before starting (see env.example)"
        )),
    }
}

pub fn state_dir() -> String {
    cfg("CHATBOT_STATE_DIR", "./state")
}

/// Parse a usable Telegram chat id. Zero and non-numeric values are refused:
/// migration v2 writes this value into every historic row, so a wrong value
/// silently reassigns the identity of the whole mirror.
pub fn parse_chat_id(raw: &str) -> Option<i64> {
    let t = raw.trim();
    if t.is_empty() {
        return None;
    }
    match t.parse::<i64>() {
        Ok(v) if v != 0 => Some(v),
        _ => None,
    }
}

/// The chat id migration v2 may backfill: the environment wins, an already
/// seeded `kv` value is the fallback (a restart without the variable still
/// knows the identity it committed earlier).
fn migration_chat_id(con: &Connection) -> Option<i64> {
    if let Ok(raw) = std::env::var("CHATBOT_CHAT_ID") {
        return parse_chat_id(&raw);
    }
    con.query_row("SELECT v FROM kv WHERE k='chat_id'", [], |r| {
        r.get::<_, String>(0)
    })
    .ok()
    .and_then(|v| parse_chat_id(&v))
}

/// Fail closed BEFORE any migration step. v1 commits `user_version=1` on its
/// own, so validating inside v2 would leave the file half-migrated; this check
/// runs while the version is still the original one.
fn require_migration_chat_id(con: &Connection, version: i64) -> Result<()> {
    if let Ok(raw) = std::env::var("CHATBOT_CHAT_ID") {
        if !raw.trim().is_empty() && parse_chat_id(&raw).is_none() {
            return Err(anyhow!(
                "CHATBOT_CHAT_ID={raw:?} is not a valid numeric Telegram chat id; \
                 refusing to migrate (user_version stays {version})"
            ));
        }
    }
    if version < 2 && migration_chat_id(con).is_none() {
        return Err(anyhow!(
            "CHATBOT_CHAT_ID is not set; refusing to migrate (user_version stays {version}). \
             Set CHATBOT_CHAT_ID to the bot's numeric chat id (for example -1001234567890) \
             and start again."
        ));
    }
    Ok(())
}

fn token() -> Result<String> {
    let path = required_cfg("CHATBOT_TOKEN_FILE")?;
    let mode = std::fs::metadata(&path)
        .with_context(|| format!("token file stat: {path}"))?
        .permissions()
        .mode();
    if mode & 0o077 != 0 {
        anyhow::bail!("refusing token file with group/other permissions: {path}");
    }
    Ok(std::fs::read_to_string(&path)
        .with_context(|| format!("token file read: {path}"))?
        .trim()
        .to_string())
}

fn api_base() -> String {
    cfg("CHATBOT_API_BASE", "https://api.telegram.org")
}

pub async fn api(method: &str, params: &Value) -> Result<Value> {
    let tok = token()?;
    let url = format!("{}/bot{}/{}", api_base(), tok, method);
    let client = reqwest::Client::builder().build()?;
    let resp = client.post(&url).json(params).send().await?;
    let body: Value = resp.json().await?;
    if body.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        return Err(anyhow!("Bot API {method}: {body}"));
    }
    body.get("result")
        .cloned()
        .ok_or_else(|| anyhow!("Bot API {method}: no result"))
}

/// One helper for the topic parameter: every send method and `typing` reach
/// the Bot API through `api_with_thread` or `api_multipart_with_thread`, so
/// `message_thread_id` cannot silently diverge between them. An explicit topic
/// rejection gets exactly one retry into General (DESIGN §5), not a second
/// answer and not a permanent drop.
pub fn apply_thread(params: &mut Value, thread: Option<i64>) {
    if let Some(t) = thread {
        params["message_thread_id"] = json!(t);
    }
}

fn is_topic_error(detail: &str) -> bool {
    let d = detail.to_lowercase();
    d.contains("message_thread_id")
        || d.contains("thread not found")
        || d.contains("topic_id_invalid")
        || d.contains("topic closed")
        || d.contains("wrong topic")
        || d.contains("thread_id_invalid")
}

pub async fn api_with_thread(method: &str, params: &Value, thread: Option<i64>) -> Result<Value> {
    let mut p = params.clone();
    apply_thread(&mut p, thread);
    match api(method, &p).await {
        Ok(v) => Ok(v),
        Err(e) if thread.is_some() && is_topic_error(&e.to_string()) => {
            dlog(&format!(
                "{method}: topic {thread:?} rejected ({e}); retrying in General once"
            ));
            api(method, params).await
        }
        Err(e) => Err(e),
    }
}

/// Multipart upload with the same topic helper and the same one-shot General
/// fallback. Kept beside `api_with_thread` so a new send method has one place
/// to copy the rule from.
pub async fn api_multipart_with_thread(
    method: &str,
    field: &str,
    path: &str,
    filename: &str,
    mime: &str,
    params: &Value,
    thread: Option<i64>,
) -> Result<Value> {
    let upload = |thread: Option<i64>| {
        let method = method.to_string();
        let field = field.to_string();
        let path = path.to_string();
        let filename = filename.to_string();
        let mime = mime.to_string();
        async move {
            let tok = token()?;
            let url = format!("{}/bot{}/{}", api_base(), tok, method);
            let bytes = std::fs::read(&path).with_context(|| format!("read media {path}"))?;
            let part = reqwest::multipart::Part::bytes(bytes)
                .file_name(filename)
                .mime_str(&mime)?;
            let mut p = params.clone();
            apply_thread(&mut p, thread);
            let mut form = reqwest::multipart::Form::new().part(field, part);
            if let Some(obj) = p.as_object() {
                for (k, v) in obj {
                    let s = match v {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    form = form.text(k.clone(), s);
                }
            }
            let client = reqwest::Client::builder().build()?;
            let body: Value = client
                .post(&url)
                .multipart(form)
                .send()
                .await?
                .json()
                .await?;
            if body.get("ok").and_then(|v| v.as_bool()) != Some(true) {
                return Err(anyhow!("Bot API {method}: {body}"));
            }
            body.get("result")
                .cloned()
                .ok_or_else(|| anyhow!("Bot API {method}: no result"))
        }
    };
    match upload(thread).await {
        Ok(v) => Ok(v),
        Err(e) if thread.is_some() && is_topic_error(&e.to_string()) => {
            dlog(&format!(
                "{method}: topic {thread:?} rejected ({e}); retrying in General once"
            ));
            upload(None).await
        }
        Err(e) => Err(e),
    }
}

/// GET file download (Bot API file path -> local media dir).
async fn download_media(file_id: &str, state: &str, prefix: &str) -> Result<String> {
    let info = api("getFile", &json!({"file_id": file_id})).await?;
    let rel = info
        .get("file_path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("getFile: no file_path"))?;
    let mut base = Path::new(rel)
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "file.bin".to_string());
    if !prefix.is_empty() {
        base = format!("{prefix}_{base}");
    }
    let dest = Path::new(state).join("media").join(&base);
    if let Some(p) = dest.parent() {
        std::fs::create_dir_all(p)?;
    }
    if !dest.exists() {
        let tok = token()?;
        let url = format!("{}/file/bot{}/{}", api_base(), tok, rel);
        let client = reqwest::Client::builder().build()?;
        let bytes = client.get(&url).send().await?.bytes().await?;
        std::fs::write(&dest, &bytes)?;
    }
    Ok(dest.to_string_lossy().to_string())
}

pub async fn preview_frame(src: &str, timeout: Duration) -> Result<String> {
    // Strip extension then append.
    let stem = match src.rfind('.') {
        Some(i) => &src[..i],
        None => src,
    };
    let dst = format!("{stem}_frame.jpg");
    if Path::new(&dst).exists() {
        return Ok(dst);
    }
    let out = tokio::time::timeout(
        timeout,
        tokio::process::Command::new("ffmpeg")
            .args([
                "-y", "-v", "error", "-ss", "0", "-i", src, "-vframes", "1", "-q:v", "4", &dst,
            ])
            .output(),
    )
    .await
    .map_err(|_| anyhow!("ffmpeg frame timed out"))??;
    if !out.status.success() || !Path::new(&dst).exists() {
        return Err(anyhow!(
            "ffmpeg frame failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(dst)
}

// ---------- sqlite ----------

/// Indexes the hot read paths need. Idempotent and additive: an existing
/// database simply gains them at the next start (a few thousand rows take
/// milliseconds). See the P1–P6 table in the plan: every entry here removes a
/// `SCAN messages` from one hot statement.
pub fn ensure_indexes(con: &Connection) -> Result<()> {
    con.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_messages_message_id ON messages(message_id);
         CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date DESC, message_id DESC);
         CREATE INDEX IF NOT EXISTS idx_messages_answered ON messages(user_id, date DESC, reply_to) WHERE reply_to IS NOT NULL;
         CREATE INDEX IF NOT EXISTS idx_messages_author_window ON messages(username, date DESC);
         DROP INDEX IF EXISTS idx_messages_gifs;
         CREATE INDEX IF NOT EXISTS idx_messages_animations ON messages(date DESC, message_id DESC, file_unique_id, text, file_id) WHERE media_kind='animation' AND file_id IS NOT NULL AND file_id <> '';",
    )?;
    Ok(())
}

// ---------- migrations ----------
//
// `PRAGMA user_version` is the single source of truth for how far the file
// has been migrated. Every step runs in its own transaction, so a crash or a
// failed check leaves the previous version intact and the daemon refuses to
// serve an inconsistent schema instead of silently repairing it.

fn migrate(con: &mut Connection) -> Result<()> {
    let v: i64 = con.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if v < 1 {
        migrate_v1(con)?;
    }
    if v < 2 {
        migrate_v2(con)?;
    }
    if v < 3 {
        migrate_v3(con)?;
    }
    if v < 4 {
        migrate_v4(con)?;
    }
    if v < 5 {
        migrate_v5(con)?;
    }
    if v < 6 {
        migrate_v6(con)?;
    }
    Ok(())
}

/// v1: rebuild `messages` around the real identity `(chat_id, message_id)`.
/// `id` is the surrogate rowid used by FTS and future foreign keys.
/// `update_id` is kept as a UNIQUE legacy column only until cycle 2 replaces
/// the old ingest path; the redesigned envelope path ignores it for identity.
fn migrate_v1(con: &mut Connection) -> Result<()> {
    let tx = con.transaction()?;
    tx.execute_batch(
        "DROP TRIGGER IF EXISTS messages_ai;
         DROP TABLE IF EXISTS messages_fts;
         DROP TABLE IF EXISTS messages_v1;
         CREATE TABLE messages_v1(
           id INTEGER PRIMARY KEY,
           chat_id INTEGER,
           message_id INTEGER,
           thread_id INTEGER,
           date INTEGER,
           user_id INTEGER,
           username TEXT,
           first_name TEXT,
           text TEXT,
           reply_to INTEGER,
           media_path TEXT,
           file_id TEXT,
           file_unique_id TEXT,
           media_kind TEXT,
           source_chat INTEGER,
           sender_chat INTEGER,
           update_id INTEGER UNIQUE
         );
         INSERT INTO messages_v1(id,chat_id,message_id,thread_id,date,user_id,username,first_name,text,reply_to,media_path,file_id,file_unique_id,media_kind,source_chat,sender_chat,update_id)
           SELECT rowid,NULL,message_id,NULL,date,user_id,username,first_name,text,reply_to,media_path,file_id,file_unique_id,media_kind,NULL,NULL,update_id FROM messages;
         DROP TABLE messages;
         ALTER TABLE messages_v1 RENAME TO messages;
         CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_key ON messages(chat_id,message_id);",
    )?;
    tx.pragma_update(None, "user_version", 1)?;
    tx.commit()?;
    Ok(())
}

/// v2: backfill `chat_id` from `kv("chat_id")`. Channel posts never entered
/// history, so the two id spaces cannot legitimately collide; if the data says
/// otherwise the whole step rolls back with the number of conflicting rows.
/// Silently collapsing rows is forbidden.
pub fn migrate_v2(con: &mut Connection) -> Result<()> {
    let tx = con.transaction()?;
    let chat = migration_chat_id(&tx).ok_or_else(|| {
        anyhow!(
            "migration v2 aborted: CHATBOT_CHAT_ID is missing, empty or not a numeric chat id; \
             user_version and every existing chat_id are unchanged"
        )
    })?;
    let (keys, rows): (i64, i64) = tx.query_row(
        "SELECT COUNT(*), COALESCE(SUM(c),0) FROM (
           SELECT COUNT(*) c FROM messages
           WHERE chat_id IS NULL AND message_id IS NOT NULL
           GROUP BY message_id HAVING COUNT(*) > 1
         )",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if keys > 0 {
        return Err(anyhow!(
            "migration v2 aborted: {rows} rows in {keys} message_id groups collide on \
             (chat_id, message_id); nothing was changed"
        ));
    }
    tx.execute(
        "UPDATE messages SET chat_id=? WHERE chat_id IS NULL",
        params![chat],
    )?;
    tx.pragma_update(None, "user_version", 2)?;
    tx.commit()?;
    Ok(())
}

/// v3: FTS over the surrogate id with all three triggers. DELETE/UPDATE must
/// keep the external-content index honest; `rebuild` seeds it from rows that
/// existed before this migration.
fn migrate_v3(con: &mut Connection) -> Result<()> {
    let tx = con.transaction()?;
    tx.execute_batch(
        "DROP TRIGGER IF EXISTS messages_ai;
         DROP TRIGGER IF EXISTS messages_ad;
         DROP TRIGGER IF EXISTS messages_au;
         DROP TABLE IF EXISTS messages_fts;
         CREATE VIRTUAL TABLE messages_fts USING fts5(
           text, username, content='messages', content_rowid='id'
         );
         CREATE TRIGGER messages_ai AFTER INSERT ON messages BEGIN
           INSERT INTO messages_fts(rowid, text, username)
           VALUES (new.id, new.text, new.username);
         END;
         CREATE TRIGGER messages_ad AFTER DELETE ON messages BEGIN
           INSERT INTO messages_fts(messages_fts, rowid, text, username)
           VALUES('delete', old.id, old.text, old.username);
         END;
         CREATE TRIGGER messages_au AFTER UPDATE ON messages BEGIN
           INSERT INTO messages_fts(messages_fts, rowid, text, username)
           VALUES('delete', old.id, old.text, old.username);
           INSERT INTO messages_fts(rowid, text, username)
           VALUES (new.id, new.text, new.username);
         END;
         INSERT INTO messages_fts(messages_fts) VALUES('rebuild');",
    )?;
    tx.pragma_update(None, "user_version", 3)?;
    tx.commit()?;
    Ok(())
}

/// v4: blob records. `messages.media_path` stays as the read projection until
/// the media cycle moves readers to the blob, so no reader breaks here.
fn migrate_v4(con: &mut Connection) -> Result<()> {
    let tx = con.transaction()?;
    tx.execute_batch(
        "CREATE TABLE IF NOT EXISTS blobs(
           id INTEGER PRIMARY KEY,
           path TEXT NOT NULL UNIQUE,
           kind TEXT,
           size INTEGER,
           created_at INTEGER NOT NULL
         );
         INSERT OR IGNORE INTO blobs(path, kind, size, created_at)
           SELECT media_path, media_kind, NULL, CAST(strftime('%s','now') AS INTEGER)
           FROM messages
           WHERE media_path IS NOT NULL AND media_path <> '';",
    )?;
    tx.pragma_update(None, "user_version", 4)?;
    tx.commit()?;
    Ok(())
}

/// v6: two-phase media and the real identity. `update_id` loses its UNIQUE
/// constraint (it is a stream cursor, not an identity); `blob_id` and
/// `media_state` carry the media descriptor, while path/size/kind live in
/// `blobs` once `finalize_blob` runs. FTS is rebuilt because the table is
/// recreated.
fn migrate_v6(con: &mut Connection) -> Result<()> {
    let tx = con.transaction()?;
    tx.execute_batch(
        "DROP TRIGGER IF EXISTS messages_ai;
         DROP TRIGGER IF EXISTS messages_ad;
         DROP TRIGGER IF EXISTS messages_au;
         DROP TABLE IF EXISTS messages_fts;
         DROP TABLE IF EXISTS messages_v6;
         CREATE TABLE messages_v6(
           id INTEGER PRIMARY KEY,
           chat_id INTEGER,
           message_id INTEGER,
           thread_id INTEGER,
           date INTEGER,
           user_id INTEGER,
           username TEXT,
           first_name TEXT,
           text TEXT,
           reply_to INTEGER,
           media_path TEXT,
           file_id TEXT,
           file_unique_id TEXT,
           media_kind TEXT,
           source_chat INTEGER,
           sender_chat INTEGER,
           update_id INTEGER,
           media_state TEXT NOT NULL DEFAULT 'absent',
           media_size INTEGER,
           blob_id INTEGER
         );
         INSERT INTO messages_v6(id,chat_id,message_id,thread_id,date,user_id,username,first_name,text,reply_to,media_path,file_id,file_unique_id,media_kind,source_chat,sender_chat,update_id,media_state,media_size,blob_id)
           SELECT id,chat_id,message_id,thread_id,date,user_id,username,first_name,text,reply_to,media_path,file_id,file_unique_id,media_kind,source_chat,sender_chat,update_id,
             CASE
               WHEN media_path IS NOT NULL AND media_path <> '' THEN 'ready'
               WHEN media_kind IN ('animation','video','photo','video_note','document')
                    AND file_id IS NOT NULL AND file_id <> '' THEN 'descriptor'
               ELSE 'absent'
             END,
             NULL,
             (SELECT b.id FROM blobs b WHERE b.path = messages.media_path)
           FROM messages;
         DROP TABLE messages;
         ALTER TABLE messages_v6 RENAME TO messages;
         CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_key ON messages(chat_id,message_id);
         CREATE VIRTUAL TABLE messages_fts USING fts5(
           text, username, content='messages', content_rowid='id'
         );
         CREATE TRIGGER messages_ai AFTER INSERT ON messages BEGIN
           INSERT INTO messages_fts(rowid, text, username)
           VALUES (new.id, new.text, new.username);
         END;
         CREATE TRIGGER messages_ad AFTER DELETE ON messages BEGIN
           INSERT INTO messages_fts(messages_fts, rowid, text, username)
           VALUES('delete', old.id, old.text, old.username);
         END;
         CREATE TRIGGER messages_au AFTER UPDATE ON messages BEGIN
           INSERT INTO messages_fts(messages_fts, rowid, text, username)
           VALUES('delete', old.id, old.text, old.username);
           INSERT INTO messages_fts(rowid, text, username)
           VALUES (new.id, new.text, new.username);
         END;
         INSERT INTO messages_fts(messages_fts) VALUES('rebuild');",
    )?;
    tx.pragma_update(None, "user_version", 6)?;
    tx.commit()?;
    Ok(())
}

/// v5: journal skeleton. Cycles 3–5 own the transitions; this step only makes
/// the tables exist with the keys the state machine addresses rows by.
fn migrate_v5(con: &mut Connection) -> Result<()> {
    let tx = con.transaction()?;
    tx.execute_batch(
        "CREATE TABLE IF NOT EXISTS batches(
           id INTEGER PRIMARY KEY,
           batch_key TEXT NOT NULL UNIQUE,
           chat_id INTEGER,
           thread_id INTEGER,
           state TEXT NOT NULL,
           outcome TEXT,
           sealed_at INTEGER,
           updated_at INTEGER
         );
         CREATE TABLE IF NOT EXISTS attempts(
           id INTEGER PRIMARY KEY,
           batch_id INTEGER NOT NULL REFERENCES batches(id),
           attempt_no INTEGER NOT NULL,
           state TEXT NOT NULL,
           started_at INTEGER,
           settled_at INTEGER,
           UNIQUE(batch_id, attempt_no)
         );
         CREATE TABLE IF NOT EXISTS answers(
           id INTEGER PRIMARY KEY,
           batch_id INTEGER REFERENCES batches(id),
           state TEXT NOT NULL,
           text TEXT,
           created_at INTEGER,
           closed_at INTEGER
         );
         CREATE TABLE IF NOT EXISTS answer_batches(
           answer_id INTEGER NOT NULL REFERENCES answers(id),
           batch_id INTEGER NOT NULL REFERENCES batches(id),
           PRIMARY KEY(answer_id, batch_id)
         );
         CREATE TABLE IF NOT EXISTS sends(
           id INTEGER PRIMARY KEY,
           answer_id INTEGER REFERENCES answers(id),
           send_key TEXT NOT NULL UNIQUE,
           kind TEXT NOT NULL,
           state TEXT NOT NULL,
           receipt TEXT,
           created_at INTEGER,
           updated_at INTEGER
         );
         CREATE TABLE IF NOT EXISTS sessions(
           id INTEGER PRIMARY KEY,
           name TEXT NOT NULL UNIQUE,
           session_uid TEXT,
           generation INTEGER,
           state TEXT NOT NULL,
           updated_at INTEGER
         );
         CREATE TABLE IF NOT EXISTS hosts(
           id INTEGER PRIMARY KEY,
           generation TEXT NOT NULL UNIQUE,
           pid INTEGER,
           ext_hash TEXT,
           state TEXT NOT NULL,
           updated_at INTEGER
         );
         CREATE TABLE IF NOT EXISTS incidents(
           id INTEGER PRIMARY KEY,
           kind TEXT NOT NULL,
           detail TEXT,
           at INTEGER NOT NULL,
           resolved_at INTEGER
         );",
    )?;
    tx.pragma_update(None, "user_version", 5)?;
    tx.commit()?;
    Ok(())
}

/// Single-instance guard: `flock` on `<state>/chatd.lock` for the lifetime of
/// the returned handle. A second `run`/`sync` gets an error naming the state
/// directory instead of racing the first on the same cursor.
#[derive(Debug)]
pub struct StateLock {
    _file: std::fs::File,
    path: PathBuf,
}

impl StateLock {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub fn acquire_state_lock(state: &str) -> Result<StateLock> {
    std::fs::create_dir_all(state)?;
    let path = Path::new(state).join("chatd.lock");
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&path)
        .with_context(|| format!("open state lock {}", path.display()))?;
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EWOULDBLOCK) {
            return Err(anyhow!(
                "another chatd instance is already running for state {} (lock {} is held); \
                 refusing to start a second instance",
                state,
                path.display()
            ));
        }
        return Err(err).with_context(|| format!("flock {}", path.display()));
    }
    Ok(StateLock { _file: file, path })
}

pub fn db(state: &str) -> Result<Connection> {
    std::fs::create_dir_all(state)?;
    let mut con = Connection::open(Path::new(state).join("history.db"))?;
    // The connection now lives in the `chatdb` worker thread for the whole
    // process lifetime (see `db.rs`), so prepared statements are worth
    // caching: `kv_get` alone runs several times per batch.
    con.set_prepared_statement_cache_capacity(32);
    // WAL keeps readers (the control socket serving `gif_list`) off the writer's
    // back, so a held write transaction cannot stall the single-threaded runtime
    // — the push writer that delivers chat batches lives on the same thread.
    // journal_mode is persisted in the file; busy_timeout is per connection and
    // deliberately SHORT (300 ms): the rusqlite default of 5 s would let any
    // residual contention freeze chat delivery. The control path reports such a
    // failure as `{"ok":false,"error":"busy"}` instead of waiting.
    let _ = con.pragma_update(None, "journal_mode", "WAL");
    con.busy_timeout(Duration::from_millis(300))?;
    // Refuse before ANY schema write: a wrong/missing chat id must not advance
    // user_version or touch a historic row. The error names the variable.
    let version: i64 = con.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    require_migration_chat_id(&con, version)?;
    con.execute_batch(SCHEMA)?;
    // Column migrations (idempotent).
    let _ = con.execute("ALTER TABLE messages ADD COLUMN file_id TEXT", []);
    // Historic rows have no file_unique_id (Bot API cannot derive one from a
    // file_id); the gif registry falls back to file_id for those.
    let _ = con.execute("ALTER TABLE messages ADD COLUMN file_unique_id TEXT", []);
    // Telegram's message field the media arrived in (`animation` = gif,
    // `video`, `photo`, `sticker`, ...). NULL on historic rows and on the
    // bot's own mirrored sends: the kind is only knowable at ingest, and a
    // row without it is honestly NOT a gif (see GIF_PAGE_SQL).
    let _ = con.execute("ALTER TABLE messages ADD COLUMN media_kind TEXT", []);
    // The chat identity is the only input of migration v2. Seed it while the
    // environment still knows it; an absent/invalid value left v2 refusing to
    // run above, so there is no silent zero backfill.
    if let Ok(raw) = std::env::var("CHATBOT_CHAT_ID") {
        let chat = raw.trim();
        if !chat.is_empty() && chat.parse::<i64>().is_ok() {
            let _ = con.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('chat_id',?)", [chat]);
        }
    }
    // Schema steps v1..v5 run before the daemon serves anything. A failure
    // (for example v2's collision check) stops startup instead of guessing.
    migrate(&mut con)?;
    ensure_journal_columns(&con)?;
    // A missing index costs latency, not correctness: log instead of refusing to
    // start when the file is momentarily write-locked.
    if let Err(e) = ensure_indexes(&con) {
        dlog(&format!("ensure_indexes failed: {e:#}"));
    }
    Ok(con)
}

/// Cycle 3 columns on the v5 journal skeleton. Additive and idempotent, in the
/// same spirit as the legacy `messages` ALTERs above: an existing database gains
/// them at the next start without a table rebuild.
fn ensure_journal_columns(con: &Connection) -> Result<()> {
    for stmt in [
        "ALTER TABLE batches ADD COLUMN message_ids TEXT",
        "ALTER TABLE batches ADD COLUMN tome TEXT",
        "ALTER TABLE answers ADD COLUMN fingerprint TEXT",
        "ALTER TABLE answers ADD COLUMN response TEXT",
        "ALTER TABLE attempts ADD COLUMN emitted_at INTEGER",
        "ALTER TABLE sends ADD COLUMN request TEXT",
    ] {
        let _ = con.execute(stmt, []);
    }
    con.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_batches_state ON batches(state);
         CREATE INDEX IF NOT EXISTS idx_answers_batch ON answers(batch_id);
         CREATE INDEX IF NOT EXISTS idx_attempts_batch ON attempts(batch_id, id DESC);
         CREATE INDEX IF NOT EXISTS idx_sends_answer ON sends(answer_id);
         CREATE INDEX IF NOT EXISTS idx_sends_state ON sends(state);",
    )?;
    Ok(())
}

pub fn kv_get(con: &Connection, k: &str, default: &str) -> String {
    con.prepare_cached("SELECT v FROM kv WHERE k=?")
        .and_then(|mut s| s.query_row([k], |r| r.get::<_, String>(0)))
        .unwrap_or_else(|_| default.to_string())
}

pub fn kv_set(con: &Connection, k: &str, v: &str) -> Result<()> {
    con.execute("INSERT OR REPLACE INTO kv(k,v) VALUES(?,?)", params![k, v])?;
    Ok(())
}

pub fn kv_del(con: &Connection, k: &str) -> Result<()> {
    con.execute("DELETE FROM kv WHERE k=?", [k])?;
    Ok(())
}

// ---------- messages ----------

/// Telegram `message_id` inside one chat. A bare integer is ambiguous across
/// chats, so it is only meaningful together with [`MessageKey::chat`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageId(pub i64);

/// The mirror's identity. `update_id` deliberately does not appear here: a
/// redelivery with a new `update_id` is the same message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MessageKey {
    pub chat: i64,
    pub message: MessageId,
}

/// Outcome of a page commit. `inserted + duplicate + ignored` is the number of
/// envelopes the page carried; there is no `Ok(None)` that hides which case
/// happened. A failed INSERT is an `Err`, never a silent duplicate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Applied {
    pub inserted: usize,
    pub duplicate: usize,
    pub ignored: usize,
}

/// A row the page actually wrote. The ingest unit needs its surrogate id to
/// enqueue the media fetch, and its key to build the batch row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InsertedRow {
    pub key: MessageKey,
    pub row_id: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AppliedPage {
    pub applied: Applied,
    pub inserted: Vec<InsertedRow>,
}

/// One admitted Telegram update, already parsed and detached from the JSON
/// frame. It owns every string, so a page of envelopes moves into the database
/// closure without borrowing the ingest task.
#[derive(Debug, Clone)]
pub struct Envelope {
    pub key: MessageKey,
    pub update_id: i64,
    pub date: i64,
    pub user_id: Option<i64>,
    pub username: String,
    pub first_name: String,
    pub text: String,
    pub reply_to: Option<i64>,
    pub thread_id: Option<i64>,
    pub media_path: String,
    pub file_id: String,
    pub file_unique_id: String,
    pub media_kind: String,
    /// `Pending` for a downloadable kind, `Descriptor` for a marker-only kind,
    /// `Absent` for text. A new row never starts `Ready`.
    pub media_state: MediaState,
    /// Telegram `file_size` when the message object carried it; the budget
    /// uses it to refuse a download before any bytes are fetched.
    pub media_size: Option<u64>,
    pub source_chat: Option<i64>,
    pub sender_chat: Option<i64>,
    /// A channel post rather than a message in the bot's chat. Kept on the
    /// envelope for routing/cursor bookkeeping; it is NOT an identity signal.
    pub cross: bool,
}

impl Envelope {
    /// A non-positive message id is not a message (service/placeholder frame):
    /// it is counted as `ignored`, and the page still advances the cursor.
    pub fn is_storable(&self) -> bool {
        self.key.message.0 > 0
    }

    /// The row the batch/context renderer consumes. Media path is empty at
    /// ingest: the text marker carries the kind until the fetcher finalizes.
    pub fn to_batch_msg(&self) -> BatchMsg {
        BatchMsg {
            message_id: self.key.message.0,
            user_id: self.user_id,
            username: self.username.clone(),
            first_name: self.first_name.clone(),
            text: self.text.clone(),
            reply_to: self.reply_to,
            media_path: self.media_path.clone(),
            file_id: self.file_id.clone(),
            media_kind: self.media_kind.clone(),
            to_me: false,
        }
    }
}

/// Result of the legacy single-message ingest path. `row` is `Some` only when a
/// new row was written; a duplicate is explicit, and an INSERT failure is an
/// `Err` from `store_prepared` instead of `Ok(None)`.
#[derive(Debug, Clone)]
pub struct StoredRow {
    pub applied: Applied,
    pub row: Option<BatchMsg>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BatchMsg {
    pub message_id: i64,
    pub user_id: Option<i64>,
    pub username: String,
    pub first_name: String,
    pub text: String,
    pub reply_to: Option<i64>,
    pub media_path: String,
    /// Telegram media field (`animation`, `video`, `photo`, `sticker`, ...);
    /// empty for a text message. Distinguishes a gif from a video on the way in.
    pub media_kind: String,
    /// Telegram file_id of the media, so a sticker can be re-sent verbatim.
    pub file_id: String,
    pub to_me: bool,
}

/// Everything needed to write one message, parsed before the database actor is
/// touched. Media is only described here; the download belongs to the fetcher.
#[derive(Debug, Clone)]
pub struct PreparedMessage {
    pub update_id: i64,
    pub message_id: Option<i64>,
    pub date: i64,
    pub user_id: Option<i64>,
    pub username: String,
    pub first_name: String,
    pub text: String,
    pub reply_to: Option<i64>,
    pub thread_id: Option<i64>,
    pub media_path: String,
    pub file_id: String,
    pub file_unique_id: String,
    /// Telegram field the media arrived in (`animation` = gif, `video`,
    /// `photo`, `sticker`, ...); empty when the message had no media.
    pub media_kind: String,
    pub media_state: MediaState,
    pub media_size: Option<u64>,
    pub source_chat: Option<i64>,
    pub sender_chat: Option<i64>,
    pub cross: bool,
}

/// The Telegram message field that carried an update's media, in the daemon's
/// precedence order. `animation` is what Telegram calls a GIF; `video` is a
/// real video, `sticker` a sticker. This distinction exists ONLY on the
/// incoming message: file_id/file_unique_id cannot be classified afterwards
/// (Telegram serves `.mp4` for both animation and video), so the field name is
/// stored as `media_kind` and never guessed from the bytes.
pub fn pick_media(msg: &Value) -> Option<(String, Value)> {
    crate::media::media_kind_of(msg).map(|(kind, obj)| (kind.as_str().to_string(), obj))
}

/// Parse one Telegram message into an envelope. No I/O: media is only
/// described here, so the page can be committed before any download starts.
/// Returns None when a strict-chat message belongs to another chat.
pub fn envelope_from_message(
    msg: &Value,
    update_id: i64,
    strict_chat: bool,
    fallback_chat: i64,
) -> Result<Option<Envelope>> {
    let chat = msg
        .get("chat")
        .and_then(|c| c.get("id"))
        .and_then(|v| v.as_i64())
        .unwrap_or(fallback_chat);
    if strict_chat {
        let want: i64 = required_cfg("CHATBOT_CHAT_ID")?
            .parse()
            .unwrap_or(fallback_chat);
        if chat != want {
            return Ok(None);
        }
    }
    let frm = msg.get("from").cloned().unwrap_or(json!({}));
    let sch = msg.get("sender_chat").cloned().unwrap_or(json!({}));
    let user_id: Option<i64> = frm
        .get("id")
        .and_then(|v| v.as_i64())
        .or_else(|| sch.get("id").and_then(|v| v.as_i64()));
    let username = frm
        .get("username")
        .and_then(|v| v.as_str())
        .or_else(|| sch.get("username").and_then(|v| v.as_str()))
        .unwrap_or("")
        .to_string();
    let first_name = frm
        .get("first_name")
        .and_then(|v| v.as_str())
        .or_else(|| sch.get("title").and_then(|v| v.as_str()))
        .unwrap_or("")
        .to_string();
    let mut text = msg
        .get("text")
        .and_then(|v| v.as_str())
        .or_else(|| msg.get("caption").and_then(|v| v.as_str()))
        .unwrap_or("")
        .to_string();
    let message_id = msg.get("message_id").and_then(|v| v.as_i64()).unwrap_or(0);
    let date = msg.get("date").and_then(|v| v.as_i64()).unwrap_or(0);
    let cross = msg
        .get("_cross_chat")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let reply_to: Option<i64> = if cross {
        None
    } else {
        msg.get("reply_to_message")
            .and_then(|r| r.get("message_id"))
            .and_then(|v| v.as_i64())
    };
    let thread_id = msg.get("message_thread_id").and_then(|v| v.as_i64());
    let source_chat = msg
        .get("forward_from_chat")
        .and_then(|c| c.get("id"))
        .and_then(|v| v.as_i64());
    let sender_chat = sch.get("id").and_then(|v| v.as_i64());

    let mut file_id = String::new();
    let mut file_unique_id = String::new();
    let mut media_kind = String::new();
    let mut media_state = MediaState::Absent;
    let mut media_size = None;
    if let Some((kind, obj)) = crate::media::media_kind_of(msg) {
        media_kind = kind.as_str().to_string();
        file_id = obj
            .get("file_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        file_unique_id = obj
            .get("file_unique_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        media_size = obj
            .get("file_size")
            .and_then(|v| v.as_i64())
            .filter(|n| *n >= 0)
            .map(|n| n as u64);
        let duration = obj.get("duration").and_then(|v| v.as_i64());
        let mime = obj.get("mime_type").and_then(|v| v.as_str()).unwrap_or("");
        text.push(' ');
        text.push_str(&kind.marker(mime, duration));
        media_state = if kind.needs_download() && !file_id.is_empty() {
            MediaState::Pending
        } else {
            MediaState::Descriptor
        };
    }

    Ok(Some(Envelope {
        key: MessageKey {
            chat,
            message: MessageId(message_id),
        },
        update_id,
        date,
        user_id,
        username,
        first_name,
        text,
        reply_to,
        thread_id,
        media_path: String::new(),
        file_id,
        file_unique_id,
        media_kind,
        media_state,
        media_size,
        source_chat,
        sender_chat,
        cross,
    }))
}

/// Legacy single-message entry point for `chatd sync`: parse only, never fetch.
pub async fn prepare_message(
    msg: &Value,
    _state: &str,
    strict_chat: bool,
) -> Result<Option<PreparedMessage>> {
    let update_id = msg
        .get("_update_id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| anyhow!("prepare_message: missing _update_id"))?;
    let fallback: i64 = cfg("CHATBOT_CHAT_ID", "0").parse().unwrap_or(0);
    let Some(env) = envelope_from_message(msg, update_id, strict_chat, fallback)? else {
        return Ok(None);
    };
    let cross = msg
        .get("_cross_chat")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    Ok(Some(PreparedMessage {
        update_id: env.update_id,
        message_id: Some(env.key.message.0),
        date: env.date,
        user_id: env.user_id,
        username: env.username,
        first_name: env.first_name,
        text: env.text,
        reply_to: env.reply_to,
        thread_id: env.thread_id,
        media_path: env.media_path,
        file_id: env.file_id,
        file_unique_id: env.file_unique_id,
        media_kind: env.media_kind,
        media_state: env.media_state,
        media_size: env.media_size,
        source_chat: env.source_chat,
        sender_chat: env.sender_chat,
        cross,
    }))
}

/// Write a prepared message as ONE atomic step: the `(chat_id, message_id)`
/// identity check and the INSERT. Content (author/date/text) is never an
/// identity: two real Telegram messages that happen to say the same thing are
/// two messages. Media is left `pending` for the fetcher.
pub fn store_prepared(con: &Connection, prep: &PreparedMessage, _state: &str) -> Result<StoredRow> {
    let PreparedMessage {
        update_id,
        message_id,
        date,
        user_id,
        username,
        first_name,
        text,
        reply_to,
        thread_id,
        media_path,
        file_id,
        file_unique_id,
        media_kind,
        media_state,
        media_size,
        source_chat,
        sender_chat,
        cross: _cross,
    } = prep.clone();
    let chat: i64 = kv_get(con, "chat_id", "0").parse().unwrap_or(0);
    // The identity is (chat_id, message_id). `update_id` is only the stream
    // cursor, so the same message redelivered under a new cursor is a
    // duplicate here too.
    if let Some(mid) = message_id {
        let exists: Option<i64> = con
            .prepare_cached("SELECT 1 FROM messages WHERE chat_id=? AND message_id=? LIMIT 1")?
            .query_row(params![chat, mid], |r| r.get(0))
            .ok();
        if exists.is_some() {
            return Ok(StoredRow {
                applied: Applied {
                    duplicate: 1,
                    ..Default::default()
                },
                row: None,
            });
        }
    }
    // Identity is (chat_id, message_id) only. Author/date/text are never a
    // second key, so there is no content lookup here.
    // `OR IGNORE` collapses only the (chat_id, message_id) key; every other
    // failure (disk, schema, trigger) is still an Err.
    let changed = con.execute(
        "INSERT OR IGNORE INTO messages(chat_id,update_id,message_id,thread_id,date,user_id,username,first_name,text,reply_to,media_path,file_id,file_unique_id,media_kind,media_state,media_size,source_chat,sender_chat) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        params![chat, update_id, message_id, thread_id, date, user_id, username, first_name, text, reply_to, media_path, file_id, file_unique_id, media_kind, media_state.as_str(), media_size.map(|s| s as i64), source_chat, sender_chat],
    )?;
    if changed == 0 {
        return Ok(StoredRow {
            applied: Applied {
                duplicate: 1,
                ..Default::default()
            },
            row: None,
        });
    }
    Ok(StoredRow {
        applied: Applied {
            inserted: 1,
            ..Default::default()
        },
        row: Some(BatchMsg {
            message_id: message_id.unwrap_or(0),
            user_id,
            username,
            first_name,
            text,
            reply_to,
            media_path,
            file_id,
            media_kind,
            to_me: false,
        }),
    })
}

/// Media sweep throttle. Measured (`dbbench --media-files 1500`): the sweep
/// costs ~2.9 ms per incoming message once the media dir holds 1500 files, which
/// is real latency on the ingest path. Sweeping at most every 5 minutes changes
/// no observable bot behaviour (files are pruned later, never lost) and removes
/// that cost from the per-message path.
const PRUNE_MIN_INTERVAL: Duration = Duration::from_secs(300);
static LAST_PRUNE_MS: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

pub fn maybe_prune_media(state: &str) {
    let now = chrono::Utc::now().timestamp_millis();
    let last = LAST_PRUNE_MS.load(std::sync::atomic::Ordering::Relaxed);
    if last != 0 && now.saturating_sub(last) < PRUNE_MIN_INTERVAL.as_millis() as i64 {
        return;
    }
    LAST_PRUNE_MS.store(now, std::sync::atomic::Ordering::Relaxed);
    maybe_prune_media_n(state, 1500, 2000);
}

pub fn maybe_prune_media_n(state: &str, keep: usize, trigger: usize) {
    let d = Path::new(state).join("media");
    let entries = match std::fs::read_dir(&d) {
        Ok(e) => e,
        Err(_) => return,
    };
    let mut files: Vec<(std::time::SystemTime, PathBuf)> = vec![];
    for e in entries.flatten() {
        let p = e.path();
        if p.is_file() {
            let mt = std::fs::metadata(&p)
                .and_then(|m| m.modified())
                .unwrap_or(std::time::UNIX_EPOCH.into());
            files.push((mt, p));
        }
    }
    if files.len() <= trigger {
        return;
    }
    files.sort_by_key(|(t, _)| *t);
    for (_, p) in files.iter().take(files.len().saturating_sub(keep)) {
        let _ = std::fs::remove_file(p);
    }
}

// ---------- omo host ----------

/// Dedicated host socket of the bot. Portable default: inside the state dir,
/// because the host is ours alone — sharing a host with another bot would leak
/// `chat_send` into foreign sessions.
pub fn host_socket() -> String {
    match std::env::var("CHATBOT_HOST_SOCKET") {
        Ok(v) if !v.is_empty() => v,
        _ => Path::new(&state_dir())
            .join("host")
            .join("rpc.sock")
            .to_string_lossy()
            .to_string(),
    }
}

/// Daemon log: stderr (journal, when it works) AND state/chatd.log
/// (append, rotated at ~2MB). Under some systemd setups daemon stdout never
/// reaches the journal, so the file is the reliable record an external
/// watchdog greps. Uses state_dir(), never panics on I/O errors.
pub fn dlog(msg: &str) {
    dlog_at(&state_dir(), msg);
}

/// Same append/rotation contract as [`dlog`], against an explicit state dir.
/// The ops tests need a log they can read without racing other tests over the
/// process-wide `CHATBOT_STATE_DIR`.
pub fn dlog_at(dir: &str, msg: &str) {
    eprintln!("{msg}");
    let path = Path::new(dir).join("chatd.log");
    if std::fs::create_dir_all(dir).is_err() {
        return;
    }
    if let Ok(m) = std::fs::metadata(&path) {
        if m.len() > 2_000_000 {
            let prev = Path::new(dir).join("chatd.log.1");
            let _ = std::fs::remove_file(&prev);
            let _ = std::fs::rename(&path, &prev);
        }
    }
    use std::io::Write;
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
    {
        let _ = writeln!(
            f,
            "{} {msg}",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ")
        );
    }
}

/// True when a host is actually listening. Cheap per-poll-cycle check:
/// a vanished socket means the host died and every delivery would stall
/// on the admit timeout — the run loop re-ensures the host instead.
/// A bare exists() is not enough: kill -9 leaves the path behind with
/// nobody listening, so a connect probe decides (still no subprocess).
/// A wedged-but-listening host stays covered by the admit-timeout
/// recreate path in deliver_batch_inner.
pub fn host_alive() -> bool {
    let sock = host_socket();
    let p = Path::new(&sock);
    if !p.exists() {
        return false;
    }
    std::os::unix::net::UnixStream::connect(p).is_ok()
}

fn agent_env(cmd: &mut tokio::process::Command) {
    let agent_dir = cfg("CHATBOT_AGENT_DIR", "");
    if !agent_dir.is_empty() {
        for k in [
            "OMO_CODING_AGENT_DIR",
            "SENPI_CODING_AGENT_DIR",
            "PI_CODING_AGENT_DIR",
        ] {
            cmd.env(k, &agent_dir);
        }
    }
}

/// Last N bytes of s, cut at a UTF-8 char boundary (byte slicing panics
/// mid-char; stderr text may be Cyrillic).
fn truncate_tail(s: &str, n: usize) -> String {
    if s.len() <= n {
        return s.to_string();
    }
    let mut i = s.len() - n;
    while !s.is_char_boundary(i) {
        i += 1;
    }
    s[i..].to_string()
}

fn truncate_err(s: &str) -> String {
    truncate_tail(s, 70)
}

/// First n bytes of s, cut at a UTF-8 char boundary.
fn truncate_head(s: &str, n: usize) -> String {
    if s.len() <= n {
        return s.to_string();
    }
    let mut end = n;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}

pub async fn omo(args: &[&str], timeout: Duration) -> Result<Value> {
    omo_on(args, timeout, &host_socket()).await
}

/// Same as omo() but against an explicit host socket (used by the
/// summarize path so it never mutates process-global routing).
#[allow(dead_code)]
pub async fn omo_on(args: &[&str], timeout: Duration, sock: &str) -> Result<Value> {
    let mut cmd = tokio::process::Command::new("omo");
    cmd.arg("host").arg("session");
    for a in args {
        cmd.arg(a);
    }
    cmd.arg("--socket").arg(&sock);
    agent_env(&mut cmd);
    let out = tokio::time::timeout(timeout, cmd.output())
        .await
        .map_err(|_| {
            let shown = args.join(" ");
            anyhow!("omo {shown}: timeout after {}s", timeout.as_secs())
        })??;
    if !out.status.success() {
        let shown = args.join(" ");
        let err = truncate_err(String::from_utf8_lossy(&out.stderr).trim());
        let text = format!("omo {shown}: rc={} {err}", out.status.code().unwrap_or(-1));
        return Err(anyhow!("{}", truncate_head(&text, 290)));
    }
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    serde_json::from_str(&stdout).map_err(|e| {
        let shown = args.join(" ");
        let pos = "unknown";
        let _ = (e, pos);
        anyhow!(
            "omo {shown}: bad output: {}",
            stdout.get(..80).unwrap_or(&stdout)
        )
    })
}

pub async fn omo_with_env(
    args: &[&str],
    timeout: Duration,
    extra_env: &HashMap<String, String>,
) -> Result<Value> {
    let sock = extra_env.get("SOCK").cloned().unwrap_or_else(host_socket);
    let mut cmd = tokio::process::Command::new("omo");
    cmd.arg("host").arg("session");
    for a in args {
        cmd.arg(a);
    }
    cmd.arg("--socket").arg(&sock);
    agent_env(&mut cmd);
    let out = tokio::time::timeout(timeout, cmd.output())
        .await
        .map_err(|_| anyhow!("omo {}: timeout", args.join(" ")))??;
    if !out.status.success() {
        return Err(anyhow!(
            "omo {}: rc={} {}",
            args.join(" "),
            out.status.code().unwrap_or(-1),
            truncate_err(String::from_utf8_lossy(&out.stderr).trim())
        ));
    }
    Ok(serde_json::from_str(&String::from_utf8_lossy(&out.stdout))?)
}

/// Runtime location of chat-ext.mjs: CHATBOT_CWD first (deployed checkout),
/// then the process working directory, then the compile-time manifest dir
/// (local `cargo run` convenience only — never relied upon in production).
fn ext_source() -> PathBuf {
    for base in [cfg("CHATBOT_CWD", ""), ".".to_string()] {
        if base.is_empty() {
            continue;
        }
        let p = Path::new(&base).join("chat-ext.mjs");
        if p.exists() {
            return p;
        }
    }
    Path::new(env!("CARGO_MANIFEST_DIR")).join("chat-ext.mjs")
}

/// Staged next to the launch spec and loaded by the same host as chat-ext.mjs.
const PHASE_STRIP_FILE: &str = "muse-spark-phase-strip.ts";

fn phase_strip_source() -> PathBuf {
    for base in [cfg("CHATBOT_CWD", ""), ".".to_string()] {
        if base.is_empty() {
            continue;
        }
        let p = Path::new(&base).join(PHASE_STRIP_FILE);
        if p.exists() {
            return p;
        }
    }
    Path::new(env!("CARGO_MANIFEST_DIR")).join(PHASE_STRIP_FILE)
}

pub fn write_launch_spec(state: &str, include_ext: bool) -> Result<PathBuf> {
    let state_path = Path::new(state);
    std::fs::create_dir_all(state_path)?;
    if include_ext {
        let src_path = ext_source();
        let src = std::fs::read(&src_path)
            .with_context(|| format!("chat-ext.mjs read: {}", src_path.display()))?;
        std::fs::write(state_path.join("chat-ext.mjs"), src)?;
        // The phase-strip extension ships with the bot so the muse-spark
        // provider no longer dead-ends on a replayed `phase` marker. Launch-spec
        // extension paths must resolve inside the spec dir, hence the copy.
        let strip_path = phase_strip_source();
        let strip = std::fs::read(&strip_path)
            .with_context(|| format!("muse-spark-phase-strip.ts read: {}", strip_path.display()))?;
        std::fs::write(state_path.join(PHASE_STRIP_FILE), strip)?;
    }
    let mut exts = vec![];
    if include_ext {
        exts.push("./chat-ext.mjs".to_string());
        exts.push(format!("./{PHASE_STRIP_FILE}"));
    }
    let mut spec = json!({
        "spec_version": 1,
        "core": {"session_runtime": "in-process", "multi_session": true, "extensions": exts},
        "env": {
            "OMO_CHATBOT_STATE_DIR": state,
            "OMO_CHATBOT_CHAT_ID": required_cfg("CHATBOT_CHAT_ID")?,
        },
    });
    // A persistent host never idle-exits, which is what keeps the bot alive
    // between messages — but the summary host uses a scratch socket and is
    // closed after summarize, so making IT persistent would strand a whole
    // supervisor+child pair at an unreachable path. Only the chat host gets it.
    if include_ext {
        if let Some(env) = spec.get_mut("env").and_then(|e| e.as_object_mut()) {
            env.insert("SENPI_RPC_HOST_COLD_START".to_string(), json!("persistent"));
        }
    }
    let spec_path = state_path.join(if include_ext {
        "launch-spec.json"
    } else {
        "launch-spec-summary.json"
    });
    std::fs::write(&spec_path, serde_json::to_string(&spec)?)?;
    Ok(spec_path)
}

pub async fn ensure_host_with(state: &str, include_ext: bool) -> Result<PathBuf> {
    ensure_host_on(state, include_ext, &host_socket()).await
}

/// Same as ensure_host_with() but against an explicit host socket.
#[allow(dead_code)]
pub async fn ensure_host_on(state: &str, include_ext: bool, sock: &str) -> Result<PathBuf> {
    let spec_path = write_launch_spec(state, include_ext)?;
    // The default socket lives under <state>/host/, which does not exist in a
    // fresh clone; `omo host ensure` would otherwise fail on its own parent dir.
    if let Some(dir) = Path::new(sock).parent() {
        std::fs::create_dir_all(dir)?;
    }
    let agent_dir = cfg("CHATBOT_AGENT_DIR", "");
    // Whether a host was already serving this socket decides whether the ensure
    // spawns a NEW generation (which loads the staged extension) or merely
    // reuses a live one (which keeps the extension it was spawned with).
    let pre_alive = host_alive();
    let mut cmd = tokio::process::Command::new("omo");
    cmd.arg("host")
        .arg("ensure")
        .arg("--launch-spec")
        .arg(&spec_path)
        .arg("--socket")
        .arg(&sock);
    if !agent_dir.is_empty() {
        for k in [
            "OMO_CODING_AGENT_DIR",
            "SENPI_CODING_AGENT_DIR",
            "PI_CODING_AGENT_DIR",
        ] {
            cmd.env(k, &agent_dir);
        }
    }
    let out = cmd.output().await?;
    if !out.status.success() {
        return Err(anyhow!(
            "omo host ensure: rc={} {}",
            out.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&out.stderr)
                .trim()
                .chars()
                .rev()
                .take(200)
                .collect::<String>()
        ));
    }
    if include_ext && !pre_alive {
        // Only a fresh generation runs the staged file. Recording the hash after
        // a REUSE would claim the new code is live while the old extension keeps
        // serving — that false match is exactly what the reload guard must not
        // believe.
        if let Ok(h) = ext_hash() {
            store_host_ext_hash(state, &h);
        }
    }
    Ok(spec_path)
}

/// Hash of the extension source that a launch spec stages.
pub fn ext_hash() -> Result<String> {
    let src = std::fs::read(ext_source())?;
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(&src);
    Ok(format!("{:x}", h.finalize())[..16].to_string())
}

pub fn stored_host_ext_hash(state: &str) -> Option<String> {
    std::fs::read_to_string(Path::new(state).join("host-ext.sha"))
        .ok()
        .map(|s| s.trim().to_string())
}

pub fn store_host_ext_hash(state: &str, hash: &str) {
    let _ = std::fs::write(Path::new(state).join("host-ext.sha"), hash);
}

/// Stop the host that owns `sock`. Needed because a live host keeps the
/// extension it was spawned with: with the idle exit gone, nothing else would
/// ever reload a changed chat-ext.mjs.
pub async fn stop_host_on(sock: &str) -> Result<()> {
    let out = tokio::process::Command::new("omo")
        .arg("host")
        .arg("stop")
        .arg("--force")
        .arg("--socket")
        .arg(sock)
        .output()
        .await?;
    if !out.status.success() {
        return Err(anyhow!(
            "omo host stop: rc={} {}",
            out.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&out.stderr)
                .trim()
                .chars()
                .rev()
                .take(200)
                .collect::<String>()
        ));
    }
    Ok(())
}

pub async fn ensure_host(state: &str) -> Result<PathBuf> {
    ensure_host_with(state, true).await
}

/// Ensure exactly one LIVE session carries the configured name, then use it.
/// Reuses the recorded durable id when the host still has it; otherwise picks
/// the surviving session the repair kept. Only when no session carries the
/// name does it open a new one. Every close inside the repair goes by durable
/// id — a name is never a close argument.
pub async fn ensure_session(
    db: &crate::db::Db,
    system_path: &str,
    ref_: Option<&str>,
) -> Result<String> {
    let r = ref_
        .map(|s| s.to_string())
        .unwrap_or_else(session::configured_name);
    let state = state_dir();
    ensure_host(&state).await?;
    let recorded = db
        .call(|con| session::live_identity(con))
        .await
        .ok()
        .flatten()
        .map(|(uid, _)| uid);
    let report = session::repair_sessions_on(&r, &host_socket(), recorded.as_deref()).await?;
    if let Some(uid) = report.keep {
        let name = r.clone();
        let live = uid.clone();
        db.write(move |tx| {
            session::record_live(tx.handle(), &name, &live)?;
            Ok(())
        })
        .await?;
        return Ok(uid);
    }
    let out = omo(
        &[
            "open",
            "--cwd",
            &cfg("CHATBOT_CWD", "."),
            "--name",
            &r,
            "--prompt",
            &format!("@{system_path}"),
        ],
        Duration::from_secs(120),
    )
    .await?;
    let uid = out
        .get("durableSessionId")
        .and_then(|v| v.as_str())
        .unwrap_or(&r)
        .to_string();
    let name = r.clone();
    let live = uid.clone();
    db.write(move |tx| {
        session::record_live(tx.handle(), &name, &live)?;
        Ok(())
    })
    .await?;
    Ok(uid)
}

pub async fn recreate_session(
    db: &crate::db::Db,
    sid: &str,
    system_path: &str,
    ref_: Option<&str>,
    reason: &str,
) -> Result<String> {
    if !reason.is_empty() {
        dlog(&format!("recreate session {sid}: {reason}"));
    }
    match omo(&["close", sid], Duration::from_secs(30)).await {
        Ok(_) => {
            let dead = sid.to_string();
            db.write(move |tx| {
                session::mark_dead(tx.handle(), &dead)?;
                Ok(())
            })
            .await?;
        }
        Err(e) => dlog(&format!(
            "recreate session {sid}: close by id failed ({e:#}); keeping it instead of creating a duplicate"
        )),
    }
    ensure_session(db, system_path, ref_).await
}

// ---------- context blocks ----------

/// One recent message row: (message_id, user_id, username, first_name, text, media_path,
/// media_kind, file_id). New fields are APPENDED so positional readers (dbbench)
/// keep their indexes.
pub type RecentRow = (
    Option<i64>,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// The newest `n` messages, newest first. Shared by the batch builder (context
/// block AND the window anchor) so the same rows are no longer read twice.
pub fn recent_rows(con: &Connection, n: i64) -> Result<Vec<RecentRow>> {
    let mut stmt = con.prepare_cached(
        "SELECT message_id,user_id,username,first_name,text,media_path,media_kind,file_id FROM messages ORDER BY date DESC, message_id DESC LIMIT ?",
    )?;
    let rows: Vec<RecentRow> = stmt
        .query_map([n], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
            ))
        })?
        .flatten()
        .collect();
    Ok(rows)
}

/// Render the context block from rows already fetched by [`recent_rows`].
pub fn render_recent_context(
    rows: &[RecentRow],
    batch_ids: &HashSet<i64>,
    limit: i64,
    cap: usize,
    self_id: i64,
    answered: &HashSet<i64>,
) -> String {
    let mut blocks = vec![];
    for (mid, uid, user, first, text, media, media_kind, file_id) in rows.iter().rev() {
        if let Some(m) = mid {
            if batch_ids.contains(m) {
                continue;
            }
        }
        let who = match user.clone().unwrap_or_default().as_str() {
            "" => first.clone().unwrap_or_default(),
            u => format!("@{u}"),
        };
        let who = if who.is_empty() {
            uid.map(|u| u.to_string()).unwrap_or_default()
        } else {
            who
        };
        let mark = if self_id != 0 && *uid == Some(self_id) {
            " [моё]"
        } else {
            ""
        };
        let mut head = format!("[{}] {}{}:", mid.unwrap_or(0), who, mark);
        if mid.map(|m| answered.contains(&m)).unwrap_or(false) {
            head.push_str(" [на это уже отвечено — не отвечать]");
        }
        let mut chunk = vec![];
        let kind = media_kind.clone().unwrap_or_default();
        let fid = file_id.clone().unwrap_or_default();
        let body = if kind == "sticker" && !fid.is_empty() {
            crate::context::message_body(text.as_deref().unwrap_or(""), &kind, &fid)
        } else {
            text.clone().unwrap_or_default()
        };
        if !body.trim().is_empty() {
            let c: String = body.chars().take(cap).collect();
            chunk.push(format!("{head} {c}"));
        }
        if let Some(m) = media {
            if !m.is_empty() {
                chunk.push(format!("{head} [file] {m} -- read it via read tool"));
            }
        }
        if !chunk.is_empty() {
            blocks.push(chunk.join("\n"));
        }
    }
    let tail: Vec<String> = blocks
        .into_iter()
        .rev()
        .take(limit as usize)
        .rev()
        .collect();
    if tail.is_empty() {
        return String::new();
    }
    format!(
        "Контекст чата (последние {}):\n{}",
        tail.len(),
        tail.join("\n")
    )
}

/// Context block for callers that hold no precomputed answered set.
pub fn recent_context(
    state: &str,
    con: &Connection,
    batch_ids: &HashSet<i64>,
    limit: i64,
    cap: usize,
) -> Result<String> {
    let answered = answered_ids(state, con);
    recent_context_with(con, batch_ids, limit, cap, &answered)
}

/// Same block, reusing an answered set the caller already computed (`answered_ids`
/// reads the events journal; reading it twice per batch doubled that work).
pub fn recent_context_with(
    con: &Connection,
    batch_ids: &HashSet<i64>,
    limit: i64,
    cap: usize,
    answered: &HashSet<i64>,
) -> Result<String> {
    let self_id: i64 = kv_get(con, "self_id", "0").parse().unwrap_or(0);
    let rows = recent_rows(con, limit + batch_ids.len() as i64 + 5)?;
    Ok(render_recent_context(
        &rows, batch_ids, limit, cap, self_id, answered,
    ))
}

pub fn mirror_own_send(
    con: &Connection,
    message_id: i64,
    text: &str,
    reply_to: Option<i64>,
) -> Result<()> {
    let self_id: i64 = kv_get(con, "self_id", "0").parse().unwrap_or(0);
    let mut self_id = self_id;
    if self_id == 0 {
        // Best-effort: without self id we cannot attribute; skip.
        return Ok(());
    }
    let _ = &mut self_id;
    let username = kv_get(con, "self_username", "");
    let first = kv_get(con, "self_first", "");
    let now = chrono::Utc::now().timestamp();
    let chat: i64 = kv_get(con, "chat_id", "0").parse().unwrap_or(0);
    let _ = con.execute(
        "INSERT OR IGNORE INTO messages(chat_id,update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        params![chat, -message_id, message_id, now, self_id, username, first, text, reply_to, "", ""],
    )?;
    Ok(())
}

pub async fn mirror_own_send_auto(
    con: &Connection,
    message_id: i64,
    text: &str,
    reply_to: Option<i64>,
) -> Result<()> {
    let self_id: i64 = kv_get(con, "self_id", "0").parse().unwrap_or(0);
    if self_id == 0 {
        match api("getMe", &json!({})).await {
            Ok(me) => {
                let id: i64 = me.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
                if id == 0 {
                    return Ok(());
                }
                kv_set(con, "self_id", &id.to_string())?;
                kv_set(
                    con,
                    "self_username",
                    me.get("username").and_then(|v| v.as_str()).unwrap_or(""),
                )?;
                kv_set(
                    con,
                    "self_first",
                    me.get("first_name").and_then(|v| v.as_str()).unwrap_or(""),
                )?;
                mirror_own_send(con, message_id, text, reply_to)?;
            }
            Err(_) => {}
        }
        return Ok(());
    }
    mirror_own_send(con, message_id, text, reply_to)
}

pub fn answered_guard(con: &Connection, limit: i64) -> Result<String> {
    let self_id: i64 = kv_get(con, "self_id", "0").parse().unwrap_or(0);
    if self_id == 0 {
        return Ok(String::new());
    }
    let mut stmt = con.prepare_cached(
        "SELECT reply_to, substr(text, 1, 120) FROM messages WHERE user_id=? AND reply_to IS NOT NULL ORDER BY date DESC LIMIT ?",
    )?;
    let rows: Vec<(Option<i64>, Option<String>)> = stmt
        .query_map(params![self_id, limit], |r| Ok((r.get(0)?, r.get(1)?)))?
        .flatten()
        .collect();
    let mut lines = vec![];
    for (qid, ans) in rows {
        let qid = match qid {
            Some(q) => q,
            None => continue,
        };
        let qtext: Option<String> = con
            .prepare_cached("SELECT substr(text, 1, 80) FROM messages WHERE message_id=? LIMIT 1")
            .and_then(|mut s| s.query_row([qid], |r| r.get(0)))
            .ok();
        let q = qtext.unwrap_or_else(|| "?".to_string());
        let a = ans.unwrap_or_else(|| "[gif]".to_string());
        let a = if a.trim().is_empty() {
            "[gif]".to_string()
        } else {
            a.trim().to_string()
        };
        lines.push(format!("- [{qid}] «{q}» → ты уже ответил: «{a}»"));
    }
    if lines.is_empty() {
        return Ok(String::new());
    }
    Ok("Отвеченное ранее (не отвечай повторно):\n".to_string() + &lines.join("\n"))
}

/// reply_to targets of our own recent sends, read from the events file tail
/// (not the mirror): receipt mirroring can lag batch building by a full
/// cycle (observed 57s: receipt 10:25:47, mirror row date 10:26:44), and a
/// filter built on the mirror alone would then miss just-answered messages.
pub fn recent_receipt_reply_tos(state: &str) -> HashSet<i64> {
    let path = events_path(state);
    let data = match std::fs::read(&path) {
        Ok(d) => d,
        Err(_) => return HashSet::new(),
    };
    const TAIL: usize = 256 * 1024;
    let cut = data.len() > TAIL;
    let slice = if cut {
        &data[data.len() - TAIL..]
    } else {
        &data[..]
    };
    let text = String::from_utf8_lossy(slice);
    let mut out = HashSet::new();
    let mut first = true;
    for line in text.split('\n') {
        if first {
            // possibly a torn head fragment from the byte cut
            first = false;
            if cut {
                continue;
            }
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let kind = v.get("kind").and_then(|x| x.as_str()).unwrap_or("");
        if kind != "chat_send_result" && kind != "chat_send_replay" {
            continue;
        }
        if let Some(q) = v
            .get("receipt")
            .and_then(|r| r.get("reply_to"))
            .and_then(|x| x.as_i64())
        {
            out.insert(q);
        }
    }
    out
}

/// Every inbound message id the bot already answered: mirror (own sends'
/// reply_to) union recent receipts (covers mirror lag). Used to mark
/// answered messages inline in context and to drop them from fresh batches,
/// so a recreated (memory-less) session cannot re-answer them.
pub fn answered_ids(state: &str, con: &Connection) -> HashSet<i64> {
    let self_id: i64 = kv_get(con, "self_id", "0").parse().unwrap_or(0);
    let mut out = recent_receipt_reply_tos(state);
    // Journal closure is the authority, not the quote. An answer sent without
    // `reply_to` has no mirror row pointing at the question, but the batch it
    // closed still names every incoming id it covered. Keeping those here is
    // what stops a compact/recovery from regenerating that batch.
    if let Ok(mut stmt) = con.prepare_cached(
        "SELECT message_ids FROM batches WHERE state='closed' AND message_ids IS NOT NULL",
    ) {
        if let Ok(rows) = stmt.query_map([], |r| r.get::<_, String>(0)) {
            for raw in rows.flatten() {
                if let Ok(ids) = serde_json::from_str::<Vec<i64>>(&raw) {
                    out.extend(ids);
                }
            }
        }
    }
    if self_id == 0 {
        return out;
    }
    let rows: Vec<Option<i64>> = match con
        .prepare_cached("SELECT reply_to FROM messages WHERE user_id=? AND reply_to IS NOT NULL")
    {
        Ok(mut stmt) => stmt
            .query_map([self_id], |r| r.get::<_, Option<i64>>(0))
            .map(|it| it.filter_map(|r| r.ok()).collect())
            .unwrap_or_default(),
        Err(_) => vec![],
    };
    out.extend(rows.into_iter().flatten());
    out
}

// ---------- delivery ledger ----------

pub fn batch_sig(text: &str) -> String {
    let mut h = Sha256::new();
    h.update(text.as_bytes());
    hex::encode(h.finalize())[..16].to_string()
}

pub fn events_path(state: &str) -> PathBuf {
    Path::new(state).join("events.jsonl")
}

pub fn read_events(state: &str, offset: u64) -> Result<(Vec<Value>, u64)> {
    let path = events_path(state);
    let data = match std::fs::read(&path) {
        Ok(d) => d,
        Err(_) => return Ok((vec![], offset)),
    };
    if (offset as usize) >= data.len() {
        return Ok((vec![], offset));
    }
    let chunk = &data[offset as usize..];
    let new_offset = data.len() as u64;
    let mut events = vec![];
    for line in chunk.split(|b| *b == b'\n') {
        let line = String::from_utf8_lossy(line).to_string();
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }
        if let Ok(ev) = serde_json::from_str::<Value>(&line) {
            if ev.is_object() {
                events.push(ev);
            }
        }
    }
    Ok((events, new_offset))
}

pub fn pending_ids(con: &Connection) -> Vec<String> {
    let raw = kv_get(con, "pending", "");
    if raw.is_empty() {
        return vec![];
    }
    serde_json::from_str::<Vec<Value>>(&raw)
        .map(|v| {
            v.iter()
                .map(|x| x.to_string().trim_matches('"').to_string())
                .collect()
        })
        .unwrap_or_default()
}

pub fn save_pending(con: &Connection, ids: &[String]) -> Result<()> {
    let tail: Vec<&String> = ids.iter().rev().take(200).rev().collect();
    kv_set(con, "pending", &serde_json::to_string(&tail)?)
}

pub fn done_ids(con: &Connection) -> HashSet<String> {
    let raw = kv_get(con, "done", "");
    if raw.is_empty() {
        return HashSet::new();
    }
    serde_json::from_str::<Vec<Value>>(&raw)
        .map(|v| {
            v.iter()
                .map(|x| x.to_string().trim_matches('"').to_string())
                .collect()
        })
        .unwrap_or_default()
}

pub fn mark_done(con: &Connection, _state: &str, sig: &str) -> Result<()> {
    let mut done = done_ids(con);
    done.insert(sig.to_string());
    let mut v: Vec<String> = done.into_iter().collect();
    v.sort();
    let tail: Vec<String> = v.into_iter().rev().take(200).rev().collect();
    kv_set(con, "done", &serde_json::to_string(&tail)?)?;
    let pending: Vec<String> = pending_ids(con).into_iter().filter(|x| x != sig).collect();
    save_pending(con, &pending)?;
    for k in [
        "pending_text",
        "pending_deliverAs",
        "pending_batch_ids",
        "pending_window",
        "pending_tome",
        "pending_owner",
    ] {
        let _ = kv_del(con, &format!("{k}:{sig}"));
    }
    Ok(())
}

/// Delivery frames are `sig#n`. `admitExternalMessage` is idempotent per
/// delivery_id, so a re-delivery MUST carry a fresh id: reusing the batch sig
/// made a batch that stalled once come back as `already_admitted`, which
/// enqueues nothing — the text never entered the context again and the bot went
/// silent while the daemon re-delivered forever (observed live 2026-11-03: 24
/// admissions, zero emissions, a session recreated every ~15 s). The ledger
/// keeps the batch sig; only the frame id differs.
static DELIVERY_FRAME_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

pub fn frame_id(sig: &str) -> String {
    format!(
        "{sig}#{}",
        DELIVERY_FRAME_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    )
}

/// Batch sig behind a frame id: `sig`, or `sig#n` from a later attempt.
pub fn sig_of_frame(frame: &str) -> &str {
    match frame.rfind('#') {
        Some(i) if i + 1 < frame.len() && frame[i + 1..].bytes().all(|b| b.is_ascii_digit()) => {
            &frame[..i]
        }
        _ => frame,
    }
}

/// True when `frame` belongs to the batch `sig` (first or any re-delivery).
pub fn frame_for(frame: &str, sig: &str) -> bool {
    sig_of_frame(frame) == sig
}

pub async fn wait_for(
    state: &str,
    mut offset: u64,
    want: &str,
    sig: &str,
    budget: Duration,
) -> Result<(Option<Value>, Vec<Value>, u64)> {
    let deadline = Instant::now() + budget;
    let mut receipts = vec![];
    while Instant::now() < deadline {
        let (events, noff) = read_events(state, offset)?;
        offset = noff;
        for ev in events {
            let kind = ev.get("kind").and_then(|v| v.as_str()).unwrap_or("");
            if (kind == "chat_send_result" || kind == "chat_send_replay")
                && ev.get("receipt").map(|r| r.is_object()).unwrap_or(false)
            {
                receipts.push(ev.get("receipt").unwrap().clone());
            }
            if kind == want
                && (want != "admitted"
                    || frame_for(
                        ev.get("delivery_id").and_then(|v| v.as_str()).unwrap_or(""),
                        sig,
                    ))
            {
                return Ok((Some(ev), receipts, offset));
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Ok((None, receipts, offset))
}

pub fn batch_rows_by_ids(con: &Connection, ids: &[Value]) -> Result<Vec<BatchMsg>> {
    let mut out = vec![];
    for mid in ids {
        let mid_int: Option<i64> = mid
            .as_i64()
            .or_else(|| mid.as_str().and_then(|s| s.parse().ok()));
        let mid_int = match mid_int {
            Some(m) => m,
            None => continue,
        };
        let row: Option<(
            Option<i64>,
            Option<i64>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<i64>,
            Option<String>,
            Option<String>,
            Option<String>,
        )> = con
            .prepare_cached(
                "SELECT message_id,user_id,username,first_name,text,reply_to,media_path,media_kind,file_id FROM messages WHERE message_id=? LIMIT 1",
            )
            .and_then(|mut stmt| {
                stmt.query_row([mid_int], |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                        r.get(7)?,
                        r.get(8)?,
                    ))
                })
            })
            .ok();
        if let Some((m, u, un, f, t, r, mp, kind, fid)) = row {
            let username = un.unwrap_or_default();
            out.push(BatchMsg {
                message_id: m.unwrap_or(mid_int),
                user_id: u,
                username,
                first_name: f.unwrap_or_default(),
                text: t.unwrap_or_default(),
                reply_to: r,
                media_path: mp.unwrap_or_default(),
                media_kind: kind.unwrap_or_default(),
                file_id: fid.unwrap_or_default(),
                to_me: false,
            });
        }
    }
    Ok(out)
}

pub fn regen_text(state: &str, con: &Connection, sig: &str) -> Result<Option<String>> {
    let ids = crate::ledger::message_ids(con, sig)?;
    if ids.is_empty() {
        return Ok(None);
    }
    let ids: Vec<Value> = ids.into_iter().map(|i| json!(i)).collect();
    let mut rows = batch_rows_by_ids(con, &ids)?;
    if rows.is_empty() {
        return Ok(None);
    }
    if let Some(view) = crate::ledger::view(con, sig)? {
        // A closed batch is terminal, with or without a quote. Regenerating it
        // would let a `compacted` event push the same question again.
        if view.state == "closed" || view.close_reason().is_some() {
            return Ok(None);
        }
        let tome: HashSet<i64> = view.tome.into_iter().collect();
        for r in rows.iter_mut() {
            if tome.contains(&r.message_id) {
                r.to_me = true;
            }
        }
    }
    // The rebuild goes through the SAME builder as a fresh batch: the only
    // difference is that the rows arrive from the journal instead of ingest.
    let built = crate::context::build(state, con, &rows)?;
    if built.ids.is_empty() {
        dlog(&format!("compacted: {sig} has no unanswered rows left"));
        return Ok(None);
    }
    Ok(Some(built.text))
}

/// Agent-cycle liveness from extension activity events. Some(true) = the
/// agent is working right now (thoughts/actions in flight): a working
/// session must never be murdered. Some(false) = stopped. None = not an
/// activity event.
pub fn activity_phase(ev: &Value) -> Option<bool> {
    if ev.get("kind").and_then(|v| v.as_str()) != Some("agent_activity") {
        return None;
    }
    match ev.get("phase").and_then(|v| v.as_str()) {
        Some("working") => Some(true),
        Some("stopped") => Some(false),
        _ => None,
    }
}

/// Incoming message ids of a pending batch, as written to the ledger at admit
/// time. The extension needs them to refuse a `reply_to` that names anything
/// outside this batch (history window, GIF catalog, another turn).
pub fn pending_batch_ids(con: &Connection, sig: &str) -> Vec<i64> {
    let raw = kv_get(con, &format!("pending_batch_ids:{sig}"), "");
    if raw.is_empty() {
        return vec![];
    }
    serde_json::from_str::<Vec<Value>>(&raw)
        .unwrap_or_default()
        .iter()
        .filter_map(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .collect()
}

/// Per-batch delivery marks, persisted so a restart re-evaluates the same
/// rules instead of re-delivering (or retiring) by accident.
pub fn pending_mark_get(con: &Connection, sig: &str, name: &str) -> Option<i64> {
    let raw = kv_get(con, &format!("pending_{name}:{sig}"), "");
    if raw.is_empty() {
        return None;
    }
    raw.parse::<i64>().ok()
}

pub fn pending_mark_set(con: &Connection, sig: &str, name: &str, at: i64) -> Result<()> {
    kv_set(con, &format!("pending_{name}:{sig}"), &at.to_string())
}

pub fn pending_marks_clear(con: &Connection, sig: &str) -> Result<()> {
    for name in ["admitted_at", "emitted_at", "settled_at"] {
        kv_set(con, &format!("pending_{name}:{sig}"), "")?;
    }
    Ok(())
}

/// Delivery phase of a pending batch. `inboxed` = pushed, not accepted yet;
/// `admitted` = the runtime took it; `emitted` = the context entry exists (the
/// agent has the text); `settled` = a run finished after that emission.
pub fn delivery_phase(
    admitted_at: Option<i64>,
    emitted_at: Option<i64>,
    settled_at: Option<i64>,
) -> &'static str {
    match (admitted_at, emitted_at, settled_at) {
        (None, _, _) => "inboxed",
        (Some(_), None, _) => "admitted",
        (Some(_), Some(_), None) => "emitted",
        (Some(_), Some(_), Some(_)) => "settled",
    }
}

/// Whether a pending batch may be retired, and why. Retiring is only ever by
/// the agent's own answer, plus one explicit exception: the run settled after
/// the delivery was emitted, so the batch was in context and the agent chose
/// silence (`[skip]`). Elapsed time and bare turn boundaries never retire.
pub fn should_retire(
    all_answered: bool,
    emitted_at: Option<i64>,
    settled_at: Option<i64>,
) -> Option<&'static str> {
    if all_answered {
        return Some("answered");
    }
    if let (Some(e), Some(s)) = (emitted_at, settled_at) {
        if s >= e {
            return Some("settled");
        }
    }
    None
}

/// No-progress clock: a batch has shown no new evidence for `budget_secs`
/// (extended while the agent is working, hard-capped by the same `extra_secs`).
pub fn stalled(
    now: i64,
    last_progress: i64,
    budget_secs: u64,
    working: bool,
    extra_secs: u64,
) -> bool {
    let limit = budget_secs as i64 + if working { extra_secs as i64 } else { 0 };
    now.saturating_sub(last_progress) >= limit.saturating_mul(1000)
}

pub fn mirror_receipts(con: &Connection, receipts: &[Value]) -> Result<()> {
    for r in receipts {
        let mid = match r.get("message_id").and_then(|v| v.as_i64()) {
            Some(m) => m,
            None => continue,
        };
        let reply_to = r.get("reply_to").and_then(|v| v.as_i64());
        let text = r.get("text").and_then(|v| v.as_str()).unwrap_or("");
        mirror_own_send(con, mid, text, reply_to)?;
    }
    Ok(())
}

pub async fn settle_session(
    state: &str,
    db: &crate::db::Db,
    offset: u64,
    budget: Duration,
) -> Result<u64> {
    let (_ev, receipts, offset) = wait_for(state, offset, "turn_end", "", budget).await?;
    if !receipts.is_empty() {
        let receipts = receipts.clone();
        db.call(move |con| mirror_receipts(con, &receipts)).await?;
    }
    Ok(offset)
}

pub fn build_batch_text(
    state: &str,
    con: &Connection,
    batch: &[BatchMsg],
) -> Result<(String, String, Vec<i64>, Vec<i64>, String)> {
    let built = crate::context::build(state, con, batch)?;
    Ok((
        built.text,
        built.sig,
        built.ids,
        built.tome,
        built.anchor.to_string(),
    ))
}

/// Summarize isolation: open a session on a SEPARATE host socket whose
/// launch-spec omits chat-ext.mjs, assert the tool list has no chat_send.
/// True when any string value in the JSON tree is exactly the tool name.
/// (Substring matching false-positives on prose: system-prompt.md itself
/// mentions `chat_send` in plain text, and `state` may echo prompt text.)
fn json_has_tool_name(v: &Value, tool: &str) -> bool {
    match v {
        Value::String(s) => s == tool,
        Value::Array(a) => a.iter().any(|x| json_has_tool_name(x, tool)),
        Value::Object(o) => o.values().any(|x| json_has_tool_name(x, tool)),
        _ => false,
    }
}

/// Best-effort teardown of a scratch summary host: close the session on it
/// (it never touches the chat host) and unlink the socket file.
/// Runs on every summarize exit path so repeated runs leave nothing behind.
pub async fn close_summary_session(sid: &str, sock: &str) {
    let _ = omo_on(&["close", sid], Duration::from_secs(30), sock).await;
    let _ = std::fs::remove_file(sock);
}

pub async fn summarize_isolated_session(system_path: &str) -> Result<(String, String)> {
    // Use a scratch socket so we never touch the chat host. The socket is
    // threaded explicitly through every call — process-global routing
    // (CHATBOT_HOST_SOCKET) is never mutated, so later omo() calls in this
    // process still follow the chat host.
    let tmp = std::env::temp_dir().join(format!("chat-summary-host-{}.sock", std::process::id()));
    let sock = tmp.to_string_lossy().to_string();
    let state = state_dir();
    ensure_host_on(&state, false, &sock).await?;
    let r = cfg("CHATBOT_SUMMARY_SESSION", "chat-summary");
    // List sessions on the scratch host to close strays best-effort; each
    // close names a durable id, never a session name.
    let _ = session::close_all_sessions_on(&sock).await;
    let out = omo_on(
        &[
            "open",
            "--cwd",
            &cfg("CHATBOT_CWD", "."),
            "--name",
            &r,
            "--prompt",
            &format!("@{system_path}"),
        ],
        Duration::from_secs(120),
        &sock,
    )
    .await;
    let out = match out {
        Ok(o) => o,
        Err(e) => {
            let _ = std::fs::remove_file(&sock);
            return Err(e);
        }
    };
    let sid = out
        .get("durableSessionId")
        .and_then(|v| v.as_str())
        .unwrap_or(&r)
        .to_string();
    // Verify no chat_send tool. The CLI offers no `tools` subcommand;
    // `state` is the introspection it has. Structural isolation already
    // holds via the extension-free launch-spec; this fails closed if a
    // chat_send tool entry ever shows up in the fresh session state.
    let st = omo_on(&["state", &sid], Duration::from_secs(30), &sock)
        .await
        .unwrap_or(json!({}));
    if json_has_tool_name(&st, "chat_send") {
        close_summary_session(&sid, &sock).await;
        return Err(anyhow!(
            "summary session exposes chat_send: isolation violated"
        ));
    }
    Ok((sid, sock))
}

// hex helper (avoid extra crate): sha256 hex encode.
mod hex {
    pub fn encode(bytes: impl AsRef<[u8]>) -> String {
        bytes.as_ref().iter().map(|b| format!("{b:02x}")).collect()
    }
}

/// Latest timestamp of an event kind in the journal. Used once at boot to
/// repair a settle that was never stamped because the delivery proof arrived
/// after the run had already settled.
pub fn last_event_ts(state: &str, kind: &str) -> Option<i64> {
    let (events, _) = read_events(state, 0).ok()?;
    events
        .iter()
        .filter(|e| e.get("kind").and_then(|v| v.as_str()) == Some(kind))
        .filter_map(|e| e.get("t").and_then(|v| v.as_i64()))
        .max()
}

/// Path of the control socket the daemon hosts and the extension connects to.
/// Derived from the state dir so the extension needs no extra environment: the
/// host passes only launch-spec keys, and the state dir is already one of them.
pub fn control_sock_path(state: &str) -> PathBuf {
    Path::new(state).join("control.sock")
}

/// Read-only MCP tool socket served by the daemon. It is separate from the
/// control socket because the extension owns that one connection; a tool call
/// must not evict it. Only the daemon can answer here, so the Bot API token and
/// every HTTP call stay inside this process.
pub fn tool_socket_path(state: &str) -> PathBuf {
    Path::new(state).join("tool.sock")
}

/// One daemon-owned tool operation. `get_media` is the only op that needs the
/// Bot API: the tool receives the cached path and byte count, never the token.
pub async fn tool_call(state: &str, op: &str, args: &Value) -> Result<String> {
    match op {
        "get_media" => {
            let file_id = args
                .get("file_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim();
            if file_id.is_empty() {
                return Err(anyhow!("file_id is required"));
            }
            let path = download_media(file_id, state, "").await?;
            let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            Ok(json!({"path": path, "bytes": bytes}).to_string())
        }
        other => Err(anyhow!("unknown tool op: {other}")),
    }
}

/// A socket file left behind by a previous daemon makes `bind` fail with
/// EADDRINUSE. Nothing else owns that path, so removing it before bind is safe.
pub fn unlink_stale_socket(path: &Path) {
    use std::os::unix::fs::FileTypeExt;
    if let Ok(meta) = std::fs::symlink_metadata(path) {
        if meta.file_type().is_socket() {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Text to re-push for a pending batch after a compaction, or after the session
/// file refused its entry. Regenerated from the mirror (answered rows dropped,
/// fully answered batches close themselves); `None` means nothing to send.
pub fn push_pending(state: &str, con: &Connection, sig: &str) -> Result<Option<String>> {
    regen_text(state, con, sig)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivery_phase_matrix() {
        assert_eq!(delivery_phase(None, None, None), "inboxed");
        assert_eq!(delivery_phase(Some(1), None, None), "admitted");
        assert_eq!(delivery_phase(Some(1), Some(2), None), "emitted");
        assert_eq!(delivery_phase(Some(1), Some(2), Some(3)), "settled");
    }

    #[test]
    fn should_retire_matrix() {
        // Only an answer, or a settled run after an emission, retires a batch.
        assert_eq!(should_retire(true, None, None), Some("answered"));
        assert_eq!(should_retire(true, Some(1), Some(2)), Some("answered"));
        assert_eq!(should_retire(false, Some(1), Some(2)), Some("settled"));
        assert_eq!(should_retire(false, Some(2), Some(1)), None);
        assert_eq!(should_retire(false, Some(1), None), None);
        assert_eq!(should_retire(false, None, None), None);
    }

    #[test]
    fn stalled_matrix() {
        // 10s of silence against a 5s budget: stalled, unless the agent is
        // working and the extra allowance still covers the gap.
        assert!(stalled(10_000, 0, 5, false, 60));
        assert!(!stalled(10_000, 0, 5, true, 60));
        assert!(stalled(70_000, 0, 5, true, 60));
        assert!(!stalled(4_999, 0, 5, false, 0));
    }

    #[test]
    fn sig_stable_vs_python_hashlib() {
        // sha256("hello")[:16] — must match Python hashlib.
        assert_eq!(&batch_sig("hello"), &{
            use sha2::Digest;
            let mut h = Sha256::new();
            h.update(b"hello");
            format!("{:x}", h.finalize())[..16].to_string()
        });
        assert_eq!(batch_sig("hello").len(), 16);
    }

    #[test]
    fn format_batch_shape() {
        let b = vec![BatchMsg {
            message_id: 1,
            user_id: Some(2),
            username: "u".into(),
            first_name: "F".into(),
            text: "hi".into(),
            reply_to: None,
            media_path: "".into(),
            media_kind: "".into(),
            file_id: "".into(),
            to_me: true,
        }];
        let s = crate::context::render_batch(&b, &HashMap::new());
        assert!(s.contains("[1] @u: [к тебе] hi"));
    }

    #[test]
    fn format_batch_renders_reply_parent_text_and_cap() {
        let mut m = mk_batch_msg(102, "мой ответ");
        m.reply_to = Some(101);
        let long = "длинный родитель ".repeat(30);
        let mut parents = HashMap::new();
        parents.insert(
            101,
            crate::context::ReplyParent {
                who: "@u".into(),
                text: long,
                media: false,
            },
        );
        let s = crate::context::render_batch(&[m], &parents);
        assert!(
            s.contains("(ответ на [101] @u: «длинный родитель"),
            "got: {s}"
        );
        assert!(s.contains('…'), "parent text was not capped: {s}");
        let body = s.split('«').nth(1).unwrap().split('»').next().unwrap();
        assert!(
            body.chars().count() <= crate::context::REPLY_PARENT_CAP + 1,
            "cap exceeded: {} chars",
            body.chars().count()
        );
    }

    #[test]
    fn format_batch_renders_media_missing_parent_and_header() {
        assert!(crate::context::render_batch(&[], &HashMap::new()).is_empty());
        let mut m = mk_batch_msg(102, "x");
        m.reply_to = Some(101);
        let missing = crate::context::render_batch(&[m.clone()], &HashMap::new());
        assert!(
            missing.contains("(ответ на [101]: сообщения нет в базе)"),
            "got: {missing}"
        );
        let mut parents = HashMap::new();
        parents.insert(
            101,
            crate::context::ReplyParent {
                who: "@u".into(),
                text: "".into(),
                media: true,
            },
        );
        assert!(
            crate::context::render_batch(&[m], &parents).contains("(ответ на [101] @u: [file])")
        );
        let header = crate::context::render_batch(&[mk_batch_msg(1, "hi")], &HashMap::new());
        assert!(
            header.contains("reply_to — только id из этой пачки"),
            "batch header missing: {header}"
        );
    }

    #[test]
    fn mirror_roundtrip() {
        let dir = std::env::temp_dir().join(format!("chat-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let state = dir.to_string_lossy().to_string();
        let con = db(&state).unwrap();
        kv_set(&con, "self_id", "42").unwrap();
        kv_set(&con, "self_username", "bot").unwrap();
        kv_set(&con, "self_first", "Bot").unwrap();
        mirror_own_send(&con, 7, "hello", None).unwrap();
        mirror_own_send(&con, 7, "hello", None).unwrap(); // INSERT OR IGNORE
        let n: i64 = con
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE message_id=7",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn truncate_cyrillic_boundary() {
        // Cyrillic tail/head cuts must not panic mid-char.
        let s = "ошибка ответ".to_string();
        let t = truncate_tail(&s, 70);
        assert!(s.ends_with(&t));
        let h = truncate_head(&s, 5);
        assert!(s.starts_with(&h));
        assert!(h.chars().count() <= s.chars().count());
        // Exact-boundary stress: every cut length stays valid UTF-8.
        for n in 0..s.len() {
            let _ = truncate_head(&s, n);
            let _ = truncate_tail(&s, n);
        }
    }

    #[test]
    fn tool_name_walk_ignores_prose() {
        // Prose mentioning chat_send (as system-prompt.md does) is not a tool.
        let prose = json!({"prompt": "ответы в чат идут только через тул chat_send"});
        assert!(!json_has_tool_name(&prose, "chat_send"));
        let tools = json!({"tools": [{"name": "chat_send"}, {"name": "history"}]});
        assert!(json_has_tool_name(&tools, "chat_send"));
        assert!(!json_has_tool_name(&tools, "missing"));
    }

    #[test]
    fn host_alive_detects_missing_socket() {
        let dir = std::env::temp_dir().join(format!("chat-ha-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("CHATBOT_HOST_SOCKET", dir.join("rpc.sock"));
        assert!(!host_alive());
        std::fs::write(dir.join("rpc.sock"), b"x").unwrap();
        assert!(!host_alive()); // stale file, nobody listening
        let _ = std::fs::remove_file(dir.join("rpc.sock"));
        let listener = std::os::unix::net::UnixListener::bind(dir.join("rpc.sock")).unwrap();
        assert!(host_alive());
        drop(listener);
        std::env::remove_var("CHATBOT_HOST_SOCKET");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn dlog_appends_to_state_log() {
        let dir = std::env::temp_dir().join(format!("chat-dl-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::env::set_var("CHATBOT_STATE_DIR", &dir);
        dlog("probe-line-123");
        let body = std::fs::read_to_string(dir.join("chatd.log")).unwrap();
        assert!(body.contains("probe-line-123"));
        std::env::remove_var("CHATBOT_STATE_DIR");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn torn_line_reader() {
        let dir = std::env::temp_dir().join(format!("chat-ev-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let state = dir.to_string_lossy().to_string();
        std::fs::create_dir_all(&state).unwrap();
        std::fs::write(events_path(&state), b"{\"kind\":\"turn_end\"}\n{\"kind\":").unwrap();
        let (evs, _) = read_events(&state, 0).unwrap();
        assert_eq!(evs.len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn mk_batch_msg(id: i64, text: &str) -> BatchMsg {
        BatchMsg {
            message_id: id,
            user_id: Some(7),
            username: "u".into(),
            first_name: "U".into(),
            text: text.into(),
            reply_to: None,
            media_path: "".into(),
            media_kind: "".into(),
            file_id: "".into(),
            to_me: true,
        }
    }

    #[test]
    fn media_kind_markers_and_sticker_file_id() {
        let mut photo = mk_batch_msg(1, "подпись [photo]");
        photo.media_kind = "photo".into();
        photo.media_path = "/s/media/p.jpg".into();
        let rendered = crate::context::render_batch(&[photo], &HashMap::new());
        assert!(rendered.contains("[photo]"), "got: {rendered}");
        assert!(
            rendered.contains("[file] /s/media/p.jpg"),
            "got: {rendered}"
        );

        let mut gif = mk_batch_msg(2, "[gif]");
        gif.media_kind = "animation".into();
        gif.media_path = "/s/media/a_frame.jpg".into();
        assert!(crate::context::render_batch(&[gif], &HashMap::new()).contains("[gif]"));

        let mut video = mk_batch_msg(3, "[video]");
        video.media_kind = "video".into();
        video.media_path = "/s/media/v_frame.jpg".into();
        assert!(crate::context::render_batch(&[video], &HashMap::new()).contains("[video]"));

        // A sticker has no caption and no media file: its file_id must be in
        // the rendered line, otherwise the model cannot re-send it.
        let mut sticker = mk_batch_msg(4, "[sticker]");
        sticker.media_kind = "sticker".into();
        sticker.file_id = "sticker_fid_1".into();
        let s = crate::context::render_batch(&[sticker], &HashMap::new());
        assert!(
            s.contains("[sticker]") && s.contains("file_id=sticker_fid_1"),
            "got: {s}"
        );
    }

    #[test]
    fn pick_media_detects_every_kind() {
        assert_eq!(pick_media(&json!({"text": "hi"})), None);
        for (key, value) in [
            ("photo", json!([{"file_id": "p1", "file_size": 1}])),
            ("animation", json!({"file_id": "a1"})),
            ("video", json!({"file_id": "v1"})),
            ("document", json!({"file_id": "d1"})),
            ("sticker", json!({"file_id": "s1"})),
            ("voice", json!({"file_id": "o1"})),
            ("audio", json!({"file_id": "m1"})),
            ("video_note", json!({"file_id": "n1"})),
        ] {
            let msg = json!({ key: value });
            let (kind, obj) = pick_media(&msg).unwrap_or_else(|| panic!("{key} not picked"));
            assert_eq!(kind, key);
            assert!(obj.get("file_id").is_some(), "{key} lost its file_id");
        }
        // A photo arrives as an array: the last (largest) size wins.
        let (_, biggest) =
            pick_media(&json!({"photo": [{"file_id": "small"}, {"file_id": "big"}]})).unwrap();
        assert_eq!(biggest.get("file_id").and_then(|v| v.as_str()), Some("big"));
    }

    fn seed_qa(state: &str) -> Connection {
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let con = db(state).unwrap();
        kv_set(&con, "self_id", "42").unwrap();
        kv_set(&con, "self_username", "bot").unwrap();
        kv_set(&con, "self_first", "Bot").unwrap();
        con.execute(
            "INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id) VALUES (1,100,1000,7,'u','U','старый вопрос',NULL,'','')",
            [],
        )
        .unwrap();
        mirror_own_send(&con, 200, "старый ответ", Some(100)).unwrap();
        con.execute(
            "INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id) VALUES (2,101,1001,7,'u','U','новый вопрос',NULL,'','')",
            [],
        )
        .unwrap();
        con
    }

    #[test]
    fn answered_ids_unions_mirror_and_recent_receipts() {
        let dir = std::env::temp_dir().join(format!("chat-au-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let state = dir.to_string_lossy().to_string();
        std::fs::create_dir_all(&state).unwrap();
        let con = seed_qa(&state);
        // 101 answered via receipt only (mirror lag simulation: no own row).
        std::fs::write(
            events_path(&state),
            "{\"kind\":\"chat_send_result\",\"receipt\":{\"message_id\":201,\"reply_to\":101,\"text\":\"a2\"}}\n",
        )
        .unwrap();
        let a = answered_ids(&state, &con);
        assert!(a.contains(&100), "mirror answer missing");
        assert!(a.contains(&101), "receipt answer missing");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn build_batch_filters_answered_rows_and_marks_context() {
        let dir = std::env::temp_dir().join(format!("chat-bf-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let state = dir.to_string_lossy().to_string();
        let con = seed_qa(&state);
        let batch = vec![
            mk_batch_msg(100, "старый вопрос"),
            mk_batch_msg(101, "новый вопрос"),
        ];
        let (text, _sig, ids, _tome, _anchor) = build_batch_text(&state, &con, &batch).unwrap();
        assert_eq!(ids, vec![101]);
        assert!(
            text.contains("[100]") && text.contains("уже отвечено"),
            "got: {text}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn regen_drops_a_fully_answered_batch() {
        let dir = std::env::temp_dir().join(format!("chat-rc-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let state = dir.to_string_lossy().to_string();
        let con = seed_qa(&state);
        // The batch identity now lives in the journal; a fully answered set is
        // dropped from the regenerated text instead of being re-asked.
        con.execute(
            "INSERT INTO batches(batch_key,chat_id,state,sealed_at,updated_at,message_ids) VALUES('s1',0,'queued',0,0,'[100]')",
            [],
        )
        .unwrap();
        let regen = regen_text(&state, &con, "s1").unwrap();
        assert!(regen.is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn activity_phase_matrix() {
        let w = json!({"kind": "agent_activity", "phase": "working"});
        assert_eq!(activity_phase(&w), Some(true));
        let wt = json!({"kind": "agent_activity", "phase": "working", "tool": "chat_send"});
        assert_eq!(activity_phase(&wt), Some(true));
        let s = json!({"kind": "agent_activity", "phase": "stopped"});
        assert_eq!(activity_phase(&s), Some(false));
        let o = json!({"kind": "agent_activity", "phase": "other"});
        assert_eq!(activity_phase(&o), None);
        let t = json!({"kind": "turn_end", "turnIndex": 1});
        assert_eq!(activity_phase(&t), None);
        let m = json!({"kind": "agent_activity"});
        assert_eq!(activity_phase(&m), None);
    }

    #[test]
    fn delivery_frames_are_unique_and_map_back_to_the_batch() {
        let a = frame_id("abc123");
        let b = frame_id("abc123");
        assert_ne!(a, b, "a re-delivery must carry a fresh frame id");
        assert!(frame_for(&a, "abc123"));
        assert!(frame_for(&b, "abc123"));
        assert!(frame_for("abc123", "abc123"), "a plain sig still matches");
        assert!(
            !frame_for(&a, "abc12"),
            "a prefix of the sig is not the batch"
        );
        assert!(
            !frame_for("abc123#x", "abc123"),
            "a non-numeric suffix is no frame"
        );
        assert_eq!(sig_of_frame(&a), "abc123");
        assert_eq!(sig_of_frame("abc123"), "abc123");
        assert_eq!(sig_of_frame("abc#7#2"), "abc#7");
    }

    /// The lock is exclusive for the process lifetime: a second open+flock on
    /// the same state file is refused with a message naming the state, and the
    /// lock is released when the first handle drops.
    #[test]
    fn second_state_lock_is_refused_with_clear_message() {
        let dir = std::env::temp_dir().join(format!("chat-lock-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let state = dir.to_string_lossy().to_string();
        let first = acquire_state_lock(&state).unwrap();
        let err = acquire_state_lock(&state).unwrap_err();
        assert!(
            err.to_string().contains("another chatd instance"),
            "unclear second-instance error: {err}"
        );
        drop(first);
        assert!(acquire_state_lock(&state).is_ok(), "lock was not released");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---------- data layer cycle 1 ----------

    fn envelope(chat: i64, message: i64, update_id: i64, text: &str) -> Envelope {
        Envelope {
            key: MessageKey {
                chat,
                message: MessageId(message),
            },
            update_id,
            date: 1_700_000_000 + update_id,
            user_id: Some(7),
            username: "u".into(),
            first_name: "U".into(),
            text: text.into(),
            reply_to: None,
            thread_id: None,
            media_path: String::new(),
            file_id: String::new(),
            file_unique_id: String::new(),
            media_kind: String::new(),
            media_state: MediaState::Absent,
            media_size: None,
            source_chat: None,
            sender_chat: None,
            cross: false,
        }
    }

    fn temp_db(tag: &str) -> (String, Connection) {
        let dir = std::env::temp_dir().join(format!("chat-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let state = dir.to_string_lossy().to_string();
        let con = db(&state).unwrap();
        (state, con)
    }

    /// The identity is (chat_id, message_id): a redelivery under a NEW
    /// update_id is a duplicate, not a second row, and the cursor still moves.
    #[test]
    fn commit_page_redelivery_with_new_update_id_is_duplicate() {
        let (state, mut con) = temp_db("cp-conflict");
        kv_set(&con, "offset", "10").unwrap();
        {
            let mut tx = crate::db::WriteTxn::new(&mut con).unwrap();
            let page = tx
                .commit_page(
                    vec![
                        envelope(-100, 100, 11, "first"),
                        envelope(-100, 101, 12, "second"),
                    ],
                    12,
                )
                .unwrap();
            assert_eq!(page.applied.inserted, 2);
            tx.commit().unwrap();
        }
        assert_eq!(kv_get(&con, "offset", "0"), "12");

        let page = {
            let mut tx = crate::db::WriteTxn::new(&mut con).unwrap();
            let page = tx
                .commit_page(
                    vec![
                        envelope(-100, 101, 13, "duplicate key"),
                        envelope(-100, 102, 14, "new row"),
                    ],
                    14,
                )
                .unwrap();
            tx.commit().unwrap();
            page
        };
        assert_eq!(page.applied.inserted, 1);
        assert_eq!(page.applied.duplicate, 1);
        assert_eq!(kv_get(&con, "offset", "0"), "14");
        let rows: i64 = con
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE message_id IN (101,102)",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(rows, 2, "the redelivered key must not create a second row");
        let _ = std::fs::remove_dir_all(&state);
    }

    /// A genuine INSERT failure (not a UNIQUE conflict) also leaves the cursor
    /// and every row of the page untouched.
    #[test]
    fn commit_page_insert_error_keeps_offset() {
        let (state, mut con) = temp_db("cp-error");
        kv_set(&con, "offset", "5").unwrap();
        con.execute_batch(
            "CREATE TRIGGER force_insert_failure BEFORE INSERT ON messages
             WHEN NEW.message_id = 999
             BEGIN SELECT RAISE(ABORT, 'forced insert failure'); END;",
        )
        .unwrap();
        let err = {
            let mut tx = crate::db::WriteTxn::new(&mut con).unwrap();
            tx.commit_page(
                vec![
                    envelope(-100, 998, 6, "would be written"),
                    envelope(-100, 999, 7, "forced to fail"),
                ],
                7,
            )
            .unwrap_err()
        };
        assert!(
            format!("{err:#}").contains("forced insert failure"),
            "unexpected error: {err:#}"
        );
        assert_eq!(kv_get(&con, "offset", "0"), "5");
        let rows: i64 = con
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE message_id IN (998,999)",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(rows, 0, "partial page survived the failed INSERT");
        let _ = std::fs::remove_dir_all(&state);
    }

    /// v2 backfills the configured chat id; a fabricated id-space collision
    /// rolls the whole step back and reports the number of conflicting rows.
    #[test]
    fn migration_v2_backfills_and_rolls_back_on_collision() {
        let dir = std::env::temp_dir().join(format!("chat-mig2-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let legacy = |name: &str| -> Connection {
            let con = Connection::open(dir.join(name)).unwrap();
            con.execute_batch(
                "CREATE TABLE messages(
                   id INTEGER PRIMARY KEY,
                   chat_id INTEGER,
                   message_id INTEGER,
                   text TEXT
                 );
                 CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT);",
            )
            .unwrap();
            con.pragma_update(None, "user_version", 1).unwrap();
            con
        };

        // Happy path: every old row gets the configured chat id.
        let mut con = legacy("ok.db");
        con.execute_batch(
            "INSERT INTO messages(chat_id,message_id,text) VALUES
               (NULL,100,'a'),(NULL,101,'b');
             INSERT INTO kv(k,v) VALUES('chat_id','-100');",
        )
        .unwrap();
        migrate_v2(&mut con).unwrap();
        let filled: i64 = con
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE chat_id=-100 AND message_id IN (100,101)",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(filled, 2);
        let v: i64 = con
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, 2);
        drop(con);

        // Collision: two rows claim the same message_id. Nothing changes and
        // the error states how many rows are involved.
        let mut con = legacy("collision.db");
        con.execute_batch(
            "INSERT INTO messages(chat_id,message_id,text) VALUES
               (NULL,200,'a'),(NULL,200,'b'),(NULL,201,'c');
             INSERT INTO kv(k,v) VALUES('chat_id','-100');",
        )
        .unwrap();
        let err = migrate_v2(&mut con).unwrap_err();
        assert!(
            err.to_string().contains("2 rows"),
            "error must state the conflicting row count: {err}"
        );
        let still_null: i64 = con
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE chat_id IS NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(still_null, 3, "v2 changed rows despite rolling back");
        let v: i64 = con
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, 1, "v2 advanced user_version although it failed");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Full v1..v6 on a legacy file: surrogate id, backfilled chat id, FTS on
    /// the new rowid, two-phase media columns, no UNIQUE on update_id, and a
    /// second run is a no-op.
    #[test]
    fn migrations_v1_to_v6_are_idempotent() {
        let dir = std::env::temp_dir().join(format!("chat-mig-full-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut con = Connection::open(dir.join("history.db")).unwrap();
        con.execute_batch(SCHEMA).unwrap();
        con.execute(
            "INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id)
             VALUES(1,77,1000,7,'u','U','привет мир',NULL,'','')",
            [],
        )
        .unwrap();
        con.execute_batch("INSERT INTO kv(k,v) VALUES('chat_id','-100');")
            .unwrap();
        migrate(&mut con).unwrap();
        migrate(&mut con).unwrap();
        let v: i64 = con
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(v, 6);
        let (id, chat): (i64, i64) = con
            .query_row(
                "SELECT id,chat_id FROM messages WHERE update_id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(chat, -100);
        let found: i64 = con
            .query_row(
                "SELECT COUNT(*) FROM messages_fts WHERE messages_fts MATCH 'привет'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(found, 1, "FTS lost the migrated row");
        con.execute("UPDATE messages SET text='прощай мир' WHERE id=?", [id])
            .unwrap();
        let old: i64 = con
            .query_row(
                "SELECT COUNT(*) FROM messages_fts WHERE messages_fts MATCH 'привет'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let new: i64 = con
            .query_row(
                "SELECT COUNT(*) FROM messages_fts WHERE messages_fts MATCH 'прощай'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!((old, new), (0, 1), "update trigger left a stale FTS row");
        con.execute("DELETE FROM messages WHERE id=?", [id])
            .unwrap();
        let gone: i64 = con
            .query_row(
                "SELECT COUNT(*) FROM messages_fts WHERE messages_fts MATCH 'прощай'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(gone, 0, "delete trigger left an FTS row");
        // update_id is a cursor, not an identity: the same value may appear
        // on two different messages without a UNIQUE conflict.
        con.execute(
            "INSERT INTO messages(chat_id,update_id,message_id,date,text) VALUES(-100,1,78,1001,'second')",
            [],
        )
        .unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
