//! chat-bot shared core: port of chatd.py (run/sync/summarize helpers) +
//! tg-chat helpers. Single-threaded async: all awaits run on a
//! `current_thread` tokio runtime (see bins); sqlite work is inline on the
//! same thread; typing indicator is a same-thread spawned task.

use anyhow::{anyhow, Context, Result};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// The database actor: one connection, one owning thread, FIFO queue. The
/// blocking sqlite work of the daemon runs there instead of on the
/// `current_thread` runtime that also polls Telegram, admits batches and
/// writes chat pushes.
pub mod db;

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
  file_unique_id TEXT
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

/// Warn once, in the daemon log, when nobody can authorise subagent dispatch.
/// Called from the ingest path because that is the first point at which the
/// setting matters; keeps an empty owner a loud misconfiguration instead of a
/// bot that mysteriously refuses to delegate.
fn warn_if_owner_unset() {
    use std::sync::atomic::{AtomicBool, Ordering};
    static WARNED: AtomicBool = AtomicBool::new(false);
    if WARNED.swap(true, Ordering::Relaxed) {
        return;
    }
    if cfg("CHATBOT_OWNER_USERNAME", "").is_empty() && cfg("CHATBOT_OWNER_ID", "0") == "0" {
        dlog(
            "warning: CHATBOT_OWNER_USERNAME/CHATBOT_OWNER_ID are not set; subagent dispatch \
             stays locked for everybody. Set your own Telegram username in the environment file.",
        );
    }
}

pub fn token() -> Result<String> {
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

pub fn api_base() -> String {
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

/// GET file download (Bot API file path -> local media dir).
pub async fn download_media(file_id: &str, state: &str, prefix: &str) -> Result<String> {
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

fn video_ext(path: &str) -> bool {
    matches!(
        Path::new(path)
            .extension()
            .map(|e| e.to_string_lossy().to_lowercase())
            .unwrap_or_default()
            .as_str(),
        "mp4" | "mov" | "webm" | "mkv"
    )
}

fn mime_is_video(mime: &str) -> bool {
    let m = mime.to_lowercase();
    m.starts_with("video/") || m == "image/gif" || m == "application/x-tgsticker"
}

fn mime_is_audio(mime: &str) -> bool {
    let m = mime.to_lowercase();
    m.starts_with("audio/") || m == "application/ogg" || m == "application/x-ogg"
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
            .args(["-y", "-v", "error", "-ss", "0", "-i", src, "-vframes", "1", "-q:v", "4", &dst])
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
         CREATE INDEX IF NOT EXISTS idx_messages_gifs ON messages(date DESC, message_id DESC, file_unique_id, text, file_id) WHERE file_id IS NOT NULL AND file_id <> '';",
    )?;
    Ok(())
}

pub fn db(state: &str) -> Result<Connection> {
    std::fs::create_dir_all(state)?;
    let con = Connection::open(Path::new(state).join("history.db"))?;
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
    con.execute_batch(SCHEMA)?;
    // Column migrations (idempotent).
    let _ = con.execute("ALTER TABLE messages ADD COLUMN file_id TEXT", []);
    // Historic rows have no file_unique_id (Bot API cannot derive one from a
    // file_id); the gif registry falls back to file_id for those.
    let _ = con.execute("ALTER TABLE messages ADD COLUMN file_unique_id TEXT", []);
    // A missing index costs latency, not correctness: log instead of refusing to
    // start when the file is momentarily write-locked.
    if let Err(e) = ensure_indexes(&con) {
        dlog(&format!("ensure_indexes failed: {e:#}"));
    }
    Ok(con)
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

#[derive(Debug, Clone)]
pub struct BatchMsg {
    pub message_id: i64,
    pub user_id: Option<i64>,
    pub username: String,
    pub first_name: String,
    pub text: String,
    pub reply_to: Option<i64>,
    pub media_path: String,
    pub to_me: bool,
    /// Author is the owner (`CHATBOT_OWNER_USERNAME` / `CHATBOT_OWNER_ID`).
    /// Subagent dispatch is structurally gated on owner-authored messages,
    /// so the batch text and the control frame both carry this mark.
    pub owner: bool,
}

/// Owner = the one person whose direct request opens subagent dispatch
/// (crystal M1: "только по запросу Димони"). Matched by username
/// (`CHATBOT_OWNER_USERNAME`) or, as a fallback for accounts without a
/// username, by numeric id (`CHATBOT_OWNER_ID`). Both default to unset, and
/// unset matches nobody — a fresh clone cannot inherit our operator's identity.
/// Deliberately not tied to `to_me`: an address/@mention is not required for
/// the owner's own request to count.
pub fn is_owner(username: &str, user_id: Option<i64>) -> bool {
    let want = cfg("CHATBOT_OWNER_USERNAME", "").to_lowercase();
    if !want.is_empty() && !username.is_empty() && username.to_lowercase() == want {
        return true;
    }
    let id: i64 = cfg("CHATBOT_OWNER_ID", "0").parse().unwrap_or(0);
    id != 0 && user_id == Some(id)
}

/// Everything needed to write one message, gathered BEFORE the database actor
/// is touched: media downloads and ffmpeg previews are awaits and must not run
/// inside a DB closure, where they would stall every other user of the database.
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
    pub media_path: String,
    pub file_id: String,
    pub file_unique_id: String,
    pub cross: bool,
}

/// Parse one update and fetch its media. Returns None when the message does not
/// belong to the bot's chat (strict mode) or carries no update id.
pub async fn prepare_message(
    msg: &Value,
    state: &str,
    strict_chat: bool,
) -> Result<Option<PreparedMessage>> {
    warn_if_owner_unset();
    let chat_id = required_cfg("CHATBOT_CHAT_ID")?;
    if strict_chat {
        let got = msg
            .get("chat")
            .and_then(|c| c.get("id"))
            .map(|v| v.to_string().trim_matches('"').to_string())
            .unwrap_or_default();
        // chat id may be numeric; compare as strings.
        let want = chat_id.clone();
        let got_norm = got.trim_matches('"').to_string();
        if got_norm != want {
            return Ok(None);
        }
    }
    let update_id = msg
        .get("_update_id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| anyhow!("prepare_message: missing _update_id"))?;
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
    let message_id = msg.get("message_id").and_then(|v| v.as_i64());
    let date = msg.get("date").and_then(|v| v.as_i64()).unwrap_or(0);
    let cross = msg.get("_cross_chat").and_then(|v| v.as_bool()).unwrap_or(false);

    let mut media_path = String::new();
    let mut file_id = String::new();
    let mut file_unique_id = String::new();
    // Media kinds: photo/animation/video/document/sticker + voice/audio/video_note.
    let mut picked: Option<(String, Value)> = None;
    for key in ["photo", "animation", "video", "document", "sticker", "voice", "audio", "video_note"] {
        if let Some(obj) = msg.get(key) {
            let o = if obj.is_array() {
                obj.as_array().unwrap().last().cloned().unwrap_or(json!(null))
            } else {
                obj.clone()
            };
            if o.get("file_id").and_then(|v| v.as_str()).is_some() {
                picked = Some((key.to_string(), o));
                break;
            }
        }
    }
    if let Some((key, obj)) = picked {
        let fid = obj.get("file_id").and_then(|v| v.as_str()).unwrap_or("");
        file_id = fid.to_string();
        file_unique_id = obj
            .get("file_unique_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let duration = obj.get("duration").and_then(|v| v.as_i64());
        let mime = obj.get("mime_type").and_then(|v| v.as_str()).unwrap_or("");
        if key == "voice" || key == "audio" {
            // Marker-only: agent read tool cannot render audio; keep file_id
            // for future STT, never download.
            let secs = duration.map(|d| format!("{d}")).unwrap_or_else(|| "?".to_string());
            text.push_str(&format!(" [{key} message, {secs} sec]"));
        } else if key == "video_note" {
            match download_media(fid, state, &update_id.to_string()).await {
                Ok(p) => match preview_frame(&p, Duration::from_secs(60)).await {
                    Ok(f) => {
                        media_path = f;
                        text.push_str(" [video note]");
                    }
                    Err(e) => {
                        dlog(&format!("frame failed: {e}"));
                        text.push_str(" [video note, preview unavailable]");
                    }
                },
                Err(e) => {
                    dlog(&format!("media download failed: {e}"));
                    text.push_str(" [media unavailable]");
                }
            }
        } else {
            // Document carrying an audio mime is voice-like: marker-only, never download.
            if key == "document" && mime_is_audio(mime) {
                let secs = duration.map(|d| d.to_string()).unwrap_or_else(|| "?".to_string());
                text.push_str(&format!(" [audio message, {secs} sec]"));
            } else {
            match download_media(fid, state, &update_id.to_string()).await {
                Ok(p) => {
                    let ext_video = video_ext(&p);
                    let is_video_kind = key == "animation" || key == "video";
                    let mime_video = mime_is_video(mime)
                        || mime.to_lowercase().starts_with("video/")
                        || mime.to_lowercase() == "image/gif";
                    if is_video_kind || ext_video || mime_video {
                        match preview_frame(&p, Duration::from_secs(60)).await {
                            Ok(f) => {
                                media_path = f;
                                text.push_str(" [gif]");
                            }
                            Err(e) => {
                                dlog(&format!("frame failed: {e}"));
                                media_path = String::new();
                                text.push_str(" [animation, preview unavailable]");
                            }
                        }
                    } else if key == "sticker"
                        && (p.to_lowercase().ends_with(".tgs") || mime.contains("tgsticker"))
                    {
                        text.push_str(" [animated sticker]");
                    } else {
                        media_path = p;
                    }
                }
                Err(e) => {
                    dlog(&format!("media download failed: {e}"));
                    text.push_str(" [media unavailable]");
                }
            }
            }
        }
    }

    let reply_to: Option<i64> = if cross {
        None
    } else {
        msg.get("reply_to_message")
            .and_then(|r| r.get("message_id"))
            .and_then(|v| v.as_i64())
    };

    Ok(Some(PreparedMessage {
        update_id,
        message_id,
        date,
        user_id,
        username,
        first_name,
        text,
        reply_to,
        media_path,
        file_id,
        file_unique_id,
        cross,
    }))
}

/// Write a prepared message as ONE atomic step: duplicate lookup (and the DELETE
/// that the 120-second text contract requires), INSERT and media prune. Splitting
/// these across separate calls would let another unit insert a row between the
/// check and the write, breaking that contract.
pub fn store_prepared(
    con: &Connection,
    prep: &PreparedMessage,
    state: &str,
) -> Result<Option<BatchMsg>> {
    let PreparedMessage {
        update_id,
        message_id,
        date,
        user_id,
        username,
        first_name,
        text,
        reply_to,
        media_path,
        file_id,
        file_unique_id,
        cross,
    } = prep.clone();
    if !text.trim().is_empty() && !username.is_empty() {
        // Predicate order follows idx_messages_author_window (username, date):
        // the 120-second window narrows the rows before `text` is compared.
        if cross {
            let dup: Option<i64> = con
                .prepare_cached(
                    "SELECT update_id FROM messages WHERE username=? AND date>=? AND date<=? AND text=? LIMIT 1",
                )?
                .query_row(params![username, date - 120, date, text], |r| r.get(0))
                .ok();
            if dup.is_some() {
                return Ok(None);
            }
        } else {
            let dup: Option<i64> = con
                .prepare_cached(
                    "SELECT update_id FROM messages WHERE username=? AND date>=? AND date<=? AND text=? AND user_id<0 LIMIT 1",
                )?
                .query_row(params![username, date - 120, date, text], |r| r.get(0))
                .ok();
            if let Some(d) = dup {
                let _ = con.execute("DELETE FROM messages WHERE update_id=?", [d]);
            }
        }
    }
    let res = con.execute(
        "INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id,file_unique_id) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        params![update_id, message_id, date, user_id, username, first_name, text, reply_to, media_path, file_id, file_unique_id],
    );
    if res.is_err() {
        return Ok(None);
    }
    maybe_prune_media(state);
    let owner = is_owner(&username, user_id);
    Ok(Some(BatchMsg {
        message_id: message_id.unwrap_or(0),
        user_id,
        username,
        first_name,
        text,
        reply_to,
        media_path,
        to_me: false,
        owner,
    }))
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
            let mt = std::fs::metadata(&p).and_then(|m| m.modified()).unwrap_or(std::time::UNIX_EPOCH.into());
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

pub fn format_batch(batch: &[BatchMsg]) -> String {
    let mut lines = vec![];
    for m in batch {
        let who = if !m.username.is_empty() {
            format!("@{}", m.username)
        } else if !m.first_name.is_empty() {
            m.first_name.clone()
        } else {
            m.user_id.map(|u| u.to_string()).unwrap_or_default()
        };
        let mut head = format!("[{}] {}:", m.message_id, who);
        if m.to_me {
            head.push_str(" [к тебе]");
        }
        if m.owner {
            head.push_str(" [владелец]");
        }
        if let Some(r) = m.reply_to {
            head.push_str(&format!(" (reply to {r})"));
        }
        if !m.text.is_empty() {
            lines.push(format!("{head} {}", m.text));
        }
        if !m.media_path.is_empty() {
            lines.push(format!("{head} [file] {} -- read it via read tool", m.media_path));
        }
    }
    lines.join("\n")
}

/// One deduplicated GIF in the chat mirror.
#[derive(Debug, Clone, PartialEq)]
pub struct GifEntry {
    /// 1-based position in the full registry (stable ordering: newest first).
    pub idx: i64,
    pub file_id: String,
    /// Caption text of the freshest message showing this GIF (may be empty).
    pub text: String,
}

/// Caps a caption so one long message cannot flood the batch context.
const GIF_TEXT_CAP: usize = 160;

fn gif_one_line(text: &str) -> String {
    let joined = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let cut: String = joined.chars().take(GIF_TEXT_CAP).collect();
    if joined.chars().count() > GIF_TEXT_CAP {
        format!("{cut}…")
    } else {
        cut
    }
}

/// Deduplicated GIF registry: key rule, ordering and page+total in one pass.
///
/// Dedup key: `file_unique_id` when present, else `file_id`. Historic rows have
/// no `file_unique_id` (the Bot API cannot convert file_id -> file_unique_id),
/// so their key is the file_id: re-sent copies of the same GIF collapse, two
/// messages carrying the very same file_id collapse, and a GIF the bot itself
/// re-sent with a fresh file_id is listed twice until a new ingest with
/// file_unique_id appears.
///
/// The page and the registry total come from one pass: `COUNT(*) OVER ()`
/// rides along with the page instead of the second full scan `gif_count`
/// used to pay. Kept as a constant so `dbbench` explains exactly this text.
pub const GIF_PAGE_SQL: &str = "WITH grouped AS (
           SELECT COALESCE(NULLIF(file_unique_id,''), file_id) AS dedup_key,
                  file_id, text, date, message_id,
                  ROW_NUMBER() OVER (
                    PARTITION BY COALESCE(NULLIF(file_unique_id,''), file_id)
                    ORDER BY date DESC, message_id DESC) AS rn
           FROM messages
           WHERE file_id IS NOT NULL AND file_id <> ''
         ),
         page AS (
           SELECT file_id, text, date, message_id, COUNT(*) OVER () AS total
           FROM grouped WHERE rn = 1
           ORDER BY date DESC, message_id DESC LIMIT ? OFFSET ?
         )
         SELECT file_id, text, total FROM page ORDER BY date DESC, message_id DESC";

pub fn gif_page(con: &Connection, offset: i64, limit: i64) -> Result<(Vec<GifEntry>, i64)> {
    let offset = offset.max(0);
    let limit = limit.clamp(1, 500);
    let mut stmt = con.prepare_cached(GIF_PAGE_SQL)?;
    let rows: Vec<(String, Option<String>, i64)> = stmt
        .query_map(params![limit, offset], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .flatten()
        .collect();
    let total = rows.first().map(|r| r.2).unwrap_or(0);
    Ok((
        rows.into_iter()
            .enumerate()
            .map(|(i, (file_id, text, _))| GifEntry {
                idx: offset + i as i64 + 1,
                file_id,
                text: gif_one_line(text.as_deref().unwrap_or("")),
            })
            .collect(),
        total,
    ))
}

/// Deduplicated GIF registry, newest first.
pub fn gif_registry(con: &Connection, offset: i64, limit: i64) -> Result<Vec<GifEntry>> {
    Ok(gif_page(con, offset, limit)?.0)
}

/// Total number of deduplicated GIFs (same key rule as `gif_registry`).
pub fn gif_count(con: &Connection) -> Result<i64> {
    Ok(con
        .prepare_cached(
            "SELECT COUNT(DISTINCT COALESCE(NULLIF(file_unique_id,''), file_id)) FROM messages WHERE file_id IS NOT NULL AND file_id <> ''",
        )?
        .query_row([], |r| r.get(0))?)
}

fn gif_entry_line(e: &GifEntry) -> String {
    if e.text.is_empty() {
        format!("[gif:{}] file_id={} · (без описания)", e.idx, e.file_id)
    } else {
        format!("[gif:{}] file_id={} · «{}»", e.idx, e.file_id, e.text)
    }
}

/// Batch-context catalog: at most `limit` deduplicated GIFs with captions.
/// The full list is served on demand by the `gif_list` tool through the
/// daemon (`gif_list_text`).
pub fn gif_catalog(con: &Connection, limit: i64) -> Result<String> {
    let entries = gif_registry(con, 0, limit)?;
    if entries.is_empty() {
        return Ok(String::new());
    }
    let lines: Vec<String> = entries.iter().map(gif_entry_line).collect();
    Ok(format!(
        "Уже побывавшие в чате гифки (чтобы отправить — передай file_id в параметр gif_file_id тула chat_send, текст рядом станет подписью). Полный список — тул gif_list:\n{}",
        lines.join("\n")
    ))
}

/// Full-registry frame for the daemon `gif_list` call (read-only).
pub fn gif_list_text(entries: &[GifEntry], total: i64) -> String {
    if entries.is_empty() {
        return "Гифок в истории пока нет.".to_string();
    }
    let lines: Vec<String> = entries.iter().map(gif_entry_line).collect();
    format!(
        "Все известные гифки ({} из {total}):\n{}",
        entries.len(),
        lines.join("\n")
    )
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
    eprintln!("{msg}");
    let dir = state_dir();
    let path = Path::new(&dir).join("chatd.log");
    if std::fs::create_dir_all(&dir).is_err() {
        return;
    }
    if let Ok(m) = std::fs::metadata(&path) {
        if m.len() > 2_000_000 {
            let prev = Path::new(&dir).join("chatd.log.1");
            let _ = std::fs::remove_file(&prev);
            let _ = std::fs::rename(&path, &prev);
        }
    }
    use std::io::Write;
    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(&path) {
        let _ = writeln!(f, "{} {msg}", chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"));
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
        for k in ["OMO_CODING_AGENT_DIR", "SENPI_CODING_AGENT_DIR", "PI_CODING_AGENT_DIR"] {
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
        anyhow!("omo {shown}: bad output: {}", stdout.get(..80).unwrap_or(&stdout))
    })
}

pub async fn omo_with_env(args: &[&str], timeout: Duration, extra_env: &HashMap<String, String>) -> Result<Value> {
    let sock = extra_env
        .get("SOCK")
        .cloned()
        .unwrap_or_else(host_socket);
    let mut cmd = tokio::process::Command::new("omo");
    cmd.arg("host").arg("session");
    for a in args {
        cmd.arg(a);
    }
    cmd.arg("--socket").arg(&sock);
    agent_env(&mut cmd);
    let out = tokio::time::timeout(timeout, cmd.output()).await.map_err(|_| {
        anyhow!("omo {}: timeout", args.join(" "))
    })??;
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

pub fn write_launch_spec(state: &str, include_ext: bool) -> Result<PathBuf> {
    let state_path = Path::new(state);
    std::fs::create_dir_all(state_path)?;
    if include_ext {
        let src_path = ext_source();
        let src = std::fs::read(&src_path)
            .with_context(|| format!("chat-ext.mjs read: {}", src_path.display()))?;
        std::fs::write(state_path.join("chat-ext.mjs"), src)?;
    }
    let mut exts = vec![];
    if include_ext {
        exts.push("./chat-ext.mjs".to_string());
    }
    let spec = json!({
        "spec_version": 1,
        "core": {"session_runtime": "in-process", "multi_session": true, "extensions": exts},
        "env": {
            "OMO_CHATBOT_STATE_DIR": state,
            "OMO_CHATBOT_TOKEN_FILE": required_cfg("CHATBOT_TOKEN_FILE")?,
            "OMO_CHATBOT_CHAT_ID": required_cfg("CHATBOT_CHAT_ID")?,
            "OMO_CHATBOT_API_BASE": api_base(),
        },
    });
    let spec_path = state_path.join(if include_ext { "launch-spec.json" } else { "launch-spec-summary.json" });
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
    let mut cmd = tokio::process::Command::new("omo");
    cmd.arg("host").arg("ensure").arg("--launch-spec").arg(&spec_path).arg("--socket").arg(&sock);
    if !agent_dir.is_empty() {
        for k in ["OMO_CODING_AGENT_DIR", "SENPI_CODING_AGENT_DIR", "PI_CODING_AGENT_DIR"] {
            cmd.env(k, &agent_dir);
        }
    }
    let out = cmd.output().await?;
    if !out.status.success() {
        return Err(anyhow!(
            "omo host ensure: rc={} {}",
            out.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&out.stderr).trim().chars().rev().take(200).collect::<String>()
        ));
    }
    Ok(spec_path)
}

pub async fn ensure_host(state: &str) -> Result<PathBuf> {
    ensure_host_with(state, true).await
}

pub async fn close_named_sessions(ref_: &str) {
    close_named_sessions_on(ref_, &host_socket()).await
}

/// Same as close_named_sessions() but against an explicit host socket.
#[allow(dead_code)]
pub async fn close_named_sessions_on(ref_: &str, sock: &str) {
    let data = match omo_on(&["list"], Duration::from_secs(30), sock).await {
        Ok(d) => d,
        Err(_) => return,
    };
    let sessions = data.get("sessions").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    for s in sessions {
        if s.get("name").and_then(|v| v.as_str()) != Some(ref_) {
            continue;
        }
        let sid = s
            .get("durableSessionId")
            .or_else(|| s.get("sessionId"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if sid.is_empty() {
            continue;
        }
        let _ = omo_on(&["close", &sid], Duration::from_secs(30), sock).await;
    }
}

pub async fn ensure_session(system_path: &str, ref_: Option<&str>) -> Result<String> {
    let r = ref_.map(|s| s.to_string()).unwrap_or_else(|| cfg("CHATBOT_SESSION", "chat-participant"));
    let state = state_dir();
    ensure_host(&state).await?;
    close_named_sessions(&r).await;
    let out = omo(
        &["open", "--cwd", &cfg("CHATBOT_CWD", "."), "--name", &r, "--prompt", &format!("@{system_path}")],
        Duration::from_secs(120),
    )
    .await?;
    Ok(out.get("durableSessionId").and_then(|v| v.as_str()).unwrap_or(&r).to_string())
}

pub async fn recreate_session(sid: &str, system_path: &str, ref_: Option<&str>, reason: &str) -> Result<String> {
    if !reason.is_empty() {
        dlog(&format!("recreate session {sid}: {reason}"));
    }
    if let Err(e) = omo(&["close", sid], Duration::from_secs(30)).await {
        dlog(&format!("recreate session {sid}: close failed ({e:#}), sweeping by name in ensure"));
    }
    ensure_session(system_path, ref_).await
}

// ---------- context blocks ----------

/// One recent message row: (message_id, user_id, username, first_name, text, media_path).
pub type RecentRow = (
    Option<i64>,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// The newest `n` messages, newest first. Shared by the batch builder (context
/// block AND the window anchor) so the same rows are no longer read twice.
pub fn recent_rows(con: &Connection, n: i64) -> Result<Vec<RecentRow>> {
    let mut stmt = con.prepare_cached(
        "SELECT message_id,user_id,username,first_name,text,media_path FROM messages ORDER BY date DESC, message_id DESC LIMIT ?",
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
    for (mid, uid, user, first, text, media) in rows.iter().rev() {
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
        let mark = if self_id != 0 && *uid == Some(self_id) { " [моё]" } else { "" };
        let mut head = format!("[{}] {}{}:", mid.unwrap_or(0), who, mark);
        if mid.map(|m| answered.contains(&m)).unwrap_or(false) {
            head.push_str(" [на это уже отвечено — не отвечать]");
        }
        let mut chunk = vec![];
        if let Some(t) = text {
            if !t.is_empty() {
                let c: String = t.chars().take(cap).collect();
                chunk.push(format!("{head} {c}"));
            }
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
    let tail: Vec<String> = blocks.into_iter().rev().take(limit as usize).rev().collect();
    if tail.is_empty() {
        return String::new();
    }
    format!("Контекст чата (последние {}):\n{}", tail.len(), tail.join("\n"))
}

/// Context block for callers that hold no precomputed answered set.
pub fn recent_context(state: &str, con: &Connection, batch_ids: &HashSet<i64>, limit: i64, cap: usize) -> Result<String> {
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
    Ok(render_recent_context(&rows, batch_ids, limit, cap, self_id, answered))
}

pub fn mirror_own_send(con: &Connection, message_id: i64, text: &str, reply_to: Option<i64>) -> Result<()> {
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
    let _ = con.execute(
        "INSERT OR IGNORE INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id) VALUES(?,?,?,?,?,?,?,?,?,?)",
        params![-message_id, message_id, now, self_id, username, first, text, reply_to, "", ""],
    )?;
    Ok(())
}

pub async fn mirror_own_send_auto(con: &Connection, message_id: i64, text: &str, reply_to: Option<i64>) -> Result<()> {
    let self_id: i64 = kv_get(con, "self_id", "0").parse().unwrap_or(0);
    if self_id == 0 {
        match api("getMe", &json!({})).await {
            Ok(me) => {
                let id: i64 = me.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
                if id == 0 {
                    return Ok(());
                }
                kv_set(con, "self_id", &id.to_string())?;
                kv_set(con, "self_username", me.get("username").and_then(|v| v.as_str()).unwrap_or(""))?;
                kv_set(con, "self_first", me.get("first_name").and_then(|v| v.as_str()).unwrap_or(""))?;
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
    let rows: Vec<(Option<i64>, Option<String>)> =
        stmt.query_map(params![self_id, limit], |r| Ok((r.get(0)?, r.get(1)?)))?.flatten().collect();
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
        let a = if a.trim().is_empty() { "[gif]".to_string() } else { a.trim().to_string() };
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
    let slice = if cut { &data[data.len() - TAIL..] } else { &data[..] };
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
        if let Some(q) = v.get("receipt").and_then(|r| r.get("reply_to")).and_then(|x| x.as_i64()) {
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
        .map(|v| v.iter().map(|x| x.to_string().trim_matches('"').to_string()).collect())
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
        .map(|v| v.iter().map(|x| x.to_string().trim_matches('"').to_string()).collect())
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

pub async fn wait_for(state: &str, mut offset: u64, want: &str, sig: &str, budget: Duration) -> Result<(Option<Value>, Vec<Value>, u64)> {
    let deadline = Instant::now() + budget;
    let mut receipts = vec![];
    while Instant::now() < deadline {
        let (events, noff) = read_events(state, offset)?;
        offset = noff;
        for ev in events {
            let kind = ev.get("kind").and_then(|v| v.as_str()).unwrap_or("");
            if (kind == "chat_send_result" || kind == "chat_send_replay") && ev.get("receipt").map(|r| r.is_object()).unwrap_or(false) {
                receipts.push(ev.get("receipt").unwrap().clone());
            }
            if kind == want && (want != "admitted" || ev.get("delivery_id").and_then(|v| v.as_str()).unwrap_or("") == sig) {
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
        let mid_int: Option<i64> = mid.as_i64().or_else(|| mid.as_str().and_then(|s| s.parse().ok()));
        let mid_int = match mid_int {
            Some(m) => m,
            None => continue,
        };
        let row: Option<(Option<i64>, Option<i64>, Option<String>, Option<String>, Option<String>, Option<i64>, Option<String>)> = con
            .prepare_cached(
                "SELECT message_id,user_id,username,first_name,text,reply_to,media_path FROM messages WHERE message_id=? LIMIT 1",
            )
            .and_then(|mut stmt| {
                stmt.query_row([mid_int], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?))
                })
            })
            .ok();
        if let Some((m, u, un, f, t, r, mp)) = row {
            let username = un.unwrap_or_default();
            out.push(BatchMsg {
                message_id: m.unwrap_or(mid_int),
                user_id: u,
                owner: is_owner(&username, u),
                username,
                first_name: f.unwrap_or_default(),
                text: t.unwrap_or_default(),
                reply_to: r,
                media_path: mp.unwrap_or_default(),
                to_me: false,
            });
        }
    }
    Ok(out)
}

pub fn regen_text(state: &str, con: &Connection, sig: &str) -> Result<Option<String>> {
    let raw_ids = kv_get(con, &format!("pending_batch_ids:{sig}"), "");
    let ids: Vec<Value> = if raw_ids.is_empty() { vec![] } else { serde_json::from_str(&raw_ids).unwrap_or_default() };
    if ids.is_empty() {
        return Ok(None);
    }
    let mut id_set = HashSet::new();
    for mid in &ids {
        if let Some(m) = mid.as_i64().or_else(|| mid.as_str().and_then(|s| s.parse().ok())) {
            id_set.insert(m);
        }
    }
    if id_set.is_empty() {
        return Ok(None);
    }
    let mut rows = batch_rows_by_ids(con, &ids)?;
    if rows.is_empty() {
        return Ok(None);
    }
    // Same structural filter as the fresh-batch path: never re-ask answered
    // rows after a compact. When nothing unanswered remains, close the batch.
    let answered = answered_ids(state, con);
    rows.retain(|r| !answered.contains(&r.message_id));
    if rows.is_empty() {
        mark_done(con, state, sig)?;
        dlog(&format!("compacted: {sig} fully answered since flush, closing"));
        return Ok(None);
    }
    let raw_tome = kv_get(con, &format!("pending_tome:{sig}"), "");
    let tome: HashSet<i64> = if raw_tome.is_empty() {
        HashSet::new()
    } else {
        serde_json::from_str::<Vec<Value>>(&raw_tome)
            .unwrap_or_default()
            .iter()
            .filter_map(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
            .collect()
    };
    for r in rows.iter_mut() {
        if tome.contains(&r.message_id) {
            r.to_me = true;
        }
    }
    let context = recent_context_with(con, &id_set, 50, 300, &answered)?;
    let guard = answered_guard(con, 5)?;
    let catalog = gif_catalog(con, 20)?;
    let mut parts = vec![];
    if !context.is_empty() {
        parts.push(context);
    }
    if !guard.is_empty() {
        parts.push(guard);
    }
    if !catalog.is_empty() {
        parts.push(catalog);
    }
    parts.push(format_batch(&rows));
    Ok(Some(parts.join("\n")))
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

/// Owner-authored message ids of a pending batch, as written to the ledger at
/// admit time. Recovery paths re-push the same frame, so they must rebuild the
/// same owner set instead of guessing.
pub fn pending_owner_ids(con: &Connection, sig: &str) -> Vec<i64> {
    let raw = kv_get(con, &format!("pending_owner:{sig}"), "");
    if raw.is_empty() {
        return vec![];
    }
    serde_json::from_str::<Vec<Value>>(&raw)
        .unwrap_or_default()
        .iter()
        .filter_map(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
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
pub fn delivery_phase(admitted_at: Option<i64>, emitted_at: Option<i64>, settled_at: Option<i64>) -> &'static str {
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
pub fn should_retire(all_answered: bool, emitted_at: Option<i64>, settled_at: Option<i64>) -> Option<&'static str> {
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
pub fn stalled(now: i64, last_progress: i64, budget_secs: u64, working: bool, extra_secs: u64) -> bool {
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

pub async fn settle_session(state: &str, db: &crate::db::Db, offset: u64, budget: Duration) -> Result<u64> {
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
) -> Result<(String, String, Vec<i64>, Vec<i64>, Vec<i64>, String)> {
    // Drop answered rows structurally (not just via the guard block): after a
    // session recreate the agent is memory-less and re-answers anything it is
    // handed, paraphrased. Exact message_id match only — a user re-asking the
    // same thing sends a NEW id and is unaffected.
    let answered = answered_ids(state, con);
    let batch: Vec<BatchMsg> = batch.iter().filter(|m| !answered.contains(&m.message_id)).cloned().collect();
    let batch_ids: HashSet<i64> = batch.iter().map(|m| m.message_id).collect();
    // ONE read of the recent window feeds both the context block and the
    // window anchor that used to cost a second, identical pass.
    let rows = recent_rows(con, 50 + batch_ids.len() as i64 + 5)?;
    let self_id: i64 = kv_get(con, "self_id", "0").parse().unwrap_or(0);
    let context = render_recent_context(&rows, &batch_ids, 50, 300, self_id, &answered);
    let guard = answered_guard(con, 5)?;
    let catalog = gif_catalog(con, 20)?;
    let mut parts = vec![];
    if !context.is_empty() {
        parts.push(context);
    }
    if !guard.is_empty() {
        parts.push(guard);
    }
    if !catalog.is_empty() {
        parts.push(catalog);
    }
    parts.push(format_batch(&batch));
    let text = parts.join("\n");
    let sig = batch_sig(&text);
    let ids: Vec<i64> = batch.iter().map(|m| m.message_id).collect();
    let tome: Vec<i64> = batch.iter().filter(|m| m.to_me).map(|m| m.message_id).collect();
    let owners: Vec<i64> = batch.iter().filter(|m| m.owner).map(|m| m.message_id).collect();
    let anchor = rows
        .iter()
        .filter_map(|r| r.0)
        .filter(|id| !batch_ids.contains(id))
        .max()
        .unwrap_or(0);
    Ok((text, sig, ids, tome, owners, anchor.to_string()))
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
    // List sessions on the scratch host to close strays best-effort.
    close_named_sessions_on(&r, &sock).await;
    let out = omo_on(
        &["open", "--cwd", &cfg("CHATBOT_CWD", "."), "--name", &r, "--prompt", &format!("@{system_path}")],
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
    let sid = out.get("durableSessionId").and_then(|v| v.as_str()).unwrap_or(&r).to_string();
    // Verify no chat_send tool. The CLI offers no `tools` subcommand;
    // `state` is the introspection it has. Structural isolation already
    // holds via the extension-free launch-spec; this fails closed if a
    // chat_send tool entry ever shows up in the fresh session state.
    let st = omo_on(&["state", &sid], Duration::from_secs(30), &sock).await.unwrap_or(json!({}));
    if json_has_tool_name(&st, "chat_send") {
        close_summary_session(&sid, &sock).await;
        return Err(anyhow!("summary session exposes chat_send: isolation violated"));
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
            to_me: true,
            owner: false,
        }];
        let s = format_batch(&b);
        assert!(s.contains("[1] @u: [к тебе] hi"));
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
        let n: i64 = con.query_row("SELECT COUNT(*) FROM messages WHERE message_id=7", [], |r| r.get(0)).unwrap();
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
        std::fs::write(events_path(&state), b"{\"kind\":\"turn_end\"}\n{\"kind\":" ).unwrap();
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
            to_me: true,
            owner: false,
        }
    }

    #[test]
    fn format_batch_marks_owner() {
        let mut m = mk_batch_msg(5, "слушай");
        m.owner = true;
        let s = format_batch(&[m]);
        assert!(s.contains("[владелец] слушай"), "got: {s}");
    }

    #[test]
    fn is_owner_matrix() {
        std::env::set_var("CHATBOT_OWNER_USERNAME", "dungeonmastergui");
        std::env::set_var("CHATBOT_OWNER_ID", "777");
        assert!(is_owner("dungeonmastergui", Some(1)));
        assert!(is_owner("DungeonMasterGui", Some(1)));
        assert!(is_owner("", Some(777)));
        assert!(!is_owner("someone", Some(778)));
        assert!(!is_owner("someone", None));
        assert!(!is_owner("", None));
        std::env::remove_var("CHATBOT_OWNER_USERNAME");
        std::env::remove_var("CHATBOT_OWNER_ID");
    }

    fn seed_qa(state: &str) -> Connection {
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
        let batch = vec![mk_batch_msg(100, "старый вопрос"), mk_batch_msg(101, "новый вопрос")];
        let (text, _sig, ids, _tome, _owners, _anchor) = build_batch_text(&state, &con, &batch).unwrap();
        assert_eq!(ids, vec![101]);
        assert!(text.contains("[100]") && text.contains("уже отвечено"), "got: {text}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn regen_closes_fully_answered_batch() {
        let dir = std::env::temp_dir().join(format!("chat-rc-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let state = dir.to_string_lossy().to_string();
        let con = seed_qa(&state);
        kv_set(&con, "pending_batch_ids:s1", "[100]").unwrap();
        kv_set(&con, "pending_tome:s1", "[]").unwrap();
        kv_set(&con, "pending_window:s1", "?").unwrap();
        save_pending(&con, &["s1".to_string()]).unwrap();
        let regen = regen_text(&state, &con, "s1").unwrap();
        assert!(regen.is_none());
        assert!(done_ids(&con).contains("s1"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn gif_registry_dedupes_limits_and_orders() {
        let dir = std::env::temp_dir().join(format!("chat-gif-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let state = dir.to_string_lossy().to_string();
        let con = db(&state).unwrap();
        let ins = |uid: i64, mid: i64, date: i64, fid: &str, fu: &str, text: &str| {
            con.execute(
                "INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id,file_unique_id) VALUES (?,?,?,7,'u','U',?,NULL,'',?,?)",
                params![uid, mid, date, text, fid, fu],
            )
            .unwrap();
        };
        // Same unique id twice: one row, freshest caption wins.
        ins(1, 501, 1000, "fid_a_old", "uniq_a", "старая подпись");
        ins(2, 502, 2000, "fid_a_new", "uniq_a", "новая подпись");
        // Historical rows without file_unique_id dedupe by file_id.
        ins(3, 903, 9170, "fid_b", "", "");
        ins(4, 904, 9240, "fid_b", "", "повтор с тем же file_id");
        ins(5, 905, 9190, "fid_c", "uniq_c", "третья гифка");

        let all = gif_registry(&con, 0, 20).unwrap();
        assert_eq!(all.len(), 3, "dedup failed: {all:?}");
        assert_eq!(all[0].file_id, "fid_b", "newest first");
        assert_eq!(all[0].text, "повтор с тем же file_id");
        assert_eq!(all[1].file_id, "fid_c");
        assert_eq!(all[2].file_id, "fid_a_new");
        assert_eq!(all[2].text, "новая подпись", "freshest caption of the group wins");
        assert_eq!(all[0].idx, 1);
        assert_eq!(all[2].idx, 3);

        let two = gif_registry(&con, 0, 2).unwrap();
        assert_eq!(two.len(), 2);
        assert_eq!(two[1].file_id, "fid_c");
        let page = gif_registry(&con, 2, 2).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].file_id, "fid_a_new");
        assert_eq!(page[0].idx, 3);

        let cat = gif_catalog(&con, 2).unwrap();
        assert_eq!(cat.matches("[gif:").count(), 2);
        assert!(cat.contains("«третья гифка»"), "captions are shown: {cat}");
        let list = gif_list_text(&gif_registry(&con, 0, 200).unwrap(), gif_count(&con).unwrap());
        assert_eq!(list.matches("[gif:").count(), 3);
        assert!(list.contains("3 из 3"));
        assert_eq!(gif_count(&con).unwrap(), 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn gif_catalog_empty_is_silent() {
        let dir = std::env::temp_dir().join(format!("chat-gif0-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let state = dir.to_string_lossy().to_string();
        let con = db(&state).unwrap();
        assert_eq!(gif_catalog(&con, 20).unwrap(), "");
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
}
