//! chat-bot shared core: port of chatd.py (run/sync/summarize helpers) +
//! tg-chat helpers. Single-threaded async: all awaits run on a
//! `current_thread` tokio runtime (see bins); sqlite work is inline on the
//! same thread; typing indicator is a same-thread spawned task.

use anyhow::{anyhow, Context, Result};
use rusqlite::Connection;
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
pub mod catalog;
pub mod context;
pub mod db;
pub mod favorites;
pub mod knowledge;
pub mod ledger;
pub mod media;
pub mod ops;
pub mod send;
pub mod store;
pub mod session;

pub use store::GIF_PAGE_SQL;
pub use catalog::{Caption, FitsWhen, GifPage, GifRecord, GifSource, MediaKind};

use crate::media::MediaState;


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

/// Opens (creating if needed) the state database. The data-layer entry point;
/// the connection itself is owned by `db::Db` (see `src/db.rs`).
pub fn db(state: &str) -> Result<Connection> {
    crate::db::connect(state)
}

pub use crate::db::{
    acquire_state_lock, ensure_indexes, ensure_journal_columns, kv_del, kv_get, kv_set, migrate, migrate_v1,
    migrate_v2, migrate_v3, migrate_v4, migrate_v5, migrate_v6, migration_chat_id,
    require_migration_chat_id, StateLock, SCHEMA,
};


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
    /// Telegram `thumbnail.file_id` for video-like messages. A video never
    /// reaches the model as bytes on this wire — only a frame can — so the row
    /// stays a descriptor and this frame id is what gets fetched.
    pub thumb_file_id: String,
    /// `Pending` while the frame is being fetched, `Ready` once it is on disk.
    pub thumb_state: MediaState,
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
            date: self.date,
            user_id: self.user_id,
            username: self.username.clone(),
            first_name: self.first_name.clone(),
            text: self.text.clone(),
            reply_to: self.reply_to,
            media_path: self.media_path.clone(),
            file_id: self.file_id.clone(),
            media_kind: self.media_kind.clone(),
            thumb_path: String::new(),
            sender_chat: self.sender_chat,
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
    /// Telegram server Unix time of this message (`Message.date`: the date
    /// the message was sent into this chat). For a forward this is the
    /// arrival time, NOT `forward_date` (the original's date): staleness is
    /// the queue delay in this chat, so a year-old message forwarded now is
    /// fresh work. `0` when unknown — never treated as stale.
    pub date: i64,
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
    /// Local path of a video frame, filled only when that frame is readable.
    /// A video never reaches the model as bytes on this wire, so this path is
    /// the only part of it the model can actually look at.
    pub thumb_path: String,
    /// Telegram `sender_chat.id`: set on the group copy of a linked channel
    /// post, where `user_id` is Telegram's 777000 service account. It lets the
    /// renderer recognise the bot's own channel posts.
    pub sender_chat: Option<i64>,
    pub to_me: bool,
}

/// A message older than this many seconds (by its Telegram server `date`)
/// is never sent to the model: after a stall or a downtime it is history,
/// not a question, and answering it wakes a dead thread. The boundary is
/// inclusive — exactly this old is still fresh.
pub const MESSAGE_MAX_AGE_SECS: i64 = 600;

/// How far ahead of our own clock a Telegram `date` may sit and still prove
/// freshness. Clocks drift by seconds, never by minutes.
pub const MESSAGE_FUTURE_SKEW_SECS: i64 = 300;

/// Whether one message is too old — or too unproven — to reach the model.
///
/// Fail-closed by contract (owner, 2026-11-16): freshness must be PROVEN by a
/// valid server `date`, so a missing date (`0`) or one implausibly far in the
/// future drops the message instead of answering it. A wrong answer to an
/// ancient message is worse than no answer at all. A future date inside the
/// skew window is still fresh: the two clocks merely disagree by seconds.
/// Pure, so the boundary is testable without a database.
pub fn is_stale_by_time(date: i64, now_secs: i64) -> bool {
    if date <= 0 {
        return true;
    }
    let age = now_secs.saturating_sub(date);
    age > MESSAGE_MAX_AGE_SECS || age < -MESSAGE_FUTURE_SKEW_SECS
}

/// Split a batch into fresh rows and rows the age guard drops. Returns the
/// fresh rows and the dropped message ids. One place: fresh, rebuilt and
/// recovered batches all pass through here before context assembly, so the
/// guard applies identically on every path.
pub fn apply_age_guard(batch: Vec<BatchMsg>, now_secs: i64) -> (Vec<BatchMsg>, Vec<i64>) {
    let mut fresh = Vec::with_capacity(batch.len());
    let mut skipped = Vec::new();
    for m in batch {
        if is_stale_by_time(m.date, now_secs) {
            skipped.push(m.message_id);
        } else {
            fresh.push(m);
        }
    }
    (fresh, skipped)
}

/// A post to a linked channel reaches ingest twice: as the `channel_post`
/// update, whose sender id is the negative channel id, and as the automatic
/// forward in the discussion group. The channel copy's `message_id` does not
/// exist in the chat the answers go to, so answering it targets a dead message
/// (and produced the second, skipped batch of every previous post). Keep the
/// group copy; drop the channel twin when it is in the same batch.
pub fn channel_post_key(m: &BatchMsg) -> Option<(String, String, String)> {
    let text = m.text.trim();
    if text.is_empty() && m.file_id.is_empty() {
        return None;
    }
    Some((text.to_string(), m.media_kind.clone(), m.file_id.clone()))
}

pub fn channel_post_identity(m: &BatchMsg) -> Option<String> {
    channel_post_key(m).map(|(text, kind, file)| format!("{text}\u{1}{kind}\u{1}{file}"))
}

/// `known_group` carries identities of group copies committed in earlier
/// batches. A channel twin can arrive seconds after its group copy; checking
/// only the current batch lets both copies be answered.
pub fn dedupe_channel_posts_known(
    batch: Vec<BatchMsg>,
    known_group: &std::collections::HashSet<String>,
) -> Vec<BatchMsg> {
    let mut group = known_group.clone();
    group.extend(
        batch
            .iter()
            .filter(|m| m.user_id.map(|u| u > 0).unwrap_or(true))
            .filter_map(channel_post_identity),
    );
    batch
        .into_iter()
        .filter(|m| {
            let channel = m.user_id.map(|u| u < 0).unwrap_or(false);
            let answered = channel_post_identity(m)
                .map(|k| group.contains(&k))
                .unwrap_or(false);
            !(channel && answered)
        })
        .collect()
}

pub fn dedupe_channel_posts(batch: Vec<BatchMsg>) -> Vec<BatchMsg> {
    dedupe_channel_posts_known(batch, &std::collections::HashSet::new())
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
    let mut thumb_file_id = String::new();
    let mut thumb_state = MediaState::Absent;
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
        // A video kind never carries its mp4 into the context: the wire has no
        // video part, and an mp4 in the image slot is a 400. Only the frame
        // can arrive, and it has its own state, so the row stays `descriptor`
        // and the mp4 is never downloaded.
        if matches!(kind.as_str(), "video" | "animation" | "video_note") {
            if let Some(thumb) = obj.get("thumbnail") {
                thumb_file_id = thumb
                    .get("file_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                if media_size.is_none() {
                    media_size = thumb
                        .get("file_size")
                        .and_then(|v| v.as_i64())
                        .filter(|n| *n >= 0)
                        .map(|n| n as u64);
                }
            }
            if !thumb_file_id.is_empty() {
                thumb_state = MediaState::Pending;
            }
            media_state = MediaState::Descriptor;
        }
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
        thumb_file_id,
        thumb_state,
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
    // stdout goes to a file, never a pipe: the CLI writes its JSON answer with
    // one write() and drops the unwritten remainder, so on a pipe a long frame
    // is cut at the 64 KiB pipe buffer (`bad output: EOF while parsing a
    // string`), while a regular file receives all of it. `read --messages` of
    // one knowledge chunk is ~180 KiB, i.e. always over that line.
    // `Command::output()` cannot be used here: tokio overrides stdout/stderr
    // with its own pipes, so the file would stay empty.
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let tmp = std::env::temp_dir().join(format!(
        "omo-out-{}-{}.json",
        std::process::id(),
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let file = std::fs::File::create(&tmp)
        .with_context(|| format!("omo stdout capture: {}", tmp.display()))?;
    cmd.stdout(std::process::Stdio::from(file));
    cmd.stderr(std::process::Stdio::piped());
    cmd.kill_on_drop(true);
    let child = cmd
        .spawn()
        .with_context(|| format!("omo spawn: {}", args.join(" ")))?;
    let out = tokio::time::timeout(timeout, child.wait_with_output()).await;
    let stdout = std::fs::read(&tmp).unwrap_or_default();
    let _ = std::fs::remove_file(&tmp);
    let out = out
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
    let stdout = String::from_utf8_lossy(&stdout).to_string();
    serde_json::from_str(&stdout).map_err(|e| {
        // The parse reason is the only actionable part of a broken answer:
        // a truncated or non-JSON frame must be diagnosable from the log.
        let shown = args.join(" ");
        anyhow!("omo {shown}: bad output: {e}")
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

/// The chat compactor tree: staged as a whole directory because the host loads
/// its entry module and the entry imports the rest of the tree by relative path.
const CHAT_COMPACTOR_DIR: &str = "chat-compactor";
const CHAT_COMPACTOR_ENTRY: &str = "./chat-compactor/index.ts";

fn source_dir(name: &str) -> PathBuf {
    for base in [cfg("CHATBOT_CWD", ""), ".".to_string()] {
        if base.is_empty() {
            continue;
        }
        let p = Path::new(&base).join(name);
        if p.exists() {
            return p;
        }
    }
    Path::new(env!("CARGO_MANIFEST_DIR")).join(name)
}

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

/// Copy one directory tree, skipping VCS and installed-dependency noise so the
/// stage stays exactly the code the host loads.
fn copy_tree(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == "node_modules" || name.starts_with('.') {
            continue;
        }
        let from = entry.path();
        let to = dst.join(entry.file_name());
        let kind = entry.file_type()?;
        if kind.is_dir() {
            copy_tree(&from, &to)?;
        } else if kind.is_file() {
            std::fs::copy(&from, &to)
                .with_context(|| format!("stage copy: {}", from.display()))?;
        }
    }
    Ok(())
}

/// Stage the compactor tree under the spec dir. The directory is recreated so a
/// module deleted from the repository cannot survive in the host's tree.
pub fn stage_chat_compactor(state: &str) -> Result<PathBuf> {
    let src = source_dir(CHAT_COMPACTOR_DIR);
    let dst = Path::new(state).join(CHAT_COMPACTOR_DIR);
    if dst.exists() {
        std::fs::remove_dir_all(&dst)?;
    }
    copy_tree(&src, &dst)?;
    let entry = dst.join("index.ts");
    if !entry.exists() {
        return Err(anyhow!("chat compactor entry missing: {}", entry.display()));
    }
    Ok(dst)
}

pub fn write_launch_spec(state: &str, include_ext: bool) -> Result<PathBuf> {
    write_launch_spec_with(state, include_ext, include_ext)
}

/// `phase_strip` stages the muse-spark phase-strip transformer without
/// chat-ext: knowledge sessions must reach the web and keep doing tool calls
/// across turns with the same provider, while staying unable to send to chat.
pub fn write_launch_spec_with(
    state: &str,
    include_ext: bool,
    phase_strip: bool,
) -> Result<PathBuf> {
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
    if include_ext || phase_strip {
        // The phase-strip extension ships with the bot so the muse-spark
        // provider no longer dead-ends on a replayed `phase` marker. Launch-spec
        // extension paths must resolve inside the spec dir, hence the copy.
        let strip_path = phase_strip_source();
        let strip = std::fs::read(&strip_path)
            .with_context(|| format!("muse-spark-phase-strip.ts read: {}", strip_path.display()))?;
        std::fs::write(state_path.join(PHASE_STRIP_FILE), strip)?;
        exts.push(format!("./{PHASE_STRIP_FILE}"));
    }
    if include_ext {
        // The compactor must be the LAST extension the host registers: builtin
        // compaction loads before the spec's extensions, and the extension that
        // answers `session_before_compact` last is the result Pi keeps. The tree
        // ships as a directory because the entry imports its modules relatively.
        stage_chat_compactor(state)?;
        exts.push(CHAT_COMPACTOR_ENTRY.to_string());
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
    ensure_host_on_with(state, include_ext, include_ext, sock).await
}

/// Same as [`ensure_host_on`] with an explicit phase-strip decision.
pub async fn ensure_host_on_with(
    state: &str,
    include_ext: bool,
    phase_strip: bool,
    sock: &str,
) -> Result<PathBuf> {
    let spec_path = write_launch_spec_with(state, include_ext, phase_strip)?;
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

/// Hash of the extension sources a launch spec stages: the chat extension, the
/// whole compactor tree, and the bot's compactor config/prompt. Editing a prompt
/// or a compactor module without changing this hash would leave the running host
/// on the old generation, so all three belong in it.
pub fn ext_hash() -> Result<String> {
    let mut h = Sha256::new();
    h.update(std::fs::read(ext_source())?);
    hash_tree(&source_dir(CHAT_COMPACTOR_DIR), &mut h)?;
    let agent = cfg("CHATBOT_AGENT_DIR", "");
    if !agent.is_empty() {
        for name in ["compactors/rich.json", "compactors/rich.md"] {
            let path = Path::new(&agent).join(name);
            h.update(name.as_bytes());
            h.update(
                std::fs::read(&path)
                    .with_context(|| format!("compactor config read: {}", path.display()))?,
            );
        }
    }
    Ok(format!("{:x}", h.finalize())[..16].to_string())
}

/// Fold a directory tree into the hash in a stable order: relative path then
/// bytes, so a rename changes the digest as surely as an edit does.
fn hash_tree(dir: &Path, h: &mut Sha256) -> Result<()> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)?
        .map(|e| e.map(|e| e.path()))
        .collect::<std::io::Result<_>>()?;
    entries.sort();
    for path in entries {
        let name = path.file_name().unwrap_or_default().to_string_lossy().to_string();
        if name == "node_modules" || (path.is_dir() && name.starts_with('.')) {
            continue;
        }
        if path.is_dir() {
            hash_tree(&path, h)?;
        } else if path.is_file() {
            let rel = path.strip_prefix(dir).unwrap_or(&path).to_string_lossy().to_string();
            h.update(rel.as_bytes());
            h.update(std::fs::read(&path)?);
        }
    }
    Ok(())
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
    store: &crate::store::Store,
    system_path: &str,
    ref_: Option<&str>,
) -> Result<String> {
    let r = ref_
        .map(|s| s.to_string())
        .unwrap_or_else(session::configured_name);
    let state = state_dir();
    ensure_host(&state).await?;
    let recorded = crate::store::live_identity_of(store)
        .await
        .ok()
        .flatten()
        .map(|(uid, _)| uid);
    let report = session::repair_sessions_on(&r, &host_socket(), recorded.as_deref()).await?;
    if let Some(uid) = report.keep {
        crate::store::record_live_of(store, r.clone(), uid.clone()).await?;
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
    crate::store::record_live_of(store, r.clone(), uid.clone()).await?;
    Ok(uid)
}

pub async fn recreate_session(
    store: &crate::store::Store,
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
            crate::store::mark_dead_of(store, sid.to_string()).await?;
        }
        Err(e) => dlog(&format!(
            "recreate session {sid}: close by id failed ({e:#}); keeping it instead of creating a duplicate"
        )),
    }
    ensure_session(store, system_path, ref_).await
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
    Option<i64>,
);

/// Render the context block from recent rows (owned by the data layer).
pub fn render_recent_context(
    rows: &[RecentRow],
    batch_ids: &HashSet<i64>,
    limit: i64,
    cap: usize,
    self_id: i64,
    answered: &HashSet<i64>,
) -> String {
    let mut blocks = vec![];
    for (mid, uid, user, first, text, media, media_kind, file_id, sender_chat) in rows.iter().rev() {
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
        let mark = if crate::context::is_own_author(self_id, *uid, *sender_chat) {
            " [это написал ты сам]"
        } else {
            " [фон чата]"
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
        "Контекст чата (фон, последние {}):\n{}",
        tail.len(),
        tail.join("\n")
    )
}

/// Context block for callers that hold no precomputed answered set.
/// Same block, reusing an answered set the caller already computed (`answered_ids`
/// reads the events journal; reading it twice per batch doubled that work).
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

/// Reply targets of our own recent sends, read from the events file tail (not
/// the mirror): receipt mirroring can lag batch building by a full cycle
/// (observed 57s: receipt 10:25:47, mirror row date 10:26:44), and a filter
/// built on the mirror alone would then miss just-answered messages.
pub fn answered_receipts(state: &str) -> HashSet<i64> {
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

/// Every inbound message id already answered: the store's journal/mirror half
/// plus the receipts read from the events file (file IO stays outside the
/// actor). Used to mark answered rows inline and drop them from fresh batches.
pub async fn answered_all(store: &crate::store::Store, state: &str) -> Result<HashSet<i64>> {
    let mut out = crate::store::answered_db_of(store).await?;
    out.extend(answered_receipts(state));
    Ok(out)
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

pub async fn regen_text(
    store: &crate::store::Store,
    state: &str,
    sig: &str,
) -> Result<Option<String>> {
    let ids = crate::store::message_ids_of(store, sig.to_string()).await?;
    if ids.is_empty() {
        return Ok(None);
    }
    let ids: Vec<Value> = ids.into_iter().map(|i| json!(i)).collect();
    let mut rows = crate::store::batch_rows_of(store, ids).await?;
    if rows.is_empty() {
        return Ok(None);
    }
    if let Some(view) = crate::store::batch_view_of(store, sig.to_string()).await? {
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
    // The age guard runs here too, so a batch that stalled past the age
    // budget is closed as skipped instead of re-delivered forever.
    let (fresh, skipped) = apply_age_guard(rows, crate::ops::now_secs());
    if !skipped.is_empty() {
        dlog(&format!(
            "age guard: {skipped:?} older than {}s, not sent to the model",
            MESSAGE_MAX_AGE_SECS
        ));
    }
    if fresh.is_empty() {
        crate::store::close_batch_of(store, sig.to_string(), "skip_by_time".to_string()).await?;
        dlog(&format!("regen {sig}: every row stale by time, closed as skip_by_time"));
        return Ok(None);
    }
    let answered = answered_all(store, state).await?;
    let snapshot = crate::store::context_snapshot(store, fresh, answered).await?;
    let built = build_windowed(store, snapshot, Some(sig)).await?;
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

/// Per-batch delivery marks, persisted so a restart re-evaluates the same
/// rules instead of re-delivering (or retiring) by accident.



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

pub async fn settle_session(
    state: &str,
    store: &crate::store::Store,
    offset: u64,
    budget: Duration,
) -> Result<u64> {
    let (_ev, receipts, offset) = wait_for(state, offset, "turn_end", "", budget).await?;
    if !receipts.is_empty() {
        crate::store::mirror_receipts_of(store, receipts).await?;
    }
    Ok(offset)
}

pub async fn build_batch_text(
    store: &crate::store::Store,
    state: &str,
    batch: &[BatchMsg],
) -> Result<(String, String, Vec<i64>, Vec<i64>, String, Vec<i64>)> {
    let answered = answered_all(store, state).await?;
    let (fresh, skipped) = apply_age_guard(batch.to_vec(), crate::ops::now_secs());
    if !skipped.is_empty() {
        dlog(&format!(
            "age guard: {skipped:?} older than {}s, not sent to the model",
            MESSAGE_MAX_AGE_SECS
        ));
    }
    let snapshot = crate::store::context_snapshot(store, fresh, answered).await?;
    // No ledger key yet: this build defines it (`sig` is the hash of the text),
    // so the batch key is whatever this document hashes to.
    let built = build_windowed(store, snapshot, None).await?;
    Ok((
        built.text,
        built.sig,
        built.ids,
        built.tome,
        built.anchor.to_string(),
        skipped,
    ))
}

/// The history window is decided HERE, once per document, for both the fresh
/// batch and every rebuild.
///
/// The window rides only a turn that gives the model a new context (a changed
/// `durableSessionId:generation`) or the first turn after a compaction. Every
/// other turn carries the batch, the answer guard and the GIF catalogue; the
/// model keeps its own conversation history in the live session.
///
/// The batch is identified by its LEDGER key (`sig`), never by the hash of the
/// text being built now: a rebuilt document differs in its guard/media lines
/// yet is the same batch, and it must keep the window it was prepared with.
/// Nothing is recorded as delivered here — that happens in
/// [`mark_window_emitted`], when the frame actually reached the model.
async fn build_windowed(
    store: &crate::store::Store,
    snapshot: crate::context::Snapshot,
    batch_key: Option<&str>,
) -> Result<crate::context::BuiltContext> {
    let full = crate::context::build_from(snapshot.clone());
    let key = batch_key.unwrap_or(&full.sig);
    let epoch = match crate::store::live_identity_of(store).await? {
        Some((uid, generation)) => Some(crate::context::epoch_of(&uid, generation)),
        None => None,
    };
    let recorded_epoch = crate::store::kv_get(store, KEY_WINDOW_EPOCH, "").await?;
    let recorded_batch = crate::store::kv_get(store, KEY_WINDOW_BATCH, "").await?;
    let force = crate::store::kv_get(store, KEY_WINDOW_FORCE, "0").await? == "1";
    let due = crate::context::window_due(
        epoch.as_deref(),
        &recorded_epoch,
        &recorded_batch,
        force,
        key,
    );
    let session = epoch.as_deref().unwrap_or("unknown");
    if !due {
        let bare = crate::context::build_from(snapshot.with_window(false));
        dlog(&format!(
            "{key}: history window omitted (same session {session}, no compaction since)",
        ));
        return Ok(bare);
    }
    // This frame carries the window but has not been delivered yet: a rebuild of
    // the same batch (changed document, retry) keeps the window, and the
    // post-compaction flag is cleared only by this frame's proof.
    crate::store::kv_set(store, KEY_WINDOW_PENDING, key).await?;
    crate::store::kv_set(
        store,
        KEY_WINDOW_PENDING_FORCE,
        if force { "1" } else { "0" },
    )
    .await?;
    dlog(&format!(
        "history window: {key} carries the last {} messages (session {session})",
        crate::context::WINDOW_LIMIT
    ));
    Ok(full)
}

const KEY_WINDOW_EPOCH: &str = "context_window_epoch";
const KEY_WINDOW_BATCH: &str = "context_window_batch";
const KEY_WINDOW_FORCE: &str = "context_window_force";
const KEY_WINDOW_PENDING: &str = "context_window_pending";
const KEY_WINDOW_PENDING_FORCE: &str = "context_window_pending_force";

/// A compaction rewrote the transcript: the next turn must carry the window
/// even in the same session. Idempotent; cleared by the proof of the windowed
/// frame (see [`mark_window_emitted`]).
pub async fn force_window_next_turn(store: &crate::store::Store) -> Result<()> {
    crate::store::kv_set(store, KEY_WINDOW_FORCE, "1").await
}

/// The windowed frame reached the model: the epoch and the batch are now served,
/// so the following turns carry the batch only. A frame of another batch, or a
/// batch that carried no window, is a no-op. The post-compaction flag is dropped
/// only when THIS frame's window covered a compaction: a compaction that arrived
/// after the build still owes the next turn its window.
pub async fn mark_window_emitted(store: &crate::store::Store, batch_key: &str) -> Result<()> {
    if crate::store::kv_get(store, KEY_WINDOW_PENDING, "").await? != batch_key {
        return Ok(());
    }
    let Some((uid, generation)) = crate::store::live_identity_of(store).await? else {
        // Identity unknown: nothing to record against, so the window stays due.
        return Ok(());
    };
    let covered_compaction =
        crate::store::kv_get(store, KEY_WINDOW_PENDING_FORCE, "0").await? == "1";
    crate::store::kv_set(
        store,
        KEY_WINDOW_EPOCH,
        &crate::context::epoch_of(&uid, generation),
    )
    .await?;
    crate::store::kv_set(store, KEY_WINDOW_BATCH, batch_key).await?;
    crate::store::kv_set(store, KEY_WINDOW_PENDING, "").await?;
    crate::store::kv_set(store, KEY_WINDOW_PENDING_FORCE, "0").await?;
    if covered_compaction {
        crate::store::kv_set(store, KEY_WINDOW_FORCE, "0").await?;
    }
    dlog(&format!(
        "{batch_key}: history window delivered; the next turn carries the batch only"
    ));
    Ok(())
}

/// The windowed frame will never be delivered (the batch is retired): its window
/// is not recorded as served, so the next turn still gets one, but its
/// post-compaction flag no longer waits for a proof that cannot arrive.
pub async fn drop_window_pending(store: &crate::store::Store, batch_key: &str) -> Result<()> {
    if crate::store::kv_get(store, KEY_WINDOW_PENDING, "").await? != batch_key {
        return Ok(());
    }
    let covered_compaction =
        crate::store::kv_get(store, KEY_WINDOW_PENDING_FORCE, "0").await? == "1";
    crate::store::kv_set(store, KEY_WINDOW_PENDING, "").await?;
    crate::store::kv_set(store, KEY_WINDOW_PENDING_FORCE, "0").await?;
    if covered_compaction {
        crate::store::kv_set(store, KEY_WINDOW_FORCE, "0").await?;
    }
    Ok(())
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
    let cwd = cfg("CHATBOT_CWD", ".");
    let name = cfg("CHATBOT_SUMMARY_SESSION", "chat-summary");
    isolated_session_of(system_path, &name, &cwd, false).await
}

/// Open a session on a SCRATCH host and return `(durable_id, socket)`.
///
/// Contract: the launch-spec carries no chat-ext (the caller asserts the tool
/// list has no `chat_send`), `cwd` is the session's home — callers pass a
/// throwaway directory so `write`/`bash` cannot touch the bot's workspace —
/// and `phase_strip` decides whether the provider's `phase` marker transformer
/// is staged. Every later call must go through the returned socket: the
/// durable id does not exist on the chat host.
pub async fn isolated_session_of(
    system_path: &str,
    session_name: &str,
    cwd: &str,
    phase_strip: bool,
) -> Result<(String, String)> {
    // Use a scratch socket so we never touch the chat host. The socket is
    // threaded explicitly through every call — process-global routing
    // (CHATBOT_HOST_SOCKET) is never mutated, so later omo() calls in this
    // process still follow the chat host.
    let tmp = std::env::temp_dir().join(format!("chat-summary-host-{}.sock", std::process::id()));
    let sock = tmp.to_string_lossy().to_string();
    let state = state_dir();
    ensure_host_on_with(&state, false, phase_strip, &sock).await?;
    let r = session_name.to_string();
    // List sessions on the scratch host to close strays best-effort; each
    // close names a durable id, never a session name.
    let _ = session::close_all_sessions_on(&sock).await;
    let out = omo_on(
        &[
            "open",
            "--cwd",
            cwd,
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
pub async fn push_pending(
    store: &crate::store::Store,
    state: &str,
    sig: &str,
) -> Result<Option<String>> {
    regen_text(store, state, sig).await
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
    fn age_guard_boundary_and_mixed_batch() {
        let now = 1_700_000_000i64;
        let fresh = BatchMsg {
            message_id: 1,
            date: now - 10,
            user_id: Some(7),
            username: "u".into(),
            first_name: "U".into(),
            text: "fresh".into(),
            reply_to: None,
            media_path: String::new(),
            media_kind: String::new(),
            file_id: String::new(),
            thumb_path: String::new(),
            sender_chat: None,
            to_me: false,
        };
        let mut exact = fresh.clone();
        exact.message_id = 2;
        exact.date = now - MESSAGE_MAX_AGE_SECS;
        let mut stale = fresh.clone();
        stale.message_id = 3;
        stale.date = now - MESSAGE_MAX_AGE_SECS - 1;
        // Boundary: exactly 600s old is still fresh; 601s is stale.
        assert!(!is_stale_by_time(exact.date, now));
        assert!(is_stale_by_time(stale.date, now));
        // Unknown (0) dates prove nothing and are dropped; a future date is
        // fresh only inside the clock-skew window.
        assert!(is_stale_by_time(0, now));
        assert!(!is_stale_by_time(now + 299, now));
        assert!(is_stale_by_time(now + MESSAGE_FUTURE_SKEW_SECS + 1, now));
        // Mixed batch keeps only fresh rows and reports the dropped ids.
        let (kept, skipped) = apply_age_guard(vec![fresh, exact, stale], now);
        assert_eq!(kept.iter().map(|m| m.message_id).collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(skipped, vec![3]);
    }

    #[test]
    fn format_batch_shape() {
        let b = vec![BatchMsg {
            message_id: 1,
            date: 0,
            user_id: Some(2),
            username: "u".into(),
            first_name: "F".into(),
            text: "hi".into(),
            reply_to: None,
            media_path: "".into(),
            media_kind: "".into(),
            file_id: "".into(),
            thumb_path: String::new(),
            sender_chat: None,
            to_me: true,
        }];
        let s = crate::context::render_batch(&b, &HashMap::new());
        assert!(s.contains("[1] @u: [к тебе] hi"));
    }

    #[test]
    fn history_marks_own_channel_posts() {
        let self_id = 9_287_666_466i64;
        let rows: Vec<RecentRow> = vec![
            (
                Some(1),
                Some(777_000),
                Some("Telegram".into()),
                Some("Telegram".into()),
                Some("мой мем".into()),
                None,
                None,
                None,
                Some(crate::context::OWN_CHANNEL_ID),
            ),
            (
                Some(2),
                Some(7),
                Some("u".into()),
                Some("U".into()),
                Some("чужое".into()),
                None,
                None,
                None,
                None,
            ),
        ];
        let text = render_recent_context(
            &rows,
            &HashSet::new(),
            50,
            300,
            self_id,
            &HashSet::new(),
        );
        assert!(
            text.contains("[1] @Telegram [это написал ты сам]: мой мем"),
            "own channel post is not marked: {text}"
        );
        assert!(
            text.contains("[2] @u [фон чата]: чужое"),
            "ambient history is not marked: {text}"
        );
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
            header.contains("не цитируешь"),
            "batch header must forbid quoting: {header}"
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
        crate::store::mirror_own_send(&con, 7, "hello", None).unwrap();
        crate::store::mirror_own_send(&con, 7, "hello", None).unwrap(); // INSERT OR IGNORE
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
            // A fixture stands for a message that just arrived: the age guard is
            // fail-closed, so a zero date would be dropped here and every test
            // using this helper would silently measure the guard instead of its
            // own subject. The guard has dedicated tests.
            date: crate::ops::now_secs(),
            user_id: Some(7),
            username: "u".into(),
            first_name: "U".into(),
            text: text.into(),
            reply_to: None,
            media_path: "".into(),
            media_kind: "".into(),
            file_id: "".into(),
            thumb_path: String::new(),
            sender_chat: None,
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
        // Fresh dates: the age guard drops provably old rows, so a 1970
        // fixture would test the guard instead of the answered filter.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(1_700_000_000);
        con.execute(
            &format!("INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id) VALUES (1,100,{now},7,'u','U','старый вопрос',NULL,'','')"),
            [],
        )
        .unwrap();
        crate::store::mirror_own_send(&con, 200, "старый ответ", Some(100)).unwrap();
        con.execute(
            &format!("INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id) VALUES (2,101,{now},7,'u','U','новый вопрос',NULL,'','')"),
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
        let mut a = crate::store::answered_db(&con);
        a.extend(answered_receipts(&state));
        assert!(a.contains(&100), "mirror answer missing");
        assert!(a.contains(&101), "receipt answer missing");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn build_batch_filters_answered_rows_and_marks_context() {
        let dir = std::env::temp_dir().join(format!("chat-bf-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let state = dir.to_string_lossy().to_string();
        let _con = seed_qa(&state);
        let batch = vec![
            mk_batch_msg(100, "старый вопрос"),
            mk_batch_msg(101, "новый вопрос"),
        ];
        let store = crate::store::Store::open(&state).unwrap();
        let (text, _sig, ids, _tome, _anchor, _skipped) = build_batch_text(&store, &state, &batch).await.unwrap();
        assert_eq!(ids, vec![101]);
        assert!(
            text.contains("[100]") && text.contains("уже отвечено"),
            "got: {text}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn regen_drops_a_fully_answered_batch() {
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
        let store = crate::store::Store::open(&state).unwrap();
        let regen = regen_text(&store, &state, "s1").await.unwrap();
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
            thumb_file_id: String::new(),
            thumb_state: MediaState::Absent,
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

#[cfg(test)]
mod dedupe_channel_posts_tests {
    use super::*;

    fn msg(id: i64, user: Option<i64>, text: &str) -> BatchMsg {
        BatchMsg {
            message_id: id,
            date: 0,
            user_id: user,
            username: String::new(),
            first_name: String::new(),
            text: text.to_string(),
            reply_to: None,
            media_path: String::new(),
            media_kind: String::new(),
            file_id: String::new(),
            thumb_path: String::new(),
            sender_chat: None,
            to_me: false,
        }
    }

    #[test]
    fn the_channel_copy_goes_when_the_group_copy_is_here() {
        let batch = vec![
            msg(3610, Some(-1004421941006), "пост"),
            msg(14240, Some(777000), "пост"),
        ];
        let out = dedupe_channel_posts(batch);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].message_id, 14240);
    }

    #[test]
    fn different_posts_are_not_merged() {
        let batch = vec![
            msg(1, Some(-1004421941006), "первый"),
            msg(2, Some(777000), "второй"),
        ];
        assert_eq!(dedupe_channel_posts(batch).len(), 2);
    }

    #[test]
    fn a_lone_channel_post_survives() {
        let out = dedupe_channel_posts(vec![msg(9, Some(-100), "один")]);
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn a_group_copy_from_an_earlier_batch_suppresses_the_channel_copy() {
        let key = channel_post_identity(&msg(14240, Some(777000), "пост")).unwrap();
        let known = std::collections::HashSet::from([key]);
        let out = dedupe_channel_posts_known(
            vec![msg(3610, Some(-1004421941006), "пост")],
            &known,
        );
        assert!(out.is_empty(), "channel twin of a committed post was kept");
    }

    #[test]
    fn a_known_group_key_does_not_suppress_a_different_channel_post() {
        let key = channel_post_identity(&msg(14240, Some(777000), "пост")).unwrap();
        let known = std::collections::HashSet::from([key]);
        let out = dedupe_channel_posts_known(
            vec![msg(3611, Some(-1004421941006), "другой пост")],
            &known,
        );
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn known_group_post_keys_reads_only_fresh_group_copies() {
        let dir = std::env::temp_dir().join(format!(
            "chat-dedupe-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let state = dir.to_string_lossy().to_string();
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let con = crate::db(&state).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        con.execute(
            "INSERT INTO messages(chat_id,message_id,date,user_id,text,media_kind,file_id)
             VALUES(-100,1,?1,777000,'свежий','',''),
                   (-100,2,?1,-1004421941006,'канал','',''),
                   (-100,3,?2,777000,'старый','','')",
            rusqlite::params![now, now - MESSAGE_MAX_AGE_SECS - 1],
        )
        .unwrap();
        let keys = crate::store::known_group_post_keys(&con, now).unwrap();
        let fresh = channel_post_identity(&msg(1, Some(777000), "свежий")).unwrap();
        let channel = channel_post_identity(&msg(2, Some(-100), "канал")).unwrap();
        let old = channel_post_identity(&msg(3, Some(777000), "старый")).unwrap();
        assert!(keys.contains(&fresh));
        assert!(!keys.contains(&channel));
        assert!(!keys.contains(&old));
        drop(con);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
