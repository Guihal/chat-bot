//! Two-phase media ingest.
//!
//! A message row is written as soon as the update is committed, carrying only
//! a descriptor (`MediaState::Pending`) — never a downloaded path. A separate
//! fetcher unit downloads under [`MediaBudget`] and the ONLY transition to
//! `Ready` is [`crate::db::WriteTxn::finalize_blob`], which writes the path,
//! kind and size into `blobs` and links the row to that blob.
//!
//! Classification uses the Telegram message FIELD (`animation`, `video`, ...)
//! only. A file_id or an extension cannot tell a GIF from an MP4: Telegram
//! serves `.mp4` for both, so looking at bytes would mislabel videos as GIFs.

use crate::db::Db;
use crate::{api, api_base, token};
use anyhow::{anyhow, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::ffi::CString;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// DESIGN §8: `media/` ceiling and the free-space reserve that text may not
/// eat into. Below `REFUSE_FREE_BYTES` only text is accepted.
pub const MEDIA_CAP_BYTES: u64 = 110 * 1024 * 1024;
pub const FREE_RESERVE_BYTES: u64 = 200 * 1024 * 1024;
pub const REFUSE_FREE_BYTES: u64 = 150 * 1024 * 1024;

/// A download that outlives this is abandoned; the row stays pending and the
/// next resume pass retries it.
pub const FETCH_DEADLINE: Duration = Duration::from_secs(120);
/// Bounded fetch queue: a full queue leaves the job in `media_state='pending'`
/// instead of growing memory or blocking the ingest loop.
pub const MEDIA_QUEUE_CAP: usize = 64;
/// An unreferenced file must be at least this old before it is an orphan.
pub const ORPHAN_GRACE: Duration = Duration::from_secs(3600);
/// Rows older than this are removed by the cleanup pass (after their reply
/// targets stop referencing them and after one row per catalogue key is kept).
/// DESIGN §8 forbids a zero default: the disk fills under history, so the
/// shipped horizon is 30 days. `CHATBOT_ROW_RETENTION_DAYS=0` remains an
/// explicit operator override, never the default.
pub const ROW_RETENTION_DAYS: i64 = 30;

/// The single media state of a message. `Descriptor` means "we know the kind
/// and file_id, but this kind is marker-only and is never downloaded".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaState {
    Absent,
    Descriptor,
    Pending,
    Ready,
    Evicted,
}

impl MediaState {
    pub fn as_str(self) -> &'static str {
        match self {
            MediaState::Absent => "absent",
            MediaState::Descriptor => "descriptor",
            MediaState::Pending => "pending",
            MediaState::Ready => "ready",
            MediaState::Evicted => "evicted",
        }
    }

    pub fn parse(s: &str) -> MediaState {
        match s {
            "descriptor" => MediaState::Descriptor,
            "pending" => MediaState::Pending,
            "ready" => MediaState::Ready,
            "evicted" => MediaState::Evicted,
            _ => MediaState::Absent,
        }
    }
}

/// Media kinds as Telegram names them in the message object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaKind {
    Animation,
    Video,
    Photo,
    Sticker,
    Voice,
    Audio,
    VideoNote,
    Document,
}

impl MediaKind {
    pub fn as_str(self) -> &'static str {
        match self {
            MediaKind::Animation => "animation",
            MediaKind::Video => "video",
            MediaKind::Photo => "photo",
            MediaKind::Sticker => "sticker",
            MediaKind::Voice => "voice",
            MediaKind::Audio => "audio",
            MediaKind::VideoNote => "video_note",
            MediaKind::Document => "document",
        }
    }

    pub fn parse(s: &str) -> Option<MediaKind> {
        Some(match s {
            "animation" => MediaKind::Animation,
            "video" => MediaKind::Video,
            "photo" => MediaKind::Photo,
            "sticker" => MediaKind::Sticker,
            "voice" => MediaKind::Voice,
            "audio" => MediaKind::Audio,
            "video_note" => MediaKind::VideoNote,
            "document" => MediaKind::Document,
            _ => return None,
        })
    }

    /// Sticker, voice and audio are marker-only: they are re-sent or described
    /// by file_id, and downloading them would add bytes the agent cannot use.
    pub fn needs_download(self) -> bool {
        !matches!(
            self,
            MediaKind::Sticker | MediaKind::Voice | MediaKind::Audio
        )
    }

    /// Text marker shown to the model. A document keeps its `document` kind
    /// even when its MIME is `video/mp4`; the MIME is shown as a label, never
    /// used to reclassify it.
    pub fn marker(self, mime: &str, duration: Option<i64>) -> String {
        match self {
            MediaKind::Animation => "[gif]".to_string(),
            MediaKind::Video => "[video]".to_string(),
            MediaKind::Photo => "[photo]".to_string(),
            MediaKind::Sticker => "[sticker]".to_string(),
            MediaKind::VideoNote => "[video note]".to_string(),
            MediaKind::Voice | MediaKind::Audio => {
                let secs = duration
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "?".to_string());
                format!("[{} message, {secs} sec]", self.as_str())
            }
            MediaKind::Document => {
                if mime.is_empty() {
                    "[file]".to_string()
                } else {
                    format!("[file, {mime}]")
                }
            }
        }
    }
}

/// Read the media field of one Telegram message. Order is fixed and follows
/// the message object; the first field that carries a `file_id` wins.
pub fn media_kind_of(msg: &Value) -> Option<(MediaKind, Value)> {
    for (field, kind) in [
        ("photo", MediaKind::Photo),
        ("animation", MediaKind::Animation),
        ("video", MediaKind::Video),
        ("document", MediaKind::Document),
        ("sticker", MediaKind::Sticker),
        ("voice", MediaKind::Voice),
        ("audio", MediaKind::Audio),
        ("video_note", MediaKind::VideoNote),
    ] {
        if let Some(obj) = msg.get(field) {
            let o = if obj.is_array() {
                obj.as_array()
                    .and_then(|a| a.last().cloned())
                    .unwrap_or(json!(null))
            } else {
                obj.clone()
            };
            if o.get("file_id").and_then(|v| v.as_str()).is_some() {
                return Some((kind, o));
            }
        }
    }
    None
}

/// A GIF candidate is a READY animation. The catalog must not offer a video,
/// a document carrying video MIME, a sticker, or a row still downloading.
pub fn is_gif_candidate(state: MediaState, kind: &str, file_id: &str) -> bool {
    state == MediaState::Ready && kind == "animation" && !file_id.is_empty()
}

/// One durable download request. `row_id` is the surrogate message id, which
/// is also the finalize guard: a row that was evicted or deleted meanwhile
/// cannot become Ready.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaJob {
    pub row_id: i64,
    pub file_id: String,
    pub kind: String,
    pub expected_size: u64,
}

/// Byte budget derived from the real filesystem. Tests construct it directly
/// to simulate an exhausted disk without filling one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaBudget {
    pub free: u64,
    pub media_used: u64,
    pub media_cap: u64,
    pub reserve: u64,
}

impl MediaBudget {
    pub fn from_disk(state: &str) -> MediaBudget {
        let dir = media_dir(state);
        let _ = std::fs::create_dir_all(&dir);
        MediaBudget {
            free: free_bytes(Path::new(state)),
            media_used: dir_usage(&dir),
            media_cap: MEDIA_CAP_BYTES,
            reserve: FREE_RESERVE_BYTES,
        }
    }

    /// DESIGN §8: `free − size ≥ 200 MB` and `media + size ≤ 110 MB`.
    pub fn can_admit(&self, size: u64) -> bool {
        self.free >= size.saturating_add(self.reserve)
            && self.media_used.saturating_add(size) <= self.media_cap
    }

    /// Below the refusal threshold the daemon accepts text only.
    pub fn refuses_all(&self) -> bool {
        self.free < REFUSE_FREE_BYTES
    }
}

pub fn media_dir(state: &str) -> PathBuf {
    Path::new(state).join("media")
}

/// Sum of regular files directly inside `dir` (the media directory is flat).
pub fn dir_usage(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter_map(|e| e.metadata().ok())
        .filter(|m| m.is_file())
        .map(|m| m.len())
        .sum()
}

/// Free bytes available to this user. `0` on failure makes the budget refuse
/// downloads instead of guessing that the disk is empty.
pub fn free_bytes(path: &Path) -> u64 {
    let Ok(c) = CString::new(path.to_string_lossy().as_bytes()) else {
        return 0;
    };
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c.as_ptr(), &mut st) };
    if rc == 0 {
        (st.f_bavail as u64).saturating_mul(st.f_frsize as u64)
    } else {
        0
    }
}

/// One downloaded file, already in memory. The name is only a filesystem hint;
/// the kind always comes from the message object.
#[derive(Debug, Clone)]
pub struct FetchedFile {
    pub name: String,
    pub bytes: Vec<u8>,
}

/// The only I/O dependency of the fetcher, so a test can supply bytes without
/// a Bot API token or network.
pub trait MediaSource {
    fn fetch<'a>(
        &'a self,
        file_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<FetchedFile>> + 'a>>;
}

pub struct TelegramSource;

impl MediaSource for TelegramSource {
    fn fetch<'a>(
        &'a self,
        file_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<FetchedFile>> + 'a>> {
        Box::pin(async move {
            let info = api("getFile", &json!({"file_id": file_id})).await?;
            let rel = info
                .get("file_path")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("getFile: no file_path"))?;
            let name = Path::new(rel)
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| "file.bin".to_string());
            let tok = token()?;
            let url = format!("{}/file/bot{}/{}", api_base(), tok, rel);
            let client = reqwest::Client::builder().build()?;
            let bytes = client.get(&url).send().await?.bytes().await?;
            Ok(FetchedFile {
                name,
                bytes: bytes.to_vec(),
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchOutcome {
    /// Budget said no: nothing was fetched, the row stays pending.
    Deferred,
    Finalized {
        path: String,
        size: u64,
    },
    /// The row stopped being pending while the bytes were in flight.
    Skipped,
}

/// Fetch one job and finalize its blob. The budget is checked BEFORE the
/// network call and again on the real byte count; a refusal never writes a
/// file and never changes the row.
pub async fn fetch_job<S: MediaSource>(
    db: &Db,
    state: &str,
    job: &MediaJob,
    budget: &MediaBudget,
    source: &S,
) -> Result<FetchOutcome> {
    if budget.refuses_all() || !budget.can_admit(job.expected_size) {
        return Ok(FetchOutcome::Deferred);
    }
    let fetched = match tokio::time::timeout(FETCH_DEADLINE, source.fetch(&job.file_id)).await {
        Ok(r) => r?,
        Err(_) => return Err(anyhow!("media fetch timed out after {FETCH_DEADLINE:?}")),
    };
    let size = fetched.bytes.len() as u64;
    if !budget.can_admit(size) {
        return Ok(FetchOutcome::Deferred);
    }
    let dir = media_dir(state);
    std::fs::create_dir_all(&dir)?;
    let safe: String = Path::new(&fetched.name)
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "file.bin".to_string())
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let name = format!("{}_{}", job.row_id, safe);
    let final_path = dir.join(&name);
    let part_path = dir.join(format!(".part-{}", name));
    std::fs::write(&part_path, &fetched.bytes)?;
    std::fs::rename(&part_path, &final_path)?;
    let path = final_path.to_string_lossy().to_string();
    let row_id = job.row_id;
    let kind = job.kind.clone();
    let stored = path.clone();
    let finalized = db
        .write(move |tx| tx.finalize_blob(row_id, &stored, &kind, size))
        .await?;
    if !finalized {
        let _ = std::fs::remove_file(&path);
        return Ok(FetchOutcome::Skipped);
    }
    Ok(FetchOutcome::Finalized { path, size })
}

/// Unfinished downloads found in the database. Startup (and the maintenance
/// pass) enqueues these; a full queue simply leaves the rest pending.
pub fn resume_pending(con: &Connection, limit: usize) -> Result<Vec<MediaJob>> {
    let mut stmt = con.prepare(
        "SELECT id, file_id, media_kind, COALESCE(media_size,0)
           FROM messages
          WHERE media_state='pending' AND file_id IS NOT NULL AND file_id <> ''
          ORDER BY id LIMIT ?",
    )?;
    let rows = stmt.query_map([limit as i64], |r| {
        Ok(MediaJob {
            row_id: r.get(0)?,
            file_id: r.get(1)?,
            kind: r.get(2)?,
            expected_size: r.get::<_, i64>(3)?.max(0) as u64,
        })
    })?;
    Ok(rows.flatten().collect())
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CleanupReport {
    pub orphans_removed: usize,
    pub blobs_evicted: usize,
    pub rows_removed: usize,
}

fn file_mtime(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

/// One cleanup pass, fixed victim order:
///   1. orphans older than `grace` (files no row or blob references),
///   2. blobs outside the media budget (oldest first; their rows become
///      `Evicted` in the same transaction, so no live row references the file
///      when it is deleted),
///   3. old rows (never a row another row still replies to).
///
/// A file referenced by a live row is never deleted: step 1 checks the
/// reference set, step 2 clears the references before unlinking, and step 3
/// leaves a file alone while another row still points at it.
pub fn cleanup_media(
    con: &Connection,
    state: &str,
    budget: &MediaBudget,
    now: SystemTime,
    grace: Duration,
    retention_days: i64,
) -> Result<CleanupReport> {
    let tx = con.unchecked_transaction()?;
    let dir = media_dir(state);
    let mut report = CleanupReport::default();

    let mut referenced: HashSet<String> = HashSet::new();
    {
        let mut stmt =
            tx.prepare("SELECT path FROM blobs WHERE path IS NOT NULL AND path <> ''")?;
        for p in stmt.query_map([], |r| r.get::<_, String>(0))?.flatten() {
            referenced.insert(p);
        }
        let mut stmt = tx.prepare(
            "SELECT media_path FROM messages WHERE media_path IS NOT NULL AND media_path <> ''",
        )?;
        for p in stmt.query_map([], |r| r.get::<_, String>(0))?.flatten() {
            referenced.insert(p);
        }
    }

    // 1. Orphans after grace.
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for e in entries.flatten() {
            let path = e.path();
            if !path.is_file() {
                continue;
            }
            let key = path.to_string_lossy().to_string();
            if referenced.contains(&key) {
                continue;
            }
            let old = file_mtime(&path)
                .and_then(|m| now.duration_since(m).ok())
                .map(|d| d >= grace)
                .unwrap_or(true);
            if old && std::fs::remove_file(&path).is_ok() {
                report.orphans_removed += 1;
            }
        }
    }

    // 2. Blobs outside the budget. `dir_usage` is measured after step 1, so
    // freed orphan bytes count toward the budget immediately.
    let mut total = dir_usage(&dir);
    if total > budget.media_cap {
        let blobs: Vec<(i64, String, Option<i64>)> = {
            let mut stmt =
                tx.prepare("SELECT id, path, size FROM blobs ORDER BY created_at ASC, id ASC")?;
            let mut out = Vec::new();
            for row in stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))? {
                out.push(row?);
            }
            out
        };
        for (id, path, size) in blobs {
            if total <= budget.media_cap {
                break;
            }
            let sz = size
                .map(|s| s.max(0) as u64)
                .or_else(|| std::fs::metadata(&path).ok().map(|m| m.len()))
                .unwrap_or(0);
            // Clear every reference first: after this UPDATE no live row
            // points at the file, so unlinking cannot break one.
            tx.execute(
                "UPDATE messages SET media_state='evicted', blob_id=NULL, media_path=''
                  WHERE blob_id=? OR media_path=?",
                params![id, path],
            )?;
            tx.execute("DELETE FROM blobs WHERE id=?", [id])?;
            let _ = std::fs::remove_file(&path);
            total = total.saturating_sub(sz);
            report.blobs_evicted += 1;
        }
    }

    // 3. Old rows. A row that another row replies to stays alive (closure of
    // `reply_to` by message_id and chat, not by surrogate id), and a pending
    // download is never a victim. One row per catalogue key is kept so the GIF
    // registry never loses its only representative of a file.
    if retention_days > 0 {
        let now_s = now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
        let cutoff = now_s.saturating_sub(retention_days.saturating_mul(86_400));
        let ids: Vec<i64> = {
            let mut stmt = tx.prepare(
                "WITH keepers AS (
                   SELECT id FROM (
                     SELECT id, ROW_NUMBER() OVER (
                       PARTITION BY COALESCE(NULLIF(file_unique_id,''), file_id)
                       ORDER BY date DESC, message_id DESC) AS rn
                     FROM messages
                     WHERE media_kind='animation' AND media_state='ready'
                       AND file_id IS NOT NULL AND file_id <> ''
                   ) WHERE rn = 1
                 )
                 SELECT m.id FROM messages m
                  WHERE m.date < ? AND m.media_state <> 'pending'
                    AND NOT EXISTS (
                      SELECT 1 FROM messages c
                       WHERE c.reply_to = m.message_id AND c.chat_id = m.chat_id
                    )
                    AND m.id NOT IN (SELECT id FROM keepers)",
            )?;
            let mut out = Vec::new();
            for row in stmt.query_map([cutoff], |r| r.get(0))? {
                out.push(row?);
            }
            out
        };
        for id in ids {
            let linked: Option<(Option<i64>, Option<String>)> = tx
                .query_row(
                    "SELECT blob_id, media_path FROM messages WHERE id=?",
                    [id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            tx.execute("DELETE FROM messages WHERE id=?", [id])?;
            report.rows_removed += 1;
            if let Some((blob_id, Some(path))) = linked {
                let shared: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM messages WHERE blob_id=? OR media_path=?",
                    params![blob_id, path],
                    |r| r.get(0),
                )?;
                if shared == 0 {
                    if let Some(blob_id) = blob_id {
                        tx.execute("DELETE FROM blobs WHERE id=?", [blob_id])?;
                    }
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
    }

    tx.commit()?;
    Ok(report)
}

/// Maintenance cadence from DESIGN §8: never more often than every five
/// minutes, plus one pass at startup.
pub const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(300);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::WriteTxn;
    use crate::{db, envelope_from_message, kv_get, Envelope, MessageKey};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn temp_state(tag: &str) -> String {
        let dir = std::env::temp_dir().join(format!("chat-media-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir.to_string_lossy().to_string()
    }

    fn photo_update(chat: i64, message: i64, file_id: &str, size: u64) -> Value {
        json!({
            "chat": {"id": chat},
            "message_id": message,
            "date": chrono::Utc::now().timestamp(),
            "from": {"id": 7, "username": "u", "first_name": "U"},
            "photo": [{"file_id": file_id, "file_unique_id": format!("{file_id}-u"), "file_size": size}]
        })
    }

    fn env_of(msg: &Value, update_id: i64, chat: i64) -> Envelope {
        envelope_from_message(msg, update_id, false, chat)
            .unwrap()
            .expect("message must produce an envelope")
    }

    /// Required check 3: the same message under a new update_id is a duplicate
    /// (one row, explicit `duplicate`), and the cursor still advances.
    #[test]
    fn redelivery_with_new_update_id_is_duplicate() {
        let state = temp_state("dup");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let mut con = db(&state).unwrap();
        let msg = photo_update(-100, 42, "f42", 10);
        let first = env_of(&msg, 100, -100);
        let mut second = first.clone();
        second.update_id = 101;

        {
            let mut tx = WriteTxn::new(&mut con).unwrap();
            let page = tx.commit_page(vec![first], 101).unwrap();
            assert_eq!(page.applied.inserted, 1);
            assert_eq!(page.applied.duplicate, 0);
            tx.commit().unwrap();
        }
        let page = {
            let mut tx = WriteTxn::new(&mut con).unwrap();
            let page = tx.commit_page(vec![second], 102).unwrap();
            tx.commit().unwrap();
            page
        };
        assert_eq!(page.applied.inserted, 0, "redelivery wrote a second row");
        assert_eq!(page.applied.duplicate, 1, "redelivery was not a duplicate");
        let rows: i64 = con
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE chat_id=-100 AND message_id=42",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(rows, 1);
        assert_eq!(kv_get(&con, "offset", "0"), "102");
        let _ = std::fs::remove_dir_all(&state);
    }

    /// Required check 5: the message FIELD decides the kind. A document with
    /// `video/mp4` stays a document, and only the animation is a GIF candidate.
    #[tokio::test(flavor = "current_thread")]
    async fn animation_video_and_video_document_are_distinct() {
        let state = temp_state("kinds");
        std::fs::create_dir_all(media_dir(&state)).unwrap();
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let db = Db::open(&state).unwrap();
        let animation = env_of(
            &json!({
                "chat": {"id": -100}, "message_id": 1, "date": chrono::Utc::now().timestamp(),
                "animation": {"file_id": "anim", "file_unique_id": "anim-u", "file_size": 11, "mime_type": "video/mp4"}
            }),
            1,
            -100,
        );
        let video = env_of(
            &json!({
                "chat": {"id": -100}, "message_id": 2, "date": chrono::Utc::now().timestamp(),
                "video": {"file_id": "vid", "file_unique_id": "vid-u", "file_size": 12, "mime_type": "video/mp4"}
            }),
            2,
            -100,
        );
        let document = env_of(
            &json!({
                "chat": {"id": -100}, "message_id": 3, "date": chrono::Utc::now().timestamp(),
                "document": {"file_id": "doc", "file_unique_id": "doc-u", "file_size": 13, "mime_type": "video/mp4"}
            }),
            3,
            -100,
        );
        assert_eq!(animation.media_kind, "animation");
        assert_eq!(video.media_kind, "video");
        assert_eq!(document.media_kind, "document", "mime must not reclassify");
        assert!(animation.text.contains("[gif]"), "got: {}", animation.text);
        assert!(video.text.contains("[video]"), "got: {}", video.text);
        assert!(
            document.text.contains("[file, video/mp4]"),
            "document must be labelled by mime: {}",
            document.text
        );

        let kinds: Vec<(String, String, u64)> = [&animation, &video, &document]
            .iter()
            .map(|e| {
                (
                    e.media_kind.clone(),
                    e.file_id.clone(),
                    e.media_size.unwrap_or(0),
                )
            })
            .collect();
        let envelopes = vec![animation, video, document];
        let page = db
            .write(move |tx| tx.commit_page(envelopes, 4))
            .await
            .unwrap();
        assert_eq!(page.applied.inserted, 3);
        for (ins, (kind, _, size)) in page.inserted.iter().zip(kinds.iter()) {
            let path = media_dir(&state)
                .join(format!("{}.bin", ins.row_id))
                .to_string_lossy()
                .to_string();
            std::fs::write(&path, vec![0u8; *size as usize]).unwrap();
            let row = ins.row_id;
            let k = kind.clone();
            let p = path.clone();
            let sz = *size;
            let ok = db
                .write(move |tx| tx.finalize_blob(row, &p, &k, sz))
                .await
                .unwrap();
            assert!(ok, "row {row} did not finalize");
        }
        let gifs = db
            .call(|con| crate::catalog::page(con, 0, 20))
            .await
            .unwrap();
        assert_eq!(
            gifs.records.len(),
            1,
            "only the animation is a GIF: {gifs:?}"
        );
        assert_eq!(gifs.records[0].file_id(), "anim");
        assert!(is_gif_candidate(MediaState::Ready, "animation", "anim"));
        assert!(!is_gif_candidate(MediaState::Ready, "video", "vid"));
        assert!(!is_gif_candidate(MediaState::Ready, "document", "doc"));
        assert!(!is_gif_candidate(MediaState::Pending, "animation", "anim"));
        let _ = std::fs::remove_dir_all(&state);
    }

    struct FakeSource {
        calls: Arc<AtomicUsize>,
        bytes: Vec<u8>,
    }

    impl MediaSource for FakeSource {
        fn fetch(&self, _file_id: &str) -> Pin<Box<dyn Future<Output = Result<FetchedFile>> + '_>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let bytes = self.bytes.clone();
            Box::pin(async move {
                Ok(FetchedFile {
                    name: "fake.mp4".to_string(),
                    bytes,
                })
            })
        }
    }

    /// Required check 6: an exhausted budget fetches nothing and leaves the
    /// row pending; the resume pass finds it, and a later fetch finalizes it.
    #[tokio::test(flavor = "current_thread")]
    async fn exhausted_budget_defers_then_resume_completes() {
        let state = temp_state("budget");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let db = Db::open(&state).unwrap();
        let env = env_of(&photo_update(-100, 50, "big", 1000), 1, -100);
        assert_eq!(env.media_state, MediaState::Pending);
        let page = db
            .write(move |tx| tx.commit_page(vec![env], 2))
            .await
            .unwrap();
        let row_id = page.inserted[0].row_id;
        let job = MediaJob {
            row_id,
            file_id: "big".into(),
            kind: "photo".into(),
            expected_size: 1000,
        };

        let calls = Arc::new(AtomicUsize::new(0));
        let fake = FakeSource {
            calls: calls.clone(),
            bytes: vec![7u8; 1000],
        };
        let exhausted = MediaBudget {
            free: 10,
            media_used: MEDIA_CAP_BYTES,
            media_cap: MEDIA_CAP_BYTES,
            reserve: FREE_RESERVE_BYTES,
        };
        let out = fetch_job(&db, &state, &job, &exhausted, &fake)
            .await
            .unwrap();
        assert_eq!(out, FetchOutcome::Deferred);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "budget refusal fetched bytes"
        );
        let (state_db, files): (String, i64) = db
            .call(move |con| {
                let s: String = con.query_row(
                    "SELECT media_state FROM messages WHERE id=?",
                    [row_id],
                    |r| r.get(0),
                )?;
                Ok((
                    s,
                    con.query_row("SELECT COUNT(*) FROM blobs", [], |r| r.get(0))?,
                ))
            })
            .await
            .unwrap();
        assert_eq!(state_db, "pending", "row left its honest pending state");
        assert_eq!(files, 0, "a blob was written without a fetch");
        assert_eq!(dir_usage(&media_dir(&state)), 0, "a file was written");

        // Resume pass: the unfinished download is found again.
        let resumed = db
            .call(|con| resume_pending(con, MEDIA_QUEUE_CAP))
            .await
            .unwrap();
        assert_eq!(resumed.len(), 1);
        assert_eq!(resumed[0].row_id, row_id);

        let ample = MediaBudget {
            free: 10 * 1024 * 1024 * 1024,
            media_used: 0,
            media_cap: MEDIA_CAP_BYTES,
            reserve: FREE_RESERVE_BYTES,
        };
        let out = fetch_job(&db, &state, &resumed[0], &ample, &fake)
            .await
            .unwrap();
        match out {
            FetchOutcome::Finalized { path, size } => {
                assert_eq!(size, 1000);
                assert!(Path::new(&path).exists(), "finalized file is missing");
            }
            other => panic!("expected Finalized, got {other:?}"),
        }
        let (st, kind, size, blob_path): (String, String, i64, String) = db
            .call(move |con| {
                Ok(con.query_row(
                    "SELECT m.media_state, b.kind, b.size, b.path
                       FROM messages m JOIN blobs b ON b.id=m.blob_id WHERE m.id=?",
                    [row_id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )?)
            })
            .await
            .unwrap();
        assert_eq!((st.as_str(), kind.as_str(), size), ("ready", "photo", 1000));
        assert!(Path::new(&blob_path).exists());
        let _ = std::fs::remove_dir_all(&state);
    }

    /// Required check 7: orphans go, every file referenced by a live row stays.
    #[test]
    fn cleanup_removes_orphans_and_keeps_live_files() {
        let state = temp_state("clean");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let mut con = db(&state).unwrap();
        let dir = media_dir(&state);
        std::fs::create_dir_all(&dir).unwrap();
        let live = dir.join("live.bin");
        let orphan = dir.join("orphan.bin");
        std::fs::write(&live, vec![1u8; 128]).unwrap();
        std::fs::write(&orphan, vec![2u8; 256]).unwrap();

        let env = env_of(&photo_update(-100, 60, "live", 128), 1, -100);
        let page = {
            let mut tx = WriteTxn::new(&mut con).unwrap();
            let page = tx.commit_page(vec![env], 2).unwrap();
            tx.commit().unwrap();
            page
        };
        let row_id = page.inserted[0].row_id;
        {
            let mut tx = WriteTxn::new(&mut con).unwrap();
            let p = live.to_string_lossy().to_string();
            assert!(tx.finalize_blob(row_id, &p, "photo", 128).unwrap());
            tx.commit().unwrap();
        }

        let budget = MediaBudget {
            free: 10 * 1024 * 1024 * 1024,
            media_used: 384,
            media_cap: MEDIA_CAP_BYTES,
            reserve: FREE_RESERVE_BYTES,
        };
        // A future `now` makes both files older than a zero grace period.
        let now = SystemTime::now() + Duration::from_secs(60);
        let report = cleanup_media(&con, &state, &budget, now, Duration::ZERO, 100_000).unwrap();
        assert_eq!(report.orphans_removed, 1);
        assert!(!orphan.exists(), "orphan survived cleanup");
        assert!(
            live.exists(),
            "cleanup deleted a file referenced by a live row"
        );
        let st: String = con
            .query_row(
                "SELECT media_state FROM messages WHERE id=?",
                [row_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(st, "ready");
        let blobs: i64 = con
            .query_row("SELECT COUNT(*) FROM blobs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(blobs, 1);
        let _ = std::fs::remove_dir_all(&state);
    }

    /// A row deleted while its bytes are in flight cannot become Ready.
    #[tokio::test(flavor = "current_thread")]
    async fn finalize_of_a_vanished_row_is_refused() {
        let state = temp_state("vanish");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let db = Db::open(&state).unwrap();
        let ok = db
            .write(|tx| tx.finalize_blob(999, "/nonexistent", "photo", 1))
            .await
            .unwrap();
        assert!(!ok);
        let _ = std::fs::remove_dir_all(&state);
    }

    /// Uniqueness of `update_id` is retired: two distinct messages may carry
    /// the same legacy value without collapsing.
    #[test]
    fn legacy_update_id_is_not_unique() {
        let state = temp_state("legacy-uid");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let con = db(&state).unwrap();
        con.execute(
            "INSERT INTO messages(chat_id,update_id,message_id,date,text) VALUES(-100,7,1,1,'a')",
            [],
        )
        .unwrap();
        con.execute(
            "INSERT INTO messages(chat_id,update_id,message_id,date,text) VALUES(-100,7,2,1,'b')",
            [],
        )
        .unwrap();
        let n: i64 = con
            .query_row("SELECT COUNT(*) FROM messages", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 2);
        let _ = std::fs::remove_dir_all(&state);
    }

    #[test]
    fn marker_matrix_is_field_driven() {
        assert_eq!(MediaKind::Animation.marker("video/mp4", None), "[gif]");
        assert_eq!(MediaKind::Video.marker("video/mp4", None), "[video]");
        assert_eq!(
            MediaKind::Document.marker("video/mp4", None),
            "[file, video/mp4]"
        );
        assert_eq!(MediaKind::Document.marker("", None), "[file]");
        assert!(MediaKind::Sticker.needs_download() == false);
        assert!(MediaKind::Voice.needs_download() == false);
        assert!(MediaKind::Audio.needs_download() == false);
        assert!(MediaKind::Photo.needs_download());
        assert!(MediaKind::Document.needs_download());
    }

    #[test]
    fn stickers_and_voice_are_descriptor_only() {
        for (field, obj) in [
            ("sticker", json!({"file_id": "s1", "file_unique_id": "s1u"})),
            (
                "voice",
                json!({"file_id": "v1", "file_unique_id": "v1u", "duration": 3}),
            ),
            (
                "audio",
                json!({"file_id": "a1", "file_unique_id": "a1u", "duration": 9}),
            ),
        ] {
            let mut msg = json!({"chat": {"id": -1}, "message_id": 1, "date": 1});
            msg[field] = obj;
            let env = env_of(&msg, 1, -1);
            assert_eq!(env.media_state, MediaState::Descriptor, "{field}");
            assert_eq!(env.media_kind, field);
            assert!(env.media_path.is_empty(), "descriptor has no path");
        }
    }

    #[test]
    fn budget_equation() {
        let b = MediaBudget {
            free: FREE_RESERVE_BYTES + 100,
            media_used: MEDIA_CAP_BYTES - 100,
            media_cap: MEDIA_CAP_BYTES,
            reserve: FREE_RESERVE_BYTES,
        };
        assert!(b.can_admit(100));
        assert!(!b.can_admit(101), "media cap breached");
        let low = MediaBudget {
            free: FREE_RESERVE_BYTES + 99,
            media_used: 0,
            media_cap: MEDIA_CAP_BYTES,
            reserve: FREE_RESERVE_BYTES,
        };
        assert!(!low.can_admit(100), "reserve breached");
        let refuse = MediaBudget {
            free: REFUSE_FREE_BYTES - 1,
            media_used: 0,
            media_cap: MEDIA_CAP_BYTES,
            reserve: FREE_RESERVE_BYTES,
        };
        assert!(refuse.refuses_all());
    }

    #[test]
    fn message_key_shape_is_chat_plus_message() {
        let k = MessageKey {
            chat: -100,
            message: crate::MessageId(5),
        };
        assert_eq!(k.message.0, 5);
    }
}
