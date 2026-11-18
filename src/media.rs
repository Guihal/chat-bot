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

use crate::store::Store;
use crate::{api, api_base, token};
use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use std::ffi::CString;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Stdio;
use std::time::{Duration, SystemTime};
use tokio::io::AsyncWriteExt;

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
/// A file younger than this survives one wipe pass: the model may still be
/// reading a file delivered moments ago, and a download may still be landing.
pub const MEDIA_GRACE: Duration = Duration::from_secs(600);
/// A batch waits at most this long for its own media before it is handed to
/// the model. Longer than any healthy fetch, shorter than a chat pause.
pub const MEDIA_WAIT_SEC: u64 = 8;
/// Rows older than this are removed by the cleanup pass (after their reply
/// targets stop referencing them and after one row per catalogue key is kept).
/// DESIGN §8 forbids a zero default: the disk fills under history, so the
/// shipped horizon is 30 days. `CHATBOT_ROW_RETENTION_DAYS=0` remains an
/// explicit operator override, never the default.
pub const ROW_RETENTION_DAYS: i64 = 30;
/// A photo larger than this is resized before its path reaches the model.
/// The built-in `read` tool returns an image as base64 in the transcript: an
/// original chat photo produced a 3.36 MB toolResult and corrupted a long
/// live session. The bound is on the stored bytes, not on prose in the prompt.
pub const PHOTO_CONTEXT_LIMIT: u64 = 256 * 1024;
const PHOTO_MAX_EDGE: u16 = 1024;

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

/// A GIF candidate is an animation the catalogue can offer. The video itself
/// is NEVER fetched (a video in the context is a frame), so the record depends
/// on the identifiers and a known size, not on local bytes.
pub fn is_gif_candidate(state: MediaState, kind: &str, file_id: &str) -> bool {
    matches!(
        state,
        MediaState::Ready | MediaState::Descriptor | MediaState::Evicted
    ) && kind == "animation"
        && !file_id.is_empty()
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
    /// Budget said no: nothing was fetched. The row stays `pending`, and
    /// maintenance re-queues it on every pass until `MEDIA_GRACE` runs out;
    /// after that `cleanup_media` stops retrying it (see its doc).
    Deferred,
    Finalized {
        path: String,
        size: u64,
    },
    /// The row stopped being pending while the bytes were in flight.
    Skipped,
}

/// (max edge, ffmpeg `-q:v`) rungs, largest/fastest first. A noisy raster whose
/// first resize still exceeds the bound is stepped down through this fixed,
/// bounded ladder instead of being stored unbounded.
pub const PHOTO_BOUND_RUNGS: [(u16, u8); 7] = [
    (PHOTO_MAX_EDGE, 5),
    (768, 7),
    (512, 9),
    (384, 12),
    (256, 16),
    (128, 22),
    (64, 31),
];

/// Resize one raster photo through ffmpeg's pipe interface. A rung is accepted
/// only when it produced a real image that is both smaller than the input and
/// inside `PHOTO_CONTEXT_LIMIT`. `Ok(None)` means no rung fit: the caller must
/// fail closed and never store the original bytes as a model-readable blob.
async fn bounded_photo_bytes(bytes: Vec<u8>) -> Result<Option<Vec<u8>>> {
    for (edge, qv) in PHOTO_BOUND_RUNGS {
        let scale = format!("scale='min({edge},iw)':-2");
        let q = qv.to_string();
        let mut child = tokio::process::Command::new("ffmpeg")
            .args([
                "-y",
                "-v",
                "error",
                "-f",
                "image2pipe",
                "-i",
                "-",
                "-vf",
                &scale,
                "-q:v",
                &q,
                "-frames:v",
                "1",
                "-f",
                "image2pipe",
                "-vcodec",
                "mjpeg",
                "-",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        if let Some(mut stdin) = child.stdin.take() {
            if stdin.write_all(&bytes).await.is_err() {
                continue;
            }
            drop(stdin);
        }
        let out = match tokio::time::timeout(FETCH_DEADLINE, child.wait_with_output()).await {
            Ok(Ok(out)) => out,
            // A rung that fails or times out is skipped, never fatal: the next
            // (smaller) rung may still produce a bounded, decodable image.
            _ => continue,
        };
        let bounded = out.stdout;
        if out.status.success()
            && !bounded.is_empty()
            && (bounded.len() as u64) <= PHOTO_CONTEXT_LIMIT
            && bounded.len() < bytes.len()
        {
            return Ok(Some(bounded));
        }
    }
    Ok(None)
}

/// Fetch one job and finalize its blob. The budget is checked BEFORE the
/// network call and again on the real byte count; a refusal never writes a
/// file and never changes the row.
pub async fn fetch_job<S: MediaSource>(
    store: &Store,
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
    let mut fetched = fetched;
    let mut resized = false;
    if job.kind == "photo" && size > PHOTO_CONTEXT_LIMIT {
        match bounded_photo_bytes(std::mem::take(&mut fetched.bytes)).await {
            Ok(Some(bytes)) => {
                fetched.bytes = bytes;
                resized = true;
            }
            Ok(None) => {
                // No rung produced a decodable image inside the bound. Fail
                // closed: never finalize an oversized blob that would reach
                // the model through the read tool as a huge base64 result.
                // Leave the row pending so maintenance retries its budget;
                // the oversized bytes are dropped here, never written.
                crate::dlog(&format!(
                    "media row {} photo exceeded {PHOTO_CONTEXT_LIMIT} bytes and no resize rung fit; refusing blob",
                    job.row_id
                ));
                return Ok(FetchOutcome::Deferred);
            }
            Err(e) => {
                crate::dlog(&format!(
                    "media row {} photo bound failed: {e:#}; refusing blob",
                    job.row_id
                ));
                return Ok(FetchOutcome::Deferred);
            }
        }
    }
    let size = fetched.bytes.len() as u64;
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
    let safe = if resized {
        Path::new(&safe)
            .with_extension("jpg")
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| format!("{}.jpg", job.row_id))
    } else {
        safe
    };
    let name = format!("{}_{}", job.row_id, safe);
    let final_path = dir.join(&name);
    let part_path = dir.join(format!(".part-{}", name));
    std::fs::write(&part_path, &fetched.bytes)?;
    std::fs::rename(&part_path, &final_path)?;
    let path = final_path.to_string_lossy().to_string();
    let row_id = job.row_id;
    let kind = job.kind.clone();
    let stored = path.clone();
    let finalized = crate::store::finalize_blob_of(store, row_id, stored, kind, size).await?;
    if !finalized {
        let _ = std::fs::remove_file(&path);
        return Ok(FetchOutcome::Skipped);
    }
    Ok(FetchOutcome::Finalized { path, size })
}

/// One durable frame request for a video-like row. The frame is not a blob:
/// it is finalized through `thumb_state`, so the mp4 row can stay `descriptor`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThumbJob {
    pub row_id: i64,
    pub file_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThumbOutcome {
    Deferred,
    Finalized { path: String, size: u64 },
    Skipped,
}

/// Fetch one frame and finalize it. Frames are small, but the same budget and
/// the same deadline apply, so a full disk cannot be made worse by them.
pub async fn fetch_thumb<S: MediaSource>(
    store: &Store,
    state: &str,
    job: &ThumbJob,
    budget: &MediaBudget,
    source: &S,
) -> Result<ThumbOutcome> {
    if budget.refuses_all() {
        return Ok(ThumbOutcome::Deferred);
    }
    let fetched = match tokio::time::timeout(FETCH_DEADLINE, source.fetch(&job.file_id)).await {
        Ok(r) => r?,
        Err(_) => return Err(anyhow!("thumb fetch timed out after {FETCH_DEADLINE:?}")),
    };
    let size = fetched.bytes.len() as u64;
    if !budget.can_admit(size) {
        return Ok(ThumbOutcome::Deferred);
    }
    let dir = media_dir(state);
    std::fs::create_dir_all(&dir)?;
    let safe: String = Path::new(&fetched.name)
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "frame.jpg".to_string())
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let name = format!("{}_thumb_{}", job.row_id, safe);
    let final_path = dir.join(&name);
    let part_path = dir.join(format!(".part-{}", name));
    std::fs::write(&part_path, &fetched.bytes)?;
    std::fs::rename(&part_path, &final_path)?;
    let path = final_path.to_string_lossy().to_string();
    let row_id = job.row_id;
    let stored = path.clone();
    let finalized = crate::store::finalize_thumb_of(store, row_id, stored, size).await?;
    if !finalized {
        let _ = std::fs::remove_file(&path);
        return Ok(ThumbOutcome::Skipped);
    }
    Ok(ThumbOutcome::Finalized { path, size })
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CleanupReport {
    pub blobs_evicted: usize,
    pub rows_removed: usize,
    /// `pending` rows nobody was waiting for any more, moved to the kind's
    /// resting state: `descriptor` for the video-like kinds (their bytes were
    /// never meant to exist) and `evicted` for the downloadable ones (a photo
    /// or a document that never arrived has nothing to point at).
    pub pending_pruned: usize,
}

fn file_mtime(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

/// One cleanup pass. The media directory is wiped to zero, not trimmed to the
/// cap: trimming stops exactly at the ceiling, which leaves `can_admit` false
/// for anything at all. Two clauses guard admission, and each has a measured
/// outage behind it: `media_used + size <= MEDIA_CAP_BYTES` (trim-to-cap) and
/// `free >= size + FREE_RESERVE_BYTES` (host space). On 2026-11-06/07 photos
/// stopped arriving because of the SECOND one - free was 165-172 MB, all of it
/// outside this directory, while media/ was already empty - so a cleanup that
/// only trims the catalogue cannot rescue that case, and this doc must not
/// point the next reader at the cap alone. Order:
///   1. the data layer reads candidates (open batch, keep paths, old pending
///      rows, retention rows) in one actor job, with no file IO;
///   2. media chooses age victims by mtime/grace on this thread;
///   3. ONE write clears every reference and deletes the rows, returning the
///      paths to unlink; the unlink happens AFTER the commit, so no committed
///      row ever points at a missing file and an unreferenced file is simply a
///      victim on the next pass.
pub async fn cleanup_media(
    store: &Store,
    state: &str,
    _budget: &MediaBudget,
    now: SystemTime,
    grace: Duration,
    retention_days: i64,
) -> Result<CleanupReport> {
    let cand = crate::store::cleanup_candidates_of(store, now, grace, retention_days).await?;
    let dir = media_dir(state);
    let mut report = CleanupReport::default();

    // A row of a batch still being collected or answered may be read by the
    // model at any moment, so its file is never a victim. Everything else is,
    // once `grace` has passed.
    let mut victims: Vec<(String, u64)> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for e in entries.flatten() {
            let path = e.path();
            if !path.is_file() {
                continue;
            }
            let key = path.to_string_lossy().to_string();
            if cand.keep_paths.contains(&key) {
                continue;
            }
            let fresh = file_mtime(&path)
                .and_then(|m| now.duration_since(m).ok())
                .map(|d| d < grace)
                .unwrap_or(false);
            if fresh {
                continue;
            }
            let size = path.metadata().map(|m| m.len()).unwrap_or(0);
            victims.push((key, size));
        }
    }

    // A `pending` row of an open batch is never pruned: the batch waits for it.
    // Outside the open batch a pending row is only a retry slot: maintenance
    // re-queues it every pass and the same budget refusal answers it. Past the
    // grace below that would repeat forever against an unchanged disk, so the
    // row is moved to its resting state (`apply_cleanup` picks it by kind) and
    // the batch marker stays honest - `[photo]` with no path to read.
    let pending_ids: Vec<i64> = cand
        .pending
        .iter()
        .filter(|(_, mid, _)| {
            !mid.map(|m| cand.open_message_ids.contains(&m))
                .unwrap_or(false)
        })
        .map(|(id, _, _)| *id)
        .collect();

    let applied = crate::store::apply_cleanup_of(
        store,
        victims.clone(),
        pending_ids,
        cand.retention_ids,
    )
    .await?;
    report.rows_removed = applied.rows_removed;
    report.pending_pruned = applied.pending_pruned;
    for path in &applied.unlink {
        if std::fs::remove_file(path).is_ok() && victims.iter().any(|(v, _)| v == path) {
            report.blobs_evicted += 1;
        }
    }
    Ok(report)
}

/// Maintenance cadence from DESIGN §8: never more often than every five
/// minutes, plus one pass at startup.
pub const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(300);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{Db, WriteTxn};
    use rusqlite::params;
    use crate::store::Store;
    use crate::{db, envelope_from_message, kv_get, Envelope, MessageKey};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn catalog_page(con: &rusqlite::Connection, offset: i64, limit: i64) -> crate::catalog::GifPage {
        let (rows, total) = crate::store::catalog_page(con, offset, limit).unwrap();
        crate::catalog::page_of(rows, offset, total)
    }

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
        // A video and an animation never download their bytes (only a frame is
        // fetched, and only for the video-like kinds); the document still does.
        for (ins, (kind, _, size)) in page.inserted.iter().zip(kinds.iter()) {
            if kind == "video" || kind == "animation" {
                continue;
            }
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
            .call(|con| Ok(catalog_page(con, 0, 20)))
            .await
            .unwrap();
        assert_eq!(
            gifs.records.len(),
            1,
            "only the animation is a GIF: {gifs:?}"
        );
        assert_eq!(gifs.records[0].file_id(), "anim");
        assert!(is_gif_candidate(MediaState::Descriptor, "animation", "anim"));
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
        let store = Store::open(&state).unwrap();
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
        let out = fetch_job(&store, &state, &job, &exhausted, &fake)
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
            .call(|con| crate::store::pending_jobs(con, MEDIA_QUEUE_CAP))
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
        let out = fetch_job(&store, &state, &resumed[0], &ample, &fake)
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

    /// The wipe is the point: a file whose row was already delivered goes, and
    /// the row keeps its size while becoming `evicted`. An unreferenced file goes
    /// in the same pass.
    #[tokio::test(flavor = "current_thread")]
    async fn cleanup_wipes_delivered_files_and_orphans() {
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
        let store = Store::open(&state).unwrap();
        let report = cleanup_media(&store, &state, &budget, now, Duration::ZERO, 100_000)
            .await
            .unwrap();
        assert_eq!(report.blobs_evicted, 2, "both files must be wiped");
        assert!(!orphan.exists(), "unreferenced file survived the wipe");
        assert!(!live.exists(), "a delivered row kept its bytes");
        let (st, size, mp): (String, i64, String) = con
            .query_row(
                "SELECT media_state, COALESCE(media_size,0), COALESCE(media_path,'')
                   FROM messages WHERE id=?",
                [row_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            (st.as_str(), size, mp.as_str()),
            ("evicted", 128, ""),
            "the row lost its size or kept a dead path"
        );
        let blobs: i64 = con
            .query_row("SELECT COUNT(*) FROM blobs", [], |r| r.get(0))
            .unwrap();
        assert_eq!(blobs, 0, "a wiped blob is still registered");
        let _ = std::fs::remove_dir_all(&state);
    }

    /// A batch that is still being answered owns its files, and a file younger
    /// than `grace` is not a victim: the model may be reading it right now.
    #[tokio::test(flavor = "current_thread")]
    async fn cleanup_keeps_the_open_batch_and_fresh_files() {
        let state = temp_state("openbatch");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let mut con = db(&state).unwrap();
        let dir = media_dir(&state);
        std::fs::create_dir_all(&dir).unwrap();
        let in_batch = dir.join("in_batch.jpg");
        let fresh = dir.join("fresh.jpg");
        std::fs::write(&in_batch, vec![2u8; 1024]).unwrap();
        std::fs::write(&fresh, vec![3u8; 512]).unwrap();

        let env = env_of(&photo_update(-100, 61, "b", 1024), 1, -100);
        let page = {
            let mut tx = WriteTxn::new(&mut con).unwrap();
            let p = tx.commit_page(vec![env], 2).unwrap();
            tx.commit().unwrap();
            p
        };
        let row_id = page.inserted[0].row_id;
        {
            let mut tx = WriteTxn::new(&mut con).unwrap();
            let p = in_batch.to_string_lossy().to_string();
            assert!(tx.finalize_blob(row_id, &p, "photo", 1024).unwrap());
            tx.commit().unwrap();
        }
        con.execute(
            "INSERT INTO batches(batch_key,chat_id,state,sealed_at,updated_at,message_ids)
             VALUES('b1',-100,'open',0,0,'[61]')",
            [],
        )
        .unwrap();

        let budget = MediaBudget {
            free: 10 * 1024 * 1024 * 1024,
            media_used: 1536,
            media_cap: MEDIA_CAP_BYTES,
            reserve: FREE_RESERVE_BYTES,
        };
        // The batch file is old; the other file was written just now.
        let store = Store::open(&state).unwrap();
        let report = cleanup_media(
            &store,
            &state,
            &budget,
            SystemTime::now(),
            MEDIA_GRACE,
            100_000,
        )
        .await
        .unwrap();
        assert_eq!(report.blobs_evicted, 0);
        assert!(in_batch.exists(), "a file of an open batch was wiped");
        assert!(fresh.exists(), "a file inside grace was wiped");
        let st: String = con
            .query_row(
                "SELECT media_state FROM messages WHERE id=?",
                [row_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(st, "ready");
        let _ = std::fs::remove_dir_all(&state);
    }

    /// An obsolete `pending` row must not re-download what the wipe removed, and
    /// must not sit in front of fresh media. A row of an open batch stays.
    #[tokio::test(flavor = "current_thread")]
    async fn obsolete_pending_rows_are_pruned() {
        let state = temp_state("prune");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let mut con = db(&state).unwrap();
        let photo = env_of(&photo_update(-100, 70, "p", 100), 1, -100);
        let stale = env_of(&photo_update(-100, 71, "q", 200), 2, -100);
        let page = {
            let mut tx = WriteTxn::new(&mut con).unwrap();
            let p = tx.commit_page(vec![photo, stale], 3).unwrap();
            tx.commit().unwrap();
            p
        };
        assert_eq!(page.inserted.len(), 2);
        // 70 is still being answered, 71 is nobody's business any more.
        con.execute(
            "INSERT INTO batches(batch_key,chat_id,state,sealed_at,updated_at,message_ids)
             VALUES('b1',-100,'open',0,0,'[70]')",
            [],
        )
        .unwrap();
        let budget = MediaBudget {
            free: 10 * 1024 * 1024 * 1024,
            media_used: 0,
            media_cap: MEDIA_CAP_BYTES,
            reserve: FREE_RESERVE_BYTES,
        };
        // A future `now` puts both rows outside grace, so only the batch rule can
        // keep the live one.
        let store = Store::open(&state).unwrap();
        let report = cleanup_media(
            &store,
            &state,
            &budget,
            SystemTime::now() + Duration::from_secs(3600),
            MEDIA_GRACE,
            0,
        )
        .await
        .unwrap();
        assert_eq!(report.pending_pruned, 1);
        let live: String = con
            .query_row("SELECT media_state FROM messages WHERE message_id=70", [], |r| r.get(0))
            .unwrap();
        let stale_state: (String, String) = con
            .query_row(
                "SELECT media_state, media_path FROM messages WHERE message_id=71",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(live, "pending", "an open batch lost its download right");
        assert_eq!(stale_state.0, "evicted", "a stale row kept downloading");
        let jobs = crate::store::pending_jobs(&con, MEDIA_QUEUE_CAP).unwrap();
        assert_eq!(jobs.len(), 1, "the stale row would be fetched again");
        assert_eq!(jobs[0].row_id, page.inserted[0].row_id);
        let _ = std::fs::remove_dir_all(&state);
    }

    /// The catalogue is keyed on identifiers and a known size, never on bytes:
    /// after a full wipe `gif_list` must still offer the animations.
    #[tokio::test(flavor = "current_thread")]
    async fn catalogue_survives_a_full_wipe() {
        let state = temp_state("catalog-wipe");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let con = db(&state).unwrap();
        let dir = media_dir(&state);
        std::fs::create_dir_all(&dir).unwrap();
        let gif = dir.join("old.gif");
        std::fs::write(&gif, vec![5u8; 2048]).unwrap();
        con.execute(
            "INSERT INTO messages(chat_id,update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id,file_unique_id,media_kind,media_state,media_size)
             VALUES(-100,1,80,1800000000,7,'u','U','гифка',NULL,?,?,?,'animation','ready',2048)",
            params![gif.to_string_lossy(), "g80", "g80-u"],
        )
        .unwrap();
        let budget = MediaBudget {
            free: 10 * 1024 * 1024 * 1024,
            media_used: 2048,
            media_cap: MEDIA_CAP_BYTES,
            reserve: FREE_RESERVE_BYTES,
        };
        let before = catalog_page(&con, 0, 5);
        assert_eq!(before.records.len(), 1, "fixture is not in the catalogue");
        let now = SystemTime::now() + Duration::from_secs(3600);
        let store = Store::open(&state).unwrap();
        cleanup_media(&store, &state, &budget, now, MEDIA_GRACE, 0)
            .await
            .unwrap();
        assert!(!gif.exists());
        let after = catalog_page(&con, 0, 5);
        assert_eq!(
            after.records.len(),
            1,
            "the wipe emptied the GIF catalogue"
        );
        assert_eq!(after.records[0].file_id(), "g80");
        let _ = std::fs::remove_dir_all(&state);
    }

    /// The frame has its own state: the mp4 row stays a descriptor while the
    /// frame becomes readable.
    #[tokio::test(flavor = "current_thread")]
    async fn thumb_fetch_finalizes_only_a_pending_frame() {
        let state = temp_state("thumb");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let db = Db::open(&state).unwrap();
        let store = Store::open(&state).unwrap();
        let env = env_of(
            &json!({
                "chat": {"id": -100},
                "message_id": 90,
                "date": chrono::Utc::now().timestamp(),
                "from": {"id": 7, "username": "u", "first_name": "U"},
                "video": {"file_id": "v90", "file_unique_id": "v90-u", "file_size": 5000,
                          "thumbnail": {"file_id": "t90", "file_unique_id": "t90-u", "file_size": 800}}
            }),
            1,
            -100,
        );
        assert_eq!(env.media_state, MediaState::Descriptor);
        assert_eq!(env.thumb_state, MediaState::Pending);
        let page = db.write(move |tx| tx.commit_page(vec![env], 2)).await.unwrap();
        let row_id = page.inserted[0].row_id;
        let job = ThumbJob {
            row_id,
            file_id: "t90".into(),
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let fake = FakeSource {
            calls: calls.clone(),
            bytes: vec![9u8; 800],
        };
        let budget = MediaBudget {
            free: 10 * 1024 * 1024 * 1024,
            media_used: 0,
            media_cap: MEDIA_CAP_BYTES,
            reserve: FREE_RESERVE_BYTES,
        };
        let out = fetch_thumb(&store, &state, &job, &budget, &fake).await.unwrap();
        let path = match out {
            ThumbOutcome::Finalized { path, size } => {
                assert_eq!(size, 800);
                path
            }
            other => panic!("expected Finalized, got {other:?}"),
        };
        assert!(Path::new(&path).exists(), "the frame is missing on disk");
        let (st, size): (String, i64) = {
            let s = state.clone();
            let _ = s;
            db.call(move |con| {
                Ok(con.query_row(
                    "SELECT thumb_state, COALESCE(media_size,0) FROM messages WHERE id=?",
                    [row_id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?)
            })
            .await
            .unwrap()
        };
        assert_eq!(st, "ready");
        assert!(size > 0, "the catalogue lost the size of a frame-only row");
        let again = fetch_thumb(&store, &state, &job, &budget, &fake).await.unwrap();
        assert_eq!(again, ThumbOutcome::Skipped, "a frame was finalized twice");
        let pending = db
            .call(|con| crate::store::pending_thumbs(con, MEDIA_QUEUE_CAP))
            .await
            .unwrap();
        assert!(pending.is_empty());
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

    /// The live regression: a 1600x1600 noise PNG whose FIRST resize (edge
    /// 1024, `-q:v 5`) is ~447 KiB, well above `PHOTO_CONTEXT_LIMIT`. The
    /// bounded ladder must step down until the stored bytes are inside the
    /// bound, and the result must still be a decodable raster.
    #[tokio::test(flavor = "current_thread")]
    async fn noisy_photo_first_resize_above_bound_is_stepped_under_it() {
        let staged = std::process::Command::new("ffmpeg")
            .args([
                "-v", "error",
                "-f", "lavfi",
                "-i", "nullsrc=s=1600x1600,geq=r='random(1)*255':g='random(2)*255':b='random(3)*255'",
                "-frames:v", "1",
                "-f", "image2pipe",
                "-vcodec", "png",
                "-",
            ])
            .output()
            .expect("ffmpeg to stage noise");
        assert!(staged.status.success(), "ffmpeg: {}", String::from_utf8_lossy(&staged.stderr));
        let source = staged.stdout;
        assert!(
            (source.len() as u64) > PHOTO_CONTEXT_LIMIT,
            "fixture must itself be oversized"
        );

        // Prove the first rung alone would breach the bound, so this test
        // genuinely exercises the stepping rather than passing on rung one.
        // Prove the first rung alone would breach the bound via a file input,
        // independent of the async ladder.
        let dir = temp_state("bound-first-rung");
        let in_path = std::path::Path::new(&dir).join("noise.png");
        std::fs::write(&in_path, &source).unwrap();
        let first = std::process::Command::new("ffmpeg")
            .args([
                "-y", "-v", "error", "-i", in_path.to_str().unwrap(),
                "-vf", "scale='min(1024,iw)':-2", "-q:v", "5",
                "-frames:v", "1", "-f", "image2pipe", "-vcodec", "mjpeg", "-",
            ])
            .output()
            .expect("ffmpeg first rung");
        assert!(first.status.success(), "first rung failed");
        assert!(
            (first.stdout.len() as u64) > PHOTO_CONTEXT_LIMIT,
            "first rung {} B must exceed the {PHOTO_CONTEXT_LIMIT} B bound",
            first.stdout.len()
        );

        let bounded = bounded_photo_bytes(source)
            .await
            .expect("ladder ok")
            .expect("a bounded rung must fit");
        assert!(
            (bounded.len() as u64) <= PHOTO_CONTEXT_LIMIT,
            "stored {} B exceeds bound",
            bounded.len()
        );
        // Decodable raster, not an arbitrary truncated byte string.
        let decode = std::process::Command::new("ffmpeg")
            .args([
                "-v", "error", "-f", "image2pipe", "-i", "-",
                "-frames:v", "1", "-f", "null", "-",
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("ffmpeg decode");
        let mut child = decode;
        use std::io::Write;
        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(&bounded).unwrap();
            drop(stdin);
        }
        let out = child.wait_with_output().expect("decode wait");
        assert!(out.status.success(), "bounded bytes are not a decodable image");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
