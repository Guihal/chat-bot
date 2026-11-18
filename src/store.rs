//! The data layer: the ONLY module that reads or writes the database on behalf
//! of business code.
//!
//! Contract:
//! * business modules never see a `rusqlite::Connection` and never contain SQL;
//! * every access from async code is one typed [`Store`] method that runs the
//!   statement inside the single `chatdb` actor ([`crate::db::Db`]);
//! * an actor job does data work only — no file IO, no HTTP, no alarm, no
//!   sleep. A caller that needs such a side effect receives plain data and
//!   performs it after the actor returns (and, for writes, after commit).
//!
//! Each operation comes in two shapes: a free function over a synchronous
//! connection (used by the CLIs, the benchmark and tests) and an async `Store`
//! method that wraps exactly one `Db::call`/`Db::write` around it.
use crate::db::{Db, WriteTxn};
use crate::ledger::{self, OpenAnswer, Receipt, SendIntent, SendOutcome, SendUnknown};
use crate::context::ReplyParent;
use crate::{BatchMsg, PreparedMessage, RecentRow, StoredRow};
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Handle to the data layer. Cheap to clone; clones share the actor queue.
#[derive(Clone)]
pub struct Store {
    db: Db,
}

impl Store {
    pub fn open(state: &str) -> Result<Store> {
        Ok(Store {
            db: Db::open(state)?,
        })
    }

    /// The actor itself, for the few life cycle paths (shutdown, reopen) that
    /// predate this layer. Business reads/writes must use the typed methods.
    pub fn actor(&self) -> &Db {
        &self.db
    }
}

// ---------- kv ----------

pub async fn kv_get(store: &Store, key: &str, default: &str) -> Result<String> {
    let k = key.to_string();
    let d = default.to_string();
    store.db.call(move |con| Ok(crate::kv_get(con, &k, &d))).await
}

/// One kv write through the actor. The value has no SQL in it, so this stays a
/// thin pass-through to the data layer.
pub async fn kv_set(store: &Store, key: &str, value: &str) -> Result<()> {
    let k = key.to_string();
    let v = value.to_string();
    store.db.call(move |con| crate::kv_set(con, &k, &v)).await
}

// ---------- durable channel-twin hold ----------
//
// A `channel_post` copy is held in chatd's in-memory grace window so the
// automatic group forward can cancel it. That window is lost on a restart,
// which is exactly when the held copy could otherwise be admitted as its own
// batch. These helpers persist the hold in `kv` (the data layer's key/value
// owner) so ingest can rebuild it on boot. The key is the message id (unique),
// the value is `identity \t first_seen_ms`.
const CHAN_HOLD_PREFIX: &str = "chanhold:";

pub fn note_held_channel(
    con: &Connection,
    message_id: i64,
    identity: &str,
    first_seen_ms: i64,
) -> Result<()> {
    let key = format!("{CHAN_HOLD_PREFIX}{message_id}");
    let value = format!("{identity}\t{first_seen_ms}");
    crate::kv_set(con, &key, &value)
}

pub fn clear_held_channel(con: &Connection, message_id: i64) -> Result<()> {
    let key = format!("{CHAN_HOLD_PREFIX}{message_id}");
    con.execute("DELETE FROM kv WHERE k=?1", [key])?;
    Ok(())
}

/// Clear every held twin matching a group copy's identity. Telegram's forward
/// carries the same (text, kind, file_id) as the channel post, so this cancels
/// the durable hold without needing the original message id at the group side.
pub fn clear_held_channel_identity(con: &Connection, identity: &str) -> Result<usize> {
    let prefix = format!("{CHAN_HOLD_PREFIX}%");
    let mut stmt = con.prepare_cached("SELECT k, v FROM kv WHERE k LIKE ?1")?;
    let keys: Vec<String> = stmt
        .query_map([prefix], |r| {
            let k: String = r.get(0)?;
            let v: String = r.get(1)?;
            Ok((k, v))
        })?
        .filter_map(Result::ok)
        .filter(|(_, v)| v.split('\t').next() == Some(identity))
        .map(|(k, _)| k)
        .collect();
    let mut cleared = 0;
    for key in keys {
        cleared += con.execute("DELETE FROM kv WHERE k=?1", [key])?;
    }
    Ok(cleared)
}

/// All durable holds: (message_id, identity, first_seen_ms).
pub fn held_channels(con: &Connection) -> Result<Vec<(i64, String, i64)>> {
    let prefix = format!("{CHAN_HOLD_PREFIX}%");
    let mut stmt = con.prepare_cached("SELECT k, v FROM kv WHERE k LIKE ?1")?;
    let rows = stmt.query_map([prefix], |r| {
        let k: String = r.get(0)?;
        let v: String = r.get(1)?;
        Ok((k, v))
    })?;
    let mut out = vec![];
    for row in rows {
        let (k, v) = row?;
        let Some(id_str) = k.strip_prefix(CHAN_HOLD_PREFIX) else {
            continue;
        };
        let Ok(message_id) = id_str.parse::<i64>() else {
            continue;
        };
        let mut parts = v.split('\t');
        let identity = parts.next().unwrap_or("").to_string();
        let first_seen_ms = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
        out.push((message_id, identity, first_seen_ms));
    }
    Ok(out)
}

pub async fn note_held_channel_of(
    store: &Store,
    message_id: i64,
    identity: &str,
    first_seen_ms: i64,
) -> Result<()> {
    let identity = identity.to_string();
    store
        .db
        .call(move |con| note_held_channel(con, message_id, &identity, first_seen_ms))
        .await
}

pub async fn clear_held_channel_of(store: &Store, message_id: i64) -> Result<()> {
    store
        .db
        .call(move |con| clear_held_channel(con, message_id))
        .await
}

pub async fn clear_held_channel_identity_of(store: &Store, identity: &str) -> Result<usize> {
    let identity = identity.to_string();
    store
        .db
        .call(move |con| clear_held_channel_identity(con, &identity))
        .await
}

pub async fn held_channels_of(store: &Store) -> Result<Vec<(i64, String, i64)>> {
    store.db.call(held_channels).await
}

/// Chat id of the mirror (0 when the database was never seeded).
pub async fn chat_id(store: &Store) -> Result<i64> {
    Ok(kv_get(store, "chat_id", "0")
        .await?
        .parse()
        .unwrap_or(0))
}

// ---------- messages ----------

/// Identity check + INSERT for one prepared message, atomically on the
/// connection. Media stays `pending` for the fetcher.
pub fn stored_row(con: &Connection, prep: &PreparedMessage) -> Result<StoredRow> {
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
    let chat: i64 = crate::kv_get(con, "chat_id", "0").parse().unwrap_or(0);
    // The identity is (chat_id, message_id). `update_id` is only the stream
    // cursor, so the same message redelivered under a new cursor is a
    // duplicate here too.
    if let Some(mid) = message_id {
        let exists: Option<i64> = con
            .prepare_cached("SELECT 1 FROM messages WHERE chat_id=? AND message_id=? LIMIT 1")?
            .query_row(rusqlite::params![chat, mid], |r| r.get(0))
            .ok();
        if exists.is_some() {
            return Ok(StoredRow {
                applied: crate::Applied {
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
        rusqlite::params![chat, update_id, message_id, thread_id, date, user_id, username, first_name, text, reply_to, media_path, file_id, file_unique_id, media_kind, media_state.as_str(), media_size.map(|s| s as i64), source_chat, sender_chat],
    )?;
    if changed == 0 {
        return Ok(StoredRow {
            applied: crate::Applied {
                duplicate: 1,
                ..Default::default()
            },
            row: None,
        });
    }
    Ok(StoredRow {
        applied: crate::Applied {
            inserted: 1,
            ..Default::default()
        },
        row: Some(BatchMsg {
            message_id: message_id.unwrap_or(0),
            date,
            user_id,
            username,
            first_name,
            text,
            reply_to,
            media_path,
            file_id,
            media_kind,
            thumb_path: String::new(),
            to_me: false,
        }),
    })
}

pub async fn store_prepared(store: &Store, prep: PreparedMessage) -> Result<StoredRow> {
    store.db.call(move |con| stored_row(con, &prep)).await
}

/// The newest `n` messages, newest first.
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

pub async fn recent_rows_of(store: &Store, n: i64) -> Result<Vec<RecentRow>> {
    store.db.call(move |con| recent_rows(con, n)).await
}

/// One non-empty row of a knowledge run's day window: `(message_id, date,
/// username, first_name, text)`, oldest first.
pub type WindowRow = (i64, i64, Option<String>, Option<String>, Option<String>);

/// Rows of `[from, to)` with a non-empty text, after the `(date, message_id)`
/// cursor. The cursor is deliberately NOT `update_id`: our own replies are
/// mirrored with a negative `update_id` (`-message_id`), so paging by it would
/// skip them or reorder the day. `ORDER BY date, message_id` is the mirror's
/// chronological order and the same tuple the cursor advances on.
pub fn knowledge_window(
    con: &Connection,
    from: i64,
    to: i64,
    after_date: i64,
    after_id: i64,
    limit: i64,
) -> Result<Vec<WindowRow>> {
    let mut stmt = con.prepare_cached(
        "SELECT message_id,date,username,first_name,text FROM messages \
         WHERE date>=?1 AND date<?2 AND text IS NOT NULL AND text<>'' \
         AND (date>?3 OR (date=?3 AND message_id>?4)) \
         ORDER BY date, message_id LIMIT ?5",
    )?;
    let rows: Vec<WindowRow> = stmt
        .query_map(
            rusqlite::params![from, to, after_date, after_id, limit],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )?
        .flatten()
        .collect();
    Ok(rows)
}

pub async fn knowledge_window_of(
    store: &Store,
    from: i64,
    to: i64,
    after_date: i64,
    after_id: i64,
    limit: i64,
) -> Result<Vec<WindowRow>> {
    store
        .db
        .call(move |con| knowledge_window(con, from, to, after_date, after_id, limit))
        .await
}

/// INSERT OR IGNORE the mirror row of one own send.
pub fn mirror_own_send(
    con: &Connection,
    message_id: i64,
    text: &str,
    reply_to: Option<i64>,
) -> Result<()> {
    let self_id: i64 = crate::kv_get(con, "self_id", "0").parse().unwrap_or(0);
    if self_id == 0 {
        // Best-effort: without self id we cannot attribute; skip.
        return Ok(());
    }
    let username = crate::kv_get(con, "self_username", "");
    let first = crate::kv_get(con, "self_first", "");
    let now = chrono::Utc::now().timestamp();
    let chat: i64 = crate::kv_get(con, "chat_id", "0").parse().unwrap_or(0);
    let _ = con.execute(
        "INSERT OR IGNORE INTO messages(chat_id,update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        rusqlite::params![chat, -message_id, message_id, now, self_id, username, first, text, reply_to, "", ""],
    )?;
    Ok(())
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

pub async fn mirror_receipts_of(store: &Store, receipts: Vec<Value>) -> Result<()> {
    store
        .db
        .call(move |con| mirror_receipts(con, &receipts))
        .await
}

/// The DB half of the answered set: journal closure (closed batches' uploaded
/// ids) plus the reply targets of our own sends. The caller unions the events
/// journal half — that read is file IO and must not run inside the actor.
pub fn answered_db(con: &Connection) -> HashSet<i64> {
    let self_id: i64 = crate::kv_get(con, "self_id", "0").parse().unwrap_or(0);
    let mut out = HashSet::new();
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

pub async fn answered_db_of(store: &Store) -> Result<HashSet<i64>> {
    store.db.call(|con| Ok(answered_db(con))).await
}

/// Answer guard: the questions our own recent sends quoted, with the answer
/// text, so a memory-less session does not answer them again.
pub fn answered_guard(con: &Connection, limit: i64) -> Result<String> {
    let self_id: i64 = crate::kv_get(con, "self_id", "0").parse().unwrap_or(0);
    if self_id == 0 {
        return Ok(String::new());
    }
    let mut stmt = con.prepare_cached(
        "SELECT reply_to, substr(text, 1, 120) FROM messages WHERE user_id=? AND reply_to IS NOT NULL ORDER BY date DESC LIMIT ?",
    )?;
    let rows: Vec<(Option<i64>, Option<String>)> = stmt
        .query_map(rusqlite::params![self_id, limit], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })?
        .flatten()
        .collect();
    let mut lines = vec![];
    for (qid, ans) in rows {
        let Some(qid) = qid else { continue };
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
    Ok("Отвечено ранее (не отвечай повторно):\n".to_string() + &lines.join("\n"))
}

pub async fn answered_guard_of(store: &Store, limit: i64) -> Result<String> {
    store
        .db
        .call(move |con| answered_guard(con, limit))
        .await
}

/// Batch rows for the given message ids, in the order of `ids`.
pub fn batch_rows_by_ids(con: &Connection, ids: &[Value]) -> Result<Vec<BatchMsg>> {
    let mut out = vec![];
    for mid in ids {
        let mid_int: Option<i64> = mid
            .as_i64()
            .or_else(|| mid.as_str().and_then(|s| s.parse().ok()));
        let Some(mid_int) = mid_int else { continue };
        let row: Option<(
            Option<i64>,
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
                "SELECT message_id,date,user_id,username,first_name,text,reply_to,media_path,media_kind,file_id FROM messages WHERE message_id=? LIMIT 1",
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
                        r.get(9)?,
                    ))
                })
            })
            .ok();
        if let Some((m, d, u, un, f, t, r, mp, kind, fid)) = row {
            out.push(BatchMsg {
                message_id: m.unwrap_or(mid_int),
                date: d.unwrap_or(0),
                user_id: u,
                username: un.unwrap_or_default(),
                first_name: f.unwrap_or_default(),
                text: t.unwrap_or_default(),
                reply_to: r,
                media_path: mp.unwrap_or_default(),
                media_kind: kind.unwrap_or_default(),
                file_id: fid.unwrap_or_default(),
                thumb_path: String::new(),
                to_me: false,
            });
        }
    }
    Ok(out)
}

/// Identities of group copies already committed in earlier batches. A
/// linked channel post reaches ingest twice, and the copies can straddle two
/// flushes; this query lets the admit unit keep the group copy only.
pub fn known_group_post_keys(con: &Connection, now_secs: i64) -> Result<HashSet<String>> {
    let earliest = now_secs - crate::MESSAGE_MAX_AGE_SECS;
    let latest = now_secs + crate::MESSAGE_FUTURE_SKEW_SECS;
    let mut stmt = con.prepare_cached(
        "SELECT TRIM(COALESCE(text,'')), COALESCE(media_kind,''), COALESCE(file_id,'')
           FROM messages
          WHERE user_id > 0 AND date BETWEEN ?1 AND ?2
            AND (COALESCE(text,'') <> '' OR COALESCE(file_id,'') <> '')",
    )?;
    let rows = stmt.query_map([earliest, latest], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
        ))
    })?;
    let mut out = HashSet::new();
    for row in rows {
        let (text, kind, file) = row?;
        out.insert(format!("{text}\u{1}{kind}\u{1}{file}"));
    }
    Ok(out)
}

pub async fn known_group_post_keys_of(store: &Store) -> Result<HashSet<String>> {
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    store
        .db
        .call(move |con| known_group_post_keys(con, now_secs))
        .await
}

pub async fn batch_rows_of(store: &Store, ids: Vec<Value>) -> Result<Vec<BatchMsg>> {
    store
        .db
        .call(move |con| batch_rows_by_ids(con, &ids))
        .await
}

/// Thread of the first message that carries one. Delivering a batch must use
/// the topic the question came from.
pub fn batch_thread(con: &Connection, ids: &[i64]) -> Option<i64> {
    for id in ids {
        if let Ok(t) = con.query_row(
            "SELECT thread_id FROM messages WHERE message_id=? LIMIT 1",
            [id],
            |r| r.get::<_, Option<i64>>(0),
        ) {
            if t.is_some() {
                return t;
            }
        }
    }
    None
}

pub async fn batch_thread_of(store: &Store, ids: Vec<i64>) -> Result<Option<i64>> {
    store
        .db
        .call(move |con| Ok(batch_thread(con, &ids)))
        .await
}

/// Re-read the mutable media projection of every batch row. Identity, author,
/// text and the reply edge stay as the batch recorded them; only the fields
/// the media pipeline changes are taken from the mirror, and a frame is
/// offered only while it is readable.
pub fn hydrate(con: &Connection, batch: &[BatchMsg]) -> Vec<BatchMsg> {
    let Ok(mut stmt) = con.prepare_cached(
        "SELECT COALESCE(media_path,''), COALESCE(thumb_path,''), COALESCE(thumb_state,'absent')
           FROM messages WHERE message_id=? LIMIT 1",
    ) else {
        return batch.to_vec();
    };
    let mut out = Vec::with_capacity(batch.len());
    for m in batch {
        let mut row = m.clone();
        if let Ok((media_path, thumb_path, thumb_state)) = stmt.query_row([m.message_id], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        }) {
            row.media_path = media_path;
            row.thumb_path = if thumb_state == "ready" {
                thumb_path
            } else {
                String::new()
            };
        }
        out.push(row);
    }
    out
}

/// The nearest reply parent of every distinct reply target in the batch. A
/// miss is absent from the map and becomes an explicit `Missing` edge.
pub fn reply_parents(con: &Connection, batch: &[BatchMsg]) -> Result<HashMap<i64, ReplyParent>> {
    let mut ids: Vec<i64> = batch.iter().filter_map(|m| m.reply_to).collect();
    ids.sort_unstable();
    ids.dedup();
    let mut out = HashMap::new();
    for id in ids {
        let row: Option<(Option<String>, Option<String>, Option<i64>, Option<String>, Option<String>)> = con
            .prepare_cached(
                "SELECT username,first_name,user_id,text,media_path FROM messages WHERE message_id=? LIMIT 1",
            )
            .and_then(|mut stmt| {
                stmt.query_row([id], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
                })
            })
            .ok();
        if let Some((username, first_name, user_id, text, media)) = row {
            out.insert(
                id,
                crate::context::ReplyParent {
                    who: crate::context::display_who(
                        &username.unwrap_or_default(),
                        &first_name.unwrap_or_default(),
                        user_id,
                    ),
                    text: text.unwrap_or_default(),
                    media: !media.unwrap_or_default().is_empty(),
                },
            );
        }
    }
    Ok(out)
}

// ---------- catalogue ----------

/// The deduplicated animation page. `media_size` is the Telegram size; when
/// the mirror never learned it, the blob size stands in. The `COUNT(*) OVER ()`
/// total rides on the page; an empty page falls back to the same statement
/// with `LIMIT -1 OFFSET 0`, so offset-past-the-end is not reported as an
/// empty catalogue.
pub const GIF_PAGE_SQL: &str = "WITH grouped AS (
           SELECT file_unique_id AS dedup_key,
                  file_id, file_unique_id, text, date, message_id, reply_to,
                  media_kind, media_state,
                  COALESCE(media_size,
                           (SELECT size FROM blobs WHERE blobs.id = messages.blob_id),
                           0) AS size,
                  ROW_NUMBER() OVER (
                    PARTITION BY file_unique_id
                    ORDER BY date DESC, message_id DESC) AS rn
           FROM messages
           WHERE media_kind='animation'
             AND media_state IN ('ready','descriptor','evicted')
             AND file_id IS NOT NULL AND file_id <> ''
             AND file_unique_id IS NOT NULL AND file_unique_id <> ''
             AND COALESCE(media_size,
                          (SELECT size FROM blobs WHERE blobs.id = messages.blob_id),
                          0) > 0
         ),
         page AS (
           SELECT file_id, file_unique_id, text, size, media_kind, media_state,
                  date, message_id, reply_to, COUNT(*) OVER () AS total
           FROM grouped WHERE rn = 1
           ORDER BY date DESC, message_id DESC LIMIT ? OFFSET ?
         )
         SELECT file_id, file_unique_id, text, size, media_kind, media_state, total,
                COALESCE(
                  (SELECT NULLIF(TRIM(substr(m2.text,1,120)),'')
                     FROM messages m2 WHERE m2.message_id = page.reply_to LIMIT 1),
                  (SELECT NULLIF(TRIM(substr(m3.text,1,120)),'')
                     FROM messages m3
                    WHERE (m3.date, m3.message_id) < (page.date, page.message_id)
                      AND m3.text IS NOT NULL AND TRIM(m3.text) <> ''
                    ORDER BY m3.date DESC, m3.message_id DESC LIMIT 1),
                  ''
                ) AS context
         FROM page ORDER BY date DESC, message_id DESC";

/// One catalogue page plus its total.
pub fn catalog_page(
    con: &Connection,
    offset: i64,
    limit: i64,
) -> Result<(Vec<crate::catalog::GifRow>, i64)> {
    let mut stmt = con.prepare_cached(GIF_PAGE_SQL)?;
    let rows: Vec<(
        String,
        Option<String>,
        Option<String>,
        i64,
        Option<String>,
        Option<String>,
        i64,
        Option<String>,
    )> = stmt
        .query_map(rusqlite::params![limit, offset], |r| {
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
    let total = match rows.first() {
        Some(r) => r.6,
        None => catalog_total(con)?,
    };
    let rows = rows
        .into_iter()
        .map(
            |(file_id, file_unique_id, text, size, kind, state, _, context)| {
                crate::catalog::GifRow {
                    file_id,
                    file_unique_id: file_unique_id.unwrap_or_default(),
                    text,
                    size,
                    media_kind: kind,
                    media_state: state,
                    context,
                }
            },
        )
        .collect();
    Ok((rows, total))
}

/// Total of the deduplicated catalogue through the one page statement itself.
pub fn catalog_total(con: &Connection) -> Result<i64> {
    let sql = format!("SELECT COUNT(*) FROM ({GIF_PAGE_SQL})");
    let mut stmt = con.prepare_cached(&sql)?;
    let total: i64 = stmt.query_row(rusqlite::params![-1i64, 0i64], |r| r.get(0))?;
    Ok(total)
}

pub async fn catalog_page_of(
    store: &Store,
    offset: i64,
    limit: i64,
) -> Result<(Vec<crate::catalog::GifRow>, i64)> {
    store
        .db
        .call(move |con| catalog_page(con, offset, limit))
        .await
}

// ---------- ingest / journal primitives ----------

pub async fn commit_page(
    store: &Store,
    envelopes: Vec<crate::Envelope>,
    through_update: i64,
) -> Result<crate::AppliedPage> {
    store
        .db
        .write(move |tx| tx.commit_page(envelopes, through_update))
        .await
}

// ---------- sessions ----------

pub fn record_live(con: &Connection, name: &str, uid: &str) -> Result<i64> {
    let current: Option<(Option<String>, Option<i64>)> = con
        .query_row(
            "SELECT session_uid, generation FROM sessions WHERE name=?",
            [name],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let now = chrono::Utc::now().timestamp();
    let generation = match current {
        Some((Some(old), Some(g))) if old == uid => g,
        Some((_, Some(g))) => g.saturating_add(1),
        _ => 1,
    };
    con.execute(
        "INSERT INTO sessions(name,session_uid,generation,state,updated_at) VALUES(?,?,?,'live',?)
         ON CONFLICT(name) DO UPDATE SET
           session_uid=excluded.session_uid,
           generation=excluded.generation,
           state='live',
           updated_at=excluded.updated_at",
        rusqlite::params![name, uid, generation, now],
    )?;
    Ok(generation)
}

pub fn mark_dead(con: &Connection, uid: &str) -> Result<()> {
    con.execute(
        "UPDATE sessions SET state='dead', updated_at=? WHERE session_uid=?",
        rusqlite::params![chrono::Utc::now().timestamp(), uid],
    )?;
    Ok(())
}

pub fn live_identity(con: &Connection) -> Result<Option<(String, i64)>> {
    let row: Option<(String, Option<i64>)> = con
        .query_row(
            "SELECT session_uid, generation FROM sessions WHERE state='live' ORDER BY generation DESC, id DESC LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    Ok(row.and_then(|(uid, generation)| {
        if uid.trim().is_empty() {
            None
        } else {
            Some((uid, generation.unwrap_or(0)))
        }
    }))
}

pub async fn record_live_of(store: &Store, name: String, uid: String) -> Result<i64> {
    store
        .db
        .write(move |tx| record_live(tx.handle(), &name, &uid))
        .await
}

pub async fn mark_dead_of(store: &Store, uid: String) -> Result<()> {
    store
        .db
        .write(move |tx| mark_dead(tx.handle(), &uid))
        .await
}

pub async fn live_identity_of(store: &Store) -> Result<Option<(String, i64)>> {
    store.db.call(|con| live_identity(con)).await
}

// ---------- media ----------

/// Unfinished downloads. A full fetch queue simply leaves the rest pending.
pub fn pending_jobs(con: &Connection, limit: usize) -> Result<Vec<crate::media::MediaJob>> {
    let mut stmt = con.prepare(
        "SELECT id, file_id, media_kind, COALESCE(media_size,0)
           FROM messages
          WHERE media_state='pending' AND file_id IS NOT NULL AND file_id <> ''
          ORDER BY id LIMIT ?",
    )?;
    let rows = stmt.query_map([limit as i64], |r| {
        Ok(crate::media::MediaJob {
            row_id: r.get(0)?,
            file_id: r.get(1)?,
            kind: r.get(2)?,
            expected_size: r.get::<_, i64>(3)?.max(0) as u64,
        })
    })?;
    Ok(rows.flatten().collect())
}

pub async fn pending_jobs_of(store: &Store, limit: usize) -> Result<Vec<crate::media::MediaJob>> {
    store
        .db
        .call(move |con| pending_jobs(con, limit))
        .await
}

/// Unfinished video frames; a full queue leaves the rest.
pub fn pending_thumbs(con: &Connection, limit: usize) -> Result<Vec<crate::media::ThumbJob>> {
    let mut stmt = con.prepare(
        "SELECT id, thumb_file_id FROM messages
          WHERE thumb_state='pending' AND thumb_file_id IS NOT NULL AND thumb_file_id <> ''
          ORDER BY id LIMIT ?",
    )?;
    let rows = stmt.query_map([limit as i64], |r| {
        Ok(crate::media::ThumbJob {
            row_id: r.get(0)?,
            file_id: r.get(1)?,
        })
    })?;
    Ok(rows.flatten().collect())
}

pub async fn pending_thumbs_of(
    store: &Store,
    limit: usize,
) -> Result<Vec<crate::media::ThumbJob>> {
    store
        .db
        .call(move |con| pending_thumbs(con, limit))
        .await
}

/// Message ids of every batch that is not delivered yet. `message_ids` holds
/// `messages.message_id` values, not surrogate ids.
pub fn open_batch_message_ids(con: &Connection) -> Result<HashSet<i64>> {
    let mut stmt = con.prepare(
        "SELECT message_ids FROM batches
          WHERE state IN ('open','queued','claimed') AND message_ids IS NOT NULL",
    )?;
    let mut out = HashSet::new();
    for raw in stmt.query_map([], |r| r.get::<_, String>(0))?.flatten() {
        if let Ok(ids) = serde_json::from_str::<Vec<i64>>(&raw) {
            out.extend(ids);
        }
    }
    Ok(out)
}

/// True while one of these fresh messages still waits for its own file/frame.
pub fn any_pending(con: &Connection, message_ids: &[i64]) -> Result<bool> {
    let cutoff = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .saturating_sub(crate::media::MEDIA_GRACE.as_secs()) as i64;
    let mut stmt = con.prepare_cached(
        "SELECT 1 FROM messages
          WHERE message_id=? AND date >= ?
            AND (media_state='pending' OR thumb_state='pending') LIMIT 1",
    )?;
    for id in message_ids {
        if stmt
            .query_row(rusqlite::params![id, cutoff], |r| r.get::<_, i64>(0))
            .optional()?
            .is_some()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub async fn any_pending_of(store: &Store, message_ids: Vec<i64>) -> Result<bool> {
    store
        .db
        .call(move |con| any_pending(con, &message_ids))
        .await
}

/// Victim selection inputs for one cleanup pass, read in one actor job. No IO
/// here: the caller inspects the media directory, chooses by mtime/grace and
/// unlinks only after [`apply_cleanup`] committed.
#[derive(Debug, Default)]
pub struct CleanupCandidates {
    pub open_message_ids: HashSet<i64>,
    pub keep_paths: HashSet<String>,
    /// `pending` rows older than the grace window: (row id, message id, kind).
    pub pending: Vec<(i64, Option<i64>, String)>,
    /// Rows past the retention horizon that no reply and no catalogue keeper
    /// protects.
    pub retention_ids: Vec<i64>,
}

pub fn cleanup_candidates(
    con: &Connection,
    now: SystemTime,
    grace: Duration,
    retention_days: i64,
) -> Result<CleanupCandidates> {
    let open = open_batch_message_ids(con)?;
    let keep_paths = keep_paths(con, &open)?;
    let cutoff = now
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .saturating_sub(grace.as_secs()) as i64;
    let pending = {
        let mut stmt = con.prepare(
            "SELECT id, message_id, COALESCE(media_kind,'') FROM messages
              WHERE media_state='pending' AND date < ?",
        )?;
        let rows: Vec<(i64, Option<i64>, String)> = stmt
            .query_map([cutoff], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .flatten()
            .collect();
        rows
    };
    let retention_ids = if retention_days > 0 {
        let now_s = now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
        let cutoff = now_s.saturating_sub(retention_days.saturating_mul(86_400));
        let mut stmt = con.prepare(
            "WITH keepers AS (
               SELECT id FROM (
                 SELECT id, ROW_NUMBER() OVER (
                   PARTITION BY COALESCE(NULLIF(file_unique_id,''), file_id)
                   ORDER BY date DESC, message_id DESC) AS rn
                 FROM messages
                 WHERE media_kind='animation'
                   AND media_state IN ('ready','descriptor','evicted')
                   AND file_id IS NOT NULL AND file_id <> ''
                   AND COALESCE(media_size,0) > 0
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
    } else {
        Vec::new()
    };
    Ok(CleanupCandidates {
        open_message_ids: open,
        keep_paths,
        pending,
        retention_ids,
    })
}


fn keep_paths(con: &Connection, open: &HashSet<i64>) -> Result<HashSet<String>> {
    let mut keep = HashSet::new();
    if open.is_empty() {
        return Ok(keep);
    }
    let mut stmt = con.prepare(
        "SELECT message_id, COALESCE(media_path,''), COALESCE(thumb_path,'')
           FROM messages
          WHERE (media_path IS NOT NULL AND media_path <> '')
             OR (thumb_path IS NOT NULL AND thumb_path <> '')",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, Option<i64>>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
        ))
    })?;
    for (mid, mp, tp) in rows.flatten() {
        if mid.map(|m| open.contains(&m)).unwrap_or(false) {
            if !mp.is_empty() {
                keep.insert(mp);
            }
            if !tp.is_empty() {
                keep.insert(tp);
            }
        }
    }
    Ok(keep)
}

/// Result of one cleanup write: row counters plus every path the caller must
/// unlink after the commit.
#[derive(Debug, Default)]
pub struct CleanupApply {
    pub rows_removed: usize,
    pub pending_pruned: usize,
    pub unlink: Vec<String>,
}

/// Apply the caller's decisions in ONE write transaction. References are
/// cleared before the caller unlinks, so no committed row points at a file
/// that is about to disappear.
pub fn apply_cleanup(
    tx: &mut WriteTxn,
    victims: &[(String, u64)],
    pending_ids: &[i64],
    retention_ids: &[i64],
) -> Result<CleanupApply> {
    let mut report = CleanupApply::default();
    for (path, size) in victims {
        // The size is the catalogue's only remaining evidence of the file.
        tx.handle().execute(
            "UPDATE messages SET media_size=?
              WHERE media_size IS NULL AND (media_path=? OR thumb_path=?)",
            rusqlite::params![*size as i64, path, path],
        )?;
        tx.handle().execute(
            "UPDATE messages SET media_state='evicted', blob_id=NULL, media_path=''
              WHERE media_path=?",
            [path],
        )?;
        tx.handle().execute(
            "UPDATE messages SET thumb_state='evicted', thumb_path='' WHERE thumb_path=?",
            [path],
        )?;
        tx.handle()
            .execute("DELETE FROM blobs WHERE path=?", [path])?;
        report.unlink.push(path.clone());
    }
    for id in pending_ids {
        let target: Option<String> = tx
            .handle()
            .query_row(
                "SELECT COALESCE(media_kind,'') FROM messages WHERE id=?",
                [id],
                |r| r.get(0),
            )
            .optional()?;
        let target = if matches!(
            target.as_deref().unwrap_or(""),
            "video" | "animation" | "video_note"
        ) {
            "descriptor"
        } else {
            // Nothing was ever downloaded, so there is no file to have lost:
            // `evicted` is the only state that keeps `media_path` empty while
            // the row keeps its message. Retrying past the grace would hammer
            // the same refusing budget on every maintenance pass.
            "evicted"
        };
        let changed = tx.handle().execute(
            "UPDATE messages SET media_state=? WHERE id=? AND media_state='pending'",
            rusqlite::params![target, id],
        )?;
        report.pending_pruned += changed;
    }
    for id in retention_ids {
        let linked: Option<(Option<i64>, Option<String>)> = tx
            .handle()
            .query_row(
                "SELECT blob_id, media_path FROM messages WHERE id=?",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        tx.handle()
            .execute("DELETE FROM messages WHERE id=?", [id])?;
        report.rows_removed += 1;
        if let Some((blob_id, Some(path))) = linked {
            let shared: i64 = tx.handle().query_row(
                "SELECT COUNT(*) FROM messages WHERE blob_id=? OR media_path=?",
                rusqlite::params![blob_id, path],
                |r| r.get(0),
            )?;
            if shared == 0 {
                if let Some(blob_id) = blob_id {
                    tx.handle()
                        .execute("DELETE FROM blobs WHERE id=?", [blob_id])?;
                }
                report.unlink.push(path);
            }
        }
    }
    Ok(report)
}

// ---------- send ----------

/// `file_unique_id` our mirror knows for a file_id (empty when unknown).
pub fn unique_id_for(con: &Connection, file_id: &str) -> String {
    con.query_row(
        "SELECT COALESCE(file_unique_id,'') FROM messages WHERE file_id=? AND COALESCE(file_unique_id,'')<>'' LIMIT 1",
        [file_id],
        |r| r.get(0),
    )
    .unwrap_or_default()
}

pub async fn unique_id_for_of(store: &Store, file_id: String) -> Result<String> {
    store
        .db
        .call(move |con| Ok(unique_id_for(con, &file_id)))
        .await
}

/// One send part, as data. Request-JSON shaping belongs to the ledger.
#[derive(Debug, Clone)]
pub struct PartMeta {
    pub kind: String,
    pub text: String,
    pub file_id: String,
    pub file_unique_id: String,
    pub media_path: String,
}

/// Outcome of opening the answer: a replay of the stored response, or a fresh
/// set of durable send intents minted in the same transaction.
#[derive(Debug, Clone)]
pub enum BeginSend {
    Replay { response: Option<String> },
    Opened(Vec<SendIntent>),
}

pub fn open_answer_with_sends(
    tx: &mut WriteTxn,
    key: &str,
    fingerprint: &str,
    chat: i64,
    thread: Option<i64>,
    reply_to: Option<i64>,
    parts: &[PartMeta],
) -> Result<BeginSend> {
    match tx.open_answer(key, fingerprint)? {
        OpenAnswer::Replay { response, .. } => Ok(BeginSend::Replay { response }),
        OpenAnswer::Opened(answer) => {
            let mut intents = Vec::new();
            for (i, part) in parts.iter().enumerate() {
                let request = ledger::send_request(
                    chat,
                    &part.text,
                    reply_to,
                    &part.file_id,
                    &part.file_unique_id,
                    &part.media_path,
                    thread,
                );
                intents.push(tx.begin_send(answer, i, &part.kind, &request)?);
            }
            Ok(BeginSend::Opened(intents))
        }
    }
}

pub async fn open_answer_with_sends_of(
    store: &Store,
    key: String,
    fingerprint: String,
    chat: i64,
    thread: Option<i64>,
    reply_to: Option<i64>,
    parts: Vec<PartMeta>,
) -> Result<BeginSend> {
    store
        .db
        .write(move |tx| {
            open_answer_with_sends(
                tx,
                &key,
                &fingerprint,
                chat,
                thread,
                reply_to,
                &parts,
            )
        })
        .await
}

pub async fn start_send_of(store: &Store, intent: SendIntent) -> Result<()> {
    store
        .db
        .write(move |tx| tx.start_send(&intent))
        .await
}

pub async fn finish_send_of(
    store: &Store,
    intent: SendIntent,
    outcome: SendOutcome,
    complete: Option<String>,
) -> Result<Receipt> {
    store
        .db
        .write(move |tx| tx.finish_send(&intent, &outcome, complete.as_deref()))
        .await
}

pub async fn resolve_sending_of(store: &Store) -> Result<usize> {
    store
        .db
        .write(|tx| ledger::resolve_sending(tx.handle()))
        .await
}

/// Downcast helper: a failed send surfaces as [`SendUnknown`] through the
/// actor's erased error. The caller fires the alarm, never the actor.
pub fn send_unknown_detail(err: &anyhow::Error) -> Option<String> {
    err.downcast_ref::<SendUnknown>().map(|e| e.detail.clone())
}

// ---------- delivery batch ----------

pub async fn seal_and_push(
    store: &Store,
    sig: String,
    chat: i64,
    thread: Option<i64>,
    ids: Vec<i64>,
    tome: Vec<i64>,
) -> Result<()> {
    store
        .db
        .write(move |tx| {
            tx.seal_batch(&sig, chat, thread, &ids, &tome)?;
            tx.mark_pushed(&sig)
        })
        .await
}

pub async fn admit_batch_of(store: &Store, sig: String, kind: String) -> Result<()> {
    store
        .db
        .write(move |tx| tx.admit_batch(&sig, &kind))
        .await
}

pub async fn mark_settled_of(store: &Store, sig: String, at: i64) -> Result<()> {
    store
        .db
        .write(move |tx| tx.mark_settled(&sig, at))
        .await
}

pub async fn fail_attempt_of(store: &Store, sig: String) -> Result<()> {
    store
        .db
        .write(move |tx| tx.fail_attempt(&sig))
        .await
}

// ---------- batch context ----------

pub async fn message_ids_of(store: &Store, sig: String) -> Result<Vec<i64>> {
    store
        .db
        .call(move |con| crate::ledger::message_ids(con, &sig))
        .await
}

pub async fn batch_view_of(
    store: &Store,
    sig: String,
) -> Result<Option<crate::ledger::BatchView>> {
    store
        .db
        .call(move |con| crate::ledger::view(con, &sig))
        .await
}

/// The whole context snapshot in ONE actor job: answered filter, media
/// re-projection, recent window, self id, reply parents, prompt catalogue and
/// answer guard. The answered set is computed by the caller (it needs the
/// events file, which is IO); every read here is sqlite only.
pub async fn context_snapshot(
    store: &Store,
    batch: Vec<BatchMsg>,
    answered: HashSet<i64>,
) -> Result<crate::context::Snapshot> {
    store
        .db
        .call(move |con| {
            let batch: Vec<BatchMsg> = batch
                .into_iter()
                .filter(|m| !answered.contains(&m.message_id))
                .collect();
            let batch = hydrate(con, &batch);
            let batch_ids: HashSet<i64> = batch.iter().map(|m| m.message_id).collect();
            let window = recent_rows(
                con,
                crate::context::WINDOW_LIMIT
                    + batch_ids.len() as i64
                    + crate::context::WINDOW_SLACK,
            )?;
            let self_id: i64 = crate::kv_get(con, "self_id", "0").parse().unwrap_or(0);
            let parents = reply_parents(con, &batch)?;
            let (rows, _total) = catalog_page(con, 0, crate::catalog::PROMPT_MAX)?;
            let catalog = crate::catalog::records_from(rows);
            let guard = answered_guard(con, crate::context::GUARD_LIMIT)?;
            Ok(crate::context::load(
                batch, answered, window, parents, catalog, guard, self_id,
            ))
        })
        .await
}

pub async fn cleanup_candidates_of(
    store: &Store,
    now: SystemTime,
    grace: Duration,
    retention_days: i64,
) -> Result<CleanupCandidates> {
    store
        .db
        .call(move |con| cleanup_candidates(con, now, grace, retention_days))
        .await
}

pub async fn apply_cleanup_of(
    store: &Store,
    victims: Vec<(String, u64)>,
    pending_ids: Vec<i64>,
    retention_ids: Vec<i64>,
) -> Result<CleanupApply> {
    store
        .db
        .write(move |tx| apply_cleanup(tx, &victims, &pending_ids, &retention_ids))
        .await
}

pub async fn finalize_blob_of(
    store: &Store,
    row_id: i64,
    path: String,
    kind: String,
    size: u64,
) -> Result<bool> {
    store
        .db
        .write(move |tx| tx.finalize_blob(row_id, &path, &kind, size))
        .await
}

pub async fn finalize_thumb_of(
    store: &Store,
    row_id: i64,
    path: String,
    size: u64,
) -> Result<bool> {
    store
        .db
        .write(move |tx| tx.finalize_thumb(row_id, &path, size))
        .await
}

/// Batch view plus the mirror chat id in one actor job: the send path needs
/// both and must not observe them from two different database states.
pub async fn batch_snapshot_of(
    store: &Store,
    key: String,
) -> Result<Option<(crate::ledger::BatchView, i64)>> {
    store
        .db
        .call(move |con| {
            let view = crate::ledger::view(con, &key)?;
            let chat: i64 = crate::kv_get(con, "chat_id", "0").parse().unwrap_or(0);
            Ok(view.map(|v| (v, chat)))
        })
        .await
}

pub async fn thread_of_batch(store: &Store, key: String) -> Result<Option<i64>> {
    store
        .db
        .call(move |con| Ok(crate::ledger::thread_id(con, &key)))
        .await
}

pub async fn open_batches_of(store: &Store) -> Result<Vec<String>> {
    store
        .db
        .call(|con| crate::ledger::open_batches(con))
        .await
}

/// Batch an answer belongs to when the caller named a message: the OPEN batch
/// that carries it. See `ledger::open_batch_of_message`.
pub async fn open_batch_of_message(store: &Store, message_id: i64) -> Result<Option<String>> {
    store
        .db
        .call(move |con| crate::ledger::open_batch_of_message(con, message_id))
        .await
}

/// Batch an unaddressed send belongs to. See `ledger::latest_open_unanswered`.
pub async fn latest_open_unanswered(store: &Store) -> Result<Option<String>> {
    store
        .db
        .call(crate::ledger::latest_open_unanswered)
        .await
}

pub async fn set_self_identity(
    store: &Store,
    bot_id: i64,
    username: String,
    first_name: String,
) -> Result<()> {
    store
        .db
        .write(move |tx| {
            crate::kv_set(tx.handle(), "self_id", &bot_id.to_string())?;
            crate::kv_set(tx.handle(), "self_username", &username)?;
            crate::kv_set(tx.handle(), "self_first", &first_name)?;
            Ok(())
        })
        .await
}

/// The last 500 chat rows for the summarize digest, oldest first.
pub fn recent_digest(
    con: &Connection,
    limit: i64,
) -> Result<Vec<(Option<String>, Option<String>, Option<String>)>> {
    let mut stmt = con.prepare(
        "SELECT username, first_name, text FROM messages ORDER BY update_id DESC LIMIT ?",
    )?;
    let rows = stmt
        .query_map([limit], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .flatten()
        .collect();
    Ok(rows)
}

pub async fn close_batch_of(store: &Store, sig: String, outcome: String) -> Result<()> {
    store
        .db
        .write(move |tx| crate::ledger::close_batch(tx.handle(), &sig, &outcome))
        .await
}

pub async fn close_batches_of(store: &Store, pairs: Vec<(String, String)>) -> Result<()> {
    store
        .db
        .write(move |tx| {
            for (sig, outcome) in &pairs {
                crate::ledger::close_batch(tx.handle(), sig, outcome)?;
            }
            Ok(())
        })
        .await
}

pub async fn mark_settled_many_of(store: &Store, sigs: Vec<String>, at: i64) -> Result<()> {
    store
        .db
        .write(move |tx| {
            for sig in &sigs {
                tx.mark_settled(sig, at)?;
            }
            Ok(())
        })
        .await
}

/// Proof of emission: stamp emitted_at, and the late settle when the reducer
/// already decided this batch settled cleanly. One transaction.
pub async fn mark_emitted_of(
    store: &Store,
    sig: String,
    at: i64,
    settled_seen: i64,
) -> Result<()> {
    store
        .db
        .write(move |tx| {
            tx.mark_emitted(&sig, at)?;
            if settled_seen > 0 {
                tx.mark_settled(&sig, settled_seen)?;
            }
            Ok(())
        })
        .await
}

pub async fn views_of(store: &Store, sigs: Vec<String>) -> Result<Vec<crate::ledger::BatchView>> {
    store
        .db
        .call(move |con| {
            let mut out = vec![];
            for sig in &sigs {
                if let Some(view) = crate::ledger::view(con, sig)? {
                    out.push(view);
                }
            }
            Ok(out)
        })
        .await
}

// ---------- read tools (tg-chat, botwatch) ----------

/// History window for the `history` tool. `before` is the legacy `update_id`
/// cursor the tool accepts.
pub fn history(
    con: &Connection,
    before: Option<i64>,
    limit: i64,
) -> Result<Vec<Value>> {
    let mut q = "SELECT message_id, username, first_name, text, reply_to, media_path FROM messages"
        .to_string();
    let mut params: Vec<Value> = vec![];
    if let Some(b) = before {
        q.push_str(" WHERE update_id < ?");
        params.push(json!(b));
    }
    q.push_str(" ORDER BY date DESC, message_id DESC LIMIT ?");
    params.push(json!(limit));
    let mut stmt = con.prepare(&q)?;
    let p: Vec<Box<dyn rusqlite::ToSql>> = params
        .iter()
        .map(|v| Box::new(v.as_i64().unwrap_or(0)) as Box<dyn rusqlite::ToSql>)
        .collect();
    let refs: Vec<&dyn rusqlite::ToSql> = p.iter().map(|b| b.as_ref()).collect();
    let rows: Vec<Value> = stmt
        .query_map(refs.as_slice(), |r| {
            Ok(json!({
                "message_id": r.get::<_, Option<i64>>(0)?,
                "user": r.get::<_, Option<String>>(1)?.or(r.get::<_, Option<String>>(2)?),
                "text": r.get::<_, Option<String>>(3)?,
                "reply_to": r.get::<_, Option<i64>>(4)?,
                "media": r.get::<_, Option<String>>(5)?,
            }))
        })?
        .flatten()
        .collect();
    Ok(rows)
}

/// One FTS query over the mirror. The caller decides the quoted-phrase retry.
pub fn fts_search(con: &Connection, query: &str, limit: i64) -> Result<Vec<Value>> {
    let mut stmt = con.prepare(
        "SELECT m.message_id, m.username, m.text FROM messages_fts f JOIN messages m ON m.id = f.rowid WHERE messages_fts MATCH ? LIMIT ?",
    )?;
    let rows: Vec<Value> = stmt
        .query_map(rusqlite::params![query, limit], |r| {
            Ok(json!({"message_id": r.get::<_, Option<i64>>(0)?, "user": r.get::<_, Option<String>>(1)?, "text": r.get::<_, Option<String>>(2)?.unwrap_or_default()}))
        })?
        .flatten()
        .collect();
    Ok(rows)
}

/// Bounds and sample texts of one chatter.
pub fn chatter_bounds(
    con: &Connection,
    name: &str,
    at: &str,
) -> (i64, Option<i64>, Option<i64>) {
    con.query_row(
        "SELECT COUNT(*), MIN(date), MAX(date) FROM messages WHERE username = ? COLLATE NOCASE OR first_name = ? COLLATE NOCASE OR ('@' || username) = ? COLLATE NOCASE",
        rusqlite::params![name, name, at],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )
    .unwrap_or((0, None, None))
}

pub fn chatter_samples(con: &Connection, name: &str) -> Vec<String> {
    let Ok(mut stmt) = con.prepare("SELECT text FROM messages WHERE (username = ? COLLATE NOCASE OR first_name = ? COLLATE NOCASE) AND text <> '' ORDER BY update_id DESC LIMIT 3")
    else {
        return vec![];
    };
    stmt.query_map(rusqlite::params![name, name], |r| {
        r.get::<_, Option<String>>(0)
    })
    .map(|it| {
        it.flatten()
            .flatten()
            .map(|s: String| s.chars().take(200).collect())
            .collect()
    })
    .unwrap_or_default()
}

#[derive(Debug, Default)]
pub struct ChatStats {
    pub total: i64,
    pub media: i64,
    pub first_date: Option<i64>,
    pub last_date: Option<i64>,
    pub top: Vec<(Option<String>, i64)>,
    /// Timestamps of the last messages (raw; the caller buckets by day).
    pub dates: Vec<i64>,
}

pub fn chat_stats(con: &Connection) -> ChatStats {
    let myself: i64 = crate::kv_get(con, "self_id", "0").parse().unwrap_or(0);
    let excl = format!("WHERE user_id IS NULL OR user_id != {myself}");
    let mut out = ChatStats::default();
    out.total = con
        .query_row(&format!("SELECT COUNT(*) FROM messages {excl}"), [], |r| {
            r.get(0)
        })
        .unwrap_or(0);
    out.media = con
        .query_row(&format!("SELECT COUNT(*) FROM messages WHERE media_path <> '' AND (user_id IS NULL OR user_id != {myself})"), [], |r| r.get(0))
        .unwrap_or(0);
    let bounds: (Option<i64>, Option<i64>) = con
        .query_row(
            &format!("SELECT MIN(date), MAX(date) FROM messages {excl}"),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap_or((None, None));
    out.first_date = bounds.0;
    out.last_date = bounds.1;
    if let Ok(mut stmt) = con.prepare(&format!("SELECT COALESCE(NULLIF(username, ''), first_name, CAST(user_id AS TEXT)), COUNT(*) FROM messages {excl} GROUP BY 1 ORDER BY 2 DESC LIMIT 10")) {
        out.top = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .map(|it| it.flatten().collect())
            .unwrap_or_default();
    }
    if let Ok(mut s) = con.prepare(&format!("SELECT date FROM messages WHERE date > 0 AND (user_id IS NULL OR user_id != {myself})")) {
        out.dates = s
            .query_map([], |r| r.get(0))
            .map(|it| it.flatten().collect())
            .unwrap_or_default();
    }
    out
}

/// The bot's own id from the mirror (None when never recorded).
pub fn bot_self_id(con: &Connection) -> Option<i64> {
    con.query_row("SELECT v FROM kv WHERE k='self_id'", [], |row| {
        row.get::<_, String>(0)
    })
    .ok()
    .and_then(|v| v.trim().parse::<i64>().ok())
}

/// Inbound/answer counts since a timestamp, for the stall watchdog.
pub fn stall_counts(con: &Connection, since: i64, bot_id: i64) -> (i64, i64) {
    let inbound: i64 = con
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE date >= ?1 AND user_id <> ?2",
            rusqlite::params![since, bot_id],
            |row| row.get(0),
        )
        .unwrap_or(0);
    let answers: i64 = con
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE date >= ?1 AND user_id = ?2",
            rusqlite::params![since, bot_id],
            |row| row.get(0),
        )
        .unwrap_or(0);
    (inbound, answers)
}
