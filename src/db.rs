//! Database actor: one `rusqlite::Connection`, one owning thread, FIFO queue.
//!
//! The daemon runs on a `current_thread` runtime. A sqlite call made from a
//! task therefore stops *everything* on that thread — Telegram polling, batch
//! admission and the socket writer that delivers chat pushes alike. Moving the
//! work to a dedicated thread is the smallest mechanism that removes that
//! coupling while keeping a single writer:
//!
//! * `rusqlite::Connection` is `Send` but **not** `Sync`, so there can be only
//!   one owner at a time; `spawn_blocking` on a `current_thread` runtime would
//!   spin up a thread pool with no ordering and no single owner.
//! * One thread + a FIFO queue gives (a) sqlite work off the runtime thread,
//!   (b) strict operation order, (c) exactly one writer per process.
//!
//! Contracts for call sites:
//!
//! 1. A closure runs with exclusive access to the connection, and the queue is
//!    FIFO — one closure is atomic with respect to every other DB user. Any
//!    multi-step sequence that must not interleave (`mark_done`, `regen_text`,
//!    the dedup-check+insert of an incoming message, recovery of one batch sig)
//!    belongs in ONE closure, never in several calls.
//! 2. A closure must never call back into `Db` — it would wait for itself and
//!    deadlock. Everything a closure needs must already be captured.
//! 3. Closures must not `await` or block on long work: the worker serves every
//!    other DB user meanwhile. File/media work stays outside.
//! 4. A panic inside a closure is caught: the caller gets `Err` and the worker
//!    keeps serving; one bad row never takes the database down until restart.
use crate::{anyhow, Applied, AppliedPage, Envelope, InsertedRow};
use anyhow::{Context, Result};
// actor-io-allow:begin opening the state lock file is the open path
use std::fs::OpenOptions;
// actor-io-allow:end
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::time::Duration;

type Job = Box<dyn FnOnce(&mut Connection) + Send + 'static>;

/// Handle to the database thread. Cheap to clone; clones share the queue.
#[derive(Clone)]
pub struct Db {
    tx: Sender<Job>,
}

impl Db {
    /// Start the worker: it opens its OWN connection (WAL, busy_timeout, schema,
    /// migrations and indexes via [`crate::db`]) and then serves the queue.
    /// `open` only returns once the connection exists, so a broken database is
    /// reported to the caller instead of dying silently in the thread.
    pub fn open(state: &str) -> Result<Db> {
        let (tx, rx): (Sender<Job>, Receiver<Job>) = channel();
        let (ready_tx, ready_rx) = channel::<Result<()>>();
        let st = state.to_string();
                // actor-io-allow:begin the database actor thread IS the mechanism: one owner, one FIFO queue
        std::thread::Builder::new()
            .name("chatdb".into())
            .spawn(move || {
                let mut con = match crate::db::connect(&st) {
                    Ok(c) => c,
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                        return;
                    }
                };
                let _ = ready_tx.send(Ok(()));
                for job in rx {
                    // `catch_unwind` keeps the connection and the queue alive
                    // after a panicking closure (the caller sees the dropped
                    // reply as an error).
                    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job(&mut con)))
                        .is_err()
                    {
                        crate::dlog("db worker: closure panicked, connection kept");
                    }
                }
            })
            .context("spawn chatdb thread")?;                // actor-io-allow:end

        match ready_rx.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(())) => Ok(Db { tx }),
            Ok(Err(e)) => Err(e.context("db worker open")),
            Err(_) => Err(anyhow!("db worker did not open within 5s")),
        }
    }

    fn enqueue<F>(&self, job: F) -> Result<()>
    where
        F: FnOnce(&mut Connection) + Send + 'static,
    {
        self.tx
            .send(Box::new(job))
            .map_err(|_| anyhow!("db worker stopped"))
    }

    /// Run `f` on the database thread from async code. The awaiting task yields;
    /// the runtime thread stays free for other units.
    pub async fn call<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
    {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.enqueue(move |con| {
            let _ = reply_tx.send(f(con));
        })?;
        match reply_rx.await {
            Ok(res) => res,
            Err(_) => {
                let e = anyhow!("db call failed: worker dropped the reply (panic or shutdown)");
                crate::ops::db_failure("db.call", &e);
                Err(e)
            }
        }
    }

    /// Run `f` inside one sqlite transaction on the database thread. `f` gets a
    /// [`WriteTxn`]; the transaction commits only after `f` returns `Ok`. An
    /// `Err` (or a panic) rolls back every statement `f` made, so a page can
    /// never be half-applied.
    pub async fn write<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: for<'a> FnOnce(&mut WriteTxn<'a>) -> Result<T> + Send + 'static,
    {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.enqueue(move |con| {
            let result = (|| -> Result<T> {
                let mut tx = WriteTxn::new(con)?;
                let out = f(&mut tx)?;
                tx.commit()?;
                Ok(out)
            })();
            let _ = reply_tx.send(result);
        })?;
        match reply_rx.await {
            Ok(res) => res,
            Err(_) => {
                let e = anyhow!("db write failed: worker dropped the reply (panic or shutdown)");
                crate::ops::db_failure("db.write", &e);
                Err(e)
            }
        }
    }

    /// Same as [`Db::call`] for the synchronous entry points (`cmd_sync`,
    /// `cmd_summarize`).
    pub fn call_blocking<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
    {
        let (reply_tx, reply_rx) = std::sync::mpsc::channel();
        self.enqueue(move |con| {
            let _ = reply_tx.send(f(con));
        })?;
        match reply_rx.recv() {
            Ok(res) => res,
            Err(_) => Err(anyhow!(
                "db call failed: worker dropped the reply (panic or shutdown)"
            )),
        }
    }
}

/// A write transaction on the database thread. Only `Db::write` starts one in
/// production; tests may start it from a synchronous connection. Statements
/// stay uncommitted until [`WriteTxn::commit`], and dropping the handle rolls
/// them back.
pub struct WriteTxn<'c> {
    tx: Option<Transaction<'c>>,
}

impl<'c> WriteTxn<'c> {
    pub fn new(con: &'c mut Connection) -> Result<Self> {
        Ok(Self {
            tx: Some(con.transaction()?),
        })
    }

    fn tx(&self) -> &Transaction<'c> {
        self.tx
            .as_ref()
            .expect("WriteTxn used after commit/rollback")
    }

    /// Journal transitions live in `ledger.rs`; they need the live transaction
    /// but must not gain permission to commit or roll it back.
    pub fn handle(&self) -> &Transaction<'c> {
        self.tx()
    }

    /// Write every message row of a fetched page and move `kv("offset")` to
    /// `through_update` in ONE transaction.
    ///
    /// A page at or below the committed cursor is a replay of an already
    /// committed page: nothing is written and every storable envelope is
    /// reported as `duplicate`. A key that already exists is also a duplicate
    /// (the same message under a new `update_id`), so it does not abort the
    /// page and does not prevent the cursor from advancing. A genuine INSERT
    /// failure is an `Err`: the whole page rolls back and `offset` stays where
    /// it was.
    pub fn commit_page(
        &mut self,
        envelopes: Vec<Envelope>,
        through_update: i64,
    ) -> Result<AppliedPage> {
        let tx = self.tx();
        let current: i64 = crate::kv_get(tx, "offset", "0").parse().unwrap_or(0);
        let mut applied = Applied::default();
        let mut inserted = Vec::new();
        if through_update <= current {
            for env in &envelopes {
                if env.is_storable() {
                    applied.duplicate += 1;
                } else {
                    applied.ignored += 1;
                }
            }
            return Ok(AppliedPage { applied, inserted });
        }
        for env in envelopes {
            if !env.is_storable() {
                applied.ignored += 1;
                continue;
            }
            let exists: Option<i64> = tx
                .query_row(
                    "SELECT 1 FROM messages WHERE chat_id=? AND message_id=? LIMIT 1",
                    rusqlite::params![env.key.chat, env.key.message.0],
                    |r| r.get(0),
                )
                .optional()?;
            if exists.is_some() {
                applied.duplicate += 1;
                continue;
            }
            // Identity is (chat_id, message_id) only. Author/date/text are not
            // a second key: two real messages that repeat the same words are
            // two messages and both must persist.
            tx.execute(
                "INSERT INTO messages(chat_id,message_id,thread_id,date,user_id,username,first_name,text,reply_to,media_path,file_id,file_unique_id,media_kind,media_state,media_size,thumb_file_id,thumb_state,source_chat,sender_chat,update_id)
                 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                rusqlite::params![
                    env.key.chat,
                    env.key.message.0,
                    env.thread_id,
                    env.date,
                    env.user_id,
                    env.username,
                    env.first_name,
                    env.text,
                    env.reply_to,
                    env.media_path,
                    env.file_id,
                    env.file_unique_id,
                    env.media_kind,
                    env.media_state.as_str(),
                    env.media_size.map(|s| s as i64),
                    env.thumb_file_id,
                    env.thumb_state.as_str(),
                    env.source_chat,
                    env.sender_chat,
                    env.update_id,
                ],
            )?;
            let row_id = tx.last_insert_rowid();
            applied.inserted += 1;
            inserted.push(InsertedRow {
                key: env.key,
                row_id,
            });
        }
        crate::kv_set(tx, "offset", &through_update.to_string())?;
        Ok(AppliedPage { applied, inserted })
    }

    /// The ONLY transition to `Ready`. Path, kind and size go into `blobs`;
    /// the message row keeps only the blob link (and the read projection).
    /// A row that is no longer `pending` is not finalized, so an evicted or
    /// deleted row cannot be resurrected by an in-flight download.
    pub fn finalize_blob(
        &mut self,
        row_id: i64,
        path: &str,
        kind: &str,
        size: u64,
    ) -> Result<bool> {
        let tx = self.tx();
        let pending: Option<i64> = tx
            .query_row(
                "SELECT id FROM messages WHERE id=? AND media_state='pending' LIMIT 1",
                [row_id],
                |r| r.get(0),
            )
            .optional()?;
        if pending.is_none() {
            return Ok(false);
        }
        tx.execute(
            "INSERT INTO blobs(path,kind,size,created_at) VALUES(?,?,?,?)",
            rusqlite::params![path, kind, size as i64, chrono::Utc::now().timestamp()],
        )?;
        let blob_id = tx.last_insert_rowid();
        let changed = tx.execute(
            "UPDATE messages SET media_state='ready', blob_id=?, media_path=?
              WHERE id=? AND media_state='pending'",
            rusqlite::params![blob_id, path, row_id],
        )?;
        if changed == 0 {
            tx.execute("DELETE FROM blobs WHERE id=?", [blob_id])?;
            return Ok(false);
        }
        Ok(true)
    }

    /// The ONLY transition of a video frame to readable. The frame is a
    /// projection of the message, not a blob: it lives in `thumb_path`, so an
    /// eviction clears it through `thumb_path` and never through `blobs`.
    /// A row whose frame is no longer `pending` is not finalized, so a stale
    /// in-flight download cannot resurrect an evicted frame. The size is kept
    /// on the message when only the frame was ever downloaded, because the GIF
    /// catalogue needs a known size and the mp4 is never fetched.
    pub fn finalize_thumb(&mut self, row_id: i64, path: &str, size: u64) -> Result<bool> {
        let tx = self.tx();
        let changed = tx.execute(
            "UPDATE messages
                SET thumb_state='ready', thumb_path=?, media_size=COALESCE(media_size, ?)
              WHERE id=? AND thumb_state='pending'",
            rusqlite::params![path, size as i64, row_id],
        )?;
        Ok(changed > 0)
    }

    /// Commit the transaction. Consumes the handle so no statement can run
    /// afterwards; dropping it instead rolls back.
    pub fn commit(mut self) -> Result<()> {
        if let Some(tx) = self.tx.take() {
            tx.commit()?;
        }
        Ok(())
    }
}

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

/// The chat id migration v2 may backfill: the environment wins, an already
/// seeded `kv` value is the fallback (a restart without the variable still
/// knows the identity it committed earlier).
pub fn migration_chat_id(con: &Connection) -> Option<i64> {
    if let Ok(raw) = std::env::var("CHATBOT_CHAT_ID") {
        return crate::parse_chat_id(&raw);
    }
    con.query_row("SELECT v FROM kv WHERE k='chat_id'", [], |r| {
        r.get::<_, String>(0)
    })
    .ok()
    .and_then(|v| crate::parse_chat_id(&v))
}

/// Fail closed BEFORE any migration step. v1 commits `user_version=1` on its
/// own, so validating inside v2 would leave the file half-migrated; this check
/// runs while the version is still the original one.
pub fn require_migration_chat_id(con: &Connection, version: i64) -> Result<()> {
    if let Ok(raw) = std::env::var("CHATBOT_CHAT_ID") {
        if !raw.trim().is_empty() && crate::parse_chat_id(&raw).is_none() {
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

pub fn migrate(con: &mut Connection) -> Result<()> {
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
pub fn migrate_v1(con: &mut Connection) -> Result<()> {
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
pub fn migrate_v3(con: &mut Connection) -> Result<()> {
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
pub fn migrate_v4(con: &mut Connection) -> Result<()> {
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
pub fn migrate_v6(con: &mut Connection) -> Result<()> {
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
pub fn migrate_v5(con: &mut Connection) -> Result<()> {
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
    // actor-io-allow:begin the state lock file handle is the mechanism
    _file: std::fs::File,
    // actor-io-allow:end
    path: PathBuf,
}

impl StateLock {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub fn acquire_state_lock(state: &str) -> Result<StateLock> {
    // actor-io-allow:begin the lock path never nests: create the state dir
    std::fs::create_dir_all(state)?;
    // actor-io-allow:end
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

pub fn connect(state: &str) -> Result<Connection> {
    // actor-io-allow:begin the open path may create the state directory
    std::fs::create_dir_all(state)?;
    // actor-io-allow:end
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
        // actor-io-allow:begin a log line on the open path, not a data decision
        crate::dlog(&format!("ensure_indexes failed: {e:#}"));
        // actor-io-allow:end
    }
    Ok(con)
}

/// Cycle 3 columns on the v5 journal skeleton. Additive and idempotent, in the
/// same spirit as the legacy `messages` ALTERs above: an existing database gains
/// them at the next start without a table rebuild.
pub fn ensure_journal_columns(con: &Connection) -> Result<()> {
    for stmt in [
        "ALTER TABLE batches ADD COLUMN message_ids TEXT",
        "ALTER TABLE batches ADD COLUMN tome TEXT",
        "ALTER TABLE answers ADD COLUMN fingerprint TEXT",
        "ALTER TABLE answers ADD COLUMN response TEXT",
        "ALTER TABLE attempts ADD COLUMN emitted_at INTEGER",
        "ALTER TABLE sends ADD COLUMN request TEXT",
        // Cycle media: a video frame is a projection of the message, not a
        // blob. `thumb_state` mirrors `media_state` for that one file, so the
        // mp4 row can stay `descriptor` while its frame becomes readable.
        "ALTER TABLE messages ADD COLUMN thumb_file_id TEXT",
        "ALTER TABLE messages ADD COLUMN thumb_state TEXT NOT NULL DEFAULT 'absent'",
        "ALTER TABLE messages ADD COLUMN thumb_path TEXT",
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    fn tmp_state(tag: &str) -> String {
        let dir = std::env::temp_dir().join(format!("chatdb-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir.to_string_lossy().to_string()
    }

    /// Property that justifies the whole actor: a blocking call must not take
    /// the runtime thread. On a `current_thread` runtime a spinner task keeps
    /// ticking only if the sqlite work really left that thread.
    #[tokio::test(flavor = "current_thread")]
    async fn blocking_call_leaves_the_runtime_free() {
        let state = tmp_state("free");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let db = Db::open(&state).unwrap();
        let ticks = Arc::new(AtomicUsize::new(0));
        let spinner_ticks = ticks.clone();
        let spinner = tokio::task::spawn(async move {
            for _ in 0..10 {
                tokio::time::sleep(Duration::from_millis(10)).await;
                spinner_ticks.fetch_add(1, Ordering::SeqCst);
            }
        });
        db.call(|_con| {
            std::thread::sleep(Duration::from_millis(220));
            Ok(())
        })
        .await
        .unwrap();
        spinner.await.unwrap();
        assert_eq!(
            ticks.load(Ordering::SeqCst),
            10,
            "runtime thread was blocked by sqlite work"
        );
        let _ = std::fs::remove_dir_all(&state);
    }

    /// FIFO: an operation enqueued first lands first, even when the second one
    /// is much cheaper.
    #[tokio::test(flavor = "current_thread")]
    async fn queue_is_fifo() {
        let state = tmp_state("fifo");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let db = Db::open(&state).unwrap();
        let order = Arc::new(Mutex::new(Vec::new()));
        let slow_order = order.clone();
        let fast_order = order.clone();
        let fast_db = db.clone();
        let slow = db.call(move |_con| {
            std::thread::sleep(Duration::from_millis(60));
            slow_order.lock().unwrap().push(1u8);
            Ok(())
        });
        let fast = fast_db.call(move |_con| {
            fast_order.lock().unwrap().push(2u8);
            Ok(())
        });
        let (a, b) = tokio::join!(slow, fast);
        a.unwrap();
        b.unwrap();
        assert_eq!(&*order.lock().unwrap(), &vec![1u8, 2u8]);
        let _ = std::fs::remove_dir_all(&state);
    }

    /// A panicking closure is reported as an error and the worker survives.
    #[tokio::test(flavor = "current_thread")]
    async fn panic_in_closure_keeps_worker_alive() {
        let state = tmp_state("panic");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let db = Db::open(&state).unwrap();
        let boom = db
            .call(|_con| -> Result<()> { panic!("boom inside db closure") })
            .await;
        assert!(boom.is_err(), "panic must surface as Err");
        let kv = db
            .call(|con| Ok(crate::kv_get(con, "absent", "fallback")))
            .await
            .unwrap();
        assert_eq!(kv, "fallback");
        let _ = std::fs::remove_dir_all(&state);
    }

    /// `Db::write` runs the page on the actor thread and commits rows + offset
    /// together; an error inside the same closure rolls both back.
    #[tokio::test(flavor = "current_thread")]
    async fn write_commits_page_and_offset_together() {
        let state = tmp_state("write");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let db = Db::open(&state).unwrap();
        let make = |message: i64, update: i64| crate::Envelope {
            key: crate::MessageKey {
                chat: -100,
                message: crate::MessageId(message),
            },
            update_id: update,
            date: 1000 + update,
            user_id: Some(7),
            username: "u".into(),
            first_name: "U".into(),
            text: format!("m{message}"),
            reply_to: None,
            thread_id: None,
            media_path: String::new(),
            file_id: String::new(),
            file_unique_id: String::new(),
            media_kind: String::new(),
            media_state: crate::media::MediaState::Absent,
            media_size: None,
            thumb_file_id: String::new(),
            thumb_state: crate::media::MediaState::Absent,
            source_chat: None,
            sender_chat: None,
            cross: false,
        };
        let applied = db
            .write(move |tx| tx.commit_page(vec![make(1, 5), make(2, 6)], 6))
            .await
            .unwrap();
        assert_eq!(applied.applied.inserted, 2);
        let (rows, offset): (i64, String) = db
            .call(|con| {
                Ok((
                    con.query_row("SELECT COUNT(*) FROM messages", [], |r| r.get(0))?,
                    crate::kv_get(con, "offset", "0"),
                ))
            })
            .await
            .unwrap();
        assert_eq!((rows, offset.as_str()), (2, "6"));

        db.call(|con| {
            con.execute_batch(
                "CREATE TRIGGER force_write_failure BEFORE INSERT ON messages
                 WHEN NEW.message_id = 3
                 BEGIN SELECT RAISE(ABORT, 'forced'); END;",
            )?;
            Ok(())
        })
        .await
        .unwrap();
        let failed = db
            .write(move |tx| tx.commit_page(vec![make(4, 7), make(3, 8)], 8))
            .await;
        assert!(failed.is_err());
        let (rows, offset): (i64, String) = db
            .call(|con| {
                Ok((
                    con.query_row("SELECT COUNT(*) FROM messages", [], |r| r.get(0))?,
                    crate::kv_get(con, "offset", "0"),
                ))
            })
            .await
            .unwrap();
        assert_eq!((rows, offset.as_str()), (2, "6"));
        let _ = std::fs::remove_dir_all(&state);
    }

    /// A caller whose worker is gone gets `Err` immediately, never a hang.
    #[tokio::test(flavor = "current_thread")]
    async fn call_after_worker_gone_errors() {
        let (tx, rx) = channel::<Job>();
        drop(rx); // the worker never existed: the channel is already closed
        let db = Db { tx };
        let res = tokio::time::timeout(Duration::from_secs(2), db.call(|_con| Ok(1i64)))
            .await
            .expect("call hung instead of failing");
        assert!(res.is_err());
        let blocking = db.call_blocking(|_con| Ok(2i64));
        assert!(blocking.is_err());
    }
}
