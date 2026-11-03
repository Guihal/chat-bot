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
use rusqlite::{Connection, OptionalExtension, Transaction};
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
        std::thread::Builder::new()
            .name("chatdb".into())
            .spawn(move || {
                let mut con = match crate::db(&st) {
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
            .context("spawn chatdb thread")?;
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
                "INSERT INTO messages(chat_id,message_id,thread_id,date,user_id,username,first_name,text,reply_to,media_path,file_id,file_unique_id,media_kind,media_state,media_size,source_chat,sender_chat,update_id)
                 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
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

    /// Commit the transaction. Consumes the handle so no statement can run
    /// afterwards; dropping it instead rolls back.
    pub fn commit(mut self) -> Result<()> {
        if let Some(tx) = self.tx.take() {
            tx.commit()?;
        }
        Ok(())
    }
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
