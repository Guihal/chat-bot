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
use anyhow::{anyhow, Context, Result};
use rusqlite::Connection;
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
            Err(_) => Err(anyhow!(
                "db call failed: worker dropped the reply (panic or shutdown)"
            )),
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

    /// A caller whose worker is gone gets `Err` immediately, never a hang.
    #[tokio::test(flavor = "current_thread")]
    async fn call_after_worker_gone_errors() {
        let (tx, rx) = channel::<Job>();
        drop(rx); // the worker never existed: the channel is already closed
        let db = Db { tx };
        let res = tokio::time::timeout(
            Duration::from_secs(2),
            db.call(|_con| Ok(1i64)),
        )
        .await
        .expect("call hung instead of failing");
        assert!(res.is_err());
        let blocking = db.call_blocking(|_con| Ok(2i64));
        assert!(blocking.is_err());
    }
}
