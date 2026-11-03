//! Journal: the single owner of batch, attempt, answer and send transitions.
//!
//! Cycle 1 created the tables; this module gives them their transitions. The
//! old delivery path decided closure from kv marks and from `reply_to` on the
//! mirror, which is why an answer without a quote left the batch open and a
//! restart re-delivered it. Here a batch closes because a journal row says so:
//! an answer was delivered, or an attempt that the agent really saw settled.
//!
//! Contract highlights (DESIGN §3.1/§3.2):
//! * `open_answer` mints an `AnswerId` inside the ledger and never anywhere
//!   else. A second distinct answer for the same batch is an error; the same
//!   fingerprint is a replay, answered from the stored response.
//! * `begin_send` writes the durable intent BEFORE any HTTP call.
//!   `start_send` moves Intent -> Sending, `finish_send` moves
//!   Sending -> Sent | Unknown. A crash between those leaves Sending, which
//!   `resolve_sending` turns into Unknown at startup with exactly one incident
//!   and no resend.
//! * The Received receipt and the own-send mirror row are written in the SAME
//!   transaction as Sending -> Sent (DESIGN I4).
use crate::db::WriteTxn;
use anyhow::{anyhow, Result};
use rusqlite::{Connection, OptionalExtension, Transaction};
use serde_json::{json, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnswerId(pub i64);

/// Result of opening the answer for one batch. `Replay` carries the exact text
/// the first call returned, because a repeated `chat_send` must be byte
/// identical and must not touch the Bot API again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenAnswer {
    Opened(AnswerId),
    Replay {
        answer: AnswerId,
        response: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct SendIntent {
    pub id: i64,
    pub answer: AnswerId,
    pub send_key: String,
    pub kind: String,
    pub request: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Receipt {
    pub message_id: i64,
    pub reply_to: Option<i64>,
    pub text: String,
    pub kind: String,
    pub file_id: String,
    pub file_unique_id: String,
    pub media_path: String,
    /// Size of the sent media as the Bot API reports it; 0 for text.
    pub media_size: u64,
    pub thread_id: Option<i64>,
}

impl Receipt {
    pub fn json(&self) -> Value {
        json!({
            "message_id": self.message_id,
            "reply_to": self.reply_to,
            "text": self.text,
            "kind": self.kind,
            "file_id": self.file_id,
            "file_unique_id": self.file_unique_id,
            "media_path": self.media_path,
            "media_size": self.media_size,
            "thread_id": self.thread_id,
        })
    }

    /// Shape the extension has always seen: message_id, reply_to, text. Extra
    /// journal coordinates ride along so a receipt is still auditable.
    pub fn tool_json(&self) -> Value {
        let mut v = json!({
            "message_id": self.message_id,
            "reply_to": self.reply_to,
            "text": self.text,
        });
        if !self.kind.is_empty() && self.kind != "text" {
            v["kind"] = json!(self.kind);
            v["file_id"] = json!(self.file_id);
            v["file_unique_id"] = json!(self.file_unique_id);
            v["media_path"] = json!(self.media_path);
            v["media_size"] = json!(self.media_size);
        }
        if let Some(t) = self.thread_id {
            v["thread_id"] = json!(t);
        }
        v
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendOutcome {
    Sent(Receipt),
    Unknown(String),
}

#[derive(Debug, Clone)]
pub struct BatchView {
    pub id: i64,
    pub batch_key: String,
    pub state: String,
    pub outcome: Option<String>,
    pub chat_id: Option<i64>,
    pub thread_id: Option<i64>,
    pub message_ids: Vec<i64>,
    pub tome: Vec<i64>,
    pub answers: i64,
    pub sent: i64,
    pub pending_sends: i64,
    pub unknown_sends: i64,
    pub admitted_at: Option<i64>,
    pub emitted_at: Option<i64>,
    pub settled_at: Option<i64>,
}

impl BatchView {
    /// Phase used only to pick a retry budget; it is not a closure decision.
    pub fn phase(&self) -> &'static str {
        if self.state == "closed" || self.state == "open" || self.state == "queued" {
            return "inboxed";
        }
        if self.admitted_at.is_none() {
            return "inboxed";
        }
        if self.emitted_at.is_none() {
            return "admitted";
        }
        if self.settled_at.is_none() {
            return "emitted";
        }
        "settled"
    }

    /// The only closure rule left. It never looks at `reply_to`: an answer is
    /// a journal row, so quoting nothing closes the batch just like quoting the
    /// incoming message would.
    pub fn close_reason(&self) -> Option<&'static str> {
        if self.sent > 0 && self.pending_sends == 0 {
            return Some("answered");
        }
        if self.unknown_sends > 0 {
            return Some("delivered_unknown");
        }
        if let (Some(e), Some(s)) = (self.emitted_at, self.settled_at) {
            if s >= e {
                return Some("settled");
            }
        }
        None
    }

    /// An attempt may return to Queued only while no send has begun. Once an
    /// intent exists the answer owns the outcome and a retry must not send a
    /// second visible message.
    pub fn send_begun(&self) -> bool {
        self.sent > 0 || self.pending_sends > 0 || self.unknown_sends > 0
    }
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

fn batch_id(tx: &Transaction, key: &str) -> Result<i64> {
    tx.query_row("SELECT id FROM batches WHERE batch_key=?", [key], |r| {
        r.get(0)
    })
    .optional()?
    .ok_or_else(|| anyhow!("unknown batch: {key}"))
}

fn parse_ids(raw: Option<String>) -> Vec<i64> {
    raw.and_then(|s| serde_json::from_str::<Vec<Value>>(&s).ok())
        .unwrap_or_default()
        .into_iter()
        .filter_map(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .collect()
}

impl<'c> WriteTxn<'c> {
    /// Record the batch on its way out of the assembler. The state machine uses
    /// `open -> queued -> claimed -> answered/closed`; the message ids live with
    /// the batch so recovery stops depending on `pending:*` kv marks.
    pub fn seal_batch(
        &mut self,
        key: &str,
        chat: i64,
        thread: Option<i64>,
        message_ids: &[i64],
        tome: &[i64],
    ) -> Result<i64> {
        let tx = self.handle();
        let ts = now();
        tx.execute(
            "INSERT INTO batches(batch_key,chat_id,thread_id,state,sealed_at,updated_at,message_ids,tome)
             VALUES(?,?,?,'open',?,?,?,?)
             ON CONFLICT(batch_key) DO UPDATE SET
               chat_id=excluded.chat_id,
               thread_id=COALESCE(excluded.thread_id,batches.thread_id),
               message_ids=excluded.message_ids,
               tome=excluded.tome,
               updated_at=excluded.updated_at",
            rusqlite::params![
                key,
                chat,
                thread,
                ts,
                ts,
                serde_json::to_string(message_ids)?,
                serde_json::to_string(tome)?,
            ],
        )?;
        batch_id(tx, key)
    }

    /// A batch the runtime has not taken yet. Idempotent: a re-push does not
    /// move a claimed/answered batch backwards.
    pub fn mark_pushed(&mut self, key: &str) -> Result<()> {
        let tx = self.handle();
        tx.execute(
            "UPDATE batches SET state='queued', updated_at=?
              WHERE batch_key=? AND state='open'",
            rusqlite::params![now(), key],
        )?;
        Ok(())
    }

    /// `admitted(started|steered|queued) -> Claimed{attempt}`.
    pub fn admit_batch(&mut self, key: &str, kind: &str) -> Result<()> {
        let tx = self.handle();
        let bid = batch_id(tx, key)?;
        let attempt_no: i64 = tx.query_row(
            "SELECT COALESCE(MAX(attempt_no),0)+1 FROM attempts WHERE batch_id=?",
            [bid],
            |r| r.get(0),
        )?;
        let ts = now();
        tx.execute(
            "INSERT INTO attempts(batch_id,attempt_no,state,started_at) VALUES(?,?,?,?)",
            rusqlite::params![bid, attempt_no, kind, ts],
        )?;
        tx.execute(
            "UPDATE batches SET state='claimed', updated_at=? WHERE id=?",
            rusqlite::params![ts, bid],
        )?;
        Ok(())
    }

    /// Delivery proof: the agent really has this batch in context. Kept on the
    /// attempt row (not in kv); it is the guard that stops an `agent_settled`
    /// from the previous turn closing a batch that was never seen.
    pub fn mark_emitted(&mut self, key: &str, at: i64) -> Result<()> {
        let tx = self.handle();
        let bid = batch_id(tx, key)?;
        tx.execute(
            "UPDATE attempts SET emitted_at=? 
              WHERE id=(SELECT id FROM attempts WHERE batch_id=? ORDER BY id DESC LIMIT 1)",
            rusqlite::params![at, bid],
        )?;
        Ok(())
    }

    /// `agent_settled` closes only an attempt that was emitted first.
    pub fn mark_settled(&mut self, key: &str, at: i64) -> Result<()> {
        let tx = self.handle();
        let bid = batch_id(tx, key)?;
        tx.execute(
            "UPDATE attempts SET settled_at=?, state='settled'
              WHERE id=(SELECT id FROM attempts WHERE batch_id=? ORDER BY id DESC LIMIT 1)
                AND emitted_at IS NOT NULL",
            rusqlite::params![at, bid],
        )?;
        Ok(())
    }

    /// The runtime refused an admitted delivery. Retry is legal only while no
    /// send has begun; otherwise the answer owns the outcome.
    pub fn fail_attempt(&mut self, key: &str) -> Result<()> {
        let tx = self.handle();
        let bid = batch_id(tx, key)?;
        let view = read_view(tx, key)?;
        if view.map(|v| v.send_begun()).unwrap_or(true) {
            return Ok(());
        }
        tx.execute(
            "UPDATE attempts SET state='failed'
              WHERE id=(SELECT id FROM attempts WHERE batch_id=? ORDER BY id DESC LIMIT 1)",
            [bid],
        )?;
        tx.execute(
            "UPDATE batches SET state='queued', updated_at=? WHERE id=?",
            rusqlite::params![now(), bid],
        )?;
        Ok(())
    }

    /// Mint the answer. This is the ONLY place an `AnswerId` is created.
    pub fn open_answer(&mut self, key: &str, fingerprint: &str) -> Result<OpenAnswer> {
        let tx = self.handle();
        let bid = batch_id(tx, key)?;
        if let Some((aid, fp, response)) = tx
            .query_row(
                "SELECT id, COALESCE(fingerprint,''), response FROM answers
                  WHERE batch_id=? ORDER BY id LIMIT 1",
                [bid],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .optional()?
        {
            if fp == fingerprint {
                return Ok(OpenAnswer::Replay {
                    answer: AnswerId(aid),
                    response,
                });
            }
            return Err(anyhow!(
                "answer_exists: batch {key} already has an answer in this turn; \
                 a second distinct chat_send is refused (repeat the same send to replay it)"
            ));
        }
        let ts = now();
        tx.execute(
            "INSERT INTO answers(batch_id,state,text,fingerprint,created_at) VALUES(?,'open',NULL,?,?)",
            rusqlite::params![bid, fingerprint, ts],
        )?;
        let aid = tx.last_insert_rowid();
        tx.execute(
            "INSERT OR IGNORE INTO answer_batches(answer_id,batch_id) VALUES(?,?)",
            rusqlite::params![aid, bid],
        )?;
        tx.execute(
            "UPDATE batches SET state='answered', updated_at=? WHERE id=? AND state <> 'closed'",
            rusqlite::params![ts, bid],
        )?;
        Ok(OpenAnswer::Opened(AnswerId(aid)))
    }

    /// Durable intent: committed BEFORE the HTTP call. `send_key` is stable per
    /// answer part, so a retried request cannot mint a second message.
    pub fn begin_send(
        &mut self,
        answer: AnswerId,
        part: usize,
        kind: &str,
        request: &Value,
    ) -> Result<SendIntent> {
        let tx = self.handle();
        let send_key = format!("answer:{}:part:{}", answer.0, part);
        let existing: Option<(i64, String, Option<String>, Option<String>)> = tx
            .query_row(
                "SELECT id,state,receipt,request FROM sends WHERE send_key=?",
                [&send_key],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;
        if let Some((id, state, receipt, req)) = existing {
            if state != "sent" && state != "intent" {
                return Err(anyhow!(
                    "send_exists: {send_key} is already {state}; refusing to begin it again"
                ));
            }
            if state == "sent" {
                return Err(anyhow!(
                    "send_exists: {send_key} was already sent ({})",
                    receipt.unwrap_or_else(|| "{}".into())
                ));
            }
            let req = req
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_else(|| request.clone());
            return Ok(SendIntent {
                id,
                answer,
                send_key,
                kind: kind.to_string(),
                request: req,
            });
        }
        let ts = now();
        tx.execute(
            "INSERT INTO sends(answer_id,send_key,kind,state,request,created_at,updated_at)
             VALUES(?,?,?,'intent',?,?,?)",
            rusqlite::params![answer.0, send_key, kind, request.to_string(), ts, ts],
        )?;
        Ok(SendIntent {
            id: tx.last_insert_rowid(),
            answer,
            send_key,
            kind: kind.to_string(),
            request: request.clone(),
        })
    }

    /// Intent -> Sending. Must be committed before the Bot API is touched.
    pub fn start_send(&mut self, intent: &SendIntent) -> Result<()> {
        let tx = self.handle();
        let changed = tx.execute(
            "UPDATE sends SET state='sending', updated_at=? WHERE id=? AND state='intent'",
            rusqlite::params![now(), intent.id],
        )?;
        if changed == 0 {
            let state: Option<String> = tx
                .query_row("SELECT state FROM sends WHERE id=?", [intent.id], |r| {
                    r.get(0)
                })
                .optional()?;
            match state.as_deref() {
                Some("sending") | Some("sent") => return Ok(()),
                Some(other) => {
                    return Err(anyhow!("send {} is {other}, not intent", intent.send_key))
                }
                None => return Err(anyhow!("send {} disappeared", intent.send_key)),
            }
        }
        Ok(())
    }

    /// Sending -> Sent | Unknown. `complete` is the exact response the tool
    /// returns; when present the answer closes and the batch outcome is
    /// Delivered. Mirror row + receipt + transition are one transaction (I4).
    pub fn finish_send(
        &mut self,
        intent: &SendIntent,
        outcome: &SendOutcome,
        complete: Option<&str>,
    ) -> Result<Receipt> {
        let tx = self.handle();
        let state: String = tx
            .query_row("SELECT state FROM sends WHERE id=?", [intent.id], |r| {
                r.get(0)
            })
            .optional()?
            .ok_or_else(|| anyhow!("send {} disappeared", intent.send_key))?;
        if state == "sent" {
            let receipt: String =
                tx.query_row("SELECT receipt FROM sends WHERE id=?", [intent.id], |r| {
                    r.get(0)
                })?;
            let v: Value = serde_json::from_str(&receipt)?;
            return Ok(receipt_from_json(&v, intent));
        }
        if state != "sending" {
            return Err(anyhow!(
                "send {} is {state}, cannot finish it",
                intent.send_key
            ));
        }
        let batch: (i64, Option<i64>) = sans_join(tx, intent.answer)?;
        let ts = now();
        match outcome {
            SendOutcome::Sent(receipt) => {
                let body = receipt.json().to_string();
                tx.execute(
                    "UPDATE sends SET state='sent', receipt=?, updated_at=? WHERE id=? AND state='sending'",
                    rusqlite::params![body, ts, intent.id],
                )?;
                Self::mirror_send(tx, &batch, receipt)?;
                if let Some(response) = complete {
                    tx.execute(
                        "UPDATE answers SET state='closed', response=?, closed_at=? WHERE id=?",
                        rusqlite::params![response, ts, intent.answer.0],
                    )?;
                    tx.execute(
                        "UPDATE batches SET state='closed', outcome='delivered', updated_at=? WHERE id=?",
                        rusqlite::params![ts, batch.0],
                    )?;
                }
                Ok(receipt.clone())
            }
            SendOutcome::Unknown(error) => {
                crate::ops::alert(
                    "send_unknown",
                    &format!("send {}: {error}", intent.send_key),
                );
                let body = json!({"error": error, "state": "unknown"}).to_string();
                tx.execute(
                    "UPDATE sends SET state='unknown', receipt=?, updated_at=? WHERE id=? AND state='sending'",
                    rusqlite::params![body, ts, intent.id],
                )?;
                tx.execute(
                    "INSERT INTO incidents(kind,detail,at) VALUES('send_unknown',?,?)",
                    rusqlite::params![
                        json!({"send_key": intent.send_key, "error": error}).to_string(),
                        ts
                    ],
                )?;
                tx.execute(
                    "UPDATE batches SET state='closed', outcome='delivered_unknown', updated_at=? WHERE id=?",
                    rusqlite::params![ts, batch.0],
                )?;
                Err(anyhow!("send_unknown: {}", error))
            }
        }
    }

    fn mirror_send(tx: &Transaction, batch: &(i64, Option<i64>), receipt: &Receipt) -> Result<()> {
        let chat: i64 = crate::kv_get(tx, "chat_id", "0").parse().unwrap_or(0);
        let self_id: i64 = crate::kv_get(tx, "self_id", "0").parse().unwrap_or(0);
        if self_id == 0 {
            return Ok(());
        }
        let username = crate::kv_get(tx, "self_username", "");
        let first = crate::kv_get(tx, "self_first", "");
        let media_kind = if receipt.kind == "text" {
            ""
        } else {
            receipt.kind.as_str()
        };
        // A GIF/sticker we sent ourselves is usable again by file_id alone, so
        // it enters the catalogue as Ready; other own-sent media stays a
        // descriptor (the file lives on disk, not in the media store).
        let media_state = if media_kind.is_empty() {
            "absent"
        } else if media_kind == "animation" || media_kind == "sticker" {
            "ready"
        } else {
            "descriptor"
        };
        tx.execute(
            "INSERT OR IGNORE INTO messages(chat_id,update_id,message_id,thread_id,date,user_id,username,first_name,text,reply_to,media_path,file_id,file_unique_id,media_kind,media_state,media_size)
             VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            rusqlite::params![
                chat,
                -receipt.message_id,
                receipt.message_id,
                batch.1,
                now(),
                self_id,
                username,
                first,
                receipt.text,
                receipt.reply_to,
                receipt.media_path,
                receipt.file_id,
                receipt.file_unique_id,
                media_kind,
                media_state,
                receipt.media_size as i64,
            ],
        )?;
        Ok(())
    }
}

fn sans_join(tx: &Transaction, answer: AnswerId) -> Result<(i64, Option<i64>)> {
    tx.query_row(
        "SELECT b.id, b.thread_id FROM answers a JOIN batches b ON b.id=a.batch_id WHERE a.id=?",
        [answer.0],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
    .map_err(|e| anyhow!("answer {} has no batch: {e}", answer.0))
}

fn receipt_from_json(v: &Value, intent: &SendIntent) -> Receipt {
    Receipt {
        message_id: v.get("message_id").and_then(|x| x.as_i64()).unwrap_or(0),
        reply_to: v.get("reply_to").and_then(|x| x.as_i64()),
        text: v
            .get("text")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string(),
        kind: v
            .get("kind")
            .and_then(|x| x.as_str())
            .unwrap_or(&intent.kind)
            .to_string(),
        file_id: v
            .get("file_id")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string(),
        file_unique_id: v
            .get("file_unique_id")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string(),
        media_path: v
            .get("media_path")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string(),
        media_size: v.get("media_size").and_then(|x| x.as_u64()).unwrap_or(0),
        thread_id: v.get("thread_id").and_then(|x| x.as_i64()),
    }
}

/// Any Sending left by a previous process becomes Unknown. An `intent` is the
/// same window one step earlier: it was committed before HTTP, so no call was
/// made, but the batch is already `answered` and a retry would only ever get
/// `answer_exists`. Both states are terminal here — nothing is resent, one
/// incident records the whole recovery, and the batch closes with the declared
/// `delivered_unknown` outcome instead of hanging forever.
pub fn resolve_sending(tx: &Transaction) -> Result<usize> {
    let rows: Vec<(i64, String, i64)> = {
        let mut stmt = tx.prepare(
            "SELECT s.id, s.send_key, a.batch_id FROM sends s
               JOIN answers a ON a.id=s.answer_id WHERE s.state IN ('intent','sending')",
        )?;
        let mut out = Vec::new();
        for r in stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))? {
            out.push(r?);
        }
        out
    };
    if rows.is_empty() {
        return Ok(0);
    }
    let ts = now();
    let mut batch_ids: Vec<i64> = rows.iter().map(|r| r.2).collect();
    batch_ids.sort_unstable();
    batch_ids.dedup();
    tx.execute(
        "UPDATE sends SET state='unknown', updated_at=? WHERE state IN ('intent','sending')",
        [ts],
    )?;
    for bid in &batch_ids {
        tx.execute(
            "UPDATE batches SET state='closed', outcome='delivered_unknown', updated_at=? WHERE id=?",
            rusqlite::params![ts, bid],
        )?;
    }
    let detail = json!({
        "count": rows.len(),
        "send_keys": rows.iter().map(|r| r.1.clone()).collect::<Vec<_>>(),
        "batches": batch_ids,
    });
    tx.execute(
        "INSERT INTO incidents(kind,detail,at) VALUES('send_unknown_recovered',?,?)",
        rusqlite::params![detail.to_string(), ts],
    )?;
    Ok(rows.len())
}

/// Close a batch with a ledger outcome. Called by track, not by kv marks.
pub fn close_batch(tx: &Transaction, key: &str, outcome: &str) -> Result<()> {
    tx.execute(
        "UPDATE batches SET state='closed', outcome=?, updated_at=? WHERE batch_key=?",
        rusqlite::params![outcome, now(), key],
    )?;
    Ok(())
}

fn read_view(tx: &Transaction, key: &str) -> Result<Option<BatchView>> {
    view(tx, key)
}

/// Read everything track needs about one batch in one place.
pub fn view(con: &Connection, key: &str) -> Result<Option<BatchView>> {
    let row: Option<(i64, String, String, Option<String>, Option<i64>, Option<i64>, Option<String>, Option<String>)> = con
        .query_row(
            "SELECT id,batch_key,state,outcome,chat_id,thread_id,message_ids,tome FROM batches WHERE batch_key=?",
            [key],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?, r.get(7)?)),
        )
        .optional()?;
    let Some((id, batch_key, state, outcome, chat_id, thread_id, mids, tome)) = row else {
        return Ok(None);
    };
    let answers: i64 =
        con.query_row("SELECT COUNT(*) FROM answers WHERE batch_id=?", [id], |r| {
            r.get(0)
        })?;
    let sent: i64 = con.query_row(
        "SELECT COUNT(*) FROM sends s JOIN answers a ON a.id=s.answer_id WHERE a.batch_id=? AND s.state='sent'",
        [id],
        |r| r.get(0),
    )?;
    let pending_sends: i64 = con.query_row(
        "SELECT COUNT(*) FROM sends s JOIN answers a ON a.id=s.answer_id WHERE a.batch_id=? AND s.state IN ('intent','sending')",
        [id],
        |r| r.get(0),
    )?;
    let unknown_sends: i64 = con.query_row(
        "SELECT COUNT(*) FROM sends s JOIN answers a ON a.id=s.answer_id WHERE a.batch_id=? AND s.state='unknown'",
        [id],
        |r| r.get(0),
    )?;
    let marks: (Option<i64>, Option<i64>, Option<i64>) = con
        .query_row(
            "SELECT
               MIN(started_at),
               MAX(emitted_at),
               MAX(settled_at)
             FROM attempts WHERE batch_id=?",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap_or((None, None, None));
    Ok(Some(BatchView {
        id,
        batch_key,
        state,
        outcome,
        chat_id,
        thread_id,
        message_ids: parse_ids(mids),
        tome: parse_ids(tome),
        answers,
        sent,
        pending_sends,
        unknown_sends,
        admitted_at: marks.0,
        emitted_at: marks.1,
        settled_at: marks.2,
    }))
}

/// Batch keys that are not closed yet, oldest first, for boot recovery.
pub fn open_batches(con: &Connection) -> Result<Vec<String>> {
    let mut stmt = con.prepare(
        "SELECT batch_key FROM batches WHERE state <> 'closed' ORDER BY sealed_at ASC, id ASC",
    )?;
    let mut out = Vec::new();
    for k in stmt.query_map([], |r| r.get::<_, String>(0))? {
        out.push(k?);
    }
    Ok(out)
}

pub fn message_ids(con: &Connection, key: &str) -> Result<Vec<i64>> {
    let raw: Option<String> = con
        .query_row(
            "SELECT message_ids FROM batches WHERE batch_key=?",
            [key],
            |r| r.get(0),
        )
        .optional()?;
    Ok(parse_ids(raw))
}

pub fn thread_id(con: &Connection, key: &str) -> Option<i64> {
    con.query_row(
        "SELECT thread_id FROM batches WHERE batch_key=?",
        [key],
        |r| r.get(0),
    )
    .optional()
    .ok()
    .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_state(tag: &str) -> String {
        let dir = std::env::temp_dir().join(format!("chat-ledger-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir.to_string_lossy().to_string()
    }

    async fn write_seal(db: &crate::db::Db) {
        db.call_blocking(|con| {
            crate::kv_set(con, "chat_id", "-100")?;
            Ok(())
        })
        .unwrap();
        db.write(move |tx| {
            tx.seal_batch("sig-a", -100, Some(77), &[11, 12], &[])?;
            tx.mark_pushed("sig-a")
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn second_distinct_answer_is_refused_and_same_fingerprint_replays() {
        let state = temp_state("answer");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let db = crate::db::Db::open(&state).unwrap();
        write_seal(&db).await;
        let opened = db
            .write(|tx| tx.open_answer("sig-a", "fp-1"))
            .await
            .unwrap();
        assert!(matches!(opened, OpenAnswer::Opened(_)), "{opened:?}");
        let replay = db
            .write(|tx| tx.open_answer("sig-a", "fp-1"))
            .await
            .unwrap();
        assert!(matches!(replay, OpenAnswer::Replay { .. }), "{replay:?}");
        let refused = db.write(|tx| tx.open_answer("sig-a", "fp-2")).await;
        let err = refused.unwrap_err().to_string();
        assert!(err.contains("answer_exists"), "{err}");
        let _ = std::fs::remove_dir_all(&state);
    }
}
