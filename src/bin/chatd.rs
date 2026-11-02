use anyhow::{anyhow, Result};
use chat_bot::db::Db;
use chat_bot::*;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let arg = std::env::args().nth(1).unwrap_or_default();
    // One thread, cooperative scheduling: a LocalSet lets the delivery units be
    // spawned side by side. Database work is the one exception: it runs on the
    // `chatdb` thread of `Db`, so a blocking sqlite call no longer stops the
    // runtime that polls Telegram and writes chat pushes.
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            match arg.as_str() {
                "run" => cmd_run().await,
                "sync" => cmd_sync().await,
                "summarize" => cmd_summarize().await,
                _ => {
                    eprintln!("usage: chatd run|sync|summarize");
                    std::process::exit(2);
                }
            }
        })
        .await
}

async fn cmd_sync() -> Result<()> {
    let state = state_dir();
    let con = db(&state)?;
    let mut offset: i64 = kv_get(&con, "offset", "0").parse().unwrap_or(0);
    loop {
        let updates = api("getUpdates", &json!({"offset": offset, "timeout": 3, "limit": 100})).await?;
        let arr = updates.as_array().cloned().unwrap_or_default();
        let n = arr.len();
        if n == 0 {
            break;
        }
        for u in arr {
            let uid = u.get("update_id").and_then(|v| v.as_i64()).unwrap_or(0);
            offset = offset.max(uid + 1);
            if let Some(msg) = u.get("message").cloned() {
                let mut msg = msg;
                msg["_update_id"] = json!(uid);
                if let Some(prep) = prepare_message(&msg, &state, true).await? {
                    let _ = store_prepared(&con, &prep, &state)?;
                }
            }
        }
        kv_set(&con, "offset", &offset.to_string())?;
        if n < 100 {
            break;
        }
    }
    println!("synced, offset {}", kv_get(&con, "offset", "0"));
    Ok(())
}

async fn cmd_summarize() -> Result<()> {
    let state = state_dir();
    let con = db(&state)?;
    let mut stmt = con.prepare("SELECT username, first_name, text FROM messages ORDER BY update_id DESC LIMIT 500")?;
    let rows: Vec<(Option<String>, Option<String>, Option<String>)> =
        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?.flatten().collect();
    let mut lines = vec![];
    for (u, n, t) in rows.iter().rev() {
        if let Some(t) = t {
            if t.is_empty() {
                continue;
            }
            let who = u.clone().unwrap_or_default();
            let who = if who.is_empty() { n.clone().unwrap_or_default() } else { who };
            let short: String = t.chars().take(300).collect();
            lines.push(format!("{who}: {short}"));
        }
    }
    let digest = lines.join("\n");
    let system_path = cfg("CHATBOT_SYSTEM", "system-prompt.md");
    let (sid, sock) = summarize_isolated_session(&system_path).await?;
    let res = summarize_prompt(&sid, &sock, &digest).await;
    close_summary_session(&sid, &sock).await;
    return res;
}

async fn summarize_prompt(sid: &str, sock: &str, digest: &str) -> Result<()> {
    // Every call below must go through the scratch summary socket: the sid
    // exists only there, never on the chat host (isolation requirement).
    omo_on(&["wait", sid, "--until", "idle", "--timeout", "600000"], Duration::from_secs(620), sock).await?;
    let prompt = format!(
        "Ниже история чата (свежие внизу). Собери chat-summary.md: смысловые темы, кто есть кто, тон и стиль, табу, где боту уместно влезать. Отвечай только текстом файла.\n\n{}",
        digest.chars().take(60000).collect::<String>()
    );
    omo_on(&["prompt", sid, &prompt], Duration::from_secs(120), sock).await?;
    omo_on(&["wait", sid, "--until", "idle", "--timeout", "600000"], Duration::from_secs(620), sock).await?;
    let data = omo_on(&["read", sid, "--tail", "3", "--messages"], Duration::from_secs(60), sock).await?;
    if let Some(msgs) = data.get("messages").and_then(|v| v.as_array()) {
        for m in msgs.iter().rev() {
            if m.get("role").and_then(|v| v.as_str()) != Some("assistant") {
                continue;
            }
            let mut texts = vec![];
            if let Some(content) = m.get("content").and_then(|v| v.as_array()) {
                for c in content {
                    if c.get("type").and_then(|v| v.as_str()) == Some("text") {
                        if let Some(t) = c.get("text").and_then(|v| v.as_str()) {
                            if !t.is_empty() {
                                texts.push(t.to_string());
                            }
                        }
                    }
                }
            }
            if !texts.is_empty() {
                std::fs::write("chat-summary.md", texts.join("\n"))?;
                dlog("chat-summary.md updated");
                return Ok(());
            }
        }
    }
    dlog("no summary produced");
    Ok(())
}

/// Delivery units: ingest (Telegram), admit (control socket + the ONLY session-lifecycle
/// owner), track (events cursor + delivery proofs + recovery requests).
struct Job {
    batch: Vec<BatchMsg>,
}

/// A recovery request from the track unit. `recreate` is false for a batch the
/// runtime never accepted (re-pushing is enough and never touches a
/// live session) and true only when the delivery provably entered a run that
/// then stopped making progress.
struct Recover {
    sig: String,
    recreate: bool,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn accepted_kind(kind: &str) -> bool {
    matches!(
        kind,
        "started" | "steered" | "queued" | "already_admitted"
    )
}

/// Frames written to the extension over the control socket.
enum Out {
    Push {
        delivery_id: String,
        text: String,
        /// message_ids authored by the owner in this batch; the extension keeps
        /// them for the turn and the `subagent` tool demands one as proof that
        /// the request really came from the owner.
        owner_msgs: Vec<i64>,
        /// Incoming message_ids of this batch; `chat_send` refuses a `reply_to`
        /// outside them (history, GIF catalog, another turn). Empty on an old
        /// ledger entry — the extension then disables the check.
        batch_ids: Vec<i64>,
    },
}

/// Frames the extension writes back.
enum Inbound {
    Hello { session: String, pid: i64 },
    Result { delivery_id: String, kind: String },
}

/// The admit unit is the sole owner of session lifecycle: `ensure_session`
/// closes every session carrying CHATBOT_SESSION, so two owners would recreate
/// storm and kill working turns. Everyone else asks through `recover_tx`.
#[allow(clippy::too_many_arguments)]
async fn admit_loop(
    db: Db,
    mut jobs: tokio::sync::mpsc::UnboundedReceiver<Job>,
    ack: tokio::sync::mpsc::UnboundedSender<String>,
    admitted_tx: tokio::sync::mpsc::UnboundedSender<String>,
    inboxed_tx: tokio::sync::mpsc::UnboundedSender<String>,
    out_tx: tokio::sync::mpsc::UnboundedSender<Out>,
    mut inbound_rx: tokio::sync::mpsc::UnboundedReceiver<Inbound>,
    mut recover: tokio::sync::mpsc::UnboundedReceiver<Recover>,
    system_path: String,
) {
    let state = state_dir();
    let ref_ = cfg("CHATBOT_SESSION", "chat-participant");
    let mut sid: Option<String> = None;
    match ensure_session(&system_path, Some(&ref_)).await {
        Ok(s) => {
            let offset = read_events(&state, 0).map(|(_, o)| o).unwrap_or(0);
            let _ = settle_session(&state, &db, offset, Duration::from_secs(180)).await;
            dlog(&format!("admit unit ready, session {s}"));
            sid = Some(s);
        }
        Err(e) => dlog(&format!("initial session ensure failed: {e:#}")),
    }
    recover_pending(
        &db,
        &state,
        &mut sid,
        &system_path,
        &ref_,
        &admitted_tx,
        &inboxed_tx,
        &out_tx,
        &mut inbound_rx,
    )
    .await;
    loop {
        tokio::select! {
            job = jobs.recv() => match job {
                Some(job) => {
                    let res = admit_job(&db, &state, &mut sid, &system_path, &ref_, &admitted_tx, &inboxed_tx, &out_tx, &mut inbound_rx, job).await;
                    if let Err(e) = res {
                        dlog(&format!("admit job failed: {e:#}"));
                    }
                    // Every received job is consumed, so the ingest unit can
                    // always release its in-flight slot and commit the offset.
                    let _ = ack.send(String::new());
                }
                None => break,
            },
            req = recover.recv() => {
                if let Some(req) = req {
                    if let Err(e) = recover_batch(&db, &state, &mut sid, &system_path, &ref_, &admitted_tx, &inboxed_tx, &out_tx, &mut inbound_rx, &req).await {
                        dlog(&format!("recover {} failed: {e:#}", req.sig));
                    }
                }
            }
        }
    }
}

/// Executes one read-only control call on the database thread. The connection is
/// waited on with the connection's own 300 ms busy_timeout, so a contended
/// database is answered promptly instead of freezing the caller.
async fn handle_control_call(db: &Db, op: &str, args: &Value) -> Result<String> {
    match op {
        "gif_list" => {
            let offset = args.get("offset").and_then(|v| v.as_i64()).unwrap_or(0);
            let limit = args.get("limit").and_then(|v| v.as_i64()).unwrap_or(200);
            db.call(move |con| {
                let (entries, total) = gif_page(con, offset, limit)?;
                Ok(gif_list_text(&entries, total))
            })
            .await
        }
        other => Err(anyhow!("unknown control op: {other}")),
    }
}

/// Hosts the control socket and moves frames between it and the admit unit.
/// One connection at a time, newest wins: a host restart brings a new extension
/// instance that reconnects on its own.
async fn control_loop(
    db: Db,
    sock: std::path::PathBuf,
    state: String,
    mut out_rx: tokio::sync::mpsc::UnboundedReceiver<Out>,
    inbound_tx: tokio::sync::mpsc::UnboundedSender<Inbound>,
) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    // Bind ONCE. Rebinding after every disconnect left a short window where a
    // client connecting at that moment landed in the backlog of a listener that
    // was about to be dropped and got its connection reset. One listener, one
    // hosted connection at a time keeps the "newest connection wins" rule and
    // removes that window.
    unlink_stale_socket(&sock);
    let listener = loop {
        match tokio::net::UnixListener::bind(&sock) {
            Ok(l) => break l,
            Err(e) => {
                dlog(&format!("control bind failed ({}): {e:#}", sock.display()));
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    };
    dlog(&format!("control socket listening on {}", sock.display()));
    loop {
        let stream = match listener.accept().await {
            Ok((s, _)) => s,
            Err(e) => {
                dlog(&format!("control accept failed: {e:#}"));
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };
        dlog("extension connected to the control socket");
        let (rd, mut wr) = stream.into_split();
        let mut rd = tokio::io::BufReader::new(rd);
        let mut line = String::new();
        // Replies to control calls: the database work happens in its own task,
        // the (single) writer drains this queue in submission order.
        let (frames_tx, mut frames_rx) = tokio::sync::mpsc::unbounded_channel::<Value>();
        loop {
            tokio::select! {
                msg = out_rx.recv() => match msg {
                    Some(Out::Push { delivery_id, text, owner_msgs, batch_ids }) => {
                        let frame = json!({"push": {"delivery_id": delivery_id, "text": text, "deliverAs": "steer", "owner_msgs": owner_msgs, "batch_ids": batch_ids}});
                        let ok = match serde_json::to_string(&frame) {
                            Ok(s) => wr.write_all(format!("{s}\n").as_bytes()).await.is_ok() && wr.flush().await.is_ok(),
                            Err(_) => false,
                        };
                        if !ok {
                            break;
                        }
                    }
                    None => return,
                },
                frame = frames_rx.recv() => match frame {
                    Some(frame) => {
                        let ok = match serde_json::to_string(&frame) {
                            Ok(s) => wr.write_all(format!("{s}\n").as_bytes()).await.is_ok() && wr.flush().await.is_ok(),
                            Err(_) => false,
                        };
                        if !ok {
                            break;
                        }
                    }
                    None => return,
                },
                read = rd.read_line(&mut line) => match read {
                    Ok(0) => break,
                    Ok(_) => {
                        let raw = line.trim().to_string();
                        line.clear();
                        if raw.is_empty() {
                            continue;
                        }
                        if let Ok(v) = serde_json::from_str::<Value>(&raw) {
                            if let Some(r) = v.get("result") {
                                let _ = inbound_tx.send(Inbound::Result {
                                    delivery_id: r.get("delivery_id").and_then(|x| x.as_str()).unwrap_or("").to_string(),
                                    kind: r.get("kind").and_then(|x| x.as_str()).unwrap_or("").to_string(),
                                });
                            } else if let Some(h) = v.get("hello") {
                                let _ = inbound_tx.send(Inbound::Hello {
                                    session: h.get("session").and_then(|x| x.as_str()).unwrap_or("").to_string(),
                                    pid: h.get("pid").and_then(|x| x.as_i64()).unwrap_or(0),
                                });
                            } else if let Some(c) = v.get("call") {
                                // Read-only request/response on the SAME socket:
                                // the extension asks the daemon for sqlite data it
                                // has no business opening itself (gif_list). The
                                // database call runs in its OWN task: awaiting a
                                // queued/contended call here would stop polling
                                // `out_rx` and stall chat push delivery.
                                let id = c.get("id").and_then(|x| x.as_str()).unwrap_or("").to_string();
                                let op = c.get("op").and_then(|x| x.as_str()).unwrap_or("").to_string();
                                let args = c.get("args").cloned().unwrap_or(json!({}));
                                let db = db.clone();
                                let frames_tx = frames_tx.clone();
                                tokio::task::spawn_local(async move {
                                    let frame = match handle_control_call(&db, &op, &args).await {
                                        Ok(text) => {
                                            dlog(&format!("control call {op} ok ({} chars)", text.chars().count()));
                                            json!({"return": {"id": id, "ok": true, "text": text}})
                                        }
                                        Err(e) => {
                                            let detail = format!("{e:#}");
                                            let lowered = detail.to_lowercase();
                                            let msg = if lowered.contains("locked") || lowered.contains("busy") {
                                                "busy".to_string()
                                            } else {
                                                detail
                                            };
                                            dlog(&format!("control call {op} failed: {msg}"));
                                            json!({"return": {"id": id, "ok": false, "error": msg}})
                                        }
                                    };
                                    let _ = frames_tx.send(frame);
                                });
                            }
                        }
                    }
                    Err(_) => break,
                },
            }
        }
        let _ = &state;
        dlog("extension disconnected from the control socket; waiting for the next connection");
    }
}

async fn ensure_sid(sid: &mut Option<String>, system_path: &str, ref_: &str) -> Result<String> {
    if let Some(s) = sid.clone() {
        if host_alive() {
            return Ok(s);
        }
        dlog(&format!("host socket missing ({}), re-ensuring host", host_socket()));
    }
    let s = ensure_session(system_path, Some(ref_)).await?;
    *sid = Some(s.clone());
    Ok(s)
}

#[allow(clippy::too_many_arguments)]
async fn admit_job(
    db: &Db,
    state: &str,
    sid_slot: &mut Option<String>,
    system_path: &str,
    ref_: &str,
    admitted_tx: &tokio::sync::mpsc::UnboundedSender<String>,
    inboxed_tx: &tokio::sync::mpsc::UnboundedSender<String>,
    out_tx: &tokio::sync::mpsc::UnboundedSender<Out>,
    inbound_rx: &mut tokio::sync::mpsc::UnboundedReceiver<Inbound>,
    job: Job,
) -> Result<()> {
    let batch = job.batch;
    // One closure: building the text and writing its pending ledger entry must
    // not be separated by another unit's write.
    let state_for_db = state.to_string();
    let (text, sig, ids, tome, owners, anchor) = {
        db.call(move |con| build_batch_text(&state_for_db, con, &batch)).await?
    };
    if ids.is_empty() {
        dlog("flush skipped: every batch row already answered");
        return Ok(());
    }
    {
        let (sig_c, text_c, ids_c, tome_c, owners_c, anchor_c) = (
            sig.clone(),
            text.clone(),
            ids.clone(),
            tome.clone(),
            owners.clone(),
            anchor.clone(),
        );
        db.call(move |con| {
            kv_set(con, &format!("pending_batch_ids:{sig_c}"), &serde_json::to_string(&ids_c)?)?;
            kv_set(con, &format!("pending_window:{sig_c}"), &anchor_c)?;
            kv_set(con, &format!("pending_tome:{sig_c}"), &serde_json::to_string(&tome_c)?)?;
            kv_set(con, &format!("pending_owner:{sig_c}"), &serde_json::to_string(&owners_c)?)?;
            kv_set(con, &format!("pending_text:{sig_c}"), &text_c)?;
            kv_set(con, &format!("pending_deliverAs:{sig_c}"), "steer")?;
            pending_marks_clear(con, &sig_c)?;
            let mut pending = pending_ids(con);
            if !pending.contains(&sig_c) {
                pending.push(sig_c.clone());
                save_pending(con, &pending)?;
            }
            Ok(())
        })
        .await?;
    }
    // Registration with the track clock happens BEFORE the push: a batch the
    // runtime refuses must keep its retry clock.
    let _ = inboxed_tx.send(sig.clone());
    deliver_sig(
        db,
        sid_slot,
        system_path,
        ref_,
        admitted_tx,
        out_tx,
        inbound_rx,
        &sig,
        &text,
        &owners,
        &ids,
    )
    .await
}

/// Push the batch over the socket and wait for an ACCEPTED admission. A refusal
/// (`turn_conflict`, `held_draft`) took nothing, so it is never reported as
/// delivered; the push is repeated and the track clock re-drives it later.
#[allow(clippy::too_many_arguments)]
async fn deliver_sig(
    db: &Db,
    sid_slot: &mut Option<String>,
    system_path: &str,
    ref_: &str,
    admitted_tx: &tokio::sync::mpsc::UnboundedSender<String>,
    out_tx: &tokio::sync::mpsc::UnboundedSender<Out>,
    inbound_rx: &mut tokio::sync::mpsc::UnboundedReceiver<Inbound>,
    sig: &str,
    text: &str,
    owners: &[i64],
    ids: &[i64],
) -> Result<()> {
    let first = Duration::from_secs_f64(cfg("CHATBOT_ADMIT_FIRST_SEC", "8").parse().unwrap_or(8.0));
    let rest = Duration::from_secs_f64(cfg("CHATBOT_ADMIT_SEC", "10").parse().unwrap_or(10.0));
    // A dead host cannot run the extension, so nothing would answer the push:
    // re-ensure it before the first frame instead of burning the budget.
    if !host_alive() {
        dlog(&format!(
            "host socket missing ({}), re-ensuring before delivery of {sig}",
            host_socket()
        ));
        let _ = ensure_sid(sid_slot, system_path, ref_).await;
    }
    for cycle in 0..3 {
        let budget = if cycle == 0 { first } else { rest };
        if out_tx
            .send(Out::Push {
                delivery_id: sig.to_string(),
                text: text.to_string(),
                owner_msgs: owners.to_vec(),
                batch_ids: ids.to_vec(),
            })
            .is_err()
        {
            dlog(&format!("control unit gone; cannot push {sig}"));
            return Ok(());
        }
        let deadline = tokio::time::Instant::now() + budget;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, inbound_rx.recv()).await {
                Ok(Some(Inbound::Result { delivery_id, kind })) => {
                    if delivery_id != sig {
                        continue;
                    }
                    if accepted_kind(&kind) {
                        let at = now_ms();
                        let sig_db = sig.to_string();
                        db.call(move |con| pending_mark_set(con, &sig_db, "admitted_at", at))
                            .await?;
                        let _ = admitted_tx.send(sig.to_string());
                        dlog(&format!("admitted {sig} ({kind})"));
                        let _ = ensure_sid(sid_slot, system_path, ref_).await;
                        return Ok(());
                    }
                    dlog(&format!("deferred {sig} ({kind}): the runtime took nothing, retrying"));
                    break;
                }
                Ok(Some(Inbound::Hello { session, pid })) => {
                    dlog(&format!("extension hello (session={session}, pid={pid})"));
                    continue;
                }
                Ok(None) => return Ok(()),
                Err(_) => break,
            }
        }
    }
    dlog(&format!("no admission for {sig} after 3 cycles; track will retry on its delivery clock"));
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn recover_batch(
    db: &Db,
    state: &str,
    sid_slot: &mut Option<String>,
    system_path: &str,
    ref_: &str,
    admitted_tx: &tokio::sync::mpsc::UnboundedSender<String>,
    inboxed_tx: &tokio::sync::mpsc::UnboundedSender<String>,
    out_tx: &tokio::sync::mpsc::UnboundedSender<Out>,
    inbound_rx: &mut tokio::sync::mpsc::UnboundedReceiver<Inbound>,
    req: &Recover,
) -> Result<()> {
    let sig = req.sig.as_str();
    // Regenerating the text and reading its owner set is one consistent view of
    // the same batch: one closure for both.
    let state_for_db = state.to_string();
    let sig_for_db = sig.to_string();
    let (text, owners, ids) = db
        .call(move |con| {
            let text = push_pending(&state_for_db, con, &sig_for_db)?;
            let owners = pending_owner_ids(con, &sig_for_db);
            let ids = pending_batch_ids(con, &sig_for_db);
            Ok((text, owners, ids))
        })
        .await?;
    let text = match text {
        Some(t) => t,
        None => {
            dlog(&format!("recover {sig}: nothing left to ask"));
            return Ok(());
        }
    };
    if req.recreate {
        dlog(&format!("recover {sig}: recreating session and re-delivering"));
        if let Some(sid) = sid_slot.clone() {
            match recreate_session(&sid, system_path, Some(ref_), &format!("stalled delivery {sig}")).await {
                Ok(new_sid) => {
                    let offset = read_events(state, 0).map(|(_, o)| o).unwrap_or(0);
                    let _ = settle_session(state, db, offset, Duration::from_secs(120)).await;
                    *sid_slot = Some(new_sid);
                }
                Err(e) => dlog(&format!("recreate for {sig} failed: {e:#}")),
            }
        } else if let Ok(s) = ensure_session(system_path, Some(ref_)).await {
            *sid_slot = Some(s);
        }
        // Delivery evidence is never wiped here: an admitted/emitted/settled
        // mark is the only thing that stops a restart from re-asking a message
        // the agent already saw (a recreate once cleared them and the next boot
        // re-delivered the batch, producing a second answer). A fresh attempt
        // overwrites admitted_at on acceptance.
    } else if !host_alive() {
        // Never-accepted batch on a dead host: the socket, not the session, is
        // what is missing, so re-ensure host+session and then re-deliver.
        dlog(&format!("recover {sig}: host socket missing, re-ensuring"));
        let _ = ensure_sid(sid_slot, system_path, ref_).await;
    } else {
        // Never-accepted batch, live host: re-push only. A live session is not
        // touched at all.
        dlog(&format!("recover {sig}: re-pushing (no session change)"));
    }
    let _ = inboxed_tx.send(sig.to_string());
    deliver_sig(db, sid_slot, system_path, ref_, admitted_tx, out_tx, inbound_rx, sig, &text, &owners, &ids).await
}

#[allow(clippy::too_many_arguments)]
async fn recover_pending(
    db: &Db,
    state: &str,
    sid_slot: &mut Option<String>,
    system_path: &str,
    ref_: &str,
    admitted_tx: &tokio::sync::mpsc::UnboundedSender<String>,
    inboxed_tx: &tokio::sync::mpsc::UnboundedSender<String>,
    out_tx: &tokio::sync::mpsc::UnboundedSender<Out>,
    inbound_rx: &mut tokio::sync::mpsc::UnboundedReceiver<Inbound>,
) {
    // One pass: the pending list and the answered set (which reads the events
    // journal) are taken once, then reused for every pending sig.
    let state_for_db = state.to_string();
    let (pending, answered) = match db
        .call(move |con| {
            let done = done_ids(con);
            let pending: Vec<String> = pending_ids(con)
                .into_iter()
                .filter(|s| !done.contains(s))
                .collect();
            Ok((pending, answered_ids(&state_for_db, con)))
        })
        .await
    {
        Ok(v) => v,
        Err(_) => return,
    };
    for sig in pending {
        let state_for_db = state.to_string();
        let sig_db = sig.clone();
        let answered_db = answered.clone();
        // One closure per sig: retirement decision and marks come from the same
        // database snapshot.
        let (all_answered, marks, text) = match db
            .call(move |con| {
                let raw = kv_get(con, &format!("pending_batch_ids:{sig_db}"), "");
                let ids: Vec<serde_json::Value> = serde_json::from_str(&raw).unwrap_or_default();
                let ids: Vec<i64> = ids.iter().filter_map(|v| v.as_i64()).collect();
                let all = !ids.is_empty() && ids.iter().all(|i| answered_db.contains(i));
                let text = match regen_text(&state_for_db, con, &sig_db) {
                    Ok(Some(t)) => t,
                    Ok(None) => kv_get(con, &format!("pending_text:{sig_db}"), ""),
                    Err(_) => String::new(),
                };
                let marks = (
                    pending_mark_get(con, &sig_db, "admitted_at"),
                    pending_mark_get(con, &sig_db, "emitted_at"),
                    pending_mark_get(con, &sig_db, "settled_at"),
                );
                Ok((all, marks, text))
            })
            .await
        {
            Ok(v) => v,
            Err(_) => return,
        };
        // A run may settle before the runtime reports the delivery proof, so a
        // batch can sit with `emitted` and no `settled`. Repair it from the
        // journal at boot instead of re-asking a message the agent already saw.
        let mut marks = marks;
        if marks.1.is_some() && marks.2.is_none() {
            if let Some(settled) = last_event_ts(state, "agent_settled") {
                if marks.0.map(|a| settled >= a).unwrap_or(false) {
                    let sig_db = sig.clone();
                    let _ = db
                        .call(move |con| pending_mark_set(con, &sig_db, "settled_at", settled))
                        .await;
                    marks.2 = Some(settled);
                    dlog(&format!("bootstrap: {sig} settled late, recorded {settled}"));
                }
            }
        }
        if let Some(reason) = should_retire(all_answered, marks.1, marks.2) {
            let state_for_db = state.to_string();
            let sig_db = sig.clone();
            let _ = db
                .call(move |con| mark_done(con, &state_for_db, &sig_db))
                .await;
            dlog(&format!("bootstrap: {sig} retired ({reason})"));
            continue;
        }
        if text.is_empty() {
            continue;
        }
        dlog(&format!(
            "bootstrap: re-delivering pending {sig} ({})",
            delivery_phase(marks.0, marks.1, marks.2)
        ));
        let _ = inboxed_tx.send(sig.clone());
        let sig_db = sig.clone();
        let (owners, ids) = db
            .call(move |con| {
                Ok((
                    pending_owner_ids(con, &sig_db),
                    pending_batch_ids(con, &sig_db),
                ))
            })
            .await
            .unwrap_or_default();
        if deliver_sig(
            db,
            sid_slot,
            system_path,
            ref_,
            admitted_tx,
            out_tx,
            inbound_rx,
            &sig,
            &text,
            &owners,
            &ids,
        )
        .await
        .is_err()
        {
            dlog(&format!("bootstrap: {sig} delivery failed, kept for later"));
        }
    }
}

/// Track unit: owns the events cursor, mirrors receipts, records delivery
/// proofs, retires batches and asks the admit unit to recover stalled ones.
async fn track_loop(
    db: Db,
    mut admitted_rx: tokio::sync::mpsc::UnboundedReceiver<String>,
    mut inboxed_rx: tokio::sync::mpsc::UnboundedReceiver<String>,
    recover_tx: tokio::sync::mpsc::UnboundedSender<Recover>,
) {
    let state = state_dir();
    let turn_budget: u64 = cfg("CHATBOT_TURN_SEC", "420").parse().unwrap_or(420);
    let extra: u64 = cfg("CHATBOT_TURN_EXTRA_SEC", "420").parse().unwrap_or(420);
    let retry_budget: u64 = cfg("CHATBOT_RETRY_SEC", "60").parse().unwrap_or(60);
    let emit_warn: u64 = cfg("CHATBOT_EMIT_SEC", "60").parse().unwrap_or(60);
    let mut offset = match read_events(&state, 0) {
        Ok((_, o)) => o,
        Err(_) => 0,
    };
    let mut inflight: HashSet<String> = HashSet::new();
    let mut last_progress: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    let mut emit_warned: HashSet<String> = HashSet::new();
    let mut last_working_at: i64 = 0;
    // Timestamp of the last fully settled run. The runtime may report the
    // delivery proof AFTER the run settled (observed live 2026-11-01: emitted
    // 11s after the answer, i.e. after agent_settled), so the settle must be
    // re-applied to a proof that arrives late — otherwise the batch never
    // retires and a restart re-asks it.
    let mut last_settled_at: i64 = 0;
    // A run is in flight from its turn_start until it ends or settles. While
    // it is, no recovery may touch the session: the extension emits no events
    // during a long model step, so silence there is normal.
    let mut turn_in_flight = false;
    let mut last_event_at = now_ms();
    loop {
        while let Ok(sig) = admitted_rx.try_recv() {
            last_progress.insert(sig.clone(), now_ms());
            inflight.insert(sig);
        }
        while let Ok(sig) = inboxed_rx.try_recv() {
            last_progress.insert(sig.clone(), now_ms());
            inflight.insert(sig);
        }
        match read_events(&state, offset) {
            Ok((events, noff)) => {
                offset = noff;
                if !events.is_empty() {
                    last_event_at = now_ms();
                }
                // Receipts of the whole cycle are mirrored in ONE closure: the
                // event loop used to open a connection per receipt.
                let mut receipts: Vec<Value> = vec![];
                for ev in &events {
                    let kind = ev.get("kind").and_then(|v| v.as_str()).unwrap_or("");
                    match kind {
                        "chat_send_result" | "chat_send_replay" => {
                            if let Some(r) = ev.get("receipt") {
                                receipts.push(r.clone());
                            }
                        }
                        "agent_activity" => {
                            if activity_phase(ev) == Some(true) {
                                last_working_at = now_ms();
                            }
                        }
                        "turn_start" => turn_in_flight = true,
                        "turn_end" => {
                            // A turn boundary is liveness only: the run may
                            // continue with another turn, so it never proves
                            // the agent is finished with the batch.
                            turn_in_flight = false;
                        }
                        "agent_end_aborted" => {
                            // An aborted/deadline run is not a deliberate skip:
                            // it stops the liveness clock and nothing else.
                            turn_in_flight = false;
                        }
                        "agent_settled" => {
                            turn_in_flight = false;
                            let at = ev.get("t").and_then(|v| v.as_i64()).unwrap_or_else(now_ms);
                            last_settled_at = at;
                            let sigs: Vec<String> = inflight.iter().cloned().collect();
                            if !sigs.is_empty() {
                                let _ = db
                                    .call(move |con| {
                                        for sig in &sigs {
                                            if pending_mark_get(con, sig, "emitted_at").is_some() {
                                                let _ = pending_mark_set(con, sig, "settled_at", at);
                                            }
                                        }
                                        Ok(())
                                    })
                                    .await;
                            }
                        }
                        "delivery_state" => {
                            let id = ev
                                .get("delivery_id")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string();
                            let st = ev.get("state").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let at = ev.get("t").and_then(|v| v.as_i64()).unwrap_or_else(now_ms);
                            match st.as_str() {
                                "emitted" => {
                                    // The context entry exists: the agent has
                                    // the batch. Not a close, just progress.
                                    let id_db = id.clone();
                                    let _ = db
                                        .call(move |con| {
                                            pending_mark_set(con, &id_db, "emitted_at", at)?;
                                            let admitted =
                                                pending_mark_get(con, &id_db, "admitted_at").unwrap_or(0);
                                            if last_settled_at > 0 && last_settled_at >= admitted {
                                                pending_mark_set(con, &id_db, "settled_at", last_settled_at)?;
                                            }
                                            Ok(())
                                        })
                                        .await;
                                    last_progress.insert(id.clone(), at);
                                }
                                "failed" => {
                                    // Refused by the session file: admissible
                                    // again, so the admit unit re-pushes it.
                                    let err = ev.get("error").and_then(|v| v.as_str()).unwrap_or("");
                                    dlog(&format!("delivery {id} failed ({err}); asking admit to re-push"));
                                    let _ = recover_tx.send(Recover {
                                        sig: id.clone(),
                                        recreate: false,
                                    });
                                    last_progress.insert(id.clone(), at);
                                }
                                _ => {}
                            }
                        }
                        "compacted" => {
                            let names: Vec<String> = ev
                                .get("delivery_ids")
                                .and_then(|v| v.as_array())
                                .map(|a| {
                                    a.iter()
                                        .map(|x| x.as_str().unwrap_or("").to_string())
                                        .collect()
                                })
                                .unwrap_or_default();
                            for sig in names {
                                if inflight.contains(&sig) {
                                    let _ = recover_tx.send(Recover {
                                        sig: sig.clone(),
                                        recreate: false,
                                    });
                                }
                            }
                        }
                        _ => {}
                    }
                }
                if !receipts.is_empty() {
                    let _ = db
                        .call(move |con| mirror_receipts(con, &receipts))
                        .await;
                }
            }
            Err(e) => dlog(&format!("track read_events failed: {e:#}")),
        }
        let now = now_ms();
        // A run whose end event never arrived must not block recovery forever:
        // silence past the hard cap means "no longer in flight".
        if turn_in_flight && now.saturating_sub(last_event_at) > ((turn_budget + extra) as i64) * 1000 {
            dlog("run liveness went quiet past the hard cap; treating it as no longer in flight");
            turn_in_flight = false;
        }
        let working = turn_in_flight || now.saturating_sub(last_working_at) < 30_000;
        // ONE database snapshot per cycle for every in-flight batch: the answered
        // set (which reads the events journal) is computed once instead of once
        // per sig package.
        let sigs: Vec<String> = inflight.iter().cloned().collect();
        if !sigs.is_empty() {
            let state_for_db = state.clone();
            let snap = db
                .call(move |con| {
                    let answered = answered_ids(&state_for_db, con);
                    let mut out = vec![];
                    for sig in &sigs {
                        let raw = kv_get(con, &format!("pending_batch_ids:{sig}"), "");
                        let ids: Vec<serde_json::Value> = serde_json::from_str(&raw).unwrap_or_default();
                        let ids: Vec<i64> = ids.iter().filter_map(|v| v.as_i64()).collect();
                        let all = !ids.is_empty() && ids.iter().all(|i| answered.contains(i));
                        out.push((
                            sig.clone(),
                            all,
                            pending_mark_get(con, sig, "admitted_at"),
                            pending_mark_get(con, sig, "emitted_at"),
                            pending_mark_get(con, sig, "settled_at"),
                        ));
                    }
                    Ok(out)
                })
                .await;
            if let Ok(snap) = snap {
                let mut retired: Vec<(String, &'static str)> = vec![];
                for (sig, all_answered, admitted_at, emitted_at, settled_at) in snap {
                    if let Some(reason) = should_retire(all_answered, emitted_at, settled_at) {
                        retired.push((sig, reason));
                        continue;
                    }
                    let last = *last_progress.get(&sig).unwrap_or(&now);
                    let phase = delivery_phase(admitted_at, emitted_at, settled_at);
                    // Waiting for the proof is bounded by a full model step: the
                    // extension emits nothing while the model thinks, so a short
                    // emit deadline would fire into a healthy turn.
                    let budget = if phase == "inboxed" { retry_budget } else { turn_budget };
                    if phase != "inboxed"
                        && !emit_warned.contains(&sig)
                        && now.saturating_sub(last) >= (emit_warn as i64) * 1000
                    {
                        emit_warned.insert(sig.clone());
                        dlog(&format!(
                            "batch {sig}: still waiting for the delivery proof after {emit_warn}s"
                        ));
                    }
                    if stalled(now, last, budget, working, extra) {
                        // A run in flight is never recreated; an unaccepted batch
                        // only needs the batch re-pushed.
                        let recreate = !turn_in_flight && phase != "inboxed";
                        dlog(&format!(
                            "batch {sig} stalled in phase {phase} (budget {budget}s, turn_in_flight={turn_in_flight}); recovery recreate={recreate}"
                        ));
                        let _ = recover_tx.send(Recover {
                            sig: sig.clone(),
                            recreate,
                        });
                        last_progress.insert(sig.clone(), now);
                    }
                }
                if !retired.is_empty() {
                    let state_for_db = state.clone();
                    let sigs_db: Vec<String> = retired.iter().map(|(s, _)| s.clone()).collect();
                    let _ = db
                        .call(move |con| {
                            for sig in &sigs_db {
                                let _ = mark_done(con, &state_for_db, sig);
                            }
                            Ok(())
                        })
                        .await;
                    for (sig, reason) in retired {
                        dlog(&format!("batch {sig} retired ({reason})"));
                        inflight.remove(&sig);
                        last_progress.remove(&sig);
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn cmd_run() -> Result<()> {
    // Fail fast on missing required config, BEFORE any network call: a daemon
    // without a chat id starts, logs, and silently drops every incoming
    // message — the "looks healthy but deaf" failure this extraction exists to
    // avoid. required_cfg names the variable in the error.
    required_cfg("CHATBOT_TOKEN_FILE")?;
    required_cfg("CHATBOT_CHAT_ID")?;
    let state = state_dir();
    // The database thread starts before any unit needs it; a broken database
    // fails here, visibly, instead of inside the worker.
    let db = Db::open(&state)?;
    let me = api("getMe", &json!({})).await?;
    let bot_id: i64 = me
        .get("id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| anyhow!("getMe: no id"))?;
    let username = cfg("CHATBOT_USERNAME", "").to_lowercase();
    {
        let self_username = me.get("username").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let self_first = me.get("first_name").and_then(|v| v.as_str()).unwrap_or("").to_string();
        db.call(move |con| {
            kv_set(con, "self_id", &bot_id.to_string())?;
            kv_set(con, "self_username", &self_username)?;
            kv_set(con, "self_first", &self_first)?;
            Ok(())
        })
        .await?;
    }
    let offset: i64 = db
        .call(|con| Ok(kv_get(con, "offset", "0").parse().unwrap_or(0)))
        .await?;
    let system_path = cfg("CHATBOT_SYSTEM", "system-prompt.md");
    let (job_tx, job_rx) = tokio::sync::mpsc::unbounded_channel::<Job>();
    let (ack_tx, ack_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let (admitted_tx, admitted_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let (inboxed_tx, inboxed_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let (recover_tx, recover_rx) = tokio::sync::mpsc::unbounded_channel::<Recover>();
    let (out_tx, out_rx) = tokio::sync::mpsc::unbounded_channel::<Out>();
    let (inbound_tx, inbound_rx) = tokio::sync::mpsc::unbounded_channel::<Inbound>();
    tokio::task::spawn_local(control_loop(
        db.clone(),
        control_sock_path(&state),
        state.clone(),
        out_rx,
        inbound_tx,
    ));
    tokio::task::spawn_local(admit_loop(
        db.clone(),
        job_rx,
        ack_tx,
        admitted_tx,
        inboxed_tx,
        out_tx,
        inbound_rx,
        recover_rx,
        system_path,
    ));
    tokio::task::spawn_local(track_loop(db.clone(), admitted_rx, inboxed_rx, recover_tx));
    dlog(&format!(
        "chatd running, independent units (ingest/admit/track), database on its own thread, control socket {} (host socket {})",
        control_sock_path(&state).display(),
        host_socket()
    ));
    ingest_loop(db, state, bot_id, username, offset, job_tx, ack_rx).await
}

/// Ingest unit: polls Telegram continuously. Parsing a message and downloading
/// its media runs in its own `spawn_local` task, so a slow download never delays
/// the next `getUpdates`; the database write itself goes to the `chatdb` thread
/// as ONE closure (dedup check + insert + prune) and never blocks this loop.
async fn ingest_loop(
    db: Db,
    state: String,
    bot_id: i64,
    username: String,
    mut offset: i64,
    job_tx: tokio::sync::mpsc::UnboundedSender<Job>,
    mut ack_rx: tokio::sync::mpsc::UnboundedReceiver<String>,
) -> Result<()> {
    let window: f64 = cfg("CHATBOT_BATCH_SEC", "10").parse().unwrap_or(10.0);
    let mut batch: Vec<BatchMsg> = vec![];
    let mut poll_failures = 0u32;
    let mut last_flush = std::time::Instant::now();
    let mut committed: i64 = db
        .call(|con| Ok(kv_get(con, "offset", "0").parse().unwrap_or(0)))
        .await?;
    let mut inflight = 0usize;
    let mut pending_stores = 0usize;
    let (row_tx, mut row_rx) = tokio::sync::mpsc::unbounded_channel::<Option<BatchMsg>>();
    loop {
        let updates: Value = match api("getUpdates", &json!({"offset": offset, "timeout": 3, "limit": 100})).await {
            Ok(u) => {
                poll_failures = 0;
                u
            }
            Err(e) => {
                poll_failures += 1;
                let exp = poll_failures.saturating_sub(1).min(4);
                let backoff = (5u64 * (1u64 << exp)).min(60);
                dlog(&format!("poll failed ({poll_failures}x), retry in {backoff}s: {e}"));
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                continue;
            }
        };
        for u in updates.as_array().cloned().unwrap_or_default() {
            let uid = u.get("update_id").and_then(|v| v.as_i64()).unwrap_or(0);
            offset = offset.max(uid + 1);
            let raw_msg = u.get("message").cloned();
            let chan_post = u.get("channel_post").cloned();
            let msg = raw_msg.clone().or(chan_post.clone());
            let mut msg = match msg {
                Some(m) => m,
                None => continue,
            };
            msg["_update_id"] = json!(uid);
            if chan_post.is_some() && raw_msg.is_none() {
                msg["_cross_chat"] = json!(true);
            }
            let strict = raw_msg.is_some();
            let st = state.clone();
            let tx = row_tx.clone();
            let db_task = db.clone();
            pending_stores += 1;
            tokio::task::spawn_local(async move {
                // Contract: every await (media download, ffmpeg preview) happens
                // BEFORE the database closure; the closure receives a finished
                // packet and does dedup+insert+prune atomically.
                let prep = match prepare_message(&msg, &st, strict).await {
                    Ok(v) => v,
                    Err(e) => {
                        // Never swallow this: a config/IO failure here means the
                        // message is lost, and silence would hide it.
                        dlog(&format!("prepare message failed: {e:#}"));
                        None
                    }
                };
                let out = match prep {
                    Some(prep) => {
                        let st2 = st.clone();
                        db_task
                            .call(move |con| store_prepared(con, &prep, &st2))
                            .await
                            .ok()
                            .flatten()
                    }
                    None => None,
                };
                let _ = tx.send(out);
            });
        }
        // Rows finished by the store tasks since the last cycle.
        while let Ok(row) = row_rx.try_recv() {
            pending_stores = pending_stores.saturating_sub(1);
            let mut row = match row {
                Some(r) => r,
                None => continue,
            };
            if row.user_id == Some(bot_id) {
                continue;
            }
            let urgent = row.to_me
                || (!username.is_empty() && row.text.to_lowercase().contains(&format!("@{username}")));
            if urgent {
                row.to_me = true;
                let mut combo = std::mem::take(&mut batch);
                combo.push(row);
                inflight += 1;
                if job_tx.send(Job { batch: combo }).is_err() {
                    dlog("admit unit gone; dropping urgent batch");
                }
                last_flush = std::time::Instant::now();
            } else {
                batch.push(row);
            }
        }
        if !batch.is_empty() && last_flush.elapsed().as_secs_f64() >= window {
            let combo = std::mem::take(&mut batch);
            inflight += 1;
            if job_tx.send(Job { batch: combo }).is_err() {
                dlog("admit unit gone; dropping batch");
            }
            last_flush = std::time::Instant::now();
        }
        // Durable-first offset: commit only once every queued batch has its
        // pending ledger entry and no store task is still running, so a crash
        // re-fetches instead of losing (mirror dedups by message_id).
        while ack_rx.try_recv().is_ok() {
            inflight = inflight.saturating_sub(1);
        }
        if batch.is_empty() && inflight == 0 && pending_stores == 0 && offset != committed {
            let target = offset;
            db.call(move |con| kv_set(con, "offset", &target.to_string())).await?;
            committed = offset;
        }
    }
}
