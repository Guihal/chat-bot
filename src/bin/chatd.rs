use anyhow::{anyhow, Result};
use chat_bot::db::Db;
use chat_bot::ledger;
use chat_bot::ops;
use chat_bot::*;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// When an instance last proved it cannot serve again (`StaleExtensionContextError`).
/// This, not bare absence, is the only evidence that lets the host generation be
/// restarted: the extension reconnects by itself and a running turn must never
/// be thrown away.
static DEAD_GATE_MS: AtomicU64 = AtomicU64::new(0);
/// When a turn was last seen running (turn start or agent working).
static TURN_OPEN_MS: AtomicU64 = AtomicU64::new(0);

fn mark_dead_gate() {
    DEAD_GATE_MS.store(now_ms().max(0) as u64, Ordering::Relaxed);
}

fn mark_turn_open() {
    TURN_OPEN_MS.store(now_ms().max(0) as u64, Ordering::Relaxed);
}

fn dead_gate_recent() -> bool {
    let t = DEAD_GATE_MS.load(Ordering::Relaxed) as i64;
    t > 0 && now_ms().saturating_sub(t) < 600_000
}

fn dead_gate_age_secs() -> u64 {
    let t = DEAD_GATE_MS.load(Ordering::Relaxed) as i64;
    if t <= 0 {
        0
    } else {
        (now_ms().saturating_sub(t) / 1000).max(0) as u64
    }
}

fn turn_recently_open() -> bool {
    let t = TURN_OPEN_MS.load(Ordering::Relaxed) as i64;
    t > 0 && now_ms().saturating_sub(t) < 30_000
}

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
                "config" => cmd_config(),
                "watchdog" => {
                    let loop_mode = std::env::args().nth(2).as_deref() == Some("--loop");
                    cmd_watchdog(loop_mode).await
                }
                _ => {
                    eprintln!(
                        "usage: chatd run|sync|summarize|config --effective|watchdog [--loop]"
                    );
                    std::process::exit(2);
                }
            }
        })
        .await
}

fn cmd_config() -> Result<()> {
    if std::env::args().nth(2).as_deref() != Some("--effective") {
        eprintln!("usage: chatd config --effective");
        std::process::exit(2);
    }
    let token_file = cfg("CHATBOT_TOKEN_FILE", "");
    let present = !token_file.is_empty() && std::fs::metadata(&token_file).is_ok();
    let mode = std::fs::metadata(&token_file)
        .ok()
        .map(|m| format!("{:o}", m.permissions().mode() & 0o777))
        .unwrap_or_else(|| "-".to_string());
    let retention_days: i64 = cfg(
        "CHATBOT_ROW_RETENTION_DAYS",
        &chat_bot::media::ROW_RETENTION_DAYS.to_string(),
    )
    .parse()
    .unwrap_or(chat_bot::media::ROW_RETENTION_DAYS);
    println!("state_dir={}", state_dir());
    println!("host_socket={}", host_socket());
    println!("session={}", cfg("CHATBOT_SESSION", "chat-participant"));
    println!("systemd_unit={}", ops::systemd_unit());
    println!("token_file={token_file}");
    println!("token_file_present={}", if present { "yes" } else { "no" });
    println!("token_file_mode={mode}");
    println!("chat_id={}", cfg("CHATBOT_CHAT_ID", ""));
    println!("retention_days={retention_days}");
    println!("media_cap_bytes={}", chat_bot::media::MEDIA_CAP_BYTES);
    println!("free_reserve_bytes={}", chat_bot::media::FREE_RESERVE_BYTES);
    println!("refuse_free_bytes={}", chat_bot::media::REFUSE_FREE_BYTES);
    println!("stale_threshold_sec={}", ops::stale_threshold_secs());
    println!("watchdog_period_sec={}", ops::watchdog_period_secs());
    println!("notify_cmd={}", cfg("CHATBOT_NOTIFY_CMD", ""));
    println!("agent_dir={}", cfg("CHATBOT_AGENT_DIR", ""));
    Ok(())
}

async fn cmd_watchdog(loop_mode: bool) -> Result<()> {
    let state = state_dir();
    let path = ops::health_path(&state);
    loop {
        let health = ops::read_health(&path).ok();
        let verdict = ops::verdict_for(
            health.as_ref(),
            &ops::unit_state(&ops::systemd_unit()),
            ops::now_secs(),
            ops::stale_threshold_secs(),
            chat_bot::media::FREE_RESERVE_BYTES,
        );
        println!("VERDICT {}", verdict.level);
        for reason in &verdict.reasons {
            println!("reason={reason}");
        }
        if !loop_mode {
            std::process::exit(verdict.exit_code());
        }
        tokio::time::sleep(Duration::from_secs(ops::watchdog_period_secs())).await;
    }
}

async fn cmd_sync() -> Result<()> {
    let state = state_dir();
    let chat_raw = required_cfg("CHATBOT_CHAT_ID")?;
    if chat_bot::parse_chat_id(&chat_raw).is_none() {
        return Err(anyhow!(
            "CHATBOT_CHAT_ID={chat_raw:?} is not a valid numeric Telegram chat id; \
             refusing to open/migrate the database"
        ));
    }
    let _lock = acquire_state_lock(&state)?;
    let db = Db::open(&state)?;
    let mut offset: i64 = db
        .call(|con| Ok(kv_get(con, "offset", "0").parse().unwrap_or(0)))
        .await?;
    let chat: i64 = db
        .call(|con| Ok(kv_get(con, "chat_id", "0").parse().unwrap_or(0)))
        .await?;
    loop {
        let updates = api(
            "getUpdates",
            &json!({"offset": offset, "timeout": 3, "limit": 100}),
        )
        .await?;
        let arr = updates.as_array().cloned().unwrap_or_default();
        let n = arr.len();
        if n == 0 {
            break;
        }
        let mut envelopes = vec![];
        let mut through = offset;
        for u in arr {
            let uid = u.get("update_id").and_then(|v| v.as_i64()).unwrap_or(0);
            through = through.max(uid + 1);
            if let Some(mut msg) = u.get("message").cloned() {
                msg["_update_id"] = json!(uid);
                if let Some(env) = envelope_from_message(&msg, uid, true, chat)? {
                    envelopes.push(env);
                }
            }
        }
        if through > offset {
            db.write(move |tx| tx.commit_page(envelopes, through))
                .await?;
            offset = through;
        }
        if n < 100 {
            break;
        }
    }
    // Rows are already durable and pending; this pass downloads them under
    // the same budget as the daemon.
    let jobs = db
        .call(|con| chat_bot::media::resume_pending(con, chat_bot::media::MEDIA_QUEUE_CAP))
        .await?;
    let source = chat_bot::media::TelegramSource;
    for job in jobs {
        let budget = chat_bot::media::MediaBudget::from_disk(&state);
        if let Err(e) = chat_bot::media::fetch_job(&db, &state, &job, &budget, &source).await {
            dlog(&format!(
                "sync media fetch failed for {}: {e:#}",
                job.row_id
            ));
        }
    }
    println!("synced, offset {}", offset);
    Ok(())
}

async fn cmd_summarize() -> Result<()> {
    let state = state_dir();
    let con = db(&state)?;
    let mut stmt = con.prepare(
        "SELECT username, first_name, text FROM messages ORDER BY update_id DESC LIMIT 500",
    )?;
    let rows: Vec<(Option<String>, Option<String>, Option<String>)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .flatten()
        .collect();
    let mut lines = vec![];
    for (u, n, t) in rows.iter().rev() {
        if let Some(t) = t {
            if t.is_empty() {
                continue;
            }
            let who = u.clone().unwrap_or_default();
            let who = if who.is_empty() {
                n.clone().unwrap_or_default()
            } else {
                who
            };
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
    omo_on(
        &["wait", sid, "--until", "idle", "--timeout", "600000"],
        Duration::from_secs(620),
        sock,
    )
    .await?;
    let prompt = format!(
        "Ниже история чата (свежие внизу). Собери chat-summary.md: смысловые темы, кто есть кто, тон и стиль, табу, где боту уместно влезать. Отвечай только текстом файла.\n\n{}",
        digest.chars().take(60000).collect::<String>()
    );
    omo_on(&["prompt", sid, &prompt], Duration::from_secs(120), sock).await?;
    omo_on(
        &["wait", sid, "--until", "idle", "--timeout", "600000"],
        Duration::from_secs(620),
        sock,
    )
    .await?;
    let data = omo_on(
        &["read", sid, "--tail", "3", "--messages"],
        Duration::from_secs(60),
        sock,
    )
    .await?;
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
    matches!(kind, "started" | "steered" | "queued" | "already_admitted")
}

/// Frames written to the extension over the control socket.
enum Out {
    Push {
        delivery_id: String,
        text: String,
        /// Incoming message_ids of this batch; `chat_send` refuses a `reply_to`
        /// outside them (history, GIF catalog, another turn). Empty on an old
        /// ledger entry — the extension then disables the check.
        batch_ids: Vec<i64>,
        /// Forum topic the batch arrived in. The extension forwards it to
        /// `typing`; sends also carry it, and the daemon is the owner of the
        /// `message_thread_id` helper.
        thread_id: Option<i64>,
    },
    /// Authoritative durable identity of the live session. The extension puts
    /// it into `hello`, so the handshake carries the durable id and generation
    /// instead of the configured name.
    Session { uid: String, generation: i64 },
}

/// Frames the extension writes back.
enum Inbound {
    Hello {
        session: String,
        generation: i64,
        pid: i64,
    },
    Result {
        delivery_id: String,
        kind: String,
    },
}

/// The admit unit is the sole owner of session lifecycle: `ensure_session`
/// repairs the one-session invariant and closes older duplicates by durable
/// id, so two owners would race the repair and kill working turns. Everyone
/// else asks through `recover_tx`.
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
    match ensure_session(&db, &system_path, Some(&ref_)).await {
        Ok(s) => {
            let offset = read_events(&state, 0).map(|(_, o)| o).unwrap_or(0);
            let _ = settle_session(&state, &db, offset, Duration::from_secs(180)).await;
            dlog(&format!("admit unit ready, session {s}"));
            sid = Some(s);
            announce_session(&db, &out_tx).await;
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
    // The host process dies on its own (observed 2026-11-02: ``extension
    // disconnected`` at 12:24:33, and the missing socket was noticed only at
    // 12:33:28, when a message arrived — nine minutes of a mute bot). The host
    // is now persistent (no idle SIGTERM), so this tick is the CRASH fallback:
    // it re-ensures a genuinely dead host before the next delivery.
    let watch_secs: u64 = cfg("CHATBOT_HOST_WATCH_SEC", "15").parse().unwrap_or(15);
    let mut host_tick = tokio::time::interval(Duration::from_secs(watch_secs.max(5)));
    host_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Healing a held session restarts the host generation, so it must be rare and
    // confirmed twice: an isolated reading of a hold is not enough to kill a host
    // that may be finishing a turn.
    const HELD_STRIKES: u32 = 2;
    const HELD_COOLDOWN_TICKS: u32 = 5;
    let mut held_strikes: u32 = 0;
    // Starts at the cooldown value: the cooldown spaces out REPEATED heals, it
    // must not delay the first one past the two-tick confirmation.
    let mut ticks_since_heal: u32 = HELD_COOLDOWN_TICKS;
    loop {
        tokio::select! {
            _ = host_tick.tick() => {
                // A session parked by the host on an interrupted delivery accepts
                // every push and executes nothing (typing runs, no turn_start).
                // Only THAT hold is treated as a fault: `unattended` is normal for
                // a client-less bot session and an external delivery still runs
                // under it, so healing on it would restart healthy sessions.
                if host_alive() {
                    if let Some(uid) = sid.clone() {
                        if let Some(h) = chat_bot::session::health_on(&uid, &host_socket()).await {
                            if chat_bot::session::is_mute(&h) {
                                held_strikes = held_strikes.saturating_add(1);
                                dlog(&format!(
                                    "session {uid} is held {:?} (strike {held_strikes}/{HELD_STRIKES})",
                                    h.holds
                                ));
                                if chat_bot::session::should_heal(
                                    held_strikes,
                                    turn_recently_open(),
                                    ticks_since_heal,
                                ) {
                                    // Confirmed hold (two strikes and a heal allowed):
                                    // this is the state in which the bot accepts deliveries
                                    // and runs nothing, so the owner must hear about it once.
                                    ops::alert_transition(
                                        "session_held",
                                        true,
                                        &format!("session {uid} stayed in interrupted-run; healing"),
                                    );
                                    dlog(&format!(
                                        "session {uid} is mute on interrupted-run; restarting the host generation"
                                    ));
                                    let _ = chat_bot::stop_host_on(&host_socket()).await;
                                    match ensure_session(&db, &system_path, Some(&ref_)).await {
                                        Ok(s) => {
                                            dlog(&format!("heal: fresh session {s}"));
                                            sid = Some(s);
                                            announce_session(&db, &out_tx).await;
                                        }
                                        Err(e) => dlog(&format!("heal: ensure failed: {e:#}")),
                                    }
                                    // The held session ate every push; re-drive the
                                    // open batches now instead of waiting for the
                                    // seven-minute stall budget.
                                    let pend = db
                                        .call(|con| Ok(ledger::open_batches(con)?))
                                        .await
                                        .unwrap_or_default();
                                    for open_sig in pend {
                                        let st = state.clone();
                                        let sg = open_sig.clone();
                                        let text = db
                                            .call(move |con| {
                                                Ok(regen_text(&st, con, &sg).ok().flatten())
                                            })
                                            .await
                                            .unwrap_or(None);
                                        let Some(text) = text else { continue };
                                        let sg2 = open_sig.clone();
                                        let ids = db
                                            .call(move |con| Ok(ledger::message_ids(con, &sg2)?))
                                            .await
                                            .unwrap_or_default();
                                        let sg3 = open_sig.clone();
                                        let tid = db
                                            .call(move |con| Ok(ledger::thread_id(con, &sg3)))
                                            .await
                                            .unwrap_or(None);
                                        activity().thread.store(
                                            tid.unwrap_or(-1),
                                            std::sync::atomic::Ordering::Relaxed,
                                        );
                                        if out_tx
                                            .send(Out::Push {
                                                delivery_id: frame_id(&open_sig),
                                                text,
                                                batch_ids: ids,
                                                thread_id: tid,
                                            })
                                            .is_ok()
                                        {
                                            dlog(&format!(
                                                "heal: re-pushed {open_sig} into the fresh session"
                                            ));
                                        }
                                    }
                                    held_strikes = 0;
                                    ticks_since_heal = 0;
                                }
                            } else {
                                held_strikes = 0;
                                // The session is healthy again: close the held
                                // state once (a transient hold never armed it).
                                ops::alert_transition("session_held", false, "");
                            }
                        }
                    }
                }
                ticks_since_heal = ticks_since_heal.saturating_add(1);
                if !host_alive() {
                    dlog(&format!(
                        "host watchdog: socket missing ({}), re-ensuring host",
                        host_socket()
                    ));
                    match ensure_session(&db, &system_path, Some(&ref_)).await {
                        Ok(s) => {
                            // No settle here: a freshly ensured idle session never
                            // emits turn_end, so waiting for one would hold the
                            // only admit loop (and any queued message) for the
                            // whole budget. Delivery paths settle when they
                            // actually opened a turn.
                            dlog(&format!("host watchdog: host re-ensured, session {s}"));
                            sid = Some(s);
                            announce_session(&db, &out_tx).await;
                        }
                        Err(e) => dlog(&format!("host watchdog: re-ensure failed: {e:#}")),
                    }
                }
            }
            frame = inbound_rx.recv() => match frame {
                Some(Inbound::Hello { session, generation, pid }) => {
                    dlog(&format!(
                        "extension hello while idle (session={session}, generation={generation}, pid={pid})"
                    ));
                    if let Some(live) = sid.as_deref() {
                        if let Err(e) = chat_bot::session::stale_hello(&session, live) {
                            mark_dead_gate();
                            dlog(&format!(
                                "{e:#}; refusing the hello of a stale instance and sending the live identity"
                            ));
                        }
                    }
                    announce_session(&db, &out_tx).await;
                }
                Some(Inbound::Result { delivery_id, kind }) => {
                    dlog(&format!("stray delivery result {delivery_id} ({kind})"));
                }
                None => break,
            },
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
        "chat_send" => chat_bot::send::chat_send(db, args).await,
        "typing" => chat_bot::send::typing(db, args).await,
        "gif_list" => {
            let (offset, limit) = chat_bot::catalog::parse_page_args(args)?;
            db.call(move |con| chat_bot::catalog::list_view(con, offset, limit))
                .await
        }
        other => Err(anyhow!("unknown control op: {other}")),
    }
}

/// Read-only MCP tool socket. Separate from the control socket so a tool call
/// never evicts the extension; every Bot API/media operation runs here, in the
/// daemon process that owns the token.
async fn tool_loop(state: String, sock: std::path::PathBuf) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    unlink_stale_socket(&sock);
    let listener = loop {
        match tokio::net::UnixListener::bind(&sock) {
            Ok(l) => break l,
            Err(e) => {
                dlog(&format!(
                    "tool socket bind failed ({}): {e:#}",
                    sock.display()
                ));
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    };
    loop {
        let stream = match listener.accept().await {
            Ok((s, _)) => s,
            Err(e) => {
                dlog(&format!("tool socket accept failed: {e:#}"));
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };
        let state = state.clone();
        tokio::task::spawn_local(async move {
            let (rd, mut wr) = stream.into_split();
            let mut rd = tokio::io::BufReader::new(rd);
            let mut line = String::new();
            loop {
                line.clear();
                match rd.read_line(&mut line).await {
                    Ok(0) => break,
                    Ok(_) => {
                        let raw = line.trim();
                        if raw.is_empty() {
                            continue;
                        }
                        let Ok(v) = serde_json::from_str::<Value>(raw) else {
                            continue;
                        };
                        let op = v.get("op").and_then(|x| x.as_str()).unwrap_or("");
                        let args = v.get("args").cloned().unwrap_or(json!({}));
                        let frame = match chat_bot::tool_call(&state, op, &args).await {
                            Ok(text) => json!({"ok": true, "text": text}),
                            Err(e) => json!({"ok": false, "error": format!("{e:#}")}),
                        };
                        if wr.write_all(format!("{frame}\n").as_bytes()).await.is_err() {
                            break;
                        }
                        let _ = wr.flush().await;
                    }
                    Err(_) => break,
                }
            }
        });
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
                    Some(Out::Push { delivery_id, text, batch_ids, thread_id }) => {
                        let frame = json!({"push": {"delivery_id": delivery_id, "text": text, "deliverAs": "steer", "batch_ids": batch_ids, "thread_id": thread_id}});
                        let ok = match serde_json::to_string(&frame) {
                            Ok(s) => wr.write_all(format!("{s}\n").as_bytes()).await.is_ok() && wr.flush().await.is_ok(),
                            Err(_) => false,
                        };
                        if !ok {
                            break;
                        }
                    }
                    Some(Out::Session { uid, generation }) => {
                        let frame = json!({"session": {"uid": uid, "generation": generation}});
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
                                    generation: h.get("generation").and_then(|x| x.as_i64()).unwrap_or(0),
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
        // No extension means nobody is working: an abrupt disconnect mid-run never
        // emits the stop hook, so without this the indicator would burn forever.
        activity()
            .working
            .store(false, std::sync::atomic::Ordering::Relaxed);
        dlog("extension disconnected from the control socket; waiting for the next connection");
    }
}

async fn announce_session(db: &Db, out_tx: &tokio::sync::mpsc::UnboundedSender<Out>) {
    if let Ok(Some((uid, generation))) = db.call(|con| chat_bot::session::live_identity(con)).await
    {
        let _ = out_tx.send(Out::Session { uid, generation });
    }
}

async fn ensure_sid(
    db: &Db,
    sid: &mut Option<String>,
    system_path: &str,
    ref_: &str,
    out_tx: &tokio::sync::mpsc::UnboundedSender<Out>,
) -> Result<String> {
    if let Some(s) = sid.clone() {
        if host_alive() {
            return Ok(s);
        }
        dlog(&format!(
            "host socket missing ({}), re-ensuring host",
            host_socket()
        ));
    }
    let s = ensure_session(db, system_path, Some(ref_)).await?;
    *sid = Some(s.clone());
    announce_session(db, out_tx).await;
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
    let (text, sig, ids, tome, _anchor) = {
        db.call(move |con| build_batch_text(&state_for_db, con, &batch))
            .await?
    };
    if ids.is_empty() {
        dlog("flush skipped: every batch row already answered");
        return Ok(());
    }
    let state_db = state.to_string();
    let ids_db = ids.clone();
    let (chat, thread) = db
        .call(move |con| {
            let _ = state_db;
            let chat: i64 = kv_get(con, "chat_id", "0").parse().unwrap_or(0);
            let mut thread = None;
            for id in &ids_db {
                if let Ok(t) = con.query_row(
                    "SELECT thread_id FROM messages WHERE message_id=? LIMIT 1",
                    [id],
                    |r| r.get::<_, Option<i64>>(0),
                ) {
                    if t.is_some() {
                        thread = t;
                        break;
                    }
                }
            }
            Ok((chat, thread))
        })
        .await?;
    {
        let (sig_c, ids_c, tome_c) = (sig.clone(), ids.clone(), tome.clone());
        db.write(move |tx| {
            tx.seal_batch(&sig_c, chat, thread, &ids_c, &tome_c)?;
            tx.mark_pushed(&sig_c)
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
        let _ = ensure_sid(db, sid_slot, system_path, ref_, out_tx).await;
    }
    let sig_thread = sig.to_string();
    let thread = db
        .call(move |con| Ok(ledger::thread_id(con, &sig_thread)))
        .await
        .unwrap_or(None);
    // Addressability before the first frame. The daemon's own `sessions.state`
    // is a belief that survives host restarts, so it is never the argument here:
    // the host is asked for the durable id and for an occupied attachment slot.
    // Pushing anyway is exactly the 10:43-12:05 mute window - batches stalled in
    // `inboxed` while no instance was attached to the control socket.
    let live_uid = db
        .call(|con| Ok(chat_bot::session::live_identity(con)?))
        .await
        .unwrap_or(None);
    if let Some((uid, _gen)) = live_uid {
        let address = chat_bot::session::address_on(ref_, &host_socket(), &uid).await;
        match chat_bot::session::next_target(&address) {
            chat_bot::session::Target::Push => {}
            chat_bot::session::Target::EnsureSession => {
                dlog(&format!(
                    "{sig}: host does not list session {uid}; ensuring it before the push"
                ));
                let _ = ensure_sid(db, sid_slot, system_path, ref_, out_tx).await;
                return Ok(());
            }
            chat_bot::session::Target::Wait => {
                let why = match &address.addressee {
                    chat_bot::session::Addressee::Unknown(r) => {
                        format!("the host could not be asked: {r}")
                    }
                    _ => "session missing".to_string(),
                };
                dlog(&format!("{sig}: not pushing yet, {why}"));
                return Ok(());
            }
        }
    }
    for cycle in 0..3 {
        let budget = if cycle == 0 { first } else { rest };
        activity()
            .thread
            .store(thread.unwrap_or(-1), std::sync::atomic::Ordering::Relaxed);
        if out_tx
            .send(Out::Push {
                delivery_id: frame_id(sig),
                text: text.to_string(),
                batch_ids: ids.to_vec(),
                thread_id: thread,
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
                    if !frame_for(&delivery_id, sig) {
                        continue;
                    }
                    if accepted_kind(&kind) {
                        let sig_db = sig.to_string();
                        let kind_db = kind.clone();
                        db.write(move |tx| tx.admit_batch(&sig_db, &kind_db))
                            .await?;
                        let _ = admitted_tx.send(sig.to_string());
                        dlog(&format!("admitted {sig} ({kind})"));
                        let _ = ensure_sid(db, sid_slot, system_path, ref_, out_tx).await;
                        return Ok(());
                    }
                    dlog(&format!(
                        "deferred {sig} ({kind}): the runtime took nothing, retrying"
                    ));
                    break;
                }
                Ok(Some(Inbound::Hello {
                    session,
                    generation,
                    pid,
                })) => {
                    dlog(&format!(
                        "extension hello (session={session}, generation={generation}, pid={pid})"
                    ));
                    if let Some(live) = sid_slot.as_deref() {
                        if let Err(e) = chat_bot::session::stale_hello(&session, live) {
                            mark_dead_gate();
                            dlog(&format!(
                                "{e:#}; refusing the hello of a stale instance and sending the live identity"
                            ));
                        }
                    }
                    announce_session(db, out_tx).await;
                    continue;
                }
                Ok(None) => return Ok(()),
                Err(_) => break,
            }
        }
    }
    // Nobody consumed the frame. Without a dead-gate this is the extension
    // reconnecting on its own (or a busy runtime) and retrying is right; with a
    // proven stale instance waiting forever is the mute window, so the
    // generation is restarted - once, bounded, never over a running turn.
    let absent = dead_gate_age_secs();
    if chat_bot::session::rearm_allowed(dead_gate_recent(), absent, turn_recently_open()) {
        dlog(&format!(
            "{sig}: no admission and the instance is dead-gated ({absent}s); rearming the host generation"
        ));
        let _ = chat_bot::stop_host_on(&host_socket()).await;
        let _ = ensure_sid(db, sid_slot, system_path, ref_, out_tx).await;
        return Ok(());
    }
    dlog(&format!(
        "no admission for {sig} after 3 cycles; track will retry on its delivery clock"
    ));
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
    let (text, ids) = db
        .call(move |con| {
            let text = push_pending(&state_for_db, con, &sig_for_db)?;
            let ids = ledger::message_ids(con, &sig_for_db)?;
            Ok((text, ids))
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
        dlog(&format!(
            "recover {sig}: recreating session and re-delivering"
        ));
        if let Some(sid) = sid_slot.clone() {
            match recreate_session(
                db,
                &sid,
                system_path,
                Some(ref_),
                &format!("stalled delivery {sig}"),
            )
            .await
            {
                Ok(new_sid) => {
                    let offset = read_events(state, 0).map(|(_, o)| o).unwrap_or(0);
                    let _ = settle_session(state, db, offset, Duration::from_secs(120)).await;
                    *sid_slot = Some(new_sid);
                }
                Err(e) => dlog(&format!("recreate for {sig} failed: {e:#}")),
            }
        } else if let Ok(s) = ensure_session(db, system_path, Some(ref_)).await {
            *sid_slot = Some(s);
            announce_session(db, out_tx).await;
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
        let _ = ensure_sid(db, sid_slot, system_path, ref_, out_tx).await;
    } else {
        // Never-accepted batch, live host: re-push only. A live session is not
        // touched at all.
        dlog(&format!("recover {sig}: re-pushing (no session change)"));
    }
    let _ = inboxed_tx.send(sig.to_string());
    deliver_sig(
        db,
        sid_slot,
        system_path,
        ref_,
        admitted_tx,
        out_tx,
        inbound_rx,
        sig,
        &text,
        &ids,
    )
    .await
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
    // Open batches come from the journal, not from `pending:*` kv marks.
    let pending = db
        .call(|con| ledger::open_batches(con))
        .await
        .unwrap_or_default();
    for sig in pending {
        let state_for_db = state.to_string();
        let sig_db = sig.clone();
        let (view, text) = match db
            .call(move |con| {
                let view = ledger::view(con, &sig_db)?;
                let text = match regen_text(&state_for_db, con, &sig_db) {
                    Ok(Some(t)) => t,
                    Ok(None) => String::new(),
                    Err(_) => String::new(),
                };
                Ok((view, text))
            })
            .await
        {
            Ok(v) => v,
            Err(_) => return,
        };
        let Some(mut view) = view else { continue };
        if view.state == "closed" {
            continue;
        }
        if view.emitted_at.is_some() && view.settled_at.is_none() {
            if let Some(settled) = last_event_ts(state, "agent_settled") {
                // A run that failed terminally after this batch was admitted has
                // NOT been settled cleanly, and its context may be wedged: never
                // stamp the late settle that would retire it as silent.
                let failed_after_admit = last_event_ts(state, "run_failed")
                    .zip(view.admitted_at)
                    .map(|(failed, admitted)| failed >= admitted)
                    .unwrap_or(false);
                if !failed_after_admit && view.admitted_at.map(|a| settled >= a).unwrap_or(false) {
                    let sig_db = sig.clone();
                    let _ = db.write(move |tx| tx.mark_settled(&sig_db, settled)).await;
                    view.settled_at = Some(settled);
                    dlog(&format!(
                        "bootstrap: {sig} settled late, recorded {settled}"
                    ));
                }
            }
        }
        if let Some(reason) = view.close_reason() {
            let sig_db = sig.clone();
            let outcome = if reason == "settled" {
                "skipped"
            } else {
                "delivered"
            };
            let _ = db
                .write(move |tx| ledger::close_batch(tx.handle(), &sig_db, outcome))
                .await;
            dlog(&format!("bootstrap: {sig} retired ({reason})"));
            continue;
        }
        if text.is_empty() {
            continue;
        }
        dlog(&format!(
            "bootstrap: re-delivering pending {sig} ({})",
            view.phase()
        ));
        let _ = inboxed_tx.send(sig.clone());
        let sig_db = sig.clone();
        let ids = db
            .call(move |con| ledger::message_ids(con, &sig_db))
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
            &ids,
        )
        .await
        .is_err()
        {
            dlog(&format!("bootstrap: {sig} delivery failed, kept for later"));
        }
    }
}

/// How many automatic re-pushes a batch gets before the daemon stops trying and
/// tells the owner instead of spinning on a broken provider path.
const FAIL_RETRY_LIMIT: u32 = 3;

/// Verdict for one batch at the end of a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SettleDecision {
    /// The run ended cleanly: an unanswered batch is the model choosing silence.
    Settle,
    /// The gateway rejected media in the context: recreate the session, re-push.
    HealAndRepush,
    /// A terminal provider error: re-push in the same session.
    Retry,
    /// Automated recovery is exhausted: keep the batch OPEN, tell the owner once.
    Escalate,
}

/// The single place that decides what a finished run means for a batch.
///
/// Ordering matters: the runtime emits `agent_end` (→ `run_failed`) BEFORE
/// `agent_settled`, so this runs before any settle mark is written and keeps a
/// failed attempt from being retired as "the model was silent".
fn settle_decision(
    poisoned: bool,
    failed: bool,
    healed: bool,
    tries: u32,
    limit: u32,
) -> SettleDecision {
    if poisoned {
        return if healed {
            SettleDecision::Escalate
        } else {
            SettleDecision::HealAndRepush
        };
    }
    if failed {
        return if tries < limit {
            SettleDecision::Retry
        } else {
            SettleDecision::Escalate
        };
    }
    SettleDecision::Settle
}

/// Frame suffix of a delivery id (`<sig>#<n>`). A larger number is a new attempt.
fn frame_seq(id: &str) -> u64 {
    id.rsplit('#')
        .next()
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Track unit: owns the events cursor, mirrors receipts, records delivery
/// proofs, retires batches and asks the admit unit to recover stalled ones.
// ---- typing ownership (daemon side) ----
//
// The extension only reports whether the agent is working; while that state is
// true the daemon itself pings Telegram once per second, and it stops as soon as
// the state goes false. `thread` is the topic of the last pushed batch, so the
// indicator lands in the same topic as the answer (not in General).
struct Activity {
    working: std::sync::atomic::AtomicBool,
    thread: std::sync::atomic::AtomicI64,
}

fn activity() -> &'static Activity {
    static A: std::sync::OnceLock<Activity> = std::sync::OnceLock::new();
    A.get_or_init(|| Activity {
        working: std::sync::atomic::AtomicBool::new(false),
        thread: std::sync::atomic::AtomicI64::new(-1),
    })
}

/// Which events move the working/not-working state. Exactly the two owner hooks
/// (agent start -> true, step end -> false) plus the run ends, nothing else.
fn activity_state_for(kind: &str, phase: Option<bool>) -> Option<bool> {
    match kind {
        "agent_activity" => phase,
        "turn_start" => Some(true),
        "turn_end" | "agent_end_aborted" | "agent_settled" => Some(false),
        _ => None,
    }
}

async fn typing_loop(db: Db) {
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut lit = false;
    let mut warned = false;
    loop {
        tick.tick().await;
        let working = activity()
            .working
            .load(std::sync::atomic::Ordering::Relaxed);
        if !working {
            if lit {
                dlog("typing off");
                lit = false;
            }
            warned = false;
            continue;
        }
        let thread = activity().thread.load(std::sync::atomic::Ordering::Relaxed);
        let args = if thread > 0 {
            json!({ "thread_id": thread })
        } else {
            json!({})
        };
        match chat_bot::send::typing(&db, &args).await {
            Ok(_) => {
                if !lit {
                    dlog(&format!("typing on (thread={thread})"));
                    lit = true;
                }
                warned = false;
            }
            Err(e) => {
                if !warned {
                    dlog(&format!("typing failed: {e:#}"));
                    warned = true;
                }
            }
        }
    }
}

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
    // The initial pass replays the JOURNAL, not the present: without this reset a
    // boot inherits "working" from the last old turn and the indicator burns in a
    // session that is doing nothing.
    activity()
        .working
        .store(false, std::sync::atomic::Ordering::Relaxed);
    let mut inflight: HashSet<String> = HashSet::new();
    let mut last_progress: std::collections::HashMap<String, i64> =
        std::collections::HashMap::new();
    let mut emit_warned: HashSet<String> = HashSet::new();
    let mut last_working_at: i64 = 0;
    // Timestamp of the last fully settled run, PER BATCH. The runtime may
    // report the delivery proof AFTER the run settled (observed live 2026-11-01:
    // emitted 11s after the answer, i.e. after agent_settled), so a late proof
    // re-applies the settle — but only for batches whose run actually settled
    // cleanly. A failed run leaves no entry here, which is what keeps it from
    // being closed as "the model was silent".
    let mut settled_sigs: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    // A terminal provider failure belongs to the ATTEMPT that produced it.
    // `run_failed` arrives from the extension on `agent_end`, which the runtime
    // emits BEFORE `agent_settled`: without these marks the batch would be
    // stamped settled in the same pass and closed as `skipped`, losing the
    // message the run died on.
    let mut poisoned: HashSet<String> = HashSet::new();
    let mut terminal_failed: HashSet<String> = HashSet::new();
    let mut healed: HashSet<String> = HashSet::new();
    let mut tries: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
    let mut seen_frames: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
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
                // Receipts are already mirrored inside the send transaction;
                // the event stream is liveness only. Nothing here re-writes a
                // receipt, so the mirror keeps one authority (I4).
                let mut _receipts = 0usize;
                for ev in &events {
                    let kind = ev.get("kind").and_then(|v| v.as_str()).unwrap_or("");
                    if let Some(next) = activity_state_for(kind, activity_phase(ev)) {
                        activity()
                            .working
                            .store(next, std::sync::atomic::Ordering::Relaxed);
                    }
                    match kind {
                        "chat_send_result" | "chat_send_replay" => {
                            _receipts += 1;
                        }
                        "agent_activity" => {
                            if activity_phase(ev) == Some(true) {
                                mark_turn_open();
                                last_working_at = now_ms();
                            }
                        }
                        "turn_start" => {
                            mark_turn_open();
                            turn_in_flight = true;
                        }
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
                        "run_failed" => {
                            // The extension classifies the run's end: a terminal
                            // provider failure means this batch was NOT delivered,
                            // which is different from the model choosing silence.
                            let media = ev
                                .get("media_rejected")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false);
                            let error = ev
                                .get("error")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string();
                            dlog(&format!(
                                "run failed ({}): {}",
                                if media {
                                    "gateway rejected media"
                                } else {
                                    "provider error"
                                },
                                error
                            ));
                            for sig in inflight.iter() {
                                if media {
                                    poisoned.insert(sig.clone());
                                }
                                terminal_failed.insert(sig.clone());
                            }
                        }
                        "agent_settled" => {
                            turn_in_flight = false;
                            let at = ev.get("t").and_then(|v| v.as_i64()).unwrap_or_else(now_ms);
                            let sigs: Vec<String> = inflight.iter().cloned().collect();
                            let mut settle_now: Vec<String> = vec![];
                            let mut heal_now: Vec<String> = vec![];
                            let mut retry_now: Vec<String> = vec![];
                            let mut escalate_now: Vec<String> = vec![];
                            for sig in &sigs {
                                let decision = settle_decision(
                                    poisoned.contains(sig),
                                    terminal_failed.contains(sig),
                                    healed.contains(sig),
                                    tries.get(sig).copied().unwrap_or(0),
                                    FAIL_RETRY_LIMIT,
                                );
                                match decision {
                                    SettleDecision::Settle => settle_now.push(sig.clone()),
                                    SettleDecision::HealAndRepush => heal_now.push(sig.clone()),
                                    SettleDecision::Retry => retry_now.push(sig.clone()),
                                    SettleDecision::Escalate => escalate_now.push(sig.clone()),
                                }
                            }
                            if !settle_now.is_empty() {
                                let sigs_db = settle_now.clone();
                                let _ = db
                                    .write(move |tx| {
                                        for sig in &sigs_db {
                                            tx.mark_settled(sig, at)?;
                                        }
                                        Ok(())
                                    })
                                    .await;
                                for sig in &settle_now {
                                    settled_sigs.insert(sig.clone(), at);
                                }
                            }
                            for sig in heal_now {
                                // The gateway rejected media inside a tool result:
                                // the transcript is wedged on that entry, so a
                                // fresh session is the only cure. One heal per
                                // batch, otherwise a repeat failure loops.
                                healed.insert(sig.clone());
                                *tries.entry(sig.clone()).or_insert(0) += 1;
                                dlog(&format!(
                                    "batch {sig}: run failed on gateway-rejected media; healing the session and re-pushing"
                                ));
                                let _ = recover_tx.send(Recover {
                                    sig: sig.clone(),
                                    recreate: true,
                                });
                            }
                            for sig in retry_now {
                                *tries.entry(sig.clone()).or_insert(0) += 1;
                                dlog(&format!(
                                    "batch {sig}: run failed with a provider error; re-pushing"
                                ));
                                let _ = recover_tx.send(Recover {
                                    sig: sig.clone(),
                                    recreate: false,
                                });
                            }
                            for sig in escalate_now {
                                // Automated recovery is exhausted. The batch stays
                                // OPEN (a batch is closed by an answer or a proven
                                // clean run), and the owner hears about it once.
                                dlog(&format!(
                                    "batch {sig}: automated retries exhausted after a terminal provider failure; batch left open"
                                ));
                                ops::alert_transition(
                                    &format!("run_failed:{sig}"),
                                    true,
                                    &format!(
                                        "batch {sig} could not be delivered after retries; left open"
                                    ),
                                );
                                // The marks stay: the batch is still the attempt
                                // that failed, so a later settle from another run
                                // cannot close it as "the model was silent".
                                // Only a NEW frame (a fresh attempt) clears them.
                            }
                        }
                        "delivery_state" => {
                            let id = ev
                                .get("delivery_id")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string();
                            let st = ev
                                .get("state")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string();
                            let at = ev.get("t").and_then(|v| v.as_i64()).unwrap_or_else(now_ms);
                            match st.as_str() {
                                "emitted" => {
                                    // The context entry exists: the agent has
                                    // the batch. Not a close, just progress. The
                                    // event carries the frame id (`sig#n`), the
                                    // ledger is keyed by the batch sig.
                                    let id_db = sig_of_frame(&id).to_string();
                                    // A new frame number is a NEW attempt: the
                                    // verdict of the previous one no longer
                                    // applies to it.
                                    let seq = frame_seq(&id);
                                    let prev = seen_frames.get(&id_db).copied().unwrap_or(0);
                                    if seq > prev {
                                        seen_frames.insert(id_db.clone(), seq);
                                        poisoned.remove(&id_db);
                                        terminal_failed.remove(&id_db);
                                    }
                                    // The proof may arrive after the run settled;
                                    // apply the settle only when the reducer
                                    // decided this batch settled cleanly.
                                    let settled_seen =
                                        settled_sigs.get(&id_db).copied().unwrap_or(0);
                                    let _ = db
                                        .write(move |tx| {
                                            tx.mark_emitted(&id_db, at)?;
                                            if settled_seen > 0 {
                                                tx.mark_settled(&id_db, settled_seen)?;
                                            }
                                            Ok(())
                                        })
                                        .await;
                                    last_progress.insert(id.clone(), at);
                                }
                                "failed" => {
                                    // Refused by the session file: admissible
                                    // again, so the admit unit re-pushes it.
                                    let err =
                                        ev.get("error").and_then(|v| v.as_str()).unwrap_or("");
                                    dlog(&format!(
                                        "delivery {id} failed ({err}); asking admit to re-push"
                                    ));
                                    let sig_db = sig_of_frame(&id).to_string();
                                    let _ = db.write(move |tx| tx.fail_attempt(&sig_db)).await;
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
                if _receipts > 0 {
                    dlog(&format!(
                        "track: {_receipts} send event(s); receipts already mirrored"
                    ));
                }
            }
            Err(e) => dlog(&format!("track read_events failed: {e:#}")),
        }
        let now = now_ms();
        // A run whose end event never arrived must not block recovery forever:
        // silence past the hard cap means "no longer in flight".
        if turn_in_flight
            && now.saturating_sub(last_event_at) > ((turn_budget + extra) as i64) * 1000
        {
            dlog("run liveness went quiet past the hard cap; treating it as no longer in flight");
            turn_in_flight = false;
        }
        let working = turn_in_flight || now.saturating_sub(last_working_at) < 30_000;
        let sigs: Vec<String> = inflight.iter().cloned().collect();
        if !sigs.is_empty() {
            let snap = db
                .call(move |con| {
                    let mut out = vec![];
                    for sig in &sigs {
                        if let Some(view) = ledger::view(con, sig)? {
                            out.push(view);
                        }
                    }
                    Ok(out)
                })
                .await;
            if let Ok(snap) = snap {
                let mut retired: Vec<(String, String)> = vec![];
                for view in snap {
                    if let Some(reason) = view.close_reason() {
                        let outcome = if reason == "settled" {
                            "skipped"
                        } else {
                            "delivered"
                        };
                        retired.push((view.batch_key.clone(), outcome.to_string()));
                        // The batch is closing on its own merits, so an earlier
                        // "could not deliver" state for it is over: one recovered
                        // line, then silence.
                        ops::alert_transition(&format!("run_failed:{}", view.batch_key), false, "");
                        continue;
                    }
                    let sig = view.batch_key.clone();
                    let last = *last_progress.get(&sig).unwrap_or(&now);
                    let phase = view.phase();
                    // Waiting for the proof is bounded by a full model step: the
                    // extension emits nothing while the model thinks, so a short
                    // emit deadline would fire into a healthy turn.
                    let budget = if phase == "inboxed" {
                        retry_budget
                    } else {
                        turn_budget
                    };
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
                    let retired_db = retired.clone();
                    let _ = db
                        .write(move |tx| {
                            for (sig, outcome) in &retired_db {
                                ledger::close_batch(tx.handle(), sig, outcome)?;
                            }
                            Ok(())
                        })
                        .await;
                    for (sig, outcome) in retired {
                        dlog(&format!("batch {sig} retired ({outcome})"));
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
    let chat_raw = required_cfg("CHATBOT_CHAT_ID")?;
    if chat_bot::parse_chat_id(&chat_raw).is_none() {
        return Err(anyhow!(
            "CHATBOT_CHAT_ID={chat_raw:?} is not a valid numeric Telegram chat id; refusing to start"
        ));
    }
    let state = state_dir();
    let _lock = acquire_state_lock(&state)?;
    // A live host keeps the extension it was spawned with, and the 15-minute
    // idle exit that used to reload it implicitly is gone (the host is now
    // persistent). So restart the generation whenever the staged extension
    // differs from the one the running host was started with; otherwise a
    // deploy would keep serving stale tool code silently.
    if host_alive() {
        match ext_hash() {
            Ok(cur) if stored_host_ext_hash(&state).as_deref() != Some(cur.as_str()) => {
                dlog("extension changed since the host started; restarting host generation");
                if let Err(e) = stop_host_on(&host_socket()).await {
                    dlog(&format!("host stop failed: {e:#}"));
                }
            }
            Ok(_) => {}
            Err(e) => dlog(&format!("extension hash failed: {e:#}")),
        }
    }
    // The database thread starts before any unit needs it; a broken database
    // fails here, visibly, instead of inside the worker.
    let db = match Db::open(&state) {
        Ok(db) => db,
        Err(e) => {
            ops::db_failure_sync("open", &e);
            return Err(e);
        }
    };
    // Crash window repair: a process that died between Intent -> Sending and
    // the Telegram reply leaves Sending behind. It becomes Unknown here, with
    // ONE incident and no HTTP call, so a restart cannot post a second message.
    match chat_bot::send::repair_sending(&db).await {
        Ok(n) if n > 0 => {
            dlog(&format!(
                "startup: {n} send(s) stuck in Sending -> Unknown (one alert, no resend)"
            ));
            ops::alert(
                "send_unknown",
                &format!("{n} send(s) left Sending at startup; not resent"),
            );
        }
        Ok(_) => {}
        Err(e) => {
            dlog(&format!("startup: repair Sending failed: {e:#}"));
            ops::db_failure("repair_sending", &e);
        }
    }
    let me = api("getMe", &json!({})).await?;
    let bot_id: i64 = me
        .get("id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| anyhow!("getMe: no id"))?;
    let username = cfg("CHATBOT_USERNAME", "").to_lowercase();
    {
        let self_username = me
            .get("username")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let self_first = me
            .get("first_name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
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
    let (media_tx, media_rx) =
        tokio::sync::mpsc::channel::<chat_bot::media::MediaJob>(chat_bot::media::MEDIA_QUEUE_CAP);
    let health_session = cfg("CHATBOT_SESSION", "chat-participant");
    tokio::task::spawn_local(ops::health_loop(
        state.clone(),
        media_tx.clone(),
        health_session,
    ));
    let retention_days: i64 = cfg(
        "CHATBOT_ROW_RETENTION_DAYS",
        &chat_bot::media::ROW_RETENTION_DAYS.to_string(),
    )
    .parse()
    .unwrap_or(chat_bot::media::ROW_RETENTION_DAYS);
    tokio::task::spawn_local(control_loop(
        db.clone(),
        control_sock_path(&state),
        state.clone(),
        out_rx,
        inbound_tx,
    ));
    tokio::task::spawn_local(tool_loop(state.clone(), chat_bot::tool_socket_path(&state)));
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
    tokio::task::spawn_local(typing_loop(db.clone()));

    // One cleanup pass at startup, then resume every unfinished download.
    // Both run before polling so a pending row from the previous process is
    // picked up without waiting for the maintenance timer.
    {
        let budget = chat_bot::media::MediaBudget::from_disk(&state);
        let st = state.clone();
        if let Err(e) = db
            .call(move |con| {
                chat_bot::media::cleanup_media(
                    con,
                    &st,
                    &budget,
                    std::time::SystemTime::now(),
                    chat_bot::media::ORPHAN_GRACE,
                    retention_days,
                )
            })
            .await
        {
            dlog(&format!("startup media cleanup failed: {e:#}"));
            ops::db_failure("startup_media_cleanup", &e);
        }
        match db
            .call(|con| chat_bot::media::resume_pending(con, chat_bot::media::MEDIA_QUEUE_CAP))
            .await
        {
            Ok(jobs) => {
                for job in jobs {
                    if media_tx.try_send(job).is_err() {
                        break;
                    }
                }
            }
            Err(e) => {
                dlog(&format!("startup media resume failed: {e:#}"));
                ops::db_failure("startup_media_resume", &e);
            }
        }
    }
    tokio::task::spawn_local(media_loop(db.clone(), state.clone(), media_rx));
    tokio::task::spawn_local(media_maintenance_loop(
        db.clone(),
        state.clone(),
        media_tx.clone(),
        retention_days,
    ));
    dlog(&format!(
        "chatd running, independent units (ingest/media/admit/track), database on its own thread, control socket {} (host socket {})",
        control_sock_path(&state).display(),
        host_socket()
    ));
    ingest_loop(
        db, state, bot_id, username, offset, job_tx, media_tx, ack_rx,
    )
    .await
}

/// Ingest unit: polls Telegram continuously. Parsing is pure; one page of
/// envelopes is committed together with `kv("offset")` in a single
/// `commit_page` transaction, and media downloads are only enqueued (the row
/// is already durable in `pending`). A commit failure leaves the cursor where
/// it was and the next poll re-fetches the same page.
async fn ingest_loop(
    db: Db,
    _state: String,
    bot_id: i64,
    username: String,
    mut offset: i64,
    job_tx: tokio::sync::mpsc::UnboundedSender<Job>,
    media_tx: tokio::sync::mpsc::Sender<chat_bot::media::MediaJob>,
    mut ack_rx: tokio::sync::mpsc::UnboundedReceiver<String>,
) -> Result<()> {
    let window: f64 = cfg("CHATBOT_BATCH_SEC", "10").parse().unwrap_or(10.0);
    let mut batch: Vec<BatchMsg> = vec![];
    let mut poll_failures = 0u32;
    let mut last_flush = std::time::Instant::now();
    let chat: i64 = db
        .call(|con| Ok(kv_get(con, "chat_id", "0").parse().unwrap_or(0)))
        .await?;
    let mut inflight = 0usize;
    loop {
        let updates: Value = match api(
            "getUpdates",
            &json!({"offset": offset, "timeout": 3, "limit": 100}),
        )
        .await
        {
            Ok(u) => {
                poll_failures = 0;
                ops::mark_poll_ok();
                u
            }
            Err(e) => {
                poll_failures += 1;
                ops::ingest_failure();
                let exp = poll_failures.saturating_sub(1).min(4);
                let backoff = (5u64 * (1u64 << exp)).min(60);
                dlog(&format!(
                    "poll failed ({poll_failures}x), retry in {backoff}s: {e}"
                ));
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                continue;
            }
        };
        let mut envelopes = vec![];
        let mut through = offset;
        for u in updates.as_array().cloned().unwrap_or_default() {
            let uid = u.get("update_id").and_then(|v| v.as_i64()).unwrap_or(0);
            through = through.max(uid + 1);
            let raw_msg = u.get("message").cloned();
            let chan_post = u.get("channel_post").cloned();
            let msg = raw_msg.clone().or(chan_post.clone());
            let mut msg = match msg {
                Some(m) => m,
                None => continue,
            };
            if let Some(id) = msg.get("message_id").and_then(|v| v.as_i64()) {
                ops::record_message_id(id);
            }
            msg["_update_id"] = json!(uid);
            if chan_post.is_some() && raw_msg.is_none() {
                msg["_cross_chat"] = json!(true);
            }
            let strict = raw_msg.is_some();
            match envelope_from_message(&msg, uid, strict, chat) {
                Ok(Some(env)) => envelopes.push(env),
                Ok(None) => {}
                Err(e) => dlog(&format!("prepare message failed: {e:#}")),
            }
        }
        if through > offset {
            let mut by_key: std::collections::HashMap<MessageKey, Envelope> =
                envelopes.iter().cloned().map(|e| (e.key, e)).collect();
            let page = match db.write(move |tx| tx.commit_page(envelopes, through)).await {
                Ok(p) => p,
                Err(e) => {
                    // The cursor was not advanced: the same page is fetched
                    // again instead of losing these updates.
                    dlog(&format!("page commit failed, offset stays {offset}: {e:#}"));
                    ops::ingest_failure();
                    ops::db_failure("commit_page", &e);
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };
            offset = through;
            ops::record_duplicates(page.applied.duplicate as u64);
            for ins in page.inserted {
                let Some(env) = by_key.remove(&ins.key) else {
                    continue;
                };
                if env.media_state == chat_bot::media::MediaState::Pending {
                    let job = chat_bot::media::MediaJob {
                        row_id: ins.row_id,
                        file_id: env.file_id.clone(),
                        kind: env.media_kind.clone(),
                        expected_size: env.media_size.unwrap_or(0),
                    };
                    if media_tx.try_send(job).is_err() {
                        dlog("media queue full; pending row will be resumed by maintenance");
                    }
                }
                if env.user_id == Some(bot_id) {
                    continue;
                }
                let mut row = env.to_batch_msg();
                let urgent = row.to_me
                    || (!username.is_empty()
                        && row.text.to_lowercase().contains(&format!("@{username}")));
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
        }
        if !batch.is_empty() && last_flush.elapsed().as_secs_f64() >= window {
            let combo = std::mem::take(&mut batch);
            inflight += 1;
            if job_tx.send(Job { batch: combo }).is_err() {
                dlog("admit unit gone; dropping batch");
            }
            last_flush = std::time::Instant::now();
        }
        while ack_rx.try_recv().is_ok() {
            inflight = inflight.saturating_sub(1);
        }
    }
}

/// Media fetcher: one job at a time, each bounded by FETCH_DEADLINE. A budget
/// refusal is not an error: the row stays `pending` and maintenance resumes it.
async fn media_loop(
    db: Db,
    state: String,
    mut rx: tokio::sync::mpsc::Receiver<chat_bot::media::MediaJob>,
) {
    let source = chat_bot::media::TelegramSource;
    while let Some(job) = rx.recv().await {
        let budget = chat_bot::media::MediaBudget::from_disk(&state);
        match chat_bot::media::fetch_job(&db, &state, &job, &budget, &source).await {
            Ok(chat_bot::media::FetchOutcome::Deferred) => {
                dlog(&format!("media budget deferred row {}", job.row_id));
            }
            Ok(chat_bot::media::FetchOutcome::Skipped) => {
                dlog(&format!(
                    "media row {} vanished before finalize",
                    job.row_id
                ));
            }
            Ok(chat_bot::media::FetchOutcome::Finalized { size, .. }) => {
                dlog(&format!("media row {} ready ({size} bytes)", job.row_id));
            }
            Err(e) => dlog(&format!("media fetch failed for {}: {e:#}", job.row_id)),
        }
    }
}

/// Startup cleanup plus a five-minute pass: one cleanup pass, then resume
/// pending downloads. The victim order inside cleanup is fixed in
/// `media::cleanup_media`.
async fn media_maintenance_loop(
    db: Db,
    state: String,
    tx: tokio::sync::mpsc::Sender<chat_bot::media::MediaJob>,
    retention_days: i64,
) {
    loop {
        tokio::time::sleep(chat_bot::media::MAINTENANCE_INTERVAL).await;
        let budget = chat_bot::media::MediaBudget::from_disk(&state);
        let st = state.clone();
        if let Err(e) = db
            .call(move |con| {
                chat_bot::media::cleanup_media(
                    con,
                    &st,
                    &budget,
                    std::time::SystemTime::now(),
                    chat_bot::media::ORPHAN_GRACE,
                    retention_days,
                )
            })
            .await
        {
            dlog(&format!("media cleanup failed: {e:#}"));
            ops::db_failure("media_cleanup", &e);
        }
        match db
            .call(|con| chat_bot::media::resume_pending(con, chat_bot::media::MEDIA_QUEUE_CAP))
            .await
        {
            Ok(jobs) => {
                for job in jobs {
                    if tx.try_send(job).is_err() {
                        break;
                    }
                }
            }
            Err(e) => {
                dlog(&format!("media resume failed: {e:#}"));
                ops::db_failure("media_resume", &e);
            }
        }
    }
}

#[cfg(test)]
mod settle_tests {
    use super::{frame_seq, settle_decision, SettleDecision};

    const LIMIT: u32 = 3;

    /// Fold a run's event sequence into the decisions the daemon would take.
    fn fold(events: &[(&str, bool)], limit: u32) -> Vec<SettleDecision> {
        let mut poisoned = false;
        let mut failed = false;
        let mut healed = false;
        let mut tries = 0u32;
        let mut out = vec![];
        for (kind, media) in events {
            match *kind {
                "run_failed" => {
                    if *media {
                        poisoned = true;
                    }
                    failed = true;
                }
                "new_frame" => {
                    poisoned = false;
                    failed = false;
                }
                "agent_settled" => {
                    let decision = settle_decision(poisoned, failed, healed, tries, limit);
                    out.push(decision);
                    match decision {
                        SettleDecision::HealAndRepush => {
                            healed = true;
                            tries += 1;
                        }
                        SettleDecision::Retry => tries += 1,
                        SettleDecision::Settle => {
                            failed = false;
                            poisoned = false;
                        }
                        SettleDecision::Escalate => {}
                    }
                }
                _ => {}
            }
        }
        out
    }

    #[test]
    fn silent_run_closes_as_before() {
        assert_eq!(
            fold(&[("agent_settled", false)], LIMIT),
            vec![SettleDecision::Settle]
        );
    }

    #[test]
    fn media_failure_heals_instead_of_closing_the_batch() {
        // The proven production shape: agent_end(run_failed) lands before the
        // settle, and the batch must be healed, never closed as skipped.
        assert_eq!(
            fold(&[("run_failed", true), ("agent_settled", false)], LIMIT),
            vec![SettleDecision::HealAndRepush]
        );
    }

    #[test]
    fn a_second_media_failure_escalates_instead_of_looping() {
        assert_eq!(
            fold(
                &[
                    ("run_failed", true),
                    ("agent_settled", false),
                    ("new_frame", false),
                    ("run_failed", true),
                    ("agent_settled", false),
                ],
                LIMIT
            ),
            vec![SettleDecision::HealAndRepush, SettleDecision::Escalate]
        );
    }

    #[test]
    fn provider_errors_retry_up_to_the_limit_then_escalate() {
        assert_eq!(
            fold(
                &[
                    ("run_failed", false),
                    ("agent_settled", false),
                    ("new_frame", false),
                    ("run_failed", false),
                    ("agent_settled", false),
                    ("new_frame", false),
                    ("run_failed", false),
                    ("agent_settled", false),
                    ("new_frame", false),
                    ("run_failed", false),
                    ("agent_settled", false),
                ],
                LIMIT
            ),
            vec![
                SettleDecision::Retry,
                SettleDecision::Retry,
                SettleDecision::Retry,
                SettleDecision::Escalate
            ]
        );
    }

    #[test]
    fn a_new_frame_starts_a_clean_attempt() {
        assert_eq!(
            fold(
                &[
                    ("run_failed", true),
                    ("agent_settled", false),
                    ("new_frame", false),
                    ("agent_settled", false),
                ],
                LIMIT
            ),
            vec![SettleDecision::HealAndRepush, SettleDecision::Settle]
        );
    }

    #[test]
    fn an_escalated_batch_stays_open_until_a_new_attempt() {
        // After escalation the failure marks stay, so an unrelated settle cannot
        // close the batch as "the model was silent".
        assert_eq!(
            fold(
                &[
                    ("run_failed", true),
                    ("agent_settled", false),
                    ("new_frame", false),
                    ("run_failed", true),
                    ("agent_settled", false),
                    ("agent_settled", false),
                ],
                LIMIT
            ),
            vec![
                SettleDecision::HealAndRepush,
                SettleDecision::Escalate,
                SettleDecision::Escalate
            ]
        );
    }

    #[test]
    fn frame_sequence_reads_the_attempt_number() {
        assert_eq!(frame_seq("abc#1"), 1);
        assert_eq!(frame_seq("abc#12"), 12);
        assert_eq!(frame_seq("abc"), 0);
    }
}

#[cfg(test)]
mod typing_tests {
    use super::activity_state_for;

    #[test]
    fn the_two_hooks_decide_the_indicator() {
        assert_eq!(activity_state_for("agent_activity", Some(true)), Some(true));
        assert_eq!(
            activity_state_for("agent_activity", Some(false)),
            Some(false)
        );
        assert_eq!(activity_state_for("turn_start", None), Some(true));
        assert_eq!(activity_state_for("turn_end", None), Some(false));
        assert_eq!(activity_state_for("agent_settled", None), Some(false));
        assert_eq!(activity_state_for("agent_end_aborted", None), Some(false));
    }

    #[test]
    fn unrelated_events_never_move_the_state() {
        for kind in [
            "chat_send_result",
            "delivery_state",
            "admitted",
            "run_failed",
        ] {
            assert_eq!(activity_state_for(kind, None), None, "{kind}");
        }
        // An activity event without a phase says nothing about working/stopped.
        assert_eq!(activity_state_for("agent_activity", None), None);
    }
}
