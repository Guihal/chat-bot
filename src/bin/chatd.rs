use anyhow::{anyhow, Result};
use chat_bot::store::Store;
use chat_bot::ops;
use chat_bot::*;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

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
                "knowledge" => cmd_knowledge().await,
                "config" => cmd_config(),
                "watchdog" => {
                    let loop_mode = std::env::args().nth(2).as_deref() == Some("--loop");
                    cmd_watchdog(loop_mode).await
                }
                _ => {
                    eprintln!(
                        "usage: chatd run|sync|summarize|knowledge --doc <discourse|persons|dictionary|donors|events> [--now <unix>]|config --effective|watchdog [--loop]"
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
    let store = Store::open(&state)?;
    let mut offset: i64 = chat_bot::store::kv_get(&store, "offset", "0")
        .await?
        .parse()
        .unwrap_or(0);
    let chat: i64 = chat_bot::store::chat_id(&store).await?;
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
            chat_bot::store::commit_page(&store, envelopes, through).await?;
            offset = through;
        }
        if n < 100 {
            break;
        }
    }
    // Rows are already durable and pending; this pass downloads them under
    // the same budget as the daemon.
    let jobs = chat_bot::store::pending_jobs_of(&store, chat_bot::media::MEDIA_QUEUE_CAP).await?;
    let source = chat_bot::media::TelegramSource;
    for job in jobs {
        let budget = chat_bot::media::MediaBudget::from_disk(&state);
        if let Err(e) = chat_bot::media::fetch_job(&store, &state, &job, &budget, &source).await {
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
    let rows = chat_bot::store::recent_digest(&con, 500)?;
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
        if let Some(text) = last_assistant_text(msgs) {
            std::fs::write("chat-summary.md", text)?;
            dlog("chat-summary.md updated");
            return Ok(());
        }
    }
    dlog("no summary produced");
    Ok(())
}

/// Text of the newest assistant message among `--messages` rows. Only the
/// LAST assistant message counts: earlier ones belong to other turns.
fn last_assistant_text(msgs: &[Value]) -> Option<String> {
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
            return Some(texts.join("\n"));
        }
    }
    None
}

/// One run of one document's knowledge job: read the new messages after the
/// persisted cursor, feed them to an isolated session in chunks, and merge only
/// this document's array. Exit status is 0 only when every chunk reached the
/// web and produced a parseable answer; a failed chunk leaves the cursor at the
/// last successful chunk boundary, never at the whole-batch end.
async fn cmd_knowledge() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(2).collect();
    let mut doc_arg: Option<String> = None;
    let mut now_arg: Option<i64> = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--doc" => match args.get(i + 1) {
                Some(v) => {
                    doc_arg = Some(v.clone());
                    i += 2;
                }
                None => {
                    eprintln!("usage: chatd knowledge --doc <discourse|persons|dictionary|donors|events> [--now <unix>] (missing value for --doc)");
                    std::process::exit(2);
                }
            },
            "--now" => match args.get(i + 1) {
                Some(v) => match v.parse::<i64>() {
                    Ok(n) => {
                        now_arg = Some(n);
                        i += 2;
                    }
                    Err(e) => {
                        eprintln!("usage: chatd knowledge --doc <slug> [--now <unix>] (bad --now value: {e})");
                        std::process::exit(2);
                    }
                },
                None => {
                    eprintln!("usage: chatd knowledge --doc <slug> [--now <unix>] (missing value for --now)");
                    std::process::exit(2);
                }
            },
            other => {
                eprintln!("usage: chatd knowledge --doc <discourse|persons|dictionary|donors|events> [--now <unix>] (unknown argument: {other})");
                std::process::exit(2);
            }
        }
    }
    let Some(slug) = doc_arg else {
        eprintln!("usage: chatd knowledge --doc <discourse|persons|dictionary|donors|events> [--now <unix>]");
        std::process::exit(2);
    };
    let Some(kind) = chat_bot::knowledge::Kind::from_slug(&slug) else {
        eprintln!("usage: chatd knowledge --doc <discourse|persons|dictionary|donors|events> (unknown document: {slug})");
        std::process::exit(2);
    };
    let now = now_arg.unwrap_or_else(|| chrono::Utc::now().timestamp());
    let offset_min: i32 = cfg("CHATBOT_KNOWLEDGE_UTC_OFFSET_MIN", "180")
        .parse()
        .unwrap_or(180);
    let window_min: i64 = cfg("CHATBOT_KNOWLEDGE_WINDOW_MIN", "60")
        .parse()
        .unwrap_or(60);
    let cap: i64 = cfg("CHATBOT_KNOWLEDGE_MAX_MESSAGES", "600")
        .parse()
        .unwrap_or(600);
    let target_dir = std::path::PathBuf::from(cfg(
        "CHATBOT_KNOWLEDGE_DIR",
        &cfg("CHATBOT_CWD", "."),
    ));
    std::fs::create_dir_all(&target_dir)?;
    let store = Store::open(&state_dir())?;
    let cursor_key = format!("knowledge:cursor:{slug}");
    let digest_key = format!("knowledge:digest:{slug}");
    let raw_cursor = chat_bot::store::kv_get(&store, &cursor_key, "").await?;
    let cursor = if raw_cursor.trim().is_empty() {
        None
    } else {
        Some(chat_bot::knowledge::Cursor::parse(&raw_cursor)?)
    };
    let (window_from, to) = chat_bot::knowledge::window_plan(now, window_min);
    let from = match cursor {
        Some(c) => window_from.min(c.date),
        None => chat_bot::knowledge::previous_local_day_start(now, offset_min)?,
    };
    let scratch = std::path::PathBuf::from(cfg("CHATBOT_KNOWLEDGE_TMP", "/tmp"))
        .join(chat_bot::knowledge::scratch_name(&slug, std::process::id()));
    std::fs::create_dir_all(&scratch)?;
    let scratch_s = scratch.to_string_lossy().to_string();
    let cursor_note = match cursor {
        None => "first-run-backfill",
        Some(c) if chat_bot::knowledge::behind_window(c.date, window_from) => "cursor-backfill",
        Some(_) => "cursor",
    };
    println!(
        "doc={slug} from={from} to={to} window_min={window_min} offset_min={offset_min} cursor={cursor_note} targets={} scratch={scratch_s}",
        target_dir.display()
    );
    let res = knowledge_run(
        kind,
        &slug,
        cursor,
        from,
        to,
        &target_dir,
        &scratch_s,
        &cfg("CHATBOT_SYSTEM", "knowledge-prompt.md"),
        &store,
        &cursor_key,
        &digest_key,
        cap,
        offset_min,
    )
    .await;
    let _ = std::fs::remove_dir_all(&scratch);
    res
}

fn local_day(ts: i64, offset_min: i32) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .and_then(|d| {
            chrono::FixedOffset::east_opt(offset_min * 60).map(|tz| d.with_timezone(&tz))
        })
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_default()
}

#[allow(clippy::too_many_arguments)]
async fn knowledge_run(
    kind: chat_bot::knowledge::Kind,
    slug: &str,
    cursor: Option<chat_bot::knowledge::Cursor>,
    from: i64,
    to: i64,
    target_dir: &std::path::Path,
    scratch: &str,
    system_path: &str,
    store: &Store,
    cursor_key: &str,
    digest_key: &str,
    cap: i64,
    offset_min: i32,
) -> Result<()> {
    let path = target_dir.join(kind.file_name());
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => chat_bot::knowledge::new_document(kind),
        Err(e) => return Err(anyhow!("read {}: {e}", path.display())),
    };
    let mut doc = chat_bot::knowledge::parse_managed(&text);
    let mut doc_bytes = text.as_bytes().to_vec();
    let mut ref_hash = file_hash(&path);
    let budget: usize = cfg("CHATBOT_KNOWLEDGE_CHUNK_CHARS", "40000")
        .parse()
        .unwrap_or(40000);
    let session = format!("knowledge-{slug}-{}", std::process::id());
    let mut scan = cursor.unwrap_or_default();
    let mut parts: Vec<(String, usize, chat_bot::knowledge::Cursor)> = vec![];
    let mut lines: Vec<String> = vec![];
    let mut used = 0usize;
    let mut messages = 0i64;
    'paging: loop {
        let rows = chat_bot::store::knowledge_window_of(
            store,
            from,
            to,
            scan.date,
            scan.message_id,
            500,
        )
        .await?;
        if rows.is_empty() {
            break;
        }
        for (mid, date, username, first_name, text) in rows {
            if messages >= cap {
                break 'paging;
            }
            scan = chat_bot::knowledge::Cursor {
                date,
                message_id: mid,
            };
            let text = text.unwrap_or_default();
            if text.trim().is_empty() {
                continue;
            }
            messages += 1;
            let who = username
                .filter(|s| !s.is_empty())
                .or(first_name)
                .unwrap_or_default();
            let clock = tz_clock(date);
            let short: String = text.chars().take(2000).collect();
            let line = format!("[{clock}] {mid} {who}: {short}");
            used += line.chars().count() + 1;
            lines.push(line);
            if used >= budget {
                parts.push((lines.join("\n"), lines.len(), scan));
                lines.clear();
                used = 0;
            }
        }
    }
    // The last, partial chunk is real work: without this flush a quiet hour
    // would report messages>0 but process nothing.
    if !lines.is_empty() {
        parts.push((lines.join("\n"), lines.len(), scan));
    }
    if messages == 0 {
        println!("doc={slug} messages=0 NO_NEW");
        return Ok(());
    }
    let full_batch = parts
        .iter()
        .map(|p| p.0.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let stored_digest = chat_bot::store::kv_get(store, digest_key, "").await?;
    if stored_digest == chat_bot::knowledge::run_digest(&full_batch, &doc_bytes) {
        println!("doc={slug} messages={messages} NO_NEW unchanged batch");
        return Ok(());
    }
    let day = local_day(to, offset_min);
    let mut prefix = String::new();
    let mut chunks = 0usize;
    let mut ok = 0usize;
    for (batch, count, chunk_cursor) in parts {
        chunks += 1;
        let prompt = knowledge_prompt_text(
            kind,
            chunks,
            &batch,
            &chat_bot::knowledge::existing_digest(&doc, &day, 300),
        );
        let (calls, chunk) = knowledge_chunk(&session, scratch, system_path, &prompt).await?;
        if !chunk.web_ok {
            return Err(anyhow!(
                "doc={slug} chunk={chunks} web.ok=false reason={}",
                chunk.web_reason
            ));
        }
        let _ = calls;
        let stats = write_target(
            kind,
            &mut doc,
            &path,
            &mut ref_hash,
            chunk.records_for(kind),
            &day,
        )?;
        prefix.push_str(&batch);
        prefix.push('\n');
        doc_bytes = std::fs::read(&path)?;
        chat_bot::store::kv_set(store, cursor_key, &chunk_cursor.format()).await?;
        chat_bot::store::kv_set(
            store,
            digest_key,
            &chat_bot::knowledge::run_digest(&prefix, &doc_bytes),
        )
        .await?;
        ok += 1;
        println!(
            "chunk {chunks} doc={slug} messages={count} ok added={} updated={}",
            stats.added, stats.updated
        );
    }
    println!(
        "doc={slug} messages={messages} chunks={chunks} ok={ok} hash-guard=target"
    );
    Ok(())
}

fn knowledge_prompt_text(
    kind: chat_bot::knowledge::Kind,
    part: usize,
    digest: &str,
    existing: &str,
) -> String {
    format!(
        "Документ «{}» (массив `{}`), часть {part} переписки (хронологический порядок).\n\n\
Заполняй ТОЛЬКО массив `{}`; все остальные массивы верни пустыми. Если материала для этого \
документа нет — верни пустой массив. Сохраняй точные имена, числа и необычные токены из \
сообщений; не заменяй их обобщением. Уже записано (ключ — что в нём уже описано):\n{existing}\n\n\
Если содержание куска попадает в такую запись, верни ЕЁ ключ и обновлённое тело; новую \
запись создавай только для материала, которого нет ни в одной из них.\n\nСообщения:\n{digest}\n\n\
Ответь ТОЛЬКО JSON по схеме из системного промпта.",
        kind.title(),
        kind.array_field(),
        kind.array_field()
    )
}

fn tz_clock(ts: i64) -> String {
    let offset_min: i32 = cfg("CHATBOT_KNOWLEDGE_UTC_OFFSET_MIN", "180")
        .parse()
        .unwrap_or(180);
    chrono::DateTime::from_timestamp(ts, 0)
        .and_then(|d| {
            chrono::FixedOffset::east_opt(offset_min * 60).map(|tz| d.with_timezone(&tz))
        })
        .map(|d| d.format("%H:%M").to_string())
        .unwrap_or_default()
}

async fn knowledge_chunk(
    session: &str,
    scratch: &str,
    system_path: &str,
    prompt: &str,
) -> Result<(usize, chat_bot::knowledge::Chunk)> {
    let (sid, sock) = isolated_session_of(system_path, session, scratch, true).await?;
    let res = knowledge_turn(&sid, &sock, prompt).await;
    close_summary_session(&sid, &sock).await;
    res
}

async fn knowledge_wait(sid: &str, sock: &str) -> Result<Value> {
    omo_on(
        &["wait", sid, "--until", "idle", "--timeout", "900000"],
        Duration::from_secs(920),
        sock,
    )
    .await
}

async fn knowledge_turn(
    sid: &str,
    sock: &str,
    prompt: &str,
) -> Result<(usize, chat_bot::knowledge::Chunk)> {
    knowledge_wait(sid, sock).await?;
    omo_on(&["prompt", sid, prompt], Duration::from_secs(120), sock).await?;
    knowledge_wait(sid, sock).await?;
    let data = omo_on(
        &["read", sid, "--tail", "10", "--messages"],
        Duration::from_secs(60),
        sock,
    )
    .await?;
    let msgs = data
        .get("messages")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow!("session read returned no messages"))?;
    let web_calls = msgs
        .iter()
        .filter_map(|m| m.get("content"))
        .map(chat_bot::knowledge::web_tool_calls)
        .sum();
    let text = last_assistant_text(msgs)
        .ok_or_else(|| anyhow!("session produced no assistant message"))?;
    Ok((web_calls, chat_bot::knowledge::parse_model_json(&text)?))
}

/// Merge one chunk into the TARGET document and write it atomically. The hash
/// guard compares only the target path: another document's job has its own
/// file and its own cursor, so it must neither fail nor be re-rendered here.
fn write_target(
    kind: chat_bot::knowledge::Kind,
    doc: &mut chat_bot::knowledge::Doc,
    path: &std::path::Path,
    ref_hash: &mut String,
    records: &[chat_bot::knowledge::Record],
    day: &str,
) -> Result<chat_bot::knowledge::MergeStats> {
    let now = file_hash(path);
    if now != *ref_hash {
        return Err(anyhow!(
            "hash-guard: {} changed by another writer during the run",
            path.display()
        ));
    }
    let stats = chat_bot::knowledge::merge(doc, records, day);
    let text = chat_bot::knowledge::render(doc, kind);
    write_atomic(path, &text)?;
    *ref_hash = sha256_hex(text.as_bytes());
    Ok(stats)
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    format!("{:x}", h.finalize())
}

fn file_hash(path: &std::path::Path) -> String {
    std::fs::read(path)
        .map(|b| sha256_hex(&b))
        .unwrap_or_default()
}

fn write_atomic(path: &std::path::Path, text: &str) -> Result<()> {
    let tmp = path.with_extension(format!("knowledge-tmp-{}", std::process::id()));
    std::fs::write(&tmp, text)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Delivery units: ingest (Telegram), admit (control socket + the ONLY session-lifecycle
/// owner), track (events cursor + delivery proofs + recovery requests).
struct Job {
    batch: Vec<BatchMsg>,
}

/// A recovery request from the track unit. `recreate` is false for every
/// failure that is not the session's own fault — a stalled batch, a terminal
/// provider error, a refused frame. Those re-deliver into the SAME session,
/// because a new one throws away the model's memory. It is true only for
/// `HealAndRepush`: the gateway rejected media in the transcript and a fresh
/// session is the only cure (the reason is logged by `recreate_session`).
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
    store: Store,
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
    match ensure_session(&store, &system_path, Some(&ref_)).await {
        Ok(s) => {
            let offset = read_events(&state, 0).map(|(_, o)| o).unwrap_or(0);
            let _ = settle_session(&state, &store, offset, Duration::from_secs(180)).await;
            dlog(&format!("admit unit ready, session {s}"));
            sid = Some(s);
            announce_session(&store, &out_tx).await;
        }
        Err(e) => dlog(&format!("initial session ensure failed: {e:#}")),
    }
    recover_pending(
        &store,
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
    // A resume is a cheap human prompt, not a restart, so it may retry sooner
    // than a heal - but not every tick, or a session that refuses would be
    // flooded with user turns.
    const RESUME_COOLDOWN_TICKS: u32 = 2;
    let mut held_strikes: u32 = 0;
    // Starts at the cooldown value: the cooldown spaces out REPEATED heals, it
    // must not delay the first one past the two-tick confirmation.
    let mut ticks_since_heal: u32 = HELD_COOLDOWN_TICKS;
    // The engine pauses a session once automatic wakes pile up with no human
    // input ("Paused while no one was here: N automatic wakes held"). A chat bot
    // NEVER produces human input, so that pause is permanent on its own: the
    // session takes every push, runs nothing, and every open batch stalls until
    // it is re-pushed. The documented remedy is a message from a human, so send
    // one instead of restarting the host (which throws the conversation away).
    let mut resume_strikes: u32 = 0;
    let mut ticks_since_resume: u32 = RESUME_COOLDOWN_TICKS;
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
                            if chat_bot::session::needs_resume(&h) {
                                resume_strikes = resume_strikes.saturating_add(1);
                                dlog(&format!(
                                    "session {uid} is paused {:?} (strike {resume_strikes}/{HELD_STRIKES})",
                                    h.holds
                                ));
                                if resume_strikes >= HELD_STRIKES
                                    && ticks_since_resume >= RESUME_COOLDOWN_TICKS
                                    && !turn_recently_open()
                                {
                                    if chat_bot::session::resume_on(&uid, &host_socket()).await {
                                        dlog(&format!("session {uid} resumed with a human prompt"));
                                        ticks_since_resume = 0;
                                        // A resume prompt consumes the held notices without
                                        // re-enqueueing them, so nothing the paused session
                                        // swallowed is in the transcript: push the open batches
                                        // again instead of trusting them.
                                        redrive_open_batches(&store, &state, &out_tx, "resume")
                                            .await;
                                    } else {
                                        dlog(&format!(
                                            "session {uid} resume was refused; retrying next tick"
                                        ));
                                    }
                                }
                            } else {
                                resume_strikes = 0;
                            }
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
                                    match ensure_session(&store, &system_path, Some(&ref_)).await {
                                        Ok(s) => {
                                            dlog(&format!("heal: fresh session {s}"));
                                            sid = Some(s);
                                            announce_session(&store, &out_tx).await;
                                        }
                                        Err(e) => dlog(&format!("heal: ensure failed: {e:#}")),
                                    }
                                    // The held session ate every push; re-drive the
                                    // open batches now instead of waiting for the stall budget.
                                    redrive_open_batches(&store, &state, &out_tx, "heal").await;
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
                ticks_since_resume = ticks_since_resume.saturating_add(1);
                if !host_alive() {
                    dlog(&format!(
                        "host watchdog: socket missing ({}), re-ensuring host",
                        host_socket()
                    ));
                    match ensure_session(&store, &system_path, Some(&ref_)).await {
                        Ok(s) => {
                            // No settle here: a freshly ensured idle session never
                            // emits turn_end, so waiting for one would hold the
                            // only admit loop (and any queued message) for the
                            // whole budget. Delivery paths settle when they
                            // actually opened a turn.
                            dlog(&format!("host watchdog: host re-ensured, session {s}"));
                            sid = Some(s);
                            announce_session(&store, &out_tx).await;
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
                    announce_session(&store, &out_tx).await;
                }
                Some(Inbound::Result { delivery_id, kind }) => {
                    dlog(&format!("stray delivery result {delivery_id} ({kind})"));
                }
                None => break,
            },
            job = jobs.recv() => match job {
                Some(job) => {
                    let res = admit_job(&store, &state, &mut sid, &system_path, &ref_, &admitted_tx, &inboxed_tx, &out_tx, &mut inbound_rx, job).await;
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
                    if let Err(e) = recover_batch(&store, &state, &mut sid, &system_path, &ref_, &admitted_tx, &inboxed_tx, &out_tx, &mut inbound_rx, &req).await {
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
async fn handle_control_call(store: &Store, state: &str, op: &str, args: &Value) -> Result<String> {
    match op {
        // A favourite is resolved here, before the transport sees the request:
        // `send.rs` keeps knowing only about Bot API arguments, and the store
        // stays with `favorites`, its only owner.
        "chat_send" => {
            let args = chat_bot::favorites::apply_favorite(state, args)?;
            chat_bot::send::chat_send(store, &args).await
        }
        "chat_favorite" => chat_bot::favorites::control(state, args),
        "typing" => chat_bot::send::typing(store, args).await,
        "gif_list" => {
            let (offset, limit) = chat_bot::catalog::parse_page_args(args)?;
            let (rows, total) = chat_bot::store::catalog_page_of(store, offset, limit).await?;
            Ok(chat_bot::catalog::list_view(chat_bot::catalog::page_of(
                rows, offset, total,
            )))
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

/// Push every open batch again. Used after a host heal and after a resume: a
/// held session parks deliveries as notices and a resume prompt consumes those
/// notices without re-enqueueing them, so the text must be pushed again rather
/// than trusted.
async fn redrive_open_batches(
    store: &chat_bot::store::Store,
    state: &str,
    out_tx: &tokio::sync::mpsc::UnboundedSender<Out>,
    why: &str,
) {
    let pend = chat_bot::store::open_batches_of(store).await.unwrap_or_default();
    for open_sig in pend {
        let text = chat_bot::regen_text(store, state, &open_sig)
            .await
            .ok()
            .flatten();
        let Some(text) = text else { continue };
        let ids = chat_bot::store::message_ids_of(store, open_sig.clone())
            .await
            .unwrap_or_default();
        let tid = chat_bot::store::thread_of_batch(store, open_sig.clone())
            .await
            .unwrap_or(None);
        activity()
            .thread
            .store(tid.unwrap_or(-1), std::sync::atomic::Ordering::Relaxed);
        if out_tx
            .send(Out::Push {
                delivery_id: frame_id(&open_sig),
                text,
                batch_ids: ids,
                thread_id: tid,
            })
            .is_ok()
        {
            dlog(&format!("{why}: re-pushed {open_sig}"));
        }
    }
}

/// Hosts the control socket and moves frames between it and the admit unit.
/// One connection at a time, newest wins: a host restart brings a new extension
/// instance that reconnects on its own.
async fn control_loop(
    store: Store,
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
    // The favourites store needs the state directory on every `chat_send`; keep
    // our own copy so the loop below can move the original into other units.
    let state_dir = state.clone();
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
                                let store = store.clone();
                                let frames_tx = frames_tx.clone();
                                let fav_state = state_dir.clone();
                                tokio::task::spawn_local(async move {
                                    let frame = match handle_control_call(&store, &fav_state, &op, &args).await {
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

async fn announce_session(store: &Store, out_tx: &tokio::sync::mpsc::UnboundedSender<Out>) {
    if let Ok(Some((uid, generation))) = chat_bot::store::live_identity_of(&store).await {
        let _ = out_tx.send(Out::Session { uid, generation });
    }
}

async fn ensure_sid(
    store: &Store,
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
    let s = ensure_session(store, system_path, Some(ref_)).await?;
    *sid = Some(s.clone());
    announce_session(store, out_tx).await;
    Ok(s)
}

#[allow(clippy::too_many_arguments)]
async fn admit_job(
    store: &Store,
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
    let raw_len = job.batch.len();
    let known_group = chat_bot::store::known_group_post_keys_of(store).await?;
    let batch = chat_bot::dedupe_channel_posts_known(job.batch, &known_group);
    if batch.len() < raw_len {
        dlog(&format!(
            "dedupe: dropped {} channel copy(ies) already answered in the discussion group",
            raw_len - batch.len()
        ));
    }
    // One closure: building the text and writing its pending ledger entry must
    // not be separated by another unit's write.
    let (text, sig, ids, tome, _anchor, skipped) =
        build_batch_text(store, state, &batch).await?;
    if ids.is_empty() {
        if !skipped.is_empty() {
            // Every row was too old to reach the model: seal the batch so the
            // ledger names it, then close it as skipped. It never pushes, so
            // it can never stall or be re-delivered.
            let chat: i64 = chat_bot::store::chat_id(store).await?;
            let thread = chat_bot::store::batch_thread_of(store, skipped.clone()).await?;
            chat_bot::store::seal_and_push(store, sig.clone(), chat, thread, skipped.clone(), vec![])
                .await?;
            let _ = chat_bot::store::close_batch_of(store, sig.clone(), "skip_by_time".to_string())
                .await;
            dlog(&format!(
                "flush skipped: {skipped:?} stale by time, closed as skip_by_time"
            ));
        } else {
            dlog("flush skipped: every batch row already answered");
        }
        return Ok(());
    }
    let chat: i64 = chat_bot::store::chat_id(store).await?;
    let thread = chat_bot::store::batch_thread_of(store, ids.clone()).await?;
    {
        chat_bot::store::seal_and_push(store, sig.clone(), chat, thread, ids.clone(), tome.clone())
            .await?;
    }
    // Registration with the track clock happens BEFORE the push: a batch the
    // runtime refuses must keep its retry clock.
    let _ = inboxed_tx.send(sig.clone());
    deliver_sig(
        store,
        state,
        sid_slot,
        system_path,
        ref_,
        admitted_tx,
        out_tx,
        inbound_rx,
        &sig,
        text,
        &ids,
    )
    .await
}

/// Whether every message of this batch is past the age budget, so pushing it
/// would wake a dead thread. The pool decides age once per batch at build time;
/// a batch that then sits in delivery longer than the budget must be dropped at
/// the point of sending instead of being answered late. This is exactly the
/// restart flood: hundreds of day-old claimed batches were re-pushed and
/// answered one by one. Fail-closed: an empty or unreadable batch is left alone
/// (nothing to judge), but any batch whose rows are all stale is dropped.
async fn batch_all_stale(store: &Store, sig: &str) -> bool {
    let ids = chat_bot::store::message_ids_of(store, sig.to_string())
        .await
        .unwrap_or_default();
    if ids.is_empty() {
        return false;
    }
    let ids: Vec<Value> = ids.into_iter().map(|i| json!(i)).collect();
    let rows = chat_bot::store::batch_rows_of(store, ids)
        .await
        .unwrap_or_default();
    if rows.is_empty() {
        return false;
    }
    let now = now_ms() / 1000;
    rows.iter()
        .all(|r| chat_bot::is_stale_by_time(r.date, now))
}

/// Push the batch over the socket and wait for an ACCEPTED admission. A refusal
/// (`turn_conflict`, `held_draft`) took nothing, so it is never reported as
/// delivered; the push is repeated and the track clock re-drives it later.
#[allow(clippy::too_many_arguments)]
async fn deliver_sig(
    store: &Store,
    state: &str,
    sid_slot: &mut Option<String>,
    system_path: &str,
    ref_: &str,
    admitted_tx: &tokio::sync::mpsc::UnboundedSender<String>,
    out_tx: &tokio::sync::mpsc::UnboundedSender<Out>,
    inbound_rx: &mut tokio::sync::mpsc::UnboundedReceiver<Inbound>,
    sig: &str,
    mut text: String,
    ids: &[i64],
) -> Result<()> {
    let first = Duration::from_secs_f64(cfg("CHATBOT_ADMIT_FIRST_SEC", "8").parse().unwrap_or(8.0));
    let rest = Duration::from_secs_f64(cfg("CHATBOT_ADMIT_SEC", "10").parse().unwrap_or(10.0));
    // The batch left the pool fresh, but delivery can stall: by the time a frame
    // goes out, every message behind it may be hours old. Drop it here instead
    // of waking a dead thread, whichever path brought us (ingest, recover,
    // bootstrap or track retry).
    if batch_all_stale(store, sig).await {
        let sig_db = sig.to_string();
        let _ = chat_bot::store::close_batch_of(store, sig_db, "skip_by_time".to_string()).await;
        dlog(&format!(
            "age guard: {sig} dropped from the send pool (every message is stale)"
        ));
        return Ok(());
    }
    // A dead host cannot run the extension, so nothing would answer the push:
    // re-ensure it before the first frame instead of burning the budget.
    if !host_alive() {
        dlog(&format!(
            "host socket missing ({}), re-ensuring before delivery of {sig}",
            host_socket()
        ));
        // Re-ensuring can open a NEW session: the text in hand was built for
        // the old context and would reach a fresh model without its history.
        let before = chat_bot::store::live_identity_of(store).await.unwrap_or(None);
        let _ = ensure_sid(store, sid_slot, system_path, ref_, out_tx).await;
        rebuild_after_session_change(store, state, sig, &mut text, before).await;
    }
    let thread = chat_bot::store::thread_of_batch(store, sig.to_string())
        .await
        .unwrap_or(None);
    // Addressability before the first frame. The daemon's own `sessions.state`
    // is a belief that survives host restarts, so it is never the argument here:
    // the host is asked for the durable id and for an occupied attachment slot.
    // Pushing anyway is exactly the 10:43-12:05 mute window - batches stalled in
    // `inboxed` while no instance was attached to the control socket.
    let live_uid = chat_bot::store::live_identity_of(store).await.unwrap_or(None);
    if let Some((uid, _gen)) = live_uid {
        let address = chat_bot::session::address_on(ref_, &host_socket(), &uid).await;
        match chat_bot::session::next_target(&address) {
            chat_bot::session::Target::Push => {}
            chat_bot::session::Target::EnsureSession => {
                dlog(&format!(
                    "{sig}: host does not list session {uid}; ensuring it before the push"
                ));
                let _ = ensure_sid(store, sid_slot, system_path, ref_, out_tx).await;
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
        // A retry that lands after the age budget must not push either: the
        // batch was fresh when it entered the pool, not necessarily now.
        if cycle > 0 && batch_all_stale(store, sig).await {
            let sig_db = sig.to_string();
            let _ = chat_bot::store::close_batch_of(store, sig_db, "skip_by_time".to_string())
                .await;
            dlog(&format!(
                "age guard: {sig} dropped from the send pool on retry (every message is stale)"
            ));
            return Ok(());
        }
        activity()
            .thread
            .store(thread.unwrap_or(-1), std::sync::atomic::Ordering::Relaxed);
        if out_tx
            .send(Out::Push {
                delivery_id: frame_id(sig),
                text: text.clone(),
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
                        chat_bot::store::admit_batch_of(store, sig_db, kind_db).await?;
                        // A fresh frame is a fresh attempt: the stall flag from
                        // an earlier exhausted run no longer applies.
                        let _ = chat_bot::store::kv_set(
                            store,
                            &format!("batch_stalled:{sig}"),
                            "",
                        )
                        .await;
                        let _ = admitted_tx.send(sig.to_string());
                        dlog(&format!("admitted {sig} ({kind})"));
                        // The indicator belongs to the daemon, not to the model's
                        // step boundaries (owner's contract, 2026-11-07): it
                        // answers "is the bot working", so it must light the
                        // moment the batch is accepted, not when the extension's
                        // first `agent_activity` lands. That gap is measurable -
                        // one admission on the live host waited 12.9 s for host
                        // warm-up with the indicator dark - and every reply
                        // shorter than the gap looked like a bot that never
                        // started typing. `agent_settled`/a dead extension still
                        // own the off switch.
                        activity()
                            .working
                            .store(true, std::sync::atomic::Ordering::Relaxed);
                        let _ = ensure_sid(store, sid_slot, system_path, ref_, out_tx).await;
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
                    announce_session(store, out_tx).await;
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
        let _ = ensure_sid(store, sid_slot, system_path, ref_, out_tx).await;
        return Ok(());
    }
    dlog(&format!(
        "no admission for {sig} after 3 cycles; track will retry on its delivery clock"
    ));
    Ok(())
}

/// Any session change invalidates the text in hand: the window decision was
/// made against the previous `durableSessionId:generation`, so a frame built
/// for the old context reaches a brand-new model without its history. Rebuild
/// through the same builder, which then sees the new epoch (or the recorded
/// sig rule keeps a rebuild byte-identical when the identity did not move).
async fn rebuild_after_session_change(
    store: &Store,
    state: &str,
    sig: &str,
    text: &mut String,
    before: Option<(String, i64)>,
) {
    let after = chat_bot::store::live_identity_of(store).await.unwrap_or(None);
    if after == before {
        return;
    }
    let name = |id: &Option<(String, i64)>| {
        id.as_ref()
            .map(|(u, g)| format!("{u}:{g}"))
            .unwrap_or_else(|| "none".to_string())
    };
    match chat_bot::regen_text(store, state, sig).await {
        Ok(Some(rebuilt)) if rebuilt != *text => {
            dlog(&format!(
                "recover {sig}: session epoch changed ({} -> {}); rebuilding the batch text",
                name(&before),
                name(&after)
            ));
            *text = rebuilt;
        }
        Ok(_) => {}
        Err(e) => dlog(&format!(
            "recover {sig}: rebuild after the session change failed: {e:#}"
        )),
    }
}

#[allow(clippy::too_many_arguments)]
async fn recover_batch(
    store: &Store,
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
    let text = chat_bot::push_pending(store, state, sig).await?;
    let ids = chat_bot::store::message_ids_of(store, sig.to_string()).await?;
    let mut text = match text {
        Some(t) => t,
        None => {
            dlog(&format!("recover {sig}: nothing left to ask"));
            return Ok(());
        }
    };
    // Any branch below may move the session, which invalidates the text.
    let before = chat_bot::store::live_identity_of(store).await.unwrap_or(None);
    if req.recreate {
        dlog(&format!(
            "recover {sig}: replacing the session and re-delivering"
        ));
        if let Some(sid) = sid_slot.clone() {
            match recreate_session(
                store,
                &sid,
                system_path,
                Some(ref_),
                &format!(
                    "gateway-rejected media in the transcript, a fresh session is the only cure (batch {sig})"
                ),
            )
            .await
            {
                Ok(new_sid) => {
                    let offset = read_events(state, 0).map(|(_, o)| o).unwrap_or(0);
                    let _ = settle_session(state, store, offset, Duration::from_secs(120)).await;
                    *sid_slot = Some(new_sid);
                }
                Err(e) => dlog(&format!("recreate for {sig} failed: {e:#}")),
            }
        } else if let Ok(s) = ensure_session(store, system_path, Some(ref_)).await {
            *sid_slot = Some(s);
            announce_session(store, out_tx).await;
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
        let _ = ensure_sid(store, sid_slot, system_path, ref_, out_tx).await;
    } else {
        // Never-accepted batch, live host: re-push only. A live session is not
        // touched at all.
        dlog(&format!("recover {sig}: re-pushing (no session change)"));
    }
    // The transcript may have been rewritten under us (a failed compaction is
    // the signature case: no event, no turn, same session): the next turn
    // recovers with the history window instead of trusting a context the
    // model may no longer hold.
    let _ = chat_bot::force_window_next_turn(store).await;
    // The window decision belongs to the session that will receive the frame:
    // a frame prepared for the old epoch would reach a fresh model without its
    // history.
    rebuild_after_session_change(store, state, sig, &mut text, before).await;
    let _ = inboxed_tx.send(sig.to_string());
    deliver_sig(
        store,
        state,
        sid_slot,
        system_path,
        ref_,
        admitted_tx,
        out_tx,
        inbound_rx,
        sig,
        text,
        &ids,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn recover_pending(
    store: &Store,
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
    let pending = chat_bot::store::open_batches_of(store)
        .await
        .unwrap_or_default();
    for sig in pending {
        let view = match chat_bot::store::batch_view_of(store, sig.clone()).await {
            Ok(v) => v,
            Err(_) => return,
        };
        let text = match chat_bot::regen_text(store, state, &sig).await {
            Ok(Some(t)) => t,
            Ok(None) => String::new(),
            // A read failure is transient: retry on the next tick instead of
            // retiring a batch that may still have a question in it.
            Err(e) => {
                dlog(&format!("bootstrap: {sig} not readable yet: {e}"));
                continue;
            }
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
                    let _ = chat_bot::store::mark_settled_of(store, sig_db, settled).await;
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
            } else if reason == "skip_by_time" {
                "skip_by_time"
            } else {
                "delivered"
            };
            let _ = chat_bot::store::close_batch_of(&store, sig_db, outcome.to_string()).await;
            dlog(&format!("bootstrap: {sig} retired ({reason})"));
            continue;
        }
        if text.is_empty() {
            // Nothing left to answer: every message of this batch is already
            // answered, or its rows were consumed by an earlier push. Retiring
            // it beats leaving it claimed forever — the track unit would wait
            // for a delivery proof that can never arrive, the batch would log
            // "still waiting" every minute, and the bot would look mute while
            // holding a queue of dead batches.
            let sig_db = sig.clone();
            let _ = chat_bot::store::close_batch_of(store, sig_db, "skipped".to_string()).await;
            dlog(&format!("bootstrap: {sig} closed: nothing left to answer"));
            continue;
        }
        dlog(&format!(
            "bootstrap: re-delivering pending {sig} ({})",
            view.phase()
        ));
        let _ = inboxed_tx.send(sig.clone());
        let sig_db = sig.clone();
        let ids = chat_bot::store::message_ids_of(store, sig_db)
        .await
        .unwrap_or_default();
        if deliver_sig(
            store,
            state,
            sid_slot,
            system_path,
            ref_,
            admitted_tx,
            out_tx,
            inbound_rx,
            &sig,
            text,
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

/// Verdict for one redelivery of a batch that never produced a run: a stall
/// with no turn in flight, or a refused prompt (`delivery_state failed`).
/// Same bound as the run-failure path (`FAIL_RETRY_LIMIT`), so a session that
/// accepts pushes but starts nothing — the failed-compaction signature —
/// escalates to the owner instead of spinning the queue forever. The stall
/// clock itself is the cooldown (a retry fires only after a full budget of no
/// progress), so this never hot-loops. Pure, so the bound is testable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RedeliveryDecision {
    Retry,
    Escalate,
}

fn redelivery_decision(tries: u32, limit: u32) -> RedeliveryDecision {
    if tries < limit {
        RedeliveryDecision::Retry
    } else {
        RedeliveryDecision::Escalate
    }
}

/// Owner-visible end of automated redelivery: the batch stays OPEN (a batch
/// is closed by an answer or a proven clean run), the owner hears about it
/// once via the existing `run_failed:<sig>` transition, and a new frame
/// clears the stall mark on its next accepted admission. Same session: a
/// failed compaction is never a reason to throw the model's memory away.
async fn escalate_no_delivery(store: &Store, sig: &str, why: &str) {
    dlog(&format!(
        "batch {sig}: {why}; automated redelivery exhausted, batch left open"
    ));
    ops::alert_transition(
        &format!("run_failed:{sig}"),
        true,
        &format!("batch {sig} could not be delivered after retries; left open"),
    );
    let _ = chat_bot::store::kv_set(store, &format!("batch_stalled:{sig}"), "1").await;
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
        // Exactly two states matter to the owner (2026-11-07): the agent started
        // working and the agent finished. Everything between is a step of the
        // same run and must never move the indicator. `agent_activity` carries
        // that boolean from the runtime; `turn_start` does NOT: it is a step
        // boundary, and using it made the light depend on the model's pace.
        "agent_activity" => phase,
        // Only the END OF THE RUN clears the indicator. `agent_end_aborted` is NOT
        // that end: the runtime emits it for a RETRYABLE abort too (willRetry),
        // where the run keeps working through the retry backoff — treating it as a
        // stop made the indicator vanish exactly in the middle of the work.
        "agent_settled" => Some(false),
        _ => None,
    }
}

/// Pings Telegram while the agent works. The tick is fast so the FIRST ping lands
/// on the same beat as the turn (owner: the indicator must be visible for the
/// whole run, and a 1 s tick made it appear only at the tail of short runs);
/// after that the cadence is one send per second, which is what Telegram wants.
async fn typing_loop(store: Store) {
    const TICK_MS: u64 = 200;
    const RESEND_MS: u128 = 1000;
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(TICK_MS));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut lit = false;
    let mut warned = false;
    let mut last_sent: Option<std::time::Instant> = None;
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
            last_sent = None;
            continue;
        }
        let due = last_sent
            .map(|at| at.elapsed().as_millis() >= RESEND_MS)
            .unwrap_or(true);
        if !due {
            continue;
        }
        last_sent = Some(std::time::Instant::now());
        let thread = activity().thread.load(std::sync::atomic::Ordering::Relaxed);
        let args = if thread > 0 {
            json!({ "thread_id": thread })
        } else {
            json!({})
        };
        match chat_bot::send::typing(&store, &args).await {
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
    store: Store,
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
                                let _ = chat_bot::store::mark_settled_many_of(&store, sigs_db, at)
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
                                    "batch {sig}: run failed with a provider error; re-delivery in the same session"
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
                                // A stalled batch must not attract someone else's
                                // answer: `chat_send` without `reply_to` skips it
                                // (`ledger::latest_open_unanswered` reads this kv),
                                // otherwise an answer lands in a stale forum topic
                                // and the real incoming batch goes unanswered.
                                // A new frame for this sig clears the flag.
                                let _ = chat_bot::store::kv_set(
                                    &store,
                                    &format!("batch_stalled:{sig}"),
                                    "1",
                                )
                                .await;
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
                                    let _ = chat_bot::store::mark_emitted_of(
                                        &store,
                                        id_db.clone(),
                                        at,
                                        settled_seen,
                                    )
                                    .await;
                                    // The model now holds this batch: a frame
                                    // that carried the history window has done
                                    // its job, and a post-compaction flag it
                                    // covered is dropped with it.
                                    let _ = chat_bot::mark_window_emitted(&store, &id_db).await;
                                    last_progress.insert(id.clone(), at);
                                }
                                "failed" => {
                                    // Refused by the session file: admissible
                                    // again, so the admit unit re-pushes it —
                                    // bounded, or a session that refuses every
                                    // prompt (committed compaction, wedged
                                    // transcript) spins this forever.
                                    let err =
                                        ev.get("error").and_then(|v| v.as_str()).unwrap_or("");
                                    let sig_db = sig_of_frame(&id).to_string();
                                    let _ =
                                        chat_bot::store::fail_attempt_of(&store, sig_db.clone())
                                            .await;
                                    let n = tries.get(&sig_db).copied().unwrap_or(0);
                                    match redelivery_decision(n, FAIL_RETRY_LIMIT) {
                                        RedeliveryDecision::Retry => {
                                            *tries.entry(sig_db.clone()).or_insert(0) += 1;
                                            dlog(&format!(
                                                "delivery {id} failed ({err}); asking admit to re-push (try {})",
                                                n + 1
                                            ));
                                            // `Recover` carries the BATCH key: the frame id
                                            // (`sig#n`) names one attempt, never a batch.
                                            let _ = recover_tx.send(Recover {
                                                sig: sig_db,
                                                recreate: false,
                                            });
                                        }
                                        RedeliveryDecision::Escalate => {
                                            escalate_no_delivery(
                                                &store,
                                                &sig_db,
                                                &format!(
                                                    "delivery refused {n} times ({err})"
                                                ),
                                            )
                                            .await;
                                        }
                                    }
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
                        "context_compacted" => {
                            // The transcript was rewritten: the model no longer
                            // holds the details, so the next turn must carry
                            // the history window even in the same session.
                            let _ = chat_bot::force_window_next_turn(&store).await;
                            dlog("context compacted: the next turn carries the history window");
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
            let snap = chat_bot::store::views_of(&store, sigs).await;
            if let Ok(snap) = snap {
                let mut retired: Vec<(String, String)> = vec![];
                for view in snap {
                    if let Some(reason) = view.close_reason() {
                        let outcome = if reason == "settled" {
                            "skipped"
                        } else if reason == "skip_by_time" {
                            "skip_by_time"
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
                        // A stalled batch is never the session's fault: the
                        // model keeps its memory and the batch is re-delivered
                        // into the same session — bounded, or a session that
                        // starts no turns (failed compaction with no event)
                        // queues redeliveries forever.
                        let n = tries.get(&sig).copied().unwrap_or(0);
                        match redelivery_decision(n, FAIL_RETRY_LIMIT) {
                            RedeliveryDecision::Retry => {
                                *tries.entry(sig.clone()).or_insert(0) += 1;
                                dlog(&format!(
                                    "batch {sig} stalled in phase {phase} (budget {budget}s, turn_in_flight={turn_in_flight}); re-delivery in the same session (try {})",
                                    n + 1
                                ));
                                let _ = recover_tx.send(Recover {
                                    sig: sig.clone(),
                                    recreate: false,
                                });
                            }
                            RedeliveryDecision::Escalate => {
                                escalate_no_delivery(
                                    &store,
                                    &sig,
                                    &format!(
                                        "stalled in phase {phase} with no turn in flight after {n} redeliveries"
                                    ),
                                )
                                .await;
                            }
                        }
                        last_progress.insert(sig.clone(), now);
                    }
                }
                if !retired.is_empty() {
                    let retired_db = retired.clone();
                    let _ = chat_bot::store::close_batches_of(&store, retired_db).await;
                    for (sig, outcome) in retired {
                        // A delivered batch's proof (`delivery_state emitted`)
                        // may legally arrive AFTER retirement. Dropping the
                        // pending window here for `delivered` erased the only
                        // key `mark_window_emitted` needs, so the epoch was
                        // never recorded and the next turn repeated the
                        // 50-message window (found by the live stand). Only a
                        // batch that will never reach the model may drop it;
                        // the late proof then records the served epoch.
                        if outcome != "delivered" {
                            let _ = chat_bot::drop_window_pending(&store, &sig).await;
                        }
                        dlog(&format!("batch {sig} retired ({outcome})"));
                        inflight.remove(&sig);
                        last_progress.remove(&sig);
                    }
                }
            }
        }
        // Activity must reach the typing indicator on the same beat as the turn:
        // a 2 s poll put the first ping at the tail of a 3 s run, which reads as
        // "the indicator does not work".
        tokio::time::sleep(Duration::from_millis(200)).await;
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
    let store = match Store::open(&state) {
        Ok(store) => store,
        Err(e) => {
            ops::db_failure_sync("open", &e);
            return Err(e);
        }
    };
    // Crash window repair: a process that died between Intent -> Sending and
    // the Telegram reply leaves Sending behind. It becomes Unknown here, with
    // ONE incident and no HTTP call, so a restart cannot post a second message.
    match chat_bot::send::repair_sending(&store).await {
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
        chat_bot::store::set_self_identity(&store, bot_id, self_username.clone(), self_first.clone())
            .await?;
    }
    let offset: i64 = chat_bot::store::kv_get(&store, "offset", "0")
        .await?
        .parse()
        .unwrap_or(0);
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
    let (thumb_tx, thumb_rx) =
        tokio::sync::mpsc::channel::<chat_bot::media::ThumbJob>(chat_bot::media::MEDIA_QUEUE_CAP);
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
        store.clone(),
        control_sock_path(&state),
        state.clone(),
        out_rx,
        inbound_tx,
    ));
    tokio::task::spawn_local(tool_loop(state.clone(), chat_bot::tool_socket_path(&state)));
    tokio::task::spawn_local(admit_loop(
        store.clone(),
        job_rx,
        ack_tx,
        admitted_tx,
        inboxed_tx,
        out_tx,
        inbound_rx,
        recover_rx,
        system_path,
    ));
    tokio::task::spawn_local(track_loop(store.clone(), admitted_rx, inboxed_rx, recover_tx));
    tokio::task::spawn_local(typing_loop(store.clone()));

    // One cleanup pass at startup, then resume every unfinished download.
    // Both run before polling so a pending row from the previous process is
    // picked up without waiting for the maintenance timer.
    {
        let budget = chat_bot::media::MediaBudget::from_disk(&state);
        let st = state.clone();
        if let Err(e) = chat_bot::media::cleanup_media(
            &store,
            &st,
            &budget,
            std::time::SystemTime::now(),
            chat_bot::media::ORPHAN_GRACE,
            retention_days,
        )
        .await
        {
            dlog(&format!("startup media cleanup failed: {e:#}"));
            ops::db_failure("startup_media_cleanup", &e);
        }
        match chat_bot::store::pending_jobs_of(&store, chat_bot::media::MEDIA_QUEUE_CAP).await
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
        match chat_bot::store::pending_thumbs_of(&store, chat_bot::media::MEDIA_QUEUE_CAP).await
        {
            Ok(jobs) => {
                for job in jobs {
                    if thumb_tx.try_send(job).is_err() {
                        break;
                    }
                }
            }
            Err(e) => {
                dlog(&format!("startup thumb resume failed: {e:#}"));
                ops::db_failure("startup_thumb_resume", &e);
            }
        }
    }
    tokio::task::spawn_local(media_loop(store.clone(), state.clone(), media_rx));
    tokio::task::spawn_local(thumb_loop(store.clone(), state.clone(), thumb_rx));
    tokio::task::spawn_local(media_maintenance_loop(
        store.clone(),
        state.clone(),
        media_tx.clone(),
        thumb_tx.clone(),
        retention_days,
    ));
    dlog(&format!(
        "chatd running, independent units (ingest/media/admit/track), database on its own thread, control socket {} (host socket {})",
        control_sock_path(&state).display(),
        host_socket()
    ));
    ingest_loop(
        store, state, bot_id, username, offset, job_tx, media_tx, thumb_tx, ack_rx,
    )
    .await
}

/// Ingest unit: polls Telegram continuously. Parsing is pure; one page of
/// envelopes is committed together with `kv("offset")` in a single
/// `commit_page` transaction, and media downloads are only enqueued (the row
/// is already durable in `pending`). A commit failure leaves the cursor where
/// it was and the next poll re-fetches the same page.
/// A linked channel post arrives twice: first as `channel_post` with a
/// negative sender id, then as the automatic group forward with a positive
/// one. The channel copy's message id is not addressable in the discussion
/// chat, so it must never become its own batch. Hold it briefly for the
/// group twin; a lone channel post is still released after the grace.
fn is_channel_copy(row: &BatchMsg) -> bool {
    row.user_id.map(|u| u < 0).unwrap_or(false)
}

fn drop_pending_twins(pending: &mut Vec<(Instant, BatchMsg)>, group: &BatchMsg) -> usize {
    let Some(key) = channel_post_identity(group) else {
        return 0;
    };
    let before = pending.len();
    pending.retain(|(_, p)| channel_post_identity(p).as_ref() != Some(&key));
    before - pending.len()
}

fn take_due_channel_copies(
    pending: &mut Vec<(Instant, BatchMsg)>,
    grace: Duration,
    now: Instant,
) -> Vec<BatchMsg> {
    let mut due = vec![];
    pending.retain(|(at, row)| {
        if now.saturating_duration_since(*at) >= grace {
            due.push(row.clone());
            false
        } else {
            true
        }
    });
    due
}

async fn ingest_loop(
    store: Store,
    _state: String,
    bot_id: i64,
    username: String,
    mut offset: i64,
    job_tx: tokio::sync::mpsc::UnboundedSender<Job>,
    media_tx: tokio::sync::mpsc::Sender<chat_bot::media::MediaJob>,
    thumb_tx: tokio::sync::mpsc::Sender<chat_bot::media::ThumbJob>,
    mut ack_rx: tokio::sync::mpsc::UnboundedReceiver<String>,
) -> Result<()> {
    let window: f64 = cfg("CHATBOT_BATCH_SEC", "10").parse().unwrap_or(10.0);
    let mut batch: Vec<BatchMsg> = vec![];
    let channel_grace = Duration::from_secs(
        cfg("CHATBOT_CHANNEL_TWIN_GRACE_SEC", "6")
            .parse()
            .unwrap_or(6),
    );
    let mut pending_channels: Vec<(Instant, BatchMsg)> = vec![];
    // A restart must not lose a channel copy that is still waiting for its
    // group forward. Rebuild the in-memory grace window from the durable kv
    // ledger; retire records older than the hold so the ledger stays bounded.
    if let Ok(held) = chat_bot::store::held_channels_of(&store).await {
        let now = now_ms();
        let stale = (channel_grace.as_millis() as i64).saturating_mul(4);
        let mut fresh_ids = vec![];
        for (mid, _identity, first_seen_ms) in &held {
            if now.saturating_sub(*first_seen_ms) <= stale {
                fresh_ids.push(json!(mid));
            } else {
                let _ = chat_bot::store::clear_held_channel_of(&store, *mid).await;
            }
        }
        if !fresh_ids.is_empty() {
            if let Ok(rows) = chat_bot::store::batch_rows_of(&store, fresh_ids).await {
                for row in rows.into_iter().filter(is_channel_copy) {
                    pending_channels.push((Instant::now(), row));
                }
            }
        }
        if !pending_channels.is_empty() {
            dlog(&format!(
                "dedupe: recovered {} durable channel hold(s) after restart",
                pending_channels.len()
            ));
        }
    }
    let mut poll_failures = 0u32;
    let mut last_flush = std::time::Instant::now();
    let chat: i64 = chat_bot::store::chat_id(&store).await?;
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
        // A consumed update that we do not store must never be invisible: without
        // these two lines a foreign-chat message or an unsupported update kind is
        // eaten silently and the bot simply looks dead. Bounded to one line a
        // minute per cause so a busy chat cannot flood the log.
        let mut noted_kind: std::collections::HashMap<String, std::time::Instant> =
            std::collections::HashMap::new();
        let mut noted_chat: std::collections::HashMap<i64, std::time::Instant> =
            std::collections::HashMap::new();
        for u in updates.as_array().cloned().unwrap_or_default() {
            let uid = u.get("update_id").and_then(|v| v.as_i64()).unwrap_or(0);
            through = through.max(uid + 1);
            let raw_msg = u.get("message").cloned();
            let chan_post = u.get("channel_post").cloned();
            let msg = raw_msg.clone().or(chan_post.clone());
            let mut msg = match msg {
                Some(m) => m,
                None => {
                    let kind = u
                        .as_object()
                        .and_then(|o| o.keys().find(|k| k.as_str() != "update_id").cloned())
                        .unwrap_or_else(|| "unknown".to_string());
                    let fresh = noted_kind
                        .get(&kind)
                        .map(|at| at.elapsed().as_secs() >= 60)
                        .unwrap_or(true);
                    if fresh {
                        noted_kind.insert(kind.clone(), std::time::Instant::now());
                        dlog(&format!("update {uid} ignored: unsupported kind {kind}"));
                    }
                    continue;
                }
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
                Ok(None) => {
                    let cid = msg
                        .get("chat")
                        .and_then(|c| c.get("id"))
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    let fresh = noted_chat
                        .get(&cid)
                        .map(|at| at.elapsed().as_secs() >= 60)
                        .unwrap_or(true);
                    if fresh {
                        noted_chat.insert(cid, std::time::Instant::now());
                        dlog(&format!(
                            "update {uid} dropped: chat {cid} is not the served chat {chat}"
                        ));
                    }
                }
                Err(e) => dlog(&format!("prepare message failed: {e:#}")),
            }
        }
        if through > offset {
            let mut by_key: std::collections::HashMap<MessageKey, Envelope> =
                envelopes.iter().cloned().map(|e| (e.key, e)).collect();
            let page = match chat_bot::store::commit_page(&store, envelopes, through).await {
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
                // A video kind is never downloaded as bytes; only its frame is.
                if env.thumb_state == chat_bot::media::MediaState::Pending {
                    let job = chat_bot::media::ThumbJob {
                        row_id: ins.row_id,
                        file_id: env.thumb_file_id.clone(),
                    };
                    if thumb_tx.try_send(job).is_err() {
                        dlog("thumb queue full; pending frame will be resumed by maintenance");
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
                }
                if is_channel_copy(&row) {
                    if let Some(identity) = chat_bot::channel_post_identity(&row) {
                        let _ = chat_bot::store::note_held_channel_of(
                            &store,
                            row.message_id,
                            &identity,
                            now_ms(),
                        )
                        .await;
                    }
                    dlog(&format!(
                        "dedupe: holding channel copy {} for group twin",
                        row.message_id
                    ));
                    pending_channels.push((Instant::now(), row));
                    continue;
                }
                let dropped = drop_pending_twins(&mut pending_channels, &row);
                if dropped > 0 {
                    if let Some(identity) = chat_bot::channel_post_identity(&row) {
                        let _ =
                            chat_bot::store::clear_held_channel_identity_of(&store, &identity).await;
                    }
                    dlog(&format!(
                        "dedupe: dropped {dropped} pending channel twin(s) when the group copy arrived"
                    ));
                }
                if urgent {
                    let mut combo = std::mem::take(&mut batch);
                    combo.push(row);
                    inflight += 1;
                    // An addressed message is answered at once, but its own photo
                    // may still be downloading: wait for it briefly instead of
                    // handing the model a bare `[photo]` marker.
                    media_settle(&store, &combo).await;
                    if job_tx.send(Job { batch: combo }).is_err() {
                        dlog("admit unit gone; dropping urgent batch");
                    }
                    last_flush = std::time::Instant::now();
                } else {
                    batch.push(row);
                }
            }
        }
        let due = take_due_channel_copies(&mut pending_channels, channel_grace, Instant::now());
        // No group twin arrived within the grace: this is a lone channel post
        // and will be admitted normally. Retire its durable hold so a later
        // restart does not re-hold an already-answered copy.
        for row in &due {
            let _ = chat_bot::store::clear_held_channel_of(&store, row.message_id).await;
        }
        let force_flush = due.iter().any(|m| m.to_me);
        batch.extend(due);
        if !batch.is_empty() && (force_flush || last_flush.elapsed().as_secs_f64() >= window) {
            let combo = std::mem::take(&mut batch);
            inflight += 1;
            media_settle(&store, &combo).await;
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

/// Wait briefly for the media of the batch that is about to be sent. The batch
/// is a snapshot taken at ingest, so a photo that lands a moment later would
/// otherwise reach the model as a bare `[photo]` marker. The wait is bounded by
/// `MEDIA_WAIT_SEC` and keyed to the real row state, never to a fixed sleep.
async fn media_settle(store: &Store, batch: &[chat_bot::BatchMsg]) {
    let deadline =
        tokio::time::Instant::now() + std::time::Duration::from_secs(chat_bot::media::MEDIA_WAIT_SEC);
    loop {
        let ids: Vec<i64> = batch.iter().map(|m| m.message_id).collect();
        let pending = chat_bot::store::any_pending_of(store, ids)
            .await
            .unwrap_or(false);
        if !pending || tokio::time::Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    }
}

/// Media fetcher: one job at a time, each bounded by FETCH_DEADLINE. A budget
/// refusal is not an error: the row stays `pending` and maintenance resumes it.
async fn media_loop(
    store: Store,
    state: String,
    mut rx: tokio::sync::mpsc::Receiver<chat_bot::media::MediaJob>,
) {
    let source = chat_bot::media::TelegramSource;
    while let Some(job) = rx.recv().await {
        let budget = chat_bot::media::MediaBudget::from_disk(&state);
        match chat_bot::media::fetch_job(&store, &state, &job, &budget, &source).await {
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

/// Frame fetcher: same discipline as the media fetcher — one job at a time, a
/// budget refusal leaves the row `pending` for maintenance. A frame is the only
/// part of a video the model can actually look at on this wire.
async fn thumb_loop(
    store: Store,
    state: String,
    mut rx: tokio::sync::mpsc::Receiver<chat_bot::media::ThumbJob>,
) {
    let source = chat_bot::media::TelegramSource;
    while let Some(job) = rx.recv().await {
        let budget = chat_bot::media::MediaBudget::from_disk(&state);
        match chat_bot::media::fetch_thumb(&store, &state, &job, &budget, &source).await {
            Ok(chat_bot::media::ThumbOutcome::Deferred) => {
                dlog(&format!("thumb budget deferred row {}", job.row_id));
            }
            Ok(chat_bot::media::ThumbOutcome::Skipped) => {
                dlog(&format!("thumb row {} vanished before finalize", job.row_id));
            }
            Ok(chat_bot::media::ThumbOutcome::Finalized { size, .. }) => {
                dlog(&format!("thumb row {} ready ({size} bytes)", job.row_id));
            }
            Err(e) => dlog(&format!("thumb fetch failed for {}: {e:#}", job.row_id)),
        }
    }
}

/// Startup cleanup plus a five-minute pass: one cleanup pass, then resume
/// pending downloads. The victim order inside cleanup is fixed in
/// `media::cleanup_media`.
async fn media_maintenance_loop(
    store: Store,
    state: String,
    tx: tokio::sync::mpsc::Sender<chat_bot::media::MediaJob>,
    thumb_tx: tokio::sync::mpsc::Sender<chat_bot::media::ThumbJob>,
    retention_days: i64,
) {
    loop {
        tokio::time::sleep(chat_bot::media::MAINTENANCE_INTERVAL).await;
        let budget = chat_bot::media::MediaBudget::from_disk(&state);
        let st = state.clone();
        if let Err(e) = chat_bot::media::cleanup_media(
            &store,
            &st,
            &budget,
            std::time::SystemTime::now(),
            chat_bot::media::MEDIA_GRACE,
            retention_days,
        )
        .await
        {
            dlog(&format!("media cleanup failed: {e:#}"));
            ops::db_failure("media_cleanup", &e);
        }
        match chat_bot::store::pending_jobs_of(&store, chat_bot::media::MEDIA_QUEUE_CAP).await
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
        match chat_bot::store::pending_thumbs_of(&store, chat_bot::media::MEDIA_QUEUE_CAP).await
        {
            Ok(jobs) => {
                for job in jobs {
                    if thumb_tx.try_send(job).is_err() {
                        break;
                    }
                }
            }
            Err(e) => {
                dlog(&format!("thumb resume failed: {e:#}"));
                ops::db_failure("thumb_resume", &e);
            }
        }
    }
}

#[cfg(test)]
mod channel_twin_tests {
    use super::{drop_pending_twins, is_channel_copy, take_due_channel_copies};
    use crate::BatchMsg;
    use std::time::{Duration, Instant};

    fn msg(id: i64, user: Option<i64>, text: &str) -> BatchMsg {
        BatchMsg {
            message_id: id,
            date: 1,
            user_id: user,
            username: "u".into(),
            first_name: "U".into(),
            text: text.into(),
            reply_to: None,
            media_path: String::new(),
            media_kind: String::new(),
            file_id: String::new(),
            thumb_path: String::new(),
            to_me: false,
        }
    }

    #[test]
    fn a_group_twin_cancels_the_held_channel_copy() {
        let mut pending = vec![(
            Instant::now(),
            msg(4255, Some(-1004421941006), "пост"),
        )];
        assert!(is_channel_copy(&pending[0].1));
        let dropped = drop_pending_twins(&mut pending, &msg(18569, Some(777000), "пост"));
        assert_eq!(dropped, 1);
        assert!(pending.is_empty());
    }

    #[test]
    fn an_unmatched_group_copy_keeps_the_held_channel_copy() {
        let mut pending = vec![(Instant::now(), msg(1, Some(-100), "пост"))];
        assert_eq!(
            drop_pending_twins(&mut pending, &msg(2, Some(777000), "другой")),
            0
        );
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn a_lone_channel_copy_is_released_after_the_grace() {
        let now = Instant::now();
        let mut pending = vec![
            (now - Duration::from_secs(7), msg(1, Some(-100), "старый")),
            (now, msg(2, Some(-100), "свежий")),
        ];
        let due = take_due_channel_copies(&mut pending, Duration::from_secs(6), now);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].message_id, 1);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].1.message_id, 2);
    }
}

#[cfg(test)]
mod settle_tests {
    use super::{
        frame_seq, redelivery_decision, settle_decision, RedeliveryDecision, SettleDecision,
        FAIL_RETRY_LIMIT,
    };

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

    #[test]
    fn stalled_and_refused_batches_retry_then_escalate() {
        // The failed-compaction signature: no run, no turn, same session.
        // Bounded retries, then the owner hears about it once.
        assert_eq!(redelivery_decision(0, FAIL_RETRY_LIMIT), RedeliveryDecision::Retry);
        assert_eq!(
            redelivery_decision(FAIL_RETRY_LIMIT - 1, FAIL_RETRY_LIMIT),
            RedeliveryDecision::Retry
        );
        assert_eq!(
            redelivery_decision(FAIL_RETRY_LIMIT, FAIL_RETRY_LIMIT),
            RedeliveryDecision::Escalate
        );
        assert_eq!(
            redelivery_decision(FAIL_RETRY_LIMIT + 5, FAIL_RETRY_LIMIT),
            RedeliveryDecision::Escalate
        );
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
        assert_eq!(
            activity_state_for("turn_start", None),
            None,
            "a step boundary must never move the indicator"
        );
        assert_eq!(activity_state_for("turn_end", None), None);
        assert_eq!(activity_state_for("agent_settled", None), Some(false));
        assert_eq!(activity_state_for("agent_end_aborted", None), None);
        // The start edge itself is lit by `admit_job` the moment the batch is
        // accepted, so the light cannot wait for the runtime's first turn.
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
