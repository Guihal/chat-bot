//! Cycle 3 acceptance: sending and batch closure through the real daemon code.
//!
//! Every test drives the public send path (`chat_bot::send::chat_send`) against
//! a fake Bot API and inspects the journal tables afterwards. Environment
//! variables are process-global, so the tests serialise on one mutex.
use chat_bot::db::Db;
use chat_bot::store::Store;
use chat_bot::ledger;
use chat_bot::ledger::SendOutcome;
use chat_bot::store::{BeginSend, PartMeta};
use chat_bot::media::{cleanup_media, MediaBudget};
use chat_bot::*;
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

static ENV_LOCK: Mutex<()> = Mutex::new(());

struct FakeApi {
    base: String,
    methods: Arc<Mutex<Vec<String>>>,
    requests: Arc<Mutex<Vec<Value>>>,
    _seq: Arc<Mutex<i64>>,
}

fn start_fake_api() -> FakeApi {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake api");
    let addr = listener.local_addr().unwrap();
    let methods = Arc::new(Mutex::new(Vec::<String>::new()));
    let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
    let seq = Arc::new(Mutex::new(1000i64));
    let m = methods.clone();
    let rq = requests.clone();
    let s = seq.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            serve(stream, m.clone(), rq.clone(), s.clone());
        }
    });
    FakeApi {
        base: format!("http://{addr}"),
        methods,
        requests,
        _seq: seq,
    }
}

fn serve(
    mut stream: TcpStream,
    methods: Arc<Mutex<Vec<String>>>,
    requests: Arc<Mutex<Vec<Value>>>,
    seq: Arc<Mutex<i64>>,
) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).unwrap_or(0) == 0 {
        return;
    }
    let mut content_len = 0usize;
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).unwrap_or(0) == 0 {
            break;
        }
        if header.trim().is_empty() {
            break;
        }
        if let Some(v) = header.to_ascii_lowercase().strip_prefix("content-length:") {
            content_len = v.trim().parse().unwrap_or(0);
        }
    }
    let mut body = vec![0u8; content_len];
    let _ = reader.read_exact(&mut body);
    let path = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .to_string();
    let method = path.rsplit('/').next().unwrap_or("").to_string();
    methods.lock().unwrap().push(method.clone());
    let payload = if method.starts_with("send") && method != "sendChatAction" {
        let mut n = seq.lock().unwrap();
        *n += 1;
        let request: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
        requests.lock().unwrap().push(request.clone());
        let requested = |field: &str| {
            request
                .get(field)
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
        };
        let mut result = json!({"message_id": *n});
        let media = match method.as_str() {
            "sendAnimation" => requested("animation"),
            "sendSticker" => requested("sticker"),
            _ => None,
        };
        if let Some(fid) = media {
            let field = if method == "sendSticker" {
                "sticker"
            } else {
                "animation"
            };
            result[field] = json!({
                "file_id": fid,
                "file_unique_id": format!("uniq-{fid}"),
                "file_size": 5120
            });
        }
        json!({"ok": true, "result": result})
    } else {
        json!({"ok": true, "result": true})
    };
    let body = payload.to_string();
    let _ = write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
}

struct Fixture {
    _guard: std::sync::MutexGuard<'static, ()>,
    state: String,
    db: Db,
    store: Store,
    api: FakeApi,
    dir: PathBuf,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn fixture(tag: &str) -> Fixture {
    let guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = std::env::temp_dir().join(format!("chat-send-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let token = dir.join("token");
    std::fs::write(&token, "1:test\n").unwrap();
    std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
    let api = start_fake_api();
    std::env::set_var("CHATBOT_API_BASE", &api.base);
    std::env::set_var("CHATBOT_TOKEN_FILE", &token);
    std::env::set_var("CHATBOT_CHAT_ID", "-100");
    std::env::set_var("CHATBOT_STATE_DIR", &dir);
    let state = dir.join("state").to_string_lossy().to_string();
    let db = Db::open(&state).unwrap();
    let store = Store::open(&state).unwrap();
    db.call(|con| {
        kv_set(con, "chat_id", "-100")?;
        kv_set(con, "self_id", "550001")?;
        kv_set(con, "self_username", "smokebot")?;
        kv_set(con, "self_first", "Smoke")?;
        Ok(())
    })
    .await
    .unwrap();
    db.write(|tx| tx.seal_batch("sig-1", -100, Some(77), &[101], &[101]))
        .await
        .unwrap();
    Fixture {
        _guard: guard,
        state,
        db,
        store,
        api,
        dir,
    }
}

fn calls(f: &Fixture) -> Vec<String> {
    f.api.methods.lock().unwrap().clone()
}

async fn view(f: &Fixture, key: &str) -> ledger::BatchView {
    let key = key.to_string();
    f.db.call(move |con| Ok(ledger::view(con, &key).unwrap().unwrap()))
        .await
        .unwrap()
}

async fn answer_state(f: &Fixture) -> (String, Option<String>, i64) {
    f.db.call(|con| {
        Ok(con.query_row(
            "SELECT state, response, (SELECT COUNT(*) FROM answers) FROM answers LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?)
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn answer_without_quote_closes_the_batch() {
    let f = fixture("quote").await;
    let raw = chat_bot::send::chat_send(
        &f.store,
        &json!({"batch_key": "sig-1", "text": "ответ без цитаты"}),
    )
    .await
    .unwrap();
    assert!(raw.contains("ответ без цитаты"), "{raw}");
    let v = view(&f, "sig-1").await;
    assert_eq!(v.state, "closed", "batch stayed open without reply_to");
    assert_eq!(v.outcome.as_deref(), Some("delivered"));
    assert_eq!(v.sent, 1);
    let (state, response, count) = answer_state(&f).await;
    assert_eq!(state, "closed");
    assert_eq!(count, 1);
    assert_eq!(response.as_deref(), Some(raw.as_str()));
    let mirror: (Option<i64>, Option<i64>, i64) =
        f.db.call(|con| {
            Ok(con.query_row(
                "SELECT reply_to, thread_id, message_id FROM messages WHERE username='smokebot'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(mirror.0, None, "reply_to must stay decoration");
    assert_eq!(mirror.1, Some(77), "mirror lost the batch thread");
    assert!(mirror.2 > 0);
}

/// Owner's contract (2026-11-08): the extension carries no batch ticket at all.
/// An unaddressed send belongs to the batch that is still waiting.
#[tokio::test(flavor = "current_thread")]
async fn a_send_without_a_ticket_answers_the_waiting_batch() {
    let f = fixture("no-ticket").await;
    let raw = chat_bot::send::chat_send(&f.store, &json!({"text": "ответ без билета"}))
        .await
        .unwrap();
    assert!(raw.contains("ответ без билета"), "{raw}");
    let v = view(&f, "sig-1").await;
    assert_eq!(v.state, "closed", "the waiting batch owns the answer");
    assert_eq!(v.outcome.as_deref(), Some("delivered"));
    assert_eq!(v.sent, 1);
}

/// Two batches can be open at once (a steer accepted mid-turn, a post and its
/// discussion copy). A late answer to the earlier message must follow its own
/// message id, not "whatever opened last" — the wrong pick silently drops
/// `reply_to` and leaves the real message unanswered.
/// A batch whose automated retries were exhausted stays open on purpose, but it
/// must never take someone else's answer: the reply would land in a stale forum
/// topic while the real message went unanswered.
#[tokio::test(flavor = "current_thread")]
async fn a_stalled_batch_does_not_take_the_answer() {
    let f = fixture("stalled").await;
    f.db.write(|tx| tx.seal_batch("sig-2", -100, Some(78), &[201], &[201]))
        .await
        .unwrap();
    chat_bot::store::kv_set(&f.store, "batch_stalled:sig-2", "1")
        .await
        .unwrap();
    let raw = chat_bot::send::chat_send(&f.store, &json!({"text": "ответ в живую пачку"}))
        .await
        .unwrap();
    assert!(raw.contains("ответ в живую пачку"), "{raw}");
    assert_eq!(
        view(&f, "sig-1").await.state,
        "closed",
        "the answer belongs to the batch that is not stalled"
    );
    assert_ne!(
        view(&f, "sig-2").await.state,
        "closed",
        "a stalled batch must stay untouched"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_late_answer_follows_its_own_message_id() {
    let f = fixture("two-open").await;
    f.db.write(|tx| tx.seal_batch("sig-2", -100, Some(78), &[201], &[201]))
        .await
        .unwrap();
    let raw = chat_bot::send::chat_send(
        &f.store,
        &json!({"text": "ответ на первое", "reply_to": 101}),
    )
    .await
    .unwrap();
    assert!(raw.contains("ответ на первое"), "{raw}");
    let first = view(&f, "sig-1").await;
    assert_eq!(first.state, "closed", "the addressed batch took the answer");
    assert_eq!(first.outcome.as_deref(), Some("delivered"));
    let second = view(&f, "sig-2").await;
    assert_ne!(second.state, "closed", "the other batch stayed waiting");
}

/// Tolerance, not an instruction. The owner removed the gate that refused a
/// reply target outside the batch, so the daemon must keep any target as given —
/// including one from the history or the GIF catalog. Whether the assistant
/// quotes at all is a prompt rule (owner: it does not), which is why this test
/// proves only that nothing is refused.
#[tokio::test(flavor = "current_thread")]
async fn an_out_of_batch_reply_is_accepted_but_not_encouraged() {
    let f = fixture("free-reply").await;
    let raw = chat_bot::send::chat_send(
        &f.store,
        &json!({"text": "ответ в историю", "reply_to": 999_001}),
    )
    .await
    .unwrap();
    assert!(raw.contains("ответ в историю"), "{raw}");
    let mirror: Option<i64> = f
        .db
        .call(|con| {
            Ok(con.query_row(
                "SELECT reply_to FROM messages WHERE username='smokebot'",
                [],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(mirror, Some(999_001), "the quote must not be restricted");
    // An id that does not exist in the answering chat is exactly what a linked
    // channel post looks like. Without this flag Telegram fails the whole call
    // and the answer is lost, so the flag is part of the contract.
    let sent = f.api.requests.lock().unwrap();
    assert_eq!(
        sent.last().and_then(|r| r.get("reply_to_message_id")).and_then(|v| v.as_i64()),
        Some(999_001),
        "the quote reaches the Bot API as given"
    );
    assert_eq!(
        sent.last().and_then(|r| r.get("allow_sending_without_reply")).and_then(|v| v.as_bool()),
        Some(true),
        "a dead reply target must not cost the message"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn second_answer_is_refused_and_same_fingerprint_replays() {
    let f = fixture("replay").await;
    let args = json!({"batch_key": "sig-1", "text": "первый"});
    let first = chat_bot::send::chat_send(&f.store, &args).await.unwrap();
    assert_eq!(calls(&f), vec!["sendMessage".to_string()]);
    let again = chat_bot::send::chat_send(&f.store, &args).await.unwrap();
    assert_eq!(again, first, "replay was not byte-identical");
    assert_eq!(calls(&f).len(), 1, "replay reached the Bot API again");
    let refused = chat_bot::send::chat_send(
        &f.store,
        &json!({"batch_key": "sig-1", "text": "второй другой"}),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(refused.contains("answer_exists"), "{refused}");
    assert_eq!(calls(&f).len(), 1, "refusal reached the Bot API");
    assert_eq!(answer_state(&f).await.2, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn crash_between_sending_and_reply_becomes_unknown_without_resend() {
    let f = fixture("crash").await;
    f.db.write(|tx| {
        let answer = match tx.open_answer("sig-1", "manual-fingerprint")? {
            ledger::OpenAnswer::Opened(a) => a,
            other => panic!("unexpected answer: {other:?}"),
        };
        let intent = tx.begin_send(answer, 0, "text", &json!({"text": "в процессе"}))?;
        tx.start_send(&intent)
    })
    .await
    .unwrap();
    let repaired = chat_bot::send::repair_sending(&f.store).await.unwrap();
    assert_eq!(repaired, 1);
    let v = view(&f, "sig-1").await;
    assert_eq!(v.state, "closed");
    assert_eq!(v.outcome.as_deref(), Some("delivered_unknown"));
    assert_eq!(v.unknown_sends, 1);
    let incidents: i64 =
        f.db.call(|con| {
            Ok(con.query_row(
                "SELECT COUNT(*) FROM incidents WHERE kind='send_unknown_recovered'",
                [],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(incidents, 1, "startup repair must emit exactly one alert");
    let err =
        chat_bot::send::chat_send(&f.store, &json!({"batch_key": "sig-1", "text": "в процессе"}))
            .await
            .unwrap_err()
            .to_string();
    assert!(err.contains("answer_exists"), "{err}");
    assert!(calls(&f).is_empty(), "a recovered Unknown was resent");
}

#[tokio::test(flavor = "current_thread")]
async fn sticker_plus_text_is_two_bot_api_calls() {
    let f = fixture("sticker").await;
    let raw = chat_bot::send::chat_send(
        &f.store,
        &json!({
            "batch_key": "sig-1",
            "text": "подпись отдельным сообщением",
            "sticker_file_id": "stick-1"
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        calls(&f),
        vec!["sendSticker".to_string(), "sendMessage".to_string()]
    );
    let payload: Value = serde_json::from_str(raw.split('\n').next().unwrap()).unwrap();
    assert_eq!(
        payload
            .get("messages")
            .and_then(|m| m.as_array())
            .map(|a| a.len()),
        Some(2),
        "{payload}"
    );
    let v = view(&f, "sig-1").await;
    assert_eq!(v.state, "closed");
    assert_eq!(v.sent, 2);
    let sends: i64 =
        f.db.call(|con| {
            Ok(
                con.query_row("SELECT COUNT(*) FROM sends WHERE state='sent'", [], |r| {
                    r.get(0)
                })?,
            )
        })
        .await
        .unwrap();
    assert_eq!(sends, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn own_sent_gif_and_sticker_mirror_the_receipt_and_feed_the_catalogue() {
    let f = fixture("ownmedia").await;
    let raw = chat_bot::send::chat_send(
        &f.store,
        &json!({"batch_key": "sig-1", "text": "гиф", "gif_file_id": "gif-src"}),
    )
    .await
    .unwrap();
    let receipt: Value = serde_json::from_str(raw.split('\n').next().unwrap()).unwrap();
    assert_eq!(receipt["file_id"], json!("gif-src"));
    assert_eq!(receipt["file_unique_id"], json!("uniq-gif-src"));
    let mid = receipt["message_id"].as_i64().unwrap();
    let row: (String, String, String, String, String, i64) = f
        .db
        .call(move |con| {
            Ok(con.query_row(
                "SELECT media_kind,file_id,file_unique_id,media_state,media_path,media_size FROM messages WHERE message_id=?",
                [mid],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                    ))
                },
            )?)
        })
        .await
        .unwrap();
    assert_eq!(
        row,
        (
            "animation".to_string(),
            "gif-src".to_string(),
            "uniq-gif-src".to_string(),
            "ready".to_string(),
            "".to_string(),
            5120
        ),
        "own sent gif lost a receipt field in the mirror"
    );
    let page =
        f.db.call(|con| {
              let (rows, total) = chat_bot::store::catalog_page(con, 0, 10)?;
              Ok(chat_bot::catalog::page_of(rows, 0, total))
          })
            .await
            .unwrap();
    assert!(
        page.records.iter().any(|r| r.file_id() == "gif-src"),
        "the catalogue did not learn our own gif: {page:?}"
    );

    f.db.write(|tx| tx.seal_batch("sig-2", -100, Some(77), &[102], &[102]))
        .await
        .unwrap();
    let raw = chat_bot::send::chat_send(
        &f.store,
        &json!({"batch_key": "sig-2", "sticker_file_id": "stick-src"}),
    )
    .await
    .unwrap();
    let receipt: Value = serde_json::from_str(raw.split('\n').next().unwrap()).unwrap();
    let mid = receipt["message_id"].as_i64().unwrap();
    let row: (String, String, String, String) = f
        .db
        .call(move |con| {
            Ok(con.query_row(
                "SELECT media_kind,file_id,file_unique_id,media_state FROM messages WHERE message_id=?",
                [mid],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(
        row,
        (
            "sticker".to_string(),
            "stick-src".to_string(),
            "uniq-stick-src".to_string(),
            "ready".to_string()
        )
    );
    let page =
        f.db.call(|con| {
              let (rows, total) = chat_bot::store::catalog_page(con, 0, 10)?;
              Ok(chat_bot::catalog::page_of(rows, 0, total))
          })
            .await
            .unwrap();
    assert!(
        !page.records.iter().any(|r| r.file_id() == "stick-src"),
        "a sticker entered the gif catalogue: {page:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn retention_removes_only_past_horizon_and_keeps_parents_and_catalogues() {
    let f = fixture("retention").await;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let old = now - 40 * 86_400;
    let recent = now - 1_000;
    f.db.call(move |con| {
        let insert = |mid: i64, date: i64, text: &str, reply: Option<i64>, kind: &str, fid: &str, uid: &str| {
            con.execute(
                "INSERT INTO messages(chat_id,update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id,file_unique_id,media_kind,media_state,media_size)
                 VALUES(-100,0,?,?,556,'u','U',?,?,'',?,?,?,?,?)",
                rusqlite::params![mid, date, text, reply, fid, uid, kind, if kind == "animation" { "ready" } else { "absent" }, if kind == "animation" { 100i64 } else { 0i64 }],
            )?;
            Ok::<(), anyhow::Error>(())
        };
        insert(900, old, "old plain", None, "", "", "")?;
        insert(901, old - 100, "old parent", None, "", "", "")?;
        insert(902, old, "reply to parent", Some(901), "", "", "")?;
        insert(903, recent, "recent plain", None, "", "", "")?;
        insert(904, old - 200, "old gif keeper", None, "animation", "gif-A", "uid-A")?;
        insert(906, old, "old gif duplicate", None, "animation", "gif-A2", "uid-A")?;
        insert(907, old, "second gif key", None, "animation", "gif-B", "uid-B")?;
        Ok(())
    })
    .await
    .unwrap();
    std::fs::create_dir_all(chat_bot::media::media_dir(&f.state)).unwrap();
    let budget = MediaBudget {
        free: 10 * 1024 * 1024 * 1024,
        media_used: 0,
        media_cap: chat_bot::media::MEDIA_CAP_BYTES,
        reserve: chat_bot::media::FREE_RESERVE_BYTES,
    };
    let state = f.state.clone();
    let report = cleanup_media(
        &f.store,
        &state,
        &budget,
        SystemTime::now(),
        Duration::ZERO,
        chat_bot::media::ROW_RETENTION_DAYS,
    )
    .await
    .unwrap();
    assert!(report.rows_removed >= 3, "{report:?}");
    let exists = |mid: i64| {
        let con = rusqlite::Connection::open(Path::new(&f.state).join("history.db")).unwrap();
        con.query_row(
            "SELECT COUNT(*) FROM messages WHERE message_id=?",
            [mid],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
            > 0
    };
    assert!(!exists(900), "old unprotected row survived");
    assert!(exists(901), "reply parent was deleted");
    assert!(exists(903), "row inside the horizon was deleted");
    assert!(
        !exists(904),
        "older duplicate of the catalogue key survived"
    );
    assert!(exists(906), "catalogue key lost its newest keeper");
    assert!(exists(907), "second catalogue key lost its keeper");
    assert!(
        chat_bot::media::ROW_RETENTION_DAYS > 0,
        "shipped retention horizon must be non-zero"
    );
    let _ = &f.dir;
}

#[tokio::test(flavor = "current_thread")]
async fn intent_only_crash_closes_the_batch_with_a_declared_outcome() {
    let f = fixture("intent-only").await;
    // Crash window: the answer and its durable intent exist, but start_send
    // (and therefore any HTTP call) never ran.
    f.db.write(|tx| {
        let answer = match tx.open_answer("sig-1", "intent-only")? {
            ledger::OpenAnswer::Opened(a) => a,
            other => panic!("unexpected answer: {other:?}"),
        };
        tx.begin_send(answer, 0, "text", &json!({"text": "в процессе"}))
    })
    .await
    .unwrap();
    let before = view(&f, "sig-1").await;
    assert_eq!(before.state, "answered");
    assert_eq!(before.pending_sends, 1);
    assert_eq!(before.sent, 0);

    let repaired = chat_bot::send::repair_sending(&f.store).await.unwrap();
    assert_eq!(repaired, 1, "intent-only crash was not recovered");
    let v = view(&f, "sig-1").await;
    assert_eq!(v.state, "closed", "intent-only batch never closed");
    assert_eq!(v.outcome.as_deref(), Some("delivered_unknown"));
    assert_eq!(v.pending_sends, 0);
    assert_eq!(v.unknown_sends, 1);
    let incidents: i64 =
        f.db.call(|con| {
            Ok(con.query_row(
                "SELECT COUNT(*) FROM incidents WHERE kind='send_unknown_recovered'",
                [],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(incidents, 1, "recovery must declare one incident");
    let err =
        chat_bot::send::chat_send(&f.store, &json!({"batch_key": "sig-1", "text": "в процессе"}))
            .await
            .unwrap_err()
            .to_string();
    assert!(err.contains("answer_exists"), "{err}");
    assert!(calls(&f).is_empty(), "a recovered intent was sent");
}

#[tokio::test(flavor = "current_thread")]
async fn compact_after_no_quote_does_not_repush_the_closed_batch() {
    let f = fixture("compact-no-quote").await;
    // The batch has a real incoming row; without it regeneration would return
    // None for the wrong reason and the test would not discriminate.
    f.db.call(|con| {
        con.execute(
            "INSERT INTO messages(chat_id,update_id,message_id,date,user_id,username,first_name,text) VALUES(-100,1,101,1000,7,'u','U','вопрос без цитаты')",
            [],
        )?;
        Ok(())
    })
    .await
    .unwrap();
    chat_bot::send::chat_send(
        &f.store,
        &json!({"batch_key": "sig-1", "text": "ответ без цитаты"}),
    )
    .await
    .unwrap();
    let v = view(&f, "sig-1").await;
    assert_eq!(v.state, "closed");
    assert_eq!(v.outcome.as_deref(), Some("delivered"));

    // The track unit reacts to `compacted` by calling push_pending for the
    // batch sig. Write the same event it reads, then call the same recovery
    // function; a closed batch must never be regenerated.
    let ev = json!({"kind": "compacted", "delivery_ids": ["sig-1#1"]});
    std::fs::write(chat_bot::events_path(&f.state), format!("{ev}\n")).unwrap();
    let (events, _) = chat_bot::read_events(&f.state, 0).unwrap();
    assert!(events
        .iter()
        .any(|e| e.get("kind").and_then(|x| x.as_str()) == Some("compacted")));
    let state = f.state.clone();
    let text = chat_bot::push_pending(&f.store, &state, "sig-1").await.unwrap();
    assert!(
        text.is_none(),
        "closed batch was regenerated after compact: {text:?}"
    );
    assert_eq!(calls(&f), vec!["sendMessage".to_string()]);
}


/// The Unknown arm of `finish_send` keeps today's rollout: the actor returns a
/// typed error, the whole write rolls back (sends stays `sending`, no incident,
/// batch not closed), and the caller — never the actor — fires the alarm.
#[tokio::test(flavor = "current_thread")]
async fn unknown_send_rolls_back_and_carries_the_alarm_detail() {
    let f = fixture("unknown-arm").await;
    let parts = vec![PartMeta {
        kind: "text".into(),
        text: "ответ".into(),
        file_id: String::new(),
        file_unique_id: String::new(),
        media_path: String::new(),
    }];
    let begin = chat_bot::store::open_answer_with_sends_of(
        &f.store,
        "sig-1".into(),
        "fp-u".into(),
        -100,
        Some(77),
        None,
        parts,
    )
    .await
    .unwrap();
    let intent = match begin {
        BeginSend::Opened(intents) => intents[0].clone(),
        BeginSend::Replay { .. } => panic!("a fresh answer must be opened"),
    };
    chat_bot::store::start_send_of(&f.store, intent.clone())
        .await
        .unwrap();
    let err = chat_bot::store::finish_send_of(
        &f.store,
        intent,
        SendOutcome::Unknown("boom".into()),
        None,
    )
    .await
    .expect_err("the unknown arm must roll back");
    assert_eq!(
        chat_bot::store::send_unknown_detail(&err).as_deref(),
        Some("send answer:1:part:0: boom")
    );

    let (send_state, incidents, batch_state, answer_state): (String, i64, String, String) = f
        .db
        .call(|con| {
            Ok((
                con.query_row("SELECT state FROM sends LIMIT 1", [], |r| r.get(0))?,
                con.query_row("SELECT COUNT(*) FROM incidents", [], |r| r.get(0))?,
                con.query_row("SELECT state FROM batches WHERE batch_key='sig-1'", [], |r| {
                    r.get(0)
                })?,
                con.query_row("SELECT state FROM answers LIMIT 1", [], |r| r.get(0))?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(send_state, "sending", "a rolled-back send changed state");
    assert_eq!(incidents, 0, "a rolled-back send wrote an incident");
    assert_ne!(batch_state, "closed", "a rolled-back send closed the batch");
    assert_eq!(answer_state, "open", "a rolled-back send closed the answer");
}
