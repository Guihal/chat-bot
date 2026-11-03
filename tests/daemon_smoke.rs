//! Live daemon smoke test: a real `chatd run` process against a fake Telegram
//! API and the real control socket.
//!
//! What it proves (each a property the plan demanded, not "the code compiles"):
//!  1. the daemon starts, listens on its control socket, and `gif_list` answers
//!     with rows seeded in the mirror — the read path still works through the DB
//!     actor;
//!  2. a control call is answered promptly while ANOTHER process holds a write
//!     transaction on the same database and the socket keeps working afterwards;
//!  3. ingest still writes: an update fetched from Telegram ends up in
//!     `messages` with its own message_id/text, a distinct message_id with the
//!     same words is a second row, and a redelivered id stays one row.
//!
//! The daemon shells out to `omo`; the test puts a stub `omo` first in PATH so
//! nothing touches a real agent host.
use chat_bot::*;
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct FakeApi {
    base: String,
    queue: Arc<Mutex<Vec<Value>>>,
}

fn start_fake_api() -> FakeApi {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake api");
    let addr = listener.local_addr().unwrap();
    let queue = Arc::new(Mutex::new(Vec::<Value>::new()));
    let q = queue.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let q = q.clone();
            std::thread::spawn(move || serve(stream, q));
        }
    });
    FakeApi {
        base: format!("http://{addr}"),
        queue,
    }
}

fn serve(mut stream: TcpStream, queue: Arc<Mutex<Vec<Value>>>) {
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
    if path.contains("/file/bot") {
        let bytes = b"smoke-photo-bytes";
        let _ = write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            bytes.len()
        );
        let _ = stream.write_all(bytes);
        return;
    }
    let payload = if path.ends_with("/getMe") {
        json!({"ok": true, "result": {"id": 550001, "is_bot": true, "first_name": "Smoke", "username": "smokebot"}})
    } else if path.ends_with("/getFile") {
        json!({"ok": true, "result": {"file_path": "photos/smoke_photo.jpg"}})
    } else if path.ends_with("/getUpdates") {
        let drained: Vec<Value> = {
            let mut q = queue.lock().unwrap();
            std::mem::take(&mut *q)
        };
        if drained.is_empty() {
            // No busy-spin: an empty long poll costs one short sleep.
            std::thread::sleep(Duration::from_millis(80));
        }
        json!({"ok": true, "result": drained})
    } else {
        // Every Bot API send method reports a message id, so the daemon's
        // receipt transaction has the coordinate it must persist.
        json!({"ok": true, "result": {"message_id": 551001}})
    };
    let body = payload.to_string();
    let _ = write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
}

struct Daemon {
    child: std::process::Child,
    dir: PathBuf,
    state: String,
    sock: PathBuf,
    tool_sock: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn write_stub_omo(dir: &Path) -> std::io::Result<()> {
    let path = dir.join("omo");
    std::fs::write(
        &path,
        "#!/bin/sh\ncase \"$*\" in\n  *list*) echo '{\"sessions\":[]}' ;;\n  *open*) echo '{\"durableSessionId\":\"smoke-session\"}' ;;\n  *) echo '{}' ;;\nesac\nexit 0\n",
    )?;
    let mut perms = std::fs::metadata(&path)?.permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&path, perms)
}

fn seed_mirror(state: &str) {
    std::env::set_var("CHATBOT_CHAT_ID", "-100");
    let con = db(state).unwrap();
    kv_set(&con, "self_id", "550001").unwrap();
    kv_set(&con, "self_username", "smokebot").unwrap();
    con.execute(
        "INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id,file_unique_id,media_kind,media_state,media_size) VALUES(7,4800,1700000000,556,'smoker','Smoke','seeded gif caption',NULL,'','gif_seed_file','gif_seed_unique','animation','ready',4096)",
        [],
    )
    .unwrap();
    // Same shape, different Telegram kind: a video with a file_id must never
    // reach the gif catalog (crystal M1: gifs and videos are not the same).
    con.execute(
        "INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id,file_unique_id,media_kind,media_state,media_size) VALUES(8,4801,1700000100,556,'smoker','Smoke','seeded video caption',NULL,'','video_seed_file','video_seed_unique','video','ready',4096)",
        [],
    )
    .unwrap();
}

fn start_daemon(api_base: &str) -> Daemon {
    // Tests in this file run in parallel: a per-process counter keeps their
    // state dirs distinct (a wall-clock/elapsed stamp collided and the two
    // daemons shared one sqlite file).
    static SMOKE_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "chatd-smoke-{}-{}",
        std::process::id(),
        SMOKE_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let stubs = dir.join("stubs");
    std::fs::create_dir_all(&stubs).unwrap();
    write_stub_omo(&stubs).unwrap();
    let token_path = dir.join("token.env");
    std::fs::write(&token_path, "1:smoke\n").unwrap();
    let mut perms = std::fs::metadata(&token_path).unwrap().permissions();
    perms.set_mode(0o600);
    std::fs::set_permissions(&token_path, perms).unwrap();
    let state = dir.join("state").to_string_lossy().to_string();
    seed_mirror(&state);
    let path_env = format!(
        "{}:{}",
        stubs.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let child = std::process::Command::new(env!("CARGO_BIN_EXE_chatd"))
        .arg("run")
        .env("CHATBOT_STATE_DIR", &state)
        .env("CHATBOT_TOKEN_FILE", &token_path)
        .env("CHATBOT_API_BASE", api_base)
        .env("CHATBOT_CHAT_ID", "-100")
        .env("CHATBOT_BATCH_SEC", "1")
        .env("CHATBOT_SESSION", "smoke")
        .env("CHATBOT_HOST_SOCKET", dir.join("host.sock"))
        .env("CHATBOT_CWD", &dir)
        .env("CHATBOT_OWNER_USERNAME", "nobody-here")
        .env("PATH", path_env)
        .spawn()
        .expect("spawn chatd");
    Daemon {
        child,
        sock: control_sock_path(&state),
        tool_sock: tool_socket_path(&state),
        dir,
        state,
    }
}

/// Round-trip one control call; returns the decoded frame and how long the
/// request/response pair took. Frames the daemon pushes unprompted are skipped.
fn call_control(sock: &Path, op: &str, args: Value) -> (Value, Duration) {
    let started = Instant::now();
    let stream = {
        let mut last_err = None;
        let mut stream = None;
        for _ in 0..40 {
            match UnixStream::connect(sock) {
                Ok(s) => {
                    stream = Some(s);
                    break;
                }
                Err(e) => {
                    last_err = Some(e);
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }
        stream.unwrap_or_else(|| panic!("connect control socket: {last_err:?}"))
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut writer = stream.try_clone().unwrap();
    let mut reader = BufReader::new(stream);
    let frame = json!({"call": {"id": "t1", "op": op, "args": args}});
    writeln!(writer, "{frame}").unwrap();
    writer.flush().unwrap();
    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line).expect("read control reply");
        assert!(n > 0, "control socket closed before a reply arrived");
        let v: Value = serde_json::from_str(line.trim()).expect("control frame is json");
        if let Some(ret) = v.get("return") {
            return (ret.clone(), started.elapsed());
        }
    }
}

fn wait_ready(daemon: &Daemon) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if daemon.sock.exists() && UnixStream::connect(&daemon.sock).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("daemon did not open {} in time", daemon.sock.display());
}

/// One request/response on the daemon's tool socket (the path that replaced
/// every direct Bot API call in tg-chat).
fn call_tool(sock: &Path, op: &str, args: Value) -> Value {
    let stream = {
        let mut stream = None;
        for _ in 0..60 {
            match UnixStream::connect(sock) {
                Ok(s) => {
                    stream = Some(s);
                    break;
                }
                Err(_) => std::thread::sleep(Duration::from_millis(50)),
            }
        }
        stream.unwrap_or_else(|| panic!("connect tool socket {}", sock.display()))
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    let mut writer = stream.try_clone().unwrap();
    writeln!(writer, "{}", json!({"op": op, "args": args})).unwrap();
    writer.flush().unwrap();
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    serde_json::from_str(line.trim()).expect("tool reply is json")
}

fn chatd_log_tail(daemon: &Daemon) -> String {
    let body =
        std::fs::read_to_string(Path::new(&daemon.state).join("chatd.log")).unwrap_or_default();
    let tail: Vec<char> = body.chars().rev().take(2500).collect();
    tail.into_iter().rev().collect()
}

fn open_control_stream(sock: &Path, wait: Duration) -> UnixStream {
    let deadline = Instant::now() + wait;
    loop {
        match UnixStream::connect(sock) {
            Ok(s) => return s,
            Err(e) => {
                if Instant::now() > deadline {
                    panic!("connect control socket: {e}");
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

fn row_count_with_text(daemon: &Daemon, text: &str) -> i64 {
    let con = rusqlite::Connection::open(Path::new(&daemon.state).join("history.db")).unwrap();
    con.query_row("SELECT COUNT(*) FROM messages WHERE text=?", [text], |r| {
        r.get(0)
    })
    .unwrap()
}

fn wait_for_row(daemon: &Daemon, message_id: i64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        let con = rusqlite::Connection::open(Path::new(&daemon.state).join("history.db")).unwrap();
        let found: i64 = con
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE message_id=?",
                [message_id],
                |r| r.get(0),
            )
            .unwrap_or(0);
        if found > 0 {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

#[test]
fn daemon_serves_control_calls_and_ingests() {
    let api = start_fake_api();
    let daemon = start_daemon(&api.base);
    wait_ready(&daemon);

    // 0a. Media is fetched through the daemon's tool socket: the reply points
    //     at real bytes, so tg-chat never needs the token or an HTTP client.
    let reply = call_tool(
        &daemon.tool_sock,
        "get_media",
        json!({"file_id": "smoke_photo"}),
    );
    assert_eq!(reply["ok"], json!(true), "tool reply: {reply}");
    let payload: Value = serde_json::from_str(reply["text"].as_str().unwrap()).unwrap();
    let path = payload["path"].as_str().unwrap();
    let bytes = std::fs::read(path).unwrap();
    assert_eq!(
        bytes, b"smoke-photo-bytes",
        "tool op returned no media bytes"
    );
    assert_eq!(payload["bytes"], json!(17));

    // 0. Facts are published by the daemon itself: poll_ok_at comes from a
    // real successful getUpdates, and health.json carries no verdict.
    let health_path = Path::new(&daemon.state).join("health.json");
    let deadline = Instant::now() + Duration::from_secs(10);
    let health: Value = loop {
        if let Ok(raw) = std::fs::read_to_string(&health_path) {
            if let Ok(v) = serde_json::from_str::<Value>(&raw) {
                break v;
            }
        }
        assert!(Instant::now() < deadline, "health.json was not published");
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(
        health.get("poll_ok_at").and_then(|v| v.as_i64()).is_some(),
        "health.json has no poll_ok_at: {health}"
    );
    assert!(
        health.get("verdict").is_none(),
        "health.json must contain facts, not a verdict: {health}"
    );
    assert!(
        health.get("ingest").is_some(),
        "ingest facts missing: {health}"
    );

    // 1. Seeded rows come back through the control socket / DB actor.
    let (ret, elapsed) = call_control(&daemon.sock, "gif_list", json!({"limit": 10}));
    assert_eq!(
        ret.get("ok").and_then(|v| v.as_bool()),
        Some(true),
        "frame: {ret}"
    );
    let text = ret.get("text").and_then(|v| v.as_str()).unwrap_or("");
    assert!(
        text.contains("gif_seed_file") && text.contains("seeded gif caption"),
        "gif_list returned: {text}"
    );
    assert!(
        !text.contains("video_seed_file") && !text.contains("seeded video caption"),
        "a video leaked into the gif registry: {text}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "gif_list took {elapsed:?}"
    );

    // 2. Another process holds the write lock: the control call must not hang,
    //    and the socket must still work straight after.
    let locker = rusqlite::Connection::open(Path::new(&daemon.state).join("history.db")).unwrap();
    locker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let (ret_locked, locked_elapsed) = call_control(&daemon.sock, "gif_list", json!({"limit": 5}));
    locker.execute_batch("COMMIT").unwrap();
    assert!(
        ret_locked.get("return").is_none() && ret_locked.get("ok").is_some(),
        "frame: {ret_locked}"
    );
    assert!(
        locked_elapsed < Duration::from_secs(2),
        "control call stalled behind a write transaction: {locked_elapsed:?}"
    );
    let (ret_after, _) = call_control(&daemon.sock, "gif_list", json!({"limit": 5}));
    assert_eq!(ret_after.get("ok").and_then(|v| v.as_bool()), Some(true));

    // 3a. Ingest through the DB actor: an update becomes a mirror row.
    let now = chrono::Utc::now().timestamp();
    api.queue.lock().unwrap().push(json!({
        "update_id": 9101,
        "message": {
            "message_id": 4811,
            "date": now,
            "chat": {"id": -100},
            "from": {"id": 557, "username": "smoker", "first_name": "Smoke"},
            "text": "hello smoke"
        }
    }));
    assert!(wait_for_row(&daemon, 4811), "ingested row never appeared");
    assert_eq!(row_count_with_text(&daemon, "hello smoke"), 1);

    // 3b. Identity is (chat_id, message_id) ONLY. A different message_id with
    //     the same author/date/text is a second real Telegram message and must
    //     persist as its own row, not be dropped by a content window.
    api.queue.lock().unwrap().push(json!({
        "update_id": 9201,
        "channel_post": {
            "message_id": 4888,
            "date": now,
            "sender_chat": {"id": -999, "username": "smoker", "title": "Smoke Channel"},
            "text": "hello smoke"
        }
    }));
    assert!(
        wait_for_row(&daemon, 4888),
        "identical-text message with a new message_id was dropped"
    );
    assert_eq!(
        row_count_with_text(&daemon, "hello smoke"),
        2,
        "content-based dedupe still collapses distinct message ids"
    );
    // 3b'. The SAME message_id redelivered under a new update cursor is one
    //      identity, so it must not create a third row.
    api.queue.lock().unwrap().push(json!({
        "update_id": 9401,
        "message": {
            "message_id": 4811,
            "date": now,
            "chat": {"id": -100},
            "from": {"id": 557, "username": "smoker", "first_name": "Smoke"},
            "text": "hello smoke"
        }
    }));
    std::thread::sleep(Duration::from_millis(700));
    assert_eq!(
        row_count_with_text(&daemon, "hello smoke"),
        2,
        "a redelivered message_id minted a second row"
    );

    // 3c. A photo is ingested in the pending state and the fetcher finalizes
    //     it: path, kind and size land in `blobs`, and the row becomes ready.
    api.queue.lock().unwrap().push(json!({
        "update_id": 9500,
        "message": {
            "message_id": 4812,
            "date": now,
            "chat": {"id": -100},
            "from": {"id": 557, "username": "smoker", "first_name": "Smoke"},
            "photo": [{"file_id": "smoke_photo", "file_unique_id": "smoke_photo_u", "file_size": 17}]
        }
    }));
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut ready = false;
    while Instant::now() < deadline {
        let con = rusqlite::Connection::open(Path::new(&daemon.state).join("history.db")).unwrap();
        let state: Option<String> = con
            .query_row(
                "SELECT media_state FROM messages WHERE message_id=4812",
                [],
                |r| r.get(0),
            )
            .ok();
        if state.as_deref() == Some("ready") {
            ready = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let row: Option<(Option<String>, Option<String>, Option<i64>)> =
        rusqlite::Connection::open(Path::new(&daemon.state).join("history.db"))
            .ok()
            .and_then(|c| {
                c.query_row(
                    "SELECT media_state,file_id,media_size FROM messages WHERE message_id=4812",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .ok()
            });
    assert!(
        ready,
        "photo never reached the ready state (row={row:?}): {}",
        chatd_log_tail(&daemon)
    );
    let con = rusqlite::Connection::open(Path::new(&daemon.state).join("history.db")).unwrap();
    let (kind, size, path): (String, i64, String) = con
        .query_row(
            "SELECT b.kind, b.size, b.path FROM messages m JOIN blobs b ON b.id=m.blob_id WHERE m.message_id=4812",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!((kind.as_str(), size), ("photo", 17));
    assert!(
        Path::new(&path).exists(),
        "finalized media file is missing: {path}"
    );
}

/// The control frame is the contract between the daemon and the extension:
/// `batch_ids` is what makes an out-of-batch `reply_to` refusable, and the batch
/// text must show the nearest parent's text (crystal M1: replies are rendered
/// "ответ на такое-то сообщение", one hop only).
#[test]
fn push_frame_carries_batch_ids_and_reply_parent() {
    let api = start_fake_api();
    let daemon = start_daemon(&api.base);
    wait_ready(&daemon);
    // Be the connected extension BEFORE the batch flushes: the daemon writes the
    // push to whoever holds the control socket.
    let stream = open_control_stream(&daemon.sock, Duration::from_secs(10));
    stream
        .set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    // The admit unit waits for the previous run to settle before serving jobs
    // (a real extension emits this on turn_end). Without it the first batch
    // would sit behind a 180 s boot settle; a settled turn is what the harness
    // would have anyway.
    std::fs::write(
        Path::new(&daemon.state).join("events.jsonl"),
        "{\"kind\":\"turn_end\"}\n",
    )
    .unwrap();
    let now = chrono::Utc::now().timestamp();
    {
        let mut q = api.queue.lock().unwrap();
        q.push(json!({
            "update_id": 9301,
            "message": {
                "message_id": 4900,
                "date": now,
                "chat": {"id": -100},
                "from": {"id": 557, "username": "smoker", "first_name": "Smoke"},
                "text": "родительский текст"
            }
        }));
        q.push(json!({
            "update_id": 9302,
            "message": {
                "message_id": 4901,
                "date": now + 1,
                "chat": {"id": -100},
                "from": {"id": 557, "username": "smoker", "first_name": "Smoke"},
                "text": "ответ на него",
                "reply_to_message": {
                    "message_id": 4900,
                    "date": now,
                    "from": {"id": 557, "username": "smoker", "first_name": "Smoke"},
                    "text": "родительский текст"
                }
            }
        }));
    }
    let mut reader = BufReader::new(stream);
    let push = loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => panic!(
                "control socket closed before a push; log tail: {}",
                chatd_log_tail(&daemon)
            ),
            Ok(_) => {
                let v: Value = serde_json::from_str(line.trim()).unwrap_or(Value::Null);
                if let Some(p) = v.get("push") {
                    break p.clone();
                }
            }
            Err(e) => panic!(
                "no push frame within 15s: {e}; log tail: {}",
                chatd_log_tail(&daemon)
            ),
        }
    };
    let ids = push
        .get("batch_ids")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        ids.iter().any(|v| v.as_i64() == Some(4901)),
        "batch_ids missing the incoming id: {push}"
    );
    let text = push.get("text").and_then(|v| v.as_str()).unwrap_or("");
    assert!(
        text.contains("(ответ на [4900] @smoker: «родительский текст»)"),
        "reply parent not rendered one hop deep: {text}"
    );
    assert!(
        text.contains("ответ на него"),
        "incoming text missing: {text}"
    );
}

/// Live RPC path: the extension sends `chat_send` over the control socket, the
/// daemon answers the Bot API and closes the journal batch. This is the one
/// seam unit tests cannot prove: socket frame -> send -> receipt -> mirror.
#[test]
fn chat_send_rpc_closes_the_journal_batch() {
    let api = start_fake_api();
    let daemon = start_daemon(&api.base);
    wait_ready(&daemon);
    let stream = open_control_stream(&daemon.sock, Duration::from_secs(10));
    stream
        .set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    std::fs::write(
        Path::new(&daemon.state).join("events.jsonl"),
        "{\"kind\":\"turn_end\"}\n",
    )
    .unwrap();
    let now = chrono::Utc::now().timestamp();
    api.queue.lock().unwrap().push(json!({
        "update_id": 9311,
        "message": {
            "message_id": 4920,
            "date": now,
            "chat": {"id": -100},
            "from": {"id": 557, "username": "smoker", "first_name": "Smoke"},
            "text": "живой вопрос"
        }
    }));
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let push = loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            panic!("control socket closed; log: {}", chatd_log_tail(&daemon));
        }
        let v: Value = serde_json::from_str(line.trim()).unwrap_or(Value::Null);
        if let Some(p) = v.get("push") {
            break p.clone();
        }
    };
    let delivery = push
        .get("delivery_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let key = delivery.split('#').next().unwrap_or("").to_string();
    writeln!(
        stream.try_clone().unwrap(),
        "{}",
        json!({"result": {"delivery_id": delivery, "kind": "steered"}})
    )
    .unwrap();
    let reply = json!({
        "call": {
            "id": "send-1",
            "op": "chat_send",
            "args": {"batch_key": key, "text": "живой ответ"}
        }
    });
    writeln!(stream.try_clone().unwrap(), "{reply}").unwrap();
    let ret = loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            panic!(
                "control socket closed before return; log: {}",
                chatd_log_tail(&daemon)
            );
        }
        let v: Value = serde_json::from_str(line.trim()).unwrap_or(Value::Null);
        if let Some(r) = v.get("return") {
            break r.clone();
        }
    };
    assert_eq!(ret.get("ok").and_then(|v| v.as_bool()), Some(true), "{ret}");
    let text = ret.get("text").and_then(|v| v.as_str()).unwrap_or("");
    assert!(text.contains("message_id"), "{text}");
    let con = rusqlite::Connection::open(Path::new(&daemon.state).join("history.db")).unwrap();
    let (state, outcome): (String, Option<String>) = con
        .query_row(
            "SELECT state, outcome FROM batches WHERE batch_key=?",
            [&key],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        (state.as_str(), outcome.as_deref()),
        ("closed", Some("delivered"))
    );
    let sent: i64 = con
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE text='живой ответ' AND username='smokebot'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(sent, 1, "mirror row missing after RPC send");
}
