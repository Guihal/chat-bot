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
use std::process::Stdio;
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
    /// The stub host socket: `host_alive()` connects to it, and the daemon asks
    /// the `omo` stub (not this socket) for the session list. Kept alive for the
    /// whole test so the daemon never enters the re-ensure loop.
    _host: std::os::unix::net::UnixListener,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        // Kill before the run ends: a surviving daemon keeps writing its own
        // state and would poison the next run of the same test.
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn write_stub_omo(dir: &Path) -> std::io::Result<()> {
    let path = dir.join("omo");
    // `list` is what decides delivery: the daemon pushes only when the host
    // names the recorded durable id. An empty list (the old stub) sent the
    // daemon into an endless "does not list session" re-ensure loop.
    //
    // Each invocation is logged next to the stub, so a test can prove whether
    // the daemon closed a session. After a close the stub reports a NEW durable
    // id, like a host that really lost the old session.
    std::fs::write(
        &path,
        concat!(
            "#!/bin/sh\n",
            "D=$(dirname \"$0\")\n",
            "echo \"$*\" >> \"$D/omo-calls.log\"\n",
            "GEN=1\n",
            "if [ -f \"$D/omo-closed\" ]; then GEN=2; fi\n",
            "case \"$*\" in\n",
            "  *list*) echo \"{\\\"sessions\\\":[{\\\"name\\\":\\\"smoke\\\",\\\"durableSessionId\\\":\\\"smoke-session-$GEN\\\",\\\"generation\\\":$GEN,\\\"attachments\\\":1}]}\" ;;\n",
            "  *close*) : > \"$D/omo-closed\"; echo '{}' ;;\n",
            "  *open*|*ensure*) echo \"{\\\"durableSessionId\\\":\\\"smoke-session-$GEN\\\"}\" ;;\n",
            "  *) echo '{}' ;;\n",
            "esac\n",
            "exit 0\n",
        ),
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
    start_daemon_with(api_base, &[])
}

/// The stall/provider scenarios need a short no-progress budget, so the test
/// may override the daemon's env.
fn start_daemon_with(api_base: &str, extra: &[(&str, &str)]) -> Daemon {
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
    // A stub host socket: without a listener here `host_alive()` is false and
    // the daemon spends the whole test re-ensuring a host that never appears.
    let host = std::os::unix::net::UnixListener::bind(dir.join("host.sock"))
        .expect("bind stub host socket");
    {
        let host = host.try_clone().expect("clone stub host listener");
        std::thread::spawn(move || {
            for stream in host.incoming() {
                if stream.is_err() {
                    break;
                }
            }
        });
    }
    let path_env = format!(
        "{}:{}",
        stubs.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_chatd"));
    cmd.arg("run")
        // The daemon must never inherit the harness pipes: a child (or an
        // orphaned grandchild such as the `omo` host stub) holding them keeps
        // cargo waiting for EOF long after the test itself finished. The raw
        // log lives in <state>/chatd.log and the test tail-reads that file.
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .env("CHATBOT_STATE_DIR", &state)
        .env("CHATBOT_TOKEN_FILE", &token_path)
        .env("CHATBOT_API_BASE", api_base)
        .env("CHATBOT_CHAT_ID", "-100")
        .env("CHATBOT_BATCH_SEC", "1")
        .env("CHATBOT_SESSION", "smoke")
        .env("CHATBOT_HOST_SOCKET", dir.join("host.sock"))
        .env("CHATBOT_CWD", &dir)
        .env("CHATBOT_OWNER_USERNAME", "nobody-here")
        .env("PATH", path_env);
    for (k, v) in extra {
        cmd.env(k, v);
    }
    let child = cmd.spawn().expect("spawn chatd");
    Daemon {
        child,
        sock: control_sock_path(&state),
        tool_sock: tool_socket_path(&state),
        dir,
        state,
        _host: host,
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

// ---------- helpers for the history-window / recovery scenarios ----------

/// Append one event to the daemon's journal - the extension's real write path.
fn append_event(daemon: &Daemon, ev: Value) {
    let path = Path::new(&daemon.state).join("events.jsonl");
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(f, "{ev}").unwrap();
    f.flush().unwrap();
}

/// Every argument list the daemon handed to the omo stub.
fn omo_calls(daemon: &Daemon) -> String {
    std::fs::read_to_string(daemon.dir.join("stubs").join("omo-calls.log")).unwrap_or_default()
}

fn live_session_uid(daemon: &Daemon) -> Option<String> {
    let con = rusqlite::Connection::open(Path::new(&daemon.state).join("history.db")).ok()?;
    con.query_row(
        "SELECT session_uid FROM sessions WHERE state='live' ORDER BY generation DESC, id DESC LIMIT 1",
        [],
        |r| r.get(0),
    )
    .ok()
}

fn queue_text(api: &FakeApi, id: i64, text: &str) {
    let now = chrono::Utc::now().timestamp();
    api.queue.lock().unwrap().push(json!({
        "update_id": 900_000 + id,
        "message": {
            "message_id": id,
            "date": now,
            "chat": {"id": -100},
            "from": {"id": 557, "username": "smoker", "first_name": "Smoke"},
            "text": text
        }
    }));
}

fn chatd_log(daemon: &Daemon) -> String {
    std::fs::read_to_string(Path::new(&daemon.state).join("chatd.log")).unwrap_or_default()
}

/// Next pushed batch frame, or None within the budget. The reader's socket
/// timeout decides the polling granularity.
fn try_read_push(reader: &mut BufReader<UnixStream>, secs: u64) -> Option<Value> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => panic!("control socket closed before a push"),
            Ok(_) => {
                let v: Value = serde_json::from_str(line.trim()).unwrap_or(Value::Null);
                if let Some(p) = v.get("push") {
                    return Some(p.clone());
                }
            }
            Err(_) => {
                if Instant::now() >= deadline {
                    return None;
                }
            }
        }
    }
}

/// Wait for the next pushed batch frame on an open control stream.
fn read_push(reader: &mut BufReader<UnixStream>, daemon: &Daemon, secs: u64) -> Value {
    try_read_push(reader, secs).unwrap_or_else(|| {
        panic!(
            "no push within {secs}s; log tail: {}",
            chatd_log_tail(daemon)
        )
    })
}

/// Admit the frame the way the extension does, so the daemon stops retrying.
fn accept_push(stream: &UnixStream, push: &Value) {
    let delivery = push
        .get("delivery_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    writeln!(
        stream.try_clone().unwrap(),
        "{}",
        json!({"result": {"delivery_id": delivery, "kind": "steered"}})
    )
    .unwrap();
}

fn push_text(push: &Value) -> String {
    push.get("text")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

fn wait_until(daemon: &Daemon, what: &str, secs: u64, f: impl Fn(&Daemon) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        if f(daemon) {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "{what} never happened; omo calls: {}; log: {}",
        omo_calls(daemon),
        chatd_log(daemon)
    );
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

/// The control frame is the contract between the daemon and the extension: it
/// carries the delivery id and the batch ids (bookkeeping, NOT a reply
/// restriction - replies are unrestricted since 2026-11-08), and the batch text
/// must show the nearest parent's text (crystal M1: replies are rendered
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

/// Owner's contract: the last 50 messages reach the model only when it gets a
/// new context (session start / new durable id) or right after a compaction.
/// Every other turn carries the batch, the guard and the catalogue only.
#[test]
fn history_window_only_at_session_start_and_after_compact() {
    let api = start_fake_api();
    let daemon = start_daemon(&api.base);
    wait_ready(&daemon);
    let stream = open_control_stream(&daemon.sock, Duration::from_secs(10));
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    std::fs::write(
        Path::new(&daemon.state).join("events.jsonl"),
        "{\"kind\":\"turn_end\"}\n",
    )
    .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let now = chrono::Utc::now().timestamp();

    // Turn 1: the session is new, so the seeded history rides along.
    queue_text(&api, 5001, "turn one question");
    let p1 = read_push(&mut reader, &daemon, 30);
    let t1 = push_text(&p1);
    assert!(t1.contains("[5001] @smoker: turn one question"), "batch missing: {t1}");
    assert!(
        t1.contains("[4800] @smoker: seeded gif caption"),
        "no history window on the first turn of a session: {t1}"
    );
    accept_push(&stream, &p1);
    append_event(
        &daemon,
        json!({"kind": "delivery_state", "delivery_id": p1["delivery_id"], "state": "emitted", "t": now}),
    );

    // Turn 2: same session, no compaction - the batch only.
    queue_text(&api, 5002, "turn two question");
    let p2 = read_push(&mut reader, &daemon, 30);
    let t2 = push_text(&p2);
    assert!(t2.contains("[5002] @smoker: turn two question"), "batch missing: {t2}");
    assert!(
        !t2.contains("[4800] @smoker:"),
        "history window repeated in an ordinary turn: {t2}"
    );
    assert!(
        !t2.contains("[5001] @smoker:"),
        "the previous turn was repeated as history: {t2}"
    );
    accept_push(&stream, &p2);
    append_event(
        &daemon,
        json!({"kind": "delivery_state", "delivery_id": p2["delivery_id"], "state": "emitted", "t": now}),
    );

    // A compaction rewrites the transcript: the next turn carries the window.
    append_event(&daemon, json!({"kind": "context_compacted"}));
    std::thread::sleep(Duration::from_millis(600));
    queue_text(&api, 5003, "turn three question");
    let p3 = read_push(&mut reader, &daemon, 30);
    let t3 = push_text(&p3);
    assert!(t3.contains("[5003] @smoker: turn three question"), "{t3}");
    assert!(
        t3.contains("[4800] @smoker:") && t3.contains("[5001] @smoker:"),
        "no history window on the turn after a compaction: {t3}"
    );
    accept_push(&stream, &p3);
    append_event(
        &daemon,
        json!({"kind": "delivery_state", "delivery_id": p3["delivery_id"], "state": "emitted", "t": now}),
    );
    wait_until(&daemon, "the post-compact window to be retired", 10, |d| {
        chatd_log(d).contains("history window delivered")
    });

    // Served once, not on every turn after the compact.
    queue_text(&api, 5004, "turn four question");
    let p4 = read_push(&mut reader, &daemon, 30);
    let t4 = push_text(&p4);
    assert!(t4.contains("[5004] @smoker: turn four question"), "{t4}");
    assert!(
        !t4.contains("[4800] @smoker:") && !t4.contains("[5003] @smoker:"),
        "the post-compact window was never retired: {t4}"
    );
    accept_push(&stream, &p4);
    assert_eq!(
        live_session_uid(&daemon).as_deref(),
        Some("smoke-session-1"),
        "the turns did not stay in one session"
    );
}

/// A frame that never reached the model keeps its window on the retry, even
/// when the rebuilt document hashes differently (a new row, a new answered
/// marker, a hydrated file). The window is recorded as served only by the
/// delivery proof, and the batch is identified by its ledger key, not by the
/// hash of the text being rebuilt.
#[test]
fn retry_keeps_the_window_when_the_document_changed() {
    let api = start_fake_api();
    let daemon = start_daemon(&api.base);
    wait_ready(&daemon);
    let stream = open_control_stream(&daemon.sock, Duration::from_secs(10));
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    std::fs::write(
        Path::new(&daemon.state).join("events.jsonl"),
        "{\"kind\":\"turn_end\"}\n",
    )
    .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let now = chrono::Utc::now().timestamp();

    queue_text(&api, 5400, "attempt one question");
    let p1 = read_push(&mut reader, &daemon, 30);
    let t1 = push_text(&p1);
    assert!(
        t1.contains("[4800] @smoker: seeded gif caption"),
        "the first turn of the session had no history window: {t1}"
    );
    accept_push(&stream, &p1);
    // No `emitted` follows: the frame never reached the model, so the window was
    // not served and the retry still owes it.

    // The document changes between the build and the retry: a new row joins the
    // history window, so `batch_sig(text)` moves.
    {
        let con = rusqlite::Connection::open(Path::new(&daemon.state).join("history.db")).unwrap();
        con.execute(
            "INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id) VALUES(11,5401,?1,556,'smoker','Smoke','arrived meanwhile',NULL,'','')",
            [now + 1],
        )
        .unwrap();
    }

    append_event(
        &daemon,
        json!({"kind": "delivery_state", "delivery_id": p1["delivery_id"], "state": "failed", "error": "session file refused", "t": now}),
    );
    let p2 = read_push(&mut reader, &daemon, 30);
    let t2 = push_text(&p2);
    assert!(t2.contains("attempt one question"), "{t2}");
    assert!(
        t2.contains("[5401] @smoker: arrived meanwhile"),
        "the document did not change between the attempts: {t2}"
    );
    assert!(
        t2.contains("[4800] @smoker: seeded gif caption"),
        "the retry dropped the history window: {t2}"
    );
    assert_eq!(
        p1["delivery_id"].as_str().unwrap().split('#').next(),
        p2["delivery_id"].as_str().unwrap().split('#').next(),
        "the retry became a different batch"
    );
    accept_push(&stream, &p2);
}

/// One window per epoch, even when the post-compaction window rides a REBUILD of
/// a batch that was originally built without one: the flag is cleared by the
/// proof of exactly that frame, so the following turn carries the batch only.
#[test]
fn single_window_despite_pending_redelivery() {
    let api = start_fake_api();
    let daemon = start_daemon(&api.base);
    wait_ready(&daemon);
    let stream = open_control_stream(&daemon.sock, Duration::from_secs(10));
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    std::fs::write(
        Path::new(&daemon.state).join("events.jsonl"),
        "{\"kind\":\"turn_end\"}\n",
    )
    .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let now = chrono::Utc::now().timestamp();

    // Turn 1: session start - the window is served and proven.
    queue_text(&api, 5500, "opening question");
    let p1 = read_push(&mut reader, &daemon, 30);
    assert!(
        push_text(&p1).contains("[4800] @smoker: seeded gif caption"),
        "no window at session start: {}",
        push_text(&p1)
    );
    accept_push(&stream, &p1);
    append_event(
        &daemon,
        json!({"kind": "delivery_state", "delivery_id": p1["delivery_id"], "state": "emitted", "t": now}),
    );
    wait_until(&daemon, "the opening window to be marked delivered", 10, |d| {
        chatd_log(d).contains("history window delivered")
    });

    // Turn 2: same session, no window. The frame hangs without a proof.
    queue_text(&api, 5501, "unproven question");
    let p2 = read_push(&mut reader, &daemon, 30);
    let t2 = push_text(&p2);
    assert!(t2.contains("unproven question"), "{t2}");
    assert!(
        !t2.contains("[4800] @smoker:"),
        "an ordinary turn repeated the history window: {t2}"
    );
    accept_push(&stream, &p2);

    // A compaction rewrites the transcript, then the hanging batch is re-delivered:
    // its rebuild carries the window.
    append_event(&daemon, json!({"kind": "context_compacted"}));
    std::thread::sleep(Duration::from_millis(600));
    let sig2 = p2["delivery_id"]
        .as_str()
        .unwrap()
        .split('#')
        .next()
        .unwrap()
        .to_string();
    append_event(
        &daemon,
        json!({"kind": "compacted", "delivery_ids": [sig2]}),
    );
    let p3 = read_push(&mut reader, &daemon, 30);
    let t3 = push_text(&p3);
    assert!(t3.contains("unproven question"), "{t3}");
    assert!(
        t3.contains("[4800] @smoker: seeded gif caption"),
        "the post-compaction rebuild carried no window: {t3}"
    );
    assert_eq!(
        p2["delivery_id"].as_str().unwrap().split('#').next(),
        p3["delivery_id"].as_str().unwrap().split('#').next(),
        "the redelivery became a different batch"
    );
    accept_push(&stream, &p3);
    append_event(
        &daemon,
        json!({"kind": "delivery_state", "delivery_id": p3["delivery_id"], "state": "emitted", "t": now}),
    );
    wait_until(&daemon, "the post-compact window to be marked delivered", 10, |d| {
        chatd_log(d).matches("history window delivered").count() >= 2
    });

    // The next turn must not repeat it.
    queue_text(&api, 5502, "after the compact");
    let p4 = read_push(&mut reader, &daemon, 30);
    let t4 = push_text(&p4);
    assert!(t4.contains("after the compact"), "{t4}");
    assert!(
        !t4.contains("[4800] @smoker:") && !t4.contains("unproven question"),
        "the post-compact window was not retired: {t4}"
    );
    accept_push(&stream, &p4);
    assert_eq!(live_session_uid(&daemon).as_deref(), Some("smoke-session-1"));
}

/// A stalled batch is not the session's fault: the re-delivery goes into the
/// SAME session and nothing is closed.
#[test]
fn stalled_batch_recovers_in_the_same_session() {
    let api = start_fake_api();
    let daemon = start_daemon_with(
        &api.base,
        &[("CHATBOT_TURN_SEC", "1"), ("CHATBOT_TURN_EXTRA_SEC", "0")],
    );
    wait_ready(&daemon);
    let stream = open_control_stream(&daemon.sock, Duration::from_secs(10));
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    std::fs::write(
        Path::new(&daemon.state).join("events.jsonl"),
        "{\"kind\":\"turn_end\"}\n",
    )
    .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());

    queue_text(&api, 5100, "stalled question");
    let p1 = read_push(&mut reader, &daemon, 30);
    accept_push(&stream, &p1);
    // No `emitted` follows: after the 1s budget the batch is stalled.
    wait_until(&daemon, "a same-session stall recovery", 25, |d| {
        chatd_log(d).contains("re-delivery in the same session")
    });
    let p2 = read_push(&mut reader, &daemon, 30);
    accept_push(&stream, &p2);
    assert_eq!(
        p1["delivery_id"].as_str().unwrap().split('#').next(),
        p2["delivery_id"].as_str().unwrap().split('#').next(),
        "the re-delivery was a different batch"
    );
    let log = chatd_log(&daemon);
    assert!(log.contains("stalled in phase"), "no stall observed: {log}");
    assert!(
        !log.contains("recreate session"),
        "a stalled batch replaced the session: {log}"
    );
    assert!(
        !omo_calls(&daemon).contains("close"),
        "a session was closed on a stall: {}",
        omo_calls(&daemon)
    );
    assert_eq!(live_session_uid(&daemon).as_deref(), Some("smoke-session-1"));
}

/// A terminal provider error is a pause, not a memory wipe: the batch is
/// re-delivered into the same session.
#[test]
fn provider_error_keeps_the_same_session() {
    let api = start_fake_api();
    let daemon = start_daemon(&api.base);
    wait_ready(&daemon);
    let stream = open_control_stream(&daemon.sock, Duration::from_secs(10));
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    std::fs::write(
        Path::new(&daemon.state).join("events.jsonl"),
        "{\"kind\":\"turn_end\"}\n",
    )
    .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let now = chrono::Utc::now().timestamp();

    queue_text(&api, 5200, "quota problem question");
    let p1 = read_push(&mut reader, &daemon, 30);
    accept_push(&stream, &p1);
    append_event(
        &daemon,
        json!({"kind": "run_failed", "error": "GoUsageLimitError: usage limit reached", "t": now}),
    );
    append_event(&daemon, json!({"kind": "agent_settled", "t": now}));
    wait_until(&daemon, "a same-session retry", 25, |d| {
        chatd_log(d).contains("re-delivery in the same session")
    });
    let p2 = read_push(&mut reader, &daemon, 30);
    accept_push(&stream, &p2);
    let log = chatd_log(&daemon);
    assert!(
        log.contains("run failed (provider error): GoUsageLimitError"),
        "the cause is missing from the log: {log}"
    );
    assert!(
        !log.contains("recreate session"),
        "a provider error replaced the session: {log}"
    );
    assert!(
        !omo_calls(&daemon).contains("close"),
        "a session was closed on a provider error: {}",
        omo_calls(&daemon)
    );
    assert_eq!(live_session_uid(&daemon).as_deref(), Some("smoke-session-1"));
}

/// The one legitimate replacement: the gateway rejected media in the
/// transcript. The reason is named, the session is closed once, and the first
/// frame of the new session carries the history window (the text is rebuilt
/// AFTER the replacement, never before it).
#[test]
fn media_heal_recreates_with_history_in_the_new_session() {
    let api = start_fake_api();
    let daemon = start_daemon(&api.base);
    wait_ready(&daemon);
    let stream = open_control_stream(&daemon.sock, Duration::from_secs(10));
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    std::fs::write(
        Path::new(&daemon.state).join("events.jsonl"),
        "{\"kind\":\"turn_end\"}\n",
    )
    .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let now = chrono::Utc::now().timestamp();

    queue_text(&api, 5300, "poisoned media question");
    let p1 = read_push(&mut reader, &daemon, 30);
    accept_push(&stream, &p1);
    append_event(
        &daemon,
        json!({"kind": "run_failed", "media_rejected": true, "error": "400 rejected media", "t": now}),
    );
    append_event(&daemon, json!({"kind": "agent_settled", "t": now}));
    wait_until(&daemon, "the poisoned session to be closed", 25, |d| {
        omo_calls(d).contains("close")
    });
    // The heal waits for a turn boundary before delivering into the new
    // session; feed one until the frame arrives.
    let mut p2 = None;
    for _ in 0..30 {
        append_event(&daemon, json!({"kind": "turn_end"}));
        if let Some(p) = try_read_push(&mut reader, 1) {
            p2 = Some(p);
            break;
        }
    }
    let p2 = p2.expect("the healed session never received the batch");
    let t2 = push_text(&p2);
    accept_push(&stream, &p2);
    assert!(
        t2.contains("[4800] @smoker: seeded gif caption"),
        "the healed session started without its history: {t2}"
    );
    let log = chatd_log(&daemon);
    assert!(
        log.contains("recreate session smoke-session-1") && log.contains("gateway-rejected media"),
        "the replacement reason is not named: {log}"
    );
    assert_eq!(
        omo_calls(&daemon).matches("close").count(),
        1,
        "the session was replaced more than once: {}",
        omo_calls(&daemon)
    );
    assert_eq!(
        live_session_uid(&daemon).as_deref(),
        Some("smoke-session-2"),
        "the heal did not land in a fresh session"
    );
}

/// The owner's contract (2026-11-07): the indicator follows "the agent is
/// working", not the model's steps, and it must not wait for the runtime's
/// first turn - on the live host admission sat 12.9 s before the first
/// `agent_activity`, so every reply shorter than that never showed "typing".
/// Here NO agent event is ever written: if the light comes on, the daemon lit
/// it at admission, which is the whole point.
#[test]
fn the_indicator_lights_at_admit_without_any_agent_event() {
    let api = start_fake_api();
    let daemon = start_daemon(&api.base);
    wait_ready(&daemon);

    let stream = open_control_stream(&daemon.sock, Duration::from_secs(10));
    let mut reader = BufReader::new(stream.try_clone().unwrap());

    queue_text(&api, 908_811, "свет включается по приёму батча");
    let push = read_push(&mut reader, &daemon, 20);
    accept_push(&stream, &push);

    wait_until(&daemon, "typing on right after admission", 10, |d| {
        chatd_log(d).contains("typing on")
    });
    let log = chatd_log(&daemon);
    assert!(
        log.contains("admitted"),
        "the indicator lit without an admission to lit it: {log}"
    );
    assert!(
        !std::fs::read_to_string(Path::new(&daemon.state).join("events.jsonl"))
            .unwrap_or_default()
            .contains("agent_activity"),
        "the test wrote a runtime event; the light must come from admission"
    );
}
