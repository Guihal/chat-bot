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
//!     `messages` with its own message_id/text, and a second copy inside the
//!     120-second window is dropped by the dedup contract instead of doubling.
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
    let path = request_line.split_whitespace().nth(1).unwrap_or("/").to_string();
    let payload = if path.ends_with("/getMe") {
        json!({"ok": true, "result": {"id": 550001, "is_bot": true, "first_name": "Smoke", "username": "smokebot"}})
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
        json!({"ok": true, "result": {}})
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
    let con = db(state).unwrap();
    kv_set(&con, "self_id", "550001").unwrap();
    kv_set(&con, "self_username", "smokebot").unwrap();
    con.execute(
        "INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id,file_unique_id) VALUES(7,4800,1700000000,556,'smoker','Smoke','seeded gif caption',NULL,'','gif_seed_file','gif_seed_unique')",
        [],
    )
    .unwrap();
}

fn start_daemon(api_base: &str) -> Daemon {
    let dir = std::env::temp_dir().join(format!(
        "chatd-smoke-{}-{}",
        std::process::id(),
        Instant::now().elapsed().as_nanos()
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
    stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
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

fn row_count_with_text(daemon: &Daemon, text: &str) -> i64 {
    let con = rusqlite::Connection::open(Path::new(&daemon.state).join("history.db")).unwrap();
    con.query_row(
        "SELECT COUNT(*) FROM messages WHERE text=?",
        [text],
        |r| r.get(0),
    )
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

    // 1. Seeded rows come back through the control socket / DB actor.
    let (ret, elapsed) = call_control(&daemon.sock, "gif_list", json!({"limit": 10}));
    assert_eq!(ret.get("ok").and_then(|v| v.as_bool()), Some(true), "frame: {ret}");
    let text = ret.get("text").and_then(|v| v.as_str()).unwrap_or("");
    assert!(
        text.contains("gif_seed_file") && text.contains("seeded gif caption"),
        "gif_list returned: {text}"
    );
    assert!(elapsed < Duration::from_secs(5), "gif_list took {elapsed:?}");

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

    // 3b. The same text inside the 120-second window, from the cross-chat path,
    //     is dropped by the dedup contract instead of being stored twice.
    api.queue.lock().unwrap().push(json!({
        "update_id": 9201,
        "channel_post": {
            "message_id": 4888,
            "date": now,
            "sender_chat": {"id": -999, "username": "smoker", "title": "Smoke Channel"},
            "text": "hello smoke"
        }
    }));
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let ingested: i64 = rusqlite::Connection::open(Path::new(&daemon.state).join("history.db"))
            .and_then(|c| c.query_row("SELECT COUNT(*) FROM messages WHERE message_id=4888", [], |r| r.get(0)))
            .unwrap_or(0);
        if ingested == 0 && row_count_with_text(&daemon, "hello smoke") == 1 {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        row_count_with_text(&daemon, "hello smoke"),
        1,
        "120-second dedup window no longer collapses the duplicate"
    );
}
