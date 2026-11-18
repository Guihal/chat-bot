//! Local stand with a REAL model: Telegram is replaced, the model is not.
//!
//! Why this test exists (owner's rule «сначала локальный стенд, потом деплой»):
//! a green `cargo test` and "the extension loaded" say nothing about whether a
//! change still lets a real model answer, let alone see a photo or a video
//! frame. This stand reproduces the battle path end to end on one machine:
//!
//! ```text
//! fake Bot API ──► real chatd ──► real omo host (real provider) ──► chat_send
//!      ▲                                   │                          │
//!      └────────── answer goes to the log ◄┘──────────────────────────┘
//! ```
//!
//! What it proves (each item is a property, not "it started"):
//!  1. the text turn reaches a REAL model through the real host, and the answer
//!     comes back through the `chat_send` tool into the fake Bot API log (the
//!     tool is not stubbed and nothing is sent to Telegram);
//!  2. the photo turn makes the model `read` the downloaded blob and name the
//!     content (four objects) — the message text says neither the number nor
//!     the shape, so the answer cannot be invented from the prompt;
//!  3. the video turn makes the model look at the FRAME (Telegram `thumbnail`),
//!     because this wire has no video content part, and name its content (three
//!     objects);
//!  4. one durable session serves all three turns: the history window rides the
//!     first turn only, the next two say `history window omitted (same session
//!     …)` with the SAME `uid:generation` — the contract a later "window" change
//!     could otherwise break unnoticed;
//!  5. a message older than the age budget is sealed and closed as
//!     `skip_by_time` at admit: no push, no model answer, one ledger row —
//!     the guard proves itself without spending model quota on the stale turn;
//!  6. every path of the stand is RELATIVE: `chatd` runs inside
//!     `target/stand/run-<pid>-<n>/` and every variable it receives
//!     (`CHATBOT_STATE_DIR`, `CHATBOT_AGENT_DIR`, `CHATBOT_CWD`,
//!     `CHATBOT_HOST_SOCKET`, `CHATBOT_TOKEN_FILE`, `CHATBOT_SYSTEM`) is a path
//!     relative to that run root. Moving the checkout to another machine needs
//!     no edit here.
//!
//! It is `#[ignore]`: a real model spends a real subscription quota, so the
//! routine `cargo test` must stay offline and fast (crystal H5). Missing
//! prerequisites fail LOUDLY (a panic listing every gap), never a silent `ok`.
//!
//! Run:
//! ```text
//! cargo test --test stand_real_model -- --ignored --nocapture
//! ```
//! Prerequisites (also checked, with a panic listing what is missing):
//!  * `omo` in `PATH` — the real host this daemon spawns;
//!  * an agent profile with `auth.json` + `models.json` that carry a
//!    vision-capable model (`minimax/MiniMax-M3.1-Flash` by default). Taken from
//!    `$CHATBOT_STAND_CREDS` or `$HOME/.omo/agent`, copied into the stand
//!    profile — the live bot profile and the server key are never touched;
//!  * the two assets in `tests/stand_assets/` (a photo with four squares and a
//!    video frame with three circles).
use chat_bot::media::PHOTO_CONTEXT_LIMIT;
use chat_bot::{control_sock_path, db};
use rusqlite::Connection;
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// ---- the fake chat -------------------------------------------------------
//
// Digits and shape words stay OUT of every incoming text and out of the file
// names the daemon sees: an answer that names "четыре" / "три" can only come
// from reading the pixels.

const CHAT_ID: i64 = -100;
const BOT_ID: i64 = 971_313;
const OWNER_ID: i64 = 774_517;
const BOT_USERNAME: &str = "stand_bot";
const OWNER_USERNAME: &str = "stand_owner";
const PHOTO_FILE_ID: &str = "photo_atom";
const FRAME_FILE_ID: &str = "frame_atom";
const CHANNEL_ID: i64 = -1004421941006;
/// Fake Bot API file_path values: neutral names, no number, no shape.
const PHOTO_REMOTE: &str = "media/a.png";
const FRAME_REMOTE: &str = "media/b.jpg";

/// The agent profile the stand runs with. Chosen because it advertises
/// `input: [text, image]` — a text-only model cannot prove property 2/3.
const STAND_PROVIDER: &str = "minimax";
const STAND_MODEL: &str = "MiniMax-M3.1-Flash";

/// Provider/model under test. Env overrides let the stand run the PRODUCTION
/// model (`CHATBOT_STAND_PROVIDER=opencode-go
/// CHATBOT_STAND_MODEL=deepseek-v4.1-flash`) instead of the historical default:
/// a stand that only ever runs another model proves nothing about the live one.
fn stand_provider() -> String {
    std::env::var("CHATBOT_STAND_PROVIDER").unwrap_or_else(|_| STAND_PROVIDER.to_string())
}

fn stand_model() -> String {
    std::env::var("CHATBOT_STAND_MODEL").unwrap_or_else(|_| STAND_MODEL.to_string())
}

/// One turn's budget. Counted from the moment the update is queued, which
/// happens after the admit unit announced readiness, so it measures the MODEL,
/// not the daemon boot.
const TURN_BUDGET: Duration = Duration::from_secs(300);
/// Host spawn + session open (measured: ~10 s on this machine).
const STARTUP_BUDGET: Duration = Duration::from_secs(360);

struct ApiState {
    queue: Mutex<Vec<Value>>,
    log: Mutex<std::fs::File>,
    next_message_id: AtomicI64,
    photo: Vec<u8>,
    frame: Vec<u8>,
}

/// Fake Telegram: only what the battle path touches. No external HTTP call is
/// ever made; every `send*` answer is written to `api-log.jsonl` — that file IS
/// the "отписывание в лог" the owner asked for.
fn start_api(log_path: &Path, photo: Vec<u8>, frame: Vec<u8>) -> (String, Arc<ApiState>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake Bot API");
    let addr = listener.local_addr().expect("fake API addr");
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .expect("open api log");
    let state = Arc::new(ApiState {
        queue: Mutex::new(vec![]),
        log: Mutex::new(log),
        next_message_id: AtomicI64::new(562_301),
        photo,
        frame,
    });
    let st = state.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let st = st.clone();
            std::thread::spawn(move || serve_api(stream, st));
        }
    });
    (format!("http://{addr}"), state)
}

fn respond(stream: &mut TcpStream, content_type: &str, body: &[u8]) {
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body);
}

fn serve_api(mut stream: TcpStream, st: Arc<ApiState>) {
    let Ok(clone) = stream.try_clone() else {
        return;
    };
    let mut reader = BufReader::new(clone);
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).unwrap_or(0) == 0 {
        return;
    }
    let mut content_len = 0usize;
    let mut content_type = String::new();
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).unwrap_or(0) == 0 {
            break;
        }
        if header.trim().is_empty() {
            break;
        }
        let lower = header.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("content-length:") {
            content_len = v.trim().parse().unwrap_or(0);
        } else if lower.starts_with("content-type:") {
            content_type = lower.trim().to_string();
        }
    }
    let mut body = vec![0u8; content_len];
    let _ = reader.read_exact(&mut body);
    let path = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .to_string();

    // Blob download: `{base}/file/bot<token>/<file_path>` (a GET).
    if path.starts_with("/file/bot") {
        // Real Telegram downloads are not instantaneous, and the daemon's
        // `media_settle` waits for the row to finalize before building the
        // batch. An instant answer reversed that order: the document was built
        // for a still-pending row and reached the model as a bare `[photo]`
        // without the `[file]` pointer (observed live 2026-11-06). The delay
        // keeps the battle ordering pending -> ready -> [file].
        std::thread::sleep(Duration::from_millis(1500));
        if path.ends_with("a.png") {
            respond(&mut stream, "image/png", &st.photo);
        } else {
            respond(&mut stream, "image/jpeg", &st.frame);
        }
        return;
    }

    let method = path.rsplit('/').next().unwrap_or("").to_string();
    let payload = if content_type.contains("application/json") {
        serde_json::from_slice::<Value>(&body).ok()
    } else {
        None
    };

    let result = match method.as_str() {
        "getMe" => json!({
            "id": BOT_ID,
            "is_bot": true,
            "first_name": "Stand",
            "username": BOT_USERNAME
        }),
        "getUpdates" => {
            let drained: Vec<Value> = {
                let mut q = st.queue.lock().unwrap();
                std::mem::take(&mut *q)
            };
            if drained.is_empty() {
                // No busy spin: an empty long poll costs one short sleep.
                std::thread::sleep(Duration::from_millis(120));
            }
            json!(drained)
        }
        "getFile" => {
            let id = payload
                .as_ref()
                .and_then(|p| p.get("file_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let remote = if id == FRAME_FILE_ID {
                FRAME_REMOTE
            } else {
                PHOTO_REMOTE
            };
            json!({"file_id": id, "file_path": remote})
        }
        _ => {
            // Every Bot API send method reports a message id, so the daemon's
            // receipt transaction has the coordinate it must persist. The send
            // itself is the log line below.
            let text = payload
                .as_ref()
                .and_then(|p| p.get("text"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let caption = payload
                .as_ref()
                .and_then(|p| p.get("caption"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let reply_to = payload
                .as_ref()
                .and_then(|p| p.get("reply_to_message_id"))
                .cloned();
            let message_id = st.next_message_id.fetch_add(1, Ordering::Relaxed);
            let line = json!({
                "method": method,
                "text": text,
                "caption": caption,
                "reply_to_message_id": reply_to,
                "message_id": message_id,
            });
            if let Ok(mut f) = st.log.lock() {
                let _ = writeln!(f, "{line}");
                let _ = f.flush();
            }
            json!({"message_id": message_id})
        }
    };
    let body = json!({"ok": true, "result": result}).to_string();
    respond(&mut stream, "application/json", body.as_bytes());
}

// ---- the run root --------------------------------------------------------

/// Preconditions are checked before any file is created, and every gap is
/// listed at once: a stand that quietly degrades to "daemon started" would be
/// exactly the fake green check this task exists to prevent.
fn check_prerequisites(creds_dir: &Path) -> Result<(), Vec<String>> {
    let mut gaps = vec![];
    match Command::new("omo").arg("--version").output() {
        Ok(out) if out.status.success() => {}
        Ok(_) => gaps.push("`omo --version` failed: the real host cannot be spawned".to_string()),
        Err(e) => gaps.push(format!("`omo` is not in PATH ({e})")),
    }
    let auth = creds_dir.join("auth.json");
    let models = creds_dir.join("models.json");
    if !auth.is_file() {
        gaps.push(format!("missing {}", auth.display()));
    }
    if !models.is_file() {
        gaps.push(format!("missing {}", models.display()));
    }
    if auth.is_file() {
        match std::fs::read(&auth)
            .ok()
            .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
        {
            Some(v) => {
                let provider = stand_provider();
                let key = v
                    .get(&provider)
                    .and_then(|p| p.get("key"))
                    .and_then(|k| k.as_str())
                    .unwrap_or("");
                if key.is_empty() {
                    gaps.push(format!("auth.json has no non-empty `{provider}.key`"));
                }
            }
            None => gaps.push(format!("cannot parse {}", auth.display())),
        }
    }
    if models.is_file() {
        let (provider, model) = (stand_provider(), stand_model());
        let vision = std::fs::read(&models)
            .ok()
            .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
            .and_then(|v| {
                let accepts_image = |m: &Value| {
                    m.get("input")
                        .and_then(|i| i.as_array())
                        .map(|input| input.iter().any(|x| x.as_str() == Some("image")))
                        .unwrap_or(false)
                };
                let p = v.get("providers").and_then(|p| p.get(&provider))?;
                // A model reaches the runtime either as a full `models[]` entry or
                // as a `modelOverrides` row over a builtin one; the override shape
                // is what carries the vision capability for a provider whose list
                // is not spelled out in the profile.
                let in_models = p
                    .get("models")
                    .and_then(|m| m.as_array())
                    .map(|models| {
                        models.iter().any(|m| {
                            m.get("id").and_then(|i| i.as_str()) == Some(model.as_str())
                                && accepts_image(m)
                        })
                    })
                    .unwrap_or(false);
                let in_overrides = p
                    .get("modelOverrides")
                    .and_then(|o| o.get(model.as_str()))
                    .map(accepts_image)
                    .unwrap_or(false);
                Some(in_models || in_overrides)
            })
            .unwrap_or(false);
        if !vision {
            gaps.push(format!(
                "models.json has no `{provider}/{model}` accepting image input: \
                 a text-only model cannot prove that the bot sees a photo or a video frame"
            ));
        }
    }
    for asset in ASSETS {
        let p = assets_dir().join(asset.0);
        if !p.is_file() {
            gaps.push(format!("missing stand asset {}", p.display()));
        }
    }
    if Command::new("ffmpeg").arg("-version").output().is_err() {
        gaps.push("ffmpeg is required to stage the oversized photo".to_string());
    }
    if !Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("system-prompt.md")
        .is_file()
    {
        gaps.push("missing system-prompt.md next to the crate root".to_string());
    }
    for rich in ["rich.json", "rich.md"] {
        let p = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("bot-home/compactors")
            .join(rich);
        if !p.is_file() {
            gaps.push(format!("missing bot-home compactor input {}", p.display()));
        }
    }
    if gaps.is_empty() {
        Ok(())
    } else {
        Err(gaps)
    }
}

/// (file name in `tests/stand_assets/`, expected object count in the picture)
const ASSETS: [(&str, u8); 2] = [("photo_4_squares.png", 4), ("video_frame_3_circles.jpg", 3)];

fn assets_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/stand_assets")
}

/// Deterministic oversized raster: the four-square asset over a fixed-seed
/// noise field. `random(1)` gave byte-identical output on repeated probes.
fn large_photo_bytes() -> Vec<u8> {
    let asset = assets_dir().join(ASSETS[0].0);
    let out = Command::new("ffmpeg")
        .args([
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "nullsrc=s=2400x1600,geq=r='random(1)*255':g='random(1)*255':b='random(1)*255'",
            "-i",
            asset.to_str().expect("asset path"),
            "-filter_complex",
            "[1:v]scale=1200:800[fg];[0:v][fg]overlay=(W-w)/2:(H-h)/2",
            "-frames:v",
            "1",
            "-f",
            "image2pipe",
            "-vcodec",
            "png",
            "-",
        ])
        .output()
        .expect("stage oversized photo");
    assert!(
        out.status.success(),
        "ffmpeg failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.stdout.len() as u64 > PHOTO_CONTEXT_LIMIT,
        "stand photo is not large enough to exercise the bound"
    );
    out.stdout
}

fn foreign_script_token(text: &str) -> Option<&str> {
    text.split_whitespace().find(|word| {
        let latin = word.chars().any(|c| c.is_ascii_alphabetic());
        let cyrillic = word.chars().any(|c| {
            ('а'..='я').contains(&c)
                || ('А'..='Я').contains(&c)
                || c == 'ё'
                || c == 'Ё'
        });
        let cjk = word
            .chars()
            .any(|c| ('\u{4e00}'..='\u{9fff}').contains(&c));
        cjk || (latin && cyrillic)
    })
}

fn crate_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Credential source: an explicit override, otherwise this machine's own agent
/// profile. Never the server profile, never the live bot profile.
fn creds_dir() -> PathBuf {
    match std::env::var("CHATBOT_STAND_CREDS") {
        Ok(v) if !v.trim().is_empty() => PathBuf::from(v),
        _ => {
            let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
            Path::new(&home).join(".omo").join("agent")
        }
    }
}

/// The daemon configuration in ONE place, because the stand's whole path rule
/// lives here: every value is relative to the run root (= the daemon cwd), so
/// the same checkout runs the stand on any machine. `CHATBOT_API_BASE` is a
/// URL, not a path, and is the only value added separately.
fn stand_env() -> Vec<(&'static str, String)> {
    vec![
        ("CHATBOT_STATE_DIR", "state".into()),
        ("CHATBOT_AGENT_DIR", "agent".into()),
        ("CHATBOT_CWD", ".".into()),
        ("CHATBOT_HOST_SOCKET", "state/host/rpc.sock".into()),
        ("CHATBOT_TOKEN_FILE", "token.env".into()),
        ("CHATBOT_SYSTEM", "system-prompt.md".into()),
        ("CHATBOT_SESSION", "stand".into()),
        ("CHATBOT_USERNAME", BOT_USERNAME.into()),
        ("CHATBOT_BATCH_SEC", "1".into()),
        ("CHATBOT_CHAT_ID", CHAT_ID.to_string()),
    ]
}

fn copy_mode_600(from: &Path, to: &Path) {
    std::fs::copy(from, to).unwrap_or_else(|e| panic!("copy {}: {e}", from.display()));
    let mut perms = std::fs::metadata(to)
        .unwrap_or_else(|e| panic!("stat {}: {e}", to.display()))
        .permissions();
    perms.set_mode(0o600);
    std::fs::set_permissions(to, perms).unwrap();
}

// ---- the daemon under test ----------------------------------------------

struct Run {
    root: PathBuf,
    child: std::process::Child,
    api_log: PathBuf,
    chat_log: PathBuf,
    events: PathBuf,
    queue: Arc<ApiState>,
    /// Opened lazily: the daemon migrates the same file at startup, and a
    /// connection racing that first migration would be a stand defect, not a
    /// product one.
    db: Option<Connection>,
}

/// SIGKILL every process whose own argv names this run root. The host
/// supervisor and its node client do; unrelated agents, shells and the test
/// itself do not, so the sweep cannot reach outside the stand.
fn kill_processes_under(root: &Path) {
    let needle = root.to_string_lossy().into_owned();
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return;
    };
    let me = std::process::id();
    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<u32>().ok())
        else {
            continue;
        };
        if pid == me {
            continue;
        }
        let Ok(cmd) = std::fs::read(entry.path().join("cmdline")) else {
            continue;
        };
        if String::from_utf8_lossy(&cmd).contains(&needle) {
            let _ = Command::new("kill")
                .args(["-9", &pid.to_string()])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}

impl Drop for Run {
    fn drop(&mut self) {
        // Diagnostics first: `chatd.log`, the event tail and the delivery log
        // are the only record of what the real model did.
        eprintln!("\n=== stand run {} ===", self.root.display());
        tail_print(&self.chat_log, 80, "chatd.log");
        tail_print(&self.events, 40, "events.jsonl");
        tail_print(&self.api_log, 40, "api-log.jsonl");
        let _ = self.child.kill();
        let _ = self.child.wait();
        // The host was spawned by the daemon: stop it through ITS socket, from
        // the run root and with the stand profile, so no other host is touched.
        let _ = Command::new("omo")
            .args(["host", "stop", "--force", "--socket", "state/host/rpc.sock"])
            .current_dir(&self.root)
            .env("OMO_CODING_AGENT_DIR", "agent")
            .env("SENPI_CODING_AGENT_DIR", "agent")
            .env("PI_CODING_AGENT_DIR", "agent")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let sock = self.root.join("state/host/rpc.sock");
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if UnixStream::connect(&sock).is_err() {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        if UnixStream::connect(&sock).is_ok() {
            eprintln!(
                "WARNING: host socket {} still accepts connections after `omo host stop --force`",
                sock.display()
            );
        }
        // `stop` closes the socket before the node host has finished exiting,
        // and that process can recreate `agent/` after a successful sweep
        // (observed 2026-11-06: an empty `agent/` stump survived a panic exit).
        // Nothing from this run root may outlive the test: kill only processes
        // whose OWN command line names this unique root, then sweep.
        kill_processes_under(&self.root);
        std::thread::sleep(Duration::from_millis(500));
        let mut removed = false;
        for attempt in 0..10 {
            match std::fs::remove_dir_all(&self.root) {
                Ok(()) => {
                    removed = true;
                    break;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    removed = true;
                    break;
                }
                Err(_) if attempt < 9 => std::thread::sleep(Duration::from_millis(300)),
                Err(e) => {
                    eprintln!(
                        "WARNING: run dir {} survived teardown: {e}",
                        self.root.display()
                    );
                }
            }
        }
        if removed {
            // One more look after the grace period: a late writer would have
            // recreated the directory by now.
            std::thread::sleep(Duration::from_millis(300));
            kill_processes_under(&self.root);
            let _ = std::fs::remove_dir_all(&self.root);
            if self.root.exists() {
                eprintln!(
                    "WARNING: run dir {} reappeared after teardown",
                    self.root.display()
                );
            }
        }
    }
}

fn tail_print(path: &Path, lines: usize, title: &str) {    let Ok(text) = std::fs::read_to_string(path) else {
        eprintln!("--- {title}: (missing) ---");
        return;
    };
    let all: Vec<&str> = text.lines().collect();
    let start = all.len().saturating_sub(lines);
    eprintln!("--- {title} (last {lines} lines of {}) ---", all.len());
    for line in &all[start.min(all.len())..] {
        eprintln!("{line}");
    }
}

impl Run {
    fn prepare() -> Run {
        let creds = creds_dir();
        if let Err(gaps) = check_prerequisites(&creds) {
            panic!(
                "stand_real_model prerequisites are not met — refusing to pretend the stand ran:\n  - {}",
                gaps.join("\n  - ")
            );
        }
        // The run root lives under `target/` (already git-ignored); every daemon
        // path is relative to it. `pid` keeps parallel/repeated runs apart.
        static SEQ: AtomicI64 = AtomicI64::new(0);
        let root = crate_dir()
            .join("target/stand")
            .join(format!(
                "run-{}-{}",
                std::process::id(),
                SEQ.fetch_add(1, Ordering::Relaxed)
            ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("state")).expect("create run state dir");
        std::fs::create_dir_all(root.join("agent/compactors")).expect("create agent dir");

        // Profile: this machine's credentials, the stand's own model choice,
        // the bot's real compactor inputs. Copied, never referenced: the stand
        // must not be able to touch the live profile.
        copy_mode_600(&creds.join("auth.json"), &root.join("agent/auth.json"));
        copy_mode_600(&creds.join("models.json"), &root.join("agent/models.json"));
        let settings = json!({
            "defaultProvider": stand_provider(),
            "defaultModel": stand_model(),
            "defaultThinkingLevel": std::env::var("CHATBOT_STAND_THINKING_LEVEL")
                .unwrap_or_else(|_| "high".to_string()),
            "permissionPreset": "full-access",
            "disabledBuiltinExtensions": ["compaction", "ttsr"],
            "todo": {"firstTurnPlan": "off", "turnEndBackstop": false},
        });
        std::fs::write(
            root.join("agent/settings.json"),
            serde_json::to_string_pretty(&settings).expect("settings json"),
        )
        .expect("write stand settings");
        // The production profile ships the sampling extensions, the retry
        // extension and the persona. Staging them here is what makes the stand
        // run the SAME configuration the host gets: a profile change that
        // never reached the stand would be tested only in production.
        std::fs::create_dir_all(root.join("agent/extensions")).expect("create extensions dir");
        std::fs::create_dir_all(root.join("agent/logs")).expect("create logs dir");
        for ext in [
            "default-min-temperature.ts",
            "sampling-payload.ts",
            "alibaba-output-retry-port.ts",
        ] {
            std::fs::copy(
                crate_dir().join("bot-home/extensions").join(ext),
                root.join("agent/extensions").join(ext),
            )
            .expect("copy profile extension");
        }
        // The prompt-plan extension is staged the same way production gets it:
        // a plain copy the runtime discovers on its own. Without it the stand
        // would prove nothing about the rules bands and the rotation.
        std::fs::copy(
            crate_dir().join("md-inject.ts"),
            root.join("agent/extensions/md-inject.ts"),
        )
        .expect("copy md-inject extension");
        // A real catalog in the run root: one light document that rides every
        // arrival, one heavy document, and the cwd notes file. The heavy
        // document holds the delivery marker, and the notes file is rotation
        // material regardless of its size (owner rule), so a two-document
        // rotation guarantees that across two later turns the marker is
        // delivered once and excluded once, and the notes ride some but not
        // every arrival.
        let knowledge = root.join("knowledge");
        std::fs::create_dir_all(&knowledge).expect("create stand knowledge dir");
        std::fs::write(knowledge.join("00-лёгкий.md"), "лёгкая заметка про чат\n")
            .expect("write light knowledge document");
        std::fs::write(
            knowledge.join("01-тяжёлый.md"),
            format!("хроника чата. nonce-StandA1b2c3\n{}", "а".repeat(4000)),
        )
        .expect("write marker knowledge document");
        std::fs::write(root.join("agents.md"), "заметки владельца для стенда\n")
            .expect("write cwd notes document");
        if crate_dir().join("bot-home/AGENTS.md").is_file() {
            std::fs::copy(
                crate_dir().join("bot-home/AGENTS.md"),
                root.join("agent/AGENTS.md"),
            )
            .expect("copy persona");
        }
        for rich in ["rich.json", "rich.md"] {
            std::fs::copy(
                crate_dir().join("bot-home/compactors").join(rich),
                root.join("agent/compactors").join(rich),
            )
            .expect("copy compactor config");
        }
        // The persona is the REAL one: the sending contract (`chat_send` only)
        // must be the battle contract, not a stand imitation.
        std::fs::copy(
            crate_dir().join("system-prompt.md"),
            root.join("system-prompt.md"),
        )
        .expect("copy system prompt");
        std::fs::write(root.join("token.env"), "1:stand-only\n").expect("write token file");
        let mut perms = std::fs::metadata(root.join("token.env"))
            .expect("stat token")
            .permissions();
        perms.set_mode(0o600);
        std::fs::set_permissions(root.join("token.env"), perms).expect("chmod token");

        let photo = large_photo_bytes();
        let frame = std::fs::read(assets_dir().join(ASSETS[1].0)).expect("read frame asset");
        let api_log = root.join("api-log.jsonl");
        let (base, queue) = start_api(&api_log, photo, frame);

        // `db()` reads CHATBOT_CHAT_ID to seed the chat identity. The test
        // process does not inherit the daemon's env, so pin it here; this test
        // binary holds a single test, so the process-global value is safe.
        std::env::set_var("CHATBOT_CHAT_ID", CHAT_ID.to_string());

        let mut cmd = Command::new(env!("CARGO_BIN_EXE_chatd"));
        cmd.arg("run")
            // Never inherit the harness pipes: a surviving grandchild (`omo`
            // host) would hold them open and hang cargo long after this test.
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .current_dir(&root)
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            // The whole configuration is RELATIVE to `root` = the daemon's
            // cwd (see `stand_env`); nothing absolute is baked into the stand.
            .env("CHATBOT_API_BASE", &base);
        for (key, value) in stand_env() {
            cmd.env(key, value);
        }
        // A developer's exported environment must not leak another chat, an
        // owner identity or an ext notifier into the stand.
        for leaked in [
            "CHATBOT_OWNER_USERNAME",
            "CHATBOT_OWNER_ID",
            "CHATBOT_SUBAGENT_MODEL",
            "CHATBOT_NOTIFY_CMD",
            "CHATBOT_KNOWLEDGE_DIR",
            "CHATBOT_KNOWLEDGE_TMP",
            "CHATBOT_STALE_SEC",
            "CHATBOT_SYSTEMD_UNIT",
            "CHATBOT_ADMIT_FIRST_SEC",
            "CHATBOT_ADMIT_SEC",
            "CHATBOT_HOST_WATCH_SEC",
        ] {
            cmd.env_remove(leaked);
        }
        let child = cmd.spawn().expect("spawn chatd");
        eprintln!("stand running in {}", root.display());

        Run {
            chat_log: root.join("state/chatd.log"),
            events: root.join("state/events.jsonl"),
            api_log,
            root,
            child,
            queue,
            db: None,
        }
    }

    /// Open the mirror once the daemon is idle: migrations are done and the
    /// connection only reads.
    fn open_db(&mut self) {
        let path = self.root.join("state");
        let path = path.to_string_lossy().to_string();
        for _ in 0..40 {
            match db(&path) {
                Ok(conn) => {
                    self.db = Some(conn);
                    return;
                }
                Err(_) => std::thread::sleep(Duration::from_millis(250)),
            }
        }
        panic!("cannot open the stand database at {path} after the daemon reported readiness");
    }

    /// Same contract as [`Self::wait_for`], but a missing result is a plain
    /// `None` instead of a panic: used for sequencing aids that are not
    /// properties of the stand.
    fn try_wait_for<T>(
        &mut self,
        budget: Duration,
        mut probe: impl FnMut(&mut Self) -> Option<T>,
    ) -> Option<T> {
        let deadline = Instant::now() + budget;
        loop {
            if let Some(v) = probe(self) {
                return Some(v);
            }
            if !self.alive() || Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    fn alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    fn chat_text(&self) -> String {
        std::fs::read_to_string(&self.chat_log).unwrap_or_default()
    }

    /// The durable session id the daemon advertises once the host is up.
    fn session_uid(&self) -> String {
        self.chat_text()
            .lines()
            .find_map(|l| {
                l.split_once("admit unit ready, session ")
                    .map(|(_, uid)| uid.trim().to_string())
            })
            .unwrap_or_else(|| panic!("no `admit unit ready, session` line yet: {}", self.chat_text()))
    }

    /// Kill the autonomous warm-up run opened together with the session
    /// (`session open --prompt`). Given only a persona, the model can wander
    /// for minutes (observed live: 16+ empty tool turns before any batch), and
    /// the stand must not spend that time or quota. `abort` ends the run but
    /// keeps the session; `wait --until idle` below then returns at once.
    fn abort_warmup(&mut self) {
        let uid = self.session_uid();
        let out = Command::new("omo")
            .args([
                "host",
                "session",
                "abort",
                &uid,
                "--socket",
                "state/host/rpc.sock",
            ])
            .current_dir(&self.root)
            .env("OMO_CODING_AGENT_DIR", "agent")
            .env("SENPI_CODING_AGENT_DIR", "agent")
            .env("PI_CODING_AGENT_DIR", "agent")
            .stdin(Stdio::null())
            .output();
        if let Ok(out) = out {
            let text = String::from_utf8_lossy(&out.stdout);
            if !text.contains("\"aborted\":true") && !text.contains("no_active_run") {
                eprintln!("warm-up abort: {}", text.trim());
            }
        }
    }

    /// Authoritative idle barrier: the runtime itself reports when the whole
    /// run has settled. `turn_end`/settle EVENTS race with the warm-up turns a
    /// fresh session runs after `session open --prompt`; a push during them is
    /// admitted as a STEER (observed live 2026-11-06: the text batch was
    /// `steered` and the photo batch never got a file pointer).
    fn wait_host_idle(&mut self) {
        let uid = self.session_uid();
        let deadline = Instant::now() + Duration::from_secs(240);
        loop {
            let out = Command::new("omo")
                .args([
                    "host",
                    "session",
                    "wait",
                    &uid,
                    "--until",
                    "idle",
                    "--timeout",
                    "120000",
                    "--socket",
                    "state/host/rpc.sock",
                ])
                .current_dir(&self.root)
                .env("OMO_CODING_AGENT_DIR", "agent")
                .env("SENPI_CODING_AGENT_DIR", "agent")
                .env("PI_CODING_AGENT_DIR", "agent")
                .stdin(Stdio::null())
                .output();
            match out {
                Ok(o) => {
                    let text = String::from_utf8_lossy(&o.stdout).to_string();
                    if o.status.success() && !text.contains("wait_timeout") {
                        return;
                    }
                    eprintln!("wait idle: {}", text.trim());
                }
                Err(e) => panic!("omo host session wait: {e}"),
            }
            if Instant::now() >= deadline {
                panic!("host never became idle; chatd.log:\n{}", self.chat_text());
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    fn log_count(&self, needle: &str) -> usize {
        self.chat_text().lines().filter(|l| l.contains(needle)).count()
    }

    /// Control socket and logs are meaningless while the daemon is dead: every
    /// wait checks liveness and fails with the log tail instead of idling.
    fn wait_for<T>(&mut self, what: &str, budget: Duration, mut probe: impl FnMut(&mut Self) -> Option<T>) -> T {
        let deadline = Instant::now() + budget;
        loop {
            if let Some(v) = probe(self) {
                return v;
            }
            if !self.alive() {
                let status = self.child.wait().ok();
                panic!(
                    "stand daemon exited while waiting for {what} ({status:?}); log:\n{}",
                    self.chat_text()
                );
            }
            if Instant::now() >= deadline {
                panic!(
                    "timed out after {:?} waiting for {what}; chatd.log:\n{}",
                    budget,
                    self.chat_text()
                );
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    fn wait_log(&mut self, what: &str, from: usize, budget: Duration) {
        self.wait_for(what, budget, |run| {
            (run.log_count(what) > from).then_some(())
        });
    }

    fn wait_socket(&mut self) {
        let sock = self.root.join(control_sock_path("state"));
        self.wait_for("control socket", Duration::from_secs(60), |_| {
            UnixStream::connect(&sock).ok().map(|_| ())
        });
    }

    /// Feed one incoming update exactly as Telegram would deliver it.
    fn push_update(&mut self, update: Value) {
        self.queue.queue.lock().unwrap().push(update);
    }

    fn send_entries(&self) -> Vec<Value> {
        let Ok(text) = std::fs::read_to_string(&self.api_log) else {
            return vec![];
        };
        text.lines()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .filter(|v| v.get("method").and_then(|m| m.as_str()) == Some("sendMessage"))
            .filter(|v| {
                !v.get("text")
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .trim()
                    .is_empty()
            })
            .collect()
    }

    /// One turn: push the update, wait for the answer the model sent through
    /// `chat_send`, then for the run to settle (otherwise the next update would
    /// steer the still-open turn), then for the ledger to retire the batch.
    fn turn(&mut self, name: &str, update: Value, media_wait: Option<(i64, Kind)>) -> String {
        let incoming_id = update
            .get("message")
            .and_then(|m| m.get("message_id"))
            .and_then(|v| v.as_i64())
            .expect("message id");
        let send_mark = self.send_entries().len();
        let retire_mark = self.log_count(" retired (");
        self.push_update(update);

        if let Some((message_id, kind)) = media_wait {
            self.wait_for(&format!("{name}: media ready"), TURN_BUDGET, |run| {
                row(run, message_id)
                    .filter(|r| kind.ready(r))
                    .map(|_| ())
            });
        }

        let answer = self.wait_for(&format!("{name}: answer on the Bot API"), TURN_BUDGET, |run| {
            run.send_entries().get(send_mark).cloned()
        });
        let text = answer
            .get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if let Some(reply_to) = answer
            .get("reply_to_message_id")
            .and_then(|v| v.as_i64())
        {
            assert_eq!(
                reply_to, incoming_id,
                "{name}: the answer quotes message {reply_to} instead of this turn's incoming {incoming_id}"
            );
        }
        // `turn_end` fires per assistant cycle, LONG before the runtime is idle:
        // pushing the next update on it lands as a STEER inside the same run.
        // The host's own idle report is the only authoritative barrier.
        self.wait_host_idle();
        // Retiring the batch is not a property of the stand, but a still-open
        // batch would fold the next message into the previous one and muddy the
        // window check; give it its own (non-fatal) moment.
        let _ = self.try_wait_for(Duration::from_secs(90), |run| {
            (run.log_count(" retired (") > retire_mark).then_some(())
        });
        eprintln!("{name}: incoming {incoming_id} -> answer {text:?}");
        text
    }
}

#[derive(Clone, Copy)]
enum Kind {
    Photo,
    Video,
}

/// One row of the mirror, as the daemon stored it.
struct Row {
    media_state: String,
    media_path: String,
    thumb_state: String,
    thumb_path: String,
}

impl Kind {
    fn ready(&self, row: &Row) -> bool {
        match self {
            Kind::Photo => row.media_state == "ready",
            // A video never becomes a downloadable blob: its FRAME is the file.
            Kind::Video => row.thumb_state == "ready",
        }
    }
}

fn row(run: &Run, message_id: i64) -> Option<Row> {
    let db = run.db.as_ref()?;
    db.query_row(
            "SELECT COALESCE(media_state,'absent'), COALESCE(media_path,''), \
                    COALESCE(thumb_state,'absent'), COALESCE(thumb_path,'') \
             FROM messages WHERE message_id=?",
            [message_id],
            |r| {
                Ok(Row {
                    media_state: r.get(0)?,
                    media_path: r.get(1)?,
                    thumb_state: r.get(2)?,
                    thumb_path: r.get(3)?,
                })
            },
        )
        .ok()
}

// ---- incoming update shapes --------------------------------------------

fn base_message(message_id: i64, _date: i64) -> Value {
    // Telegram dates must be CURRENT: `any_pending` only lets `media_settle`
    // wait for fresh messages (date >= now - MEDIA_GRACE). Hard-coded 2025
    // stamps made the photo row look ancient, so the batch was built for a
    // still-pending row and reached the model as a bare `[photo]` without the
    // `[file]` pointer (observed live 2026-11-06).
    json!({
        "message_id": message_id,
        "date": chrono::Utc::now().timestamp(),
        "chat": {"id": CHAT_ID, "type": "supergroup", "title": "Stand"},
        "from": {
            "id": OWNER_ID,
            "is_bot": false,
            "first_name": "Owner",
            "username": OWNER_USERNAME,
        },
    })
}

fn text_update(update_id: i64, message_id: i64, text: &str) -> Value {
    let mut msg = base_message(message_id, 1_735_670_000 + update_id);
    msg["text"] = json!(text);
    json!({"update_id": update_id, "message": msg})
}

/// First half of the live duplicate pair: a `channel_post` with a negative
/// sender id and no `from`, arriving before the group forward.
fn channel_post_update(update_id: i64, message_id: i64, text: &str) -> Value {
    let mut msg = base_message(message_id, 1_735_670_000 + update_id);
    if let Some(obj) = msg.as_object_mut() {
        obj.remove("from");
    }
    msg["sender_chat"] = json!({
        "id": CHANNEL_ID,
        "title": "Stand Channel",
        "username": "stand_channel"
    });
    msg["text"] = json!(text);
    json!({"update_id": update_id, "channel_post": msg})
}

/// A message older than the daemon's age budget: the guard must drop it
/// before context assembly, so it is never pushed and the model never sees it.
fn stale_text_update(update_id: i64, message_id: i64, text: &str, age_secs: i64) -> Value {
    let mut msg = base_message(message_id, 0);
    msg["text"] = json!(text);
    msg["date"] = json!(chrono::Utc::now().timestamp() - age_secs);
    json!({"update_id": update_id, "message": msg})
}

fn photo_update(update_id: i64, message_id: i64, caption: &str) -> Value {
    let mut msg = base_message(message_id, 1_735_671_000 + update_id);
    msg["caption"] = json!(caption);
    msg["photo"] = json!([{
        "file_id": PHOTO_FILE_ID,
        "file_unique_id": "photo_atom_u",
        "width": 761,
        "height": 621,
        "file_size": 5_000_000,
    }]);
    json!({"update_id": update_id, "message": msg})
}

fn video_update(update_id: i64, message_id: i64, caption: &str, thumb_size: u64) -> Value {
    let mut msg = base_message(message_id, 1_735_672_000 + update_id);
    msg["caption"] = json!(caption);
    msg["video"] = json!({
        "file_id": "video_atom",
        "file_unique_id": "video_atom_u",
        "duration": 2,
        "width": 871,
        "height": 411,
        "mime_type": "video/mp4",
        "file_size": 9751,
        // The only part of a video this wire can carry into the context.
        "thumbnail": {
            "file_id": FRAME_FILE_ID,
            "file_unique_id": "frame_atom_u",
            "width": 881,
            "height": 421,
            "file_size": thumb_size,
        },
    });
    json!({"update_id": update_id, "message": msg})
}

// ---- what the model answered -------------------------------------------

/// True when `text` states `n` as a digits token or as the Russian word. Word
/// boundaries matter: "три" inside "тридцать" is not an answer about three.
fn names_number(text: &str, n: u8) -> bool {
    let digit = n.to_string();
    let words: &[&str] = match n {
        3 => &["три", "трёх", "трех", "трое"],
        4 => &["четыре", "четырёх", "четырех", "четверо", "четырём", "четырем"],
        _ => &[],
    };
    let mut token = String::new();
    for ch in text.chars().chain(std::iter::once(' ')) {
        if ch.is_alphanumeric() || ch == 'ё' {
            token.extend(ch.to_lowercase());
        } else if !token.is_empty() {
            if token == digit || words.contains(&token.as_str()) {
                return true;
            }
            token.clear();
        }
    }
    false
}

/// Read tool calls recorded in the session transcript, as raw argument paths.
fn transcript_read_paths(agent_dir: &Path) -> Vec<String> {
    let mut out = vec![];
    let mut stack = vec![agent_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
                continue;
            }
            let Ok(text) = std::fs::read_to_string(&path) else {
                continue;
            };
            for line in text.lines() {
                let Ok(rec) = serde_json::from_str::<Value>(line) else {
                    continue;
                };
                let Some(content) = rec
                    .get("message")
                    .and_then(|m| m.get("content"))
                    .and_then(|c| c.as_array())
                else {
                    continue;
                };
                for part in content {
                    if part.get("type").and_then(|t| t.as_str()) != Some("toolCall") {
                        continue;
                    }
                    if part.get("name").and_then(|n| n.as_str()) != Some("read") {
                        continue;
                    }
                    if let Some(p) = part
                        .get("arguments")
                        .and_then(|a| a.get("path"))
                        .and_then(|p| p.as_str())
                    {
                        out.push(p.to_string());
                    }
                }
            }
        }
    }
    out
}

fn same_file(root: &Path, shown: &str, opened: &str) -> bool {
    let a = std::fs::canonicalize(root.join(shown));
    let b = std::fs::canonicalize(root.join(opened));
    matches!((a, b), (Ok(a), Ok(b)) if a == b)
}

// ---- the stand ----------------------------------------------------------

#[test]
#[ignore = "real model, real quota: run explicitly with --ignored"]
fn text_then_photo_then_video_with_a_real_model() {
    let mut run = Run::prepare();
    let frame_asset = ASSETS[1].0;
    let mut answers = vec![];

    run.wait_socket();
    // The admit unit settles whatever turn the host already has open before it
    // accepts jobs. Opening a session is not passive: the host runs and aborts
    // its own warm-up turn, and THAT `turn_end` releases the startup settle
    // (observed: readiness after ~10 s, not the 180 s worst case of the budget).
    // Waiting for the line — instead of assuming a delay — keeps the turn
    // budgets about the model.
    let ready_mark = run.log_count("admit unit ready, session");
    if ready_mark == 0 {
        eprintln!("waiting for the admit unit (host spawn + session open)…");
    }
    run.wait_log("admit unit ready, session", ready_mark, STARTUP_BUDGET);
    let session_line = run
        .chat_text()
        .lines()
        .find(|l| l.contains("admit unit ready, session"))
        .unwrap_or_default()
        .to_string();
    eprintln!("{session_line}");
    // A straggler from the warm-up turn (its settle can land a moment after
    // readiness) or the warm-up itself must be OVER before the first push:
    // pushing into it would steer the batch into a run with no pointer yet.
    run.abort_warmup();
    run.wait_host_idle();
    run.open_db();

    // 1. text
    // The first window rides this batch, but the daemon records it as SERVED
    // only when the delivery proof arrives (``history window delivered``).
    // Without this wait the next batch is built while the epoch is still
    // unknown and wrongly carries the 50-message window a second time —
    // observed in the first live run (`378cdb... carries`).
    let window_mark = run.log_count("history window delivered");
    let text_answer = run.turn(
        "text",
        text_update(501, 830_041, &format!("@{BOT_USERNAME} ты на связи? ответь через chat_send")),
        None,
    );
    run.wait_log("history window delivered", window_mark, TURN_BUDGET);
    run.wait_host_idle();
    assert!(
        !text_answer.trim().is_empty(),
        "text turn: the model answered with an empty message"
    );
    assert!(
        text_answer.chars().count() <= 280,
        "text turn: casual reply is not a short chat turn ({} chars): {text_answer:?}",
        text_answer.chars().count()
    );
    answers.push(("text", text_answer.as_str()));

    // 1a. mirrored channel post: channel copy first, group forward second.
    // The channel copy is held; the group copy cancels it, so exactly one
    // answer reaches the fake Bot API.
    let mirror_text = "@stand_bot скажи слово радуга";
    let channel_id = 830_021;
    let group_id = 830_022;
    let hold_mark = run.log_count("holding channel copy");
    let send_mark = run.send_entries().len();
    run.push_update(channel_post_update(511, channel_id, mirror_text));
    run.wait_log(
        "holding channel copy",
        hold_mark,
        Duration::from_secs(30),
    );
    assert_eq!(
        run.send_entries().len(),
        send_mark,
        "channel twin answered before the group copy arrived"
    );
    let mirror_answer = run.turn(
        "mirror",
        text_update(512, group_id, mirror_text),
        None,
    );
    assert!(
        !mirror_answer.trim().is_empty(),
        "mirror turn: the group copy produced no answer"
    );
    assert_eq!(
        run.send_entries().len(),
        send_mark + 1,
        "a mirrored channel post produced more than one answer"
    );
    assert!(
        run.log_count("dropped 1 pending channel twin(s)") > 0,
        "the daemon never reported cancelling the channel twin"
    );
    answers.push(("mirror", mirror_answer.as_str()));

    // 2. photo: the number of objects is nowhere in the caption
    let photo_id = 830_051;
    let photo_caption = format!(
        "@{BOT_USERNAME} посмотри фото: открой файл из строки [file] инструментом read и скажи, сколько предметов на нём?"
    );
    assert!(!names_number(&photo_caption, ASSETS[0].1));
    let photo_answer = run.turn(
        "photo",
        photo_update(513, photo_id, &photo_caption),
        Some((photo_id, Kind::Photo)),
    );
    assert!(
        names_number(&photo_answer, ASSETS[0].1),
        "photo turn: the answer does not name the four objects on the photo, so the model \
         cannot have seen its content: {photo_answer:?}"
    );
    answers.push(("photo", photo_answer.as_str()));

    // 3. video: only the frame travels, so the answer must describe the frame
    let frame_size = std::fs::metadata(assets_dir().join(frame_asset))
        .expect("stat frame asset")
        .len();
    let video_id = 830_061;
    let video_caption = format!(
        "@{BOT_USERNAME} посмотри кадр видео: открой файл из строки [file] инструментом read и скажи, сколько предметов в нём?"
    );
    assert!(!names_number(&video_caption, ASSETS[1].1));
    let frame_answer = run.turn(
        "video",
        video_update(514, video_id, &video_caption, frame_size),
        Some((video_id, Kind::Video)),
    );
    assert!(
        names_number(&frame_answer, ASSETS[1].1),
        "video turn: the answer does not name the three objects on the frame, so the model \
         cannot have seen the video frame: {frame_answer:?}"
    );
    answers.push(("video", frame_answer.as_str()));

    // 3a. the injection budget, observed in the live hook log: the first
    // arrival of a run carries the whole catalog and both rule bands, every
    // later arrival carries the light document plus exactly ONE heavy one, and
    // the delivery marker is only reported on turns that really carried it.
    let inject_log = run.root.join("agent/logs/md-inject.log");
    let inject_text = std::fs::read_to_string(&inject_log).unwrap_or_default();
    assert!(
        !inject_text.trim().is_empty(),
        "md-inject wrote no log at {} — the extension never ran in the stand",
        inject_log.display()
    );
    assert!(
        !inject_text.contains("knowledge_dir_missing"),
        "the injection never found the stand knowledge dir:\n{inject_text}"
    );
    let records: Vec<Value> = inject_text
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter(|v| v.get("event").and_then(|e| e.as_str()) == Some("before_agent_start"))
        .collect();
    assert!(
        records.len() >= 3,
        "the three turns produced only {} injection records: {inject_text}",
        records.len()
    );
    let listed = |rec: &Value, key: &str| -> Vec<String> {
        rec.get(key)
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    };
    let rotation_docs = ["01-тяжёлый.md", "agents.md"];
    let rules_of = |rec: &Value| -> Vec<u64> {
        rec.get("rules_ids")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|v| v.as_u64()).collect())
            .unwrap_or_default()
    };
    let mut first_bytes: Option<u64> = None;
    let mut marker_excluded = false;
    let mut marker_delivered = false;
    let mut notes_seen = false;
    let mut notes_absent = false;
    for rec in &records {
        let arrival = rec.get("arrival").and_then(|a| a.as_u64()).unwrap_or(0);
        let paths = listed(rec, "paths");
        let carries = |name: &str| paths.iter().any(|p| p.ends_with(name));
        let bytes = rec.get("knowledge_bytes").and_then(|b| b.as_u64()).unwrap_or(0);
        let ids = rules_of(rec);
        assert!(
            ids.contains(&10),
            "arrival {arrival} dropped rule 10, the language rule that must ride every turn: {rec}"
        );
        assert!(
            ids.contains(&5),
            "arrival {arrival} dropped rule 5, the short-default rule that must ride every turn: {rec}"
        );
        if arrival > 1 {
            assert_eq!(
                ids.contains(&11),
                arrival % 3 == 0,
                "arrival {arrival} carried discourse rule 11 against its demoted every-third tier: {rec}"
            );
            assert_eq!(
                ids.contains(&7),
                arrival % 3 == 0,
                "arrival {arrival} carried rule 7 against its tier: {rec}"
            );
        }
        assert!(
            carries("00-лёгкий.md"),
            "arrival {arrival} dropped the light document: {rec}"
        );
        if arrival == 1 {
            assert!(
                rotation_docs.iter().all(|name| carries(name)),
                "the first arrival of a run must carry every rotation document: {rec}"
            );
            assert_eq!(ids.len(), 11, "the first arrival must carry all 11 rules: {rec}");
            first_bytes = Some(bytes);
        } else {
            let present: Vec<&str> = rotation_docs
                .iter()
                .copied()
                .filter(|name| carries(name))
                .collect();
            assert_eq!(
                present.len(),
                1,
                "arrival {arrival} carried {} rotation documents ({present:?}), the budget allows one: {rec}",
                present.len()
            );
            let full = first_bytes.expect("the first arrival record precedes later ones");
            assert!(
                bytes < full,
                "arrival {arrival} paid {bytes} B, no less than the full catalog {full} B: {rec}"
            );
            if carries("01-тяжёлый.md") {
                marker_delivered = true;
            } else {
                assert!(
                    rec.get("nonce").map(|v| v.is_null()).unwrap_or(false),
                    "arrival {arrival} reported a delivery marker of a document it did not carry: {rec}"
                );
                marker_excluded = true;
            }
            if carries("agents.md") {
                notes_seen = true;
            } else {
                notes_absent = true;
            }
        }
    }
    assert!(
        marker_delivered,
        "the rotated-in marker document never reached a later arrival: {inject_text}"
    );
    assert!(
        marker_excluded,
        "no later arrival excluded the marker document, so the honest nonce was never exercised: {inject_text}"
    );
    assert!(
        notes_seen,
        "the cwd notes file never reached a later arrival: {inject_text}"
    );
    assert!(
        notes_absent,
        "the cwd notes file rode every arrival, but the owner ruled it must rotate: {inject_text}"
    );

    // 3b. stale message: an hour old, older than the age budget. It is scaled
    // and closed as `skip_by_time` at admit and never pushed, so the model
    // never sees it and no turn starts for it. Cheap quota-wise: nothing
    // reaches the model on this turn.
    let stale_id = 830_071;
    let stale_mark = run.send_entries().len();
    let stale_log = run.log_count("skip_by_time");
    run.push_update(stale_text_update(
        515,
        stale_id,
        &format!("@{BOT_USERNAME} это сообщение опоздало"),
        3600,
    ));
    run.wait_log("skip_by_time", stale_log, Duration::from_secs(120));
    assert_eq!(
        run.send_entries().len(),
        stale_mark,
        "a stale message must never produce a model answer"
    );
    let skipped: i64 = run
        .db
        .as_ref()
        .expect("db opened")
        .query_row(
            "SELECT COUNT(*) FROM batches WHERE outcome='skip_by_time'",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);
    assert!(
        skipped >= 1,
        "no batch was closed as skip_by_time for the stale message"
    );

    // 4. media rows: relative paths, and the video row stays a descriptor
    let photo_row = row(&run, photo_id).expect("photo row in the mirror");
    assert_eq!(photo_row.media_state, "ready", "photo blob not finalized");
    let photo_size = std::fs::metadata(run.root.join(&photo_row.media_path))
        .expect("stat bounded photo")
        .len();
    assert!(
        photo_size <= PHOTO_CONTEXT_LIMIT,
        "oversized photo stored at {photo_size} bytes, above the context bound"
    );
    assert!(
        photo_row.media_path.starts_with("state/media/"),
        "photo media_path is not relative to the run root (H6): {:?}",
        photo_row.media_path
    );
    assert!(
        run.root.join(&photo_row.media_path).is_file(),
        "photo blob {} does not exist under the run root",
        photo_row.media_path
    );
    let video_row = row(&run, video_id).expect("video row in the mirror");
    assert_eq!(
        video_row.media_state, "descriptor",
        "the video mp4 must never be downloaded — only its frame travels"
    );
    assert_eq!(video_row.thumb_state, "ready", "video frame not finalized");
    assert!(
        video_row.thumb_path.starts_with("state/media/")
            && run.root.join(&video_row.thumb_path).is_file(),
        "video frame path is not a relative, existing file: {:?}",
        video_row.thumb_path
    );

    // 4a. live regression: no mixed-language tokens in Russian prompts.
    for (name, answer) in &answers {
        assert!(
            foreign_script_token(answer).is_none(),
            "{name} answer mixed scripts: {answer:?}"
        );
    }

    // 5. the model opened the very files the batch pointed at
    let photo_shown = photo_row.media_path.clone();
    let frame_shown = video_row.thumb_path.clone();
    let reads = run.wait_for(
        "read tool calls for the photo and the video frame",
        Duration::from_secs(120),
        |state| {
            let paths = transcript_read_paths(&state.root.join("agent"));
            let photo = paths
                .iter()
                .any(|p| same_file(&state.root, &photo_shown, p));
            let frame = paths
                .iter()
                .any(|p| same_file(&state.root, &frame_shown, p));
            (photo && frame).then_some(paths)
        },
    );
    for (what, shown) in [
        ("photo", photo_shown.as_str()),
        ("video frame", frame_shown.as_str()),
    ] {
        assert!(
            reads.iter().any(|p| same_file(&run.root, shown, p)),
            "the model never `read` the {what} the batch pointed at ({shown}); reads: {reads:?}"
        );
    }

    // 6. one session, window on the first turn only
    let window_lines: Vec<String> = run
        .chat_text()
        .lines()
        .filter(|l| l.contains("history window"))
        .map(|l| l.to_string())
        .collect();
    let carried: Vec<&String> = window_lines
        .iter()
        .filter(|l| l.contains("carries the last"))
        .collect();
    let omitted: Vec<&String> = window_lines
        .iter()
        .filter(|l| l.contains("history window omitted (same session"))
        .collect();
    assert_eq!(
        carried.len(),
        1,
        "the history window must ride exactly one turn, got: {window_lines:#?}"
    );
    assert!(
        omitted.len() >= 2,
        "the two later turns must say the window was omitted for the same session, got: {window_lines:#?}"
    );
    let epoch = |line: &str| {
        // The first turn says `(session <uid>:<gen>)`; later ones say
        // `(same session <uid>:<gen>, no compaction since)`. Split on the
        // common `session ` token, not on the leading parenthesis.
        line.split("session ")
            .nth(1)
            .map(|s| {
                s.split(')')
                    .next()
                    .unwrap_or("")
                    .split(',')
                    .next()
                    .unwrap_or("")
                    .trim()
                    .to_string()
            })
            .unwrap_or_default()
    };
    let first_epoch = epoch(carried[0]);
    assert!(!first_epoch.is_empty(), "window line carries no session epoch: {carried:?}");
    for line in &omitted {
        assert_eq!(
            epoch(line), first_epoch,
            "a turn was served by a DIFFERENT session than the first one; the single-session \
             contract broke: {line}"
        );
    }
    let live: i64 = run
        .db
        .as_ref()
        .expect("db opened before the session checks")
        .query_row("SELECT COUNT(*) FROM sessions WHERE state='live'", [], |r| {
            r.get(0)
        })
        .unwrap_or(0);
    assert_eq!(live, 1, "expected exactly one live session, found {live}");

    // 7. the path rule itself: every configured value the daemon receives is
    //    relative to the run root (H6) — moving the checkout changes nothing.
    for (key, value) in stand_env() {
        assert!(
            !value.starts_with('/'),
            "{key}={value:?} is an absolute path; the stand must stay relocatable"
        );
    }

    eprintln!(
        "\nSTAND OK — text, photo ({} objects) and video frame ({} objects) were answered by \
         {}/{} through chat_send; log: {}",
        ASSETS[0].1,
        ASSETS[1].1,
        stand_provider(),
        stand_model(),
        run.api_log.display()
    );
}
