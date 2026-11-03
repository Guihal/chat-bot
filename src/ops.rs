//! Observability and operations: facts the daemon publishes, the verdict an
//! external watcher derives from them, alert lines, and the release manifest.
//!
//! Contract: `health.json` holds FACTS only. A verdict is never persisted
//! there — if the file is old that only says when the last facts were written,
//! not whether the bot works. The watcher compares `poll_ok_at` with its own
//! threshold and the systemd unit state, so a live process whose Telegram poll
//! has gone deaf is `stale`, never `up`.
//!
//! All timing below derives from one configured value: `CHATBOT_STALE_SEC`.
//! Both the watcher tick and the daemon's health publish tick are
//! `stale / 2`; there is no independent watchdog period setting.

use crate::cfg;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

/// Staleness threshold for `poll_ok_at`. A dead but still-running poller is
/// detected after at most this many seconds without a successful getUpdates.
pub const DEFAULT_STALE_SEC: u64 = 90;

/// Stable prefix of every alert line. Operators and log scrapers match it.
pub const ALERT_PREFIX: &str = "ALERT";

static POLL_OK_AT: AtomicI64 = AtomicI64::new(0);
static LAST_MESSAGE_ID: AtomicI64 = AtomicI64::new(0);
static DUPLICATES: AtomicU64 = AtomicU64::new(0);
static INGEST_FAILURES: AtomicU64 = AtomicU64::new(0);

pub fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// The single configured threshold. Values below 2s would make the derived
/// tick a busy loop, so they are clamped; that is a validity floor, not a
/// second tuning knob.
pub fn stale_threshold_secs() -> u64 {
    cfg("CHATBOT_STALE_SEC", &DEFAULT_STALE_SEC.to_string())
        .parse()
        .unwrap_or(DEFAULT_STALE_SEC)
        .max(2)
}

/// Watchdog period derived from the SAME value as the threshold. Integer
/// division floors, so `period * 2 <= threshold` for every accepted value.
pub fn derive_period(stale: u64) -> u64 {
    (stale.max(2) / 2).max(1)
}

pub fn watchdog_period_secs() -> u64 {
    derive_period(stale_threshold_secs())
}

/// Systemd unit whose liveness decides `down`. Kept configurable because the
/// deployed unit name is an installation detail, not a bot property.
pub fn systemd_unit() -> String {
    cfg("CHATBOT_SYSTEMD_UNIT", "chat-bot.service")
}

/// `systemctl is-active` output. `unknown` (no systemd, missing binary) is
/// treated as not-live by [`derive_verdict`]: a watcher that cannot prove the
/// unit is running must not report `up`.
pub fn unit_state(unit: &str) -> String {
    match std::process::Command::new("systemctl")
        .args(["is-active", unit])
        .output()
    {
        Ok(out) => {
            let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if s.is_empty() {
                "unknown".to_string()
            } else {
                s
            }
        }
        Err(_) => "unknown".to_string(),
    }
}

fn unit_live(state: &str) -> bool {
    matches!(state, "active" | "activating" | "reloading")
}

// ---------- facts ----------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct HostSocketFacts {
    pub path: String,
    /// `listening | missing | stale_path | not_socket`.
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct SizeFacts {
    pub media_bytes: u64,
    /// history.db plus its WAL/SHM sidecars: the space the database really
    /// holds, not just the main file.
    pub db_bytes: u64,
    pub log_bytes: u64,
    pub free_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct IngestFacts {
    /// Highest Telegram `message_id` observed by ingest.
    pub last_message_id: i64,
    /// Cumulative duplicates reported by committed pages.
    pub duplicates: u64,
    /// Cumulative poll or page-commit failures since this process started.
    pub failures: u64,
}

/// Facts only. `written_at` is the write time, never a health verdict; the
/// watcher derives freshness from `poll_ok_at`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HealthFacts {
    pub written_at: i64,
    pub poll_ok_at: Option<i64>,
    pub queue_depth: usize,
    pub host_socket: HostSocketFacts,
    pub sessions_named: Option<usize>,
    pub sizes: SizeFacts,
    pub ingest: IngestFacts,
}

pub fn health_path(state: &str) -> PathBuf {
    Path::new(state).join("health.json")
}

/// Socket state without spawning anything: a path that exists but refuses a
/// connect is the `kill -9` leftover, not a live host.
pub fn socket_state(path: &str) -> &'static str {
    let p = Path::new(path);
    match std::fs::symlink_metadata(p) {
        Err(_) => "missing",
        Ok(m) => {
            if !m.file_type().is_socket() {
                return "not_socket";
            }
            if std::os::unix::net::UnixStream::connect(p).is_ok() {
                "listening"
            } else {
                "stale_path"
            }
        }
    }
}

pub fn mark_poll_ok() {
    POLL_OK_AT.store(now_secs(), Ordering::Relaxed);
}

/// Last successful long poll (unix seconds), `None` before the first one.
/// The staleness authority is `stale_threshold_secs()` — SECONDS, the same unit
/// `derive_verdict` uses, so the alert and the operator-facing verdict cannot
/// disagree.
pub fn poll_ok_at() -> Option<i64> {
    match POLL_OK_AT.load(Ordering::Relaxed) {
        0 => None,
        v => Some(v),
    }
}

/// Whether the long poll is stale, and what to tell the owner about it. Pure and
/// unit-tested: the alert must agree with `derive_verdict` at the boundary
/// (exactly at the threshold is still healthy, one second past is not).
pub fn poll_stale_decision(now: i64, last_ok: Option<i64>, stale_secs: i64) -> (bool, String) {
    match last_ok {
        Some(ts) => {
            let age = now.saturating_sub(ts);
            (
                age > stale_secs,
                format!("last successful getUpdates {age}s ago (threshold {stale_secs}s)"),
            )
        }
        None => (true, "no successful getUpdates yet".to_string()),
    }
}

pub fn record_message_id(id: i64) {
    if id > 0 {
        LAST_MESSAGE_ID.fetch_max(id, Ordering::Relaxed);
    }
}

pub fn record_duplicates(n: u64) {
    if n > 0 {
        DUPLICATES.fetch_add(n, Ordering::Relaxed);
    }
}

pub fn ingest_failure() {
    INGEST_FAILURES.fetch_add(1, Ordering::Relaxed);
}

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

fn sidecar_sum(base: &Path) -> u64 {
    let mut total = file_len(base);
    if let Some(name) = base.file_name().and_then(|n| n.to_str()) {
        let dir = base.parent().unwrap_or_else(|| Path::new("."));
        for suffix in ["-wal", "-shm"] {
            total = total.saturating_add(file_len(&dir.join(format!("{name}{suffix}"))));
        }
    }
    total
}

/// Collect the facts the daemon can observe without guessing: atomics filled
/// by ingest, file sizes on disk, and (when the caller could ask the host) the
/// count of sessions carrying the configured name.
pub fn collect_health(
    state: &str,
    sock: &str,
    queue_depth: usize,
    sessions_named: Option<usize>,
) -> HealthFacts {
    let base = Path::new(state);
    let db = base.join("history.db");
    let log = base.join("chatd.log");
    let log_prev = base.join("chatd.log.1");
    let media = crate::media::media_dir(state);
    HealthFacts {
        written_at: now_secs(),
        poll_ok_at: match POLL_OK_AT.load(Ordering::Relaxed) {
            0 => None,
            v => Some(v),
        },
        queue_depth,
        host_socket: HostSocketFacts {
            path: sock.to_string(),
            state: socket_state(sock).to_string(),
        },
        sessions_named,
        sizes: SizeFacts {
            media_bytes: crate::media::dir_usage(&media),
            db_bytes: sidecar_sum(&db),
            log_bytes: file_len(&log).saturating_add(file_len(&log_prev)),
            free_bytes: crate::media::free_bytes(base),
        },
        ingest: IngestFacts {
            last_message_id: LAST_MESSAGE_ID.load(Ordering::Relaxed),
            duplicates: DUPLICATES.load(Ordering::Relaxed),
            failures: INGEST_FAILURES.load(Ordering::Relaxed),
        },
    }
}

/// Atomic publication: readers never observe a half-written JSON file. A
/// failed rename leaves the previous facts in place, which is honest: they are
/// old, and the watcher will see an old `poll_ok_at`, not a fabricated one.
pub fn write_health(state: &str, facts: &HealthFacts) -> Result<()> {
    std::fs::create_dir_all(state).with_context(|| format!("create state dir {state}"))?;
    let path = health_path(state);
    let tmp = path.with_extension(format!("json.tmp.{}", std::process::id()));
    let bytes = serde_json::to_vec_pretty(facts)?;
    std::fs::write(&tmp, &bytes).with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, &path).with_context(|| format!("rename {}", path.display()))?;
    Ok(())
}

pub fn read_health(path: &Path) -> Result<HealthFacts> {
    let raw = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_slice(&raw).with_context(|| format!("parse {}", path.display()))
}

// ---------- verdict ----------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    /// One of `up | degraded | stale | down`.
    pub level: &'static str,
    pub reasons: Vec<String>,
}

impl Verdict {
    pub fn exit_code(&self) -> i32 {
        match self.level {
            "up" => 0,
            "degraded" => 1,
            "stale" => 2,
            _ => 3,
        }
    }
}

/// The watcher's contract. The unit comes first: an inactive unit is `down`
/// even if a recent `poll_ok_at` is still present in the file. A live unit with
/// a stale/absent poll is `stale` — the live-but-deaf case. Only a fresh poll
/// can be `up` or `degraded`.
pub fn derive_verdict(
    health: &HealthFacts,
    unit_state: &str,
    now: i64,
    stale: u64,
    reserve: u64,
) -> Verdict {
    if !unit_live(unit_state) {
        return Verdict {
            level: "down",
            reasons: vec![format!("systemd unit is {unit_state}")],
        };
    }
    let stale = stale.max(2);
    let age = health.poll_ok_at.map(|at| now.saturating_sub(at));
    match age {
        None => {
            return Verdict {
                level: "stale",
                reasons: vec!["no successful getUpdates recorded".to_string()],
            }
        }
        Some(age) if age > stale as i64 => {
            return Verdict {
                level: "stale",
                reasons: vec![format!(
                    "last successful getUpdates was {age}s ago (threshold {stale}s)"
                )],
            }
        }
        Some(_) => {}
    }
    let mut reasons = Vec::new();
    if health.host_socket.state != "listening" {
        reasons.push(format!(
            "host socket {} is {}",
            health.host_socket.path, health.host_socket.state
        ));
    }
    if let Some(n) = health.sessions_named {
        if n != 1 {
            reasons.push(format!(
                "{n} sessions carry the configured name (expected 1)"
            ));
        }
    }
    if health.sizes.free_bytes < reserve {
        reasons.push(format!(
            "free space {} B is below reserve {} B",
            health.sizes.free_bytes, reserve
        ));
    }
    if reasons.is_empty() {
        Verdict {
            level: "up",
            reasons: Vec::new(),
        }
    } else {
        Verdict {
            level: "degraded",
            reasons,
        }
    }
}

/// Missing facts are not "fresh": with a live unit they are `stale`, with a
/// dead unit `down`. The age of `health.json` itself is never consulted.
pub fn verdict_for(
    health: Option<&HealthFacts>,
    unit_state: &str,
    now: i64,
    stale: u64,
    reserve: u64,
) -> Verdict {
    match health {
        Some(h) => derive_verdict(h, unit_state, now, stale, reserve),
        None if !unit_live(unit_state) => Verdict {
            level: "down",
            reasons: vec![format!("systemd unit is {unit_state}")],
        },
        None => Verdict {
            level: "stale",
            reasons: vec!["health.json is missing or unreadable".to_string()],
        },
    }
}

// ---------- alerts ----------

/// A Telegram message is one line: collapse whitespace, drop JSON bodies (the
/// reader needs the fact, not the payload) and cut on a word boundary.
const ALERT_NOTIFY_MAX: usize = 200;
/// A bare cause (`db_failure` and friends) is suppressed while it keeps firing
/// and is CLOSED by expiry — that is what gives it a recovery line without
/// hooks in db.rs/ledger.rs. Transition causes (`alert_transition`) are never
/// TTL-closed: their owner is the false edge.
const ALERT_ACTIVE_TTL_SECS: i64 = 900;
/// How long one notifier call may take before it is killed and counted failed.
const NOTIFY_TIMEOUT_SECS: u64 = 10;
/// A failed notifier is retried by the next sweep, but not forever.
const NOTIFY_MAX_TRIES: u32 = 5;
/// Kinds that are EVENTS, not states: each occurrence is its own news and is
/// never suppressed. `send_unknown` fires per send, so TTL gifts nothing there.
const ALERT_EVENT_KINDS: &[&str] = &["send_unknown"];

fn alert_line(kind: &str, detail: &str) -> String {
    format!("{ALERT_PREFIX} {kind}: {detail}")
}

pub fn alert_notify_text(kind: &str, detail: &str) -> String {
    let mut collapsed = String::with_capacity(detail.len());
    let mut in_braces = false;
    for ch in detail.chars() {
        match ch {
            '{' => {
                in_braces = true;
                collapsed.push('{');
            }
            '}' if in_braces => {
                in_braces = false;
                collapsed.push('}');
            }
            _ if in_braces => {}
            '\n' | '\r' | '\t' => collapsed.push(' '),
            _ => collapsed.push(ch),
        }
    }
    let squeezed = collapsed.split_whitespace().collect::<Vec<_>>().join(" ");
    let body = if squeezed.chars().count() > ALERT_NOTIFY_MAX {
        let mut out = String::new();
        for word in squeezed.split(' ') {
            if out.chars().count() + word.chars().count() + 1 > ALERT_NOTIFY_MAX {
                break;
            }
            if !out.is_empty() {
                out.push(' ');
            }
            out.push_str(word);
        }
        if out.is_empty() {
            squeezed.chars().take(ALERT_NOTIFY_MAX).collect()
        } else {
            format!("{out}…")
        }
    } else {
        squeezed
    };
    alert_line(kind, &body)
}

fn gate() -> &'static Mutex<HashMap<String, bool>> {
    static GATE: OnceLock<Mutex<HashMap<String, bool>>> = OnceLock::new();
    GATE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn notify_state_path(state: &str) -> std::path::PathBuf {
    std::path::Path::new(state).join("notify-state.json")
}

/// Suppression marks that must survive a restart (`Restart=always` would
/// otherwise re-send the same line every 15 s during a crash loop) plus the
/// notifier lines that failed to go out.
#[derive(Default)]
struct NotifyState {
    /// cause key -> last time it was seen (unix seconds)
    seen: HashMap<String, i64>,
    /// cause key -> line still undelivered, with its attempt count
    pending: HashMap<String, (String, u32)>,
}

fn load_notify_state(state: &str) -> NotifyState {
    let path = notify_state_path(state);
    let Ok(text) = std::fs::read_to_string(&path) else {
        return NotifyState::default();
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
        return NotifyState::default();
    };
    let mut out = NotifyState::default();
    if let Some(map) = value.get("seen").and_then(|v| v.as_object()) {
        for (k, v) in map {
            if let Some(ts) = v.as_i64() {
                out.seen.insert(k.clone(), ts);
            }
        }
    }
    if let Some(map) = value.get("pending").and_then(|v| v.as_object()) {
        for (k, v) in map {
            let line = v.get("line").and_then(|x| x.as_str()).unwrap_or("");
            let tries = v.get("tries").and_then(|x| x.as_u64()).unwrap_or(0) as u32;
            if !line.is_empty() {
                out.pending.insert(k.clone(), (line.to_string(), tries));
            }
        }
    }
    out
}

fn save_notify_state(state: &str, st: &NotifyState) {
    let mut seen = serde_json::Map::new();
    for (k, v) in &st.seen {
        seen.insert(k.clone(), serde_json::json!(v));
    }
    let mut pending = serde_json::Map::new();
    for (k, (line, tries)) in &st.pending {
        pending.insert(k.clone(), serde_json::json!({"line": line, "tries": tries}));
    }
    let body = serde_json::json!({"seen": seen, "pending": pending}).to_string();
    let path = notify_state_path(state);
    let tmp = path.with_extension("json.tmp");
    // Best effort: an unwritable state dir must not stop the alert itself.
    if std::fs::write(&tmp, body).is_ok() {
        let _ = std::fs::rename(&tmp, &path);
    }
}

/// Run the notifier with a deadline and report HOW it ended. Today the code
/// spawns and forgets with stdio to /dev/null, so `rc!=0` is invisible.
fn run_notifier(cmd: &str, line: &str) -> Result<(), String> {
    use std::process::{Command, Stdio};
    let mut child = Command::new(cmd)
        .arg(line)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("spawn: {e}"))?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(NOTIFY_TIMEOUT_SECS);
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                return if status.success() {
                    Ok(())
                } else {
                    Err(format!("rc={}", status.code().unwrap_or(-1)))
                }
            }
            Ok(None) => {
                if std::time::Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err("timeout".to_string());
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            Err(e) => return Err(format!("wait: {e}")),
        }
    }
}

fn notify_line(state: &str, key: &str, line: &str) {
    let Ok(cmd) = std::env::var("CHATBOT_NOTIFY_CMD") else {
        return;
    };
    if cmd.trim().is_empty() {
        return;
    }
    match run_notifier(&cmd, line) {
        Ok(()) => {
            // The retry (or the first attempt) went out: the waiting line is done.
            let mut st = load_notify_state(state);
            if st.pending.remove(key).is_some() {
                save_notify_state(state, &st);
            }
        }
        Err(err) => {
            crate::dlog_at(state, &format!("notify_failed: {key} {err}"));
            let mut st = load_notify_state(state);
            let tries = st.pending.get(key).map(|(_, t)| *t).unwrap_or(0) + 1;
            if tries >= NOTIFY_MAX_TRIES {
                crate::dlog_at(
                    state,
                    &format!("notify_failed: {key} giving up after {tries} attempts"),
                );
                st.pending.remove(key);
            } else {
                st.pending
                    .insert(key.to_string(), (line.to_string(), tries));
            }
            save_notify_state(state, &st);
        }
    }
}

/// Emit one alert line through the daemon log and, if configured, the external
/// notifier (`CHATBOT_NOTIFY_CMD`, receiving ONE line). A failed notifier never
/// hides the log line and is retried by the next sweep.
pub fn alert(kind: &str, detail: &str) {
    alert_to(&crate::state_dir(), kind, detail);
}

pub fn alert_to(state: &str, kind: &str, detail: &str) {
    let line = alert_line(kind, detail);
    crate::dlog_at(state, &line);
    let now = now_secs();
    let mut st = load_notify_state(state);
    let seen = st.seen.get(kind).copied().unwrap_or(0);
    let is_event = ALERT_EVENT_KINDS.contains(&kind);
    // The cause is already live and has already been delivered: log only.
    // The expiry sweep is what eventually closes it and sends `recovered`.
    if !is_event && seen > 0 && now.saturating_sub(seen) < ALERT_ACTIVE_TTL_SECS {
        st.seen.insert(kind.to_string(), now);
        save_notify_state(state, &st);
        return;
    }
    st.seen.insert(kind.to_string(), now);
    save_notify_state(state, &st);
    let notify = alert_notify_text(kind, detail);
    let state_owned = state.to_string();
    let kind_owned = kind.to_string();
    // The notifier runs in its own thread: waiting inside the caller would
    // stall a health tick on a slow HTTP client.
    std::thread::spawn(move || notify_line(&state_owned, &kind_owned, &notify));
}

/// Persistent conditions (socket gone, disk below reserve, session mismatch,
/// held session, stale poll) alert once per transition: the escalation is owned
/// by the false→true edge and the recovery line by the true→false edge.
/// A TTL sweep never closes these — that would claim "recovered" while the
/// fault is still there, because a persistent cause does not re-alert.
pub fn alert_transition(kind: &str, active: bool, detail: &str) {
    alert_transition_to(&crate::state_dir(), kind, active, detail);
}

/// State-scoped form, so the contract can be exercised without racing the
/// process-wide `CHATBOT_STATE_DIR`.
pub fn alert_transition_to(state: &str, kind: &str, active: bool, detail: &str) {
    let was_active = {
        let mut g = gate().lock().unwrap_or_else(|e| e.into_inner());
        let was = g.get(kind).copied().unwrap_or(false);
        if active {
            g.insert(kind.to_string(), true);
        } else {
            g.remove(kind);
        }
        was
    };
    if active {
        if !was_active {
            alert_to(state, kind, detail);
        }
    } else if was_active {
        // Closing the cause also clears its suppression mark, so a NEW outage
        // of the same kind alerts immediately instead of waiting out the TTL.
        let mut st = load_notify_state(state);
        st.seen.remove(kind);
        save_notify_state(state, &st);
        let recovered = format!("{kind}:recovered");
        crate::dlog_at(state, &alert_line(&recovered, "back to normal"));
        let notify = alert_notify_text(&recovered, "back to normal");
        let key = recovered.clone();
        let state_owned = state.to_string();
        std::thread::spawn(move || notify_line(&state_owned, &key, &notify));
    }
}

/// One alert tick: close TTL-owned causes that stopped happening (emitting the
/// recovery line once) and retry notifier lines that failed. Transition causes
/// are skipped by construction — their owner is the false edge.
pub fn sweep_alerts(state: &str) {
    let now = now_secs();
    let active_transitions: Vec<String> = {
        let g = gate().lock().unwrap_or_else(|e| e.into_inner());
        g.iter()
            .filter(|(_, v)| **v)
            .map(|(k, _)| k.clone())
            .collect()
    };
    let mut st = load_notify_state(state);
    let expired: Vec<String> = st
        .seen
        .iter()
        .filter(|(k, ts)| {
            now.saturating_sub(**ts) >= ALERT_ACTIVE_TTL_SECS && !active_transitions.contains(k)
        })
        .map(|(k, _)| k.clone())
        .collect();
    for key in expired {
        st.seen.remove(&key);
        let kind = format!("{key}:recovered");
        let detail = format!("no recurrence within {ALERT_ACTIVE_TTL_SECS}s");
        crate::dlog_at(state, &alert_line(&kind, &detail));
        let notify = alert_notify_text(&kind, &detail);
        let state_owned = state.to_string();
        std::thread::spawn(move || notify_line(&state_owned, &kind, &notify));
    }
    let retries: Vec<(String, String)> = st
        .pending
        .iter()
        .map(|(k, (line, _))| (k.clone(), line.clone()))
        .collect();
    save_notify_state(state, &st);
    for (key, line) in retries {
        let state_owned = state.to_string();
        std::thread::spawn(move || notify_line(&state_owned, &key, &line));
    }
}

pub fn db_failure(context: &str, err: &anyhow::Error) {
    db_failure_to(&crate::state_dir(), context, err);
}

pub fn db_failure_to(state: &str, context: &str, err: &anyhow::Error) {
    alert_to(state, "db_failure", &format!("{context}: {err:#}"));
}

// ---------- daemon health unit ----------

/// One health tick: derive alert conditions from the same facts that are
/// published, then atomically replace `health.json`. The session count comes
/// from the host's own list (authoritative), not from the daemon's wish.
pub async fn health_tick(
    state: &str,
    sock: &str,
    media_tx: &tokio::sync::mpsc::Sender<crate::media::MediaJob>,
    session_name: &str,
    alerts: bool,
) {
    let sock_state = socket_state(sock);
    if alerts {
        alert_transition(
            "host_socket_gone",
            sock_state != "listening",
            &format!("{sock} is {sock_state}"),
        );

        let reserve = crate::media::FREE_RESERVE_BYTES;
        let free = crate::media::free_bytes(Path::new(state));
        alert_transition(
            "free_below_reserve",
            free < reserve,
            &format!("free {free} B below reserve {reserve} B"),
        );
    }

    let sessions_named = if sock_state == "listening" {
        match crate::omo_on(&["list"], std::time::Duration::from_secs(10), sock).await {
            Ok(v) => Some(crate::session::parse_sessions(&v, session_name).len()),
            Err(_) => None,
        }
    } else {
        None
    };
    if let Some(n) = sessions_named {
        if alerts {
            alert_transition(
                "session_mismatch",
                n != 1,
                &format!("{n} sessions named {session_name} (expected 1)"),
            );
        }
    }

    // A live process that stopped polling is mute without looking broken: the
    // unit is active, the socket is there, nothing runs. The rule is the one the
    // watchdog verdict already applies (seconds!), so both read the same truth.
    if alerts {
        let stale = stale_threshold_secs() as i64;
        let (active, detail) = poll_stale_decision(now_secs(), poll_ok_at(), stale);
        alert_transition("poll_stale", active, &detail);
    }
    // Close TTL-owned causes that stopped happening and retry notifier lines
    // that failed to leave the box. Transition causes are skipped by design.
    sweep_alerts(state);

    let depth = media_tx.max_capacity().saturating_sub(media_tx.capacity());
    let facts = collect_health(state, sock, depth, sessions_named);
    if let Err(e) = write_health(state, &facts) {
        crate::dlog_at(state, &format!("health: write failed: {e:#}"));
    }
}

pub async fn health_loop(
    state: String,
    media_tx: tokio::sync::mpsc::Sender<crate::media::MediaJob>,
    session_name: String,
) {
    let period = watchdog_period_secs();
    // Give the first long poll a chance to land before the first publish, so
    // a healthy start does not briefly advertise "no successful getUpdates".
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(period));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut first = true;
    loop {
        tick.tick().await;
        let sock = crate::host_socket();
        health_tick(&state, &sock, &media_tx, &session_name, !first).await;
        first = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_state(tag: &str) -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "chatd-ops-{tag}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn facts(poll: Option<i64>) -> HealthFacts {
        HealthFacts {
            written_at: now_secs(),
            poll_ok_at: poll,
            queue_depth: 0,
            host_socket: HostSocketFacts {
                path: "/tmp/host.sock".into(),
                state: "listening".into(),
            },
            sessions_named: Some(1),
            sizes: SizeFacts {
                media_bytes: 1,
                db_bytes: 2,
                log_bytes: 3,
                free_bytes: 10_000,
            },
            ingest: IngestFacts::default(),
        }
    }

    /// Verification 2: an old facts file is stale, a fresh one is up, and an
    /// inactive unit is down regardless of the file contents.
    #[test]
    fn health_file_old_poll_is_stale_fresh_is_up_inactive_is_down() {
        let dir = temp_state("verdict");
        let state = dir.to_string_lossy().to_string();
        let path = health_path(&state);
        let now = now_secs();
        let stale = 90u64;

        write_health(&state, &facts(Some(now - 3600))).unwrap();
        let old = read_health(&path).unwrap();
        assert_eq!(old.poll_ok_at, Some(now - 3600));
        assert_eq!(
            derive_verdict(&old, "active", now, stale, 5_000).level,
            "stale"
        );

        write_health(&state, &facts(Some(now - 5))).unwrap();
        let fresh = read_health(&path).unwrap();
        assert_eq!(
            derive_verdict(&fresh, "active", now, stale, 5_000).level,
            "up"
        );

        assert_eq!(
            derive_verdict(&fresh, "inactive", now, stale, 5_000).level,
            "down"
        );
        assert_eq!(
            derive_verdict(&fresh, "failed", now, stale, 5_000).level,
            "down"
        );

        // A stale poll on a live unit is stale, not down: the process is alive
        // but deaf. A fresh poll on a dead unit is still down.
        assert_eq!(
            derive_verdict(&old, "active", now, stale, 5_000).level,
            "stale"
        );
        assert_eq!(
            verdict_for(None, "active", now, stale, 5_000).level,
            "stale"
        );
        assert_eq!(
            verdict_for(None, "inactive", now, stale, 5_000).level,
            "down"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Verification 3: one configured number yields both threshold and period;
    /// the derived period never exceeds half the threshold.
    #[test]
    fn watchdog_period_is_derived_from_one_stale_threshold() {
        for stale in 2u64..=1000 {
            let period = derive_period(stale);
            assert!(
                period * 2 <= stale,
                "period {period} exceeds half of threshold {stale}"
            );
            assert!(period >= 1);
        }
        assert_eq!(derive_period(90), 45);
        assert_eq!(
            watchdog_period_secs(),
            derive_period(stale_threshold_secs())
        );
    }

    /// Verification 4: the alert path used by database failures writes the
    /// stable prefix into the daemon log.
    #[test]
    fn simulated_database_failure_emits_stable_alert_prefix() {
        let dir = temp_state("alert");
        let state = dir.to_string_lossy().to_string();
        let err = anyhow::anyhow!("simulated disk I/O error");
        db_failure_to(&state, "commit_page", &err);
        let log = std::fs::read_to_string(dir.join("chatd.log")).unwrap();
        assert!(
            log.lines()
                .any(|l| l.contains("ALERT db_failure: commit_page: simulated disk I/O error")),
            "alert line missing, log was: {log}"
        );
        assert!(alert_line("send_unknown", "x").starts_with("ALERT "));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn degraded_is_reported_only_with_a_fresh_poll() {
        let now = now_secs();
        let mut h = facts(Some(now - 1));
        h.host_socket.state = "missing".into();
        assert_eq!(
            derive_verdict(&h, "active", now, 90, 5_000).level,
            "degraded"
        );
        h.poll_ok_at = Some(now - 900);
        assert_eq!(derive_verdict(&h, "active", now, 90, 5_000).level, "stale");
    }
}
