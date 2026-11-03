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

fn alert_line(kind: &str, detail: &str) -> String {
    format!("{ALERT_PREFIX} {kind}: {detail}")
}

fn gate() -> &'static Mutex<HashMap<String, bool>> {
    static GATE: OnceLock<Mutex<HashMap<String, bool>>> = OnceLock::new();
    GATE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Emit one alert line through the daemon log and, if configured, the existing
/// external notifier. The repository has no notifier of its own, so the
/// deployed `tg-notify` (or any equivalent) is wired with `CHATBOT_NOTIFY_CMD`
/// and receives the full alert line as its single argument. A failed notifier
/// never hides the log line.
pub fn alert(kind: &str, detail: &str) {
    alert_to(&crate::state_dir(), kind, detail);
}

pub fn alert_to(state: &str, kind: &str, detail: &str) {
    let line = alert_line(kind, detail);
    crate::dlog_at(state, &line);
    if let Ok(cmd) = std::env::var("CHATBOT_NOTIFY_CMD") {
        if !cmd.trim().is_empty() {
            let _ = std::process::Command::new(cmd)
                .arg(&line)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn();
        }
    }
}

/// Persistent conditions (socket gone, disk below reserve, session mismatch)
/// alert once per transition instead of once per tick; recovery re-arms them.
pub fn alert_transition(kind: &str, active: bool, detail: &str) {
    let mut g = gate().lock().unwrap_or_else(|e| e.into_inner());
    if active {
        if !g.get(kind).copied().unwrap_or(false) {
            g.insert(kind.to_string(), true);
            drop(g);
            alert(kind, detail);
        }
    } else {
        g.remove(kind);
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
