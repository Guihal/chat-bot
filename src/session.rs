//! Session lifecycle: durable identity, the one-session invariant and the
//! duplicate repair. Closing is ALWAYS `close <durable id>`; a configured name
//! only ever selects candidates, never the close argument.
use crate::{cfg, dlog, omo_on};
use anyhow::{anyhow, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveSession {
    pub name: String,
    pub session_uid: String,
    pub generation: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionPlan {
    pub keep: Option<LiveSession>,
    pub close: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RepairReport {
    pub keep: Option<String>,
    pub closed: Vec<String>,
    pub duplicates: usize,
}

/// Read the sessions the host reports, keeping only the configured name.
pub fn parse_sessions(data: &Value, name: &str) -> Vec<LiveSession> {
    let mut out = Vec::new();
    for s in data
        .get("sessions")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
    {
        if s.get("name").and_then(|v| v.as_str()) != Some(name) {
            continue;
        }
        let uid = s
            .get("durableSessionId")
            .or_else(|| s.get("sessionId"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if uid.is_empty() {
            // No durable id: this instance cannot be addressed or closed, so
            // it cannot be part of the invariant either.
            continue;
        }
        out.push(LiveSession {
            name: name.to_string(),
            session_uid: uid,
            generation: s.get("generation").and_then(|v| v.as_i64()),
        });
    }
    out
}

/// The invariant: exactly one session carries the configured name. Picks the
/// survivor and lists every other durable id to close.
///
/// Survivor order: an explicitly recorded live id (the session the daemon has
/// been delivering into), else the highest generation, else the first listed.
/// Duplicates are counted before any close so the caller can log the repair.
pub fn plan_single_session(
    sessions: &[LiveSession],
    name: &str,
    keep: Option<&str>,
) -> SessionPlan {
    let candidates: Vec<&LiveSession> = sessions.iter().filter(|s| s.name == name).collect();
    if candidates.is_empty() {
        return SessionPlan {
            keep: None,
            close: Vec::new(),
        };
    }
    let survivor = keep
        .and_then(|uid| candidates.iter().copied().find(|s| s.session_uid == uid))
        .or_else(|| {
            candidates
                .iter()
                .copied()
                .max_by_key(|s| s.generation.unwrap_or(i64::MIN))
        })
        .or_else(|| candidates.first().copied());
    let keep_uid = survivor.map(|s| s.session_uid.clone());
    let close = candidates
        .iter()
        .filter(|s| Some(&s.session_uid) != keep_uid.as_ref())
        .map(|s| s.session_uid.clone())
        .collect();
    SessionPlan {
        keep: survivor.cloned(),
        close,
    }
}

/// What the host reports about a session's runnability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionHealth {
    pub paused: bool,
    /// Holds as `kind:key`, in host order, so logs stay diagnosable.
    pub holds: Vec<String>,
}

/// Parse `omo host session state` output.
pub fn parse_state(data: &Value) -> Option<SessionHealth> {
    let state = data.get("state")?;
    let paused = state
        .get("paused")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let mut holds = Vec::new();
    for h in state
        .get("holds")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
    {
        let kind = h.get("kind").and_then(|v| v.as_str()).unwrap_or("").trim();
        let key = h.get("key").and_then(|v| v.as_str()).unwrap_or("").trim();
        if !kind.is_empty() {
            holds.push(format!("{kind}:{key}"));
        }
    }
    Some(SessionHealth { paused, holds })
}

/// The ONLY decisive mute: a run parked on an interrupted delivery.
///
/// `unattended` alone is normal - every session without an attached client
/// carries it, and an external delivery still starts a turn under it. Healing
/// on `paused` or on `unattended` would restart healthy sessions.
pub fn is_mute(health: &SessionHealth) -> bool {
    health.paused
        && health
            .holds
            .iter()
            .any(|h| h == "session_hold:interrupted-run")
}

/// Should a mute session be healed on this tick?
///
/// Two confirmations (a single reading may be a transient), no healing over a
/// recent turn (that would throw a live run away), and a cooldown so repeated
/// heals cannot thrash the host. Kept pure so the matrix is testable without a
/// running daemon.
pub fn should_heal(strikes: u32, turn_recent: bool, ticks_since_heal: u32) -> bool {
    strikes >= 2 && !turn_recent && ticks_since_heal >= 5
}

/// Ask the host about the session. `None` means the host could not answer; that
/// is not evidence of a fault, so it must never trigger healing.
pub async fn health_on(uid: &str, sock: &str) -> Option<SessionHealth> {
    match omo_on(&["state", uid], Duration::from_secs(20), sock).await {
        Ok(v) => parse_state(&v),
        Err(_) => None,
    }
}

/// What the host says about the session the daemon believes it delivers to.
///
/// `Absent` and `Unknown` are deliberately distinct: only the host's own answer
/// may drive a session to be opened. A timeout is not evidence of absence, and
/// opening on it is how duplicate sessions with one name appear.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Addressee {
    /// The host lists this exact durable id.
    Present(String),
    /// The host answered and this id is not among its sessions.
    Absent,
    /// The host could not be asked (timeout, transport error).
    Unknown(String),
}

/// The host's answer plus whether an extension instance is attached to that
/// session. Both come from ONE `omo host session list` call: the host keeps
/// `attachments` per session, so "is anybody listening" is answered by the
/// authority instead of inferred from our own traffic (an idle extension sends
/// nothing, so traffic-based guessing would block real delivery).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Address {
    pub addressee: Addressee,
    pub attached: bool,
}

/// Ask the host for the addressability of `uid`. One call per invocation; the
/// caller decides the cadence, never this function.
pub async fn address_on(name: &str, sock: &str, uid: &str) -> Address {
    match omo_on(&["list"], Duration::from_secs(30), sock).await {
        Ok(data) => {
            let sessions = data
                .get("sessions")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            for s in &sessions {
                if s.get("name").and_then(|v| v.as_str()) != Some(name) {
                    continue;
                }
                let id = s
                    .get("durableSessionId")
                    .or_else(|| s.get("sessionId"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .trim();
                if !uid.is_empty() && id == uid {
                    let attached = s.get("attachments").and_then(|v| v.as_i64()).unwrap_or(0) > 0;
                    return Address {
                        addressee: Addressee::Present(id.to_string()),
                        attached,
                    };
                }
            }
            Address {
                addressee: Addressee::Absent,
                attached: false,
            }
        }
        Err(e) => Address {
            addressee: Addressee::Unknown(format!("{e:#}")),
            attached: false,
        },
    }
}

/// What the delivery path is allowed to do before it pushes a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    /// Push now: the host lists our durable id and an extension instance is attached.
    Push,
    /// The host said the session is gone: let the caller open it, bounded.
    EnsureSession,
    /// Nothing yet: keep the batch queued and retry on the next tick.
    Wait,
}

/// Standing rule for "can this batch be pushed".
///
/// Only the host's own answer decides. Presence of the durable id is enough to
/// push: whether an instance will consume the frame is proven by the runtime
/// accepting it, never by a flag (the live host reports `attachments: 0` for a
/// perfectly healthy session, so gating on it would mute delivery outright).
pub const RECONNECT_PAUSE_SECS: u64 = 5;

pub fn next_target(address: &Address) -> Target {
    match &address.addressee {
        Addressee::Unknown(_) => Target::Wait,
        Addressee::Absent => Target::EnsureSession,
        Addressee::Present(_) => Target::Push,
    }
}

/// May the host generation be restarted after a push that nobody consumed?
///
/// Only when an instance already proved it cannot serve (stale ctx), the
/// unattended stretch outlasted the extension's own reconnect pause, and no turn
/// is running - killing a live turn is data loss, which the design forbids.
pub fn rearm_allowed(dead_gate: bool, absent_secs: u64, turn_in_flight: bool) -> bool {
    dead_gate && absent_secs > RECONNECT_PAUSE_SECS && !turn_in_flight
}

/// Check and repair the invariant against a live host. Every close goes by
/// durable id; a failed close is logged and reported, never retried by name.
pub async fn repair_sessions_on(
    name: &str,
    sock: &str,
    keep: Option<&str>,
) -> Result<RepairReport> {
    let data = omo_on(&["list"], Duration::from_secs(30), sock).await?;
    let sessions = parse_sessions(&data, name);
    let plan = plan_single_session(&sessions, name, keep);
    let mut closed = Vec::new();
    for uid in &plan.close {
        match omo_on(&["close", uid], Duration::from_secs(30), sock).await {
            Ok(_) => closed.push(uid.clone()),
            Err(e) => dlog(&format!("session repair: close {uid} failed: {e:#}")),
        }
    }
    let report = RepairReport {
        duplicates: plan.close.len(),
        keep: plan.keep.map(|s| s.session_uid),
        closed,
    };
    if !report.closed.is_empty() {
        dlog(&format!(
            "session invariant: {name} had {} duplicates, closed by durable id: {:?}",
            report.closed.len(),
            report.closed
        ));
    }
    Ok(report)
}

/// Close every session the host reports, by durable id. Used for the scratch
/// summary host, where a leftover session of any name is a stray.
pub async fn close_all_sessions_on(sock: &str) -> Result<usize> {
    let data = omo_on(&["list"], Duration::from_secs(30), sock).await?;
    let mut n = 0;
    for s in data
        .get("sessions")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
    {
        let uid = s
            .get("durableSessionId")
            .or_else(|| s.get("sessionId"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if uid.is_empty() {
            continue;
        }
        let _ = omo_on(&["close", &uid], Duration::from_secs(30), sock).await;
        n += 1;
    }
    Ok(n)
}

/// Record the session the daemon is now live in and return its generation. A
/// new durable id bumps the generation; the same id keeps it.
pub fn record_live(con: &Connection, name: &str, uid: &str) -> Result<i64> {
    let current: Option<(Option<String>, Option<i64>)> = con
        .query_row(
            "SELECT session_uid, generation FROM sessions WHERE name=?",
            [name],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let now = chrono::Utc::now().timestamp();
    let generation = match current {
        Some((Some(old), Some(g))) if old == uid => g,
        Some((_, Some(g))) => g.saturating_add(1),
        _ => 1,
    };
    con.execute(
        "INSERT INTO sessions(name,session_uid,generation,state,updated_at) VALUES(?,?,?,'live',?)
         ON CONFLICT(name) DO UPDATE SET
           session_uid=excluded.session_uid,
           generation=excluded.generation,
           state='live',
           updated_at=excluded.updated_at",
        params![name, uid, generation, now],
    )?;
    Ok(generation)
}

pub fn mark_dead(con: &Connection, uid: &str) -> Result<()> {
    con.execute(
        "UPDATE sessions SET state='dead', updated_at=? WHERE session_uid=?",
        params![chrono::Utc::now().timestamp(), uid],
    )?;
    Ok(())
}

pub fn live_identity(con: &Connection) -> Result<Option<(String, i64)>> {
    let row: Option<(String, Option<i64>)> = con
        .query_row(
            "SELECT session_uid, generation FROM sessions WHERE state='live' ORDER BY generation DESC, id DESC LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    Ok(row.and_then(|(uid, generation)| {
        if uid.trim().is_empty() {
            None
        } else {
            Some((uid, generation.unwrap_or(0)))
        }
    }))
}

pub fn configured_name() -> String {
    cfg("CHATBOT_SESSION", "chat-participant")
}

pub async fn repair_configured_on(sock: &str, keep: Option<&str>) -> Result<RepairReport> {
    let name = configured_name();
    repair_sessions_on(&name, sock, keep).await
}

/// Error helper used by the daemon when a hello disagrees with the live
/// durable identity: the sender is a stale extension instance.
pub fn stale_hello(claimed: &str, live: &str) -> Result<()> {
    if claimed.is_empty() || claimed == live {
        Ok(())
    } else {
        Err(anyhow!(
            "stale extension instance: hello {claimed} != live {live}"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(name: &str, uid: &str, generation: Option<i64>) -> LiveSession {
        LiveSession {
            name: name.into(),
            session_uid: uid.into(),
            generation,
        }
    }

    #[test]
    fn one_of_two_sessions_with_the_same_name_survives() {
        let sessions = vec![
            s("chat-participant", "sid-old", Some(4)),
            s("chat-participant", "sid-new", Some(5)),
            s("other", "sid-other", Some(1)),
        ];
        let plan = plan_single_session(&sessions, "chat-participant", None);
        assert_eq!(
            plan.keep.as_ref().map(|s| s.session_uid.as_str()),
            Some("sid-new")
        );
        assert_eq!(plan.close, vec!["sid-old".to_string()]);
        assert!(!plan.close.iter().any(|id| id == "chat-participant"));
    }

    #[test]
    fn recorded_live_id_wins_over_generation() {
        let sessions = vec![
            s("chat-participant", "sid-a", Some(9)),
            s("chat-participant", "sid-b", Some(3)),
        ];
        let plan = plan_single_session(&sessions, "chat-participant", Some("sid-b"));
        assert_eq!(
            plan.keep.as_ref().map(|s| s.session_uid.as_str()),
            Some("sid-b")
        );
        assert_eq!(plan.close, vec!["sid-a".to_string()]);
    }

    #[test]
    fn empty_durable_id_cannot_be_part_of_the_invariant() {
        let data = serde_json::json!({"sessions": [
            {"name": "chat-participant", "durableSessionId": ""},
            {"name": "chat-participant", "durableSessionId": "sid-ok"},
        ]});
        let parsed = parse_sessions(&data, "chat-participant");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].session_uid, "sid-ok");
    }

    #[test]
    fn only_the_host_answer_decides_whether_a_batch_may_be_pushed() {
        let present = Address {
            addressee: Addressee::Present("sid-1".to_string()),
            attached: false,
        };
        // the live host reports attachments:0 for a healthy session, so a push
        // must not depend on that flag
        assert_eq!(next_target(&present), Target::Push);
        // host answered: the id is gone -> the caller opens it, bounded
        let absent = Address {
            addressee: Addressee::Absent,
            attached: false,
        };
        assert_eq!(next_target(&absent), Target::EnsureSession);
        // a timeout is not absence: never open a new session on it
        let unknown = Address {
            addressee: Addressee::Unknown("timeout".into()),
            attached: false,
        };
        assert_eq!(next_target(&unknown), Target::Wait);
    }

    #[test]
    fn heal_needs_two_confirmations_an_idle_agent_and_a_cooldown() {
        assert!(
            !should_heal(1, false, u32::MAX),
            "one reading is not enough"
        );
        assert!(!should_heal(2, true, u32::MAX), "never over a live turn");
        assert!(!should_heal(2, false, 4), "respect the cooldown");
        assert!(should_heal(2, false, 5));
        assert!(
            should_heal(2, false, u32::MAX),
            "the first heal is not throttled"
        );
    }

    #[test]
    fn only_an_interrupted_run_counts_as_mute() {
        let held = parse_state(&serde_json::json!({"state": {"paused": true, "holds": [
            {"kind": "session_hold", "key": "interrupted-run"},
            {"kind": "unattended", "key": "unattended"},
        ]}}))
        .unwrap();
        assert!(is_mute(&held));

        let unattended_only = parse_state(&serde_json::json!({"state": {"paused": true, "holds": [
            {"kind": "unattended", "key": "unattended"},
        ]}}))
        .unwrap();
        assert!(
            !is_mute(&unattended_only),
            "a client-less session is normal"
        );

        let healthy =
            parse_state(&serde_json::json!({"state": {"paused": false, "holds": []}})).unwrap();
        assert!(!is_mute(&healthy));
        assert_eq!(parse_state(&serde_json::json!({"action": "error"})), None);
    }

    #[test]
    fn rearm_needs_a_dead_gate_idleness_and_no_running_turn() {
        assert!(!rearm_allowed(false, 999, false));
        assert!(!rearm_allowed(true, 1, false));
        assert!(!rearm_allowed(true, 60, true));
        assert!(rearm_allowed(true, 60, false));
    }

    #[test]
    fn generation_bumps_only_for_a_new_durable_id() {
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let dir = std::env::temp_dir().join(format!("chat-sess-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let state = dir.to_string_lossy().to_string();
        let con = crate::db(&state).unwrap();
        assert_eq!(record_live(&con, "chat-participant", "sid-1").unwrap(), 1);
        assert_eq!(record_live(&con, "chat-participant", "sid-1").unwrap(), 1);
        assert_eq!(record_live(&con, "chat-participant", "sid-2").unwrap(), 2);
        assert_eq!(live_identity(&con).unwrap(), Some(("sid-2".to_string(), 2)));
        mark_dead(&con, "sid-2").unwrap();
        assert_eq!(live_identity(&con).unwrap(), None);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
