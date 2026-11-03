use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use serde_json::{json, Value};

const NO_NEW: &str = "NO_NEW";
const HEALTHY_UNIT: [&str; 3] = ["active", "activating", "reloading"];
const STATE_KEYS: [&str; 1] = ["stalled"];
const EVENT_KINDS: [&str; 5] = [
    "admit_error",
    "control_stale",
    "control_stale_session",
    "chat_send_error",
    "run_failed",
];
const ERR_PATTERNS: [&str; 8] = [
    "flush failed",
    "Traceback",
    "read tail failed",
    "urgent flush failed",
    "keeping batch",
    "host re-ensure failed",
    "no turn_end",
    "not admitted",
];

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn cfg() -> Cfg {
    let state = env_or("BOTWATCH_STATE", "/opt/omo-chat/data/chat-state");
    Cfg {
        log: env_or("BOTWATCH_LOG", &format!("{state}/chatd.log")),
        events: env_or("BOTWATCH_EVENTS", &format!("{state}/events.jsonl")),
        mirror: env_or("BOTWATCH_MIRROR", &format!("{state}/history.db")),
        agent_log: env_or(
            "BOTWATCH_AGENT_LOG",
            "/opt/omo-chat/state/agent/logs/session.log",
        ),
        baseline: env_or(
            "BOTWATCH_BASELINE",
            "/root/bot-watchdog/state/baseline.json",
        ),
        window_min: env_or("BOTWATCH_SINCE_MIN", "120").parse().unwrap_or(120),
        stall_min: env_or("BOTWATCH_STALL_MIN", "20").parse().unwrap_or(20),
        bot_id: env_or("BOTWATCH_BOT_ID", "0").parse().unwrap_or(0),
        unit: env_or("BOTWATCH_UNIT", "omo-chat"),
        unit_cmd: std::env::var("BOTWATCH_UNIT_CMD").ok(),
        journal_cmd: std::env::var("BOTWATCH_JOURNAL_CMD").ok(),
    }
}

struct Cfg {
    log: String,
    events: String,
    mirror: String,
    agent_log: String,
    baseline: String,
    window_min: i64,
    stall_min: i64,
    bot_id: i64,
    unit: String,
    unit_cmd: Option<String>,
    journal_cmd: Option<String>,
}

fn run_capture(program: &str, args: &[&str], timeout: Duration) -> (i32, String) {
    let mut child = match Command::new(program)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return (-1, e.to_string()),
    };
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let mut out = String::new();
                if let Some(mut pipe) = child.stdout.take() {
                    let _ = std::io::Read::read_to_string(&mut pipe, &mut out);
                }
                return (status.code().unwrap_or(-1), out);
            }
            Ok(None) => {
                if std::time::Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return (-1, "timeout".to_string());
                }
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(e) => return (-1, e.to_string()),
        }
    }
}

fn unit_state(c: &Cfg) -> String {
    let (code, out) = match &c.unit_cmd {
        Some(cmd) => run_capture(cmd, &[], Duration::from_secs(15)),
        None => run_capture(
            "systemctl",
            &["is-active", &c.unit],
            Duration::from_secs(15),
        ),
    };
    let text = out.trim().to_string();
    if text.is_empty() {
        format!("unknown({code})")
    } else {
        text
    }
}

fn unit_restart(c: &Cfg) {
    match &c.unit_cmd {
        Some(cmd) => {
            let _ = run_capture(cmd, &["restart"], Duration::from_secs(90));
        }
        None => {
            let _ = run_capture("systemctl", &["restart", &c.unit], Duration::from_secs(90));
        }
    }
}

fn read_to_string(path: &str) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

fn iso_of(dt: chrono::DateTime<chrono::Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

fn parse_iso(text: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|dt| dt.with_timezone(&chrono::Utc))
}

fn events_in_window(c: &Cfg, since_ms: i64, now_ms: i64) -> BTreeMap<String, String> {
    let mut causes = BTreeMap::new();
    let Some(blob) = read_to_string(&c.events) else {
        return causes;
    };
    for line in blob.lines() {
        let Ok(rec) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let t = rec.get("t").and_then(|v| v.as_i64()).unwrap_or(0);
        if t < since_ms || t > now_ms + 1000 {
            continue;
        }
        let kind = rec.get("kind").and_then(|v| v.as_str()).unwrap_or("?");
        let key = if kind == "delivery_state" {
            if rec.get("state").and_then(|v| v.as_str()) != Some("failed") {
                continue;
            }
            "delivery_state:failed".to_string()
        } else if EVENT_KINDS.contains(&kind) {
            kind.to_string()
        } else {
            continue;
        };
        let detail = rec
            .get("error")
            .or_else(|| rec.get("detail"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .chars()
            .take(160)
            .collect::<String>();
        causes.entry(format!("events:{key}")).or_insert(detail);
    }
    causes
}

fn daemon_log_causes(c: &Cfg, since: chrono::DateTime<chrono::Utc>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let Some(blob) = read_to_string(&c.log) else {
        return out;
    };
    let mut alert = String::new();
    let mut notify_failed = String::new();
    let mut errors: Vec<String> = Vec::new();
    for line in blob.lines() {
        let stamp = line.chars().take(20).collect::<String>();
        let ts = match parse_iso(&stamp) {
            Some(ts) => ts,
            None => continue,
        };
        if ts < since {
            continue;
        }
        if line.contains("notify_failed") {
            notify_failed = line.trim().chars().take(200).collect();
        } else if line.contains("ALERT ") {
            alert = line.trim().chars().take(200).collect();
        }
        if ERR_PATTERNS.iter().any(|p| line.contains(p)) {
            errors.push(line.trim().chars().take(160).collect());
        }
    }
    // `ALERT ...` lines are already delivered to Telegram by the daemon itself
    // through CHATBOT_NOTIFY_CMD; forwarding them here would send one alert twice.
    let _ = alert;
    if !notify_failed.is_empty() {
        out.insert("notify_failed".to_string(), notify_failed);
    }
    if !errors.is_empty() {
        let head: Vec<String> = errors.iter().rev().take(3).rev().cloned().collect();
        out.insert("log_errors".to_string(), head.join(" | "));
    }
    out
}

fn journal_errors(c: &Cfg) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let window = format!("{} minutes ago", c.window_min);
    let (_, text) = match &c.journal_cmd {
        Some(cmd) => run_capture(cmd, &[], Duration::from_secs(30)),
        None => run_capture(
            "journalctl",
            &[
                "-u",
                &format!("{}.service", c.unit),
                "--no-pager",
                "--since",
                &window,
            ],
            Duration::from_secs(30),
        ),
    };
    let hits: Vec<String> = text
        .lines()
        .filter(|l| ERR_PATTERNS.iter().any(|p| l.contains(p)))
        .rev()
        .take(3)
        .map(|l| l.trim().chars().take(160).collect())
        .collect();
    if !hits.is_empty() {
        let mut ordered = hits;
        ordered.reverse();
        out.insert("journal_errors".to_string(), ordered.join(" | "));
    }
    out
}

fn provider_errors(c: &Cfg, since: chrono::DateTime<chrono::Utc>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let Some(blob) = read_to_string(&c.agent_log) else {
        return out;
    };
    let mut last = String::new();
    for line in blob.lines() {
        if !line.contains("\"provider_error\"") {
            continue;
        }
        let Ok(rec) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let ts = rec.get("ts").and_then(|v| v.as_str()).unwrap_or("");
        if let Some(parsed) = parse_iso(ts) {
            if parsed >= since {
                last = ts.to_string();
            }
        }
    }
    if !last.is_empty() {
        out.insert("provider_error".to_string(), last);
    }
    out
}

fn dialogue_stalled(c: &Cfg, now: chrono::DateTime<chrono::Utc>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let Ok(con) = rusqlite::Connection::open_with_flags(
        &c.mirror,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    ) else {
        return out;
    };
    let since = (now - chrono::Duration::minutes(c.stall_min)).timestamp();
    // Without an explicit BOTWATCH_BOT_ID, take the bot's own id from the mirror:
    // a zero here would count every message as inbound and invent a stall.
    let bot_id = if c.bot_id != 0 {
        c.bot_id
    } else {
        con.query_row("SELECT v FROM kv WHERE k='self_id'", [], |row| {
            row.get::<_, String>(0)
        })
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(0)
    };
    if bot_id == 0 {
        return out;
    }
    let inbound: i64 = con
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE date >= ?1 AND user_id <> ?2",
            rusqlite::params![since, bot_id],
            |row| row.get(0),
        )
        .unwrap_or(0);
    let answers: i64 = con
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE date >= ?1 AND user_id = ?2",
            rusqlite::params![since, bot_id],
            |row| row.get(0),
        )
        .unwrap_or(0);
    if inbound >= 3 && answers == 0 {
        out.insert("stalled".to_string(), format!("{inbound} incoming"));
    }
    out
}

fn load_json(path: &str) -> Value {
    read_to_string(path)
        .and_then(|t| serde_json::from_str::<Value>(&t).ok())
        .unwrap_or_else(|| json!({}))
}

fn save_json(path: &str, value: &Value) {
    let p = Path::new(path);
    if let Some(dir) = p.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    let tmp = p.with_extension("json.tmp");
    if let Ok(mut f) = std::fs::File::create(&tmp) {
        let _ = f.write_all(value.to_string().as_bytes());
        let _ = std::fs::rename(&tmp, p);
    }
}

fn collect(c: &Cfg, now: chrono::DateTime<chrono::Utc>) -> BTreeMap<String, String> {
    let mut facts = BTreeMap::new();
    let state = unit_state(c);
    facts.insert("service".to_string(), state.clone());
    if !HEALTHY_UNIT.contains(&state.as_str()) {
        unit_restart(c);
        let after = unit_state(c);
        if HEALTHY_UNIT.contains(&after.as_str()) {
            facts.insert(
                "heal_ok".to_string(),
                format!("daemon was {state}, restarted, now {after}"),
            );
        } else {
            facts.insert(
                "heal_failed".to_string(),
                format!("was {state}, restart attempted, still {after}"),
            );
        }
    }
    let since = now - chrono::Duration::minutes(c.window_min);
    let now_ms = now.timestamp_millis();
    facts.extend(events_in_window(c, since.timestamp_millis(), now_ms));
    facts.extend(daemon_log_causes(c, since));
    facts.extend(journal_errors(c));
    facts.extend(provider_errors(c, since));
    facts.extend(dialogue_stalled(c, now));
    facts
}

fn render(c: &Cfg, facts: &BTreeMap<String, String>, previous: &Value) -> (Vec<String>, Value) {
    let mut lines = Vec::new();
    let mut next = serde_json::Map::new();
    let state = facts
        .get("service")
        .cloned()
        .unwrap_or_else(|| "unknown".into());
    next.insert("service".to_string(), json!(state));
    let prev_service = previous
        .get("service")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if !HEALTHY_UNIT.contains(&state.as_str()) && prev_service != state {
        lines.push(format!("BOT DOWN: {} is {state}", c.unit));
    }
    if HEALTHY_UNIT.contains(&state.as_str())
        && !prev_service.is_empty()
        && !HEALTHY_UNIT.contains(&prev_service)
    {
        lines.push("HEALED: unit is active again".to_string());
    }
    for (key, detail) in facts {
        next.insert(key.clone(), json!(detail));
        if key == "service" || previous.get(key).is_some() {
            continue;
        }
        match key.as_str() {
            "heal_ok" => lines.push(format!("HEALED: {detail}")),
            "heal_failed" => lines.push(format!("DOWN PERSISTS: {detail}")),
            "notify_failed" => lines.push(format!("NOTIFIER BROKEN: {detail}")),
            "log_errors" => lines.push(format!("DAEMON LOG ERRORS: {detail}")),
            "journal_errors" => lines.push(format!("JOURNAL ERRORS: {detail}")),
            "provider_error" => lines.push(format!("PROVIDER_ERROR: {detail}")),
            "stalled" => lines.push(format!(
                "STALLED: {detail}, no answers in {} min",
                c.stall_min
            )),
            other if other.starts_with("events:") => {
                let kind = other.split_once(':').map(|(_, k)| k).unwrap_or(other);
                let suffix = if detail.is_empty() {
                    String::new()
                } else {
                    format!(": {detail}")
                };
                lines.push(format!("EVENTS {kind}{suffix}"));
            }
            _ => {}
        }
    }
    if facts.contains_key("heal_ok") {
        next.insert("service".to_string(), json!("active"));
    }
    if let Some(prev) = previous.as_object() {
        for key in prev.keys() {
            if key == "service" || facts.contains_key(key) || !STATE_KEYS.contains(&key.as_str()) {
                continue;
            }
            lines.push(format!("CLEARED: {key}"));
        }
    }
    (lines, Value::Object(next))
}

fn watchdog() -> i32 {
    let c = cfg();
    let now = chrono::Utc::now();
    let facts = collect(&c, now);
    let previous = load_json(&c.baseline);
    let (lines, next) = render(&c, &facts, &previous);
    save_json(&c.baseline, &next);
    if lines.is_empty() {
        println!("{NO_NEW}");
    } else {
        for line in lines {
            println!("{line}");
        }
    }
    0
}

fn send(text: &str) -> i32 {
    let home = home_dir();
    let cmd = env_or("LIVENESS_NOTIFY", &format!("{home}/.local/bin/tg-notify"));
    let env_file = env_or(
        "LIVENESS_NOTIFY_ENV",
        &format!("{home}/.config/tg-notify.env"),
    );
    if cmd.is_empty() {
        return 1;
    }
    let child = Command::new(&cmd)
        .arg(text)
        .env("TG_NOTIFY_ENV", env_file)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
    match child {
        Ok(mut child) => {
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            loop {
                match child.try_wait() {
                    Ok(Some(status)) => return status.code().unwrap_or(-1),
                    Ok(None) => {
                        if std::time::Instant::now() >= deadline {
                            let _ = child.kill();
                            let _ = child.wait();
                            return -1;
                        }
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    Err(_) => return -1,
                }
            }
        }
        Err(_) => -1,
    }
}

fn probe() -> bool {
    if let Ok(cmd) = std::env::var("LIVENESS_PROBE") {
        let (code, _) = run_capture(&cmd, &[], Duration::from_secs(25));
        return code == 0;
    }
    let Some(server) = std::env::var("LIVENESS_SERVER")
        .ok()
        .filter(|s| !s.trim().is_empty())
    else {
        eprintln!("LIVENESS_SERVER is not set (e.g. root@host); refusing to guess a target");
        return false;
    };
    let (code, _) = run_capture(
        "ssh",
        &[
            "-q",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "BatchMode=yes",
            &server,
            "true",
        ],
        Duration::from_secs(25),
    );
    code == 0
}

fn liveness() -> i32 {
    let state_path = env_or(
        "LIVENESS_STATE",
        &format!("{}/.local/state/omo-chat-liveness.json", home_dir()),
    );
    let confirm: i64 = env_or("LIVENESS_CONFIRM", "2").parse().unwrap_or(2);
    let server = env_or("LIVENESS_SERVER", "host");
    let is_up = probe();
    let mut state = load_json(&state_path);
    let mut delivered = true;
    let mut to_print: Vec<String> = Vec::new();

    if let Some(pending) = state.get("pending").cloned() {
        if pending.get("text").and_then(|v| v.as_str()).is_some() {
            let text = pending
                .get("text")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let kind = pending
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("down")
                .to_string();
            if send(&text) == 0 {
                state["reported"] = json!(kind);
                state.as_object_mut().map(|m| m.remove("pending"));
                to_print.push(text);
            } else {
                delivered = false;
            }
        }
    }

    let previous_streak = state.get("streak").and_then(|v| v.as_bool());
    let count = if previous_streak == Some(is_up) {
        state.get("count").and_then(|v| v.as_i64()).unwrap_or(0) + 1
    } else {
        1
    };
    state["streak"] = json!(is_up);
    state["count"] = json!(count);

    let reported = state
        .get("reported")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    if count >= confirm {
        let message = if !is_up && reported.as_deref() != Some("down") {
            Some((
                "down",
                format!("ХОСТ НЕДОСТУПЕН: {server} — бот на связи не отвечает"),
            ))
        } else if is_up && reported.as_deref() == Some("down") {
            Some(("up", format!("ХОСТ СНОВА НА СВЯЗИ: {server}")))
        } else {
            if is_up && reported.is_none() {
                state["reported"] = json!("up");
            }
            None
        };
        if let Some((kind, text)) = message {
            if state.get("pending").is_none() {
                if send(&text) == 0 {
                    state["reported"] = json!(kind);
                    to_print.push(text);
                } else {
                    state["pending"] = json!({"text": text, "kind": kind});
                    delivered = false;
                }
            }
        }
    }
    save_json(&state_path, &state);
    for line in to_print {
        println!("{line}");
    }
    if !delivered {
        eprintln!("notify failed, kept for the next run");
        return 1;
    }
    0
}

fn home_dir() -> String {
    std::env::var("HOME").unwrap_or_else(|_| "/root".to_string())
}

fn selftest() -> i32 {
    let dir = std::env::temp_dir().join(format!("botwatch-selftest-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut failures: Vec<String> = Vec::new();
    let mut check = |name: &str, ok: bool, detail: String| {
        if !ok {
            failures.push(format!("{name}: {detail}"));
        }
    };

    let unit_flag = dir.join("unit-state");
    std::fs::write(&unit_flag, "active").unwrap();
    let unit_stub = dir.join("unit.sh");
    std::fs::write(
        &unit_stub,
        format!(
            "#!/bin/sh\nif [ \"$1\" = restart ]; then echo active > '{f}'; exit 0; fi\ncat '{f}'\n",
            f = unit_flag.display()
        ),
    )
    .unwrap();
    let journal_stub = dir.join("journal.sh");
    std::fs::write(&journal_stub, "#!/bin/sh\nexit 0\n").unwrap();
    for script in [&unit_stub, &journal_stub] {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(script).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(script, perms).unwrap();
        }
    }
    std::env::set_var("BOTWATCH_STATE", dir.display().to_string());
    std::env::set_var("BOTWATCH_LOG", dir.join("chatd.log").display().to_string());
    std::env::set_var(
        "BOTWATCH_EVENTS",
        dir.join("events.jsonl").display().to_string(),
    );
    std::env::set_var(
        "BOTWATCH_AGENT_LOG",
        dir.join("session.log").display().to_string(),
    );
    std::env::set_var(
        "BOTWATCH_MIRROR",
        dir.join("history.db").display().to_string(),
    );
    std::env::set_var(
        "BOTWATCH_BASELINE",
        dir.join("baseline.json").display().to_string(),
    );
    std::env::set_var("BOTWATCH_UNIT_CMD", unit_stub.display().to_string());
    std::env::set_var("BOTWATCH_JOURNAL_CMD", journal_stub.display().to_string());

    let mut c = cfg();
    c.bot_id = 9137;
    c.unit_cmd = Some(unit_stub.display().to_string());
    c.journal_cmd = Some(journal_stub.display().to_string());

    let now = chrono::Utc::now();
    let now_ms = now.timestamp_millis();
    let iso = iso_of(now);

    std::fs::write(
        &c.events,
        format!(
            "{}\n",
            json!({"t": now_ms, "kind": "delivery_state", "state": "emitted"})
        ),
    )
    .unwrap();
    let previous = load_json(&c.baseline);
    let (lines, next) = render(&c, &collect(&c, now), &previous);
    check(
        "healthy delivery is silence",
        lines.is_empty(),
        format!("{lines:?}"),
    );
    save_json(&c.baseline, &next);

    {
        let mut fh = std::fs::OpenOptions::new()
            .append(true)
            .open(&c.events)
            .unwrap();
        let _ = writeln!(
            fh,
            "{}",
            json!({"t": now_ms, "kind": "delivery_state", "state": "failed", "error": "boom"})
        );
    }
    let previous = load_json(&c.baseline);
    let (lines, next) = render(&c, &collect(&c, now), &previous);
    check(
        "a failed delivery reports once",
        lines.len() == 1 && lines[0].contains("delivery_state:failed"),
        format!("{lines:?}"),
    );
    save_json(&c.baseline, &next);

    {
        let mut fh = std::fs::OpenOptions::new()
            .append(true)
            .open(&c.events)
            .unwrap();
        let _ = writeln!(
            fh,
            "{}",
            json!({"t": now_ms, "kind": "delivery_state", "state": "failed", "error": "boom"})
        );
    }
    let previous = load_json(&c.baseline);
    let (lines, next) = render(&c, &collect(&c, now), &previous);
    check(
        "a repeating failure is not re-sent",
        lines.is_empty(),
        format!("{lines:?}"),
    );
    save_json(&c.baseline, &next);

    {
        let mut fh = std::fs::OpenOptions::new()
            .append(true)
            .open(&c.events)
            .unwrap();
        let _ = writeln!(fh, "{}", json!({"t": now_ms, "kind": "chat_send_error"}));
    }
    let previous = load_json(&c.baseline);
    let (lines, next) = render(&c, &collect(&c, now), &previous);
    check(
        "a new cause reports",
        lines.len() == 1 && lines[0].contains("chat_send_error"),
        format!("{lines:?}"),
    );
    save_json(&c.baseline, &next);

    std::fs::write(&c.log, format!("{iso} ALERT db_failure: open: refused\n")).unwrap();
    let previous = load_json(&c.baseline);
    let (lines, next) = render(&c, &collect(&c, now), &previous);
    check(
        "a daemon alert is not duplicated here",
        !lines.iter().any(|l| l.contains("DAEMON ALERT")),
        format!("{lines:?}"),
    );
    save_json(&c.baseline, &next);

    {
        let mut fh = std::fs::OpenOptions::new()
            .append(true)
            .open(&c.log)
            .unwrap();
        let _ = writeln!(fh, "{iso} notify_failed: db_failure rc=2");
    }
    let previous = load_json(&c.baseline);
    let (lines, next) = render(&c, &collect(&c, now), &previous);
    check(
        "a broken notifier is surfaced",
        lines.iter().any(|l| l.contains("NOTIFIER BROKEN")),
        format!("{lines:?}"),
    );
    save_json(&c.baseline, &next);

    {
        let mut fh = std::fs::OpenOptions::new()
            .append(true)
            .open(&c.log)
            .unwrap();
        let _ = writeln!(fh, "{iso} keeping batch abc: tried 4 times");
    }
    let previous = load_json(&c.baseline);
    let (lines, next) = render(&c, &collect(&c, now), &previous);
    check(
        "ordinary log errors are surfaced",
        lines.iter().any(|l| l.contains("DAEMON LOG ERRORS")),
        format!("{lines:?}"),
    );
    save_json(&c.baseline, &next);

    std::fs::write(
        &c.agent_log,
        format!(
            "{}\n",
            json!({"ts": iso, "level": "error", "event": "provider_error"})
        ),
    )
    .unwrap();
    let previous = load_json(&c.baseline);
    let (lines, next) = render(&c, &collect(&c, now), &previous);
    check(
        "provider errors are surfaced",
        lines.iter().any(|l| l.contains("PROVIDER_ERROR")),
        format!("{lines:?}"),
    );
    save_json(&c.baseline, &next);

    {
        let mut fh = std::fs::OpenOptions::new()
            .append(true)
            .open(&c.agent_log)
            .unwrap();
        let _ = writeln!(
            fh,
            "{}",
            json!({"ts": iso, "level": "error", "event": "provider_error"})
        );
    }
    let previous = load_json(&c.baseline);
    let (lines, next) = render(&c, &collect(&c, now), &previous);
    check(
        "another provider error is not re-sent",
        lines.is_empty(),
        format!("{lines:?}"),
    );
    save_json(&c.baseline, &next);

    if let Ok(con) = rusqlite::Connection::open(&c.mirror) {
        let _ = con.execute(
            "CREATE TABLE messages (message_id INTEGER PRIMARY KEY, user_id INTEGER, date INTEGER)",
            [],
        );
        for (id, uid) in [(1, 501), (2, 502), (3, 503)] {
            let _ = con.execute(
                "INSERT INTO messages VALUES (?1, ?2, ?3)",
                rusqlite::params![id, uid, now.timestamp()],
            );
        }
    }
    let previous = load_json(&c.baseline);
    let (lines, next) = render(&c, &collect(&c, now), &previous);
    check(
        "a stalled dialogue is surfaced",
        lines.iter().any(|l| l.contains("STALLED")),
        format!("{lines:?}"),
    );
    save_json(&c.baseline, &next);

    if let Ok(con) = rusqlite::Connection::open(&c.mirror) {
        let _ = con.execute(
            "INSERT INTO messages VALUES (4, ?1, ?2)",
            rusqlite::params![c.bot_id, now.timestamp()],
        );
    }
    let previous = load_json(&c.baseline);
    let (lines, next) = render(&c, &collect(&c, now), &previous);
    check(
        "a healed stall is cleared",
        lines.iter().any(|l| l.contains("CLEARED: stalled")),
        format!("{lines:?}"),
    );
    save_json(&c.baseline, &next);

    let later = now + chrono::Duration::minutes(c.window_min + 5);
    let previous = load_json(&c.baseline);
    let (lines, _) = render(&c, &collect(&c, later), &previous);
    check(
        "ageing out of the window is not a recovery",
        lines.is_empty(),
        format!("{lines:?}"),
    );

    std::fs::write(&unit_flag, "failed").unwrap();
    c.unit_cmd = Some(unit_stub.display().to_string());
    let previous = load_json(&c.baseline);
    let (lines, next) = render(&c, &collect(&c, now), &previous);
    check(
        "a failed unit is restarted and reported",
        lines.iter().any(|l| l.contains("HEALED")),
        format!("{lines:?}"),
    );
    save_json(&c.baseline, &next);

    std::fs::write(&unit_flag, "active").unwrap();
    let previous = load_json(&c.baseline);
    let (lines, _) = render(&c, &collect(&c, now), &previous);
    check(
        "one outage yields one recovery, not two",
        !lines.iter().any(|l| l.contains("HEALED"))
            && !lines.iter().any(|l| l.contains("BOT DOWN")),
        format!("{lines:?}"),
    );

    std::fs::write(&c.events, "{not json\n").unwrap();
    let previous = load_json(&c.baseline);
    let (lines, _) = render(&c, &collect(&c, now), &previous);
    check(
        "a broken journal does not crash",
        lines.len() < 10,
        format!("{lines:?}"),
    );

    let _ = std::fs::remove_dir_all(&dir);
    if failures.is_empty() {
        println!("SELFTEST OK");
        0
    } else {
        for f in failures {
            println!("SELFTEST FAIL: {f}");
        }
        1
    }
}

fn main() {
    let mut mode = "watchdog".to_string();
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--selftest" => {
                std::process::exit(selftest());
            }
            "--mode" => {}
            other => mode = other.to_string(),
        }
    }
    if let Ok(explicit) = std::env::var("BOTWATCH_MODE") {
        mode = explicit;
    }
    let code = match mode.as_str() {
        "liveness" => liveness(),
        _ => watchdog(),
    };
    std::process::exit(code);
}
