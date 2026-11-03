//! Alert surface contract: one line per news item, a suppressed repeat while the
//! cause is still live, one recovery line when it stops, and a failed notifier
//! that is not mistaken for success. The tests drive the real code path against a
//! throwaway state dir and a stub notifier, asserting observable effects only.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use chat_bot::ops;

/// `CHATBOT_NOTIFY_CMD` is process-wide: the tests must not fight over it.
static ENV: Mutex<()> = Mutex::new(());

fn env_guard() -> MutexGuard<'static, ()> {
    ENV.lock().unwrap_or_else(|e| e.into_inner())
}

fn temp_state(tag: &str) -> PathBuf {
    static SEQ: OnceLock<Mutex<u64>> = OnceLock::new();
    let seq = SEQ.get_or_init(|| Mutex::new(0));
    let n = {
        let mut guard = seq.lock().unwrap_or_else(|e| e.into_inner());
        *guard += 1;
        *guard
    };
    let dir = std::env::temp_dir().join(format!("chatd-alert-{tag}-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn stub_notifier(dir: &Path, code: i32) -> String {
    let capture = dir.join("notify-calls.txt");
    let script = dir.join("notify.sh");
    let body = format!(
        "#!/bin/sh\nprintf '%s\\n' \"$1\" >> '{cap}'\nexit {code}\n",
        cap = capture.display()
    );
    std::fs::write(&script, body).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script, perms).unwrap();
    }
    script.display().to_string()
}

fn calls(dir: &Path) -> Vec<String> {
    let path = dir.join("notify-calls.txt");
    match std::fs::read_to_string(path) {
        Ok(text) => text.lines().map(|l| l.to_string()).collect(),
        Err(_) => Vec::new(),
    }
}

fn wait_calls(dir: &Path, want: usize) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let got = calls(dir);
        if got.len() >= want || Instant::now() >= deadline {
            return got;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn wait_pending_cleared(dir: &Path, key: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let text = std::fs::read_to_string(dir.join("notify-state.json")).unwrap_or_default();
        let cleared = !text.contains(&format!("\"{key}\":{{\"line\""));
        if cleared || Instant::now() >= deadline {
            return text;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn log_lines(dir: &Path) -> Vec<String> {
    std::fs::read_to_string(dir.join("chatd.log"))
        .unwrap_or_default()
        .lines()
        .map(|l| l.to_string())
        .collect()
}

fn settle() {
    std::thread::sleep(Duration::from_millis(150));
}

#[test]
fn the_notified_text_is_one_line_without_json_body() {
    let text = ops::alert_notify_text(
        "db_failure",
        "open: could not open database\n{\"code\":\"SQLITE_BUSY\",\"nested\":[1,2,3]}\nretry later",
    );
    assert!(!text.contains('\n'), "must be a single line: {text:?}");
    assert!(text.contains("db_failure: "), "{text:?}");
    assert!(
        !text.contains("SQLITE_BUSY"),
        "the JSON payload is noise for the reader: {text:?}"
    );
    assert!(text.contains("could not open database"), "{text:?}");
    assert!(text.contains("retry later"), "{text:?}");
}

#[test]
fn a_long_detail_is_cut_on_a_word_boundary() {
    let detail = "word ".repeat(200);
    let text = ops::alert_notify_text("db_failure", &detail);
    assert!(text.ends_with('…'), "cut marker expected: {text:?}");
    assert!(text.chars().count() <= 260, "bounded: {}", text.len());
    assert!(
        !text.contains("wor…"),
        "must not cut inside a word: {text:?}"
    );
}

#[test]
fn a_repeating_cause_is_delivered_once_and_logged_every_time() {
    let _guard = env_guard();
    let dir = temp_state("dedupe");
    let script = stub_notifier(&dir, 0);
    std::env::set_var("CHATBOT_NOTIFY_CMD", &script);
    let state = dir.display().to_string();

    ops::alert_to(&state, "host_socket_gone", "socket missing");
    let got = wait_calls(&dir, 1);
    ops::alert_to(&state, "host_socket_gone", "socket missing");
    settle();
    std::env::remove_var("CHATBOT_NOTIFY_CMD");

    assert_eq!(got.len(), 1, "one delivery per live cause: {got:?}");
    assert_eq!(calls(&dir).len(), 1, "no repeat delivery");
    let log = log_lines(&dir);
    let alert_lines = log.iter().filter(|l| l.contains("ALERT ")).count();
    assert_eq!(alert_lines, 2, "both occurrences stay in the log: {log:?}");
}

#[test]
fn an_expired_cause_reports_recovered_once_and_can_alert_again() {
    let _guard = env_guard();
    let dir = temp_state("ttl");
    let script = stub_notifier(&dir, 0);
    std::env::set_var("CHATBOT_NOTIFY_CMD", &script);
    let state = dir.display().to_string();

    let stale = chat_bot::ops::now_secs() - 10_000;
    std::fs::write(
        dir.join("notify-state.json"),
        format!("{{\"seen\":{{\"db_failure\":{stale}}},\"pending\":{{}}}}"),
    )
    .unwrap();
    ops::sweep_alerts(&state);
    let got = wait_calls(&dir, 1);
    assert!(
        got.iter().any(|l| l.contains("db_failure:recovered")),
        "recovery line expected: {got:?}"
    );
    settle();

    ops::alert_to(&state, "db_failure", "open: refused");
    let after = wait_calls(&dir, 2);
    std::env::remove_var("CHATBOT_NOTIFY_CMD");
    assert_eq!(after.len(), 2, "recurrence is delivered: {after:?}");
    assert!(
        after[1].contains("db_failure: open: refused"),
        "the fresh occurrence carries its detail: {after:?}"
    );
}

#[test]
fn a_live_transition_is_never_closed_by_the_sweep() {
    let _guard = env_guard();
    let dir = temp_state("trans");
    let script = stub_notifier(&dir, 0);
    std::env::set_var("CHATBOT_NOTIFY_CMD", &script);
    let state = dir.display().to_string();

    ops::alert_transition_to(&state, "disk_low", true, "free 100 MiB");
    let _ = wait_calls(&dir, 1);
    settle();
    std::fs::write(
        dir.join("notify-state.json"),
        format!(
            "{{\"seen\":{{\"disk_low\":{}}},\"pending\":{{}}}}",
            chat_bot::ops::now_secs() - 10_000
        ),
    )
    .unwrap();
    ops::sweep_alerts(&state);
    settle();
    let during = calls(&dir);
    assert!(
        !during.iter().any(|l| l.contains("recovered")),
        "a live cause must not be closed: {during:?}"
    );

    ops::alert_transition_to(&state, "disk_low", false, "");
    let closed = wait_calls(&dir, during.len() + 1);
    settle();
    ops::alert_transition_to(&state, "disk_low", false, "");
    settle();
    std::env::remove_var("CHATBOT_NOTIFY_CMD");

    let recovered: Vec<&String> = closed.iter().filter(|l| l.contains("recovered")).collect();
    assert_eq!(recovered.len(), 1, "exactly one recovery line: {closed:?}");
    assert_eq!(
        calls(&dir).len(),
        closed.len(),
        "a repeated false edge adds nothing"
    );
}

#[test]
fn a_failing_notifier_is_recorded_and_retried() {
    let _guard = env_guard();
    let dir = temp_state("rcfail");
    let failing = stub_notifier(&dir, 2);
    std::env::set_var("CHATBOT_NOTIFY_CMD", &failing);
    let state = dir.display().to_string();

    ops::alert_to(&state, "db_failure", "open: refused");
    let first = wait_calls(&dir, 1);
    assert_eq!(first.len(), 1);
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && !log_lines(&dir).iter().any(|l| l.contains("notify_failed"))
    {
        std::thread::sleep(Duration::from_millis(20));
    }
    let log = log_lines(&dir);
    assert!(
        log.iter()
            .any(|l| l.contains("notify_failed: db_failure rc=2")),
        "a failed delivery must be recorded: {log:?}"
    );
    let persisted = std::fs::read_to_string(dir.join("notify-state.json")).unwrap();
    assert!(
        persisted.contains("pending") && persisted.contains("db_failure"),
        "the undelivered line waits for a retry: {persisted}"
    );

    let healthy = stub_notifier(&dir, 0);
    std::env::set_var("CHATBOT_NOTIFY_CMD", &healthy);
    let before = calls(&dir).len();
    ops::sweep_alerts(&state);
    let retried = wait_calls(&dir, before + 1);
    std::env::remove_var("CHATBOT_NOTIFY_CMD");
    assert_eq!(retried.len(), before + 1, "the retry went out: {retried:?}");
    let persisted = wait_pending_cleared(&dir, "db_failure");
    assert!(
        persisted.contains("\"pending\":{}"),
        "a delivered line is no longer pending: {persisted}"
    );
}

#[test]
fn an_event_kind_is_delivered_every_time() {
    let _guard = env_guard();
    let dir = temp_state("event");
    let script = stub_notifier(&dir, 0);
    std::env::set_var("CHATBOT_NOTIFY_CMD", &script);
    let state = dir.display().to_string();

    ops::alert_to(&state, "send_unknown", "send 12 unknown");
    let _ = wait_calls(&dir, 1);
    ops::alert_to(&state, "send_unknown", "send 13 unknown");
    let got = wait_calls(&dir, 2);
    std::env::remove_var("CHATBOT_NOTIFY_CMD");

    assert_eq!(got.len(), 2, "events are not states: {got:?}");
}
