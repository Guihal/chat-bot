//! Deploy contract for the owner's Telegram wiring, proven with a stub that
//! reproduces `tg-notify`'s exit-2-without-credentials behaviour: the daemon
//! runs `CHATBOT_NOTIFY_CMD` with ONE argument, and a refusal must be visible
//! instead of quietly counting as a delivered alert.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use chat_bot::ops;

static ENV: Mutex<()> = Mutex::new(());

fn guard() -> MutexGuard<'static, ()> {
    ENV.lock().unwrap_or_else(|e| e.into_inner())
}

fn temp_dir(tag: &str) -> PathBuf {
    static SEQ: OnceLock<Mutex<u64>> = OnceLock::new();
    let seq = SEQ.get_or_init(|| Mutex::new(0));
    let n = {
        let mut g = seq.lock().unwrap_or_else(|e| e.into_inner());
        *g += 1;
        *g
    };
    let dir = std::env::temp_dir().join(format!("chatd-notify-{tag}-{}-{}", std::process::id(), n));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Exit 2 without credentials, exactly like the deployed notifier.
fn credential_notifier(dir: &Path) -> String {
    let delivered = dir.join("delivered.txt");
    let script = dir.join("tg-notify-stub.sh");
    let body = format!(
        "#!/bin/sh\n\
         if [ -z \"$TG_NOTIFY_ENV\" ] || [ ! -r \"$TG_NOTIFY_ENV\" ]; then exit 2; fi\n\
         printf '%s\\n' \"$1\" >> '{out}'\n",
        out = delivered.display()
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

fn delivered(dir: &Path) -> Vec<String> {
    std::fs::read_to_string(dir.join("delivered.txt"))
        .map(|t| t.lines().map(|l| l.to_string()).collect())
        .unwrap_or_default()
}

fn wait_delivered(dir: &Path, want: usize) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let got = delivered(dir);
        if got.len() >= want || Instant::now() >= deadline {
            return got;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn wait_log(dir: &Path, needle: &str) -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let text = std::fs::read_to_string(dir.join("chatd.log")).unwrap_or_default();
        if text.contains(needle) || Instant::now() >= deadline {
            return text.contains(needle);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn without_credentials_the_refusal_is_recorded_and_the_line_survives() {
    let _g = guard();
    let dir = temp_dir("nocreds");
    let cmd = credential_notifier(&dir);
    std::env::set_var("CHATBOT_NOTIFY_CMD", &cmd);
    std::env::remove_var("TG_NOTIFY_ENV");
    let state = dir.display().to_string();

    ops::alert_to(&state, "host_socket_gone", "socket missing");

    let recorded = wait_log(&dir, "notify_failed: host_socket_gone");
    std::env::remove_var("CHATBOT_NOTIFY_CMD");
    assert!(
        recorded,
        "a notifier without credentials must not look like a delivery"
    );
    assert!(
        delivered(&dir).is_empty(),
        "nothing may be reported as delivered"
    );
    let persisted = std::fs::read_to_string(dir.join("notify-state.json")).unwrap_or_default();
    assert!(
        persisted.contains("host_socket_gone"),
        "the undelivered alert is kept for a retry: {persisted}"
    );
}

#[test]
fn with_credentials_the_alert_reaches_the_notifier_as_one_line() {
    let _g = guard();
    let dir = temp_dir("creds");
    let cmd = credential_notifier(&dir);
    let env_file = dir.join("tg-notify.env");
    std::fs::write(&env_file, "TG_BOT_TOKEN=stub\nTG_CHAT_ID=1\n").unwrap();
    std::env::set_var("CHATBOT_NOTIFY_CMD", &cmd);
    std::env::set_var("TG_NOTIFY_ENV", &env_file);
    let state = dir.display().to_string();

    ops::alert_to(&state, "disk_low", "free 412 MiB\n{\"du\":[1,2]}");

    let got = wait_delivered(&dir, 1);
    std::env::remove_var("CHATBOT_NOTIFY_CMD");
    std::env::remove_var("TG_NOTIFY_ENV");
    assert_eq!(got.len(), 1, "one alert is one message: {got:?}");
    assert!(
        got[0].contains("disk_low: free 412 MiB"),
        "the news survives: {got:?}"
    );
    assert!(
        !got[0].contains('\n') && !got[0].contains("\"du\""),
        "the raw payload is stripped: {got:?}"
    );
}

#[test]
fn a_second_identical_outage_does_not_repeat_but_a_recovery_is_announced() {
    let _g = guard();
    let dir = temp_dir("cycle");
    let cmd = credential_notifier(&dir);
    let env_file = dir.join("tg-notify.env");
    std::fs::write(&env_file, "TG_BOT_TOKEN=stub\n").unwrap();
    std::env::set_var("CHATBOT_NOTIFY_CMD", &cmd);
    std::env::set_var("TG_NOTIFY_ENV", &env_file);
    let state = dir.display().to_string();

    ops::alert_transition_to(&state, "host_socket_gone", true, "socket missing");
    let _ = wait_delivered(&dir, 1);
    ops::alert_transition_to(&state, "host_socket_gone", true, "socket missing");
    std::thread::sleep(Duration::from_millis(200));
    let during = delivered(&dir);
    assert_eq!(during.len(), 1, "a live outage is not re-sent: {during:?}");

    ops::alert_transition_to(&state, "host_socket_gone", false, "");
    let after = wait_delivered(&dir, 2);
    std::env::remove_var("CHATBOT_NOTIFY_CMD");
    std::env::remove_var("TG_NOTIFY_ENV");
    assert!(
        after
            .iter()
            .any(|l| l.contains("host_socket_gone:recovered")),
        "recovery reaches the owner: {after:?}"
    );
}
