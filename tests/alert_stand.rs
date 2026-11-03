//! Process-level stand for the fatal startup path: the real binary, a real
//! notifier child, and an assertion on what the owner receives. A background
//! notifier thread dies on that path before the child starts, so only running
//! the binary can show it.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

fn stand_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("chatd-stand-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn notifier(dir: &Path) -> String {
    let out = dir.join("delivered.txt");
    let script = dir.join("notify.sh");
    std::fs::write(
        &script,
        format!("#!/bin/sh\nprintf '%s\\n' \"$1\" >> {}\n", out.display()),
    )
    .unwrap();
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

fn run_daemon(dir: &Path, cmd: &str) {
    let token = dir.join("token.env");
    std::fs::write(&token, "TELEGRAM_BOT_TOKEN=123:stub\n").unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_chatd"))
        .arg("run")
        .env("CHATBOT_STATE_DIR", dir)
        .env("CHATBOT_NOTIFY_CMD", cmd)
        .env("CHATBOT_TOKEN_FILE", &token)
        .env("CHATBOT_CHAT_ID", "-1004493415890")
        .env("OMO_CODING_AGENT_DIR", dir.join("agent"))
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("daemon must start");
    // history.db is a directory: the open fails at boot, the deadline only guards a hang.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut killer = child;
    loop {
        if let Ok(Some(_)) = killer.try_wait() {
            return;
        }
        if Instant::now() >= deadline {
            let _ = killer.kill();
            let _ = killer.wait();
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn a_fatal_startup_reaches_the_notifier_and_is_not_repeated_after_a_restart() {
    let dir = stand_dir("fatal");
    std::fs::create_dir_all(dir.join("history.db")).unwrap();
    let cmd = notifier(&dir);

    run_daemon(&dir, &cmd);
    let first = delivered(&dir);
    assert_eq!(
        first.len(),
        1,
        "the owner must hear about a bot that cannot open its database: {first:?}"
    );
    assert!(
        first[0].contains("db_failure"),
        "the message names the cause: {first:?}"
    );
    assert!(
        !first[0].contains('\n'),
        "one message is one line: {first:?}"
    );
    assert!(
        first[0].contains("unable to open database file"),
        "the message says what failed: {first:?}"
    );
    assert!(
        !first[0].contains('{') && first[0].chars().count() <= 220,
        "no JSON body and a readable length: {first:?}"
    );

    // Restart=always repeats this fatal condition; the second boot must be silent.
    run_daemon(&dir, &cmd);
    let second = delivered(&dir);
    assert_eq!(
        second.len(),
        1,
        "a crash loop is not a stream of messages: {second:?}"
    );

    let log = std::fs::read_to_string(dir.join("chatd.log")).unwrap_or_default();
    assert_eq!(
        log.matches("ALERT db_failure").count(),
        2,
        "both starts stay in the log: {log}"
    );
}
