//! Cycle 4 acceptance: the one-session invariant is repaired by durable id.
//!
//! The daemon shells out to `omo`; this test puts a stub `omo` first on PATH so
//! the close argument can be inspected byte for byte. Environment is
//! process-global, so this binary serialises on one mutex.
use chat_bot::session::{plan_single_session, repair_sessions_on, LiveSession};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Mutex;

static ENV_LOCK: Mutex<()> = Mutex::new(());

struct Stub {
    _guard: std::sync::MutexGuard<'static, ()>,
    dir: PathBuf,
    close_log: PathBuf,
}

impl Drop for Stub {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn stub_omo(tag: &str) -> Stub {
    let guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = std::env::temp_dir().join(format!("chat-session-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let close_log = dir.join("closed.txt");
    let script = dir.join("omo");
    let body = format!(
        "#!/bin/sh\n\
         case \"$3\" in\n\
           list) echo '{{\"sessions\":[{{\"name\":\"chat-participant\",\"durableSessionId\":\"sid-old\",\"generation\":1}},{{\"name\":\"chat-participant\",\"durableSessionId\":\"sid-new\",\"generation\":2}},{{\"name\":\"other\",\"durableSessionId\":\"sid-other\",\"generation\":9}}]}}' ;;\n\
           close) echo \"$4\" >> '{}' ; echo '{{}}' ;;\n\
           *) echo '{{}}' ;;\n\
         esac\n\
         exit 0\n",
        close_log.display()
    );
    std::fs::write(&script, body).unwrap();
    let mut perms = std::fs::metadata(&script).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script, perms).unwrap();
    let path = format!(
        "{}:{}",
        dir.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    std::env::set_var("PATH", path);
    Stub {
        _guard: guard,
        dir,
        close_log,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn two_sessions_with_one_name_leave_one_and_close_by_durable_id() {
    let stub = stub_omo("repair");
    let report = repair_sessions_on("chat-participant", "/tmp/ignored.sock", None)
        .await
        .unwrap();
    assert_eq!(report.keep.as_deref(), Some("sid-new"), "{report:?}");
    assert_eq!(report.closed, vec!["sid-old".to_string()], "{report:?}");
    assert_eq!(report.duplicates, 1);

    let closed = std::fs::read_to_string(&stub.close_log).unwrap_or_default();
    let args: Vec<&str> = closed
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();
    assert_eq!(
        args,
        vec!["sid-old"],
        "close was not called with the durable id"
    );
    assert!(
        !args.iter().any(|a| *a == "chat-participant"),
        "close was called with the session NAME: {args:?}"
    );
    assert!(
        !args.iter().any(|a| *a == "sid-other"),
        "a session of another name was closed: {args:?}"
    );
}

#[test]
fn an_explicit_live_uid_is_the_survivor() {
    let sessions = vec![
        LiveSession {
            name: "chat-participant".into(),
            session_uid: "sid-a".into(),
            generation: Some(9),
        },
        LiveSession {
            name: "chat-participant".into(),
            session_uid: "sid-b".into(),
            generation: Some(2),
        },
    ];
    let plan = plan_single_session(&sessions, "chat-participant", Some("sid-b"));
    assert_eq!(
        plan.keep.as_ref().map(|s| s.session_uid.as_str()),
        Some("sid-b")
    );
    assert_eq!(plan.close, vec!["sid-a".to_string()]);
}
