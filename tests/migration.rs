//! Migration v2 identity guard: a missing/invalid chat id must abort before
//! `user_version` moves, and a valid one must preserve every row while
//! assigning that chat id. These drive the real binaries, so the refusal is
//! observed through the same startup path production uses.
use rusqlite::Connection;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

fn legacy_db(dir: &Path) -> PathBuf {
    let path = dir.join("history.db");
    let con = Connection::open(&path).unwrap();
    con.execute_batch(
        "CREATE TABLE messages(
           update_id INTEGER UNIQUE,
           message_id INTEGER,
           date INTEGER,
           user_id INTEGER,
           username TEXT,
           first_name TEXT,
           text TEXT,
           reply_to INTEGER,
           media_path TEXT,
           file_id TEXT,
           file_unique_id TEXT,
           media_kind TEXT
         );
         CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT);
         INSERT INTO messages VALUES (1,101,1000,7,'u','U','same words',NULL,'','','',NULL);
         INSERT INTO messages VALUES (2,102,1000,7,'u','U','same words',NULL,'','','',NULL);",
    )
    .unwrap();
    path
}

fn fixture(tag: &str) -> (PathBuf, PathBuf, PathBuf) {
    let dir = std::env::temp_dir().join(format!("chat-mig-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let token = dir.join("token");
    std::fs::write(&token, "1:test\n").unwrap();
    std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o600)).unwrap();
    let state = dir.join("state");
    std::fs::create_dir_all(state.join("media")).unwrap();
    legacy_db(&state);
    (dir, state, token)
}

fn user_version(state: &Path) -> i64 {
    let con = Connection::open(state.join("history.db")).unwrap();
    con.query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap()
}

fn rows(state: &Path) -> i64 {
    let con = Connection::open(state.join("history.db")).unwrap();
    con.query_row("SELECT COUNT(*) FROM messages", [], |r| r.get(0))
        .unwrap()
}

fn has_chat_column(state: &Path) -> bool {
    let con = Connection::open(state.join("history.db")).unwrap();
    let mut stmt = con.prepare("PRAGMA table_info(messages)").unwrap();
    let names: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(1))
        .unwrap()
        .flatten()
        .collect();
    names.iter().any(|n| n == "chat_id")
}

fn run_sync(state: &Path, token: &Path, chat: Option<&str>) -> std::process::Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_chatd"));
    cmd.arg("sync")
        .env("CHATBOT_STATE_DIR", state)
        .env("CHATBOT_TOKEN_FILE", token)
        .env("CHATBOT_API_BASE", "http://127.0.0.1:1")
        .env_remove("CHATBOT_CHAT_ID");
    if let Some(chat) = chat {
        cmd.env("CHATBOT_CHAT_ID", chat);
    }
    cmd.output().unwrap()
}

fn run_tg_chat(state: &Path, token: &Path, chat: Option<&str>) -> std::process::Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_tg-chat"));
    cmd.env("CHATBOT_STATE_DIR", state)
        .env("CHATBOT_TOKEN_FILE", token)
        .env_remove("CHATBOT_CHAT_ID");
    if let Some(chat) = chat {
        cmd.env("CHATBOT_CHAT_ID", chat);
    }
    cmd.stdin(std::process::Stdio::null()).output().unwrap()
}

#[test]
fn missing_empty_or_nonnumeric_chat_id_refuses_before_migrating() {
    for (tag, chat) in [
        ("missing", None),
        ("empty", Some("")),
        ("nonnumeric", Some("-100abc")),
        ("zero", Some("0")),
    ] {
        let (dir, state, token) = fixture(tag);
        let before = user_version(&state);
        let before_rows = rows(&state);
        assert_eq!(before, 0, "{tag}: fixture must start unmigrated");

        let out = run_sync(&state, &token, chat);
        assert!(
            !out.status.success(),
            "{tag}: sync exited 0 without a usable chat id"
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("CHATBOT_CHAT_ID"),
            "{tag}: error does not name the variable: {stderr}"
        );
        assert_eq!(user_version(&state), before, "{tag}: user_version moved");
        assert_eq!(rows(&state), before_rows, "{tag}: rows changed");
        assert!(
            !has_chat_column(&state),
            "{tag}: a refusing migration rewrote the messages schema"
        );

        // The MCP path must refuse the same way instead of serving a poisoned DB.
        let tg = run_tg_chat(&state, &token, chat);
        assert!(
            !tg.status.success(),
            "{tag}: tg-chat exited 0 on an unmigrated DB"
        );
        assert!(
            String::from_utf8_lossy(&tg.stderr).contains("CHATBOT_CHAT_ID"),
            "{tag}: tg-chat error does not name the variable"
        );
        assert_eq!(user_version(&state), before, "{tag}: tg-chat migrated");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[test]
fn valid_chat_id_migrates_and_keeps_every_row() {
    let (dir, state, token) = fixture("valid");
    let before_rows = rows(&state);
    let out = run_sync(&state, &token, Some("-1001234567890"));
    // The network call after migration fails (dead API base); the migration
    // itself is what this asserts.
    assert!(
        !out.status.success(),
        "a dead API base cannot make sync succeed"
    );
    assert_eq!(
        user_version(&state),
        6,
        "valid chat id did not migrate to v6"
    );
    assert_eq!(rows(&state), before_rows, "migration lost rows");
    let con = Connection::open(state.join("history.db")).unwrap();
    let wrong: i64 = con
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE chat_id IS NOT -1001234567890",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(wrong, 0, "rows did not receive the configured chat id");
    let _ = std::fs::remove_dir_all(&dir);
}
