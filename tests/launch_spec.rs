//! Launch-spec contract for the chat compactor: the tree is staged inside the
//! spec directory (the host refuses paths outside it), the entry is declared
//! LAST so it answers `session_before_compact` after builtin compaction, a module
//! deleted from the repository never survives a restage, and the release hash
//! covers the tree plus the bot's compactor config/prompt — otherwise a prompt
//! edit would leave the running host on the previous generation.
use chat_bot::{stage_chat_compactor, write_launch_spec_with};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

static ENV_LOCK: Mutex<()> = Mutex::new(());

fn lock_env() -> MutexGuard<'static, ()> {
    ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn tmp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "chatd-launch-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A throwaway checkout: the chat extension, the compactor tree and an agent
/// profile with the compactor contract, so the test never edits the repository.
fn fixture(tag: &str) -> (PathBuf, PathBuf) {
    let root = tmp_dir(tag);
    let code = root.join("code");
    let agent = root.join("agent");
    std::fs::create_dir_all(&code).unwrap();
    std::fs::create_dir_all(agent.join("compactors")).unwrap();
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"));
    std::fs::copy(repo.join("chat-ext.mjs"), code.join("chat-ext.mjs")).unwrap();
    std::fs::copy(
        repo.join("muse-spark-phase-strip.ts"),
        code.join("muse-spark-phase-strip.ts"),
    )
    .unwrap();
    copy_tree(&repo.join("chat-compactor"), &code.join("chat-compactor"));
    for name in ["rich.json", "rich.md"] {
        std::fs::copy(
            repo.join("bot-home/compactors").join(name),
            agent.join("compactors").join(name),
        )
        .unwrap();
    }
    (code, agent)
}

fn copy_tree(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let to = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), to).unwrap();
        }
    }
}

#[test]
fn launch_spec_stages_the_compactor_tree_and_declares_it_last() {
    let _guard = lock_env();
    let (code, agent) = fixture("stage");
    let state = code.parent().unwrap().join("state");
    std::env::set_var("CHATBOT_CHAT_ID", "-1001234567890");
    std::env::set_var("CHATBOT_CWD", &code);
    std::env::set_var("CHATBOT_AGENT_DIR", &agent);

    // A module that no longer exists in the repository must not survive.
    let stale = state.join("chat-compactor/lib/dead-module.ts");
    std::fs::create_dir_all(stale.parent().unwrap()).unwrap();
    std::fs::write(&stale, b"export const dead = true;\n").unwrap();

    let spec_path = write_launch_spec_with(&state.to_string_lossy(), true, true).unwrap();
    let spec: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&spec_path).unwrap()).unwrap();
    let exts: Vec<&str> = spec["core"]["extensions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(
        exts,
        vec![
            "./chat-ext.mjs",
            "./muse-spark-phase-strip.ts",
            "./chat-compactor/index.ts"
        ]
    );

    let staged = state.join("chat-compactor");
    assert!(staged.join("index.ts").is_file());
    assert!(staged.join("lib/agent-dir.ts").is_file());
    assert!(staged.join("lib/rich-compaction/contracts/chat-sections.ts").is_file());
    assert!(staged.join("lib/rich-compaction/contracts/summary-contract.ts").is_file());
    assert!(!staged.join("lib/rich-compaction/host-analysis.ts").exists());
    assert!(!stale.exists(), "a deleted module survived the restage");

    // The summary host gets no extensions at all, so no compactor tree either.
    let summary_state = code.parent().unwrap().join("state-summary");
    let summary = write_launch_spec_with(&summary_state.to_string_lossy(), false, false).unwrap();
    let summary_spec: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&summary).unwrap()).unwrap();
    assert!(summary_spec["core"]["extensions"].as_array().unwrap().is_empty());
    assert!(!summary_state.join("chat-compactor").exists());

    std::env::remove_var("CHATBOT_CWD");
    std::env::remove_var("CHATBOT_AGENT_DIR");
    std::env::remove_var("CHATBOT_CHAT_ID");
    let _ = std::fs::remove_dir_all(code.parent().unwrap());
}

#[test]
fn release_hash_covers_the_tree_and_the_compactor_contract() {
    let _guard = lock_env();
    let (code, agent) = fixture("hash");
    std::env::set_var("CHATBOT_CWD", &code);
    std::env::set_var("CHATBOT_AGENT_DIR", &agent);

    let before = chat_bot::ext_hash().unwrap();
    assert_eq!(before.len(), 16);

    // A module edit must change the hash, or a deploy would keep the old host.
    let module = code.join("chat-compactor/lib/rich-compaction/contracts/chat-sections.ts");
    let mut body = std::fs::read_to_string(&module).unwrap();
    body.push_str("\n");
    std::fs::write(&module, body).unwrap();
    let after_module = chat_bot::ext_hash().unwrap();
    assert_ne!(before, after_module);

    // So must a prompt edit: the prompt is part of the shipped contract.
    let prompt = agent.join("compactors/rich.md");
    let mut prompt_body = std::fs::read_to_string(&prompt).unwrap();
    prompt_body.push_str("\n## Extra\n");
    std::fs::write(&prompt, prompt_body).unwrap();
    let after_prompt = chat_bot::ext_hash().unwrap();
    assert_ne!(after_module, after_prompt);

    // And the reload guard compares exactly this value against the recorded one.
    let state = code.parent().unwrap().join("state");
    std::fs::create_dir_all(&state).unwrap();
    chat_bot::store_host_ext_hash(&state.to_string_lossy(), &before);
    assert_eq!(
        chat_bot::stored_host_ext_hash(&state.to_string_lossy()).as_deref(),
        Some(before.as_str())
    );
    assert_ne!(
        chat_bot::stored_host_ext_hash(&state.to_string_lossy()).as_deref(),
        Some(after_prompt.as_str())
    );

    // An agent profile without the contract cannot be hashed as the shipped set.
    std::env::remove_var("CHATBOT_AGENT_DIR");
    let without_agent = chat_bot::ext_hash().unwrap();
    assert_ne!(without_agent, after_prompt);

    std::env::remove_var("CHATBOT_CWD");
    let _ = std::fs::remove_dir_all(code.parent().unwrap());
}

#[test]
fn staging_refuses_a_tree_without_an_entry() {
    let _guard = lock_env();
    let root = tmp_dir("noentry");
    let code = root.join("code");
    std::fs::create_dir_all(code.join("chat-compactor/lib")).unwrap();
    std::fs::write(code.join("chat-compactor/lib/x.ts"), b"export {};\n").unwrap();
    std::env::set_var("CHATBOT_CWD", &code);
    let state = root.join("state");
    let err = stage_chat_compactor(&state.to_string_lossy()).unwrap_err();
    assert!(
        err.to_string().contains("entry missing"),
        "unexpected error: {err}"
    );
    std::env::remove_var("CHATBOT_CWD");
    let _ = std::fs::remove_dir_all(&root);
}
