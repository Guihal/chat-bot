//! Release-set contract: one hash covers every code file, rollback restores that
//! set byte-for-byte, and AGENT-OWNED files (system-prompt.md, the global
//! AGENTS.md, the workspace agents.md, knowledge) are deliberately outside the
//! set and must survive a deploy untouched.
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn tmp_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "chatd-release-{}-{}",
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

fn write(path: &Path, bytes: &[u8]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}

fn hash_file(path: &Path) -> String {
    let bytes = std::fs::read(path).unwrap();
    format!("{:x}", Sha256::digest(bytes))
}

fn run_release(args: &[&str]) -> Output {
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/deploy/release.sh");
    Command::new("sh")
        .arg(script)
        .args(args)
        .output()
        .expect("run release.sh")
}

fn assert_ok(out: &Output, what: &str) {
    assert!(
        out.status.success(),
        "{what} failed: status {:?}\nstdout: {}\nstderr: {}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Rows of the manifest, in file order and **not** collapsed by label: `bin` is carried by two
/// release files (`chatd`, `botwatch`), so a label-keyed map would silently drop one of them and
/// let a mutation of that file pass unnoticed.
fn manifest_rows(path: &Path) -> (String, Vec<(String, String, String)>) {
    let text = std::fs::read_to_string(path).unwrap();
    let mut set_hash = String::new();
    let mut rows = Vec::new();
    for line in text.lines() {
        if let Some(hash) = line.strip_prefix("set_hash ") {
            set_hash = hash.trim().to_string();
            continue;
        }
        let mut it = line.split('\t');
        if it.next().unwrap_or("") == "file" {
            rows.push((
                it.next().unwrap().to_string(),
                it.next().unwrap().to_string(),
                it.next().unwrap().to_string(),
            ));
        }
    }
    assert!(!set_hash.is_empty(), "manifest has no set_hash");
    (set_hash, rows)
}

fn manifest_set_hash(manifest: &Path) -> String {
    manifest_rows(manifest).0
}

#[test]
fn manifest_hash_covers_every_file_and_rollback_restores_set() {
    let dir = tmp_dir();
    let root = dir.join("root");
    let env = dir.join("chat-bot.env");
    let token = dir.join("token");
    let backup = dir.join("backup");
    let agent = dir.join("agent");
    write(&agent.join("extensions/md-inject.ts"), b"AGENT RULES\n");
    let provision =
        std::fs::read(Path::new(env!("CARGO_MANIFEST_DIR")).join("provision-agent.sh")).unwrap();

    let prompt = b"system prompt v1\nAGENT EDIT\n";
    let persona = b"persona v1\nAGENT EDIT\n";
    let fixture: &[(&str, &[u8])] = &[
        ("target/release/chatd", b"chatd binary v1\n"),
        ("target/release/botwatch", b"botwatch binary v1\n"),
        ("chat-ext.mjs", b"extension v1\n"),
        ("md-inject.ts", b"export default function () {}\n"),
        ("muse-spark-phase-strip.ts", b"phase strip v1\n"),
        ("chat-compactor/index.ts", b"export default function () {}\n"),
        ("chat-compactor/lib/rich-compaction/contracts/chat-sections.ts", b"export const x = 1;\n"),
        ("bot-home/compactors/rich.json", b"{\"enabled\":true}\n"),
        ("bot-home/compactors/rich.md", b"# contract v1"),
        ("chat-control-task.mjs", b"chat context extension v1\n"),
        ("system-prompt.md", prompt),
        ("bot-home/AGENTS.md", persona),
        (
            "bot-home/settings.json",
            b"{\"defaultThinkingLevel\":\"low\",\"disabledBuiltinExtensions\":[\"compaction\",\"ttsr\"]}\n",
        ),
        (
            "bot-home/extensions/default-min-temperature.ts",
            b"export default function () {}\n",
        ),
        (
            "bot-home/extensions/sampling-payload.ts",
            b"export function projectSampling() {}\n",
        ),
        (
            "bot-home/mcp.json",
            b"{\"code\":\"__CODE_DIR__\",\"state\":\"__STATE_DIR__\",\"chat\":\"__CHAT_ID__\"}\n",
        ),
        (
            "deploy/chat-bot.service",
            b"[Service]\nExecStart=chatd run\n",
        ),
    ];
    for (rel, bytes) in fixture {
        write(&root.join(rel), bytes);
    }
    write(&root.join("provision-agent.sh"), &provision);
    write(&token, b"1:test\n");
    write(
        &env,
        format!(
            "CHATBOT_TOKEN_FILE={}\nCHATBOT_CHAT_ID=-100TEST\nCHATBOT_STATE_DIR={}\n",
            token.display(),
            dir.join("state").display()
        )
        .as_bytes(),
    );

    let root_s = root.to_str().unwrap();
    let env_s = env.to_str().unwrap();
    let backup_s = backup.to_str().unwrap();
    let agent_s = agent.to_str().unwrap();

    let out = run_release(&[
        "backup",
        "--root",
        root_s,
        "--env-file",
        env_s,
        "--dest",
        backup_s,
        "--agent-dir",
        agent_s,
    ]);
    assert_ok(&out, "backup");

    let before = backup.join("before.manifest");
    let (set_hash, rows) = manifest_rows(&before);
    assert_eq!(
        rows.len(),
        16,
        "every code entry must be listed, including both binaries, the expanded compactor tree and the profile extensions"
    );
    let mut labels: Vec<&str> = rows.iter().map(|(l, _, _)| l.as_str()).collect();
    labels.sort_unstable();
    assert_eq!(
        labels,
        vec![
            "agent_ext",
            "agent_ext",
            "agent_mcp",
            "agent_settings",
            "bin",
            "bin",
            "compactor",
            "compactor",
            "compactor_cfg",
            "compactor_cfg",
            "control_task",
            "env",
            "ext",
            "phase",
            "provision",
            "unit"
        ],
        "manifest rows must cover the whole release set"
    );
    let rels: Vec<&str> = rows.iter().map(|(_, rel, _)| rel.as_str()).collect();
    assert!(
        rels.contains(&"target/release/chatd") && rels.contains(&"target/release/botwatch"),
        "both release binaries must be covered: {rels:?}"
    );
    assert!(
        rels.contains(&"chat-compactor/index.ts")
            && rels.contains(&"chat-compactor/lib/rich-compaction/contracts/chat-sections.ts"),
        "the compactor tree must be covered file by file, never as one directory row: {rels:?}"
    );
    assert!(!rels.contains(&"chat-compactor"), "a directory row leaked into the manifest");
    for owned in [
        "system-prompt.md",
        "bot-home/AGENTS.md",
        "agents.md",
        "md-inject.ts",
    ] {
        assert!(
            !rels.contains(&owned),
            "agent-owned {owned} must never be a release-set row"
        );
    }
    assert!(
        !backup.join("files/system-prompt.md").exists()
            && !backup.join("files/bot-home/AGENTS.md").exists()
            && !backup.join("files/md-inject.ts").exists(),
        "the backup must not contain agent-owned files it is not allowed to restore"
    );

    write(&root.join("system-prompt.md"), b"AGENT PROMPT EDITED\n");
    write(&root.join("bot-home/AGENTS.md"), b"AGENT GLOBAL NOTES\n");
    write(&root.join("agents.md"), b"AGENT WORKSPACE NOTES\n");
    write(&agent.join("AGENTS.md"), b"AGENT PROFILE NOTES\n");
    write(&agent.join("knowledge/Дискурс.md"), b"agent discourse\n");

    let out = run_release(&[
        "rollback",
        "--root",
        root_s,
        "--backup",
        backup_s,
        "--agent-dir",
        agent_s,
    ]);
    assert_ok(&out, "rollback");
    assert!(
        String::from_utf8_lossy(&out.stdout).contains(&format!("MANIFEST_OK {set_hash}")),
        "rollback post-check did not print the backup hash"
    );
    assert_eq!(
        std::fs::read(root.join("system-prompt.md")).unwrap(),
        b"AGENT PROMPT EDITED\n".to_vec(),
        "rollback must not touch the agent-owned prompt"
    );
    assert_eq!(
        std::fs::read(root.join("bot-home/AGENTS.md")).unwrap(),
        b"AGENT GLOBAL NOTES\n".to_vec(),
        "rollback must not touch the global AGENTS.md"
    );
    assert_eq!(
        std::fs::read(root.join("agents.md")).unwrap(),
        b"AGENT WORKSPACE NOTES\n".to_vec(),
        "rollback must not touch the workspace agents.md"
    );
    assert_eq!(
        std::fs::read(agent.join("AGENTS.md")).unwrap(),
        b"AGENT PROFILE NOTES\n".to_vec(),
        "provisioning must leave the live global AGENTS.md untouched"
    );
    assert_eq!(
        std::fs::read(agent.join("knowledge/Дискурс.md")).unwrap(),
        b"agent discourse\n".to_vec(),
        "provisioning must leave knowledge untouched"
    );
    assert_eq!(
        std::fs::read(agent.join("extensions/chat-control-task.js")).unwrap(),
        b"chat context extension v1\n".to_vec(),
        "the durable chat-context extension must be staged into the agent dir"
    );
    let mcp = std::fs::read_to_string(agent.join("mcp.json")).unwrap();
    assert!(
        mcp.contains("-100TEST"),
        "agent mcp.json was not provisioned"
    );
    assert!(
        !mcp.contains("__CHAT_ID__"),
        "placeholder left in agent mcp.json"
    );
    assert_eq!(
        std::fs::read(agent.join("extensions/md-inject.ts")).unwrap(),
        b"AGENT RULES\n".to_vec(),
        "provisioning must preserve an existing agent-authored md-inject rule block"
    );
    assert!(
        !mcp.contains("CHATBOT_TOKEN_FILE"),
        "agent mcp.json still exposes the Bot API token"
    );

    assert!(
        std::fs::read(agent.join("compactors/rich.md")).is_ok()
            && std::fs::read(agent.join("compactors/rich.json")).is_ok(),
        "the compactor contract must be provisioned into the agent dir"
    );
    let agent_settings = std::fs::read_to_string(agent.join("settings.json")).unwrap();
    assert!(
        agent_settings.contains("\"compaction\""),
        "builtin compaction must be disabled in the agent profile: {agent_settings}"
    );
    let post = dir.join("post.manifest");
    let out = run_release(&[
        "manifest",
        "--root",
        root_s,
        "--env-file",
        env_s,
        "--manifest",
        post.to_str().unwrap(),
    ]);
    assert_ok(&out, "post manifest");
    assert_eq!(
        manifest_set_hash(&post),
        set_hash,
        "post-rollback set hash must equal the backup manifest hash"
    );
    let out = run_release(&[
        "verify",
        "--root",
        root_s,
        "--env-file",
        env_s,
        "--manifest",
        before.to_str().unwrap(),
    ]);
    assert_ok(&out, "verify");

    for (index, (label, rel, _sha)) in rows.iter().enumerate() {
        let path = if rel.starts_with('/') {
            PathBuf::from(rel)
        } else {
            root.join(rel)
        };
        let original = std::fs::read(&path).unwrap();
        let mut changed = original.clone();
        changed.push(b'X');
        std::fs::write(&path, &changed).unwrap();
        let mf = dir.join(format!("mut-{index}-{label}.manifest"));
        let out = run_release(&[
            "manifest",
            "--root",
            root_s,
            "--env-file",
            env_s,
            "--manifest",
            mf.to_str().unwrap(),
        ]);
        assert_ok(&out, "mutated manifest");
        assert_ne!(
            manifest_set_hash(&mf),
            set_hash,
            "set hash must change when {label} changes"
        );
        std::fs::write(&path, &original).unwrap();
    }

    let _ = std::fs::remove_dir_all(&dir);
}
