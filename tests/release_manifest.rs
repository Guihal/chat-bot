//! Release-set contract: one hash covers every file, rollback restores the
//! previous set byte-for-byte, and the agent profile is re-provisioned from the
//! restored persona sources before the manifest hash is checked again.
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
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

fn manifest_rows(path: &Path) -> (String, BTreeMap<String, (String, String)>) {
    let text = std::fs::read_to_string(path).unwrap();
    let mut set_hash = String::new();
    let mut rows = BTreeMap::new();
    for line in text.lines() {
        if let Some(hash) = line.strip_prefix("set_hash ") {
            set_hash = hash.trim().to_string();
            continue;
        }
        let mut it = line.split('\t');
        match it.next().unwrap_or("") {
            "file" => {
                let label = it.next().unwrap().to_string();
                let rel = it.next().unwrap().to_string();
                let sha = it.next().unwrap().to_string();
                rows.insert(label, (rel, sha));
            }
            _ => {}
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
    let provision =
        std::fs::read(Path::new(env!("CARGO_MANIFEST_DIR")).join("provision-agent.sh")).unwrap();

    let prompt = b"system prompt v1\n";
    let persona = b"persona v1\n";
    let fixture: &[(&str, &[u8])] = &[
        ("target/release/chatd", b"chatd binary v1\n"),
        ("chat-ext.mjs", b"extension v1\n"),
        ("muse-spark-phase-strip.ts", b"phase strip v1\n"),
        ("system-prompt.md", prompt),
        ("bot-home/AGENTS.md", persona),
        ("bot-home/settings.json", b"{}\n"),
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
    assert_eq!(rows.len(), 10, "every release-set entry must be listed");
    for label in [
        "bin",
        "ext",
        "phase",
        "prompt",
        "persona",
        "agent_settings",
        "agent_mcp",
        "provision",
        "unit",
        "env",
    ] {
        assert!(rows.contains_key(label), "manifest misses {label}");
    }
    assert_eq!(rows["prompt"].1, hash_file(&root.join("system-prompt.md")));

    let mutated = [prompt.as_slice(), b"MUTATED\n"].concat();
    std::fs::write(root.join("system-prompt.md"), &mutated).unwrap();

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
        prompt,
        "rollback must restore byte-identical content"
    );
    assert_eq!(hash_file(&root.join("system-prompt.md")), rows["prompt"].1);

    assert_eq!(
        std::fs::read(agent.join("AGENTS.md")).unwrap(),
        persona,
        "agent profile must be re-provisioned from the restored persona"
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
    assert!(
        !mcp.contains("CHATBOT_TOKEN_FILE"),
        "agent mcp.json still exposes the Bot API token"
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

    for (label, (rel, _sha)) in &rows {
        let path = if rel.starts_with('/') {
            PathBuf::from(rel)
        } else {
            root.join(rel)
        };
        let original = std::fs::read(&path).unwrap();
        let mut changed = original.clone();
        changed.push(b'X');
        std::fs::write(&path, &changed).unwrap();
        let mf = dir.join(format!("mut-{label}.manifest"));
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
