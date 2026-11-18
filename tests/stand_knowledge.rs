use chat_bot::knowledge::{self, Kind};
use chat_bot::store::{self, Store};
use chat_bot::{Envelope, MessageId, MessageKey};
use std::path::{Path, PathBuf};
use std::process::Command;

fn credentials() -> PathBuf {
    let path = std::env::var_os("CHATBOT_STAND_CREDS")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|home| Path::new(&home).join(".omo/agent"))
        })
        .expect("set CHATBOT_STAND_CREDS or HOME with an omo agent profile");
    for name in ["auth.json", "models.json"] {
        assert!(
            path.join(name).is_file(),
            "missing stand credential {}/{}",
            path.display(),
            name
        );
    }
    path
}

fn copy_profile(source: &Path, target: &Path) {
    std::fs::create_dir_all(target).unwrap();
    for name in ["auth.json", "models.json", "settings.json"] {
        let from = source.join(name);
        if from.is_file() {
            std::fs::copy(&from, target.join(name)).unwrap();
        }
    }
}

fn message(chat: i64, message_id: i64, update_id: i64, date: i64, text: &str) -> Envelope {
    Envelope {
        key: MessageKey {
            chat,
            message: MessageId(message_id),
        },
        update_id,
        date,
        user_id: Some(7),
        username: "stand_user".into(),
        first_name: "Stand".into(),
        text: text.into(),
        reply_to: None,
        thread_id: None,
        media_path: String::new(),
        file_id: String::new(),
        file_unique_id: String::new(),
        media_kind: String::new(),
        media_state: chat_bot::media::MediaState::Absent,
        media_size: None,
        thumb_file_id: String::new(),
        thumb_state: chat_bot::media::MediaState::Absent,
        source_chat: None,
        sender_chat: None,
        cross: false,
    }
}

fn run_knowledge(root: &Path, doc: &str, now: i64) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_chatd"));
    command
        .args(["knowledge", "--doc", doc, "--now", &now.to_string()])
        .env("CHATBOT_STATE_DIR", root.join("state"))
        .env("CHATBOT_CHAT_ID", "-100")
        .env("CHATBOT_CWD", root.as_os_str())
        .env("CHATBOT_AGENT_DIR", root.join("agent"))
        .env("CHATBOT_KNOWLEDGE_DIR", root.join("knowledge"))
        .env("CHATBOT_KNOWLEDGE_TMP", root.join("tmp"))
        .env(
            "CHATBOT_SYSTEM",
            Path::new(env!("CARGO_MANIFEST_DIR")).join("knowledge-prompt.md"),
        )
        .env("CHATBOT_KNOWLEDGE_UTC_OFFSET_MIN", "180")
        .env("CHATBOT_KNOWLEDGE_WINDOW_MIN", "60")
        .env("CHATBOT_KNOWLEDGE_MAX_MESSAGES", "600")
        .env("CHATBOT_KNOWLEDGE_CHUNK_CHARS", "40000");
    command.output().expect("run chatd knowledge")
}

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn per_document_knowledge_uses_the_real_model_and_dedupes() {
    let profile = credentials();
    let root = std::env::temp_dir().join(format!("chat-stand-knowledge-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("state")).unwrap();
    std::fs::create_dir_all(root.join("knowledge")).unwrap();
    std::fs::create_dir_all(root.join("tmp")).unwrap();
    copy_profile(&profile, &root.join("agent"));

    let assets = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/stand_assets/knowledge");
    for kind in knowledge::kinds() {
        std::fs::copy(
            assets.join(kind.file_name()),
            root.join("knowledge").join(kind.file_name()),
        )
        .unwrap();
    }
    let target = root.join("knowledge").join(Kind::Discourse.file_name());
    let prefix = "РУЧНОЙ-ПРЕФИКС";
    let suffix = "РУЧНОЙ-ХВОСТ-ДИСКУРС";
    let token = "кварц-7";
    let now = chrono::Utc::now().timestamp();
    let date = now - 100;

    std::env::set_var("CHATBOT_CHAT_ID", "-100");
    let store = Store::open(&root.join("state").to_string_lossy()).unwrap();
    let rows = vec![
        message(
            -100,
            1,
            1,
            date,
            "Вчера на стриме обсуждали релиз кварц-7: новая версия выходит 20 ноября.",
        ),
        message(
            -100,
            2,
            2,
            date,
            "Кварц-7 обещает поддержку старых устройств и работу без блокировок.",
        ),
    ];
    store::commit_page(&store, rows, 2).await.unwrap();
    drop(store);

    let siblings: Vec<(String, Vec<u8>)> = knowledge::kinds()
        .iter()
        .filter(|kind| **kind != Kind::Discourse)
        .map(|kind| {
            let bytes = std::fs::read(root.join("knowledge").join(kind.file_name())).unwrap();
            (kind.file_name().to_string(), bytes)
        })
        .collect();

    let first = run_knowledge(&root, "discourse", now);
    println!("first stdout:\n{}", String::from_utf8_lossy(&first.stdout));
    println!("first stderr:\n{}", String::from_utf8_lossy(&first.stderr));
    assert!(first.status.success(), "first knowledge run failed");

    let text = std::fs::read_to_string(&target).unwrap();
    assert!(text.contains(prefix), "outside prefix changed");
    assert!(text.contains(suffix), "outside suffix changed");
    assert!(text.contains(token), "the model did not read the seeded message");
    let managed = text
        .split("<!-- knowledge:begin -->")
        .nth(1)
        .unwrap_or("")
        .split("<!-- knowledge:end -->")
        .next()
        .unwrap_or("");
    assert!(
        managed.lines().any(|line| {
            let line = line.trim();
            !line.is_empty() && !line.starts_with("### ") && !line.starts_with("- обновлено:")
        }),
        "the model created an entry without a body"
    );
    for (name, bytes) in &siblings {
        assert_eq!(
            &std::fs::read(root.join("knowledge").join(name)).unwrap(),
            bytes,
            "sibling {name} changed"
        );
    }

    let store = Store::open(&root.join("state").to_string_lossy()).unwrap();
    let cursor = store::kv_get(&store, "knowledge:cursor:discourse", "").await.unwrap();
    assert_eq!(cursor, format!("{date}:2"));
    assert_eq!(
        store::kv_get(&store, "knowledge:cursor:dictionary", "").await.unwrap(),
        ""
    );
    drop(store);

    let second = run_knowledge(&root, "discourse", now);
    println!("second stdout:\n{}", String::from_utf8_lossy(&second.stdout));
    assert!(second.status.success(), "second knowledge run failed");
    assert!(
        String::from_utf8_lossy(&second.stdout).contains("NO_NEW"),
        "second run was not a no-op"
    );
    assert_eq!(std::fs::read(&target).unwrap(), text.as_bytes().to_vec());

    let dictionary_root = std::env::temp_dir().join(format!(
        "chat-stand-dictionary-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dictionary_root);
    std::fs::create_dir_all(dictionary_root.join("state")).unwrap();
    std::fs::create_dir_all(dictionary_root.join("knowledge")).unwrap();
    std::fs::create_dir_all(dictionary_root.join("tmp")).unwrap();
    copy_profile(&profile, &dictionary_root.join("agent"));
    let dictionary = dictionary_root
        .join("knowledge")
        .join(Kind::Dictionary.file_name());
    std::fs::copy(
        assets.join(Kind::Dictionary.file_name()),
        &dictionary,
    )
    .unwrap();
    let dictionary_before = std::fs::read(&dictionary).unwrap();
    let store = Store::open(&dictionary_root.join("state").to_string_lossy()).unwrap();
    store::commit_page(
        &store,
        vec![message(-100, 1, 1, date, "сегодня просто 12345")],
        1,
    )
    .await
    .unwrap();
    drop(store);
    let empty = run_knowledge(&dictionary_root, "dictionary", now);
    println!("dictionary stdout:\n{}", String::from_utf8_lossy(&empty.stdout));
    assert!(empty.status.success(), "dictionary knowledge run failed");
    assert_eq!(std::fs::read(&dictionary).unwrap(), dictionary_before);

    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&dictionary_root);
    println!("STAND_KNOWLEDGE_OK");
}
