//! Daily knowledge run: the day window and the document contract.
//!
//! Two independent claims:
//! 1. the window reaches the whole day, including our own mirrored replies
//!    (negative `update_id`) and the tail of the day, paged by
//!    `(date, message_id)`;
//! 2. merging the same keys twice never grows the documents, and a record
//!    update rewrites the body instead of adding a second entry.
use chat_bot::knowledge::{self, Kind, Record};
use chat_bot::store::Store;
use chat_bot::{Envelope, MessageId, MessageKey};

fn envelope(chat: i64, message: i64, update: i64, date: i64, text: &str) -> Envelope {
    Envelope {
        key: MessageKey {
            chat,
            message: MessageId(message),
        },
        update_id: update,
        date,
        user_id: Some(7),
        username: "u".into(),
        first_name: "U".into(),
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

fn temp_store(tag: &str) -> (String, Store) {
    let dir = std::env::temp_dir().join(format!("chat-knowledge-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let state = dir.to_string_lossy().to_string();
    std::env::set_var("CHATBOT_CHAT_ID", "-100");
    let store = Store::open(&state).expect("store open");
    (state, store)
}

#[tokio::test(flavor = "current_thread")]
async fn window_reaches_own_replies_and_the_tail_of_the_day() {
    let (state, store) = temp_store("window");
    let day_from = 1_800_000_000i64;
    let day_to = day_from + 86_400;
    // Three rows: an inbound message, our own mirrored reply (negative
    // update_id, exactly how store::mirror_own_send writes it), and the last
    // message of the day. An empty-text row must not be counted.
    let rows = vec![
        envelope(-100, 10, 10, day_from + 10, "начало дня"),
        envelope(-100, 11, -11, day_from + 20, "наш ответ"),
        envelope(-100, 12, 12, day_from + 30, ""),
        envelope(-100, 13, 13, day_to - 5, "хвост дня"),
        envelope(-100, 9, 9, day_from - 10, "вчерашнее"),
    ];
    chat_bot::store::commit_page(&store, rows, 14).await.unwrap();

    let mut seen = vec![];
    let mut cursor = (0i64, 0i64);
    loop {
        let page = chat_bot::store::knowledge_window_of(&store, day_from, day_to, cursor.0, cursor.1, 1)
            .await
            .unwrap();
        if page.is_empty() {
            break;
        }
        for row in page {
            cursor = (row.1, row.0);
            seen.push(row.4.unwrap_or_default());
        }
    }
    assert_eq!(seen, vec!["начало дня", "наш ответ", "хвост дня"]);
    let _ = std::fs::remove_dir_all(&state);
}

#[tokio::test(flavor = "current_thread")]
async fn cursor_paging_resumes_at_the_tail_without_duplicates() {
    let (state, store) = temp_store("paging");
    let from = 1_800_000_000i64;
    let rows: Vec<_> = (10..15)
        .map(|i| envelope(-100, i, i, from + i, &format!("строка-{i}")))
        .collect();
    chat_bot::store::commit_page(&store, rows, 15)
        .await
        .unwrap();

    let mut cursor = (0i64, 0i64);
    let mut seen = vec![];
    loop {
        let page = chat_bot::store::knowledge_window_of(&store, from, from + 100, cursor.0, cursor.1, 2)
            .await
            .unwrap();
        if page.is_empty() {
            break;
        }
        for row in page {
            cursor = (row.1, row.0);
            seen.push(row.4.unwrap_or_default());
        }
    }
    assert_eq!(seen.len(), 5);
    assert_eq!(seen.first().unwrap(), "строка-10");
    assert_eq!(seen.last().unwrap(), "строка-14");
    assert_eq!(seen.iter().collect::<std::collections::HashSet<_>>().len(), 5);
    let _ = std::fs::remove_dir_all(&state);
}

#[tokio::test(flavor = "current_thread")]
async fn rerun_of_the_same_keys_does_not_grow_the_document() {
    let mut doc = knowledge::parse_managed(&knowledge::new_document(Kind::Persons));
    let mut first = Record {
        key: "Илон Маск".into(),
        text: "предприниматель".into(),
        sources: vec!["https://example.com/musk".into()],
    };
    first.sources.push("https://example.org/x".into());
    knowledge::merge(&mut doc, &[first.clone()], "2026-11-05");
    knowledge::merge(&mut doc, &[first], "2026-11-06");
    let text = knowledge::render(&doc, Kind::Persons);
    assert_eq!(text.matches("### ").count(), 1);
    assert!(text.contains("- обновлено: 2026-11-06"));
    assert!(text.contains("- источники: https://example.com/musk, https://example.org/x"));
    let reparsed = knowledge::parse_managed(&text);
    assert_eq!(reparsed.entries.len(), 1);
    assert_eq!(knowledge::render(&reparsed, Kind::Persons), text);
}
