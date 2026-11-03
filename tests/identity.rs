//! Identity contract: `(chat_id, message_id)` is the only key. Author, date
//! and text are content, never identity, so two real messages that repeat the
//! same words both persist and a redelivered id does not.
use chat_bot::db::Db;
use chat_bot::*;
use serde_json::{json, Value};

fn message(message_id: i64, update_id: i64, text: &str) -> Value {
    json!({
        "_update_id": update_id,
        "message_id": message_id,
        "date": 1_700_000_000,
        "chat": {"id": -100},
        "from": {"id": 7, "username": "u", "first_name": "U"},
        "text": text
    })
}

fn cross_envelope(message_id: i64, update_id: i64, text: &str) -> Envelope {
    let mut m = message(message_id, update_id, text);
    m["_cross_chat"] = json!(true);
    envelope_from_message(&m, update_id, false, -100)
        .unwrap()
        .unwrap()
}

fn count(con: &rusqlite::Connection, text: &str) -> i64 {
    con.query_row("SELECT COUNT(*) FROM messages WHERE text=?", [text], |r| {
        r.get(0)
    })
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn identical_content_persists_and_same_id_is_one_row() {
    std::env::set_var("CHATBOT_CHAT_ID", "-100");
    let dir = std::env::temp_dir().join(format!("chat-identity-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let state = dir.to_string_lossy().to_string();
    let db = Db::open(&state).unwrap();

    let first = cross_envelope(101, 1, "same words");
    let second = cross_envelope(102, 2, "same words");
    let first_again = cross_envelope(101, 3, "same words");
    let page = db
        .write(move |tx| tx.commit_page(vec![first, second], 3))
        .await
        .unwrap();
    assert_eq!(page.inserted.len(), 2, "distinct message ids collapsed");

    let duplicate = db
        .write(move |tx| tx.commit_page(vec![first_again], 4))
        .await
        .unwrap();
    assert_eq!(duplicate.applied.inserted, 0);
    assert_eq!(duplicate.applied.duplicate, 1);

    let state_for_prep = state.clone();
    let mut cross = message(103, 4, "same words");
    cross["_cross_chat"] = json!(true);
    let prep = prepare_message(&cross, &state_for_prep, false)
        .await
        .unwrap()
        .unwrap();
    let stored = db
        .call(move |con| store_prepared(con, &prep, ""))
        .await
        .unwrap();
    assert_eq!(stored.applied.inserted, 1, "legacy path dropped by content");

    let rows = db.call(|con| Ok(count(con, "same words"))).await.unwrap();
    assert_eq!(rows, 3, "content was used as a second identity key");
    let _ = std::fs::remove_dir_all(&dir);
}
