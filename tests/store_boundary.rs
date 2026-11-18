//! The data-layer gate.
//!
//! Two independent claims:
//! 1. source shape — business modules contain no SQL and no sqlite handle,
//!    while the layer modules contain no side effect inside an actor job;
//! 2. behaviour — the same round trips the daemon performs work when driven
//!    only through [`chat_bot::store::Store`].
use chat_bot::media::{
    cleanup_media, MediaBudget, FREE_RESERVE_BYTES, MEDIA_CAP_BYTES, MEDIA_GRACE,
};
use chat_bot::store::Store;
use chat_bot::{Envelope, MessageId, MessageKey};
use std::collections::HashSet;
use std::path::Path;
use std::time::{Duration, SystemTime};

const BUSINESS: [&str; 11] = [
    "src/lib.rs",
    "src/media.rs",
    "src/context.rs",
    "src/catalog.rs",
    "src/send.rs",
    "src/session.rs",
    "src/favorites.rs",
    "src/knowledge.rs",
    "src/bin/chatd.rs",
    "src/bin/tg-chat.rs",
    "src/bin/botwatch.rs",
];

const LAYER: [&str; 3] = ["src/store.rs", "src/db.rs", "src/ledger.rs"];

fn source(file: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(file);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {file}: {e}"))
}

/// Cut in-module tests (they legitimately keep raw sqlite), then every marked
/// exemption block. What remains is the production text the rule is about.
fn production_text(file: &str) -> String {
    let text = source(file);
    let text = match text.find("#[cfg(test)]") {
        Some(i) => &text[..i],
        None => &text[..],
    };
    let text = strip_marked(text, "// boundary-skip:begin", "// boundary-skip:end");
    let text = strip_marked(&text, "// actor-io-allow:begin", "// actor-io-allow:end");
    strip_comment_lines(&text)
}

fn strip_comment_lines(text: &str) -> String {
    text.lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("
")
}

fn strip_marked(text: &str, begin: &str, end: &str) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some(b) = rest.find(begin) {
        out.push_str(&rest[..b]);
        let after = &rest[b..];
        match after.find(end) {
            Some(e) => rest = &after[e + end.len()..],
            None => return out,
        }
    }
    out.push_str(rest);
    out
}

#[test]
fn business_modules_carry_no_sql_and_no_connection() {
    let banned = [
        "&Connection",
        "&mut Connection",
        "&Transaction",
        "SELECT ",
        "INSERT ",
        "UPDATE ",
        "DELETE FROM",
        "CREATE TABLE",
        "ALTER TABLE",
    ];
    for file in BUSINESS {
        let text = production_text(file);
        for needle in banned {
            assert!(
                !text.contains(needle),
                "{file}: business module exposes `{needle}`; the data layer owns it"
            );
        }
    }
}

#[test]
fn actor_jobs_carry_no_side_effects() {
    let banned = ["fs::", "sleep", "api(", "alert(", "spawn"];
    for file in LAYER {
        let text = production_text(file);
        for needle in banned {
            assert!(
                !text.contains(needle),
                "{file}: layer code runs `{needle}` inside the actor; move it to the caller"
            );
        }
    }
}

fn envelope(chat: i64, message: i64, update: i64, text: &str) -> Envelope {
    Envelope {
        key: MessageKey {
            chat,
            message: MessageId(message),
        },
        update_id: update,
        date: 1_700_000_000 + update,
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
    let dir = std::env::temp_dir().join(format!("chat-boundary-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let state = dir.to_string_lossy().to_string();
    std::env::set_var("CHATBOT_CHAT_ID", "-100");
    let store = Store::open(&state).expect("store open");
    (state, store)
}

#[tokio::test(flavor = "current_thread")]
async fn store_round_trip_matches_the_daemon_reads() {
    let (state, store) = temp_store("roundtrip");
    let mut photo = envelope(-100, 2, 2, "[photo]");
    photo.media_kind = "photo".into();
    photo.file_id = "p2".into();
    photo.file_unique_id = "p2-u".into();
    photo.media_state = chat_bot::media::MediaState::Pending;
    photo.media_size = Some(10);
    let page = chat_bot::store::commit_page(
        &store,
        vec![envelope(-100, 1, 1, "hello"), photo],
        3,
    )
    .await
    .unwrap();
    assert_eq!(page.applied.inserted, 2);
    assert_eq!(page.applied.duplicate, 0);

    let recent = chat_bot::store::recent_rows_of(&store, 10).await.unwrap();
    assert_eq!(recent.len(), 2, "recent window did not see both rows");

    let ids = vec![serde_json::json!(1i64), serde_json::json!(2i64)];
    let rows = chat_bot::store::batch_rows_of(&store, ids).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|r| r.message_id == 1 && r.text == "hello"));

    let answered = HashSet::new();
    let snapshot = chat_bot::store::context_snapshot(&store, rows.clone(), answered)
        .await
        .unwrap();
    let built = chat_bot::context::build_from(snapshot);
    assert!(built.text.contains("hello"), "context lost the row: {}", built.text);
    assert_eq!(built.ids, vec![1, 2]);

    let pending = chat_bot::store::pending_jobs_of(&store, 16).await.unwrap();
    assert_eq!(pending.len(), 1, "{pending:?}");
    assert_eq!(pending[0].file_id, "p2");

    let row_id = page.inserted.iter().find(|r| r.key.message.0 == 2).unwrap().row_id;
    assert!(
        chat_bot::store::finalize_blob_of(&store, row_id, "/s/p2.jpg".into(), "photo".into(), 10)
            .await
            .unwrap()
    );
    assert!(
        !chat_bot::store::finalize_blob_of(&store, row_id, "/s/p2.jpg".into(), "photo".into(), 10)
            .await
            .unwrap(),
        "a finalized row was resurrected through the pending transition"
    );
    assert!(
        chat_bot::store::pending_jobs_of(&store, 16).await.unwrap().is_empty(),
        "a finalized row stayed in the pending queue"
    );

    let _ = std::fs::remove_dir_all(&state);
}

#[tokio::test(flavor = "current_thread")]
async fn thumb_finalize_is_pending_only() {
    let (state, store) = temp_store("thumb");
    let mut video = envelope(-100, 5, 5, "[video]");
    video.media_kind = "video".into();
    video.file_id = "v5".into();
    video.file_unique_id = "v5-u".into();
    video.media_state = chat_bot::media::MediaState::Descriptor;
    video.media_size = Some(5000);
    video.thumb_file_id = "t5".into();
    video.thumb_state = chat_bot::media::MediaState::Pending;
    let page = chat_bot::store::commit_page(&store, vec![video], 6)
        .await
        .unwrap();
    let row_id = page.inserted[0].row_id;
    let jobs = chat_bot::store::pending_thumbs_of(&store, 16).await.unwrap();
    assert_eq!(jobs.len(), 1);
    assert!(
        chat_bot::store::finalize_thumb_of(&store, row_id, "/s/t5.jpg".into(), 800)
            .await
            .unwrap()
    );
    assert!(
        !chat_bot::store::finalize_thumb_of(&store, row_id, "/s/t5.jpg".into(), 800)
            .await
            .unwrap(),
        "a finalized frame was finalized twice"
    );
    let _ = std::fs::remove_dir_all(&state);
}

#[tokio::test(flavor = "current_thread")]
async fn catalogue_page_reads_through_the_layer() {
    let (state, store) = temp_store("catalog");
    let mut gif = envelope(-100, 7, 7, "[gif]");
    gif.media_kind = "animation".into();
    gif.file_id = "g7".into();
    gif.file_unique_id = "g7-u".into();
    gif.media_state = chat_bot::media::MediaState::Descriptor;
    gif.media_size = Some(2048);
    chat_bot::store::commit_page(&store, vec![gif], 8)
        .await
        .unwrap();
    let (rows, total) = chat_bot::store::catalog_page_of(&store, 0, 20).await.unwrap();
    assert_eq!(total, 1);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].file_id, "g7");
    assert_eq!(rows[0].size, 2048);
    let _ = std::fs::remove_dir_all(&state);
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_evicts_bytes_but_keeps_the_catalogue_entry() {
    let (state, store) = temp_store("cleanup");
    let dir = chat_bot::media::media_dir(&state);
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("old.gif");
    std::fs::write(&file, vec![5u8; 2048]).unwrap();

    let mut gif = envelope(-100, 9, 9, "[gif]");
    gif.media_kind = "animation".into();
    gif.file_id = "g9".into();
    gif.file_unique_id = "g9-u".into();
    gif.media_path = file.to_string_lossy().to_string();
    gif.media_state = chat_bot::media::MediaState::Descriptor;
    gif.media_size = Some(2048);
    chat_bot::store::commit_page(&store, vec![gif], 10)
        .await
        .unwrap();
    let (before, _) = chat_bot::store::catalog_page_of(&store, 0, 5).await.unwrap();
    assert_eq!(before.len(), 1);

    let budget = MediaBudget {
        free: 10 * 1024 * 1024 * 1024,
        media_used: 2048,
        media_cap: MEDIA_CAP_BYTES,
        reserve: FREE_RESERVE_BYTES,
    };
    let now = SystemTime::now() + Duration::from_secs(3600);
    let report = cleanup_media(&store, &state, &budget, now, MEDIA_GRACE, 0)
        .await
        .unwrap();
    assert_eq!(report.blobs_evicted, 1, "{report:?}");
    assert!(!file.exists(), "the wipe left the bytes behind");
    let (after, _) = chat_bot::store::catalog_page_of(&store, 0, 5).await.unwrap();
    assert_eq!(after.len(), 1, "the catalogue lost its entry after the wipe");
    assert_eq!(after[0].size, 2048, "the row lost the known size");
    let _ = std::fs::remove_dir_all(&state);
}
