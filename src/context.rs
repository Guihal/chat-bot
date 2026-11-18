//! The ONE context builder: `load(snapshot) -> assemble -> render_document`.
//!
//! A fresh batch, the rebuild after a compaction and the recovery after a
//! restart all call [`build`]. They differ only in how the batch rows arrive
//! (ingest, journal ids, journal ids + tome), never in how the document is
//! assembled or rendered, so "fresh and rebuilt say different things" is
//! structurally impossible.
use crate::catalog::{self, GifRecord};
use crate::{batch_sig, render_recent_context, BatchMsg, RecentRow};
use std::collections::{HashMap, HashSet};

pub const WINDOW_LIMIT: i64 = 50;
pub const WINDOW_CAP: usize = 300;
pub const REPLY_PARENT_CAP: usize = 120;
pub const GUARD_LIMIT: i64 = 5;
pub const WINDOW_SLACK: i64 = 5;

/// One reply parent, resolved by the message key directly. Only one hop is
/// ever resolved: the nearest link, never the chain behind it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ReplyParent {
    pub who: String,
    pub text: String,
    pub media: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReplyEdge {
    None,
    Resolved {
        parent: i64,
        who: String,
        excerpt: String,
    },
    Missing(i64),
}

#[derive(Debug, Clone, PartialEq)]
pub struct BatchBlock {
    pub message: BatchMsg,
    pub edge: ReplyEdge,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ContextDocument {
    pub context: String,
    pub guard: String,
    pub catalog: String,
    pub batch: Vec<BatchBlock>,
}

/// Everything the document needs, read in one pass before assembly.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub batch: Vec<BatchMsg>,
    pub window: Vec<crate::RecentRow>,
    pub parents: HashMap<i64, ReplyParent>,
    pub catalog: Vec<GifRecord>,
    pub answered: HashSet<i64>,
    pub guard: String,
    pub self_id: i64,
    /// Whether this document carries the history window. The batch, the guard
    /// and the catalogue are the same either way; the decision belongs to
    /// [`window_due`] and is applied by the caller.
    pub include_window: bool,
}

impl Snapshot {
    /// Same snapshot, different window decision. Used by the builder to render
    /// the document for a turn that already has its history.
    pub fn with_window(mut self, include: bool) -> Self {
        self.include_window = include;
        self
    }
}

/// `<durableSessionId>:<generation>` — the identity of one model context. A
/// new durable id or a bumped generation means the model starts from nothing
/// and needs the window again.
pub fn epoch_of(uid: &str, generation: i64) -> String {
    format!("{uid}:{generation}")
}

/// The owner's rule for the history window, as one pure predicate.
///
/// The window is due when a compaction asked for it (`force`), when the session
/// identity is unknown (safe side: the model may have no history at all), when
/// the identity differs from the one the window was last delivered for, or when
/// the very batch that carried the window is rebuilt — a rebuild must keep the
/// history it was prepared with, or a retry would silently strip it.
///
/// `recorded_batch` is the batch's LEDGER key (`sig`), not the hash of the text
/// being built: a rebuilt document changes with the guard and hydration lines,
/// yet it is the same batch.
///
/// Every other turn carries only the batch (plus guard and catalogue).
pub fn window_due(
    current: Option<&str>,
    recorded_epoch: &str,
    recorded_batch: &str,
    force: bool,
    batch_key: &str,
) -> bool {
    force || current.is_none() || current != Some(recorded_epoch) || batch_key == recorded_batch
}

/// The shape the daemon consumes. `text` is the only artifact either path
/// sends; the rest is journal bookkeeping derived from the same snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct BuiltContext {
    pub text: String,
    pub sig: String,
    pub ids: Vec<i64>,
    pub tome: Vec<i64>,
    pub anchor: i64,
}

/// Printed at the top of every batch: it is the instruction the model actually
/// reads each turn, so it must match the send contract. Replies are NOT
/// restricted to this batch (owner, 2026-11-08, twice) — a quote may name any
/// message the model can see, including the history block and the gif catalog.
const BATCH_HEADER: &str = "Пачка: сообщения этой доставки. Отвечаешь своими словами обычным сообщением — или не отвечаешь вовсе: это твоё решение, а не сбой. Чужое не цитируешь: ни реплаем, ни в кавычках, ни пересказом.";

pub(crate) fn display_who(username: &str, first_name: &str, user_id: Option<i64>) -> String {
    if !username.is_empty() {
        format!("@{username}")
    } else if !first_name.is_empty() {
        first_name.to_string()
    } else {
        user_id.map(|u| u.to_string()).unwrap_or_default()
    }
}

fn one_line_capped(text: &str, cap: usize) -> String {
    let joined = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let cut: String = joined.chars().take(cap).collect();
    if joined.chars().count() > cap {
        format!("{cut}…")
    } else {
        cut
    }
}

/// Message body as the model sees it. A sticker has no caption and no media
/// file, so its file_id is the only way to re-send it: print it next to the
/// `[sticker]` marker.
pub(crate) fn message_body(text: &str, media_kind: &str, file_id: &str) -> String {
    if media_kind == "sticker" && !file_id.is_empty() {
        if text.trim().is_empty() {
            format!("[sticker] file_id={file_id}")
        } else {
            format!("{text} file_id={file_id}")
        }
    } else {
        text.to_string()
    }
}

fn edge_for(message: &BatchMsg, parents: &HashMap<i64, ReplyParent>) -> ReplyEdge {
    let Some(parent) = message.reply_to else {
        return ReplyEdge::None;
    };
    match parents.get(&parent) {
        Some(p) => {
            let excerpt = if !p.text.trim().is_empty() {
                format!("«{}»", one_line_capped(&p.text, REPLY_PARENT_CAP))
            } else if p.media {
                "[file]".to_string()
            } else {
                "без текста".to_string()
            };
            ReplyEdge::Resolved {
                parent,
                who: p.who.trim().to_string(),
                excerpt,
            }
        }
        None => ReplyEdge::Missing(parent),
    }
}

/// The snapshot from data the data layer already read: the batch is filtered
/// by the answered set and re-hydrated there, so every path applies the same
/// filter. This constructor only packages the parts.
pub fn load(
    batch: Vec<BatchMsg>,
    answered: HashSet<i64>,
    window: Vec<RecentRow>,
    parents: HashMap<i64, ReplyParent>,
    catalog: Vec<GifRecord>,
    guard: String,
    self_id: i64,
) -> Snapshot {
    Snapshot {
        batch,
        window,
        parents,
        catalog,
        answered,
        guard,
        self_id,
        include_window: true,
    }
}

/// Pure assembly: the snapshot decides everything the document contains.
pub fn assemble(snapshot: Snapshot) -> ContextDocument {
    let batch_ids: HashSet<i64> = snapshot.batch.iter().map(|m| m.message_id).collect();
    let context = if snapshot.include_window {
        render_recent_context(
            &snapshot.window,
            &batch_ids,
            WINDOW_LIMIT,
            WINDOW_CAP,
            snapshot.self_id,
            &snapshot.answered,
        )
    } else {
        // The model already has its history in the live session: repeating it
        // every turn is what made it forget the current batch.
        String::new()
    };
    let batch = snapshot
        .batch
        .iter()
        .map(|m| BatchBlock {
            message: m.clone(),
            edge: edge_for(m, &snapshot.parents),
        })
        .collect();
    ContextDocument {
        context,
        guard: snapshot.guard,
        catalog: catalog::render_prompt(&snapshot.catalog),
        batch,
    }
}

pub fn render_document(doc: &ContextDocument) -> String {
    let mut parts = vec![];
    if !doc.context.is_empty() {
        parts.push(doc.context.clone());
    }
    if !doc.guard.is_empty() {
        parts.push(doc.guard.clone());
    }
    if !doc.catalog.is_empty() {
        parts.push(doc.catalog.clone());
    }
    parts.push(render_batch_blocks(&doc.batch));
    parts.join("\n")
}

/// The batch renderer. The reply marker comes from the pre-resolved edge, so a
/// parent outside the window cannot sneak text in and a missing parent cannot
/// acquire invented text.
pub fn render_batch_blocks(blocks: &[BatchBlock]) -> String {
    if blocks.is_empty() {
        return String::new();
    }
    let mut lines = vec![BATCH_HEADER.to_string()];
    for block in blocks {
        let m = &block.message;
        let who = display_who(&m.username, &m.first_name, m.user_id);
        let mut head = format!("[{}] {}:", m.message_id, who);
        if m.to_me {
            head.push_str(" [к тебе]");
        }
        match &block.edge {
            ReplyEdge::None => {}
            ReplyEdge::Resolved {
                parent,
                who,
                excerpt,
            } => {
                let who = if who.is_empty() {
                    format!("[{parent}]")
                } else {
                    format!("[{parent}] {who}:")
                };
                head.push_str(&format!(" (ответ на {who} {excerpt})"));
            }
            ReplyEdge::Missing(parent) => {
                head.push_str(&format!(" (ответ на [{parent}]: сообщения нет в базе)"));
            }
        }
        if !m.text.is_empty() || (m.media_kind == "sticker" && !m.file_id.is_empty()) {
            lines.push(format!(
                "{head} {}",
                message_body(&m.text, &m.media_kind, &m.file_id)
            ));
        }
        if !m.media_path.is_empty() {
            lines.push(format!(
                "{head} [file] {} -- read it via read tool",
                m.media_path
            ));
        } else if !m.thumb_path.is_empty() {
            // A video never reaches the model as bytes on this wire: the frame
            // is the only part of it that can be looked at.
            lines.push(format!(
                "{head} [file] {} -- read it via read tool (кадр видео)",
                m.thumb_path
            ));
        }
    }
    lines.join("\n")
}

/// Convenience renderer for callers that hold a batch and its parent map
/// (tests, diagnostics). Production goes through `load -> assemble`.
pub fn render_batch(batch: &[BatchMsg], parents: &HashMap<i64, ReplyParent>) -> String {
    let blocks: Vec<BatchBlock> = batch
        .iter()
        .map(|m| BatchBlock {
            message: m.clone(),
            edge: edge_for(m, parents),
        })
        .collect();
    render_batch_blocks(&blocks)
}

/// The single entry point for a fresh batch, a rebuild and a recovery: pure
/// assembly over one snapshot.
pub fn build_from(snapshot: Snapshot) -> BuiltContext {
    let ids: Vec<i64> = snapshot.batch.iter().map(|m| m.message_id).collect();
    let tome: Vec<i64> = snapshot
        .batch
        .iter()
        .filter(|m| m.to_me)
        .map(|m| m.message_id)
        .collect();
    let anchor = snapshot
        .window
        .iter()
        .filter_map(|r| r.0)
        .filter(|id| !ids.contains(id))
        .max()
        .unwrap_or(0);
    let document = assemble(snapshot);
    let text = render_document(&document);
    BuiltContext {
        sig: batch_sig(&text),
        text,
        ids,
        tome,
        anchor,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use rusqlite::Connection;
    use crate::BatchMsg;

    fn temp_state(tag: &str) -> String {
        let dir = std::env::temp_dir().join(format!("chat-ctx-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir.to_string_lossy().to_string()
    }

    fn msg(id: i64, text: &str, reply_to: Option<i64>) -> BatchMsg {
        BatchMsg {
            message_id: id,
            date: 0,
            user_id: Some(7),
            username: "u".into(),
            first_name: "U".into(),
            text: text.into(),
            reply_to,
            media_path: String::new(),
            media_kind: String::new(),
            file_id: String::new(),
            thumb_path: String::new(),
            to_me: false,
        }
    }

    fn snapshot_sync(con: &Connection, state: &str, batch: &[BatchMsg]) -> Snapshot {
        let mut answered = crate::store::answered_db(con);
        answered.extend(crate::answered_receipts(state));
        let filtered: Vec<BatchMsg> = batch
            .iter()
            .filter(|m| !answered.contains(&m.message_id))
            .cloned()
            .collect();
        let batch = crate::store::hydrate(con, &filtered);
        let ids: HashSet<i64> = batch.iter().map(|m| m.message_id).collect();
        let window =
            crate::store::recent_rows(con, WINDOW_LIMIT + ids.len() as i64 + WINDOW_SLACK).unwrap();
        let parents = crate::store::reply_parents(con, &batch).unwrap();
        let (rows, _) = crate::store::catalog_page(con, 0, catalog::PROMPT_MAX).unwrap();
        let catalog = catalog::records_from(rows);
        let guard = crate::store::answered_guard(con, GUARD_LIMIT).unwrap();
        let self_id: i64 = crate::kv_get(con, "self_id", "0").parse().unwrap_or(0);
        load(batch, answered, window, parents, catalog, guard, self_id)
    }

    fn build_sync(con: &Connection, state: &str, batch: &[BatchMsg]) -> BuiltContext {
        build_from(snapshot_sync(con, state, batch))
    }

    fn seed(con: &Connection) {
        crate::kv_set(con, "self_id", "42").unwrap();
        crate::kv_set(con, "self_username", "bot").unwrap();
        crate::kv_set(con, "self_first", "Bot").unwrap();
        // Fresh dates: the age guard drops provably old rows, so a 1970
        // fixture would test the guard instead of the window/edge rules.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(1_700_000_000);
        con.execute(
            &format!("INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id) VALUES (1,100,{now},7,'u','U','старый вопрос',NULL,'','')"),
            [],
        )
        .unwrap();
        con.execute(
            &format!("INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id) VALUES (2,101,{now},7,'u','U','новый вопрос',NULL,'','')"),
            [],
        )
        .unwrap();
        con.execute(
            &format!("INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id) VALUES (3,102,{now},7,'u','U','уточнение',101,'','')"),
            [],
        )
        .unwrap();
        con.execute(
            &format!("INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id) VALUES (4,103,{now},7,'u','U','свежий вопрос',NULL,'','')"),
            [],
        )
        .unwrap();
        con.execute(
            &format!("INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id,media_kind,media_state,file_unique_id,media_size) VALUES (5,104,{now},7,'u','U','гифка',NULL,'','gif_fid','animation','ready','gif_uniq',4096)"),
            [],
        )
        .unwrap();
    }

    #[test]
    fn window_rule_matrix() {
        let epoch = epoch_of("uid-a", 3);
        assert_eq!(epoch, "uid-a:3");
        // New context: nothing recorded as delivered yet.
        assert!(window_due(Some("uid-a:3"), "", "", false, "s1"));
        // The windowed frame was never proven delivered: another batch in the
        // same epoch still gets the window instead of starting without history.
        assert!(window_due(Some("uid-a:3"), "", "", false, "s2"));
        // Same session, different batch, no compaction: the batch only.
        assert!(!window_due(Some("uid-a:3"), "uid-a:3", "s1", false, "s2"));
        // Same session, the SAME batch rebuilt: the history it was prepared with
        // is kept, whatever its text hashes to now.
        assert!(window_due(Some("uid-a:3"), "uid-a:3", "s1", false, "s1"));
        // A compact asked for it even in the same session.
        assert!(window_due(Some("uid-a:3"), "uid-a:3", "s1", true, "s2"));
        // A new generation of the same durable id is a new context.
        assert!(window_due(Some("uid-a:4"), "uid-a:3", "s1", false, "s2"));
        // Unknown identity: safe side, the window is rendered.
        assert!(window_due(None, "uid-a:3", "s1", false, "s2"));
    }

    #[test]
    fn a_snapshot_without_the_window_renders_no_history() {
        let state = temp_state("gate");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let con = db(&state).unwrap();
        seed(&con);
        // Row 103 is the batch; rows 100-102 are history the model already has.
        let batch = vec![msg(103, "fresh", None)];
        let with = render_document(&assemble(snapshot_sync(&con, &state, &batch)));
        assert!(
            with.contains("[100] @u:") && with.contains("[102] @u:"),
            "history missing from the windowed document: {with}"
        );
        let without = render_document(&assemble(
            snapshot_sync(&con, &state, &batch).with_window(false),
        ));
        assert!(
            !without.contains("[100] @u:") && !without.contains("[102] @u:"),
            "history leaked into a turn that already has it: {without}"
        );
        assert!(
            without.contains("[103] @u:"),
            "the batch itself vanished: {without}"
        );
        let _ = std::fs::remove_dir_all(&state);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rebuilt_context_equals_fresh_context_byte_for_byte() {
        let state = temp_state("eq");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let mut con = db(&state).unwrap();
        seed(&con);
        let batch = vec![
            msg(100, "старый вопрос", None),
            msg(101, "новый вопрос", None),
            msg(102, "уточнение", Some(101)),
            msg(103, "свежий вопрос", None),
        ];
        let fresh = build_sync(&con, &state, &batch);
        {
            let mut tx = crate::db::WriteTxn::new(&mut con).unwrap();
            tx.seal_batch(&fresh.sig, -100, Some(77), &fresh.ids, &fresh.tome)
                .unwrap();
            tx.commit().unwrap();
        }
        let store = crate::store::Store::open(&state).unwrap();
        let rebuilt = crate::regen_text(&store, &state, &fresh.sig)
            .await
            .unwrap()
            .expect("rebuilt document");
        assert_eq!(rebuilt.as_bytes(), fresh.text.as_bytes());
        let _ = std::fs::remove_dir_all(&state);
    }

    #[test]
    fn hydration_offers_a_file_that_finished_after_the_snapshot() {
        let state = temp_state("hydrate");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let con = db(&state).unwrap();
        seed(&con);
        // The batch was taken at ingest, when the download had not finished: the
        // snapshot carries an empty media_path.
        con.execute(
            "INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id,media_kind,media_state)
             VALUES(9,105,1700000500,7,'u','U','фото',NULL,'','f105','photo','pending')",
            [],
        )
        .unwrap();
        let batch = vec![msg(105, "фото", None)];
        let before = assemble(snapshot_sync(&con, &state, &batch));
        assert!(
            !render_document(&before).contains("[file]"),
            "a pending download must not be offered as a file"
        );

        // The download lands; the same batch must now carry the path.
        con.execute(
            "UPDATE messages SET media_state='ready', media_path='/state/media/105.jpg' WHERE message_id=105",
            [],
        )
        .unwrap();
        let after = assemble(snapshot_sync(&con, &state, &batch));
        let doc = render_document(&after);
        assert!(
            doc.contains("[file] /state/media/105.jpg -- read it via read tool"),
            "a finished download did not reach the batch: {doc}"
        );
        let _ = std::fs::remove_dir_all(&state);
    }

    #[test]
    fn a_video_frame_is_offered_only_while_it_is_readable() {
        let state = temp_state("frame");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let con = db(&state).unwrap();
        seed(&con);
        con.execute(
            "INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id,media_kind,media_state,thumb_state,thumb_path)
             VALUES(10,106,1700000600,7,'u','U','[video]',NULL,'','v106','video','descriptor','ready','/state/media/106_frame.jpg')",
            [],
        )
        .unwrap();
        let batch = vec![msg(106, "[video]", None)];
        let doc = render_document(&assemble(snapshot_sync(&con, &state, &batch)));
        assert!(
            doc.contains("[file] /state/media/106_frame.jpg -- read it via read tool (кадр видео)"),
            "the frame of a video did not reach the batch: {doc}"
        );

        con.execute(
            "UPDATE messages SET thumb_state='evicted', thumb_path='' WHERE message_id=106",
            [],
        )
        .unwrap();
        let doc = render_document(&assemble(snapshot_sync(&con, &state, &batch)));
        assert!(
            !doc.contains("[file]"),
            "an evicted frame left a dead path in front of the model: {doc}"
        );
        let _ = std::fs::remove_dir_all(&state);
    }

    #[test]
    fn absent_parent_is_explicit_and_present_parent_is_one_hop() {
        let state = temp_state("edge");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let con = db(&state).unwrap();
        seed(&con);

        let absent = vec![msg(900, "вопрос", Some(899))];
        let blocks = vec![BatchBlock {
            message: absent[0].clone(),
            edge: edge_for(&absent[0], &HashMap::new()),
        }];
        let rendered = render_batch_blocks(&blocks);
        assert!(
            rendered.contains("(ответ на [899]: сообщения нет в базе)"),
            "missing parent state not explicit: {rendered}"
        );
        assert!(
            !rendered.contains("«"),
            "missing parent invented quotation: {rendered}"
        );

        let batch = vec![msg(103, "свежий вопрос", Some(102))];
        let parents = crate::store::reply_parents(&con, &batch).unwrap();
        let rendered = render_batch(&batch, &parents);
        assert!(
            rendered.contains("(ответ на [102] @u: «уточнение»)"),
            "{rendered}"
        );
        assert!(
            !rendered.contains("«новый вопрос»"),
            "chain leaked into nearest link: {rendered}"
        );
        assert!(
            !rendered.contains("«старый вопрос»"),
            "older parent leaked: {rendered}"
        );

        let _ = std::fs::remove_dir_all(&state);
    }

    #[test]
    fn reply_parent_lookup_finds_rows_outside_the_window() {
        let state = temp_state("pk");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let con = db(&state).unwrap();
        seed(&con);
        let batch = vec![msg(103, "x", Some(100))];
        let parents = crate::store::reply_parents(&con, &batch).unwrap();
        assert_eq!(
            parents.get(&100).map(|p| p.text.as_str()),
            Some("старый вопрос"),
            "parent was resolved by window position instead of message key"
        );
        let _ = std::fs::remove_dir_all(&state);
    }

    #[test]
    fn answered_rows_are_dropped_identically_on_both_paths() {
        let state = temp_state("ans");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let con = db(&state).unwrap();
        seed(&con);
        crate::store::mirror_own_send(&con, 200, "ответ", Some(101)).unwrap();
        let batch = vec![
            msg(100, "старый вопрос", None),
            msg(101, "новый вопрос", None),
        ];
        let built = build_sync(&con, &state, &batch);
        assert_eq!(built.ids, vec![100], "answered row stayed in the batch");
        assert!(
            built.text.contains("[101]"),
            "context keeps the answered row: {}",
            built.text
        );
        let _ = std::fs::remove_dir_all(&state);
    }
}
