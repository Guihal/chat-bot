//! The ONE context builder: `load(snapshot) -> assemble -> render_document`.
//!
//! A fresh batch, the rebuild after a compaction and the recovery after a
//! restart all call [`build`]. They differ only in how the batch rows arrive
//! (ingest, journal ids, journal ids + tome), never in how the document is
//! assembled or rendered, so "fresh and rebuilt say different things" is
//! structurally impossible.
use crate::catalog::{self, GifRecord};
use crate::{
    answered_guard, answered_ids, batch_sig, recent_rows, render_recent_context, BatchMsg,
};
use anyhow::Result;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};

pub const WINDOW_LIMIT: i64 = 50;
pub const WINDOW_CAP: usize = 300;
pub const REPLY_PARENT_CAP: usize = 120;
const GUARD_LIMIT: i64 = 5;
const WINDOW_SLACK: i64 = 5;

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

const BATCH_HEADER: &str = "Пачка: это сообщения, на которые можно ответить. reply_to — только id из этой пачки; id из контекста выше и каталога гифок — история, не цель ответа.";

fn display_who(username: &str, first_name: &str, user_id: Option<i64>) -> String {
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

/// Nearest parent of every reply, resolved from the mirror by message key.
/// One targeted lookup per distinct reply target; a miss is simply absent from
/// the map and becomes an explicit `Missing` edge, never invented content.
pub fn reply_parents(con: &Connection, batch: &[BatchMsg]) -> Result<HashMap<i64, ReplyParent>> {
    let mut ids: Vec<i64> = batch.iter().filter_map(|m| m.reply_to).collect();
    ids.sort_unstable();
    ids.dedup();
    let mut out = HashMap::new();
    for id in ids {
        let row: Option<(Option<String>, Option<String>, Option<i64>, Option<String>, Option<String>)> = con
            .prepare_cached(
                "SELECT username,first_name,user_id,text,media_path FROM messages WHERE message_id=? LIMIT 1",
            )
            .and_then(|mut stmt| {
                stmt.query_row([id], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
                })
            })
            .ok();
        if let Some((username, first_name, user_id, text, media)) = row {
            let username = username.unwrap_or_default();
            let first_name = first_name.unwrap_or_default();
            out.insert(
                id,
                ReplyParent {
                    who: display_who(&username, &first_name, user_id),
                    text: text.unwrap_or_default(),
                    media: !media.unwrap_or_default().is_empty(),
                },
            );
        }
    }
    Ok(out)
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

/// Read the whole snapshot. `batch` may still contain already-answered rows:
/// they are dropped here once, so every path applies the same filter.
pub fn load(state: &str, con: &Connection, batch: &[BatchMsg]) -> Result<Snapshot> {
    let answered = answered_ids(state, con);
    let batch: Vec<BatchMsg> = batch
        .iter()
        .filter(|m| !answered.contains(&m.message_id))
        .cloned()
        .collect();
    let batch_ids: HashSet<i64> = batch.iter().map(|m| m.message_id).collect();
    let window = recent_rows(con, WINDOW_LIMIT + batch_ids.len() as i64 + WINDOW_SLACK)?;
    let self_id: i64 = crate::kv_get(con, "self_id", "0").parse().unwrap_or(0);
    let parents = reply_parents(con, &batch)?;
    let catalog = catalog::page(con, 0, catalog::PROMPT_MAX)?.records;
    let guard = answered_guard(con, GUARD_LIMIT)?;
    Ok(Snapshot {
        batch,
        window,
        parents,
        catalog,
        answered,
        guard,
        self_id,
    })
}

/// Pure assembly: the snapshot decides everything the document contains.
pub fn assemble(snapshot: Snapshot) -> ContextDocument {
    let batch_ids: HashSet<i64> = snapshot.batch.iter().map(|m| m.message_id).collect();
    let context = render_recent_context(
        &snapshot.window,
        &batch_ids,
        WINDOW_LIMIT,
        WINDOW_CAP,
        snapshot.self_id,
        &snapshot.answered,
    );
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

/// The single entry point for a fresh batch, a rebuild and a recovery.
pub fn build(state: &str, con: &Connection, batch: &[BatchMsg]) -> Result<BuiltContext> {
    let snapshot = load(state, con, batch)?;
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
    Ok(BuiltContext {
        sig: batch_sig(&text),
        text,
        ids,
        tome,
        anchor,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
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
            user_id: Some(7),
            username: "u".into(),
            first_name: "U".into(),
            text: text.into(),
            reply_to,
            media_path: String::new(),
            media_kind: String::new(),
            file_id: String::new(),
            to_me: false,
        }
    }

    fn seed(con: &Connection) {
        crate::kv_set(con, "self_id", "42").unwrap();
        crate::kv_set(con, "self_username", "bot").unwrap();
        crate::kv_set(con, "self_first", "Bot").unwrap();
        con.execute(
            "INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id) VALUES (1,100,1000,7,'u','U','старый вопрос',NULL,'','')",
            [],
        )
        .unwrap();
        con.execute(
            "INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id) VALUES (2,101,1001,7,'u','U','новый вопрос',NULL,'','')",
            [],
        )
        .unwrap();
        con.execute(
            "INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id) VALUES (3,102,1002,7,'u','U','уточнение',101,'','')",
            [],
        )
        .unwrap();
        con.execute(
            "INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id) VALUES (4,103,1003,7,'u','U','свежий вопрос',NULL,'','')",
            [],
        )
        .unwrap();
        con.execute(
            "INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id,media_kind,media_state,file_unique_id,media_size) VALUES (5,104,1004,7,'u','U','гифка',NULL,'','gif_fid','animation','ready','gif_uniq',4096)",
            [],
        )
        .unwrap();
    }

    #[test]
    fn rebuilt_context_equals_fresh_context_byte_for_byte() {
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
        let fresh = build(&state, &con, &batch).unwrap();
        {
            let mut tx = crate::db::WriteTxn::new(&mut con).unwrap();
            tx.seal_batch(&fresh.sig, -100, Some(77), &fresh.ids, &fresh.tome)
                .unwrap();
            tx.commit().unwrap();
        }
        let rebuilt = crate::regen_text(&state, &con, &fresh.sig)
            .unwrap()
            .expect("rebuilt document");
        assert_eq!(rebuilt.as_bytes(), fresh.text.as_bytes());
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
        let parents = reply_parents(&con, &batch).unwrap();
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
        let parents = reply_parents(&con, &batch).unwrap();
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
        crate::mirror_own_send(&con, 200, "ответ", Some(101)).unwrap();
        let batch = vec![
            msg(100, "старый вопрос", None),
            msg(101, "новый вопрос", None),
        ];
        let built = build(&state, &con, &batch).unwrap();
        assert_eq!(built.ids, vec![100], "answered row stayed in the batch");
        assert!(
            built.text.contains("[101]"),
            "context keeps the answered row: {}",
            built.text
        );
        let _ = std::fs::remove_dir_all(&state);
    }
}
