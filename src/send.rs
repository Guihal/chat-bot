//! Daemon-owned Bot API send path.
//!
//! The extension no longer knows the token or talks HTTP: it forwards
//! `chat_send`/`typing` over the control socket and the daemon performs every
//! send here. The order is the contract from DESIGN §3.2: durable intent, then
//! Intent -> Sending, then exactly one HTTP call, then Sending -> Sent with the
//! receipt and the mirror row in the same transaction. A crash before the answer
//! leaves Sending, which startup turns into Unknown without a resend.
use crate::db::Db;
use crate::ledger::{self, OpenAnswer, Receipt, SendOutcome};
use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use std::path::Path;

const TEXT_LIMIT: usize = 4000;
const CAPTION_LIMIT: usize = 1024;

#[derive(Debug, Clone, Default)]
struct Request {
    text: String,
    reply_to: Option<i64>,
    gif_file_id: String,
    sticker_file_id: String,
    media_path: String,
}

#[derive(Debug, Clone)]
struct Part {
    kind: String,
    text: String,
    file_id: String,
    file_unique_id: String,
    media_path: String,
}

fn arg_str(args: &Value, key: &str) -> String {
    args.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string()
}

fn clip(s: &str, limit: usize) -> String {
    s.chars().take(limit).collect()
}

fn parse_request(args: &Value) -> Result<Request> {
    let text = args
        .get("text")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let reply_to = args.get("reply_to").and_then(|v| v.as_i64()).or_else(|| {
        args.get("reply_to")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok())
    });
    Ok(Request {
        text,
        reply_to,
        gif_file_id: arg_str(args, "gif_file_id"),
        sticker_file_id: arg_str(args, "sticker_file_id"),
        media_path: arg_str(args, "media_path"),
    })
}

fn media_targets(r: &Request) -> usize {
    [&r.gif_file_id, &r.sticker_file_id, &r.media_path]
        .iter()
        .filter(|s| !s.is_empty())
        .count()
}

fn file_kind(path: &str) -> (String, &'static str, &'static str) {
    let ext = Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    match ext.as_str() {
        "gif" => ("animation".into(), "animation", "image/gif"),
        "jpg" | "jpeg" => ("photo".into(), "photo", "image/jpeg"),
        "png" => ("photo".into(), "photo", "image/png"),
        "webp" => ("photo".into(), "photo", "image/webp"),
        "mp4" => ("video".into(), "video", "video/mp4"),
        "mov" => ("video".into(), "video", "video/quicktime"),
        "webm" => ("video".into(), "video", "video/webm"),
        "mkv" => ("video".into(), "video", "video/x-matroska"),
        "pdf" => ("document".into(), "document", "application/pdf"),
        _ => ("document".into(), "document", "application/octet-stream"),
    }
}

fn lookup_unique_id(con: &rusqlite::Connection, file_id: &str) -> String {
    con.query_row(
        "SELECT COALESCE(file_unique_id,'') FROM messages WHERE file_id=? AND COALESCE(file_unique_id,'')<>'' LIMIT 1",
        [file_id],
        |r| r.get(0),
    )
    .unwrap_or_default()
}

/// A sticker plus text is TWO messages: sendSticker takes no caption, so the
/// text becomes its own send in the same answer. Every other shape is one
/// message; media arguments stay mutually exclusive because the Bot API call
/// itself cannot express two attachments.
fn build_parts(r: &Request, unique: &str) -> Result<Vec<Part>> {
    if media_targets(r) > 1 {
        return Err(anyhow!(
            "gif_file_id, sticker_file_id и media_path взаимоисключающие"
        ));
    }
    let mut parts = Vec::new();
    if !r.sticker_file_id.is_empty() {
        parts.push(Part {
            kind: "sticker".into(),
            text: String::new(),
            file_id: r.sticker_file_id.clone(),
            file_unique_id: unique.to_string(),
            media_path: String::new(),
        });
        if !r.text.is_empty() {
            parts.push(Part {
                kind: "text".into(),
                text: clip(&r.text, TEXT_LIMIT),
                file_id: String::new(),
                file_unique_id: String::new(),
                media_path: String::new(),
            });
        }
        return Ok(parts);
    }
    if !r.gif_file_id.is_empty() {
        parts.push(Part {
            kind: "animation".into(),
            text: clip(&r.text, CAPTION_LIMIT),
            file_id: r.gif_file_id.clone(),
            file_unique_id: unique.to_string(),
            media_path: String::new(),
        });
        return Ok(parts);
    }
    if !r.media_path.is_empty() {
        if !Path::new(&r.media_path).is_file() {
            return Err(anyhow!("media file not found: {}", r.media_path));
        }
        let (kind, _, _) = file_kind(&r.media_path);
        parts.push(Part {
            kind,
            text: clip(&r.text, CAPTION_LIMIT),
            file_id: String::new(),
            file_unique_id: String::new(),
            media_path: r.media_path.clone(),
        });
        return Ok(parts);
    }
    if r.text.is_empty() {
        return Err(anyhow!("empty body, nothing sent"));
    }
    parts.push(Part {
        kind: "text".into(),
        text: clip(&r.text, TEXT_LIMIT),
        file_id: String::new(),
        file_unique_id: String::new(),
        media_path: String::new(),
    });
    Ok(parts)
}

fn fingerprint(r: &Request) -> String {
    json!({
        "text": r.text,
        "reply_to": r.reply_to,
        "gif_file_id": r.gif_file_id,
        "sticker_file_id": r.sticker_file_id,
        "media_path": r.media_path,
    })
    .to_string()
}

fn response_text(receipts: &[Receipt], note: Option<&str>) -> String {
    let mut body = if receipts.len() == 1 {
        receipts[0].tool_json().to_string()
    } else {
        json!({
            "messages": receipts.iter().map(|r| r.tool_json()).collect::<Vec<_>>()
        })
        .to_string()
    };
    if let Some(n) = note {
        body.push('\n');
        body.push_str(n);
    }
    body
}

fn media_from_result<'a>(result: &'a Value, kind: &str) -> Option<&'a Value> {
    match kind {
        "animation" => result.get("animation"),
        "sticker" => result.get("sticker"),
        "video" => result.get("video"),
        "document" => result.get("document"),
        "photo" => result
            .get("photo")
            .and_then(|v| v.as_array())
            .and_then(|a| a.last()),
        _ => None,
    }
}

async fn send_one(request: &Value, kind: &str) -> Result<Receipt> {
    let chat_id = request.get("chat_id").and_then(|v| v.as_i64()).unwrap_or(0);
    let text = request.get("text").and_then(|v| v.as_str()).unwrap_or("");
    let file_id = request
        .get("file_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let media_path = request
        .get("media_path")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let reply_to = request.get("reply_to").and_then(|v| v.as_i64());
    let thread = request.get("thread_id").and_then(|v| v.as_i64());
    let unique = request
        .get("file_unique_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let mut params = json!({"chat_id": chat_id});
    if let Some(r) = reply_to {
        params["reply_to_message_id"] = json!(r);
    }
    let result = match kind {
        "sticker" => {
            params["sticker"] = json!(file_id);
            crate::api_with_thread("sendSticker", &params, thread).await?
        }
        "animation" if !file_id.is_empty() => {
            params["animation"] = json!(file_id);
            if !text.is_empty() {
                params["caption"] = json!(clip(text, CAPTION_LIMIT));
            }
            crate::api_with_thread("sendAnimation", &params, thread).await?
        }
        "text" => {
            params["text"] = json!(clip(text, TEXT_LIMIT));
            crate::api_with_thread("sendMessage", &params, thread).await?
        }
        kind if !media_path.is_empty() => {
            if !text.is_empty() {
                params["caption"] = json!(clip(text, CAPTION_LIMIT));
            }
            let method = match kind {
                "animation" => "sendAnimation",
                "photo" => "sendPhoto",
                "video" => "sendVideo",
                _ => "sendDocument",
            };
            let field = match kind {
                "animation" => "animation",
                "photo" => "photo",
                "video" => "video",
                _ => "document",
            };
            let filename = Path::new(media_path)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("file");
            let (_, _, mime) = file_kind(media_path);
            crate::api_multipart_with_thread(
                method, field, media_path, filename, mime, &params, thread,
            )
            .await?
        }
        other => return Err(anyhow!("unsupported send kind: {other}")),
    };
    let message_id = result
        .get("message_id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| anyhow!("Bot API send returned no message_id: {result}"))?;
    // The receipt carries what Telegram actually accepted: file_id,
    // file_unique_id and size of the sent media object. The catalogue learns
    // our own GIFs from exactly that, so it never has to guess from the
    // request or from a stale chat row.
    let sent = media_from_result(&result, kind);
    let sent_id = sent
        .and_then(|m| m.get("file_id"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let sent_unique = sent
        .and_then(|m| m.get("file_unique_id"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let sent_size = sent
        .and_then(|m| m.get("file_size"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    Ok(Receipt {
        message_id,
        reply_to,
        text: if kind == "text" {
            clip(text, TEXT_LIMIT)
        } else {
            clip(text, CAPTION_LIMIT)
        },
        kind: kind.to_string(),
        file_id: if sent_id.is_empty() {
            file_id.to_string()
        } else {
            sent_id
        },
        file_unique_id: if sent_unique.is_empty() {
            unique
        } else {
            sent_unique
        },
        media_path: media_path.to_string(),
        media_size: sent_size,
        thread_id: thread,
    })
}

/// `open_answer` mints the id; the same fingerprint replays the stored response
/// byte for byte, a different one is refused with `answer_exists`.
pub async fn chat_send(db: &Db, args: &Value) -> Result<String> {
    let batch_key = args
        .get("batch_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if batch_key.is_empty() {
        return Err(anyhow!(
            "no active batch: chat_send arrived outside a pushed batch"
        ));
    }
    let mut req = parse_request(args)?;
    if req.text == "[skip]" {
        return Ok("silence acknowledged, nothing sent".to_string());
    }

    // Read the batch snapshot (`reply_to` is decoration: an id outside the batch
    // is dropped, and its absence never keeps the batch open).
    let bk = batch_key.clone();
    let (view, chat) = db
        .call(move |con| {
            let view = ledger::view(con, &bk)?;
            let chat: i64 = crate::kv_get(con, "chat_id", "0").parse().unwrap_or(0);
            Ok((view, chat))
        })
        .await?;
    let view = view.ok_or_else(|| anyhow!("unknown batch: {batch_key}"))?;
    let lookup_id = req_sticker_or_gif(&req);
    let unique = if lookup_id.is_empty() {
        String::new()
    } else {
        db.call(move |con| Ok(lookup_unique_id(con, &lookup_id)))
            .await
            .unwrap_or_default()
    };
    let mut note: Option<String> = None;
    if let Some(q) = req.reply_to {
        if !view.message_ids.contains(&q) {
            note = Some(format!(
                "sent without quote: reply_to {q} is not in this batch (valid: {:?})",
                view.message_ids
            ));
            req.reply_to = None;
        }
    }
    let parts = build_parts(&req, &unique)?;
    let fp = fingerprint(&req);
    let thread = view.thread_id;

    let begin = {
        let bk = batch_key.clone();
        let parts = parts.clone();
        let thread = thread;
        db.write(move |tx| match tx.open_answer(&bk, &fp)? {
            OpenAnswer::Replay { answer, response } => Ok((answer, None, response)),
            OpenAnswer::Opened(answer) => {
                let mut intents = Vec::new();
                for (i, part) in parts.iter().enumerate() {
                    let request = json!({
                        "chat_id": chat,
                        "text": part.text,
                        "reply_to": req.reply_to,
                        "file_id": part.file_id,
                        "file_unique_id": part.file_unique_id,
                        "media_path": part.media_path,
                        "thread_id": thread,
                    });
                    intents.push(tx.begin_send(answer, i, &part.kind, &request)?);
                }
                Ok((answer, Some(intents), None))
            }
        })
        .await?
    };
    let (_answer, intents, replay) = begin;
    match (intents, replay) {
        (Some(intents), _) => {
            let total = intents.len();
            let mut receipts: Vec<Receipt> = Vec::new();
            for (i, intent) in intents.iter().enumerate() {
                let it = intent.clone();
                db.write(move |tx| tx.start_send(&it)).await?;
                match send_one(&intent.request, &intent.kind).await {
                    Ok(receipt) => {
                        receipts.push(receipt.clone());
                        let complete = if i + 1 == total {
                            Some(response_text(&receipts, note.as_deref()))
                        } else {
                            None
                        };
                        let it = intent.clone();
                        db.write(move |tx| {
                            tx.finish_send(&it, &SendOutcome::Sent(receipt), complete.as_deref())
                                .map(|_| ())
                        })
                        .await?;
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        let it = intent.clone();
                        let _ = db
                            .write(move |tx| {
                                tx.finish_send(&it, &SendOutcome::Unknown(msg), None)
                                    .map(|_| ())
                            })
                            .await;
                        return Err(anyhow!("Bot API send failed: {e}; recorded Unknown, not resent"));
                    }
                }
            }
            Ok(response_text(&receipts, note.as_deref()))
        }
        (None, Some(response)) => Ok(response),
        (None, None) => Err(anyhow!(
            "answer_exists: this batch already has an answer whose send did not reach a receipt (state Unknown); refusing a second message"
        )),
    }
}

fn req_sticker_or_gif(r: &Request) -> String {
    if !r.gif_file_id.is_empty() {
        r.gif_file_id.clone()
    } else {
        r.sticker_file_id.clone()
    }
}

/// `typing` is RPC too: the daemon owns the token and the topic parameter.
pub async fn typing(db: &Db, args: &Value) -> Result<String> {
    let chat: i64 = db
        .call(|con| Ok(crate::kv_get(con, "chat_id", "0").parse().unwrap_or(0)))
        .await?;
    let mut thread = args.get("thread_id").and_then(|v| v.as_i64());
    if thread.is_none() {
        if let Some(bk) = args.get("batch_key").and_then(|v| v.as_str()) {
            let bk = bk.to_string();
            thread = db
                .call(move |con| Ok(ledger::thread_id(con, &bk)))
                .await
                .unwrap_or(None);
        }
    }
    crate::api_with_thread(
        "sendChatAction",
        &json!({"chat_id": chat, "action": "typing"}),
        thread,
    )
    .await?;
    Ok("ok".to_string())
}

/// Startup repair: anything left Sending becomes Unknown in ONE transaction,
/// with ONE incident and no HTTP call (therefore no second message).
pub async fn repair_sending(db: &Db) -> Result<usize> {
    db.write(|tx| ledger::resolve_sending(tx.handle())).await
}
