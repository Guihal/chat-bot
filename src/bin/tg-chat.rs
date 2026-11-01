use anyhow::Result;
use chat_bot::*;
use serde_json::{json, Value};
use std::io::{Read, Write};

fn mcp_state_dir() -> String {
    // Same rule as the daemon: the only fallback is ./state, so a clone never
    // reads a neighbouring host installation's database.
    state_dir()
}

fn portraits_dir() -> std::path::PathBuf {
    // Runtime resolution (same pattern as ext_source in lib.rs): deployed
    // checkout via CHATBOT_CWD first, then the process working directory,
    // then the compile-time manifest dir (local `cargo run` convenience only).
    // env!("CARGO_MANIFEST_DIR") alone is a build-machine constant and does
    // not exist next to a deployed binary.
    for base in [cfg("CHATBOT_CWD", ""), ".".to_string()] {
        if base.is_empty() {
            continue;
        }
        let p = std::path::Path::new(&base).join("portraits");
        if p.is_dir() {
            return p;
        }
    }
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("portraits")
}

fn norm_name(name: &str) -> String {
    let b = std::path::Path::new(name.trim().trim_start_matches('@'))
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    b.trim().trim_end_matches(".md").to_string()
}

fn load_portrait(name: &str) -> Option<(String, String)> {
    let base = norm_name(name);
    if base.is_empty() {
        return None;
    }
    let d = portraits_dir();
    let files: Vec<String> = std::fs::read_dir(&d).ok()?.flatten().map(|e| e.file_name().to_string_lossy().to_string()).collect();
    let mut lower: std::collections::HashMap<String, String> = Default::default();
    for f in &files {
        lower.insert(f.to_lowercase(), f.clone());
    }
    let mut hit: Option<String> = None;
    for cand in [base.clone(), format!("{base}.md")] {
        if files.contains(&cand) {
            hit = Some(cand);
            break;
        }
        if let Some(f) = lower.get(&cand.to_lowercase()) {
            hit = Some(f.clone());
            break;
        }
    }
    let f = hit?;
    let text = std::fs::read_to_string(d.join(&f)).ok()?;
    Some((f, text))
}

fn tool_history(args: &Value) -> String {
    let state = mcp_state_dir();
    let con = match db(&state) {
        Ok(c) => c,
        Err(e) => return json!({"error": e.to_string()}).to_string(),
    };
    let mut q = "SELECT message_id, username, first_name, text, reply_to, media_path FROM messages".to_string();
    let mut params: Vec<Value> = vec![];
    if let Some(b) = args.get("before").and_then(|v| v.as_i64()) {
        q.push_str(" WHERE update_id < ?");
        params.push(json!(b));
    }
    let limit = args.get("limit").and_then(|v| v.as_i64()).unwrap_or(50);
    q.push_str(" ORDER BY date DESC, message_id DESC LIMIT ?");
    params.push(json!(limit));
    let mut stmt = match con.prepare(&q) {
        Ok(s) => s,
        Err(e) => return json!({"error": e.to_string()}).to_string(),
    };
    let p: Vec<Box<dyn rusqlite::ToSql>> = params.iter().map(|v| Box::new(v.as_i64().unwrap_or(0)) as Box<dyn rusqlite::ToSql>).collect();
    let refs: Vec<&dyn rusqlite::ToSql> = p.iter().map(|b| b.as_ref()).collect();
    let rows: Vec<Value> = stmt
        .query_map(refs.as_slice(), |r| {
            Ok(json!({
                "message_id": r.get::<_, Option<i64>>(0)?,
                "user": r.get::<_, Option<String>>(1)?.or(r.get::<_, Option<String>>(2)?),
                "text": r.get::<_, Option<String>>(3)?,
                "reply_to": r.get::<_, Option<i64>>(4)?,
                "media": r.get::<_, Option<String>>(5)?,
            }))
        })
        .map(|it| it.flatten().collect())
        .unwrap_or_default();
    let mut rows = rows;
    rows.reverse();
    serde_json::to_string(&rows).unwrap_or_default()
}

fn tool_search(args: &Value) -> String {
    let state = mcp_state_dir();
    let con = match db(&state) {
        Ok(c) => c,
        Err(e) => return json!({"error": e.to_string()}).to_string(),
    };
    let query = args.get("query").and_then(|v| v.as_str()).unwrap_or("");
    let limit = args.get("limit").and_then(|v| v.as_i64()).unwrap_or(20);
    let sql = "SELECT m.message_id, m.username, m.text FROM messages_fts f JOIN messages m ON m.update_id = f.rowid WHERE messages_fts MATCH ? LIMIT ?";
    let run = |q: &str| -> Result<Vec<Value>> {
        let mut stmt = con.prepare(sql)?;
        let rows: Vec<Value> = stmt
            .query_map(rusqlite::params![q, limit], |r| {
                Ok(json!({"message_id": r.get::<_, Option<i64>>(0)?, "user": r.get::<_, Option<String>>(1)?, "text": r.get::<_, Option<String>>(2)?.unwrap_or_default()}))
            })?
            .flatten()
            .collect();
        Ok(rows)
    };
    let rows = run(query).unwrap_or_default();
    let rows = if rows.is_empty() {
        let phrase = format!("\"{}\"", query.replace('"', "\"\""));
        run(&phrase).unwrap_or_default()
    } else {
        rows
    };
    let trimmed: Vec<Value> = rows
        .iter()
        .map(|r| {
            let t = r.get("text").and_then(|v| v.as_str()).unwrap_or("");
            let t: String = t.chars().take(500).collect();
            json!({"message_id": r.get("message_id"), "user": r.get("user"), "text": t})
        })
        .collect();
    serde_json::to_string(&trimmed).unwrap_or_default()
}

async fn tool_get_media(args: &Value) -> String {
    let fid = args.get("file_id").and_then(|v| v.as_str()).unwrap_or("");
    let state = mcp_state_dir();
    match download_media(fid, &state, "").await {
        Ok(p) => json!({"path": p}).to_string(),
        Err(e) => json!({"error": e.to_string()}).to_string(),
    }
}

fn tool_chatter_brief(args: &Value) -> String {
    let name = norm_name(args.get("name").and_then(|v| v.as_str()).unwrap_or(""));
    if name.is_empty() {
        return json!({"error": "name is required"}).to_string();
    }
    let state = mcp_state_dir();
    let con = match db(&state) {
        Ok(c) => c,
        Err(e) => return json!({"error": e.to_string()}).to_string(),
    };
    let at = if name.starts_with('@') { name.clone() } else { format!("@{name}") };
    let row: (i64, Option<i64>, Option<i64>) = con
        .query_row(
            "SELECT COUNT(*), MIN(date), MAX(date) FROM messages WHERE username = ? COLLATE NOCASE OR first_name = ? COLLATE NOCASE OR ('@' || username) = ? COLLATE NOCASE",
            rusqlite::params![name, name, at],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap_or((0, None, None));
    let mut stmt = con
        .prepare("SELECT text FROM messages WHERE (username = ? COLLATE NOCASE OR first_name = ? COLLATE NOCASE) AND text <> '' ORDER BY update_id DESC LIMIT 3")
        .unwrap();
    let samples: Vec<String> = stmt
        .query_map(rusqlite::params![name, name], |r| r.get::<_, Option<String>>(0))
        .map(|it| it.flatten().flatten().map(|s| s.chars().take(200).collect()).collect())
        .unwrap_or_default();
    let mut lines = vec![format!("@{name} — {} messages in mirror", row.0)];
    if let Some((f, text)) = load_portrait(&name) {
        lines.push(format!("portrait ({f}):"));
        lines.extend(text.lines().map(|l| l.trim().to_string()).filter(|l| !l.is_empty()).take(4));
    } else {
        lines.push("no portrait on file".to_string());
    }
    if let Some(s) = samples.first() {
        let s: String = s.chars().take(160).collect();
        lines.push(format!("recent: {s}"));
    }
    let lines: Vec<String> = lines.into_iter().take(6).collect();
    json!({"name": name, "brief": lines.join("\n")}).to_string()
}

fn tool_chatter_full(args: &Value) -> String {
    let name = norm_name(args.get("name").and_then(|v| v.as_str()).unwrap_or(""));
    if name.is_empty() {
        return json!({"error": "name is required"}).to_string();
    }
    match load_portrait(&name) {
        Some((f, text)) => json!({"name": name, "found": true, "file": f, "text": text}).to_string(),
        None => json!({"name": name, "found": false, "text": format!("no portrait on file for @{name}")}).to_string(),
    }
}

fn tool_chat_stats() -> String {
    let state = mcp_state_dir();
    let con = match db(&state) {
        Ok(c) => c,
        Err(e) => return json!({"error": e.to_string()}).to_string(),
    };
    let myself: i64 = kv_get(&con, "self_id", "0").parse().unwrap_or(0);
    let excl = format!("WHERE user_id IS NULL OR user_id != {myself}");
    let total: i64 = con.query_row(&format!("SELECT COUNT(*) FROM messages {excl}"), [], |r| r.get(0)).unwrap_or(0);
    let media: i64 = con
        .query_row(&format!("SELECT COUNT(*) FROM messages WHERE media_path <> '' AND (user_id IS NULL OR user_id != {myself})"), [], |r| r.get(0))
        .unwrap_or(0);
    let bounds: (Option<i64>, Option<i64>) = con.query_row(&format!("SELECT MIN(date), MAX(date) FROM messages {excl}"), [], |r| Ok((r.get(0)?, r.get(1)?))).unwrap_or((None, None));
    let mut stmt = con.prepare(&format!("SELECT COALESCE(NULLIF(username, ''), first_name, CAST(user_id AS TEXT)), COUNT(*) FROM messages {excl} GROUP BY 1 ORDER BY 2 DESC LIMIT 10")).unwrap();
    let top: Vec<(Option<String>, i64)> = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).map(|it| it.flatten().collect()).unwrap_or_default();
    let days_rows: Vec<Option<i64>> = {
        let mut s = con.prepare(&format!("SELECT date FROM messages WHERE date > 0 AND (user_id IS NULL OR user_id != {myself})")).unwrap();
        s.query_map([], |r| r.get(0)).map(|it| it.flatten().collect()).unwrap_or_default()
    };
    let mut days: std::collections::HashMap<String, i64> = Default::default();
    for ts in days_rows.into_iter().flatten() {
        let d = chrono::DateTime::from_timestamp(ts, 0).map(|t| t.format("%Y-%m-%d").to_string()).unwrap_or_default();
        if !d.is_empty() {
            *days.entry(d).or_default() += 1;
        }
    }
    let mut recent: Vec<(String, i64)> = days.into_iter().collect();
    recent.sort();
    let recent: Vec<Value> = recent.into_iter().rev().take(14).rev().map(|(d, c)| json!({"day": d, "messages": c})).collect();
    json!({
        "total_messages": total,
        "media_messages": media,
        "first_date": bounds.0,
        "last_date": bounds.1,
        "top_chatters": top.iter().map(|(u, c)| json!({"user": u, "messages": c})).collect::<Vec<_>>(),
        "activity_last_14_days": recent,
    })
    .to_string()
}

fn read_msg() -> Result<Option<Value>> {
    let stdin = std::io::stdin();
    let mut header = Vec::new();
    let mut lock = stdin.lock();
    // Read byte-by-byte until \r\n\r\n.
    let mut buf = [0u8; 1];
    loop {
        let n = lock.read(&mut buf)?;
        if n == 0 {
            if header.is_empty() {
                return Ok(None);
            }
            return Ok(None);
        }
        header.extend_from_slice(&buf);
        if header.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let header_s = String::from_utf8_lossy(&header).to_string();
    let mut length = 0usize;
    for line in header_s.split("\r\n") {
        if line.to_lowercase().starts_with("content-length:") {
            length = line.split(':').nth(1).unwrap_or("0").trim().parse().unwrap_or(0);
        }
    }
    let mut body = vec![0u8; length];
    lock.read_exact(&mut body)?;
    Ok(Some(serde_json::from_slice(&body)?))
}

fn write_msg(obj: &Value) -> Result<()> {
    let body = serde_json::to_vec(obj)?;
    let out = std::io::stdout();
    let mut lock = out.lock();
    lock.write_all(format!("Content-Length: {}\r\n\r\n", body.len()).as_bytes())?;
    lock.write_all(&body)?;
    lock.flush()?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    loop {
        let req = read_msg()?;
        let req = match req {
            Some(r) => r,
            None => return Ok(()),
        };
        let rid = req.get("id").cloned().unwrap_or(Value::Null);
        let method = req.get("method").and_then(|v| v.as_str()).unwrap_or("");
        match method {
            "initialize" => write_msg(&json!({"jsonrpc": "2.0", "id": rid, "result": {"protocolVersion": "2024-11-05", "capabilities": {"tools": {}}, "serverInfo": {"name": "tg-chat", "version": "0.1"}}}))?,
            "notifications/initialized" => {}
            "ping" => write_msg(&json!({"jsonrpc": "2.0", "id": rid, "result": {}}))?,
            "tools/list" => write_msg(&json!({"jsonrpc": "2.0", "id": rid, "result": {"tools": [
                {"name": "history", "description": "Recent chat messages from the local mirror (daemon-fed, ~10s behind live; this tool never polls Telegram)", "inputSchema": {"type": "object", "properties": {"limit": {"type": "integer", "default": 50}, "before": {"type": "integer"}}}},
                {"name": "search", "description": "Full-text search over the mirrored chat history (mirror lags live by the daemon batch window; never polls Telegram)", "inputSchema": {"type": "object", "properties": {"query": {"type": "string"}, "limit": {"type": "integer", "default": 20}}, "required": ["query"]}},
                {"name": "get_media", "description": "Download a file by file_id, return local path", "inputSchema": {"type": "object", "properties": {"file_id": {"type": "string"}}, "required": ["file_id"]}},
                {"name": "chatter_brief", "description": "Short 3-6 line dossier on a chatter for chat context", "inputSchema": {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}},
                {"name": "chatter_full", "description": "Full portrait of a chatter", "inputSchema": {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}},
                {"name": "chat_stats", "description": "Chat statistics from the local mirror (~10s behind live): message counts, top chatters, activity", "inputSchema": {"type": "object", "properties": {}}}
            ]}}))?,
            "tools/call" => {
                let name = req.pointer("/params/name").and_then(|v| v.as_str()).unwrap_or("");
                let args = req.pointer("/params/arguments").cloned().unwrap_or(json!({}));
                let text = match name {
                    "history" => tool_history(&args),
                    "search" => tool_search(&args),
                    "get_media" => tool_get_media(&args).await,
                    "chatter_brief" => tool_chatter_brief(&args),
                    "chatter_full" => tool_chatter_full(&args),
                    "chat_stats" => tool_chat_stats(),
                    _ => {
                        write_msg(&json!({"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": "unknown tool"}}))?;
                        continue;
                    }
                };
                // Any handler may return {"error": ...}; surface isError so the
                // agent never mistakes an error string for data.
                let is_err = serde_json::from_str::<Value>(&text).map(|v| v.get("error").is_some()).unwrap_or(false);
                let mut res = json!({"jsonrpc": "2.0", "id": rid, "result": {"content": [{"type": "text", "text": text}]}});
                if is_err {
                    res["result"]["isError"] = json!(true);
                }
                write_msg(&res)?;
            }
            _ => write_msg(&json!({"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": "unknown method"}}))?,
        }
    }
}
