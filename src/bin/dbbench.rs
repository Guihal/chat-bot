//! Database benchmark: the legacy statement and the optimized one, side by side
//! on ONE file and in ONE run, so "before" and "after" are measured under
//! identical conditions and their results can be compared.
//!
//! Usage: `dbbench [--rows 744,50000] [--repeat 200] [--media-files 1500]`
//!
//! Phases for every row count:
//!  1. seed a temporary database, DROP the new indexes, measure each legacy
//!     statement (the "before" world: old SQL, no index);
//!  2. run `ensure_indexes`, then measure the new calls;
//!  3. compare both outputs — an optimization that changes results is a bug,
//!     not a speed-up.
use anyhow::Result;
use chat_bot::*;
use rusqlite::{params, Connection};
use std::path::PathBuf;
use std::time::Instant;

const NEW_INDEXES: [&str; 5] = [
    "idx_messages_message_id",
    "idx_messages_date",
    "idx_messages_answered",
    "idx_messages_author_window",
    "idx_messages_gifs",
];

const GIF_PAGE_LEGACY: &str = "WITH grouped AS (
   SELECT COALESCE(NULLIF(file_unique_id,''), file_id) AS dedup_key,
          file_id, text, date, message_id,
          ROW_NUMBER() OVER (
            PARTITION BY COALESCE(NULLIF(file_unique_id,''), file_id)
            ORDER BY date DESC, message_id DESC) AS rn
   FROM messages
   WHERE file_id IS NOT NULL AND file_id <> ''
 )
 SELECT file_id, text FROM grouped WHERE rn = 1
 ORDER BY date DESC, message_id DESC LIMIT ? OFFSET ?";
const GIF_COUNT_LEGACY: &str =
    "SELECT COUNT(DISTINCT COALESCE(NULLIF(file_unique_id,''), file_id)) FROM messages WHERE file_id IS NOT NULL AND file_id <> ''";
const MSG_BY_ID_LEGACY: &str =
    "SELECT message_id,user_id,username FROM messages WHERE message_id=? LIMIT 1";
const RECENT_LEGACY: &str =
    "SELECT message_id,text FROM messages ORDER BY date DESC, message_id DESC LIMIT ?";
const ANSWERED_LEGACY: &str =
    "SELECT reply_to FROM messages WHERE user_id=? AND reply_to IS NOT NULL";
const GUARD_LEGACY: &str = "SELECT reply_to, substr(text, 1, 120) FROM messages WHERE user_id=? AND reply_to IS NOT NULL ORDER BY date DESC LIMIT ?";
const DEDUP_LEGACY: &str =
    "SELECT update_id FROM messages WHERE text=? AND username=? AND date>=? AND date<=? LIMIT 1";
const KV_LEGACY: &str = "SELECT v FROM kv WHERE k=?";

const DEDUP_TEXT: &str = "message 427 payload 870";
const DEDUP_USER: &str = "user17";
const DEDUP_FROM: i64 = 1_669_466_329;
const DEDUP_TO: i64 = 1_769_462_371;

fn main() -> Result<()> {
    let mut rows_wanted: Vec<i64> = vec![744, 50_000];
    let mut repeat: usize = 200;
    let mut media_files: usize = 1500;
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--rows" => {
                rows_wanted = args
                    .get(i + 1)
                    .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect())
                    .unwrap_or(rows_wanted);
                i += 2;
            }
            "--repeat" => {
                repeat = args.get(i + 1).and_then(|s| s.parse().ok()).unwrap_or(repeat);
                i += 2;
            }
            "--media-files" => {
                media_files = args.get(i + 1).and_then(|s| s.parse().ok()).unwrap_or(media_files);
                i += 2;
            }
            other => {
                eprintln!("unknown argument: {other}");
                std::process::exit(2);
            }
        }
    }
    println!(
        "dbbench: rows={rows_wanted:?} repeat={repeat} media_files={media_files} cpus={}",
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0)
    );
    for rows in rows_wanted {
        run_for(rows, repeat)?;
    }
    if media_files > 0 {
        bench_prune(media_files)?;
    }
    Ok(())
}

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "dbbench-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    ));
    let _ = std::fs::create_dir_all(&dir);
    dir
}

/// Deterministic corpus: 40 authors, a GIF-bearing row every 20th message, a
/// reply every 31st, and one own-answer row per 41 messages.
fn seed(con: &Connection, rows: i64) -> Result<()> {
    con.execute("BEGIN", [])?;
    {
        let mut ins = con.prepare(
            "INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id,file_unique_id) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        )?;
        for i in 0..rows {
            let user_idx = i % 40;
            let (file_id, file_unique_id) = if i % 20 == 0 {
                let n = i / 20;
                (format!("fid_{n}"), format!("uniq_{n}"))
            } else {
                (String::new(), String::new())
            };
            let reply_to: Option<i64> = if i % 31 == 0 && i > 7 { Some(1000 + i - 7) } else { None };
            ins.execute(params![
                i + 1,
                1000 + i,
                1_670_428_146 + i * 143,
                1000 + user_idx,
                format!("user{user_idx}"),
                format!("User {user_idx}"),
                format!("message {i} payload {}", i % 971),
                reply_to,
                "",
                file_id,
                file_unique_id
            ])?;
        }
    }
    con.execute("COMMIT", [])?;
    kv_set(con, "self_id", "2033")?;
    kv_set(con, "self_username", "bot")?;
    let mut ins = con.prepare(
        "INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id,file_unique_id) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
    )?;
    for i in 0..(rows / 41).max(1) {
        let q = 793 + i * 151;
        ins.execute(params![
            -(1_000_000 + i),
            900_000 + i,
            1_770_262_372 - i * 349,
            2033,
            "bot",
            "Bot",
            format!("bot answer {i}"),
            Some(1000 + q),
            "",
            "",
            ""
        ])?;
    }
    Ok(())
}

fn drop_new_indexes(con: &Connection) {
    for name in NEW_INDEXES {
        let _ = con.execute(&format!("DROP INDEX IF EXISTS {name}"), []);
    }
}

fn explain(con: &Connection, sql: &str) -> String {
    let full = format!("EXPLAIN QUERY PLAN {sql}");
    let mut stmt = match con.prepare(&full) {
        Ok(s) => s,
        Err(e) => return format!("<explain failed: {e}>"),
    };
    let mut out = Vec::new();
    if let Ok(rows) = stmt.query_map([], |r| r.get::<_, String>(3)) {
        for d in rows.flatten() {
            out.push(d);
        }
    }
    out.join(" | ")
}

/// A heap scan of `messages` — the property the indexes must remove from the hot
/// statements. Walking an index (even a full one) keeps the required order and
/// touches no table pages until the few selected rows are fetched.
fn has_heap_scan(plan: &str) -> bool {
    plan.split(" | ").any(|step| {
        let step = step.trim();
        step.starts_with("SCAN messages") && !step.contains("INDEX")
    })
}

fn trim_label(text: &str) -> String {
    text.chars().take(14).collect()
}

fn legacy_p1(con: &Connection) -> Result<String> {
    let mut stmt = con.prepare(GIF_PAGE_LEGACY)?;
    let page: Vec<String> = stmt
        .query_map(params![200i64, 0i64], |r| {
            Ok(format!(
                "{}|{}",
                r.get::<_, String>(0)?,
                r.get::<_, Option<String>>(1)?.unwrap_or_default()
            ))
        })?
        .flatten()
        .collect();
    let total: i64 = con.query_row(GIF_COUNT_LEGACY, [], |r| r.get(0))?;
    Ok(format!("total={total};{}", page.join(",")))
}

fn legacy_p2(con: &Connection) -> Result<String> {
    let mut stmt = con.prepare(MSG_BY_ID_LEGACY)?;
    let row: String = stmt
        .query_map([1042i64], |r| {
            Ok(format!(
                "{}|{}|{}",
                r.get::<_, Option<i64>>(0)?.unwrap_or_default(),
                r.get::<_, Option<i64>>(1)?.unwrap_or_default(),
                r.get::<_, Option<String>>(2)?.unwrap_or_default()
            ))
        })?
        .flatten()
        .next()
        .unwrap_or_default();
    Ok(row)
}

fn legacy_p3(con: &Connection) -> Result<String> {
    let mut stmt = con.prepare(RECENT_LEGACY)?;
    let rows: Vec<String> = stmt
        .query_map([50i64], |r| {
            Ok(format!(
                "{}|{}",
                r.get::<_, Option<i64>>(0)?.unwrap_or_default(),
                r.get::<_, Option<String>>(1)?.unwrap_or_default()
            ))
        })?
        .flatten()
        .collect();
    Ok(rows.join(","))
}

fn legacy_answered(con: &Connection) -> Result<String> {
    let mut stmt = con.prepare(ANSWERED_LEGACY)?;
    let mut ids: Vec<i64> = stmt
        .query_map([2033i64], |r| r.get::<_, Option<i64>>(0))?
        .flatten()
        .flatten()
        .collect();
    // `answered_ids` consumes this as a HashSet, so only membership matters;
    // sort to make the comparison independent of the plan's row order.
    ids.sort_unstable();
    Ok(format!("{ids:?}"))
}

fn legacy_p4(con: &Connection) -> Result<String> {
    let ids = legacy_answered(con)?;
    let mut guard = con.prepare(GUARD_LEGACY)?;
    let qids: Vec<i64> = guard
        .query_map(params![2033i64, 5i64], |r| r.get::<_, Option<i64>>(0))?
        .flatten()
        .flatten()
        .collect();
    Ok(format!("answered={ids};guard={qids:?}"))
}

fn legacy_p5(con: &Connection) -> Result<String> {
    let mut stmt = con.prepare(DEDUP_LEGACY)?;
    let id: Option<i64> = stmt
        .query_row(
            params![DEDUP_TEXT, DEDUP_USER, DEDUP_FROM, DEDUP_TO],
            |r| r.get(0),
        )
        .ok();
    Ok(format!("{id:?}"))
}

fn legacy_p6(con: &Connection) -> Result<String> {
    let v: String = con.query_row(KV_LEGACY, ["self_id"], |r| r.get(0))?;
    Ok(v)
}

fn new_p1(con: &Connection) -> Result<String> {
    let (entries, total) = gif_page(con, 0, 200)?;
    let page: Vec<String> = entries
        .iter()
        .map(|e| format!("{}|{}", e.file_id, e.text))
        .collect();
    Ok(format!("total={total};{}", page.join(",")))
}

fn new_p2(con: &Connection) -> Result<String> {
    let ids = [serde_json::json!(1042i64)];
    let rows = batch_rows_by_ids(con, &ids)?;
    Ok(rows
        .iter()
        .map(|r| format!("{}|{}|{}", r.message_id, r.user_id.unwrap_or_default(), r.username))
        .collect::<Vec<_>>()
        .join(","))
}

fn new_p3(con: &Connection) -> Result<String> {
    let rows = recent_rows(con, 50)?;
    Ok(rows
        .iter()
        .map(|r| {
            format!(
                "{}|{}",
                r.0.unwrap_or_default(),
                r.4.clone().unwrap_or_default()
            )
        })
        .collect::<Vec<_>>()
        .join(","))
}

fn guard_qids(text: &str) -> Vec<i64> {
    text.lines()
        .filter_map(|l| l.trim().strip_prefix("- ["))
        .filter_map(|rest| rest.split(']').next())
        .filter_map(|n| n.parse().ok())
        .collect()
}

fn new_p4(con: &Connection) -> Result<String> {
    let ids = legacy_answered(con)?;
    let guard = guard_qids(&answered_guard(con, 5)?);
    Ok(format!("answered={ids};guard={guard:?}"))
}

fn new_p5(con: &Connection) -> Result<String> {
    let mut stmt = con.prepare_cached(DEDUP_LEGACY)?;
    let id: Option<i64> = stmt
        .query_row(
            params![DEDUP_TEXT, DEDUP_USER, DEDUP_FROM, DEDUP_TO],
            |r| r.get(0),
        )
        .ok();
    Ok(format!("{id:?}"))
}

fn new_p6(con: &Connection) -> Result<String> {
    Ok(kv_get(con, "self_id", ""))
}

type Run = fn(&Connection) -> Result<String>;

struct Case {
    id: &'static str,
    title: &'static str,
    legacy_plan_sql: String,
    new_plan_sql: String,
    run_legacy: Run,
    run_new: Run,
}

fn lit(sql: &str, values: &[&str]) -> String {
    let mut out = sql.to_string();
    for v in values {
        out = out.replacen('?', v, 1);
    }
    out
}

fn cases() -> Vec<Case> {
    let gif_lit = |sql: &str| -> String { sql.replace("LIMIT ? OFFSET ?", "LIMIT 200 OFFSET 0") };
    vec![
        Case {
            id: "P1",
            title: "gif page + registry total",
            legacy_plan_sql: gif_lit(GIF_PAGE_LEGACY),
            new_plan_sql: gif_lit(GIF_PAGE_SQL),
            run_legacy: legacy_p1,
            run_new: new_p1,
        },
        Case {
            id: "P2",
            title: "row by message_id",
            legacy_plan_sql: lit(MSG_BY_ID_LEGACY, &["1042"]),
            new_plan_sql: lit(MSG_BY_ID_LEGACY, &["1042"]),
            run_legacy: legacy_p2,
            run_new: new_p2,
        },
        Case {
            id: "P3",
            title: "recent window",
            legacy_plan_sql: lit(RECENT_LEGACY, &["50"]),
            new_plan_sql: lit(RECENT_LEGACY, &["50"]),
            run_legacy: legacy_p3,
            run_new: new_p3,
        },
        Case {
            id: "P4",
            title: "answered ids + guard",
            legacy_plan_sql: lit(ANSWERED_LEGACY, &["2033"]),
            new_plan_sql: lit(ANSWERED_LEGACY, &["2033"]),
            run_legacy: legacy_p4,
            run_new: new_p4,
        },
        Case {
            id: "P5",
            title: "inbound dedup window",
            legacy_plan_sql: lit(
                DEDUP_LEGACY,
                &[
                    &format!("'{DEDUP_TEXT}'"),
                    &format!("'{DEDUP_USER}'"),
                    &DEDUP_FROM.to_string(),
                    &DEDUP_TO.to_string(),
                ],
            ),
            new_plan_sql: lit(
                DEDUP_LEGACY,
                &[
                    &format!("'{DEDUP_TEXT}'"),
                    &format!("'{DEDUP_USER}'"),
                    &DEDUP_FROM.to_string(),
                    &DEDUP_TO.to_string(),
                ],
            ),
            run_legacy: legacy_p5,
            run_new: new_p5,
        },
        Case {
            id: "P6",
            title: "kv_get",
            legacy_plan_sql: lit(KV_LEGACY, &["'self_id'"]),
            new_plan_sql: lit(KV_LEGACY, &["'self_id'"]),
            run_legacy: legacy_p6,
            run_new: new_p6,
        },
    ]
}

fn time_it(repeat: usize, f: Run, con: &Connection) -> Result<(f64, String)> {
    for _ in 0..5 {
        f(con)?;
    }
    let start = Instant::now();
    let mut out = String::new();
    for _ in 0..repeat {
        out = f(con)?;
    }
    Ok((start.elapsed().as_secs_f64() * 1000.0, out))
}

fn run_for(rows: i64, repeat: usize) -> Result<()> {
    let dir = temp_dir("seed");
    let state = dir.to_string_lossy().to_string();
    let con = db(&state)?;
    let seed_start = Instant::now();
    seed(&con, rows)?;
    let seed_ms = seed_start.elapsed().as_secs_f64() * 1000.0;
    drop_new_indexes(&con);
    let cases = cases();
    println!("\n=== rows={rows} (seeded in {seed_ms:.0} ms), repeat={repeat} ===");
    println!(
        "{:<3} {:<26} {:>11} {:>11} {:>8} {:>5} {:>5}",
        "#", "statement", "legacy ms", "new ms", "speedup", "old", "new"
    );

    let mut legacy_ms = Vec::new();
    let mut legacy_out = Vec::new();
    let mut legacy_plan = Vec::new();
    for case in &cases {
        legacy_plan.push(explain(&con, &case.legacy_plan_sql));
        let (ms, out) = time_it(repeat, case.run_legacy, &con)?;
        legacy_ms.push(ms / repeat as f64);
        legacy_out.push(out);
    }
    ensure_indexes(&con)?;
    let mut new_ms = Vec::new();
    let mut new_out = Vec::new();
    let mut new_plan = Vec::new();
    for case in &cases {
        new_plan.push(explain(&con, &case.new_plan_sql));
        let (ms, out) = time_it(repeat, case.run_new, &con)?;
        new_ms.push(ms / repeat as f64);
        new_out.push(out);
    }

    for (i, case) in cases.iter().enumerate() {
        let speedup = if new_ms[i] > 0.0 {
            legacy_ms[i] / new_ms[i]
        } else {
            0.0
        };
        println!(
            "{:<3} {:<26} {:>9.4}ms {:>9.4}ms {:>7.1}x {:>5} {:>5}",
            case.id,
            trim_label(case.title),
            legacy_ms[i],
            new_ms[i],
            speedup,
            if has_heap_scan(&legacy_plan[i]) { "heap" } else { "ok" },
            if has_heap_scan(&new_plan[i]) { "heap" } else { "ok" }
        );
        println!("    legacy plan: {}", legacy_plan[i]);
        println!("    new plan:    {}", new_plan[i]);
        if legacy_out[i] != new_out[i] {
            println!("    RESULT DIFFERS");
            println!("      legacy: {}", trim_label(&legacy_out[i]));
            println!("      new:    {}", trim_label(&new_out[i]));
        }
    }
    let diverged: Vec<&str> = cases
        .iter()
        .enumerate()
        .filter(|(i, _)| legacy_out[*i] != new_out[*i])
        .map(|(_, c)| c.id)
        .collect();
    if diverged.is_empty() {
        println!("equivalence: identical for all {} statements", cases.len());
    } else {
        println!("equivalence: DIVERGED for {diverged:?}");
    }
    drop(con);
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

/// Cost of the media sweep `store_prepared` runs after every insert (`read_dir`
/// plus `metadata` per file), measured at the supported scale so the
/// keep-or-throttle decision rests on a number. Reports both the raw sweep and
/// the throttled entry point that production now uses.
fn bench_prune(files: usize) -> Result<()> {
    let dir = temp_dir("prune");
    let state = dir.to_string_lossy().to_string();
    let media = dir.join("media");
    std::fs::create_dir_all(&media)?;
    for i in 0..files {
        std::fs::write(media.join(format!("f{i}.bin")), b"x")?;
    }
    for _ in 0..3 {
        maybe_prune_media_n(&state, files + 100, files - 1);
    }
    let repeat = 20;
    let start = Instant::now();
    for _ in 0..repeat {
        maybe_prune_media_n(&state, files + 100, files - 1);
    }
    let sweep_ms = start.elapsed().as_secs_f64() * 1000.0 / repeat as f64;
    maybe_prune_media(&state);
    let start = Instant::now();
    for _ in 0..repeat {
        maybe_prune_media(&state);
    }
    let throttled_ms = start.elapsed().as_secs_f64() * 1000.0 / repeat as f64;
    println!(
        "\nprune: {files} files, raw sweep {sweep_ms:.3} ms/call, throttled entry {throttled_ms:.4} ms/call => {}",
        if sweep_ms < 1.0 {
            "below 1 ms, nothing to do"
        } else {
            "above 1 ms, throttled to one sweep per 5 min in production"
        }
    );
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
