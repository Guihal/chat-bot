//! GIF catalogue: ONE owner of the record shape, the query and both renders.
//!
//! One data source (`messages` rows that Telegram itself delivered as a real
//! animation) feeds two projections:
//!
//! * the batch prompt view — at most 20 records, no total counter;
//! * the `gif_list` tool page — 30 records by default, hard cap 200.
//!
//! `GifRecord::try_new` is the ONLY constructor. It rejects exactly what is
//! genuinely invalid (empty identifiers, a kind that is not `Animation`, a
//! non-ready media state, a zero size). An empty caption and "no signal when
//! this GIF fits" are valid, distinct states, not errors.
use anyhow::{anyhow, Result};
use serde_json::Value;

/// Prompt projection cap (DESIGN §5/§6).
pub const PROMPT_MAX: i64 = 20;
/// Tool page size and hard maximum (DESIGN §6).
pub const LIST_PAGE_DEFAULT: i64 = 30;
pub const LIST_PAGE_MAX: i64 = 200;

/// Media kinds the catalogue explicitly refuses. Kept as a closed set so a
/// sticker, document or video can never be mistaken for an animation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaKind {
    Animation,
    Video,
    Photo,
    Document,
    Sticker,
    Voice,
    Other,
}

impl MediaKind {
    pub fn parse(raw: &str) -> MediaKind {
        match raw.trim().to_ascii_lowercase().as_str() {
            "animation" => MediaKind::Animation,
            "video" => MediaKind::Video,
            "photo" => MediaKind::Photo,
            "document" => MediaKind::Document,
            "sticker" => MediaKind::Sticker,
            "voice" => MediaKind::Voice,
            _ => MediaKind::Other,
        }
    }
}

/// Empty caption and a caption are different states; the prompt renders them
/// differently and neither is an error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Caption {
    Absent,
    Text(String),
}

/// "When this GIF fits" is a hint, not a requirement. No hint is Unknown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FitsWhen {
    Reply(String),
    Moment(String),
    Unknown,
}

/// One deduplicated GIF. Private fields force every record through `try_new`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GifRecord {
    file_id: String,
    file_unique_id: String,
    caption: Caption,
    fits: FitsWhen,
    size: u64,
}

/// Everything a catalogue row must provide. The query fills it; `try_new` is
/// the one place that decides whether it is a record at all.
#[derive(Debug, Clone, Default)]
pub struct GifSource<'a> {
    pub media_kind: &'a str,
    pub media_state: &'a str,
    pub file_id: &'a str,
    pub file_unique_id: &'a str,
    pub size: u64,
    pub caption: Option<&'a str>,
    /// Text of the message the GIF answered, if any.
    pub reply_hint: Option<&'a str>,
    /// Text of the message right before it, if any.
    pub moment_hint: Option<&'a str>,
}

impl GifRecord {
    /// The single constructor. Rejects only genuinely wrong input; empty
    /// caption and an absent fit hint are represented, not rejected.
    pub fn try_new(src: GifSource<'_>) -> Result<GifRecord> {
        if MediaKind::parse(src.media_kind) != MediaKind::Animation {
            return Err(anyhow!("not an animation: {}", src.media_kind));
        }
        // An animation is a catalogue entry when its identifiers are real and
        // its size is known. Local bytes are NOT part of the contract: the wipe
        // removes the file and keeps the row as `evicted`, while sending works
        // by `file_id`.
        if !matches!(src.media_state, "ready" | "descriptor" | "evicted") {
            return Err(anyhow!("media is not listed: {}", src.media_state));
        }
        if src.file_id.trim().is_empty() {
            return Err(anyhow!("empty file_id"));
        }
        if src.file_unique_id.trim().is_empty() {
            return Err(anyhow!("empty file_unique_id"));
        }
        if src.size == 0 {
            return Err(anyhow!("zero media size"));
        }
        let caption = match src.caption.map(str::trim).filter(|s| !s.is_empty()) {
            Some(text) => Caption::Text(text.to_string()),
            None => Caption::Absent,
        };
        let fits = match (
            src.reply_hint.map(str::trim).filter(|s| !s.is_empty()),
            src.moment_hint.map(str::trim).filter(|s| !s.is_empty()),
        ) {
            (Some(text), _) => FitsWhen::Reply(text.to_string()),
            (None, Some(text)) => FitsWhen::Moment(text.to_string()),
            (None, None) => FitsWhen::Unknown,
        };
        Ok(GifRecord {
            file_id: src.file_id.trim().to_string(),
            file_unique_id: src.file_unique_id.trim().to_string(),
            caption,
            fits,
            size: src.size,
        })
    }

    pub fn file_id(&self) -> &str {
        &self.file_id
    }

    pub fn file_unique_id(&self) -> &str {
        &self.file_unique_id
    }

    pub fn caption(&self) -> &Caption {
        &self.caption
    }

    pub fn fits(&self) -> &FitsWhen {
        &self.fits
    }

    pub fn size(&self) -> u64 {
        self.size
    }
}

/// One page of the single source. `offset` makes the 1-based `gif:N` marker
/// stable across pages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GifPage {
    pub records: Vec<GifRecord>,
    pub offset: i64,
    pub total: i64,
}

/// Parse the `gif_list` bounds. Never clamps: a negative offset or a limit
/// outside `1..=LIST_PAGE_MAX` is an explicit `bad offset` / `bad limit`.
pub fn parse_page_args(args: &Value) -> Result<(i64, i64)> {
    let offset = match args.get("offset") {
        None | Some(Value::Null) => 0,
        Some(Value::Number(n)) => match n.as_i64() {
            Some(v) if v >= 0 => v,
            Some(v) => return Err(anyhow!("bad offset: must be >= 0, got {v}")),
            None => return Err(anyhow!("bad offset: must be an integer")),
        },
        Some(_) => return Err(anyhow!("bad offset: must be an integer")),
    };
    let limit = match args.get("limit") {
        None | Some(Value::Null) => LIST_PAGE_DEFAULT,
        Some(Value::Number(n)) => match n.as_i64() {
            Some(v) if (1..=LIST_PAGE_MAX).contains(&v) => v,
            Some(v) if v < 1 => {
                return Err(anyhow!(
                    "bad limit: must be between 1 and {LIST_PAGE_MAX}, got {v}"
                ))
            }
            Some(v) => {
                return Err(anyhow!(
                    "bad limit: must be at most {LIST_PAGE_MAX}, got {v}"
                ))
            }
            None => return Err(anyhow!("bad limit: must be an integer")),
        },
        Some(_) => return Err(anyhow!("bad limit: must be an integer")),
    };
    Ok((offset, limit))
}

/// Plain row of one catalogue page as the data layer returns it.
#[derive(Debug, Clone, Default)]
pub struct GifRow {
    pub file_id: String,
    pub file_unique_id: String,
    pub text: Option<String>,
    pub size: i64,
    pub media_kind: Option<String>,
    pub media_state: Option<String>,
    pub context: Option<String>,
}

/// Build the typed records from one data-layer page. A row that cannot pass
/// the constructor is dropped, never rendered as an invented GIF.
pub fn records_from(rows: Vec<GifRow>) -> Vec<GifRecord> {
    let mut records = Vec::new();
    for row in rows {
        let src = GifSource {
            media_kind: row.media_kind.as_deref().unwrap_or(""),
            media_state: row.media_state.as_deref().unwrap_or(""),
            file_id: &row.file_id,
            file_unique_id: row.file_unique_id.as_str().trim(),
            size: row.size.max(0) as u64,
            caption: row.text.as_deref(),
            reply_hint: row.context.as_deref(),
            moment_hint: None,
        };
        if let Ok(record) = GifRecord::try_new(src) {
            records.push(record);
        }
    }
    records
}

/// Render the prompt projection from already-loaded records. This is the only
/// renderer for that view, so the fresh and rebuilt context cannot differ.
pub fn render_prompt(records: &[GifRecord]) -> String {
    if records.is_empty() {
        return String::new();
    }
    let lines: Vec<String> = records
        .iter()
        .enumerate()
        .map(|(i, r)| record_line(i as i64 + 1, r))
        .collect();
    format!(
        "Гифки, уже побывавшие в чате (только настоящие анимации; подпись — что было под гифкой, момент — на что её отвечали, подсказка, когда уместно). Чтобы отправить — передай file_id в параметр gif_file_id тула chat_send, текст рядом станет подписью. Полный список — тул gif_list:\n{}",
        lines.join("\n")
    )
}

/// Wrap a data-layer page in the typed projection.
pub fn page_of(rows: Vec<GifRow>, offset: i64, total: i64) -> GifPage {
    GifPage {
        records: records_from(rows),
        offset,
        total,
    }
}

/// Tool projection: one page plus the total counter. The page size is the
/// caller's validated limit; the render does not clamp anything either.
pub fn list_view(page: GifPage) -> String {
    if page.records.is_empty() {
        if page.total == 0 {
            return "Гифок в истории пока нет (только подтверждённые анимации; старые записи без типа не угадываются)."
                .to_string();
        }
        return format!(
            "Страница пуста: всего гифок {}, запрошено со смещения {}. Уменьши offset.",
            page.total, page.offset
        );
    }
    let lines: Vec<String> = page
        .records
        .iter()
        .enumerate()
        .map(|(i, r)| record_line(page.offset + i as i64 + 1, r))
        .collect();
    format!(
        "Все известные гифки ({} из {}):\n{}",
        page.records.len(),
        page.total,
        lines.join("\n")
    )
}

/// Prompt projection: at most `max` (capped at 20) records, no total counter.
/// Empty string when the catalogue is empty, so the renderer can omit it.
pub fn prompt_view(rows: Vec<GifRow>, max: i64) -> String {
    let limit = max.clamp(1, PROMPT_MAX) as usize;
    let mut records = records_from(rows);
    records.truncate(limit);
    render_prompt(&records)
}

fn record_line(idx: i64, r: &GifRecord) -> String {
    let mut line = format!("[gif:{}] file_id={}", idx, r.file_id());
    match r.caption() {
        Caption::Absent => line.push_str(" · (без подписи)"),
        Caption::Text(text) => line.push_str(&format!(" · «{text}»")),
    }
    match r.fits() {
        FitsWhen::Reply(text) => line.push_str(&format!(" · момент: «{}»", one_line(text))),
        FitsWhen::Moment(text) => line.push_str(&format!(" · момент: «{}»", one_line(text))),
        FitsWhen::Unknown => {}
    }
    line
}

const HINT_CAP: usize = 160;

fn one_line(text: &str) -> String {
    let joined = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let cut: String = joined.chars().take(HINT_CAP).collect();
    if joined.chars().count() > HINT_CAP {
        format!("{cut}…")
    } else {
        cut
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use crate::db;

    fn temp_state(tag: &str) -> String {
        let dir = std::env::temp_dir().join(format!("chat-cat-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir.to_string_lossy().to_string()
    }

    fn load_page(con: &Connection, offset: i64, limit: i64) -> GifPage {
        let (rows, total) = crate::store::catalog_page(con, offset, limit).unwrap();
        page_of(rows, offset, total)
    }

    fn insert(
        con: &Connection,
        uid: i64,
        mid: i64,
        kind: &str,
        state: &str,
        file_id: &str,
        unique: &str,
        size: i64,
        text: &str,
        reply_to: Option<i64>,
    ) {
        con.execute(
            "INSERT INTO messages(update_id,message_id,date,user_id,username,first_name,text,reply_to,media_path,file_id,file_unique_id,media_kind,media_state,media_size) VALUES (?,?,?,7,'u','U',?,?,'',?,?,?,?,?)",
            rusqlite::params![uid, mid, 1000 + uid, text, reply_to, file_id, unique, kind, state, size],
        )
        .unwrap();
    }

    fn src<'a>(
        kind: &'a str,
        state: &'a str,
        fid: &'a str,
        unique: &'a str,
        size: u64,
    ) -> GifSource<'a> {
        GifSource {
            media_kind: kind,
            media_state: state,
            file_id: fid,
            file_unique_id: unique,
            size,
            ..Default::default()
        }
    }

    #[test]
    fn constructor_rejects_only_genuinely_invalid_input() {
        assert!(GifRecord::try_new(src("video", "ready", "f", "u", 10)).is_err());
        assert!(GifRecord::try_new(src("sticker", "ready", "f", "u", 10)).is_err());
        assert!(GifRecord::try_new(src("document", "ready", "f", "u", 10)).is_err());
        assert!(GifRecord::try_new(src("animation", "pending", "f", "u", 10)).is_err());
        assert!(GifRecord::try_new(src("animation", "absent", "f", "u", 10)).is_err());
        assert!(GifRecord::try_new(src("animation", "ready", "", "u", 10)).is_err());
        assert!(GifRecord::try_new(src("animation", "ready", "f", "", 10)).is_err());
        assert!(GifRecord::try_new(src("animation", "ready", "f", "u", 0)).is_err());
        let empty_caption = GifRecord::try_new(src("animation", "ready", "f", "u", 10)).unwrap();
        assert_eq!(empty_caption.caption(), &Caption::Absent);
        assert_eq!(empty_caption.fits(), &FitsWhen::Unknown);
    }

    /// A wipe removes the bytes, not the entry: an animation whose file is gone
    /// stays catalogued because sending works by `file_id` and the size is kept.
    #[test]
    fn evicted_and_descriptor_animations_stay_catalogued() {
        for state in ["ready", "descriptor", "evicted"] {
            let record = GifRecord::try_new(src("animation", state, "f", "u", 10))
                .unwrap_or_else(|e| panic!("{state} must be catalogued: {e}"));
            assert_eq!(record.file_id(), "f");
            assert_eq!(record.size(), 10);
        }
    }

    #[test]
    fn only_real_animations_are_catalogued_and_caption_vs_fit_stay_distinct() {
        let state = temp_state("kinds");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let con = db(&state).unwrap();
        insert(
            &con,
            1,
            501,
            "animation",
            "ready",
            "fid_b",
            "uniq_b",
            4096,
            "подпись",
            None,
        );
        insert(
            &con,
            2,
            500,
            "",
            "absent",
            "",
            "",
            0,
            "обычная реплика",
            None,
        );
        insert(
            &con,
            3,
            502,
            "animation",
            "ready",
            "fid_a",
            "uniq_a",
            4096,
            "",
            Some(500),
        );
        insert(
            &con,
            4,
            503,
            "video",
            "ready",
            "fid_v",
            "uniq_v",
            4096,
            "видео",
            None,
        );
        insert(
            &con,
            5,
            504,
            "document",
            "ready",
            "fid_d",
            "uniq_d",
            4096,
            "документ",
            None,
        );
        insert(
            &con,
            6,
            505,
            "sticker",
            "ready",
            "fid_s",
            "uniq_s",
            4096,
            "стикер",
            None,
        );
        insert(
            &con,
            7,
            506,
            "animation",
            "ready",
            "fid_n",
            "",
            4096,
            "без unique",
            None,
        );
        insert(
            &con,
            8,
            507,
            "animation",
            "pending",
            "fid_x",
            "uniq_x",
            4096,
            "ещё качается",
            None,
        );
        insert(
            &con,
            9,
            508,
            "animation",
            "ready",
            "fid_z",
            "uniq_z",
            0,
            "нулевой размер",
            None,
        );

        let (rows, total) = crate::store::catalog_page(&con, 0, 200).unwrap();
        let first = page_of(rows, 0, total);
        assert_eq!(
            first.total, 2,
            "non-animations leaked into the catalogue: {first:?}"
        );
        assert_eq!(first.records.len(), 2);
        let a = &first.records[0];
        assert_eq!(a.file_id(), "fid_a");
        assert_eq!(a.caption(), &Caption::Absent);
        assert_eq!(a.fits(), &FitsWhen::Reply("обычная реплика".to_string()));

        let (rows, total) = crate::store::catalog_page(&con, 0, 20).unwrap();
        let both = page_of(rows, 0, total);
        assert_eq!(both.total, 2, "{both:?}");
        assert_eq!(both.records[0].file_id(), "fid_a");
        assert_eq!(both.records[1].file_id(), "fid_b");
        assert_eq!(
            both.records[0].caption(),
            &Caption::Absent,
            "empty caption stayed empty"
        );
        assert_eq!(
            both.records[1].caption(),
            &Caption::Text("подпись".to_string())
        );
        assert_eq!(
            both.records[1].fits(),
            &FitsWhen::Unknown,
            "a caption is not a fit hint"
        );

        let prompt = render_prompt(&both.records);
        assert!(
            prompt.contains("[gif:1]") && prompt.contains("fid_a"),
            "{prompt}"
        );
        assert!(
            prompt.contains("(без подписи)"),
            "empty caption state lost: {prompt}"
        );
        assert!(prompt.contains("· момент: «обычная реплика»"), "{prompt}");
        for absent in ["fid_v", "fid_d", "fid_s", "fid_n", "fid_x", "fid_z"] {
            assert!(!prompt.contains(absent), "{absent} leaked: {prompt}");
        }
        let _ = std::fs::remove_dir_all(&state);
    }

    #[test]
    fn paging_parser_rejects_bad_bounds_instead_of_clamping() {
        assert_eq!(parse_page_args(&serde_json::json!({})).unwrap(), (0, 30));
        assert_eq!(
            parse_page_args(&serde_json::json!({"offset": 5, "limit": 200})).unwrap(),
            (5, 200)
        );
        for (args, needle) in [
            (serde_json::json!({"offset": -1}), "bad offset"),
            (serde_json::json!({"offset": "7"}), "bad offset"),
            (serde_json::json!({"offset": 1.5}), "bad offset"),
            (serde_json::json!({"limit": 0}), "bad limit"),
            (serde_json::json!({"limit": -3}), "bad limit"),
            (serde_json::json!({"limit": 201}), "bad limit"),
            (serde_json::json!({"limit": "30"}), "bad limit"),
        ] {
            let err = parse_page_args(&args).unwrap_err().to_string();
            assert!(err.contains(needle), "{args} -> {err}");
        }
    }

    #[test]
    fn list_total_is_truthful_at_page_and_offset_boundaries() {
        let state = temp_state("pages");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let con = db(&state).unwrap();
        for i in 0..40 {
            let fid = format!("fid_p{i:02}");
            let uniq = format!("uniq_p{i:02}");
            insert(
                &con,
                200 + i,
                2000 + i,
                "animation",
                "ready",
                &fid,
                &uniq,
                2048,
                &format!("подпись {i}"),
                None,
            );
        }
        let (rows, total) = crate::store::catalog_page(&con, 0, 30).unwrap();
        let first = page_of(rows, 0, total);
        assert_eq!(
            first.total, 40,
            "a non-empty page lost the whole-catalogue total"
        );
        assert_eq!(first.records.len(), 30);
        let list = list_view(load_page(&con, 0, 30));
        assert!(list.contains("30 из 40"), "{list}");
        let beyond = list_view(load_page(&con, 35, 30));
        assert!(beyond.contains("5 из 40"), "{beyond}");
        let empty = list_view(load_page(&con, 100, 30));
        assert_eq!(
            crate::store::catalog_page(&con, 100, 30).unwrap().1,
            40,
            "an empty page reported total 0"
        );
        assert!(
            empty.contains("Страница пуста") && empty.contains("40"),
            "an offset past the end claimed an empty catalogue: {empty}"
        );
        assert!(!empty.contains("Гифок в истории пока нет"), "{empty}");
        let _ = std::fs::remove_dir_all(&state);
    }

    #[test]
    fn prompt_view_caps_at_twenty_without_a_total_counter() {
        let state = temp_state("prompt");
        std::env::set_var("CHATBOT_CHAT_ID", "-100");
        let con = db(&state).unwrap();
        for i in 0..25 {
            let fid = format!("fid_{i:02}");
            let uniq = format!("uniq_{i:02}");
            insert(
                &con,
                100 + i,
                1000 + i,
                "animation",
                "ready",
                &fid,
                &uniq,
                2048,
                &format!("подпись {i}"),
                None,
            );
        }
        let prompt = prompt_view(crate::store::catalog_page(&con, 0, 50).unwrap().0, 50);
        assert_eq!(prompt.matches("[gif:").count(), PROMPT_MAX as usize);
        assert!(
            !prompt.contains("из 25"),
            "prompt leaked a total counter: {prompt}"
        );

        let list = list_view(load_page(&con, 0, LIST_PAGE_DEFAULT));
        assert_eq!(list.matches("[gif:").count(), 25);
        assert!(
            list.contains("25 из 25"),
            "tool projection needs the total: {list}"
        );
        assert_eq!(crate::store::catalog_page(&con, 0, 200).unwrap().1, 25);
        assert!(list_view(load_page(&con, 20, 30)).contains("[gif:21]"));
        let _ = std::fs::remove_dir_all(&state);
    }
}
