//! Managed knowledge documents: parsing, merging and rendering.
//!
//! Contract:
//! * the managed zone is bounded by [`BEGIN`] / [`END`]; every byte outside it
//!   belongs to other writers (`seed-knowledge.sh`, a human) and is preserved
//!   verbatim — the knowledge run may only ever own the zone;
//! * an entry is `### <key>`, then `- обновлено: <day>`, optionally
//!   `- источники: <urls|нет>` (persons), then free text;
//! * a re-run with the same key UPDATES the entry instead of appending a
//!   second one (dedup by normalized key), and a fresh non-empty body/sources
//!   replace the old ones while an empty one keeps them — a failed second
//!   lookup must not erase a confirmed link;
//! * no SQL and no file IO: the caller reads and atomically writes the file.
//! * a run owns ONE document: the per-document cursor is `<date>:<message_id>`,
//!   the run digest is sha256(batch + that document's bytes), and the
//!   scoped hash-guard only compares the target path.
//!
//! The model answers records, never a file, so this module — not model
//! diligence — decides what the document looks like.

use anyhow::{anyhow, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const BEGIN: &str = "<!-- knowledge:begin -->";
pub const END: &str = "<!-- knowledge:end -->";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Discourse,
    Persons,
    Dictionary,
    Donors,
    Events,
}

impl Kind {
    pub fn file_name(self) -> &'static str {
        match self {
            Kind::Discourse => "Дискурс.md",
            Kind::Persons => "Важные персоны вне канала.md",
            Kind::Dictionary => "Словарь.md",
            Kind::Donors => "Список легенд(донатеров).md",
            Kind::Events => "Список важных событий.md",
        }
    }

    pub fn title(self) -> &'static str {
        match self {
            Kind::Discourse => "Дискурс",
            Kind::Persons => "Важные персоны вне канала",
            Kind::Dictionary => "Словарь",
            Kind::Donors => "Список легенд (донатеры)",
            Kind::Events => "Список важных событий",
        }
    }

    /// Only persons carry web sources; the other documents keep their evidence
    /// in the body text so their existing format does not change.
    pub fn has_sources(self) -> bool {
        matches!(self, Kind::Persons)
    }

    /// CLI/document slug: the stable identity in job names and kv keys.
    pub fn slug(self) -> &'static str {
        match self {
            Kind::Discourse => "discourse",
            Kind::Persons => "persons",
            Kind::Dictionary => "dictionary",
            Kind::Donors => "donors",
            Kind::Events => "events",
        }
    }

    pub fn from_slug(slug: &str) -> Option<Kind> {
        match slug.trim() {
            "discourse" => Some(Kind::Discourse),
            "persons" => Some(Kind::Persons),
            "dictionary" => Some(Kind::Dictionary),
            "donors" => Some(Kind::Donors),
            "events" => Some(Kind::Events),
            _ => None,
        }
    }

    /// The JSON array the model must fill for this run. Other arrays stay
    /// empty and are ignored by the merge, so one job cannot write another
    /// document.
    pub fn array_field(self) -> &'static str {
        match self {
            Kind::Discourse => "discourse",
            Kind::Persons => "persons",
            Kind::Dictionary => "dictionary",
            Kind::Donors => "donors",
            Kind::Events => "events",
        }
    }
}

pub fn kinds() -> [Kind; 5] {
    [
        Kind::Discourse,
        Kind::Persons,
        Kind::Dictionary,
        Kind::Donors,
        Kind::Events,
    ]
}

/// Persisted high-water mark for one document run, in the mirror's order:
/// `(date, message_id)`, never `update_id` (own replies carry negative ids).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Cursor {
    pub date: i64,
    pub message_id: i64,
}

impl Cursor {
    /// Parse the persisted `knowledge:cursor:<slug>` value. An unparsable value
    /// is an error, never a silent reset to the beginning of history.
    pub fn parse(raw: &str) -> Result<Self> {
        let raw = raw.trim();
        let (date, message_id) = raw
            .split_once(':')
            .ok_or_else(|| anyhow!("bad knowledge cursor {raw:?}: expected <date>:<message_id>"))?;
        Ok(Cursor {
            date: date
                .parse()
                .map_err(|e| anyhow!("bad knowledge cursor date {date:?}: {e}"))?,
            message_id: message_id
                .parse()
                .map_err(|e| anyhow!("bad knowledge cursor message_id {message_id:?}: {e}"))?,
        })
    }

    pub fn format(self) -> String {
        format!("{}:{}", self.date, self.message_id)
    }
}

/// The rolling window and the exclusive upper bound for a run that starts now.
/// The cursor may be older than this window; the caller scans from the cursor
/// then, so a cap-limited backlog continues on the next hourly run.
pub fn window_plan(now: i64, window_min: i64) -> (i64, i64) {
    let window = window_min.max(0).saturating_mul(60);
    (now.saturating_sub(window), now)
}

/// Start of the previous local calendar day. The first per-document run must
/// start here, not at `now - window`: the old daily job covered the previous
/// day and the current day would otherwise be skipped at cutover.
pub fn previous_local_day_start(now: i64, offset_min: i32) -> Result<i64> {
    let tz = chrono::FixedOffset::east_opt(offset_min * 60)
        .ok_or_else(|| anyhow!("bad knowledge UTC offset {offset_min}"))?;
    let local = chrono::DateTime::from_timestamp(now, 0)
        .ok_or_else(|| anyhow!("bad timestamp {now}"))?
        .with_timezone(&tz)
        .date_naive();
    let previous = local - chrono::Duration::days(1);
    Ok(previous
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| anyhow!("cannot resolve previous local midnight"))?
        .and_utc()
        .timestamp()
        - i64::from(offset_min) * 60)
}

pub fn behind_window(cursor_date: i64, from: i64) -> bool {
    cursor_date < from
}

/// Deduplication identity: the exact batch text and the exact target document
/// bytes. An identical pair means the previous successful run already applied
/// this material; a changed document must not be skipped.
pub fn run_digest(batch_text: &str, doc_bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(batch_text.as_bytes());
    h.update([0u8]);
    h.update(doc_bytes);
    format!("{:x}", h.finalize())
}

/// Append one chunk to the digest text with exactly one separator between
/// chunks and no trailing newline. Both the full-batch comparison and the
/// per-chunk stored prefix must use this function so their bytes match.
pub fn append_batch(prefix: &mut String, batch: &str) {
    if !prefix.is_empty() {
        prefix.push('\n');
    }
    prefix.push_str(batch);
}

/// Per-run scratch directory name. The slug and pid make it impossible for one
/// document's run to remove another document's scratch directory.
pub fn scratch_name(slug: &str, pid: u32) -> String {
    format!("knowledge-{slug}-{pid}")
}

/// One document entry: a stable key plus its current body.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Entry {
    pub key: String,
    pub updated: String,
    pub sources: Vec<String>,
    pub body: String,
}

/// A whole file split around the managed zone.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Doc {
    /// Everything before [`BEGIN`] (for a freshly created file: the title).
    pub prefix: String,
    /// Non-entry text inside the zone, kept so a foreign note is not lost.
    pub preamble: String,
    pub entries: Vec<Entry>,
    /// Everything after [`END`].
    pub suffix: String,
    pub managed: bool,
}

/// One record the model returned for one chunk.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Record {
    pub key: String,
    pub text: String,
    pub sources: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MergeStats {
    pub added: usize,
    pub updated: usize,
}

/// What one chunk's answer carries.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Chunk {
    /// Whether the session could actually reach the web. A missing signal is
    /// an error, not a silent "no persons today".
    pub web_ok: bool,
    pub web_reason: String,
    pub discourse: Vec<Record>,
    pub persons: Vec<Record>,
    pub dictionary: Vec<Record>,
    pub donors: Vec<Record>,
    pub events: Vec<Record>,
}

impl Chunk {
    /// The records belonging to one managed document. A per-document run
    /// ignores the other arrays: another document's material is that job's
    /// responsibility and its own cursor's range.
    pub fn records_for(&self, kind: Kind) -> &[Record] {
        match kind {
            Kind::Discourse => &self.discourse,
            Kind::Persons => &self.persons,
            Kind::Dictionary => &self.dictionary,
            Kind::Donors => &self.donors,
            Kind::Events => &self.events,
        }
    }
}

impl Entry {
    fn push_line(&mut self, line: &str) {
        let t = line.trim();
        if t.is_empty() {
            return;
        }
        if let Some(v) = t.strip_prefix("- обновлено:") {
            self.updated = v.trim().to_string();
            return;
        }
        if let Some(v) = t.strip_prefix("- источники:") {
            let v = v.trim();
            if v != "нет" && !v.is_empty() {
                self.sources = v
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty() && s != "нет")
                    .collect();
            }
            return;
        }
        if !self.body.is_empty() {
            self.body.push('\n');
        }
        self.body.push_str(t);
    }
}

/// A fresh file: a title above an empty managed zone.
pub fn new_document(kind: Kind) -> String {
    format!("# {}\n\n", kind.title())
}

pub fn parse_managed(text: &str) -> Doc {
    let (Some(b), Some(e)) = (text.find(BEGIN), text.find(END)) else {
        return Doc {
            prefix: text.to_string(),
            ..Default::default()
        };
    };
    if e < b {
        return Doc {
            prefix: text.to_string(),
            ..Default::default()
        };
    }
    let prefix = text[..b].to_string();
    let raw_suffix = &text[e + END.len()..];
    let suffix = raw_suffix.strip_prefix('\n').unwrap_or(raw_suffix).to_string();
    let zone = &text[b + BEGIN.len()..e];
    let zone = zone.strip_prefix('\n').unwrap_or(zone);
    let mut preamble = String::new();
    let mut entries: Vec<Entry> = vec![];
    for line in zone.lines() {
        let t = line.trim_end();
        if let Some(rest) = t.trim_start().strip_prefix("### ") {
            entries.push(Entry {
                key: rest.trim().to_string(),
                ..Default::default()
            });
            continue;
        }
        match entries.last_mut() {
            Some(entry) => entry.push_line(t),
            None => {
                if !t.trim().is_empty() {
                    preamble.push_str(t);
                    preamble.push('\n');
                }
            }
        }
    }
    Doc {
        prefix,
        preamble,
        entries,
        suffix,
        managed: true,
    }
}

/// Dedup key: case-folded, whitespace runs collapsed to a single `-`, quotes
/// dropped (the model may quote a term). Cyrillic is preserved as-is.
pub fn normalize_key(key: &str) -> String {
    let mut out = String::new();
    for ch in key.trim().chars() {
        if ch.is_whitespace() || ch == '-' || ch == '_' {
            if !out.is_empty() && !out.ends_with('-') {
                out.push('-');
            }
            continue;
        }
        if matches!(ch, '`' | '"' | '\'' | '«' | '»') {
            continue;
        }
        for low in ch.to_lowercase() {
            out.push(low);
        }
    }
    out.trim_matches('-').to_string()
}

/// Merge records into the doc. Same key → update (never a second entry);
/// empty body/sources keep the previous value.
pub fn merge(doc: &mut Doc, records: &[Record], day: &str) -> MergeStats {
    let mut stats = MergeStats::default();
    for rec in records {
        let key = normalize_key(&rec.key);
        if key.is_empty() {
            continue;
        }
        let text = rec.text.trim();
        if let Some(e) = doc
            .entries
            .iter_mut()
            .find(|e| normalize_key(&e.key) == key)
        {
            e.key = key;
            e.updated = day.to_string();
            if !text.is_empty() {
                e.body = text.to_string();
            }
            if !rec.sources.is_empty() {
                e.sources = rec.sources.clone();
            }
            stats.updated += 1;
        } else {
            doc.entries.push(Entry {
                key,
                updated: day.to_string(),
                sources: rec.sources.clone(),
                body: text.to_string(),
            });
            stats.added += 1;
        }
    }
    stats
}

pub fn render(doc: &Doc, kind: Kind) -> String {
    let mut out = String::new();
    out.push_str(&doc.prefix);
    if !out.is_empty() && !out.ends_with('\n') {
        out.push('\n');
    }
    out.push_str(BEGIN);
    out.push('\n');
    out.push_str(&doc.preamble);
    for e in &doc.entries {
        out.push_str(&format!("### {}\n", e.key));
        out.push_str(&format!("- обновлено: {}\n", e.updated));
        if kind.has_sources() {
            let sources = if e.sources.is_empty() {
                "нет".to_string()
            } else {
                e.sources.join(", ")
            };
            out.push_str(&format!("- источники: {sources}\n"));
        }
        let body = e.body.trim();
        if !body.is_empty() {
            out.push_str(body);
            out.push('\n');
        }
        out.push('\n');
    }
    out.push_str(END);
    out.push('\n');
    out.push_str(&doc.suffix);
    out
}

/// Existing keys of the managed zone, for the prompt (the model is told to
/// reuse a key when the topic is the same; dedup itself is by key below).
pub fn keys_of(doc: &Doc, limit: usize) -> Vec<String> {
    doc.entries
        .iter()
        .map(|e| e.key.clone())
        .take(limit)
        .collect()
}

/// What the chunk prompt shows about the document: the entries already written
/// on THIS day WITH a body excerpt, then the bare keys of the other days.
/// A bare key list is not enough for convergence: a re-run sees the same chunk,
/// groups it into slightly different topics and appends a second partition of
/// the day. Showing what the key already covers lets the model update that
/// entry instead of naming the same material again.
pub fn existing_digest(doc: &Doc, day: &str, limit: usize) -> String {
    let clip = |s: &str| -> String {
        let one = s.split_whitespace().collect::<Vec<_>>().join(" ");
        let mut out: String = one.chars().take(140).collect();
        if out.chars().count() < one.chars().count() {
            out.push('…');
        }
        out
    };
    let mut lines: Vec<String> = vec![];
    for e in doc.entries.iter().filter(|e| e.updated == day).take(limit) {
        lines.push(format!("{} — {}", e.key, clip(&e.body)));
    }
    let today = lines.len();
    for e in doc
        .entries
        .iter()
        .filter(|e| e.updated != day)
        .take(limit.saturating_sub(today))
    {
        lines.push(e.key.clone());
    }
    lines.join("\n")
}

/// A tool call counts as web research: the builtin `web_search`/`web_fetch`
/// and any browser MCP server the profile registers. Session `state` does not
/// enumerate tools, so this — a real call in the transcript — is the only
/// honest proof that the persons step could reach the web.
pub fn is_web_tool(name: &str) -> bool {
    let n = name.to_lowercase();
    [
        "web_search",
        "web_fetch",
        "web-inspector",
        "web_inspector",
        "playwright",
        "browser-inspector",
        "browser_inspector",
        "navigate",
    ]
    .iter()
    .any(|needle| n.contains(needle))
}

/// Number of web-tool calls in one message's content array.
pub fn web_tool_calls(content: &Value) -> usize {
    let Some(items) = content.as_array() else {
        return 0;
    };
    items
        .iter()
        .filter(|c| {
            c.get("type")
                .and_then(|t| t.as_str())
                .map(|t| t.contains("tool"))
                .unwrap_or(false)
        })
        .filter(|c| {
            c.get("name")
                .and_then(|n| n.as_str())
                .map(is_web_tool)
                .unwrap_or(false)
        })
        .count()
}

fn records(v: &Value, field: &str) -> Result<Vec<Record>> {
    let Some(arr) = v.get(field) else {
        return Ok(vec![]);
    };
    let arr = arr
        .as_array()
        .ok_or_else(|| anyhow!("`{field}` is not an array"))?;
    let mut out = vec![];
    for (i, item) in arr.iter().enumerate() {
        let key = item
            .get("key")
            .and_then(|k| k.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if key.is_empty() {
            return Err(anyhow!("{field}[{i}] has no key"));
        }
        let text = item
            .get("body")
            .or_else(|| item.get("text"))
            .and_then(|t| t.as_str())
            .unwrap_or("")
            .to_string();
        let sources: Vec<String> = item
            .get("sources")
            .and_then(|s| s.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|s| s.as_str())
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        out.push(Record {
            key,
            text,
            sources,
        });
    }
    Ok(out)
}

/// Parse the model's strict JSON answer. The first `{` .. last `}` window
/// tolerates a stray fence or a leading sentence; anything else is an error,
/// because a chunk whose answer cannot be read must fail the run, not vanish.
pub fn parse_model_json(answer: &str) -> Result<Chunk> {
    let start = answer
        .find('{')
        .ok_or_else(|| anyhow!("model answer has no JSON object"))?;
    let end = answer
        .rfind('}')
        .ok_or_else(|| anyhow!("model answer has no JSON object"))?;
    if end < start {
        return Err(anyhow!("model answer has no JSON object"));
    }
    let v: Value = serde_json::from_str(&answer[start..=end])
        .map_err(|e| anyhow!("model answer is not valid JSON: {e}"))?;
    let web = v
        .get("web")
        .and_then(|w| w.as_object())
        .ok_or_else(|| anyhow!("model answer has no web signal"))?;
    let web_ok = web
        .get("ok")
        .and_then(|b| b.as_bool())
        .ok_or_else(|| anyhow!("web.ok is not a boolean"))?;
    Ok(Chunk {
        web_ok,
        web_reason: web
            .get("reason")
            .and_then(|r| r.as_str())
            .unwrap_or("")
            .to_string(),
        discourse: records(&v, "discourse")?,
        persons: records(&v, "persons")?,
        dictionary: records(&v, "dictionary")?,
        donors: records(&v, "donors")?,
        events: records(&v, "events")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(key: &str, text: &str) -> Record {
        Record {
            key: key.into(),
            text: text.into(),
            sources: vec![],
        }
    }

    #[test]
    fn append_batch_matches_join_without_trailing_newline() {
        let parts = ["первый", "второй", "третий"];
        let mut acc = String::new();
        for part in parts {
            append_batch(&mut acc, part);
        }
        assert_eq!(acc, parts.join("\n"));
        assert!(!acc.ends_with('\n'));
    }

    #[test]
    fn parses_body_field_from_model_schema() {
        let raw = r#"{"web":{"ok":true,"reason":""},"discourse":[{"key":"2026-11-18-кварц-7","body":"обсуждали релиз","sources":[]}]}"#;
        let chunk = parse_model_json(raw).unwrap();
        assert_eq!(chunk.discourse[0].text, "обсуждали релиз");
    }

    #[test]
    fn repeated_merge_updates_instead_of_duplicating() {
        let mut doc = parse_managed(&new_document(Kind::Discourse));
        let recs = vec![rec("2026-11-05-тема", "первое тело")];
        let first = merge(&mut doc, &recs, "2026-11-05");
        assert_eq!(first.added, 1);
        assert_eq!(first.updated, 0);
        let second = merge(&mut doc, &[rec("2026-11-05-тема", "второе тело")], "2026-11-06");
        assert_eq!(second.added, 0);
        assert_eq!(second.updated, 1);
        assert_eq!(doc.entries.len(), 1);
        assert_eq!(doc.entries[0].body, "второе тело");
        assert_eq!(doc.entries[0].updated, "2026-11-06");
    }

    #[test]
    fn empty_body_keeps_previous_text_and_sources() {
        let mut doc = parse_managed(&new_document(Kind::Persons));
        let mut with_src = rec("Илон Маск", "предприниматель");
        with_src.sources = vec!["https://example.com/musk".into()];
        merge(&mut doc, &[with_src], "2026-11-05");
        merge(&mut doc, &[rec("илон  маск", "")], "2026-11-06");
        assert_eq!(doc.entries.len(), 1);
        assert_eq!(doc.entries[0].body, "предприниматель");
        assert_eq!(doc.entries[0].sources, vec!["https://example.com/musk"]);
    }

    #[test]
    fn foreign_text_outside_the_zone_is_preserved() {
        let text = "# Словарь\n\nруками написанное\n\n<!-- knowledge:begin -->\n### ap\n- обновлено: 2026-11-01\nстарое\n<!-- knowledge:end -->\nхвост файла\n";
        let mut doc = parse_managed(text);
        assert!(doc.managed);
        assert_eq!(doc.entries.len(), 1);
        merge(&mut doc, &[rec("ap", "новое")], "2026-11-05");
        let out = render(&doc, Kind::Dictionary);
        assert!(out.starts_with("# Словарь\n\nруками написанное\n\n<!-- knowledge:begin -->"));
        assert!(out.contains("### ap\n- обновлено: 2026-11-05\nновое\n"));
        assert!(out.ends_with("<!-- knowledge:end -->\nхвост файла\n"));
    }

    #[test]
    fn missing_zone_is_appended_and_render_is_stable() {
        let mut doc = parse_managed("просто текст без зоны\n");
        assert!(!doc.managed);
        merge(&mut doc, &[rec("Термин", "объяснение")], "2026-11-05");
        let once = render(&doc, Kind::Dictionary);
        let twice = render(&parse_managed(&once), Kind::Dictionary);
        assert_eq!(once, twice);
        assert!(once.contains("<!-- knowledge:begin -->"));
        assert!(once.find("просто текст без зоны").unwrap() < once.find(BEGIN).unwrap());
    }

    #[test]
    fn slugs_and_array_fields_are_one_mapping() {
        for kind in kinds() {
            assert_eq!(Kind::from_slug(kind.slug()), Some(kind));
            assert_eq!(kind.array_field(), kind.slug());
        }
        assert_eq!(Kind::from_slug(" unknown "), None);
    }

    #[test]
    fn cursor_round_trips_and_rejects_garbage() {
        let c = Cursor {
            date: 1_800_000_000,
            message_id: 42,
        };
        assert_eq!(Cursor::parse(&c.format()).unwrap(), c);
        assert!(Cursor::parse("").is_err());
        assert!(Cursor::parse("1795012296").is_err());
        assert!(Cursor::parse("x:1").is_err());
        assert!(Cursor::parse("1:y").is_err());
    }

    #[test]
    fn window_and_behind_window_are_pure() {
        assert_eq!(window_plan(10_000, 60), (6_400, 10_000));
        assert!(behind_window(6_399, 6_400));
        assert!(!behind_window(6_400, 6_400));
        let (from, to) = window_plan(10_000, -5);
        assert_eq!((from, to), (10_000, 10_000));
    }

    #[test]
    fn previous_local_day_starts_at_local_midnight() {
        let now = 1_800_000_000i64;
        let start = previous_local_day_start(now, 180).unwrap();
        let tz = chrono::FixedOffset::east_opt(180 * 60).unwrap();
        let local = chrono::DateTime::from_timestamp(start, 0)
            .unwrap()
            .with_timezone(&tz);
        assert_eq!(local.format("%H:%M:%S").to_string(), "00:00:00");
        let current = chrono::DateTime::from_timestamp(now, 0)
            .unwrap()
            .with_timezone(&tz)
            .date_naive();
        assert_eq!(local.date_naive(), current - chrono::Duration::days(1));
    }

    #[test]
    fn digest_changes_with_batch_or_document() {
        let a = run_digest("one", b"doc");
        assert_eq!(a, run_digest("one", b"doc"));
        assert_ne!(a, run_digest("two", b"doc"));
        assert_ne!(a, run_digest("one", b"changed"));
        assert_eq!(a.len(), 64);
    }

    #[test]
    fn scratch_name_is_per_document_and_process() {
        assert_ne!(scratch_name("discourse", 1), scratch_name("persons", 1));
        assert_ne!(scratch_name("discourse", 1), scratch_name("discourse", 2));
        assert_eq!(scratch_name("discourse", 7), "knowledge-discourse-7");
    }

    #[test]
    fn keys_are_normalized() {
        assert_eq!(normalize_key("  «Слово   дня» "), "слово-дня");
        assert_eq!(normalize_key("API-ключ"), "api-ключ");
        assert_eq!(normalize_key("---"), "");
    }

    #[test]
    fn broken_or_signalless_json_is_rejected() {
        assert!(parse_model_json("никакого json").is_err());
        assert!(parse_model_json("{\"web\":{\"ok\":true},").is_err());
        assert!(parse_model_json("{\"discourse\":[]}").is_err());
        assert!(parse_model_json("{\"web\":{\"ok\":true},\"discourse\":[{\"text\":\"x\"}]}").is_err());
        let ok = parse_model_json(
            "```json\n{\"web\":{\"ok\":true,\"reason\":\"\"},\"discourse\":[],\"persons\":[{\"key\":\"X\",\"text\":\"кто\",\"sources\":[\"https://e/x\"]}],\"dictionary\":[]}\n```",
        )
        .unwrap();
        assert!(ok.web_ok);
        assert_eq!(ok.persons.len(), 1);
        assert_eq!(ok.persons[0].sources, vec!["https://e/x"]);
    }
}
