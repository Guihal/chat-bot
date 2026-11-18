//! Favourites: a durable store the model can save into and send from later.
//!
//! The directory lives OUTSIDE `<state>/media`, so `cleanup_media` — which wipes
//! that directory to zero — never sees it. This module is the only writer of the
//! store and the only reader of its index; `send.rs` stays the transport and
//! never learns where favourites keep their bytes.
use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

const INDEX: &str = "index.json";

/// Root of the never-cleaned store.
pub fn dir(state: &str) -> PathBuf {
    Path::new(state).join("favorites")
}

fn index_path(state: &str) -> PathBuf {
    dir(state).join(INDEX)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Favorite {
    pub id: String,
    pub kind: String,
    pub file_id: String,
    pub note: String,
    /// Local copy. Empty only when the record was saved by `file_id` alone.
    pub path: String,
    pub saved_at: i64,
}

impl Favorite {
    fn to_json(&self) -> Value {
        json!({
            "id": self.id,
            "kind": self.kind,
            "file_id": self.file_id,
            "note": self.note,
            "path": self.path,
            "saved_at": self.saved_at,
        })
    }

    fn from_json(v: &Value) -> Option<Favorite> {
        Some(Favorite {
            id: v.get("id")?.as_str()?.to_string(),
            kind: v.get("kind")?.as_str()?.to_string(),
            file_id: v.get("file_id").and_then(|x| x.as_str()).unwrap_or("").to_string(),
            note: v.get("note").and_then(|x| x.as_str()).unwrap_or("").to_string(),
            path: v.get("path").and_then(|x| x.as_str()).unwrap_or("").to_string(),
            saved_at: v.get("saved_at").and_then(|x| x.as_i64()).unwrap_or(0),
        })
    }
}

/// A broken index is an empty store, never a panic: the store is a convenience,
/// and losing it must not take the daemon down.
fn read_index(state: &str) -> Vec<Favorite> {
    let Ok(raw) = std::fs::read_to_string(index_path(state)) else {
        return Vec::new();
    };
    let Ok(Value::Array(items)) = serde_json::from_str::<Value>(&raw) else {
        return Vec::new();
    };
    items.iter().filter_map(Favorite::from_json).collect()
}

fn write_index(state: &str, items: &[Favorite]) -> Result<()> {
    let dir = dir(state);
    std::fs::create_dir_all(&dir)?;
    let body = Value::Array(items.iter().map(Favorite::to_json).collect());
    let tmp = dir.join(".index.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(&body)?)?;
    std::fs::rename(&tmp, index_path(state))?;
    Ok(())
}

fn stable_id(parts: &[&str]) -> String {
    let mut h = DefaultHasher::new();
    for p in parts {
        p.hash(&mut h);
        0u8.hash(&mut h);
    }
    format!("{:x}", h.finish())
}

fn kind_of_path(path: &str) -> String {
    let ext = Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    match ext.as_str() {
        "gif" => "animation",
        "jpg" | "jpeg" | "png" | "webp" => "photo",
        "mp4" | "mov" | "webm" | "mkv" => "video",
        "webm_s" => "sticker",
        _ => "document",
    }
    .to_string()
}

pub fn list(state: &str, limit: usize) -> Vec<Favorite> {
    let mut items = read_index(state);
    items.sort_by(|a, b| b.saved_at.cmp(&a.saved_at).then(b.id.cmp(&a.id)));
    items.truncate(limit);
    items
}

pub fn resolve(state: &str, id: &str) -> Result<Favorite> {
    read_index(state)
        .into_iter()
        .find(|f| f.id == id)
        .ok_or_else(|| anyhow!("избранного с id {id} нет"))
}

/// Save one record. The note is mandatory: a favourite without a reason to send
/// it later is just a second copy of a file that the wipe would have removed.
/// Saving the same thing twice returns the same record instead of duplicating it.
pub fn save(state: &str, args: &Value) -> Result<Favorite> {
    let note = args
        .get("note")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if note.is_empty() {
        return Err(anyhow!("нужна заметка (note): зачем это избранное"));
    }
    let media_path = args
        .get("media_path")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let file_id = args
        .get("file_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if media_path.is_empty() && file_id.is_empty() {
        return Err(anyhow!("нужен media_path или file_id"));
    }
    let kind = args
        .get("kind")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            if media_path.is_empty() {
                "sticker".to_string()
            } else {
                kind_of_path(&media_path)
            }
        });
    let id = stable_id(&[&kind, &file_id, &media_path, &note]);
    let mut items = read_index(state);
    if let Some(existing) = items.iter().find(|f| f.id == id) {
        return Ok(existing.clone());
    }

    let mut stored_path = String::new();
    if !media_path.is_empty() {
        let src = Path::new(&media_path);
        let name = src
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .ok_or_else(|| anyhow!("у пути медиа нет имени файла: {media_path}"))?;
        std::fs::create_dir_all(dir(state))?;
        let dst = dir(state).join(format!("{id}_{name}"));
        std::fs::copy(src, &dst)
            .map_err(|e| anyhow!("не удалось сохранить избранное {media_path}: {e}"))?;
        stored_path = dst.to_string_lossy().to_string();
    }
    let record = Favorite {
        id,
        kind,
        file_id,
        note,
        path: stored_path,
        saved_at: chrono::Utc::now().timestamp(),
    };
    items.push(record.clone());
    write_index(state, &items)?;
    Ok(record)
}

/// `chat_favorite` control op: `save` needs the same arguments as [`save`],
/// `list` returns the newest records. Both answer with JSON, never with prose.
pub fn control(state: &str, args: &Value) -> Result<String> {
    let action = args
        .get("action")
        .and_then(|v| v.as_str())
        .unwrap_or("list")
        .trim()
        .to_lowercase();
    match action.as_str() {
        "save" => {
            let record = save(state, args)?;
            Ok(json!({"ok": true, "favorite": record.to_json()}).to_string())
        }
        "list" => {
            let limit = args
                .get("limit")
                .and_then(|v| v.as_u64())
                .unwrap_or(20)
                .clamp(1, 100) as usize;
            let items: Vec<Value> = list(state, limit).iter().map(|f| f.to_json()).collect();
            Ok(json!({"ok": true, "favorites": items}).to_string())
        }
        other => Err(anyhow!("неизвестное действие избранного: {other}")),
    }
}

/// Fill the media argument of a `chat_send` from a favourite. The transport
/// still sees exactly one of `gif_file_id` / `sticker_file_id` / `media_path`.
pub fn apply_favorite(state: &str, args: &Value) -> Result<Value> {
    let Some(id) = args
        .get("favorite_id")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return Ok(args.clone());
    };
    let favorite = resolve(state, id)?;
    let has_media = ["gif_file_id", "sticker_file_id", "media_path"]
        .iter()
        .any(|k| args.get(*k).and_then(|v| v.as_str()).map(|s| !s.trim().is_empty()).unwrap_or(false));
    if has_media {
        return Err(anyhow!(
            "favorite_id и явное медиа взаимоисключающие: выбери одно"
        ));
    }
    let mut out = args.clone();
    let obj = out
        .as_object_mut()
        .ok_or_else(|| anyhow!("аргументы chat_send должны быть объектом"))?;
    match favorite.kind.as_str() {
        "animation" if !favorite.file_id.is_empty() => {
            obj.insert("gif_file_id".into(), json!(favorite.file_id));
        }
        "sticker" if !favorite.file_id.is_empty() => {
            obj.insert("sticker_file_id".into(), json!(favorite.file_id));
        }
        _ if !favorite.path.is_empty() => {
            obj.insert("media_path".into(), json!(favorite.path));
        }
        _ => {
            return Err(anyhow!(
                "избранное {} нельзя отправить: нет ни локального файла, ни file_id подходящего вида",
                favorite.id
            ))
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_state(tag: &str) -> String {
        let dir = std::env::temp_dir().join(format!("chat-fav-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir.to_string_lossy().to_string()
    }

    #[test]
    fn save_copies_the_file_and_is_idempotent() {
        let state = temp_state("save");
        let src = Path::new(&state).join("media").join("1_pic.jpg");
        std::fs::create_dir_all(src.parent().unwrap()).unwrap();
        std::fs::write(&src, b"jpeg-bytes").unwrap();
        let args = json!({"note": "мем про кабана", "media_path": src.to_string_lossy()});
        let first = save(&state, &args).unwrap();
        let second = save(&state, &args).unwrap();
        assert_eq!(first.id, second.id, "the same save must not duplicate");
        assert!(Path::new(&first.path).exists());
        assert!(first.path.starts_with(dir(&state).to_string_lossy().as_ref()));
        assert_eq!(list(&state, 10).len(), 1);
    }

    #[test]
    fn a_note_is_required_and_an_unknown_id_is_an_error() {
        let state = temp_state("note");
        assert!(save(&state, &json!({"file_id": "f"})).is_err());
        assert!(resolve(&state, "nope").is_err());
    }

    #[test]
    fn a_favorite_becomes_exactly_one_send_argument() {
        let state = temp_state("send");
        let src = Path::new(&state).join("media").join("2_clip.mp4");
        std::fs::create_dir_all(src.parent().unwrap()).unwrap();
        std::fs::write(&src, b"mp4").unwrap();
        let fav = save(
            &state,
            &json!({"note": "клип", "media_path": src.to_string_lossy()}),
        )
        .unwrap();
        let args = apply_favorite(&state, &json!({"text": "держи", "favorite_id": fav.id})).unwrap();
        assert_eq!(args["text"], "держи");
        assert!(args["media_path"].as_str().unwrap().contains("favorites"));
        assert!(args.get("gif_file_id").is_none());

        let sticker = save(&state, &json!({"note": "стикер", "file_id": "stk1", "kind": "sticker"}))
            .unwrap();
        let args = apply_favorite(&state, &json!({"favorite_id": sticker.id})).unwrap();
        assert_eq!(args["sticker_file_id"], "stk1");
    }

    #[test]
    fn an_explicit_media_argument_and_a_favorite_cannot_both_be_given() {
        let state = temp_state("both");
        let fav = save(&state, &json!({"note": "стикер", "file_id": "stk1", "kind": "sticker"})).unwrap();
        let err = apply_favorite(
            &state,
            &json!({"favorite_id": fav.id, "sticker_file_id": "other"}),
        );
        assert!(err.is_err());
    }
}
