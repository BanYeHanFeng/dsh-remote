use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::time::UNIX_EPOCH;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::exec::error::{ToolError, ToolResult};
use crate::exec::paths;

pub const MAX_READ_BYTES: usize = 32 * 1024;
pub const MAX_READ_LINES: u64 = 5_000;
pub const MAX_LINE_BYTES: usize = 4 * 1024;
pub const MAX_WRITE_BYTES: usize = 48 * 1024;
pub const MAX_EDIT_FILE: u64 = 8 * 1024 * 1024;
pub const MAX_LIST_ENTRIES: usize = 500;
pub const MAX_LIST_BYTES: usize = 48 * 1024;
pub const MAX_LIST_DEPTH: u64 = 5;
pub const DEFAULT_READ_LINES: u64 = 2_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadReport {
    pub path: String,
    pub size: u64,
    pub mtime: u64,
    pub binary: bool,
    pub offset: u64,
    pub limit: u64,
    pub lines: Vec<String>,
    pub next_offset: u64,
    pub eof: bool,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteReport {
    pub path: String,
    pub size: u64,
    pub created: bool,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EditReport {
    pub path: String,
    pub replacements: u64,
    pub size: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub name: String,
    pub path: String,
    pub kind: String,
    pub size: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListReport {
    pub path: String,
    pub depth: u64,
    pub entries: Vec<Entry>,
    pub skipped: u64,
    pub truncated: bool,
}

pub fn read(roots: &[String], raw_path: &str, offset: u64, limit: u64) -> ToolResult<ReadReport> {
    if limit == 0 {
        return Err(ToolError::protocol("limit 必须大于 0"));
    }
    let limit = limit.min(MAX_READ_LINES);
    let path = paths::resolve_existing(raw_path, roots)?;
    let meta = fs::metadata(&path)
        .map_err(|err| ToolError::io(format!("读取 {} 失败: {err}", path.display())))?;
    if !meta.is_file() {
        return Err(ToolError::io(format!("不是普通文件: {}", path.display())));
    }

    let file = fs::File::open(&path)
        .map_err(|err| ToolError::io(format!("打开 {} 失败: {err}", path.display())))?;
    let mut reader = BufReader::new(file);
    let binary = looks_binary(&mut reader)?;

    let mut report = ReadReport {
        path: path.display().to_string(),
        size: meta.len(),
        mtime: mtime_ms(&meta),
        binary,
        offset,
        limit,
        lines: Vec::new(),
        next_offset: offset,
        eof: true,
        truncated: false,
    };
    if binary {
        return Ok(report);
    }

    let mut line_no = 0u64;
    let mut stored = 0usize;
    loop {
        let (bytes_read, raw_line, line_truncated) =
            read_bounded_line(&mut reader, MAX_LINE_BYTES)?;
        if bytes_read == 0 {
            break;
        }
        line_no += 1;
        if line_no <= offset {
            continue;
        }
        if report.lines.len() as u64 >= limit {
            report.eof = false;
            break;
        }
        let text = String::from_utf8_lossy(trim_newline(&raw_line));
        let rendered = format!("{line_no}\t{text}");
        if stored + rendered.len() > MAX_READ_BYTES && !report.lines.is_empty() {
            report.eof = false;
            report.truncated = true;
            break;
        }
        stored += rendered.len();
        report.lines.push(rendered);
        report.next_offset = line_no;
        if line_truncated {
            report.truncated = true;
        }
    }
    if !report.eof && report.lines.is_empty() {
        report.next_offset = offset;
    }
    if report.eof && line_no > report.next_offset {
        report.next_offset = line_no;
    }
    Ok(report)
}

pub fn write(
    roots: &[String],
    raw_path: &str,
    content: &str,
    create_parents: bool,
    max_file: u64,
) -> ToolResult<WriteReport> {
    let limit = max_file.min(MAX_WRITE_BYTES as u64);
    if content.len() as u64 > limit {
        return Err(ToolError::too_large(format!(
            "内容 {} 字节超过 write 上限 {limit} 字节，请改用 upload",
            content.len()
        )));
    }
    let path = paths::resolve_for_write(raw_path, roots, create_parents)?;
    let created = !path.exists();
    write_atomic(&path, content.as_bytes(), max_file)?;
    Ok(WriteReport {
        path: path.display().to_string(),
        size: content.len() as u64,
        created,
        sha256: sha256_hex(content.as_bytes()),
    })
}

pub fn edit(
    roots: &[String],
    raw_path: &str,
    old_string: &str,
    new_string: &str,
    replace_all: bool,
    max_file: u64,
) -> ToolResult<EditReport> {
    if old_string.is_empty() {
        return Err(ToolError::protocol("old_string 不能为空"));
    }
    if old_string == new_string {
        return Err(ToolError::protocol("old_string 与 new_string 相同"));
    }
    let path = paths::resolve_existing(raw_path, roots)?;
    let meta = fs::metadata(&path)
        .map_err(|err| ToolError::io(format!("读取 {} 失败: {err}", path.display())))?;
    if !meta.is_file() {
        return Err(ToolError::io(format!("不是普通文件: {}", path.display())));
    }
    let limit = max_file.min(MAX_EDIT_FILE);
    if meta.len() > limit {
        return Err(ToolError::too_large(format!(
            "文件 {} 字节超过 edit 上限 {limit} 字节",
            meta.len()
        )));
    }
    let bytes = fs::read(&path)
        .map_err(|err| ToolError::io(format!("读取 {} 失败: {err}", path.display())))?;
    let text = String::from_utf8(bytes)
        .map_err(|_| ToolError::io(format!("文件不是 UTF-8 文本: {}", path.display())))?;
    let matches = text.matches(old_string).count() as u64;
    if matches == 0 {
        return Err(ToolError::not_found("文件中没有找到 old_string"));
    }
    if matches > 1 && !replace_all {
        return Err(ToolError::conflict(format!(
            "old_string 匹配到 {matches} 处，请提供更长的唯一片段或开启 replace_all"
        )));
    }
    let replaced = if replace_all {
        text.replace(old_string, new_string)
    } else {
        text.replacen(old_string, new_string, 1)
    };
    if replaced.len() as u64 > max_file {
        return Err(ToolError::too_large(format!(
            "编辑后文件 {} 字节超过上限 {max_file} 字节",
            replaced.len()
        )));
    }
    write_atomic(&path, replaced.as_bytes(), max_file)?;
    Ok(EditReport {
        path: path.display().to_string(),
        replacements: if replace_all { matches } else { 1 },
        size: replaced.len() as u64,
        sha256: sha256_hex(replaced.as_bytes()),
    })
}

pub fn list(roots: &[String], raw_path: &str, depth: u64) -> ToolResult<ListReport> {
    let path = paths::resolve_existing(raw_path, roots)?;
    let meta = fs::metadata(&path)
        .map_err(|err| ToolError::io(format!("读取 {} 失败: {err}", path.display())))?;
    if !meta.is_dir() {
        return Err(ToolError::io(format!("不是目录: {}", path.display())));
    }
    fs::read_dir(&path)
        .map_err(|err| ToolError::io(format!("读取目录 {} 失败: {err}", path.display())))?;
    let depth = depth.clamp(1, MAX_LIST_DEPTH);
    let canonical_roots = paths::canonical_roots(roots);
    let mut walk = Walk {
        roots: &canonical_roots,
        max_depth: depth,
        entries: Vec::new(),
        skipped: 0,
        truncated: false,
        used: 0,
    };
    walk.visit(&path, 1)?;
    Ok(ListReport {
        path: path.display().to_string(),
        depth,
        entries: walk.entries,
        skipped: walk.skipped,
        truncated: walk.truncated,
    })
}

struct Walk<'a> {
    roots: &'a [std::path::PathBuf],
    max_depth: u64,
    entries: Vec<Entry>,
    skipped: u64,
    truncated: bool,
    used: usize,
}

impl Walk<'_> {
    fn visit(&mut self, dir: &Path, level: u64) -> ToolResult<()> {
        let read_dir = match fs::read_dir(dir) {
            Ok(read_dir) => read_dir,
            Err(_) => {
                self.skipped += 1;
                return Ok(());
            }
        };
        let mut items = Vec::new();
        for item in read_dir {
            match item {
                Ok(item) => items.push(item),
                Err(_) => self.skipped += 1,
            }
            if items.len() > MAX_LIST_ENTRIES {
                self.truncated = true;
                break;
            }
        }
        items.sort_by_key(|item| item.file_name());

        for item in items {
            if self.entries.len() >= MAX_LIST_ENTRIES {
                self.truncated = true;
                return Ok(());
            }
            let path = item.path();
            let meta = match fs::symlink_metadata(&path) {
                Ok(meta) => meta,
                Err(_) => {
                    self.skipped += 1;
                    continue;
                }
            };
            if meta.file_type().is_symlink() {
                let allowed = path
                    .canonicalize()
                    .map(|target| self.roots.iter().any(|root| target.starts_with(root)))
                    .unwrap_or(false);
                if !allowed {
                    self.skipped += 1;
                    continue;
                }
            }
            let name = item.file_name().to_string_lossy().to_string();
            let cost = name.len() + path.to_string_lossy().len() + 64;
            if self.used + cost > MAX_LIST_BYTES && !self.entries.is_empty() {
                self.truncated = true;
                return Ok(());
            }
            self.used += cost;
            self.entries.push(Entry {
                name,
                path: path.display().to_string(),
                kind: kind_of(&meta).to_string(),
                size: meta.is_file().then_some(meta.len()),
            });
            if meta.is_dir() && !meta.file_type().is_symlink() && level < self.max_depth {
                self.visit(&path, level + 1)?;
                if self.truncated {
                    return Ok(());
                }
            }
        }
        Ok(())
    }
}

fn write_atomic(path: &Path, data: &[u8], max_file: u64) -> ToolResult<()> {
    if data.len() as u64 > max_file {
        return Err(ToolError::too_large(format!(
            "内容 {} 字节超过上限 {max_file} 字节",
            data.len()
        )));
    }
    let parent = path
        .parent()
        .ok_or_else(|| ToolError::protocol("路径缺少父目录"))?;
    let temp = parent.join(format!(
        ".dsh-remote-{}-{:016x}.tmp",
        std::process::id(),
        rand::random::<u64>()
    ));
    let permissions = fs::metadata(path).ok().map(|meta| meta.permissions());
    let result = (|| -> ToolResult<()> {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .map_err(|err| ToolError::io(format!("创建临时文件失败: {err}")))?;
        file.write_all(data)
            .map_err(|err| ToolError::io(format!("写入临时文件失败: {err}")))?;
        file.sync_all()
            .map_err(|err| ToolError::io(format!("同步临时文件失败: {err}")))?;
        if let Some(permissions) = permissions {
            let _ = fs::set_permissions(&temp, permissions);
        }
        fs::rename(&temp, path)
            .map_err(|err| ToolError::io(format!("替换文件 {} 失败: {err}", path.display())))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

fn looks_binary<R: BufRead>(reader: &mut R) -> ToolResult<bool> {
    let head = reader
        .fill_buf()
        .map_err(|err| ToolError::io(format!("读取文件失败: {err}")))?;
    if head.is_empty() {
        return Ok(false);
    }
    if head.contains(&0) {
        return Ok(true);
    }
    match std::str::from_utf8(head) {
        Ok(_) => Ok(false),
        Err(err) => Ok(err.error_len().is_some()),
    }
}

fn read_bounded_line<R: BufRead>(reader: &mut R, cap: usize) -> ToolResult<(usize, Vec<u8>, bool)> {
    let mut line = Vec::new();
    let mut total = 0usize;
    let mut truncated = false;
    loop {
        let available = reader
            .fill_buf()
            .map_err(|err| ToolError::io(format!("读取文件失败: {err}")))?;
        if available.is_empty() {
            break;
        }
        let (take, has_newline) = match available.iter().position(|byte| *byte == b'\n') {
            Some(position) => (position + 1, true),
            None => (available.len(), false),
        };
        total += take;
        if line.len() < cap {
            let room = cap - line.len();
            let copy = take.min(room);
            line.extend_from_slice(&available[..copy]);
            if copy < take {
                truncated = true;
            }
        } else {
            truncated = true;
        }
        reader.consume(take);
        if has_newline {
            break;
        }
    }
    Ok((total, line, truncated))
}

fn trim_newline(line: &[u8]) -> &[u8] {
    let mut end = line.len();
    if end > 0 && line[end - 1] == b'\n' {
        end -= 1;
    }
    if end > 0 && line[end - 1] == b'\r' {
        end -= 1;
    }
    &line[..end]
}

fn kind_of(meta: &fs::Metadata) -> &'static str {
    let file_type = meta.file_type();
    if file_type.is_symlink() {
        "symlink"
    } else if file_type.is_dir() {
        "dir"
    } else if file_type.is_file() {
        "file"
    } else {
        "other"
    }
}

fn mtime_ms(meta: &fs::Metadata) -> u64 {
    meta.modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn numbers_and_pages_lines() {
        let root = std::env::temp_dir().join(format!(
            "dsh-remote-files-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("a.txt");
        fs::write(&path, "one\ntwo\nthree\nfour\n").unwrap();
        let roots = vec![root.to_string_lossy().to_string()];

        let first = read(&roots, path.to_str().unwrap(), 0, 2).unwrap();
        assert_eq!(first.lines, vec!["1\tone", "2\ttwo"]);
        assert_eq!(first.next_offset, 2);
        assert!(!first.eof);

        let second = read(&roots, path.to_str().unwrap(), first.next_offset, 2).unwrap();
        assert_eq!(second.lines, vec!["3\tthree", "4\tfour"]);
        assert!(second.eof);
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn detects_binary() {
        let mut reader = BufReader::new(Cursor::new(vec![0u8, 1, 2, 3]));
        assert!(looks_binary(&mut reader).unwrap());
        let mut text = BufReader::new(Cursor::new("你好".as_bytes().to_vec()));
        assert!(!looks_binary(&mut text).unwrap());
    }

    #[test]
    fn bounded_line_marks_truncated() {
        let data = vec![b'a'; 100];
        let mut reader = BufReader::new(Cursor::new(data));
        let (total, line, truncated) = read_bounded_line(&mut reader, 10).unwrap();
        assert_eq!(total, 100);
        assert_eq!(line.len(), 10);
        assert!(truncated);
    }

    #[test]
    fn edit_requires_unique_match() {
        let root = std::env::temp_dir().join(format!(
            "dsh-remote-edit-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("a.txt");
        fs::write(&path, "same same").unwrap();
        let roots = vec![root.to_string_lossy().to_string()];
        let error = edit(&roots, path.to_str().unwrap(), "same", "x", false, 1024).unwrap_err();
        assert_eq!(error.code, "conflict");
        let report = edit(&roots, path.to_str().unwrap(), "same", "x", true, 1024).unwrap();
        assert_eq!(report.replacements, 2);
        assert_eq!(fs::read_to_string(&path).unwrap(), "x x");
        fs::remove_dir_all(root).ok();
    }
}
