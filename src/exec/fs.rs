use std::fs;
use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ciborium::value::Value;

use crate::exec::error::{ToolError, ToolResult};
use crate::exec::paths;
use crate::proto::params::{FsEntry, FsLink, FsList, FsParams};
use crate::proto::value;

pub const LIST_DEFAULT_LIMIT: u64 = 256;
pub const LIST_MAX_LIMIT: u64 = 4096;
const LIST_BUDGET_BYTES: usize = 40_000;
const ENTRY_OVERHEAD: usize = 64;

pub fn run(roots: &[String], params: FsParams) -> ToolResult<Value> {
    let payload = match params.op.as_str() {
        "stat" => serialize(&stat(roots, &params.path)?),
        "list" => serialize(&list(roots, &params)?),
        "readlink" => serialize(&readlink(roots, &params.path)?),
        "symlink" => serialize(&symlink(roots, &params)?),
        "mkdir" => serialize(&mkdir(roots, &params)?),
        "remove" => serialize(&remove(roots, &params.path)?),
        "rename" => serialize(&rename(roots, &params)?),
        "setattr" => serialize(&setattr(roots, &params)?),
        "" => Err(ToolError::protocol("缺少 op 参数")),
        other => Err(ToolError::new(
            "unsupported",
            format!("不支持的 fs 操作: {other}"),
        )),
    }?;
    Ok(payload)
}

fn serialize<T: serde::Serialize>(value: &T) -> ToolResult<Value> {
    value::serialize(value)
        .map_err(|err| ToolError::internal(format!("序列化 fs 结果失败: {err:#}")))
}

pub fn stat(roots: &[String], raw_path: &str) -> ToolResult<FsEntry> {
    let path = paths::resolve_entry(raw_path, roots)?;
    let meta = fs::symlink_metadata(&path).map_err(|err| io_error(&path, err))?;
    Ok(named(&path, &meta))
}

pub fn readlink(roots: &[String], raw_path: &str) -> ToolResult<FsLink> {
    let path = paths::resolve_entry(raw_path, roots)?;
    let meta = fs::symlink_metadata(&path).map_err(|err| io_error(&path, err))?;
    if !meta.file_type().is_symlink() {
        return Err(ToolError::protocol(format!(
            "不是符号链接: {}",
            path.display()
        )));
    }
    let target = fs::read_link(&path).map_err(|err| io_error(&path, err))?;
    Ok(FsLink {
        target: target.to_string_lossy().to_string(),
    })
}

pub fn symlink(roots: &[String], params: &FsParams) -> ToolResult<FsEntry> {
    if params.target.is_empty() {
        return Err(ToolError::protocol("缺少 target 参数"));
    }
    let roots = paths::canonical_roots(roots);
    let path = paths::resolve_entry_for_write(&params.path, &roots, params.create_parents)?;
    if fs::symlink_metadata(&path).is_ok() {
        return Err(ToolError::conflict(format!(
            "路径已存在: {}",
            path.display()
        )));
    }
    std::os::unix::fs::symlink(&params.target, &path).map_err(|err| io_error(&path, err))?;
    let meta = fs::symlink_metadata(&path).map_err(|err| io_error(&path, err))?;
    Ok(named(&path, &meta))
}

pub fn list(roots: &[String], params: &FsParams) -> ToolResult<FsList> {
    let path = paths::resolve_existing(&params.path, roots)?;
    let meta = fs::metadata(&path).map_err(|err| io_error(&path, err))?;
    if !meta.is_dir() {
        return Err(ToolError::protocol(format!("不是目录: {}", path.display())));
    }
    let mut entries = Vec::new();
    let reader = fs::read_dir(&path).map_err(|err| io_error(&path, err))?;
    for item in reader {
        let item = item.map_err(|err| io_error(&path, err))?;
        let name = item.file_name().to_string_lossy().to_string();
        let meta = match item.metadata() {
            Ok(meta) => meta,
            Err(_) => continue,
        };
        entries.push(entry(&name, &meta));
    }
    entries.sort_by(|left, right| left.name.cmp(&right.name));

    let offset = params.offset.unwrap_or(0).min(entries.len() as u64) as usize;
    let limit = params
        .limit
        .unwrap_or(LIST_DEFAULT_LIMIT)
        .clamp(1, LIST_MAX_LIMIT) as usize;
    let total = entries.len();
    let mut end = offset;
    let mut budget = 0usize;
    while end < total && end - offset < limit {
        let cost = ENTRY_OVERHEAD + entries[end].name.len() * 2;
        if end > offset && budget + cost > LIST_BUDGET_BYTES {
            break;
        }
        budget += cost;
        end += 1;
    }
    let selected = entries.drain(offset..end).collect();
    let next = if end < total { Some(end as u64) } else { None };
    Ok(FsList {
        entries: selected,
        next,
    })
}

pub fn mkdir(roots: &[String], params: &FsParams) -> ToolResult<FsEntry> {
    let roots = paths::canonical_roots(roots);
    let path = paths::resolve_entry_for_write(&params.path, &roots, params.create_parents)?;
    match fs::symlink_metadata(&path) {
        Ok(_) => {
            return Err(ToolError::conflict(format!(
                "路径已存在: {}",
                path.display()
            )))
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(io_error(&path, err)),
    }
    fs::create_dir(&path).map_err(|err| io_error(&path, err))?;
    let mode = params.mode.unwrap_or(0o755) & 0o7777;
    fs::set_permissions(&path, fs::Permissions::from_mode(mode))
        .map_err(|err| io_error(&path, err))?;
    let meta = fs::symlink_metadata(&path).map_err(|err| io_error(&path, err))?;
    Ok(named(&path, &meta))
}

pub fn remove(roots: &[String], raw_path: &str) -> ToolResult<FsEntry> {
    let path = paths::resolve_entry(raw_path, roots)?;
    let meta = fs::symlink_metadata(&path).map_err(|err| io_error(&path, err))?;
    if meta.is_dir() {
        fs::remove_dir(&path).map_err(|err| io_error(&path, err))?;
    } else {
        fs::remove_file(&path).map_err(|err| io_error(&path, err))?;
    }
    Ok(named(&path, &meta))
}

pub fn rename(roots: &[String], params: &FsParams) -> ToolResult<FsEntry> {
    let roots = paths::canonical_roots(roots);
    let from = paths::resolve_entry_for_write(&params.path, &roots, false)?;
    let to = paths::resolve_entry_for_write(&params.to_path, &roots, params.create_parents)?;
    let meta = fs::symlink_metadata(&from).map_err(|err| io_error(&from, err))?;
    if from == to {
        return Ok(named(&from, &meta));
    }
    if !params.overwrite && fs::symlink_metadata(&to).is_ok() {
        return Err(ToolError::conflict(format!("目标已存在: {}", to.display())));
    }
    fs::rename(&from, &to).map_err(|err| io_error(&from, err))?;
    Ok(named(&to, &meta))
}

pub fn setattr(roots: &[String], params: &FsParams) -> ToolResult<FsEntry> {
    let path = paths::resolve_entry(&params.path, roots)?;
    let meta = fs::symlink_metadata(&path).map_err(|err| io_error(&path, err))?;
    if meta.file_type().is_symlink() {
        return Ok(named(&path, &meta));
    }
    if let Some(mode) = params.mode {
        fs::set_permissions(&path, fs::Permissions::from_mode(mode & 0o7777))
            .map_err(|err| io_error(&path, err))?;
    }
    if params.atime_ms.is_some() || params.mtime_ms.is_some() {
        let atime = params.atime_ms.map(from_ms);
        let mtime = params.mtime_ms.map(from_ms);
        set_times(&path, atime, mtime).map_err(|err| io_error(&path, err))?;
    }
    let meta = fs::symlink_metadata(&path).map_err(|err| io_error(&path, err))?;
    Ok(named(&path, &meta))
}

fn set_times(path: &Path, atime: Option<SystemTime>, mtime: Option<SystemTime>) -> io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let raw = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "路径包含空字节"))?;
    let times = [time_or_omit(atime), time_or_omit(mtime)];
    let result = unsafe {
        libc::utimensat(
            libc::AT_FDCWD,
            raw.as_ptr(),
            times.as_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn time_or_omit(value: Option<SystemTime>) -> libc::timespec {
    match value {
        Some(time) => {
            let seconds = time
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_secs() as libc::time_t;
            libc::timespec {
                tv_sec: seconds,
                tv_nsec: 0,
            }
        }
        None => libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_OMIT,
        },
    }
}

fn from_ms(value: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(value)
}

fn named(path: &Path, meta: &fs::Metadata) -> FsEntry {
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_default();
    entry(&name, meta)
}

fn entry(name: &str, meta: &fs::Metadata) -> FsEntry {
    let kind = if meta.file_type().is_symlink() {
        "symlink"
    } else if meta.is_dir() {
        "dir"
    } else if meta.is_file() {
        "file"
    } else {
        "other"
    };
    FsEntry {
        name: name.to_string(),
        kind: kind.to_string(),
        size: meta.len(),
        mtime_ms: ms(meta.mtime(), meta.mtime_nsec()),
        atime_ms: ms(meta.atime(), meta.atime_nsec()),
        ctime_ms: ms(meta.ctime(), meta.ctime_nsec()),
        perm: meta.mode() & 0o7777,
        uid: meta.uid(),
        gid: meta.gid(),
        nlink: meta.nlink() as u32,
    }
}

fn ms(seconds: i64, nanos: i64) -> u64 {
    if seconds < 0 {
        return 0;
    }
    (seconds as u64) * 1000 + (nanos.clamp(0, 999_999_999) as u64) / 1_000_000
}

fn io_error(path: &Path, err: io::Error) -> ToolError {
    match err.kind() {
        io::ErrorKind::NotFound => ToolError::not_found(format!("路径不存在: {}", path.display())),
        io::ErrorKind::PermissionDenied => {
            ToolError::denied(format!("没有权限: {}", path.display()))
        }
        io::ErrorKind::AlreadyExists => {
            ToolError::conflict(format!("路径已存在: {}", path.display()))
        }
        io::ErrorKind::DirectoryNotEmpty => {
            ToolError::not_empty(format!("目录非空: {}", path.display()))
        }
        io::ErrorKind::IsADirectory | io::ErrorKind::NotADirectory => {
            ToolError::new("protocol", format!("路径类型不符: {}", path.display()))
        }
        _ => ToolError::io(format!("文件操作失败 {}: {err}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dsh-remote-fs-{}-{name}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn params(op: &str, path: &Path) -> FsParams {
        FsParams {
            op: op.to_string(),
            path: path.to_string_lossy().to_string(),
            ..FsParams::default()
        }
    }

    #[test]
    fn stat_reports_file_and_dir() {
        let root = temp_root("stat");
        let file = root.join("a.txt");
        fs::write(&file, b"hello").unwrap();
        let roots = vec![root.to_string_lossy().to_string()];

        let file_entry = stat(&roots, file.to_str().unwrap()).unwrap();
        assert_eq!(file_entry.kind, "file");
        assert_eq!(file_entry.size, 5);
        assert!(file_entry.mtime_ms > 0);

        let dir_entry = stat(&roots, root.to_str().unwrap()).unwrap();
        assert_eq!(dir_entry.kind, "dir");
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn list_is_sorted_and_paged() {
        let root = temp_root("list");
        for name in ["c.txt", "a.txt", "b.txt"] {
            fs::write(root.join(name), b"x").unwrap();
        }
        let roots = vec![root.to_string_lossy().to_string()];

        let mut params = params("list", &root);
        params.limit = Some(2);
        let first = list(&roots, &params).unwrap();
        assert_eq!(first.entries.len(), 2);
        assert_eq!(first.entries[0].name, "a.txt");
        assert_eq!(first.entries[1].name, "b.txt");
        assert_eq!(first.next, Some(2));

        params.offset = first.next;
        let second = list(&roots, &params).unwrap();
        assert_eq!(second.entries.len(), 1);
        assert_eq!(second.entries[0].name, "c.txt");
        assert_eq!(second.next, None);
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn mkdir_remove_and_rename() {
        let root = temp_root("mutate");
        let roots = vec![root.to_string_lossy().to_string()];

        let dir = params("mkdir", &root.join("sub"));
        mkdir(&roots, &dir).unwrap();
        assert!(root.join("sub").is_dir());

        let file = root.join("sub/old.txt");
        fs::write(&file, b"data").unwrap();
        let mut move_params = params("rename", &file);
        move_params.to_path = root.join("sub/new.txt").to_string_lossy().to_string();
        rename(roots.as_slice(), &move_params).unwrap();
        assert!(root.join("sub/new.txt").exists());
        assert!(!file.exists());

        let mut clash = params("rename", &root.join("sub/new.txt"));
        clash.to_path = root.join("sub/other.txt").to_string_lossy().to_string();
        fs::write(root.join("sub/other.txt"), b"keep").unwrap();
        assert_eq!(
            rename(roots.as_slice(), &clash).unwrap_err().code,
            "conflict"
        );

        remove(&roots, root.join("sub/new.txt").to_str().unwrap()).unwrap();
        assert!(!root.join("sub/new.txt").exists());
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn setattr_changes_mode_and_mtime() {
        let root = temp_root("setattr");
        let file = root.join("a.txt");
        fs::write(&file, b"hello").unwrap();
        let roots = vec![root.to_string_lossy().to_string()];

        let mut params = params("setattr", &file);
        params.mode = Some(0o600);
        params.mtime_ms = Some(1_600_000_000_000);
        let entry = setattr(&roots, &params).unwrap();
        assert_eq!(entry.perm, 0o600);
        assert_eq!(entry.mtime_ms, 1_600_000_000_000);
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn readlink_returns_target() {
        let root = temp_root("readlink");
        let link = root.join("link");
        std::os::unix::fs::symlink("target.txt", &link).unwrap();
        let roots = vec![root.to_string_lossy().to_string()];

        let report = readlink(&roots, link.to_str().unwrap()).unwrap();
        assert_eq!(report.target, "target.txt");
        assert_eq!(
            stat(&roots, link.to_str().unwrap()).unwrap().kind,
            "symlink"
        );
        fs::remove_dir_all(root).ok();
    }

    #[cfg(unix)]
    #[test]
    fn symlink_creates_link() {
        let root = temp_root("symlink");
        fs::write(root.join("target.txt"), b"x").unwrap();
        let roots = vec![root.to_string_lossy().to_string()];
        let params = FsParams {
            op: "symlink".into(),
            path: root.join("link").to_string_lossy().to_string(),
            target: "target.txt".into(),
            ..FsParams::default()
        };
        let entry = symlink(&roots, &params).unwrap();
        assert_eq!(entry.kind, "symlink");
        assert_eq!(
            fs::read_link(root.join("link")).unwrap(),
            std::path::PathBuf::from("target.txt")
        );
        assert_eq!(symlink(&roots, &params).unwrap_err().code, "conflict");
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn rejects_unknown_op_and_missing_op() {
        let root = temp_root("op");
        let roots = vec![root.to_string_lossy().to_string()];
        let mut params = params("chmod", &root);
        assert_eq!(run(&roots, params.clone()).unwrap_err().code, "unsupported");
        params.op.clear();
        assert_eq!(run(&roots, params).unwrap_err().code, "protocol");
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn denies_outside_roots() {
        let root = temp_root("deny");
        let roots = vec![root.to_string_lossy().to_string()];
        assert_eq!(stat(&roots, "/etc/passwd").unwrap_err().code, "denied");
        fs::remove_dir_all(root).ok();
    }
}
