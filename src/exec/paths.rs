use std::path::{Component, Path, PathBuf};

use anyhow::{anyhow, bail, Result};

use crate::exec::error::{ToolError, ToolResult};

pub fn canonical_roots(raw: &[String]) -> Vec<PathBuf> {
    raw.iter()
        .map(|root| {
            let path = PathBuf::from(root);
            path.canonicalize().unwrap_or(path)
        })
        .collect()
}

pub fn resolve_existing(raw: &str, raw_roots: &[String]) -> ToolResult<PathBuf> {
    let roots = canonical_roots(raw_roots);
    let absolute = absolute_path(raw)?;
    if !within(&roots, &absolute) {
        return Err(denied_message(&absolute));
    }
    let canonical = absolute
        .canonicalize()
        .map_err(|err| ToolError::not_found(format!("路径不可用 {}: {err}", absolute.display())))?;
    if !within(&roots, &canonical) {
        return Err(denied_message(&canonical));
    }
    Ok(canonical)
}

pub fn resolve_for_write(
    raw: &str,
    raw_roots: &[String],
    create_parents: bool,
) -> ToolResult<PathBuf> {
    let roots = canonical_roots(raw_roots);
    let candidate = resolve_entry_for_write(raw, &roots, create_parents)?;
    if candidate.exists() {
        let canonical = candidate.canonicalize().map_err(|err| {
            ToolError::not_found(format!("路径不可用 {}: {err}", candidate.display()))
        })?;
        if !within(&roots, &canonical) {
            return Err(denied_message(&canonical));
        }
        return Ok(canonical);
    }
    Ok(candidate)
}

pub fn resolve_entry(raw: &str, raw_roots: &[String]) -> ToolResult<PathBuf> {
    let roots = canonical_roots(raw_roots);
    resolve_entry_for_write(raw, &roots, false)
}

pub fn resolve_entry_for_write(
    raw: &str,
    roots: &[PathBuf],
    create_parents: bool,
) -> ToolResult<PathBuf> {
    let absolute = absolute_path(raw)?;
    if !within(roots, &absolute) {
        return Err(denied_message(&absolute));
    }
    if roots.iter().any(|root| root == &absolute) {
        return Ok(absolute);
    }
    let parent = absolute
        .parent()
        .ok_or_else(|| ToolError::protocol("路径缺少父目录"))?;
    let name = absolute
        .file_name()
        .ok_or_else(|| ToolError::protocol("路径缺少文件名"))?;

    if !parent.exists() {
        if !create_parents {
            return Err(ToolError::not_found(format!(
                "目录不存在: {}",
                parent.display()
            )));
        }
        let existing = canonical_existing_ancestor(parent, roots)?;
        if !within(roots, &existing) {
            return Err(denied_message(&existing));
        }
        std::fs::create_dir_all(parent)
            .map_err(|err| ToolError::io(format!("创建目录 {} 失败: {err}", parent.display())))?;
    }

    let canonical_parent = parent
        .canonicalize()
        .map_err(|err| ToolError::not_found(format!("目录不可用 {}: {err}", parent.display())))?;
    if !within(roots, &canonical_parent) {
        return Err(denied_message(&canonical_parent));
    }
    Ok(canonical_parent.join(name))
}

pub fn root_cwd(raw_roots: &[String]) -> Result<PathBuf> {
    let roots = canonical_roots(raw_roots);
    let fallback = roots.first().cloned().unwrap_or_else(|| PathBuf::from("/"));
    let canonical = fallback
        .canonicalize()
        .map_err(|err| anyhow!("目录不可用 {}: {err}", fallback.display()))?;
    if roots.iter().any(|root| canonical.starts_with(root)) {
        Ok(canonical)
    } else {
        bail!("目录不在允许范围内: {}", canonical.display())
    }
}

fn absolute_path(raw: &str) -> ToolResult<PathBuf> {
    if raw.trim().is_empty() {
        return Err(ToolError::protocol("路径不能为空"));
    }
    let path = Path::new(raw);
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|err| ToolError::io(format!("获取当前目录失败: {err}")))?
            .join(path)
    };
    Ok(normalize(&absolute))
}

fn normalize(path: &Path) -> PathBuf {
    let mut output = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                output.pop();
            }
            other => output.push(other.as_os_str()),
        }
    }
    output
}

fn within(roots: &[PathBuf], path: &Path) -> bool {
    roots.iter().any(|root| path.starts_with(root))
}

fn canonical_existing_ancestor(path: &Path, roots: &[PathBuf]) -> ToolResult<PathBuf> {
    let mut current = path;
    loop {
        if current.exists() {
            let canonical = current.canonicalize().map_err(|err| {
                ToolError::not_found(format!("目录不可用 {}: {err}", current.display()))
            })?;
            if !within(roots, &canonical) {
                return Err(denied_message(&canonical));
            }
            return Ok(canonical);
        }
        current = current
            .parent()
            .ok_or_else(|| ToolError::denied("路径不在允许范围内"))?;
    }
}

fn denied_message(path: &Path) -> ToolError {
    ToolError::denied(format!("路径不在允许范围内: {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_root(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dsh-remote-paths-{}-{name}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn root_cwd_is_first_root() {
        let root = temp_root("cwd");
        let roots = vec![root.to_string_lossy().to_string(), "/tmp".to_string()];
        let path = root_cwd(&roots).unwrap();
        assert_eq!(path, root.canonicalize().unwrap());
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn root_cwd_survives_missing_root() {
        let roots = vec!["/tmp".to_string()];
        assert_eq!(
            root_cwd(&roots).unwrap(),
            Path::new("/tmp").canonicalize().unwrap()
        );
    }

    #[test]
    fn resolves_new_file_for_write() {
        let root = temp_root("write");
        let roots = vec![root.to_string_lossy().to_string()];
        let target =
            resolve_for_write(root.join("sub/a.txt").to_str().unwrap(), &roots, true).unwrap();
        assert!(target.starts_with(root.canonicalize().unwrap()));
        assert!(root.join("sub").is_dir());
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn denies_write_outside() {
        let root = temp_root("deny-write");
        let roots = vec![root.to_string_lossy().to_string()];
        assert!(resolve_for_write("/etc/dsh-remote-test", &roots, false).is_err());
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn normalizes_parent_segments() {
        let root = temp_root("dotdot");
        let roots = vec![root.to_string_lossy().to_string()];
        let escaped = format!("{}/../outside", root.display());
        assert!(resolve_existing(&escaped, &roots).is_err());
        fs::remove_dir_all(root).ok();
    }

    #[cfg(unix)]
    #[test]
    fn denies_symlink_escape() {
        let root = temp_root("symlink");
        let link = root.join("escape");
        std::os::unix::fs::symlink("/etc", &link).unwrap();
        let roots = vec![root.to_string_lossy().to_string()];
        assert!(resolve_existing(link.to_str().unwrap(), &roots).is_err());
        assert!(resolve_for_write(link.join("passwd").to_str().unwrap(), &roots, false).is_err());
        fs::remove_dir_all(root).ok();
    }

    #[cfg(unix)]
    #[test]
    fn entry_resolution_keeps_final_symlink() {
        let root = temp_root("entry");
        let link = root.join("link");
        std::os::unix::fs::symlink("/etc", &link).unwrap();
        let roots = vec![root.to_string_lossy().to_string()];
        let resolved = resolve_entry(link.to_str().unwrap(), &roots).unwrap();
        assert_eq!(resolved, root.canonicalize().unwrap().join("link"));
        assert!(fs::symlink_metadata(&resolved)
            .unwrap()
            .file_type()
            .is_symlink());
        assert!(resolve_entry(link.join("passwd").to_str().unwrap(), &roots).is_err());
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn entry_resolution_accepts_the_root_itself() {
        let root = temp_root("entry-root");
        let roots = vec![root.to_string_lossy().to_string()];
        assert_eq!(
            resolve_entry(root.to_str().unwrap(), &roots).unwrap(),
            root.canonicalize().unwrap()
        );
        assert_eq!(
            resolve_entry("/", &["/".to_string()]).unwrap(),
            Path::new("/")
        );
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn entry_resolution_requires_existing_parent() {
        let root = temp_root("entry-missing");
        let roots = vec![root.to_string_lossy().to_string()];
        assert!(resolve_entry(root.join("nope/a").to_str().unwrap(), &roots).is_err());
        fs::remove_dir_all(root).ok();
    }
}
