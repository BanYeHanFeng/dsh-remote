use std::ffi::OsStr;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request, TimeOrNow,
};
use tokio::runtime::Handle;
use tracing::{debug, warn};

use crate::mount::{Inner, MountError};
use crate::proto::params::{FsEntry, FsParams};

const BLOCK_SIZE: u32 = 4096;
const NAME_MAX: u32 = 255;

pub struct Fs {
    inner: Arc<Inner>,
    rt: Handle,
}

impl Fs {
    pub fn new(inner: Arc<Inner>, rt: Handle) -> Self {
        Self { inner, rt }
    }

    fn offload<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        drop(self.rt.spawn(task));
    }

    fn read_only(&self) -> bool {
        self.inner.read_only()
    }
}

impl Filesystem for Fs {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let Some(name) = name.to_str() else {
            reply.error(libc::EINVAL);
            return;
        };
        let inner = self.inner.clone();
        let ttl = self.inner.ttl();
        let name = name.to_string();
        self.offload(async move {
            let outcome = async {
                let parent_path = inner
                    .path(parent)
                    .ok_or_else(|| MountError::new(libc::ESTALE, "未知 inode"))?;
                let path = inner
                    .child(&parent_path, &name)
                    .ok_or_else(|| MountError::new(libc::EINVAL, "名称不合法"))?;
                let entry = inner
                    .lookup_child(&parent_path, &name)
                    .await?
                    .ok_or_else(|| MountError::new(libc::ENOENT, "路径不存在"))?;
                Ok::<_, MountError>((path, entry))
            }
            .await;
            match outcome {
                Ok((path, entry)) => {
                    let ino = inner.ino(&path);
                    reply.entry(&ttl, &to_attr(&entry, ino), 0);
                }
                Err(err) => {
                    note(&name, &err);
                    reply.error(err.errno);
                }
            }
        });
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        let inner = self.inner.clone();
        let ttl = self.inner.ttl();
        self.offload(async move {
            match stat_of(&inner, ino).await {
                Ok((path, entry)) => reply.attr(&ttl, &to_attr(&entry, inner.ino(&path))),
                Err(err) => {
                    note("getattr", &err);
                    reply.error(err.errno);
                }
            }
        });
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let inner = self.inner.clone();
        self.offload(async move {
            let outcome = async {
                let path = inner
                    .path(ino)
                    .ok_or_else(|| MountError::new(libc::ESTALE, "未知 inode"))?;
                let entries = inner.list(&path).await?;
                Ok::<_, MountError>((path, entries))
            }
            .await;
            let (path, entries) = match outcome {
                Ok(value) => value,
                Err(err) => {
                    note("readdir", &err);
                    reply.error(err.errno);
                    return;
                }
            };
            let mut rows: Vec<(String, u64, FileType)> = Vec::with_capacity(entries.len() + 2);
            rows.push((".".to_string(), ino, FileType::Directory));
            let parent_path = inner.parent(&path);
            rows.push((
                "..".to_string(),
                inner.ino(&parent_path),
                FileType::Directory,
            ));
            for entry in &entries {
                let child = inner.child(&path, &entry.name).unwrap_or_default();
                rows.push((entry.name.clone(), inner.ino(&child), kind_to_type(entry)));
            }
            let mut index = offset.max(0) as usize;
            while index < rows.len() {
                let (name, child_ino, kind) = rows[index].clone();
                if reply.add(child_ino, (index + 1) as i64, kind, name) {
                    break;
                }
                index += 1;
            }
            reply.ok();
        });
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let inner = self.inner.clone();
        let writing = flags & (libc::O_WRONLY | libc::O_RDWR) != 0;
        let truncate = flags & libc::O_TRUNC != 0;
        let create = flags & libc::O_CREAT != 0;
        let read_only = self.read_only();
        self.offload(async move {
            if read_only && writing {
                reply.error(libc::EROFS);
                return;
            }
            let outcome = async {
                let path = inner
                    .path(ino)
                    .ok_or_else(|| MountError::new(libc::ESTALE, "未知 inode"))?;
                let spool = inner.ensure_spool(&path, create).await?;
                if truncate && writing {
                    let mut guard = spool.lock().await;
                    guard.truncate(0)?;
                }
                let mut guard = spool.lock().await;
                guard.opened += 1;
                drop(guard);
                Ok::<_, MountError>((spool, path))
            }
            .await;
            match outcome {
                Ok((spool, path)) => {
                    let fh = inner.attach(spool);
                    debug!(path, fh, flags, "打开文件");
                    reply.opened(fh, 0);
                }
                Err(err) => {
                    note("open", &err);
                    reply.error(err.errno);
                }
            }
        });
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let Some(spool) = self.inner.handle(fh) else {
            reply.error(libc::EIO);
            return;
        };
        self.offload(async move {
            let mut guard = spool.lock().await;
            match guard.read(offset.max(0) as u64, size) {
                Ok(data) => reply.data(&data),
                Err(err) => reply.error(err.raw_os_error().unwrap_or(libc::EIO)),
            }
        });
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let Some(spool) = self.inner.handle(fh) else {
            reply.error(libc::EIO);
            return;
        };
        if self.read_only() {
            reply.error(libc::EROFS);
            return;
        }
        let data = data.to_vec();
        self.offload(async move {
            let mut guard = spool.lock().await;
            match guard.write(offset.max(0) as u64, &data) {
                Ok(()) => reply.written(data.len() as u32),
                Err(err) => reply.error(err.raw_os_error().unwrap_or(libc::EIO)),
            }
        });
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        let Some(spool) = self.inner.handle(fh) else {
            reply.error(libc::EIO);
            return;
        };
        let inner = self.inner.clone();
        self.offload(async move {
            match inner.flush(&spool).await {
                Ok(()) => reply.ok(),
                Err(err) => {
                    warn!(error = %err.message, "回传影子文件失败");
                    reply.error(err.errno);
                }
            }
        });
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let Some(spool) = self.inner.handle(fh) else {
            reply.ok();
            return;
        };
        let inner = self.inner.clone();
        self.offload(async move {
            if let Err(err) = inner.flush(&spool).await {
                warn!(error = %err.message, "关闭文件时回传失败");
            }
            let mut guard = spool.lock().await;
            guard.opened = guard.opened.saturating_sub(1);
            drop(guard);
            inner.detach(fh);
            reply.ok();
        });
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        if self.read_only() {
            reply.error(libc::EROFS);
            return;
        }
        let Some(name) = name.to_str() else {
            reply.error(libc::EINVAL);
            return;
        };
        let inner = self.inner.clone();
        let ttl = self.inner.ttl();
        let name = name.to_string();
        self.offload(async move {
            let outcome = async {
                let parent_path = inner
                    .path(parent)
                    .ok_or_else(|| MountError::new(libc::ESTALE, "未知 inode"))?;
                let path = inner
                    .child(&parent_path, &name)
                    .ok_or_else(|| MountError::new(libc::EINVAL, "名称不合法"))?;
                let spool = inner.create_file(&path).await?;
                let mut guard = spool.lock().await;
                guard.opened += 1;
                let entry = FsEntry {
                    name: name.clone(),
                    kind: "file".to_string(),
                    size: 0,
                    mtime_ms: now_ms(),
                    atime_ms: now_ms(),
                    ctime_ms: now_ms(),
                    perm: mode & !umask & 0o7777,
                    uid: 0,
                    gid: 0,
                    nlink: 1,
                };
                drop(guard);
                inner.insert_child(&parent_path, entry);
                Ok::<_, MountError>((spool, path))
            }
            .await;
            match outcome {
                Ok((spool, path)) => {
                    let ino = inner.ino(&path);
                    let entry = inner.cached_attr(&path).unwrap_or_default();
                    let fh = inner.attach(spool);
                    reply.created(&ttl, &to_attr(&entry, ino), 0, fh, 0);
                }
                Err(err) => {
                    note(&name, &err);
                    reply.error(err.errno);
                }
            }
        });
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        if self.read_only() {
            reply.error(libc::EROFS);
            return;
        }
        let Some(name) = name.to_str() else {
            reply.error(libc::EINVAL);
            return;
        };
        let inner = self.inner.clone();
        let ttl = self.inner.ttl();
        let name = name.to_string();
        self.offload(async move {
            let outcome = async {
                let parent_path = inner
                    .path(parent)
                    .ok_or_else(|| MountError::new(libc::ESTALE, "未知 inode"))?;
                let path = inner
                    .child(&parent_path, &name)
                    .ok_or_else(|| MountError::new(libc::EINVAL, "名称不合法"))?;
                let entry = inner.mkdir(&path, mode & !umask).await?;
                Ok::<_, MountError>((path, entry))
            }
            .await;
            match outcome {
                Ok((path, entry)) => reply.entry(&ttl, &to_attr(&entry, inner.ino(&path)), 0),
                Err(err) => {
                    note(&name, &err);
                    reply.error(err.errno);
                }
            }
        });
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        self.remove_entry(parent, name, "unlink", reply);
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        self.remove_entry(parent, name, "rmdir", reply);
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
        reply: ReplyEmpty,
    ) {
        if self.read_only() {
            reply.error(libc::EROFS);
            return;
        }
        const RENAME_NOREPLACE: u32 = 1;
        const RENAME_EXCHANGE: u32 = 2;
        if flags & RENAME_EXCHANGE != 0 {
            reply.error(libc::EINVAL);
            return;
        }
        let (Some(name), Some(newname)) = (name.to_str(), newname.to_str()) else {
            reply.error(libc::EINVAL);
            return;
        };
        let overwrite = flags & RENAME_NOREPLACE == 0;
        let inner = self.inner.clone();
        let (name, newname) = (name.to_string(), newname.to_string());
        self.offload(async move {
            let outcome = async {
                let from_parent = inner
                    .path(parent)
                    .ok_or_else(|| MountError::new(libc::ESTALE, "未知 inode"))?;
                let to_parent = inner
                    .path(newparent)
                    .ok_or_else(|| MountError::new(libc::ESTALE, "未知 inode"))?;
                let from = inner
                    .child(&from_parent, &name)
                    .ok_or_else(|| MountError::new(libc::EINVAL, "名称不合法"))?;
                let to = inner
                    .child(&to_parent, &newname)
                    .ok_or_else(|| MountError::new(libc::EINVAL, "名称不合法"))?;
                inner.rename(&from, &to, overwrite).await
            }
            .await;
            match outcome {
                Ok(()) => reply.ok(),
                Err(err) => {
                    note("rename", &err);
                    reply.error(err.errno);
                }
            }
        });
    }

    fn symlink(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        link_name: &OsStr,
        target: &std::path::Path,
        reply: ReplyEntry,
    ) {
        if self.read_only() {
            reply.error(libc::EROFS);
            return;
        }
        let (Some(link_name), Some(target)) = (link_name.to_str(), target.to_str()) else {
            reply.error(libc::EINVAL);
            return;
        };
        let inner = self.inner.clone();
        let ttl = self.inner.ttl();
        let (link_name, target) = (link_name.to_string(), target.to_string());
        self.offload(async move {
            let outcome = async {
                let parent_path = inner
                    .path(parent)
                    .ok_or_else(|| MountError::new(libc::ESTALE, "未知 inode"))?;
                let path = inner
                    .child(&parent_path, &link_name)
                    .ok_or_else(|| MountError::new(libc::EINVAL, "名称不合法"))?;
                let entry = inner.symlink(&path, &target).await?;
                Ok::<_, MountError>((path, entry))
            }
            .await;
            match outcome {
                Ok((path, entry)) => reply.entry(&ttl, &to_attr(&entry, inner.ino(&path)), 0),
                Err(err) => {
                    note("symlink", &err);
                    reply.error(err.errno);
                }
            }
        });
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        let inner = self.inner.clone();
        self.offload(async move {
            let outcome = async {
                let path = inner
                    .path(ino)
                    .ok_or_else(|| MountError::new(libc::ESTALE, "未知 inode"))?;
                inner.readlink(&path).await
            }
            .await;
            match outcome {
                Ok(target) => reply.data(target.as_bytes()),
                Err(err) => {
                    note("readlink", &err);
                    reply.error(err.errno);
                }
            }
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        if self.read_only() {
            reply.error(libc::EROFS);
            return;
        }
        if uid.is_some() || gid.is_some() {
            reply.error(libc::EPERM);
            return;
        }
        let inner = self.inner.clone();
        let ttl = self.inner.ttl();
        let params = FsParams {
            op: "setattr".to_string(),
            mode,
            atime_ms: atime.map(time_or_now),
            mtime_ms: mtime.map(time_or_now),
            ..FsParams::default()
        };
        self.offload(async move {
            let outcome = async {
                let path = inner
                    .path(ino)
                    .ok_or_else(|| MountError::new(libc::ESTALE, "未知 inode"))?;
                if let Some(size) = size {
                    let spool = inner.ensure_spool(&path, false).await?;
                    {
                        let mut guard = spool.lock().await;
                        guard.truncate(size)?;
                    }
                    inner.flush(&spool).await?;
                }
                let entry = if params.mode.is_some()
                    || params.atime_ms.is_some()
                    || params.mtime_ms.is_some()
                {
                    inner
                        .setattr(
                            &path,
                            FsParams {
                                path: path.clone(),
                                ..params
                            },
                        )
                        .await?
                } else {
                    inner.stat(&path).await?
                };
                Ok::<_, MountError>((path, entry))
            }
            .await;
            match outcome {
                Ok((path, entry)) => reply.attr(&ttl, &to_attr(&entry, inner.ino(&path))),
                Err(err) => {
                    note("setattr", &err);
                    reply.error(err.errno);
                }
            }
        });
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyStatfs) {
        reply.statfs(
            1 << 20,
            1 << 19,
            1 << 19,
            1 << 20,
            1 << 19,
            BLOCK_SIZE,
            NAME_MAX,
            BLOCK_SIZE,
        );
    }
}

impl Fs {
    fn remove_entry(&self, parent: u64, name: &OsStr, op: &'static str, reply: ReplyEmpty) {
        if self.read_only() {
            reply.error(libc::EROFS);
            return;
        }
        let Some(name) = name.to_str() else {
            reply.error(libc::EINVAL);
            return;
        };
        let inner = self.inner.clone();
        let name = name.to_string();
        self.offload(async move {
            let outcome = async {
                let parent_path = inner
                    .path(parent)
                    .ok_or_else(|| MountError::new(libc::ESTALE, "未知 inode"))?;
                let path = inner
                    .child(&parent_path, &name)
                    .ok_or_else(|| MountError::new(libc::EINVAL, "名称不合法"))?;
                if let Some(entry) = inner.cached_attr(&path) {
                    if op == "rmdir" && !entry.is_dir() {
                        return Err(MountError::new(libc::ENOTDIR, "不是目录"));
                    }
                    if op == "unlink" && entry.is_dir() {
                        return Err(MountError::new(libc::EISDIR, "是目录"));
                    }
                }
                inner.remove(&path).await
            }
            .await;
            match outcome {
                Ok(()) => reply.ok(),
                Err(err) => {
                    note(op, &err);
                    reply.error(err.errno);
                }
            }
        });
    }
}

async fn stat_of(inner: &Arc<Inner>, ino: u64) -> Result<(String, FsEntry), MountError> {
    let path = inner
        .path(ino)
        .ok_or_else(|| MountError::new(libc::ESTALE, "未知 inode"))?;
    let entry = inner.stat(&path).await?;
    Ok((path, entry))
}

fn to_attr(entry: &FsEntry, ino: u64) -> FileAttr {
    FileAttr {
        ino,
        size: entry.size,
        blocks: entry.size.div_ceil(512),
        atime: from_ms(entry.atime_ms),
        mtime: from_ms(entry.mtime_ms),
        ctime: from_ms(entry.ctime_ms),
        crtime: from_ms(entry.mtime_ms),
        kind: kind_to_type(entry),
        perm: entry.perm as u16,
        nlink: entry.nlink.max(1),
        uid: entry.uid,
        gid: entry.gid,
        rdev: 0,
        blksize: BLOCK_SIZE,
        flags: 0,
    }
}

fn kind_to_type(entry: &FsEntry) -> FileType {
    match entry.kind.as_str() {
        "dir" => FileType::Directory,
        "symlink" => FileType::Symlink,
        "file" => FileType::RegularFile,
        _ => FileType::RegularFile,
    }
}

fn from_ms(value: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(value)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis() as u64)
        .unwrap_or_default()
}

fn time_or_now(time: TimeOrNow) -> u64 {
    match time {
        TimeOrNow::Now => now_ms(),
        TimeOrNow::SpecificTime(value) => value
            .duration_since(UNIX_EPOCH)
            .map(|value| value.as_millis() as u64)
            .unwrap_or_default(),
    }
}

fn note(context: &str, err: &MountError) {
    if err.errno == libc::ENOENT {
        debug!(context, errno = err.errno, error = %err.message, "挂载操作失败");
    } else {
        warn!(context, errno = err.errno, error = %err.message, "挂载操作失败");
    }
}
