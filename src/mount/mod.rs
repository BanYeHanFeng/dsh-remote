pub mod fuse;
pub mod registry;
pub mod spool;
pub mod tree;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::call;
use crate::config::MountConfig;
use crate::proto::params::{FsEntry, FsParams};
use crate::session::{RemoteError, Session};
use spool::Spool;
use tree::Tree;

const LIST_CAP: usize = 50_000;
const UPLOAD_BATCH_MAX: usize = 32;
const UPLOAD_BATCH_WINDOW: Duration = Duration::from_millis(20);
const UPLOAD_CONCURRENCY: usize = 4;
const UPLOAD_BARRIER_LIMIT: Duration = Duration::from_secs(300);
const SHUTDOWN_WAIT: Duration = Duration::from_secs(5);
const CONNECT_LIMIT: Duration = Duration::from_secs(10);
const MOUNT_SOURCE_PREFIX: &str = "dsh-remote:";

pub const DEFAULT_ATTR_TTL_MS: u64 = 2_000;
pub const DEFAULT_SPOOL_LIMIT: u64 = 536_870_912;

#[derive(Debug, Clone)]
pub struct MountSpec {
    pub target: String,
    pub mountpoint: PathBuf,
    pub remote_root: String,
    pub read_only: bool,
    pub allow_other: bool,
    pub attr_ttl: Duration,
    pub spool_dir: Option<PathBuf>,
    pub spool_limit: u64,
}

impl MountSpec {
    pub fn new(target: impl Into<String>, mountpoint: impl Into<PathBuf>) -> Self {
        Self {
            target: target.into(),
            mountpoint: mountpoint.into(),
            remote_root: "/".to_string(),
            read_only: false,
            allow_other: false,
            attr_ttl: Duration::from_millis(DEFAULT_ATTR_TTL_MS),
            spool_dir: None,
            spool_limit: DEFAULT_SPOOL_LIMIT,
        }
    }
}

impl From<&MountConfig> for MountSpec {
    fn from(cfg: &MountConfig) -> Self {
        Self {
            target: cfg.target.clone(),
            mountpoint: cfg.mountpoint.clone(),
            remote_root: cfg.remote_root.clone(),
            read_only: cfg.read_only,
            allow_other: cfg.allow_other,
            attr_ttl: cfg.attr_ttl,
            spool_dir: cfg.spool_dir.clone(),
            spool_limit: cfg.spool_limit,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MountInfo {
    pub target: String,
    pub mountpoint: PathBuf,
    pub remote_root: String,
    pub read_only: bool,
    pub elapsed: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnmountOutcome {
    Clean,
    Detached { reason: String },
    Stuck { reason: String },
}

#[derive(Debug, Clone)]
pub struct UnmountReport {
    pub info: MountInfo,
    pub outcome: UnmountOutcome,
}

#[derive(Debug, Clone)]
pub struct MountError {
    pub errno: i32,
    pub message: String,
}

impl MountError {
    pub fn new(errno: i32, message: impl Into<String>) -> Self {
        Self {
            errno,
            message: message.into(),
        }
    }
}

impl From<RemoteError> for MountError {
    fn from(error: RemoteError) -> Self {
        let errno = match error.code.as_str() {
            "not_found" => libc::ENOENT,
            "denied" => libc::EACCES,
            "conflict" => libc::EEXIST,
            "not_empty" => libc::ENOTEMPTY,
            "too_large" => libc::EFBIG,
            "unsupported" => libc::ENOSYS,
            "timeout" => libc::ETIMEDOUT,
            "stale" => libc::ESTALE,
            "busy" => libc::EAGAIN,
            "protocol" => libc::EINVAL,
            _ => libc::EIO,
        };
        Self::new(errno, format!("{}: {}", error.code, error.message))
    }
}

impl From<anyhow::Error> for MountError {
    fn from(error: anyhow::Error) -> Self {
        match error.downcast::<RemoteError>() {
            Ok(error) => error.into(),
            Err(error) => Self::new(libc::EIO, format!("{error:#}")),
        }
    }
}

impl From<std::io::Error> for MountError {
    fn from(error: std::io::Error) -> Self {
        Self::new(
            error.raw_os_error().unwrap_or(libc::EIO),
            format!("本地文件操作失败: {error}"),
        )
    }
}

pub async fn run(cfg: MountConfig, shutdown: CancellationToken) -> Result<()> {
    let session = Session::new(&cfg.client);
    info!(
        hub = %cfg.client.hub,
        target = %cfg.target,
        mountpoint = %cfg.mountpoint.display(),
        root = %cfg.remote_root,
        "启动挂载"
    );
    let connection = {
        let session = session.clone();
        let token = shutdown.child_token();
        tokio::spawn(async move { session.run(token).await })
    };
    if session.wait_connected(CONNECT_LIMIT).await {
        check_target(&session, &cfg).await;
    } else {
        warn!("首次连接中继超时，挂载点等连接恢复后才能用");
    }

    let mount = Mount::start(&session, MountSpec::from(&cfg)).await?;
    shutdown.cancelled().await;
    mount.stop().await;
    let _ = tokio::time::timeout(SHUTDOWN_WAIT, connection).await;
    Ok(())
}

pub struct Mount {
    target: String,
    mountpoint: PathBuf,
    remote_root: String,
    read_only: bool,
    since: Instant,
    inner: Arc<Inner>,
    unmounter: Option<fuser::SessionUnmounter>,
    worker: Option<std::thread::JoinHandle<()>>,
    spool_dir: PathBuf,
    cancel: CancellationToken,
}

impl Mount {
    pub async fn start(session: &Session, spec: MountSpec) -> Result<Self> {
        detach_stale(&spec.mountpoint)?;
        let mountpoint = spec
            .mountpoint
            .canonicalize()
            .with_context(|| format!("挂载点不可用: {}", spec.mountpoint.display()))?;
        if !mountpoint.is_dir() {
            bail!("挂载点不是目录: {}", mountpoint.display());
        }

        let spool_dir = spec.spool_dir.clone().unwrap_or_else(|| {
            std::env::temp_dir().join(format!(
                "dsh-remote-spool-{}-{:016x}",
                std::process::id(),
                rand::random::<u64>()
            ))
        });
        std::fs::create_dir_all(&spool_dir)
            .with_context(|| format!("创建影子目录失败: {}", spool_dir.display()))?;

        let cancel = CancellationToken::new();
        let (inner, queue) = Inner::new(
            session.clone(),
            spec.target.clone(),
            spec.remote_root.clone(),
            SpoolSet {
                dir: spool_dir.clone(),
                limit: spec.spool_limit,
            },
            spec.attr_ttl,
            spec.read_only,
            cancel.clone(),
        );
        let inner = Arc::new(inner);
        inner.spawn_uploads(queue);

        let handle = tokio::runtime::Handle::current();
        let mut fuse_session = match fuser::Session::new(
            fuse::Fs::new(inner.clone(), handle),
            &mountpoint,
            &mount_options(&spec),
        ) {
            Ok(fuse_session) => fuse_session,
            Err(err) => {
                let _ = std::fs::remove_dir_all(&spool_dir);
                return Err(err).with_context(|| format!("挂载失败: {}", mountpoint.display()));
            }
        };
        let unmounter = fuse_session.unmount_callable();
        let worker = match std::thread::Builder::new()
            .name("dsh-remote-fuse".to_string())
            .spawn(move || {
                if let Err(err) = fuse_session.run() {
                    warn!(error = %err, "FUSE 会话结束");
                }
            }) {
            Ok(worker) => worker,
            Err(err) => {
                let _ = std::fs::remove_dir_all(&spool_dir);
                return Err(err).context("创建 FUSE 线程失败");
            }
        };
        info!(mountpoint = %mountpoint.display(), "挂载就绪");
        Ok(Self {
            target: spec.target,
            mountpoint,
            remote_root: spec.remote_root,
            read_only: spec.read_only,
            since: Instant::now(),
            inner,
            unmounter: Some(unmounter),
            worker: Some(worker),
            spool_dir,
            cancel,
        })
    }

    pub fn mountpoint(&self) -> &Path {
        &self.mountpoint
    }

    pub async fn settle(&self) -> Result<(), MountError> {
        self.inner.sync_pending().await
    }

    pub fn info(&self) -> MountInfo {
        MountInfo {
            target: self.target.clone(),
            mountpoint: self.mountpoint.clone(),
            remote_root: self.remote_root.clone(),
            read_only: self.read_only,
            elapsed: self.since.elapsed(),
        }
    }

    pub async fn stop(mut self) -> UnmountReport {
        let info = self.info();
        let outcome = self.release().await;
        UnmountReport { info, outcome }
    }

    async fn release(&mut self) -> UnmountOutcome {
        if let Err(err) = self.inner.sync_pending().await {
            warn!(error = %err.message, "卸载前仍有影子文件没回传成功");
        }
        let outcome = self.unmount();
        if let Some(worker) = self.worker.take() {
            if matches!(outcome, UnmountOutcome::Detached { .. }) {
                debug!(
                    mountpoint = %self.mountpoint.display(),
                    "挂载点已强制解除绑定，FUSE 线程等占用释放后自己退出"
                );
            } else {
                let join = tokio::task::spawn_blocking(move || worker.join());
                if tokio::time::timeout(SHUTDOWN_WAIT, join).await.is_err() {
                    match outcome {
                        UnmountOutcome::Clean => debug!(
                            mountpoint = %self.mountpoint.display(),
                            "挂载点已离开挂载表，FUSE 线程等占用释放后自己退出"
                        ),
                        _ => warn!(
                            mountpoint = %self.mountpoint.display(),
                            "FUSE 线程没按时退出，挂载点可能还占着"
                        ),
                    }
                }
            }
        }
        self.inner.shutdown().await;
        let _ = std::fs::remove_dir_all(&self.spool_dir);
        info!(mountpoint = %self.mountpoint.display(), "挂载已退出");
        outcome
    }

    fn unmount(&mut self) -> UnmountOutcome {
        if let Some(mut unmounter) = self.unmounter.take() {
            if let Err(err) = unmounter.unmount() {
                warn!(error = %err, mountpoint = %self.mountpoint.display(), "卸载调用出错");
            }
        }
        let Some(source) = mounted_source(&self.mountpoint) else {
            return UnmountOutcome::Clean;
        };
        if !source.starts_with(MOUNT_SOURCE_PREFIX) {
            warn!(source = %source, mountpoint = %self.mountpoint.display(), "挂载点已被别的文件系统接管");
            return UnmountOutcome::Stuck {
                reason: format!("挂载点已被其它文件系统占用（{source}），没有动它"),
            };
        }
        warn!(source = %source, mountpoint = %self.mountpoint.display(), "卸载后挂载点还在，改用懒卸载");
        if let Err(err) = force_detach(&self.mountpoint) {
            return UnmountOutcome::Stuck {
                reason: format!("懒卸载失败（{err}）"),
            };
        }
        match mounted_source(&self.mountpoint) {
            None => UnmountOutcome::Detached {
                reason: "挂载点被占用，已强制解除绑定".to_string(),
            },
            Some(_) => UnmountOutcome::Stuck {
                reason: "懒卸载后挂载点仍在挂载表里".to_string(),
            },
        }
    }
}

impl Drop for Mount {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(mut unmounter) = self.unmounter.take() {
            let _ = unmounter.unmount();
        }
        if mounted_source(&self.mountpoint)
            .is_some_and(|source| source.starts_with(MOUNT_SOURCE_PREFIX))
        {
            let _ = force_detach(&self.mountpoint);
        }
    }
}

fn mount_options(spec: &MountSpec) -> Vec<fuser::MountOption> {
    let mut options = vec![
        fuser::MountOption::FSName(format!("{MOUNT_SOURCE_PREFIX}{}", spec.target)),
        fuser::MountOption::Subtype("dsh-remote".to_string()),
        fuser::MountOption::DefaultPermissions,
        fuser::MountOption::NoDev,
        fuser::MountOption::NoSuid,
    ];
    if spec.read_only {
        options.push(fuser::MountOption::RO);
    }
    if spec.allow_other {
        options.push(fuser::MountOption::AllowOther);
    }
    options
}

async fn check_target(session: &Session, cfg: &MountConfig) {
    let targets = match call::targets(session).await {
        Ok(targets) => targets,
        Err(err) => {
            warn!(error = %err, "读取目标列表失败，挂载继续");
            return;
        }
    };
    let Some(target) = targets.iter().find(|target| target.name == cfg.target) else {
        warn!(target = %cfg.target, "目标当前不在线，挂载会等其他节点上线");
        return;
    };
    info!(
        target = %cfg.target,
        roots = %target.roots.join(":"),
        "目标在线"
    );
    if !root_allowed(&target.roots, &cfg.remote_root) {
        warn!(
            root = %cfg.remote_root,
            roots = %target.roots.join(":"),
            "挂载根不在目标的允许目录里，读写会被告知越界"
        );
    }
}

pub fn root_allowed(roots: &[String], path: &str) -> bool {
    let path = path.trim_end_matches('/');
    roots.iter().any(|root| {
        let root = root.trim_end_matches('/');
        root.is_empty() || path == root || path.starts_with(&format!("{root}/"))
    })
}

fn detach_stale(mountpoint: &Path) -> Result<()> {
    let Some(source) = mounted_source(mountpoint) else {
        return Ok(());
    };
    if !source.starts_with(MOUNT_SOURCE_PREFIX) {
        bail!(
            "挂载点已被其它文件系统占用: {} ({})",
            mountpoint.display(),
            source
        );
    }
    info!(source = %source, "清理残留挂载");
    if let Err(err) = force_detach(mountpoint) {
        warn!(error = %err, mountpoint = %mountpoint.display(), "清理残留挂载失败");
    }
    Ok(())
}

fn mounted_source(mountpoint: &Path) -> Option<String> {
    let mut wanted = vec![mountpoint.to_string_lossy().to_string()];
    if let Ok(canonical) = mountpoint.canonicalize() {
        wanted.push(canonical.to_string_lossy().to_string());
    }
    let mounts = std::fs::read_to_string("/proc/self/mounts").unwrap_or_default();
    mounts.lines().find_map(|line| {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 3 || !wanted.iter().any(|path| *path == unescape(fields[1])) {
            return None;
        }
        Some(fields[0].to_string())
    })
}

fn force_detach(mountpoint: &Path) -> std::io::Result<()> {
    let raw = std::ffi::CString::new(mountpoint.to_string_lossy().as_bytes())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "挂载点包含空字节"))?;
    let result = unsafe { libc::umount2(raw.as_ptr(), libc::MNT_DETACH) };
    if result == 0 {
        return Ok(());
    }
    Err(std::io::Error::last_os_error())
}

fn unescape(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = String::with_capacity(value.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\' && index + 3 < bytes.len() {
            let code = std::str::from_utf8(&bytes[index + 1..index + 4]).unwrap_or("000");
            if let Ok(value) = u8::from_str_radix(code, 8) {
                out.push(value as char);
                index += 4;
                continue;
            }
        }
        out.push(bytes[index] as char);
        index += 1;
    }
    out
}

struct SpoolSet {
    dir: PathBuf,
    limit: u64,
}

pub struct Inner {
    session: Session,
    target: String,
    read_only: bool,
    tree: Mutex<Tree>,
    spools: Mutex<HashMap<String, Arc<AsyncMutex<Spool>>>>,
    handles: Mutex<HashMap<u64, Arc<AsyncMutex<Spool>>>>,
    next_handle: AtomicU64,
    spool_dir: PathBuf,
    spool_limit: u64,
    spool_seq: AtomicU64,
    spool_total: AtomicU64,
    uploads: mpsc::UnboundedSender<Arc<AsyncMutex<Spool>>>,
    upload_paths: Mutex<HashSet<String>>,
    upload_count: watch::Sender<usize>,
    upload_errors: Mutex<Vec<MountError>>,
    upload_worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
    cancel: CancellationToken,
}

impl Inner {
    fn new(
        session: Session,
        target: String,
        root: String,
        spool: SpoolSet,
        attr_ttl: Duration,
        read_only: bool,
        cancel: CancellationToken,
    ) -> (Self, mpsc::UnboundedReceiver<Arc<AsyncMutex<Spool>>>) {
        let (uploads, queue) = mpsc::unbounded_channel();
        let (upload_count, _) = watch::channel(0usize);
        let inner = Self {
            session,
            target,
            read_only,
            tree: Mutex::new(Tree::new(root, attr_ttl)),
            spools: Mutex::new(HashMap::new()),
            handles: Mutex::new(HashMap::new()),
            next_handle: AtomicU64::new(1),
            spool_dir: spool.dir,
            spool_limit: spool.limit,
            spool_seq: AtomicU64::new(0),
            spool_total: AtomicU64::new(0),
            uploads,
            upload_paths: Mutex::new(HashSet::new()),
            upload_count,
            upload_errors: Mutex::new(Vec::new()),
            upload_worker: Mutex::new(None),
            cancel,
        };
        (inner, queue)
    }

    pub fn read_only(&self) -> bool {
        self.read_only
    }

    pub fn ttl(&self) -> Duration {
        self.tree().ttl()
    }

    fn tree(&self) -> MutexGuard<'_, Tree> {
        lock(&self.tree)
    }

    pub fn ino(&self, path: &str) -> u64 {
        self.tree().intern(path)
    }

    pub fn path(&self, ino: u64) -> Option<String> {
        self.tree().path(ino)
    }

    pub fn child(&self, parent: &str, name: &str) -> Option<String> {
        self.tree().child(parent, name)
    }

    pub fn parent(&self, path: &str) -> String {
        self.tree().parent_or_root(path)
    }

    pub fn cached_attr(&self, path: &str) -> Option<FsEntry> {
        self.tree().attr(path)
    }

    pub fn put_attr(&self, path: &str, entry: FsEntry) {
        self.tree().set_attr(path, entry);
    }

    pub fn insert_child(&self, parent: &str, entry: FsEntry) {
        self.tree().insert_child(parent, entry);
    }

    fn record(&self, path: &str, mut entry: FsEntry) -> FsEntry {
        if entry.name.is_empty() {
            entry.name = path.rsplit('/').next().unwrap_or_default().to_string();
        }
        self.insert_child(&self.parent(path), entry.clone());
        entry
    }

    pub fn remove_child(&self, parent: &str, name: &str) {
        self.tree().remove_child(parent, name);
    }

    pub async fn stat(&self, path: &str) -> Result<FsEntry, MountError> {
        if let Some(spool) = self.spool(path) {
            let guard = spool.lock().await;
            let cached = self.cached_attr(path).unwrap_or_else(|| FsEntry {
                kind: "file".to_string(),
                ..FsEntry::default()
            });
            if guard.dirty || !guard.loaded {
                return Ok(guard.attr(&cached));
            }
        }
        if let Some(entry) = self.cached_attr(path) {
            return Ok(entry);
        }
        self.remote_stat(path).await
    }

    pub async fn remote_stat(&self, path: &str) -> Result<FsEntry, MountError> {
        let entry = call::fs_stat(&self.session, &self.target, path, &self.cancel)
            .await
            .map_err(MountError::from)?;
        Ok(self.record(path, entry))
    }

    pub async fn list(&self, path: &str) -> Result<Vec<FsEntry>, MountError> {
        if let Some(cache) = self.tree().dir(path) {
            return Ok(cache.entries.clone());
        }
        self.sync_pending().await?;
        let mut entries = Vec::new();
        let mut offset = 0u64;
        loop {
            let page = call::fs_list(&self.session, &self.target, path, offset, &self.cancel)
                .await
                .map_err(MountError::from)?;
            entries.extend(page.entries);
            match page.next {
                Some(next) if entries.len() < LIST_CAP => offset = next,
                Some(_) => {
                    warn!(path, "目录太大，只缓存前 {LIST_CAP} 条");
                    break;
                }
                None => break,
            }
        }
        self.tree().set_dir(path, entries.clone());
        Ok(entries)
    }

    pub async fn lookup_child(
        &self,
        parent: &str,
        name: &str,
    ) -> Result<Option<FsEntry>, MountError> {
        let Some(path) = self.child(parent, name) else {
            return Ok(None);
        };
        match name {
            "." | ".." => self.stat(&path).await.map(Some),
            _ => {
                let entries = self.list(parent).await?;
                Ok(entries.into_iter().find(|entry| entry.name == name))
            }
        }
    }

    pub async fn ensure_spool(
        &self,
        path: &str,
        create: bool,
    ) -> Result<Arc<AsyncMutex<Spool>>, MountError> {
        if let Some(spool) = self.spool(path) {
            let guard = spool.lock().await;
            if guard.loaded && guard.dirty {
                return Ok(spool.clone());
            }
        }
        self.settle_path(path).await?;
        let remote = match self.stat(path).await {
            Ok(entry) => Some(entry),
            Err(err) if create && err.errno == libc::ENOENT => None,
            Err(err) => return Err(err),
        };
        if let Some(entry) = remote.as_ref() {
            if entry.is_dir() {
                return Err(MountError::new(libc::EISDIR, "目标是目录"));
            }
            if entry.is_symlink() {
                return Err(MountError::new(libc::ELOOP, "目标是符号链接"));
            }
            if entry.size > self.session.max_file() {
                return Err(MountError::new(
                    libc::EFBIG,
                    format!("文件超过单文件上限 {} 字节", self.session.max_file()),
                ));
            }
        }
        if let Some(spool) = self.spool(path) {
            let guard = spool.lock().await;
            let reusable = match remote.as_ref() {
                Some(entry) => guard.loaded && guard.matches_remote(entry),
                None => guard.loaded && guard.size == 0,
            };
            if reusable {
                return Ok(spool.clone());
            }
        }
        self.spawn_spool(path, remote).await
    }

    async fn spawn_spool(
        &self,
        path: &str,
        remote: Option<FsEntry>,
    ) -> Result<Arc<AsyncMutex<Spool>>, MountError> {
        let seq = self.spool_seq.fetch_add(1, Ordering::Relaxed);
        let local = self.spool_dir.join(format!("{seq}.part"));
        let spool = match remote {
            None => Spool::empty(path, local),
            Some(entry) => {
                let report = call::download(
                    &self.session,
                    &self.target,
                    path,
                    &local,
                    false,
                    &self.cancel,
                )
                .await
                .map_err(MountError::from)?;
                debug!(path, size = report.size, "影子文件已下载");
                Spool::adopt(path, local, report.size, entry.mtime_ms)
            }
        };
        let size = spool.size;
        let spool = Arc::new(AsyncMutex::new(spool));
        self.store_spool(path, spool.clone(), size);
        self.evict_spools();
        Ok(spool)
    }

    fn store_spool(&self, path: &str, spool: Arc<AsyncMutex<Spool>>, size: u64) {
        let previous = lock(&self.spools).insert(path.to_string(), spool);
        if let Some(previous) = previous {
            if let Ok(guard) = previous.try_lock() {
                self.spool_total.fetch_sub(guard.size, Ordering::Relaxed);
            }
        }
        self.spool_total.fetch_add(size, Ordering::Relaxed);
    }

    fn spool(&self, path: &str) -> Option<Arc<AsyncMutex<Spool>>> {
        lock(&self.spools).get(path).cloned()
    }

    fn evict_spools(&self) {
        if self.spool_total.load(Ordering::Relaxed) <= self.spool_limit {
            return;
        }
        let mut spools = lock(&self.spools);
        let mut candidates: Vec<(String, std::time::Instant, u64)> = Vec::new();
        for (path, spool) in spools.iter() {
            if let Ok(guard) = spool.try_lock() {
                if guard.opened == 0 && !guard.dirty {
                    candidates.push((path.clone(), guard.last_used, guard.size));
                }
            }
        }
        candidates.sort_by_key(|(_, used, _)| *used);
        for (path, _, size) in candidates {
            if self.spool_total.load(Ordering::Relaxed) <= self.spool_limit {
                break;
            }
            spools.remove(&path);
            let _ = self.spool_total.fetch_sub(size, Ordering::Relaxed);
            debug!(path, "淘汰影子文件缓存");
        }
    }

    pub fn attach(&self, spool: Arc<AsyncMutex<Spool>>) -> u64 {
        let fh = self.next_handle.fetch_add(1, Ordering::Relaxed);
        lock(&self.handles).insert(fh, spool);
        fh
    }

    pub fn handle(&self, fh: u64) -> Option<Arc<AsyncMutex<Spool>>> {
        lock(&self.handles).get(&fh).cloned()
    }

    pub fn detach(&self, fh: u64) {
        lock(&self.handles).remove(&fh);
    }

    fn spawn_uploads(self: &Arc<Self>, mut queue: mpsc::UnboundedReceiver<Arc<AsyncMutex<Spool>>>) {
        let inner = self.clone();
        let worker = tokio::spawn(async move {
            let cancel = inner.cancel.clone();
            loop {
                let first = tokio::select! {
                    _ = cancel.cancelled() => break,
                    first = queue.recv() => match first {
                        Some(first) => first,
                        None => break,
                    },
                };
                let mut batch = vec![first];
                let deadline = tokio::time::Instant::now() + UPLOAD_BATCH_WINDOW;
                while batch.len() < UPLOAD_BATCH_MAX {
                    let next = tokio::select! {
                        _ = cancel.cancelled() => None,
                        next = tokio::time::timeout_at(deadline, queue.recv()) => {
                            next.ok().flatten()
                        }
                    };
                    match next {
                        Some(spool) => batch.push(spool),
                        None => break,
                    }
                }
                let slots = Arc::new(tokio::sync::Semaphore::new(UPLOAD_CONCURRENCY));
                let mut tasks = tokio::task::JoinSet::new();
                for spool in batch {
                    let inner = inner.clone();
                    let slots = slots.clone();
                    tasks.spawn(async move {
                        if let Ok(_slot) = slots.acquire_owned().await {
                            inner.upload_spool(&spool).await;
                        }
                    });
                }
                while tasks.join_next().await.is_some() {}
            }
        });
        *lock(&self.upload_worker) = Some(worker);
    }

    async fn upload_spool(&self, spool: &Arc<AsyncMutex<Spool>>) {
        loop {
            let mut guard = spool.lock().await;
            let path = guard.path.clone();
            if !guard.dirty {
                guard.queued = false;
                drop(guard);
                self.finish_upload(&path);
                return;
            }
            let outcome = self.flush_locked(&path, &mut guard).await;
            drop(guard);
            match outcome {
                Ok(()) => {}
                Err(err) => {
                    warn!(path, error = %err.message, "影子文件回传失败，下次远端操作会重报");
                    lock(&self.upload_errors).push(err);
                }
            }
            self.finish_upload(&path);
            let mut guard = spool.lock().await;
            if !guard.dirty {
                guard.queued = false;
                return;
            }
            drop(guard);
            self.mark_upload(&path);
        }
    }

    async fn flush_locked(&self, path: &str, guard: &mut Spool) -> Result<(), MountError> {
        let entry = guard
            .flush(&self.session, &self.target, &self.cancel)
            .await
            .map_err(MountError::from)?;
        let pending_mode = guard.pending_mode.take();
        let synced = match entry {
            Some(entry) => Some(entry),
            None => match self.remote_stat(path).await {
                Ok(entry) => Some(entry),
                Err(err) => {
                    warn!(path, error = %err.message, "回传后读取属性失败");
                    None
                }
            },
        };
        if let Some(entry) = synced {
            guard.mark_synced(&entry);
            let parent = self.parent(path);
            self.insert_child(&parent, entry);
        }
        if let Some(mode) = pending_mode {
            let params = FsParams {
                op: "setattr".to_string(),
                path: path.to_string(),
                mode: Some(mode),
                ..FsParams::default()
            };
            match call::fs_call(&self.session, &self.target, params, &self.cancel).await {
                Ok(result) => {
                    if let Err(err) = check(result) {
                        warn!(path, error = %err.message, "回传后设置权限失败");
                    }
                }
                Err(err) => warn!(path, error = %err, "回传后设置权限失败"),
            }
        }
        Ok(())
    }

    fn mark_upload(&self, path: &str) {
        lock(&self.upload_paths).insert(path.to_string());
        self.upload_count.send_modify(|count| *count += 1);
    }

    fn finish_upload(&self, path: &str) {
        if lock(&self.upload_paths).remove(path) {
            self.upload_count
                .send_modify(|count| *count = count.saturating_sub(1));
        }
    }

    pub async fn queue_flush(&self, spool: &Arc<AsyncMutex<Spool>>) {
        let path = {
            let mut guard = spool.lock().await;
            if !guard.dirty || guard.queued {
                return;
            }
            guard.queued = true;
            guard.path.clone()
        };
        self.mark_upload(&path);
        if self.uploads.send(spool.clone()).is_err() {
            self.finish_upload(&path);
        }
    }

    pub async fn sync_pending(&self) -> Result<(), MountError> {
        let mut count = self.upload_count.subscribe();
        let waiting = async {
            loop {
                if *count.borrow_and_update() == 0 {
                    return;
                }
                if count.changed().await.is_err() {
                    return;
                }
            }
        };
        let _ = tokio::time::timeout(UPLOAD_BARRIER_LIMIT, waiting).await;
        let failure = lock(&self.upload_errors).pop();
        match failure {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    async fn settle_path(&self, path: &str) -> Result<(), MountError> {
        let mut count = self.upload_count.subscribe();
        let waiting = async {
            loop {
                if *count.borrow_and_update() == 0 || !lock(&self.upload_paths).contains(path) {
                    return;
                }
                if count.changed().await.is_err() {
                    return;
                }
            }
        };
        let _ = tokio::time::timeout(UPLOAD_BARRIER_LIMIT, waiting).await;
        let failure = lock(&self.upload_errors).pop();
        match failure {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    pub async fn mkdir(&self, path: &str, mode: u32) -> Result<FsEntry, MountError> {
        self.sync_pending().await?;
        let params = FsParams {
            op: "mkdir".to_string(),
            path: path.to_string(),
            mode: Some(mode),
            ..FsParams::default()
        };
        let entry = call::fs_call(&self.session, &self.target, params, &self.cancel)
            .await
            .map_err(MountError::from)?
            .into_value()
            .map_err(MountError::from)?;
        let entry: FsEntry = crate::proto::value::deserialize(&entry)
            .map_err(|err| MountError::new(libc::EIO, format!("响应格式不符: {err:#}")))?;
        Ok(self.record(path, entry))
    }

    pub async fn replace_spool(&self, path: &str) -> Result<Arc<AsyncMutex<Spool>>, MountError> {
        if let Some(entry) = self.cached_attr(path) {
            if entry.is_dir() {
                return Err(MountError::new(libc::EISDIR, "目标是目录"));
            }
            if entry.is_symlink() {
                return Err(MountError::new(libc::ELOOP, "目标是符号链接"));
            }
        }
        let spool = match self.spool(path) {
            Some(spool) => spool,
            None => {
                let seq = self.spool_seq.fetch_add(1, Ordering::Relaxed);
                let local = self.spool_dir.join(format!("{seq}.part"));
                let spool = Arc::new(AsyncMutex::new(Spool::empty(path, local)));
                self.store_spool(path, spool.clone(), 0);
                spool
            }
        };
        {
            let mut guard = spool.lock().await;
            guard.truncate(0)?;
        }
        Ok(spool)
    }

    pub async fn remove(&self, path: &str) -> Result<(), MountError> {
        self.sync_pending().await?;
        let params = FsParams {
            op: "remove".to_string(),
            path: path.to_string(),
            ..FsParams::default()
        };
        check(call::fs_call(&self.session, &self.target, params, &self.cancel).await?)?;
        let name = path.rsplit('/').next().unwrap_or_default().to_string();
        let parent = self.parent(path);
        self.tree().invalidate_subtree(path);
        self.remove_child(&parent, &name);
        let removed = { lock(&self.spools).remove(path) };
        if let Some(spool) = removed {
            let mut guard = spool.lock().await;
            self.spool_total.fetch_sub(guard.size, Ordering::Relaxed);
            guard.cleanup();
        }
        Ok(())
    }

    pub async fn rename(&self, from: &str, to: &str, overwrite: bool) -> Result<(), MountError> {
        self.sync_pending().await?;
        let params = FsParams {
            op: "rename".to_string(),
            path: from.to_string(),
            to_path: to.to_string(),
            overwrite,
            ..FsParams::default()
        };
        check(call::fs_call(&self.session, &self.target, params, &self.cancel).await?)?;
        self.tree().rename_path(from, to);
        {
            let mut spools = lock(&self.spools);
            if let Some(spool) = spools.remove(from) {
                spools.insert(to.to_string(), spool);
            }
        }
        if let Some(spool) = self.spool(to) {
            let mut guard = spool.lock().await;
            guard.path = to.to_string();
        }
        Ok(())
    }

    pub async fn setattr(&self, path: &str, params: FsParams) -> Result<FsEntry, MountError> {
        self.sync_pending().await?;
        if let Some(spool) = self.spool(path) {
            let mut guard = spool.lock().await;
            if guard.dirty {
                if let Some(mode) = params.mode {
                    guard.pending_mode = Some(mode & 0o7777);
                }
                let cached = self
                    .cached_attr(path)
                    .unwrap_or_else(|| guard.attr(&FsEntry::default()));
                let mut entry = guard.attr(&cached);
                if let Some(mode) = params.mode {
                    entry.perm = mode & 0o7777;
                }
                if let Some(mtime) = params.mtime_ms {
                    entry.mtime_ms = mtime;
                }
                if let Some(atime) = params.atime_ms {
                    entry.atime_ms = atime;
                }
                drop(guard);
                self.put_attr(path, entry.clone());
                return Ok(entry);
            }
        }
        let entry = call::fs_call(&self.session, &self.target, params, &self.cancel)
            .await
            .map_err(MountError::from)?
            .into_value()
            .map_err(MountError::from)?;
        let entry: FsEntry = crate::proto::value::deserialize(&entry)
            .map_err(|err| MountError::new(libc::EIO, format!("响应格式不符: {err:#}")))?;
        self.put_attr(path, entry.clone());
        Ok(entry)
    }

    pub async fn symlink(&self, path: &str, target: &str) -> Result<FsEntry, MountError> {
        self.sync_pending().await?;
        let params = FsParams {
            op: "symlink".to_string(),
            path: path.to_string(),
            target: target.to_string(),
            ..FsParams::default()
        };
        self.decode_fs(path, params).await
    }

    async fn decode_fs(&self, path: &str, params: FsParams) -> Result<FsEntry, MountError> {
        let value = call::fs_call(&self.session, &self.target, params, &self.cancel)
            .await
            .map_err(MountError::from)?
            .into_value()
            .map_err(MountError::from)?;
        let entry: FsEntry = crate::proto::value::deserialize(&value)
            .map_err(|err| MountError::new(libc::EIO, format!("响应格式不符: {err:#}")))?;
        Ok(self.record(path, entry))
    }

    pub async fn readlink(&self, path: &str) -> Result<String, MountError> {
        self.sync_pending().await?;
        let params = FsParams {
            op: "readlink".to_string(),
            path: path.to_string(),
            ..FsParams::default()
        };
        let value = call::fs_call(&self.session, &self.target, params, &self.cancel)
            .await
            .map_err(MountError::from)?
            .into_value()
            .map_err(MountError::from)?;
        let link: crate::proto::params::FsLink = crate::proto::value::deserialize(&value)
            .map_err(|err| MountError::new(libc::EIO, format!("响应格式不符: {err:#}")))?;
        Ok(link.target)
    }

    pub async fn shutdown(&self) {
        if let Err(err) = self.sync_pending().await {
            warn!(error = %err.message, "退出前仍有影子文件没回传成功");
        }
        self.cancel.cancel();
        let spools: Vec<Arc<AsyncMutex<Spool>>> =
            lock(&self.spools).drain().map(|(_, spool)| spool).collect();
        for spool in spools {
            let mut guard = spool.lock().await;
            guard.cleanup();
        }
        lock(&self.handles).clear();
    }
}

fn check(result: crate::session::CallResult) -> Result<(), MountError> {
    result.into_value().map(|_| ()).map_err(MountError::from)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_allowed_matches_prefixes() {
        let roots = vec!["/data".to_string(), "/home/ops/".to_string()];
        assert!(root_allowed(&roots, "/data"));
        assert!(root_allowed(&roots, "/data/"));
        assert!(root_allowed(&roots, "/data/projects"));
        assert!(root_allowed(&roots, "/home/ops"));
        assert!(!root_allowed(&roots, "/database"));
        assert!(!root_allowed(&roots, "/home/ops2"));
        assert!(root_allowed(&["/".to_string()], "/data"));
        assert!(root_allowed(&[String::new()], "/data"));
    }
}
