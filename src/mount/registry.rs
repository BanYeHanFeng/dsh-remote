use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::session::Session;

use super::{Mount, MountInfo, MountSpec};

#[derive(Clone, Default)]
pub struct Mounts {
    entries: Arc<Mutex<Vec<Mount>>>,
}

impl Mounts {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn list(&self) -> Vec<MountInfo> {
        let entries = self.entries.lock().await;
        entries.iter().map(Mount::info).collect()
    }

    pub async fn add(&self, session: &Session, spec: MountSpec) -> Result<MountInfo, String> {
        let mountpoint = spec
            .mountpoint
            .canonicalize()
            .map_err(|err| format!("挂载点不可用: {}（{err}）", spec.mountpoint.display()))?;
        if !mountpoint.is_dir() {
            return Err(format!("挂载点不是目录: {}", mountpoint.display()));
        }
        let mut entries = self.entries.lock().await;
        if let Some(mounted) = entries
            .iter()
            .find(|mount| mount.mountpoint() == mountpoint.as_path())
        {
            let info = mounted.info();
            return Err(format!(
                "挂载点 {} 已经挂着 {}:{}，先用 unmount 卸掉",
                mountpoint.display(),
                info.target,
                info.remote_root
            ));
        }
        let spec = MountSpec {
            mountpoint,
            ..spec
        };
        let mount = Mount::start(session, spec)
            .await
            .map_err(|err| format!("{err:#}"))?;
        let info = mount.info();
        entries.push(mount);
        Ok(info)
    }

    pub async fn remove(&self, mountpoint: Option<&Path>) -> Vec<MountInfo> {
        let taken: Vec<Mount> = {
            let mut entries = self.entries.lock().await;
            match mountpoint {
                Some(path) => {
                    let wanted = canonical_or_self(path);
                    match entries
                        .iter()
                        .position(|mount| mount.mountpoint() == wanted.as_path())
                    {
                        Some(index) => vec![entries.remove(index)],
                        None => Vec::new(),
                    }
                }
                None => entries.drain(..).collect(),
            }
        };
        let mut removed = Vec::with_capacity(taken.len());
        for mount in taken {
            removed.push(mount.info());
            mount.stop().await;
        }
        removed
    }

    pub async fn stop_all(&self) {
        self.remove(None).await;
    }
}

fn canonical_or_self(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}
