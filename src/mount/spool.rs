use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::time::Instant;

use tokio_util::sync::CancellationToken;

use crate::call;
use crate::proto::params::FsEntry;
use crate::session::{RemoteError, Session};

pub struct Spool {
    pub path: String,
    pub local: PathBuf,
    file: Option<File>,
    pub loaded: bool,
    pub dirty: bool,
    pub size: u64,
    pub base_size: u64,
    pub base_mtime_ms: u64,
    pub pending_mode: Option<u32>,
    pub opened: u32,
    pub have_baseline: bool,
    pub last_used: Instant,
}

impl Spool {
    pub fn empty(path: &str, local: PathBuf) -> Spool {
        Self {
            path: path.to_string(),
            local,
            file: None,
            loaded: true,
            dirty: true,
            size: 0,
            base_size: 0,
            base_mtime_ms: 0,
            pending_mode: None,
            opened: 0,
            have_baseline: false,
            last_used: Instant::now(),
        }
    }

    pub fn adopt(path: &str, local: PathBuf, size: u64, mtime_ms: u64) -> Self {
        Self {
            path: path.to_string(),
            local,
            file: None,
            loaded: true,
            dirty: false,
            size,
            base_size: size,
            base_mtime_ms: mtime_ms,
            pending_mode: None,
            opened: 0,
            have_baseline: true,
            last_used: Instant::now(),
        }
    }

    pub fn read(&mut self, offset: u64, size: u32) -> io::Result<Vec<u8>> {
        if offset >= self.size {
            return Ok(Vec::new());
        }
        let want = ((self.size - offset).min(size as u64)) as usize;
        let mut buffer = vec![0u8; want];
        let read = self.file()?.read_at(&mut buffer, offset)?;
        buffer.truncate(read);
        self.touch();
        Ok(buffer)
    }

    pub fn write(&mut self, offset: u64, data: &[u8]) -> io::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.file()?.write_all_at(data, offset)?;
        self.size = self.size.max(offset + data.len() as u64);
        self.dirty = true;
        self.touch();
        Ok(())
    }

    pub fn truncate(&mut self, size: u64) -> io::Result<()> {
        self.file()?.set_len(size)?;
        self.size = size;
        self.dirty = true;
        self.touch();
        Ok(())
    }

    pub fn attr(&self, cached: &FsEntry) -> FsEntry {
        FsEntry {
            size: self.size,
            kind: "file".to_string(),
            ..cached.clone()
        }
    }

    pub fn matches_remote(&self, remote: &FsEntry) -> bool {
        !self.dirty && remote.size == self.base_size && remote.mtime_ms == self.base_mtime_ms
    }

    pub fn mark_synced(&mut self, remote: &FsEntry) {
        self.base_size = remote.size;
        self.base_mtime_ms = remote.mtime_ms;
        self.have_baseline = true;
        self.dirty = false;
    }

    fn baseline(&self) -> Option<(u64, u64)> {
        self.have_baseline
            .then_some((self.base_mtime_ms, self.base_size))
    }

    pub async fn flush(
        &mut self,
        session: &Session,
        target: &str,
        cancel: &CancellationToken,
    ) -> Result<Option<FsEntry>, RemoteError> {
        if !self.dirty {
            return Ok(None);
        }
        let report = call::upload_checked(
            session,
            target,
            &self.local,
            &self.path,
            true,
            true,
            self.baseline(),
            cancel,
        )
        .await
        .map_err(remote_error)?;
        self.dirty = false;
        Ok(report.entry)
    }

    pub fn cleanup(&mut self) {
        self.file = None;
        let _ = std::fs::remove_file(&self.local);
    }

    fn file(&mut self) -> io::Result<&File> {
        if self.file.is_none() {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&self.local)?;
            self.file = Some(file);
        }
        Ok(self.file.as_ref().expect("刚打开"))
    }

    fn touch(&mut self) {
        self.last_used = Instant::now();
    }
}

impl Drop for Spool {
    fn drop(&mut self) {
        self.file = None;
        let _ = std::fs::remove_file(&self.local);
    }
}

fn remote_error(err: anyhow::Error) -> RemoteError {
    match err.downcast::<RemoteError>() {
        Ok(error) => error,
        Err(err) => RemoteError::new("io", format!("{err:#}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_file(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "dsh-remote-spool-{}-{name}-{}",
            std::process::id(),
            rand::random::<u32>()
        ))
    }

    #[test]
    fn writes_and_reads_through_the_shadow() {
        let local = temp_file("rw");
        let mut spool = Spool::empty("/data/a.txt", local.clone());
        spool.write(0, b"hello").unwrap();
        spool.write(5, b" world").unwrap();
        assert_eq!(spool.size, 11);
        assert_eq!(spool.read(0, 5).unwrap(), b"hello");
        assert_eq!(spool.read(6, 100).unwrap(), b"world");
        assert!(spool.read(11, 10).unwrap().is_empty());
        assert!(spool.dirty);
        spool.cleanup();
        assert!(!local.exists());
    }

    #[test]
    fn truncate_and_padding() {
        let local = temp_file("truncate");
        let mut spool = Spool::empty("/data/a.txt", local.clone());
        spool.write(0, b"0123456789").unwrap();
        spool.truncate(4).unwrap();
        assert_eq!(spool.size, 4);
        assert_eq!(spool.read(0, 100).unwrap(), b"0123");
        spool.write(6, b"x").unwrap();
        assert_eq!(spool.size, 7);
        assert_eq!(spool.read(0, 100).unwrap(), b"0123\0\0x");
        spool.cleanup();
    }

    #[test]
    fn detects_remote_changes() {
        let local = temp_file("match");
        let mut spool = Spool::adopt("/data/a.txt", local.clone(), 3, 100);
        let remote = FsEntry {
            size: 3,
            mtime_ms: 100,
            ..FsEntry::default()
        };
        assert!(spool.matches_remote(&remote));
        spool.write(0, b"abc").unwrap();
        assert!(!spool.matches_remote(&remote));
        spool.mark_synced(&FsEntry {
            size: 3,
            mtime_ms: 200,
            ..FsEntry::default()
        });
        assert_eq!(spool.base_mtime_ms, 200);
        assert!(!spool.dirty);
        spool.cleanup();
    }
}
