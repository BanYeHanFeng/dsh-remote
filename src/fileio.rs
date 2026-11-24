use std::path::{Path, PathBuf};

use rand::RngCore;
use sha2::{Digest, Sha256};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::protocol::{ErrorCode, Failure, Res};

pub const CHUNK: usize = 64 * 1024;

pub async fn open_regular(path: &Path) -> Res<(File, u64, Option<u32>)> {
    let file = File::open(path)
        .await
        .map_err(|e| Failure::io(format!("打开 {} 失败", path.display()), e))?;
    let meta = file
        .metadata()
        .await
        .map_err(|e| Failure::io(format!("读取 {} 状态失败", path.display()), e))?;
    if !meta.is_file() {
        return Err(Failure::new(
            ErrorCode::NotRegularFile,
            format!("{} 不是普通文件", path.display()),
        ));
    }
    Ok((file, meta.len(), mode_of(&meta)))
}

pub fn mode_of(meta: &std::fs::Metadata) -> Option<u32> {
    use std::os::unix::fs::PermissionsExt;
    Some(meta.permissions().mode() & 0o7777)
}

pub async fn pump_file_to<W>(file: &mut File, out: &mut W, size: u64) -> Res<String>
where
    W: AsyncWrite + Unpin,
{
    copy_counted(file, out, Some(size)).await
}

pub async fn write_hashed_copy<R, W>(src: &mut R, dst: &mut W, size: u64) -> Res<String>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    copy_counted(src, dst, Some(size)).await
}

async fn copy_counted<R, W>(src: &mut R, dst: &mut W, size: Option<u64>) -> Res<String>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut hasher = Sha256::new();
    let mut copied = 0u64;
    let mut buf = vec![0u8; CHUNK];
    loop {
        let want = match size {
            Some(size) if copied >= size => break,
            Some(size) => ((size - copied) as usize).min(buf.len()),
            None => buf.len(),
        };
        let read = src
            .read(&mut buf[..want])
            .await
            .map_err(|e| Failure::io("读取数据失败", e))?;
        if read == 0 {
            if size.is_some() {
                return Err(Failure::new(ErrorCode::IoError, "对端提前结束了文件数据"));
            }
            break;
        }
        hasher.update(&buf[..read]);
        dst.write_all(&buf[..read])
            .await
            .map_err(|e| Failure::io("发送数据失败", e))?;
        copied += read as u64;
    }
    dst.flush()
        .await
        .map_err(|e| Failure::io("发送数据失败", e))?;
    Ok(hex::encode(hasher.finalize()))
}

pub struct RecvTarget {
    path: PathBuf,
    temp: PathBuf,
    file: File,
    committed: bool,
}

impl Drop for RecvTarget {
    fn drop(&mut self) {
        if !self.committed {
            let _ = std::fs::remove_file(&self.temp);
        }
    }
}

impl RecvTarget {
    pub async fn write_from<R>(&mut self, src: &mut R, size: u64) -> Res<String>
    where
        R: AsyncRead + Unpin,
    {
        copy_counted(src, &mut self.file, Some(size)).await
    }

    pub async fn commit(mut self, mode: Option<u32>) -> Res<()> {
        let result = self.commit_inner(mode).await;
        if result.is_err() {
            let _ = fs::remove_file(&self.temp).await;
        }
        result
    }

    async fn commit_inner(&mut self, mode: Option<u32>) -> Res<()> {
        self.file
            .flush()
            .await
            .map_err(|e| Failure::io("写入临时文件失败", e))?;
        self.file
            .sync_all()
            .await
            .map_err(|e| Failure::io("写入临时文件失败", e))?;
        if let Some(mode) = mode {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&self.temp, std::fs::Permissions::from_mode(mode)).await;
        }
        fs::rename(&self.temp, &self.path)
            .await
            .map_err(|e| Failure::io(format!("替换 {} 失败", self.path.display()), e))?;
        self.committed = true;
        Ok(())
    }

    pub async fn abort(self) {
        drop(self);
    }
}

pub async fn begin_recv(path: &Path, overwrite: bool) -> Res<RecvTarget> {
    if !overwrite && fs::metadata(path).await.is_ok() {
        return Err(Failure::new(
            ErrorCode::AlreadyExists,
            format!("{} 已存在", path.display()),
        ));
    }
    let temp = temp_sibling(path);
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)
        .await
        .map_err(|e| Failure::io(format!("创建临时文件 {} 失败", temp.display()), e))?;
    Ok(RecvTarget {
        path: path.to_path_buf(),
        temp,
        file,
        committed: false,
    })
}

pub async fn recv_file<R>(
    src: &mut R,
    path: &Path,
    size: u64,
    mode: Option<u32>,
    overwrite: bool,
) -> Res<(u64, String)>
where
    R: AsyncRead + Unpin,
{
    let mut target = begin_recv(path, overwrite).await?;
    let sha = target.write_from(src, size).await?;
    target.commit(mode).await?;
    Ok((size, sha))
}

pub fn temp_sibling(path: &Path) -> PathBuf {
    let mut suffix = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut suffix);
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "file".to_string());
    let temp = format!(".{}.dsh-remote.tmp.{}", name, hex::encode(suffix));
    match path.parent() {
        Some(dir) => dir.join(temp),
        None => PathBuf::from(temp),
    }
}

pub fn kill_group(pid: Option<u32>) {
    if let Some(pid) = pid {
        if pid > 0 {
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
        }
    }
}
