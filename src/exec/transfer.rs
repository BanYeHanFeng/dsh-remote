use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

use crate::exec::error::{ToolError, ToolResult};
use crate::proto::Msg;

pub const CHUNK_SIZE: usize = 32 * 1024;
pub const MAX_CHUNK_SIZE: usize = 64 * 1024;
const TRANSFER_TIMEOUT_FLOOR: u64 = 300;
const TRANSFER_TIMEOUT_MAX: u64 = 3_600;
const TRANSFER_BYTES_PER_SECOND: u64 = 256 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferReport {
    pub path: String,
    pub size: u64,
    pub sha256: String,
}

pub fn transfer_limit(size: u64) -> Duration {
    let extra = size / TRANSFER_BYTES_PER_SECOND;
    Duration::from_secs(
        TRANSFER_TIMEOUT_FLOOR
            .saturating_add(extra)
            .min(TRANSFER_TIMEOUT_MAX),
    )
}

pub struct Incoming {
    final_path: PathBuf,
    temp_path: PathBuf,
    file: tokio::fs::File,
    hasher: Sha256,
    max_file: u64,
    received: u64,
    next_seq: u64,
    permissions: Option<std::fs::Permissions>,
    committed: bool,
}

impl Incoming {
    pub async fn open(final_path: PathBuf, overwrite: bool, max_file: u64) -> ToolResult<Self> {
        let mut permissions = None;
        match tokio::fs::symlink_metadata(&final_path).await {
            Ok(meta) => {
                if meta.is_dir() {
                    return Err(ToolError::io(format!(
                        "目标是目录: {}",
                        final_path.display()
                    )));
                }
                if !overwrite {
                    return Err(ToolError::conflict(format!(
                        "目标已存在: {}",
                        final_path.display()
                    )));
                }
                permissions = Some(meta.permissions());
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(ToolError::io(format!(
                    "检查 {} 失败: {err}",
                    final_path.display()
                )));
            }
        }
        let parent = final_path
            .parent()
            .ok_or_else(|| ToolError::protocol("路径缺少父目录"))?
            .to_path_buf();
        if !tokio::fs::try_exists(&parent).await.unwrap_or(false) {
            return Err(ToolError::not_found(format!(
                "目录不存在: {}",
                parent.display()
            )));
        }

        let mut last_error = None;
        for _ in 0..8 {
            let temp_path = parent.join(format!(
                ".dsh-remote-{}-{:016x}.part",
                std::process::id(),
                rand::random::<u64>()
            ));
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp_path)
                .await
            {
                Ok(file) => {
                    return Ok(Self {
                        final_path,
                        temp_path,
                        file,
                        hasher: Sha256::new(),
                        max_file,
                        received: 0,
                        next_seq: 0,
                        permissions,
                        committed: false,
                    });
                }
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    last_error = Some(err);
                }
                Err(err) => {
                    return Err(ToolError::io(format!("创建临时文件失败: {err}")));
                }
            }
        }
        Err(ToolError::io(format!(
            "创建临时文件失败: {}",
            last_error
                .map(|err| err.to_string())
                .unwrap_or_else(|| "重试次数用尽".to_string())
        )))
    }

    pub async fn push(&mut self, seq: u64, data: &[u8]) -> ToolResult<()> {
        if seq != self.next_seq {
            return Err(ToolError::protocol(format!(
                "分块序号错乱：期望 {}，收到 {seq}",
                self.next_seq
            )));
        }
        if data.is_empty() || data.len() > MAX_CHUNK_SIZE {
            return Err(ToolError::protocol(format!(
                "分块长度非法: {} 字节",
                data.len()
            )));
        }
        let received = self.received.saturating_add(data.len() as u64);
        if received > self.max_file {
            return Err(ToolError::too_large(format!(
                "文件超过上限 {} 字节",
                self.max_file
            )));
        }
        self.file
            .write_all(data)
            .await
            .map_err(|err| ToolError::io(format!("写入临时文件失败: {err}")))?;
        self.hasher.update(data);
        self.received = received;
        self.next_seq += 1;
        Ok(())
    }

    pub async fn finish(
        &mut self,
        size: Option<u64>,
        sha256: Option<&str>,
    ) -> ToolResult<TransferReport> {
        if let Some(size) = size {
            if size != self.received {
                return Err(ToolError::protocol(format!(
                    "字节数不一致：声明 {size}，收到 {}",
                    self.received
                )));
            }
        }
        let digest = hex::encode(self.hasher.clone().finalize());
        if let Some(expected) = sha256 {
            if !expected.eq_ignore_ascii_case(&digest) {
                return Err(ToolError::protocol("sha256 校验失败，临时文件已丢弃"));
            }
        }
        self.file
            .flush()
            .await
            .map_err(|err| ToolError::io(format!("刷新临时文件失败: {err}")))?;
        self.file
            .sync_all()
            .await
            .map_err(|err| ToolError::io(format!("同步临时文件失败: {err}")))?;
        if let Some(permissions) = self.permissions.clone() {
            let _ = tokio::fs::set_permissions(&self.temp_path, permissions).await;
        }
        tokio::fs::rename(&self.temp_path, &self.final_path)
            .await
            .map_err(|err| {
                ToolError::io(format!(
                    "重命名到 {} 失败: {err}",
                    self.final_path.display()
                ))
            })?;
        self.committed = true;
        Ok(TransferReport {
            path: self.final_path.display().to_string(),
            size: self.received,
            sha256: digest,
        })
    }

    pub async fn abort(&mut self) {
        if self.committed {
            return;
        }
        let _ = self.file.flush().await;
        let _ = tokio::fs::remove_file(&self.temp_path).await;
    }
}

pub async fn send_chunk(sink: &mpsc::Sender<Msg>, id: u64, seq: u64, data: &[u8]) -> Result<()> {
    sink.send(Msg::Chunk {
        id,
        seq,
        stream: crate::proto::STREAM_STDOUT,
        data: data.to_vec(),
    })
    .await
    .map_err(|_| anyhow!("发送分块失败"))
}
