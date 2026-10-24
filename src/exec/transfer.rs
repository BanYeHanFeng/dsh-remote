use std::collections::BTreeMap;
use std::io::Read;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

use crate::exec::error::{ToolError, ToolResult};
use crate::proto::params::FsEntry;

pub const CHUNK_SIZE: usize = 60 * 1024;
pub const MAX_CHUNK_SIZE: usize = 64 * 1024;
pub const MAX_PENDING_CHUNKS: usize = 512;
const TRANSFER_TIMEOUT_FLOOR: u64 = 300;
const TRANSFER_TIMEOUT_MAX: u64 = 3_600;
const TRANSFER_BYTES_PER_SECOND: u64 = 256 * 1024;
const PACK_MIN_BYTES: usize = 1024;

pub fn pack(data: &[u8], gain_percent: u8) -> Option<Vec<u8>> {
    if data.len() < PACK_MIN_BYTES {
        return None;
    }
    let packed =
        ruzstd::encoding::compress_to_vec(data, ruzstd::encoding::CompressionLevel::Fastest);
    let keep = 100u64.saturating_sub(gain_percent.min(90) as u64) as usize;
    if packed.is_empty() || packed.len() * 100 >= data.len() * keep {
        return None;
    }
    Some(packed)
}

pub fn unpack(data: &[u8]) -> ToolResult<Vec<u8>> {
    let decoder = ruzstd::decoding::StreamingDecoder::new(data)
        .map_err(|err| ToolError::protocol(format!("解压分块失败: {err}")))?;
    let mut out = Vec::with_capacity(data.len() * 4);
    let mut limited = decoder.take(MAX_CHUNK_SIZE as u64 + 1);
    limited
        .read_to_end(&mut out)
        .map_err(|err| ToolError::protocol(format!("解压分块失败: {err}")))?;
    if out.len() > MAX_CHUNK_SIZE {
        return Err(ToolError::protocol("解压后分块超过上限"));
    }
    Ok(out)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferReport {
    pub path: String,
    pub size: u64,
    pub sha256: String,
    #[serde(default)]
    pub entry: Option<FsEntry>,
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
    pending: BTreeMap<u64, Vec<u8>>,
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
                        pending: BTreeMap::new(),
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

    pub async fn push(&mut self, seq: u64, packed: bool, data: &[u8]) -> ToolResult<()> {
        if data.is_empty() || data.len() > MAX_CHUNK_SIZE {
            return Err(ToolError::protocol(format!(
                "分块长度非法: {} 字节",
                data.len()
            )));
        }
        let data = if packed { unpack(data)? } else { data.to_vec() };
        if data.is_empty() {
            return Err(ToolError::protocol("分块解压后为空"));
        }
        let received = self.received.saturating_add(data.len() as u64);
        if received > self.max_file {
            return Err(ToolError::too_large(format!(
                "文件超过上限 {} 字节",
                self.max_file
            )));
        }
        if seq < self.next_seq {
            return Err(ToolError::protocol(format!(
                "分块重复：{} 已写入，当前期望 {}",
                seq, self.next_seq
            )));
        }
        self.received = received;
        if seq != self.next_seq {
            if self.pending.len() >= MAX_PENDING_CHUNKS {
                return Err(ToolError::protocol(format!(
                    "乱序分块超过 {} 片，放弃该次上传",
                    MAX_PENDING_CHUNKS
                )));
            }
            if self.pending.insert(seq, data).is_some() {
                return Err(ToolError::protocol(format!("分块重复：{seq}")));
            }
            return Ok(());
        }
        self.write_next(&data).await?;
        while let Some(chunk) = self.pending.remove(&self.next_seq) {
            self.write_next(&chunk).await?;
        }
        Ok(())
    }

    async fn write_next(&mut self, data: &[u8]) -> ToolResult<()> {
        self.file
            .write_all(data)
            .await
            .map_err(|err| ToolError::io(format!("写入临时文件失败: {err}")))?;
        self.hasher.update(data);
        self.next_seq += 1;
        Ok(())
    }

    pub async fn finish(
        &mut self,
        size: Option<u64>,
        sha256: Option<&str>,
    ) -> ToolResult<TransferReport> {
        if !self.pending.is_empty() {
            return Err(ToolError::protocol(format!(
                "缺少 {} 片数据，临时文件已丢弃",
                self.pending.len()
            )));
        }
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
        let entry = match tokio::fs::symlink_metadata(&self.final_path).await {
            Ok(meta) => Some(crate::exec::fs::named(&self.final_path, &meta)),
            Err(_) => None,
        };
        Ok(TransferReport {
            path: self.final_path.display().to_string(),
            size: self.received,
            sha256: digest,
            entry,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packs_repetitive_chunks() {
        let text = "fn main() { println!(\"hello\"); }\n".repeat(1_000);
        let packed = pack(text.as_bytes(), 12).expect("重复文本应该压得动");
        assert!(packed.len() * 4 < text.len());
        assert_eq!(unpack(&packed).unwrap(), text.as_bytes());
    }

    #[test]
    fn skips_tiny_and_random_chunks() {
        assert!(pack(b"tiny", 12).is_none());
        let mut raw = Vec::with_capacity(60 * 1024);
        let mut seed = 0u64;
        while raw.len() < 60 * 1024 {
            raw.extend_from_slice(&Sha256::digest(seed.to_be_bytes()));
            seed += 1;
        }
        assert!(pack(&raw, 12).is_none());
    }
}
