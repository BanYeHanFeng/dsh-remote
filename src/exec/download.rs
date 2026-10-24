use std::path::PathBuf;

use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;

use super::error::{ToolError, ToolResult};
use super::transfer::{pack, CHUNK_SIZE};
use crate::net::Sink;
use crate::proto::{EndStatus, Msg, STREAM_STDOUT};

pub struct Request {
    pub path: PathBuf,
    pub pack: bool,
    pub pack_gain: u8,
    pub max_file: u64,
    pub reply_id: u64,
    pub sink: Sink,
    pub cancel: CancellationToken,
}

pub async fn run(request: Request) {
    let Request {
        path,
        pack: pack_enabled,
        pack_gain,
        max_file,
        reply_id,
        sink,
        cancel,
    } = request;

    let mut file = match tokio::fs::File::open(&path).await {
        Ok(file) => file,
        Err(err) => {
            super::reply_error(
                &sink,
                reply_id,
                "io",
                format!("打开 {} 失败: {err}", path.display()),
            )
            .await;
            return;
        }
    };
    let meta = match file.metadata().await {
        Ok(meta) => meta,
        Err(err) => {
            super::reply_error(&sink, reply_id, "io", format!("读取文件信息失败: {err}")).await;
            return;
        }
    };
    if !meta.is_file() {
        super::reply_error(
            &sink,
            reply_id,
            "io",
            format!("不是普通文件: {}", path.display()),
        )
        .await;
        return;
    }
    let size = meta.len();
    if size > max_file {
        super::reply_error(
            &sink,
            reply_id,
            "too_large",
            format!("文件 {size} 字节超过下载上限 {max_file} 字节"),
        )
        .await;
        return;
    }

    let deadline = super::transfer::transfer_limit(size);
    let packing = pack_enabled.then_some(pack_gain);
    match tokio::time::timeout(
        deadline,
        stream(&mut file, &sink, reply_id, packing, &cancel),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            let status = match err.code {
                "cancelled" => EndStatus::Cancelled,
                _ => EndStatus::Error,
            };
            let _ = sink
                .send(Msg::End {
                    id: reply_id,
                    status,
                    size: None,
                    sha256: None,
                })
                .await;
        }
        Err(_) => {
            let _ = sink
                .send(Msg::End {
                    id: reply_id,
                    status: EndStatus::Error,
                    size: None,
                    sha256: None,
                })
                .await;
        }
    }
}

async fn stream(
    file: &mut tokio::fs::File,
    sink: &Sink,
    reply_id: u64,
    packing: Option<u8>,
    cancel: &CancellationToken,
) -> ToolResult<()> {
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; CHUNK_SIZE];
    let mut seq = 0u64;
    let mut sent = 0u64;
    loop {
        let read = tokio::select! {
            _ = cancel.cancelled() => {
                return Err(ToolError::new("cancelled", "下载已取消"));
            }
            read = file.read(&mut buffer) => read,
        };
        let read = read.map_err(|err| ToolError::io(format!("读取文件失败: {err}")))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        sent += read as u64;
        let (data, packed) = match packing.and_then(|gain| pack(&buffer[..read], gain)) {
            Some(packed) => (packed, true),
            None => (buffer[..read].to_vec(), false),
        };
        let chunk = Msg::Chunk {
            id: reply_id,
            seq,
            stream: STREAM_STDOUT,
            packed,
            data,
        };
        tokio::select! {
            _ = cancel.cancelled() => {
                return Err(ToolError::new("cancelled", "下载已取消"));
            }
            result = sink.send(chunk) => {
                if result.is_err() {
                    return Err(ToolError::io("发送下载数据失败"));
                }
            }
        }
        seq += 1;
    }
    let digest = hex::encode(hasher.finalize());
    sink.send(Msg::End {
        id: reply_id,
        status: EndStatus::Ok,
        size: Some(sent),
        sha256: Some(digest),
    })
    .await
    .map_err(|_| ToolError::io("发送下载结果失败"))?;
    Ok(())
}
