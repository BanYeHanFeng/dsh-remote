use std::path::PathBuf;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::error::{ToolError, ToolResult};
use super::transfer::{Incoming, TransferReport};
use crate::net::Sink;
use crate::proto::{EndStatus, Msg, STREAM_STDOUT};

pub struct Request {
    pub path: PathBuf,
    pub overwrite: bool,
    pub expected_size: Option<u64>,
    pub max_file: u64,
    pub reply_id: u64,
    pub sink: Sink,
    pub chunks: mpsc::Receiver<Msg>,
    pub cancel: CancellationToken,
}

pub async fn run(request: Request) {
    let Request {
        path,
        overwrite,
        expected_size,
        max_file,
        reply_id,
        sink,
        mut chunks,
        cancel,
    } = request;

    if let Some(size) = expected_size {
        if size > max_file {
            super::reply_error(
                &sink,
                reply_id,
                "too_large",
                format!("文件声明 {size} 字节，超过上限 {max_file} 字节"),
            )
            .await;
            return;
        }
    }

    let mut incoming = match Incoming::open(path, overwrite, max_file).await {
        Ok(incoming) => incoming,
        Err(err) => {
            super::reply_error(&sink, reply_id, err.code, err.message).await;
            return;
        }
    };

    let deadline = super::transfer::transfer_limit(expected_size.unwrap_or(0));
    let outcome = tokio::time::timeout(deadline, pump(&mut incoming, &mut chunks, &cancel)).await;
    match outcome {
        Ok(Ok(report)) => {
            super::reply_transfer(&sink, reply_id, &report).await;
        }
        Ok(Err(err)) => {
            incoming.abort().await;
            super::reply_error(&sink, reply_id, err.code, err.message).await;
        }
        Err(_) => {
            incoming.abort().await;
            super::reply_error(&sink, reply_id, "timeout", "上传超时，临时文件已丢弃").await;
        }
    }
}

async fn pump(
    incoming: &mut Incoming,
    chunks: &mut mpsc::Receiver<Msg>,
    cancel: &CancellationToken,
) -> ToolResult<TransferReport> {
    loop {
        let msg = tokio::select! {
            _ = cancel.cancelled() => {
                return Err(ToolError::new("cancelled", "上传已取消"));
            }
            msg = chunks.recv() => match msg {
                Some(msg) => msg,
                None => return Err(ToolError::io("上传流已断开")),
            },
        };
        match msg {
            Msg::Chunk {
                stream, seq, data, ..
            } => {
                if stream != STREAM_STDOUT {
                    return Err(ToolError::protocol("上传只接受数据流"));
                }
                incoming.push(seq, &data).await?;
            }
            Msg::End {
                status,
                size,
                sha256,
                ..
            } => {
                if status != EndStatus::Ok {
                    return Err(ToolError::new("io", format!("上传被中断: {status:?}")));
                }
                return incoming.finish(size, sha256.as_deref()).await;
            }
            other => {
                return Err(ToolError::protocol(format!(
                    "上传流中收到意外消息: {other:?}"
                )));
            }
        }
    }
}
