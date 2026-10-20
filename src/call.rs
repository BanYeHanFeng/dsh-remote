use std::path::Path;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::config::ClientConfig;
use crate::exec::files::{EditReport, ListReport, ReadReport, MAX_WRITE_BYTES};
use crate::exec::transfer::{transfer_limit, TransferReport, CHUNK_SIZE};
use crate::proto::value;
use crate::proto::{EndStatus, Msg, TargetInfo, STREAM_STDOUT};
use crate::session::{CallHandle, Session};

pub use crate::session::{CallResult, RemoteError, RemoteResult};

pub async fn connect(cfg: &ClientConfig) -> Result<Session> {
    let session = Session::new(cfg);
    session.connect_now(CancellationToken::new()).await?;
    Ok(session)
}

pub async fn targets(session: &Session) -> Result<Vec<TargetInfo>> {
    let result = session
        .call(
            "hub",
            "targets",
            value::empty_map(),
            Duration::from_secs(10),
        )
        .await?;
    let value = result.into_value()?;
    value::deserialize(&value)
}

pub async fn call(
    session: &Session,
    target: &str,
    method: &str,
    params: ciborium::value::Value,
    limit: Duration,
    cancel: &CancellationToken,
) -> RemoteResult<CallResult> {
    session
        .call_cancel(target, method, params, limit, cancel)
        .await
}

async fn call_value(
    session: &Session,
    target: &str,
    method: &str,
    params: ciborium::value::Value,
    limit: Duration,
    cancel: &CancellationToken,
) -> Result<ciborium::value::Value> {
    let result = session
        .call_cancel(target, method, params, limit, cancel)
        .await?;
    result.into_value().map_err(Into::into)
}

pub async fn read(
    session: &Session,
    target: &str,
    path: &str,
    offset: u64,
    limit: u64,
    cancel: &CancellationToken,
) -> Result<ReadReport> {
    let params = value::map(vec![
        ("path", ciborium::value::Value::Text(path.to_string())),
        ("offset", ciborium::value::Value::Integer(offset.into())),
        ("limit", ciborium::value::Value::Integer(limit.into())),
    ]);
    let value = call_value(
        session,
        target,
        "read",
        params,
        Duration::from_secs(60),
        cancel,
    )
    .await?;
    value::deserialize(&value)
}

pub async fn write(
    session: &Session,
    target: &str,
    path: &str,
    content: &str,
    create_parents: bool,
    cancel: &CancellationToken,
) -> Result<crate::exec::files::WriteReport> {
    if content.len() > MAX_WRITE_BYTES {
        bail!(
            "内容 {} 字节超过 write 上限 {MAX_WRITE_BYTES} 字节，请改用 upload",
            content.len()
        );
    }
    let params = value::map(vec![
        ("path", ciborium::value::Value::Text(path.to_string())),
        ("content", ciborium::value::Value::Text(content.to_string())),
        (
            "create_parents",
            ciborium::value::Value::Bool(create_parents),
        ),
    ]);
    let value = call_value(
        session,
        target,
        "write",
        params,
        Duration::from_secs(60),
        cancel,
    )
    .await?;
    value::deserialize(&value)
}

pub async fn edit(
    session: &Session,
    target: &str,
    path: &str,
    old_string: &str,
    new_string: &str,
    replace_all: bool,
    cancel: &CancellationToken,
) -> Result<EditReport> {
    if old_string.len() + new_string.len() > MAX_WRITE_BYTES {
        bail!("old_string 与 new_string 合计超过 {MAX_WRITE_BYTES} 字节");
    }
    let params = value::map(vec![
        ("path", ciborium::value::Value::Text(path.to_string())),
        (
            "old_string",
            ciborium::value::Value::Text(old_string.to_string()),
        ),
        (
            "new_string",
            ciborium::value::Value::Text(new_string.to_string()),
        ),
        ("replace_all", ciborium::value::Value::Bool(replace_all)),
    ]);
    let value = call_value(
        session,
        target,
        "edit",
        params,
        Duration::from_secs(60),
        cancel,
    )
    .await?;
    value::deserialize(&value)
}

pub async fn list(
    session: &Session,
    target: &str,
    path: &str,
    depth: u64,
    cancel: &CancellationToken,
) -> Result<ListReport> {
    let params = value::map(vec![
        ("path", ciborium::value::Value::Text(path.to_string())),
        ("depth", ciborium::value::Value::Integer(depth.into())),
    ]);
    let value = call_value(
        session,
        target,
        "list",
        params,
        Duration::from_secs(60),
        cancel,
    )
    .await?;
    value::deserialize(&value)
}

pub async fn upload(
    session: &Session,
    target: &str,
    local: &Path,
    remote: &str,
    overwrite: bool,
    create_parents: bool,
    cancel: &CancellationToken,
) -> Result<TransferReport> {
    let mut file = tokio::fs::File::open(local)
        .await
        .with_context(|| format!("打开本地文件失败: {}", local.display()))?;
    let meta = file.metadata().await?;
    if !meta.is_file() {
        bail!("本地路径不是普通文件: {}", local.display());
    }
    if meta.len() > session.max_file() {
        bail!(
            "本地文件 {} 字节超过上限 {} 字节",
            meta.len(),
            session.max_file()
        );
    }

    let params = value::map(vec![
        (
            "remote_path",
            ciborium::value::Value::Text(remote.to_string()),
        ),
        ("size", ciborium::value::Value::Integer(meta.len().into())),
        ("overwrite", ciborium::value::Value::Bool(overwrite)),
        (
            "create_parents",
            ciborium::value::Value::Bool(create_parents),
        ),
    ]);
    let mut handle = session.begin(target, "upload", params).await?;
    let outcome = stream_upload(&mut handle, &mut file, meta.len(), local, cancel).await;
    let digest = match outcome {
        Ok(digest) => digest,
        Err(err) => {
            handle.cancel("upload aborted").await;
            return Err(err);
        }
    };
    let id = handle.id();
    handle
        .send(Msg::End {
            id,
            status: EndStatus::Ok,
            exit_code: None,
            size: Some(meta.len()),
            sha256: Some(digest),
            truncated: Some(false),
        })
        .await?;

    let deadline = transfer_limit(meta.len()) + Duration::from_secs(10);
    let result = handle.finish(deadline, cancel).await?;
    let value = result.into_value()?;
    value::deserialize(&value)
}

async fn stream_upload(
    handle: &mut CallHandle,
    file: &mut tokio::fs::File,
    expected: u64,
    local: &Path,
    cancel: &CancellationToken,
) -> Result<String> {
    let id = handle.id();
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; CHUNK_SIZE];
    let mut seq = 0u64;
    let mut sent = 0u64;
    loop {
        if cancel.is_cancelled() {
            bail!("上传已取消");
        }
        if let Some(msg) = handle.try_recv() {
            if let Some(err) = early_failure(msg) {
                return Err(err);
            }
        }
        let read = file
            .read(&mut buffer)
            .await
            .with_context(|| format!("读取本地文件失败: {}", local.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        sent += read as u64;
        handle
            .send(Msg::Chunk {
                id,
                seq,
                stream: STREAM_STDOUT,
                data: buffer[..read].to_vec(),
            })
            .await?;
        seq += 1;
    }
    if sent != expected {
        bail!("本地文件在上传过程中发生变化：声明 {expected} 字节，实际 {sent} 字节");
    }
    Ok(hex::encode(hasher.finalize()))
}

fn early_failure(msg: Msg) -> Option<anyhow::Error> {
    match msg {
        Msg::Result { ok, error, .. } if !ok => Some(match error {
            Some(error) => RemoteError::new(error.code, error.message).into(),
            None => anyhow!("上传失败"),
        }),
        Msg::End { status, .. } if status != EndStatus::Ok => Some(anyhow!("上传中断: {status:?}")),
        _ => None,
    }
}

pub async fn download(
    session: &Session,
    target: &str,
    remote: &str,
    local: &Path,
    overwrite: bool,
    cancel: &CancellationToken,
) -> Result<TransferReport> {
    if local.is_dir() {
        bail!("本地路径是目录: {}", local.display());
    }
    if !overwrite && local.exists() {
        bail!("本地文件已存在: {}", local.display());
    }
    let parent = local.parent().filter(|path| !path.as_os_str().is_empty());
    let parent = parent.unwrap_or_else(|| Path::new("."));
    if !parent.exists() {
        bail!("本地目录不存在: {}", parent.display());
    }
    let temp = parent.join(format!(
        ".dsh-remote-{}-{:016x}.part",
        std::process::id(),
        rand::random::<u64>()
    ));

    let max_file = session.max_file();
    let params = value::map(vec![
        (
            "remote_path",
            ciborium::value::Value::Text(remote.to_string()),
        ),
        ("max_size", ciborium::value::Value::Integer(max_file.into())),
    ]);
    let mut handle = session.begin(target, "download", params).await?;

    let deadline = transfer_limit(max_file) + Duration::from_secs(10);
    let mut created = false;
    let outcome = timeout(deadline, async {
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .await
            .with_context(|| format!("创建临时文件失败: {}", temp.display()))?;
        created = true;
        let mut hasher = Sha256::new();
        let mut next_seq = 0u64;
        let mut received = 0u64;
        loop {
            let msg = tokio::select! {
                _ = cancel.cancelled() => bail!("下载已取消"),
                msg = handle.recv() => msg,
            };
            let Some(msg) = msg else {
                bail!("中继连接中断，下载未完成");
            };
            match msg {
                Msg::Chunk {
                    seq, stream, data, ..
                } => {
                    if stream != STREAM_STDOUT {
                        bail!("下载只接受数据流");
                    }
                    if seq != next_seq {
                        bail!("下载分块序号错乱：期望 {next_seq}，收到 {seq}");
                    }
                    received += data.len() as u64;
                    if received > max_file {
                        bail!("下载超过上限 {max_file} 字节");
                    }
                    file.write_all(&data).await?;
                    hasher.update(&data);
                    next_seq += 1;
                }
                Msg::End {
                    status,
                    size,
                    sha256,
                    ..
                } => {
                    if status != EndStatus::Ok {
                        bail!("下载中断: {status:?}");
                    }
                    if let Some(size) = size {
                        if size != received {
                            bail!("下载字节数不一致：声明 {size}，收到 {received}");
                        }
                    }
                    let digest = hex::encode(hasher.finalize());
                    if let Some(expected) = sha256 {
                        if !expected.eq_ignore_ascii_case(&digest) {
                            bail!("下载 sha256 校验失败");
                        }
                    }
                    file.flush().await?;
                    file.sync_all().await?;
                    drop(file);
                    tokio::fs::rename(&temp, local)
                        .await
                        .with_context(|| format!("重命名到 {} 失败", local.display()))?;
                    return Ok(TransferReport {
                        path: local.display().to_string(),
                        size: received,
                        sha256: digest,
                    });
                }
                Msg::Result { ok, error, .. } => {
                    if ok {
                        bail!("下载收到了意外的成功结果");
                    }
                    return Err(match error {
                        Some(error) => RemoteError::new(error.code, error.message).into(),
                        None => anyhow!("下载失败"),
                    });
                }
                Msg::Cancel { .. } => {}
                other => debug!(msg = ?other, "忽略消息"),
            }
        }
    })
    .await;

    match outcome {
        Ok(Ok(report)) => Ok(report),
        Ok(Err(err)) => {
            handle.cancel("download aborted").await;
            if created {
                let _ = tokio::fs::remove_file(&temp).await;
            }
            Err(err)
        }
        Err(_) => {
            handle.cancel("download timeout").await;
            if created {
                let _ = tokio::fs::remove_file(&temp).await;
            }
            bail!("下载超时")
        }
    }
}
