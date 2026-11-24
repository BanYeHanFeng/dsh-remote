use std::path::PathBuf;
use std::time::Duration;

use tokio::sync::mpsc::Sender;

use crate::controller::Controller;
use crate::fileio;
use crate::frame::{read_frame, write_frame};
use crate::ipc::{IpcEvent, IpcRequest};
use crate::protocol::{ControlMsg, ErrorCode, Failure, OpFrame, OpRequest, TransferDone};
use crate::transport::Stream;

type Sink = Sender<IpcEvent>;

pub async fn run(ctl: &Controller, request: IpcRequest, sink: Sink) {
    let outcome = match request {
        IpcRequest::Nodes => {
            let nodes = ctl.list().await;
            send(&sink, IpcEvent::Nodes { nodes }).await
        }
        IpcRequest::Exec {
            node,
            command,
            workdir,
            timeout_secs,
        } => exec(ctl, &node, command, workdir, timeout_secs, &sink).await,
        IpcRequest::Upload {
            node,
            local_path,
            remote_path,
            overwrite,
        } => upload(ctl, &node, &local_path, &remote_path, overwrite, &sink).await,
        IpcRequest::Download {
            node,
            remote_path,
            local_path,
            overwrite,
        } => download(ctl, &node, &remote_path, &local_path, overwrite, &sink).await,
        IpcRequest::Transfer {
            src_node,
            src_path,
            dst_node,
            dst_path,
            overwrite,
        } => {
            transfer(
                ctl, &src_node, &src_path, &dst_node, &dst_path, overwrite, &sink,
            )
            .await
        }
    };

    if let Err(failure) = outcome {
        let _ = sink
            .send(IpcEvent::Err {
                code: failure.code,
                message: failure.message,
            })
            .await;
    }
}

async fn send(sink: &Sink, event: IpcEvent) -> Result<(), Failure> {
    sink.send(event)
        .await
        .map_err(|_| Failure::new(ErrorCode::Internal, "本地调用方已断开"))
}

async fn exec(
    ctl: &Controller,
    node: &str,
    command: String,
    workdir: Option<String>,
    timeout_secs: u64,
    sink: &Sink,
) -> Result<(), Failure> {
    let mut stream = ctl.route(node).await?;
    write_frame(
        &mut stream,
        &OpRequest::Exec {
            command,
            workdir,
            timeout_secs,
        },
    )
    .await
    .map_err(|error| Failure::new(ErrorCode::IoError, format!("发送执行请求失败：{:#}", error)))?;

    let guard = Duration::from_secs(timeout_secs.saturating_add(30).max(30));
    let relay = async {
        loop {
            let frame: OpFrame = read_frame(&mut stream).await.map_err(|error| {
                Failure::new(
                    ErrorCode::IoError,
                    format!("读取节点 {} 的响应失败：{:#}", node, error),
                )
            })?;
            match frame {
                OpFrame::Stdout { d } => send(sink, IpcEvent::Stdout { d }).await?,
                OpFrame::Stderr { d } => send(sink, IpcEvent::Stderr { d }).await?,
                OpFrame::Exit {
                    code,
                    signal,
                    timed_out,
                } => {
                    send(
                        sink,
                        IpcEvent::Exit {
                            code,
                            signal,
                            timed_out,
                        },
                    )
                    .await?;
                    return Ok(());
                }
                OpFrame::Err { code, message } => return Err(Failure { code, message }),
                _ => {}
            }
        }
    };

    tokio::time::timeout(guard, relay)
        .await
        .map_err(|_| Failure::new(ErrorCode::Timeout, format!("等待节点 {} 的响应超时", node)))?
}

async fn upload(
    ctl: &Controller,
    node: &str,
    local_path: &str,
    remote_path: &str,
    overwrite: bool,
    sink: &Sink,
) -> Result<(), Failure> {
    let path = PathBuf::from(local_path);
    let (mut file, size, mode) = fileio::open_regular(&path).await?;
    let mut stream = ctl.route(node).await?;
    write_frame(
        &mut stream,
        &OpRequest::Upload {
            remote_path: remote_path.to_string(),
            size,
            mode,
            overwrite,
        },
    )
    .await
    .map_err(|error| Failure::new(ErrorCode::IoError, format!("发送上传请求失败：{:#}", error)))?;

    let ack = read_reply(&mut stream, node).await?;
    match ack {
        OpFrame::Ack { .. } => {}
        other => {
            return Err(other.into_result().err().unwrap_or_else(|| {
                Failure::new(ErrorCode::Internal, "节点返回了意料之外的响应帧")
            }))
        }
    }

    let pumped = fileio::pump_file_to(&mut file, &mut stream, size).await;
    let frame: OpFrame = match read_frame(&mut stream).await {
        Ok(frame) => frame,
        Err(error) => {
            return Err(pumped.err().unwrap_or_else(|| {
                Failure::new(
                    ErrorCode::IoError,
                    format!("读取节点 {} 的响应失败：{:#}", node, error),
                )
            }))
        }
    };
    let done = frame.into_result()?;
    let sha256 = pumped?;
    if done.sha256 != sha256 {
        return Err(Failure::new(
            ErrorCode::ChecksumMismatch,
            format!("节点 {} 收到的数据校验值不一致", node),
        ));
    }
    send(
        sink,
        IpcEvent::Done {
            bytes: done.bytes,
            sha256: done.sha256,
            from: None,
            to: None,
        },
    )
    .await
}

async fn download(
    ctl: &Controller,
    node: &str,
    remote_path: &str,
    local_path: &str,
    overwrite: bool,
    sink: &Sink,
) -> Result<(), Failure> {
    if !overwrite && tokio::fs::metadata(local_path).await.is_ok() {
        return Err(Failure::new(
            ErrorCode::AlreadyExists,
            format!("{} 已存在", local_path),
        ));
    }
    let mut stream = ctl.route(node).await?;
    write_frame(
        &mut stream,
        &OpRequest::Download {
            remote_path: remote_path.to_string(),
        },
    )
    .await
    .map_err(|error| Failure::new(ErrorCode::IoError, format!("发送下载请求失败：{:#}", error)))?;

    let (size, mode) = match read_reply(&mut stream, node).await? {
        OpFrame::Ack { size, mode } => (size, mode),
        other => {
            return Err(other.into_result().err().unwrap_or_else(|| {
                Failure::new(ErrorCode::Internal, "节点返回了意料之外的响应帧")
            }))
        }
    };

    let path = PathBuf::from(local_path);
    let (bytes, sha256) = fileio::recv_file(&mut stream, &path, size, mode, overwrite).await?;
    let done = read_reply(&mut stream, node).await?.into_result()?;
    if done.sha256 != sha256 {
        return Err(Failure::new(
            ErrorCode::ChecksumMismatch,
            format!("从节点 {} 下载的数据校验值不一致", node),
        ));
    }
    send(
        sink,
        IpcEvent::Done {
            bytes,
            sha256: done.sha256,
            from: None,
            to: None,
        },
    )
    .await
}

async fn transfer(
    ctl: &Controller,
    src_node: &str,
    src_path: &str,
    dst_node: &str,
    dst_path: &str,
    overwrite: bool,
    sink: &Sink,
) -> Result<(), Failure> {
    let done = if ctl.same_relay(src_node, dst_node).await {
        match ctl.relay_link().await {
            Some(link) => {
                link.request(|req_id| ControlMsg::BridgeTransfer {
                    req_id,
                    src_node: src_node.to_string(),
                    src_path: src_path.to_string(),
                    dst_node: dst_node.to_string(),
                    dst_path: dst_path.to_string(),
                    overwrite,
                })
                .await?
            }
            None => stream_between(ctl, src_node, src_path, dst_node, dst_path, overwrite).await?,
        }
    } else {
        stream_between(ctl, src_node, src_path, dst_node, dst_path, overwrite).await?
    };

    send(
        sink,
        IpcEvent::Done {
            bytes: done.bytes,
            sha256: done.sha256,
            from: Some((src_node.to_string(), src_path.to_string())),
            to: Some((dst_node.to_string(), dst_path.to_string())),
        },
    )
    .await
}

async fn stream_between(
    ctl: &Controller,
    src_node: &str,
    src_path: &str,
    dst_node: &str,
    dst_path: &str,
    overwrite: bool,
) -> Result<TransferDone, Failure> {
    let mut dl = ctl.route(src_node).await?;
    write_frame(
        &mut dl,
        &OpRequest::Download {
            remote_path: src_path.to_string(),
        },
    )
    .await
    .map_err(|error| Failure::new(ErrorCode::IoError, format!("发送下载请求失败：{:#}", error)))?;
    let (size, mode) = match read_reply(&mut dl, src_node).await? {
        OpFrame::Ack { size, mode } => (size, mode),
        OpFrame::Err { code, message } => return Err(Failure { code, message }),
        _ => {
            return Err(Failure::new(
                ErrorCode::Internal,
                "源节点返回了意料之外的响应帧",
            ))
        }
    };

    let mut up = ctl.route(dst_node).await?;
    write_frame(
        &mut up,
        &OpRequest::Upload {
            remote_path: dst_path.to_string(),
            size,
            mode,
            overwrite,
        },
    )
    .await
    .map_err(|error| Failure::new(ErrorCode::IoError, format!("发送上传请求失败：{:#}", error)))?;
    match read_reply(&mut up, dst_node).await? {
        OpFrame::Ack { .. } => {}
        other => {
            return Err(other.into_result().err().unwrap_or_else(|| {
                Failure::new(ErrorCode::Internal, "目标节点返回了意料之外的响应帧")
            }))
        }
    }

    let sha256 = fileio::write_hashed_copy(&mut dl, &mut up, size).await?;
    let source = read_final(&mut dl, src_node).await?;
    let done = read_final(&mut up, dst_node).await?;
    if done.sha256 != sha256 || source.sha256 != sha256 {
        return Err(Failure::new(
            ErrorCode::ChecksumMismatch,
            "节点之间互传的数据校验值不一致",
        ));
    }
    Ok(done)
}

async fn read_final(stream: &mut Stream, node: &str) -> Result<TransferDone, Failure> {
    read_reply(stream, node).await?.into_result()
}

async fn read_reply(stream: &mut Stream, node: &str) -> Result<OpFrame, Failure> {
    read_frame(stream).await.map_err(|error| {
        Failure::new(
            ErrorCode::IoError,
            format!("读取节点 {} 的响应失败：{:#}", node, error),
        )
    })
}
