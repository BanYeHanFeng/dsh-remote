use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::mpsc;

use crate::fileio;
use crate::frame::{read_frame, write_frame};
use crate::protocol::{Failure, OpFrame, OpRequest, Role};
use crate::text::ChunkDecoder;
use crate::transport::{self, Stream};

pub struct AgentOptions {
    pub connect: String,
    pub id: String,
    pub tags: Vec<String>,
    pub psk: Vec<u8>,
}

pub async fn run(opts: AgentOptions) -> Result<()> {
    let mut backoff = 1u64;
    loop {
        match session(&opts).await {
            Ok(()) => {
                tracing::info!("与控制端的会话已结束，准备重连");
                backoff = 1;
            }
            Err(error) => {
                tracing::warn!(error = %format!("{error:#}"), "连接控制端失败");
                backoff = (backoff * 2).min(30);
            }
        }
        tokio::time::sleep(Duration::from_secs(backoff)).await;
    }
}

async fn session(opts: &AgentOptions) -> Result<()> {
    let session = transport::connect(&opts.connect, &opts.psk, Role::Agent, &opts.id, &opts.tags)
        .await
        .with_context(|| format!("接入 {} 失败", opts.connect))?;
    let mut control = session.open().await.context("打开控制流失败")?;
    write_frame(
        &mut control,
        &crate::protocol::ControlMsg::Hello {
            node_id: opts.id.clone(),
        },
    )
    .await
    .context("发送控制流问候失败")?;
    tracing::info!(node = %opts.id, peer = %session.peer(), "已接入控制端");

    let mut control_task = tokio::spawn(control_loop(control));
    loop {
        tokio::select! {
            accepted = session.accept() => match accepted {
                Ok(stream) => {
                    tokio::spawn(async move {
                        if let Err(error) = handle_op(stream).await {
                            tracing::warn!(error = %format!("{error:#}"), "处理远程操作失败");
                        }
                    });
                }
                Err(_) => break,
            },
            _ = &mut control_task => break,
            _ = session.closed() => {
                tracing::info!("与控制端的连接已关闭");
                break;
            }
        }
    }
    control_task.abort();
    session.close().await;
    Ok(())
}

async fn control_loop(mut control: Stream) {
    while read_frame::<_, crate::protocol::ControlMsg>(&mut control)
        .await
        .is_ok()
    {}
}

async fn handle_op(mut stream: Stream) -> Result<()> {
    let request: OpRequest = match read_frame(&mut stream).await {
        Ok(request) => request,
        Err(error) => {
            tracing::warn!(error = %format!("{error:#}"), "读取操作请求失败");
            return Ok(());
        }
    };
    match request {
        OpRequest::Exec {
            command,
            workdir,
            timeout_secs,
        } => run_exec(stream, command, workdir, timeout_secs).await,
        OpRequest::Upload {
            remote_path,
            size,
            mode,
            overwrite,
        } => run_upload(stream, remote_path, size, mode, overwrite).await,
        OpRequest::Download { remote_path } => run_download(stream, remote_path).await,
    }
    Ok(())
}

async fn run_exec(stream: Stream, command: String, workdir: Option<String>, timeout_secs: u64) {
    let (mut recv, send) = tokio::io::split(stream);
    let mut cmd = Command::new("bash");
    cmd.arg("-c")
        .arg(&command)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    if let Some(dir) = &workdir {
        cmd.current_dir(dir);
    }

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(error) => {
            let failure = Failure::io("启动 bash 失败", error);
            let mut send = send;
            let _ = write_frame(&mut send, &OpFrame::err(&failure)).await;
            let _ = send.shutdown().await;
            return;
        }
    };
    let pid = child.id();

    let (tx, mut rx) = mpsc::channel::<OpFrame>(256);
    let (broken_tx, mut broken_rx) = mpsc::channel::<()>(1);
    let cancel_tx = broken_tx.clone();
    let watcher = tokio::spawn(async move {
        let mut buf = [0u8; 64];
        loop {
            match recv.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(_) => continue,
            }
        }
        let _ = cancel_tx.send(()).await;
    });
    let writer = tokio::spawn(async move {
        let mut send = send;
        while let Some(frame) = rx.recv().await {
            if write_frame(&mut send, &frame).await.is_err() {
                let _ = broken_tx.send(()).await;
                return;
            }
        }
        let _ = send.shutdown().await;
    });

    let mut readers = Vec::new();
    if let Some(stdout) = child.stdout.take() {
        readers.push(tokio::spawn(pump(stdout, tx.clone(), true)));
    }
    if let Some(stderr) = child.stderr.take() {
        readers.push(tokio::spawn(pump(stderr, tx.clone(), false)));
    }

    let wait = child.wait();
    tokio::pin!(wait);
    let limit = Duration::from_secs(timeout_secs.max(1));
    let mut timed_out = false;

    let status = tokio::select! {
        status = &mut wait => status.ok(),
        _ = tokio::time::sleep(limit) => {
            timed_out = true;
            fileio::kill_group(pid);
            (&mut wait).await.ok()
        }
        _ = broken_rx.recv() => {
            fileio::kill_group(pid);
            (&mut wait).await.ok()
        }
    };

    for reader in readers {
        let _ = tokio::time::timeout(Duration::from_secs(2), reader).await;
    }

    use std::os::unix::process::ExitStatusExt;
    let frame = OpFrame::Exit {
        code: status.as_ref().and_then(|s| s.code()),
        signal: status.as_ref().and_then(|s| s.signal()),
        timed_out,
    };
    let _ = tx.send(frame).await;
    drop(tx);
    watcher.abort();
    let _ = tokio::time::timeout(Duration::from_secs(5), writer).await;
}

async fn pump<R>(mut reader: R, tx: mpsc::Sender<OpFrame>, is_stdout: bool)
where
    R: AsyncRead + Unpin,
{
    let mut decoder = ChunkDecoder::default();
    let mut buf = vec![0u8; 32 * 1024];
    loop {
        match reader.read(&mut buf).await {
            Ok(0) => break,
            Ok(read) => {
                let text = decoder.push(&buf[..read]);
                if !text.is_empty() && !deliver(&tx, is_stdout, text).await {
                    return;
                }
            }
            Err(_) => break,
        }
    }
    let rest = decoder.finish();
    if !rest.is_empty() {
        deliver(&tx, is_stdout, rest).await;
    }
}

async fn deliver(tx: &mpsc::Sender<OpFrame>, is_stdout: bool, text: String) -> bool {
    let frame = if is_stdout {
        OpFrame::Stdout { d: text }
    } else {
        OpFrame::Stderr { d: text }
    };
    tx.send(frame).await.is_ok()
}

async fn run_upload(
    mut stream: Stream,
    remote_path: String,
    size: u64,
    mode: Option<u32>,
    overwrite: bool,
) {
    let path = PathBuf::from(&remote_path);
    let mut target = match fileio::begin_recv(&path, overwrite).await {
        Ok(target) => target,
        Err(failure) => {
            let _ = write_frame(&mut stream, &OpFrame::err(&failure)).await;
            let _ = stream.shutdown().await;
            return;
        }
    };
    if write_frame(&mut stream, &OpFrame::Ack { size, mode })
        .await
        .is_err()
    {
        return;
    }
    let sha256 = match target.write_from(&mut stream, size).await {
        Ok(sha256) => sha256,
        Err(failure) => {
            let _ = write_frame(&mut stream, &OpFrame::err(&failure)).await;
            let _ = stream.shutdown().await;
            return;
        }
    };
    let frame = match target.commit(mode).await {
        Ok(()) => OpFrame::Done {
            bytes: size,
            sha256,
        },
        Err(failure) => OpFrame::err(&failure),
    };
    let _ = write_frame(&mut stream, &frame).await;
    let _ = stream.shutdown().await;
}

async fn run_download(mut stream: Stream, remote_path: String) {
    let path = PathBuf::from(&remote_path);
    let (mut file, size, mode) = match fileio::open_regular(&path).await {
        Ok(parts) => parts,
        Err(failure) => {
            let _ = write_frame(&mut stream, &OpFrame::err(&failure)).await;
            let _ = stream.shutdown().await;
            return;
        }
    };
    if write_frame(&mut stream, &OpFrame::Ack { size, mode })
        .await
        .is_err()
    {
        return;
    }
    match fileio::pump_file_to(&mut file, &mut stream, size).await {
        Ok(sha256) => {
            let _ = write_frame(
                &mut stream,
                &OpFrame::Done {
                    bytes: size,
                    sha256,
                },
            )
            .await;
        }
        Err(failure) => {
            let _ = write_frame(&mut stream, &OpFrame::err(&failure)).await;
        }
    }
    let _ = stream.shutdown().await;
}

pub fn default_node_id() -> String {
    hostname::get()
        .ok()
        .and_then(|name| name.into_string().ok())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| format!("node-{}", std::process::id()))
}
