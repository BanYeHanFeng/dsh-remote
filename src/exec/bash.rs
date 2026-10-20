use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::proto::{EndStatus, Msg};

pub const CHUNK_SIZE: usize = 32 * 1024;

pub struct Request {
    pub command: String,
    pub cwd: PathBuf,
    pub timeout: Duration,
    pub stdin: Option<Vec<u8>>,
    pub max_output: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stop {
    Timeout,
    Cancelled,
}

pub struct Outcome {
    pub exit_code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub truncated: bool,
    pub stop: Option<Stop>,
}

pub async fn run(request: Request, cancel: CancellationToken) -> Result<Outcome> {
    let mut command = Command::new("sh");
    command.arg("-c").arg(&request.command);
    command.current_dir(&request.cwd);
    command.stdin(if request.stdin.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    });
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    command.kill_on_drop(true);
    set_process_group(&mut command);

    let mut child = command
        .spawn()
        .with_context(|| format!("启动命令失败: {}", request.command))?;
    let pid = child.id().unwrap_or_default() as i32;

    if let Some(input) = request.stdin.clone() {
        if let Some(mut stdin) = child.stdin.take() {
            tokio::spawn(async move {
                let _ = stdin.write_all(&input).await;
            });
        }
    }

    let stdout_task = tokio::spawn(read_limited(child.stdout.take(), request.max_output));
    let stderr_task = tokio::spawn(read_limited(child.stderr.take(), request.max_output));

    let (stop, status) = tokio::select! {
        status = child.wait() => (None, Some(status?)),
        _ = tokio::time::sleep(request.timeout) => (Some(Stop::Timeout), None),
        _ = cancel.cancelled() => (Some(Stop::Cancelled), None),
    };

    if stop.is_some() {
        kill_group(pid);
        let _ = child.wait().await;
    }

    let (stdout, stdout_truncated) = stdout_task.await??;
    let (stderr, stderr_truncated) = stderr_task.await??;

    Ok(Outcome {
        exit_code: status.and_then(|status| status.code()),
        stdout,
        stderr,
        truncated: stdout_truncated || stderr_truncated,
        stop,
    })
}

pub async fn send_output(sink: &mpsc::Sender<Msg>, reply_id: u64, outcome: &Outcome) -> Result<()> {
    send_chunks(sink, reply_id, crate::proto::STREAM_STDOUT, &outcome.stdout).await?;
    send_chunks(sink, reply_id, crate::proto::STREAM_STDERR, &outcome.stderr).await?;
    let status = match outcome.stop {
        Some(Stop::Timeout) => EndStatus::Error,
        Some(Stop::Cancelled) => EndStatus::Cancelled,
        None => EndStatus::Ok,
    };
    sink.send(Msg::End {
        id: reply_id,
        status,
        exit_code: outcome.exit_code,
        size: None,
        sha256: None,
        truncated: Some(outcome.truncated),
    })
    .await
    .map_err(|_| anyhow!("发送执行结果失败"))
}

async fn send_chunks(
    sink: &mpsc::Sender<Msg>,
    reply_id: u64,
    stream: u8,
    data: &[u8],
) -> Result<()> {
    for (seq, chunk) in data.chunks(CHUNK_SIZE).enumerate() {
        sink.send(Msg::Chunk {
            id: reply_id,
            seq: seq as u64,
            stream,
            data: chunk.to_vec(),
        })
        .await
        .map_err(|_| anyhow!("发送输出失败"))?;
    }
    Ok(())
}

async fn read_limited<R>(reader: Option<R>, limit: usize) -> Result<(Vec<u8>, bool)>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let Some(mut reader) = reader else {
        return Ok((Vec::new(), false));
    };
    let mut buffer = vec![0u8; 16 * 1024];
    let mut data = Vec::new();
    let mut truncated = false;
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        if data.len() < limit {
            let take = (limit - data.len()).min(read);
            data.extend_from_slice(&buffer[..take]);
            if take < read {
                truncated = true;
            }
        } else {
            truncated = true;
        }
    }
    Ok((data, truncated))
}

#[cfg(unix)]
fn set_process_group(command: &mut Command) {
    command.process_group(0);
}

#[cfg(not(unix))]
fn set_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn kill_group(pid: i32) {
    if pid > 0 {
        unsafe {
            libc::kill(-pid, libc::SIGKILL);
        }
    }
}

#[cfg(not(unix))]
fn kill_group(_pid: i32) {}
