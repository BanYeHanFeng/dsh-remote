use std::io::ErrorKind;
use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, ChildStdout, Command};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::exec::error::{ToolError, ToolResult};

const READ_CHUNK: usize = 16 * 1024;
const SCROLLBACK_BYTES: usize = 1024 * 1024;
const TEXT_BYTES_LIMIT: usize = 48 * 1024;
const DRAIN_LIMIT: Duration = Duration::from_millis(200);

const TRUNCATED: &str =
    "<输出已截断> 为节省上下文只返回了一部分输出，建议先用 grep -n 定位行号再重试";
const LOST_PREFIX: &str = "<输出已截断> 命令输出的开头已超过保留上限，下面是仍保留的最早内容\n";

pub struct Request {
    pub command: String,
    pub cwd: String,
    pub timeout: Duration,
    pub max_output_chars: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Report {
    pub output: String,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
}

impl Report {
    pub fn failed(&self) -> bool {
        self.timed_out || self.exit_code != Some(0)
    }
}

pub async fn run(request: Request, cancel: &CancellationToken) -> ToolResult<Report> {
    let command = request.command.trim_end().to_string();
    if command.trim().is_empty() {
        return Err(ToolError::protocol("command 必须是非空字符串"));
    }
    if cancel.is_cancelled() {
        return Err(ToolError::new("cancelled", "命令已取消"));
    }
    let (mut child, stdout, pid) = spawn(&command, &request.cwd)?;
    let buffer = Arc::new(Mutex::new(Buffer {
        data: Vec::new(),
        lost: false,
    }));
    let mut reader = tokio::spawn(read_output(stdout, buffer.clone()));
    let deadline = Instant::now() + request.timeout;

    let ending = tokio::select! {
        _ = cancel.cancelled() => Ending::Cancelled,
        _ = tokio::time::sleep_until(deadline) => Ending::TimedOut,
        status = child.wait() => {
            let status = status
                .map_err(|err| ToolError::io(format!("等待命令结束失败: {err}")))?;
            Ending::Exited(status)
        }
    };

    if !matches!(ending, Ending::Exited(_)) {
        kill_group(pid);
        let _ = child.wait().await;
    }
    let captured = collect(&buffer, &mut reader).await;

    match ending {
        Ending::Cancelled => Err(ToolError::new("cancelled", "命令已取消")),
        Ending::TimedOut => {
            let note = timeout_note(request.timeout);
            Ok(captured.report(request.max_output_chars, None, &note, None, true))
        }
        Ending::Exited(status) => {
            let code = status.code();
            let line = match (code, signal_of(&status)) {
                (Some(code), _) => format!("[命令结束，退出码 {code}]"),
                (None, Some(signal)) => format!("[Shell 被信号终止：{signal}]"),
                (None, None) => "[命令已结束，未拿到退出码]".to_string(),
            };
            Ok(captured.report(request.max_output_chars, Some(&line), "", code, false))
        }
    }
}

enum Ending {
    Exited(std::process::ExitStatus),
    TimedOut,
    Cancelled,
}

fn spawn(command: &str, cwd: &str) -> ToolResult<(Child, ChildStdout, i32)> {
    match spawn_with("bash", command, cwd) {
        Ok(spawned) => Ok(spawned),
        Err(err) if err.kind() == ErrorKind::NotFound => spawn_with("sh", command, cwd)
            .map_err(|err| ToolError::io(format!("启动 Shell 失败（bash 与 sh 都不可用）: {err}"))),
        Err(err) => Err(ToolError::io(format!("启动 Shell 失败: {err}"))),
    }
}

fn spawn_with(
    program: &str,
    command: &str,
    cwd: &str,
) -> std::io::Result<(Child, ChildStdout, i32)> {
    let mut child_command = Command::new(program);
    child_command.arg("-c").arg(command);
    child_command.current_dir(Path::new(cwd));
    child_command.stdin(Stdio::null());
    child_command.stdout(Stdio::piped());
    #[cfg(unix)]
    child_command.stderr(Stdio::null());
    #[cfg(not(unix))]
    child_command.stderr(Stdio::piped());
    child_command.kill_on_drop(true);
    set_process_group(&mut child_command);
    merge_stderr(&mut child_command);
    let mut child = child_command.spawn()?;
    let pid = child.id().unwrap_or_default() as i32;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| std::io::Error::other("命令没有 stdout"))?;
    Ok((child, stdout, pid))
}

async fn read_output(mut stdout: ChildStdout, buffer: Arc<Mutex<Buffer>>) {
    let mut chunk = vec![0u8; READ_CHUNK];
    loop {
        match stdout.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(read) => push(&buffer, &chunk[..read]),
        }
    }
}

struct Captured {
    text: String,
    lost: bool,
}

impl Captured {
    fn report(
        &self,
        max_output_chars: usize,
        line: Option<&str>,
        note: &str,
        exit_code: Option<i32>,
        timed_out: bool,
    ) -> Report {
        Report {
            output: render(self, max_output_chars, line, note),
            exit_code,
            timed_out,
        }
    }
}

async fn collect(buffer: &Arc<Mutex<Buffer>>, reader: &mut JoinHandle<()>) -> Captured {
    if tokio::time::timeout(DRAIN_LIMIT, &mut *reader)
        .await
        .is_err()
    {
        reader.abort();
    }
    let buffer = lock(buffer);
    Captured {
        text: decode(&buffer.data),
        lost: buffer.lost,
    }
}

fn render(captured: &Captured, max_output_chars: usize, line: Option<&str>, note: &str) -> String {
    let mut body = truncate_chars(&captured.text, max_output_chars);
    if captured.lost && !body.is_empty() {
        body = format!("{LOST_PREFIX}{body}");
    }
    let tail = [line, Some(note).filter(|note| !note.is_empty())]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n");
    if body.is_empty() {
        return tail;
    }
    if tail.is_empty() {
        return body;
    }
    format!("{body}\n{tail}")
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    let over_chars = text.chars().count() > max_chars;
    let mut body = if over_chars {
        text.chars().take(max_chars).collect::<String>()
    } else {
        text.to_string()
    };
    let mut truncated = over_chars;
    if body.len() > TEXT_BYTES_LIMIT {
        let mut cut = TEXT_BYTES_LIMIT;
        while cut > 0 && !body.is_char_boundary(cut) {
            cut -= 1;
        }
        body.truncate(cut);
        truncated = true;
    }
    if truncated {
        if body.is_empty() {
            return TRUNCATED.to_string();
        }
        return format!("{body}\n{TRUNCATED}");
    }
    body
}

fn decode(raw: &[u8]) -> String {
    let raw = raw
        .strip_prefix(b"\r\n")
        .or_else(|| raw.strip_prefix(b"\n"))
        .unwrap_or(raw);
    let text = String::from_utf8_lossy(raw);
    text.trim_end_matches(['\r', '\n']).to_string()
}

fn timeout_note(timeout: Duration) -> String {
    let seconds = timeout.as_secs().max(1);
    format!("[命令超过 {seconds} 秒仍未结束，已被终止]")
}

struct Buffer {
    data: Vec<u8>,
    lost: bool,
}

fn push(target: &Arc<Mutex<Buffer>>, data: &[u8]) {
    let mut buffer = lock(target);
    buffer.data.extend_from_slice(data);
    if buffer.data.len() > SCROLLBACK_BYTES {
        let cut = trim_point(&buffer.data, buffer.data.len() - SCROLLBACK_BYTES);
        buffer.data.drain(..cut);
        buffer.lost = true;
    }
}

fn trim_point(data: &[u8], from: usize) -> usize {
    let mut cut = from.min(data.len());
    while cut < data.len() && (data[cut] & 0xc0) == 0x80 {
        cut += 1;
    }
    cut
}

fn lock(buffer: &Arc<Mutex<Buffer>>) -> MutexGuard<'_, Buffer> {
    buffer
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(unix)]
fn set_process_group(command: &mut Command) {
    command.process_group(0);
}

#[cfg(not(unix))]
fn set_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn merge_stderr(command: &mut Command) {
    unsafe {
        command.pre_exec(|| {
            if libc::dup2(1, 2) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(unix))]
fn merge_stderr(_command: &mut Command) {}

#[cfg(unix)]
fn signal_of(status: &std::process::ExitStatus) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;
    status.signal()
}

#[cfg(not(unix))]
fn signal_of(_status: &std::process::ExitStatus) -> Option<i32> {
    None
}

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
