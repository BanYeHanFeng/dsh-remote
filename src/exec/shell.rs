use std::io::ErrorKind;
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::exec::error::{ToolError, ToolResult};

const POLL_INTERVAL: Duration = Duration::from_millis(25);
const SCROLLBACK_BYTES: usize = 1024 * 1024;
const READ_CHUNK: usize = 16 * 1024;
const TEXT_BYTES_LIMIT: usize = 48 * 1024;

const TRUNCATED: &str =
    "<输出已截断> 为节省上下文只返回了一部分输出。建议先用 grep -n 定位行号再重试。";
const LOST_PREFIX: &str = "<输出已截断> 命令输出的开头已被回滚上限丢弃，下面是仍保留的最早内容。\n";
const TIMEOUT_PREFIX: &str = "命令超过 {seconds} 秒仍未结束。下面是已产生的部分输出：";
const TIMEOUT_MARKER: &str = "[命令超时]";
const RESET: &str =
    "持久 Shell 已重置；下一次调用会从工作目录重新开始（当前目录与环境变量都是新的）。";

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

#[derive(Default)]
pub struct Shells {
    state: AsyncMutex<Option<Shell>>,
}

impl Shells {
    pub async fn run(&self, request: Request, cancel: &CancellationToken) -> ToolResult<Report> {
        let command = request.command.trim_end().to_string();
        if command.trim().is_empty() {
            return Err(ToolError::protocol("command 必须是非空字符串"));
        }
        let mut guard = tokio::select! {
            _ = cancel.cancelled() => return Err(ToolError::new("cancelled", "命令已取消")),
            guard = self.state.lock() => guard,
        };
        if cancel.is_cancelled() {
            return Err(ToolError::new("cancelled", "命令已取消"));
        }
        if guard.is_none() {
            *guard = Some(Shell::spawn(&request.cwd)?);
        }
        let marker = Marker::new();
        let write = {
            let shell = guard.as_mut().expect("刚创建的 Shell 不见了");
            shell.send(&marker.wrap(&command)).await
        };
        if let Err(err) = write {
            reset(&mut guard);
            return Err(ToolError::io(format!("写入持久 Shell 失败: {err}")));
        }

        let deadline = Instant::now() + request.timeout;
        loop {
            if let Some(captured) = capture(&guard, &marker) {
                return Ok(captured.report(request.max_output_chars, None, false));
            }
            let exited = guard.as_mut().and_then(Shell::exited);
            if let Some(code) = exited {
                let partial = capture_partial(&guard, &marker);
                reset(&mut guard);
                let note = exit_note(code);
                return Ok(partial.report(request.max_output_chars, Some(&note), false));
            }
            if Instant::now() >= deadline {
                let partial = capture_partial(&guard, &marker);
                reset(&mut guard);
                let note = timeout_note(request.timeout);
                return Ok(partial.report(request.max_output_chars, Some(&note), true));
            }
            tokio::select! {
                _ = cancel.cancelled() => {
                    reset(&mut guard);
                    return Err(ToolError::new("cancelled", "命令已取消"));
                }
                _ = tokio::time::sleep(POLL_INTERVAL) => {}
            }
        }
    }

    pub async fn shutdown(&self) {
        let mut guard = self.state.lock().await;
        reset(&mut guard);
    }
}

fn reset(guard: &mut Option<Shell>) {
    if let Some(shell) = guard.take() {
        shell.kill();
    }
}

fn exit_note(code: i32) -> String {
    format!("[Shell 已退出，退出码 {code}]\n{RESET}")
}

fn timeout_note(timeout: Duration) -> String {
    let seconds = timeout.as_secs().max(1);
    let head = TIMEOUT_PREFIX.replace("{seconds}", &seconds.to_string());
    format!("{head}\n{TIMEOUT_MARKER}\n{RESET}")
}

struct Marker {
    start: String,
    end: String,
}

impl Marker {
    fn new() -> Self {
        let nonce = format!(
            "{:016x}{:016x}",
            rand::random::<u64>(),
            rand::random::<u64>()
        );
        Self {
            start: format!("__DSH_SHELL_START_{nonce}__"),
            end: format!("__DSH_SHELL_END_{nonce}:"),
        }
    }

    fn wrap(&self, command: &str) -> String {
        format!(
            "printf '%s\\n' {start}\neval {command}\n__dsh_shell_status=$?\nprintf '%s%s\\n' {end} \"$__dsh_shell_status\"\n",
            start = quote(&self.start),
            command = quote(command),
            end = quote(&self.end),
        )
    }
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

struct Captured {
    text: String,
    lost: bool,
    exit_code: Option<i32>,
}

impl Captured {
    fn report(&self, max_output_chars: usize, note: Option<&str>, timed_out: bool) -> Report {
        Report {
            output: render(self, max_output_chars, note),
            exit_code: self.exit_code,
            timed_out,
        }
    }
}

fn render(captured: &Captured, max_output_chars: usize, note: Option<&str>) -> String {
    let mut body = truncate_chars(&captured.text, max_output_chars);
    if captured.lost && !body.is_empty() {
        body = format!("{LOST_PREFIX}{body}");
    }
    let status = captured
        .exit_code
        .map(|code| format!("[命令结束，退出码 {code}]"));
    let tail = [status, note.map(str::to_string)]
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

fn capture(guard: &Option<Shell>, marker: &Marker) -> Option<Captured> {
    let shell = guard.as_ref()?;
    let buffer = shell.buffer();
    let data = &buffer.data;
    let end = rfind(data, marker.end.as_bytes())?;
    let after = &data[end + marker.end.len()..];
    let digits = after
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if digits == 0 || after.get(digits) != Some(&b'\n') {
        return None;
    }
    let exit_code = std::str::from_utf8(&after[..digits])
        .ok()
        .and_then(|text| text.parse::<i32>().ok());
    Some(match rfind(&data[..end], marker.start.as_bytes()) {
        Some(start) => Captured {
            text: decode(&data[start + marker.start.len()..end]),
            lost: buffer.lost,
            exit_code,
        },
        None => Captured {
            text: decode(&data[..end]),
            lost: true,
            exit_code,
        },
    })
}

fn capture_partial(guard: &Option<Shell>, marker: &Marker) -> Captured {
    let Some(shell) = guard.as_ref() else {
        return Captured {
            text: String::new(),
            lost: true,
            exit_code: None,
        };
    };
    let buffer = shell.buffer();
    let data = &buffer.data;
    match rfind(data, marker.start.as_bytes()) {
        Some(start) => Captured {
            text: decode(&data[start + marker.start.len()..]),
            lost: buffer.lost,
            exit_code: None,
        },
        None => Captured {
            text: decode(data),
            lost: true,
            exit_code: None,
        },
    }
}

fn decode(raw: &[u8]) -> String {
    let raw = raw
        .strip_prefix(b"\r\n")
        .or_else(|| raw.strip_prefix(b"\n"))
        .unwrap_or(raw);
    let text = String::from_utf8_lossy(raw);
    text.trim_end_matches(['\r', '\n']).to_string()
}

fn rfind(data: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || data.len() < needle.len() {
        return None;
    }
    data.windows(needle.len())
        .rposition(|window| window == needle)
}

struct Shell {
    child: Child,
    stdin: ChildStdin,
    pid: i32,
    buffer: Arc<Mutex<Buffer>>,
    eof: Arc<AtomicBool>,
}

struct Buffer {
    data: Vec<u8>,
    lost: bool,
}

impl Shell {
    fn spawn(cwd: &str) -> ToolResult<Self> {
        match Self::spawn_with("bash", cwd) {
            Ok(shell) => Ok(shell),
            Err(err) if err.kind() == ErrorKind::NotFound => {
                Self::spawn_with("sh", cwd).map_err(|err| {
                    ToolError::io(format!("启动持久 Shell 失败（bash 与 sh 都不可用）: {err}"))
                })
            }
            Err(err) => Err(ToolError::io(format!("启动持久 Shell 失败: {err}"))),
        }
    }

    fn spawn_with(program: &str, cwd: &str) -> std::io::Result<Self> {
        let mut command = Command::new(program);
        command.current_dir(Path::new(cwd));
        command.stdin(Stdio::piped());
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());
        command.kill_on_drop(true);
        set_process_group(&mut command);
        merge_stderr(&mut command);
        let mut child = command.spawn()?;
        let pid = child.id().unwrap_or_default() as i32;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| std::io::Error::other("持久 Shell 没有 stdin"))?;
        let buffer = Arc::new(Mutex::new(Buffer {
            data: Vec::new(),
            lost: false,
        }));
        let eof = Arc::new(AtomicBool::new(false));
        if let Some(mut stdout) = child.stdout.take() {
            let target = buffer.clone();
            let eof = eof.clone();
            tokio::spawn(async move {
                let mut chunk = vec![0u8; READ_CHUNK];
                loop {
                    match stdout.read(&mut chunk).await {
                        Ok(0) | Err(_) => break,
                        Ok(read) => push(&target, &chunk[..read]),
                    }
                }
                eof.store(true, Ordering::Relaxed);
            });
        }
        if let Some(mut stderr) = child.stderr.take() {
            let target = buffer.clone();
            tokio::spawn(async move {
                let mut chunk = vec![0u8; READ_CHUNK];
                while let Ok(read) = stderr.read(&mut chunk).await {
                    if read == 0 {
                        break;
                    }
                    push(&target, &chunk[..read]);
                }
            });
        }
        Ok(Self {
            child,
            stdin,
            pid,
            buffer,
            eof,
        })
    }

    async fn send(&mut self, text: &str) -> std::io::Result<()> {
        {
            let mut buffer = lock(&self.buffer);
            buffer.data.clear();
            buffer.lost = false;
        }
        self.stdin.write_all(text.as_bytes()).await?;
        self.stdin.flush().await
    }

    fn buffer(&self) -> MutexGuard<'_, Buffer> {
        lock(&self.buffer)
    }

    fn exited(&mut self) -> Option<i32> {
        match self.child.try_wait() {
            Ok(Some(status)) => Some(status.code().unwrap_or(-1)),
            Ok(None) => {
                if self.eof.load(Ordering::Relaxed) {
                    Some(-1)
                } else {
                    None
                }
            }
            Err(_) => Some(-1),
        }
    }

    fn kill(&self) {
        kill_group(self.pid);
    }
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
fn kill_group(pid: i32) {
    if pid > 0 {
        unsafe {
            libc::kill(-pid, libc::SIGKILL);
        }
    }
}

#[cfg(not(unix))]
fn kill_group(_pid: i32) {}
