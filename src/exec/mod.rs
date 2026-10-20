pub mod bash;
pub mod download;
pub mod error;
pub mod files;
pub mod paths;
pub mod transfer;
pub mod upload;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use ciborium::value::Value;
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::config::{ClientConfig, HubConfig};
use crate::proto::value;
use crate::proto::{Msg, ProtoError};
use error::{ToolError, ToolResult};

pub const DEFAULT_TIMEOUT_MS: u64 = 120_000;
pub const MAX_TIMEOUT_MS: u64 = 600_000;
pub const MAX_RESULT_BYTES: usize = 60_000;
const FEED_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_GRACE: Duration = Duration::from_millis(300);

#[derive(Clone)]
pub struct Settings {
    pub roots: Vec<String>,
    pub allow_bash: bool,
    pub max_output: usize,
    pub max_file: u64,
}

impl Settings {
    pub fn from_client(cfg: &ClientConfig) -> Self {
        Self {
            roots: cfg.roots.clone(),
            allow_bash: cfg.allow_bash,
            max_output: cfg.max_output,
            max_file: cfg.max_file,
        }
    }

    pub fn from_hub(cfg: &HubConfig) -> Self {
        Self {
            roots: cfg.roots.clone(),
            allow_bash: cfg.allow_bash,
            max_output: cfg.max_output,
            max_file: cfg.max_file,
        }
    }
}

struct Running {
    token: CancellationToken,
    server_id: u64,
    reply_id: u64,
    chunks: Option<mpsc::Sender<Msg>>,
}

pub struct Executor {
    settings: Settings,
    running: Arc<Mutex<HashMap<u64, Running>>>,
    slots: Arc<Semaphore>,
    shutdown: Option<CancellationToken>,
}

impl Executor {
    pub fn new(settings: Settings) -> Self {
        let slots = std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(4)
            .max(1);
        Self {
            settings,
            running: Arc::new(Mutex::new(HashMap::new())),
            slots: Arc::new(Semaphore::new(slots)),
            shutdown: None,
        }
    }

    pub fn with_shutdown(settings: Settings, shutdown: CancellationToken) -> Self {
        Self {
            shutdown: Some(shutdown),
            ..Self::new(settings)
        }
    }

    pub async fn dispatch(
        &self,
        call_id: u64,
        server_id: u64,
        reply_id: u64,
        method: &str,
        params: Value,
        sink: mpsc::Sender<Msg>,
    ) {
        debug!(call_id, reply_id, method, "执行工具");
        match method {
            "bash" => {
                self.dispatch_bash(call_id, server_id, reply_id, params, sink)
                    .await
            }
            "read" | "write" | "edit" | "list" => {
                self.dispatch_file(reply_id, method, params, sink).await
            }
            "upload" => {
                self.dispatch_upload(call_id, server_id, reply_id, params, sink)
                    .await
            }
            "download" => {
                self.dispatch_download(call_id, server_id, reply_id, params, sink)
                    .await
            }
            "shutdown" => self.dispatch_shutdown(reply_id, sink).await,
            other => {
                reply_error(
                    &sink,
                    reply_id,
                    "unsupported",
                    format!("不支持的方法: {other}"),
                )
                .await
            }
        }
    }

    async fn dispatch_shutdown(&self, reply_id: u64, sink: mpsc::Sender<Msg>) {
        let Some(shutdown) = self.shutdown.clone() else {
            reply_error(&sink, reply_id, "unsupported", "该目标不支持远程关停").await;
            return;
        };
        info!(
            reply_id,
            grace_ms = SHUTDOWN_GRACE.as_millis() as u64,
            "收到关停指令，准备退出"
        );
        reply_value(
            &sink,
            reply_id,
            value::map(vec![("stopping", Value::Bool(true))]),
        )
        .await;
        tokio::spawn(async move {
            tokio::time::sleep(SHUTDOWN_GRACE).await;
            shutdown.cancel();
        });
    }

    async fn dispatch_bash(
        &self,
        call_id: u64,
        server_id: u64,
        reply_id: u64,
        params: Value,
        sink: mpsc::Sender<Msg>,
    ) {
        if !self.settings.allow_bash {
            reply_error(&sink, reply_id, "denied", "该目标已禁用 bash").await;
            return;
        }
        let command = match value::text(&params, "command") {
            Some(command) if !command.trim().is_empty() => command,
            _ => {
                reply_error(&sink, reply_id, "protocol", "缺少 command 参数").await;
                return;
            }
        };
        let cwd = match value::text(&params, "cwd") {
            Some(cwd) => match paths::ensure_allowed(Some(&cwd), &self.settings.roots) {
                Ok(path) => path,
                Err(err) => {
                    reply_error(&sink, reply_id, "denied", format!("{err:#}")).await;
                    return;
                }
            },
            None => match paths::ensure_allowed(None, &self.settings.roots) {
                Ok(path) => path,
                Err(err) => {
                    reply_error(&sink, reply_id, "denied", format!("{err:#}")).await;
                    return;
                }
            },
        };
        let timeout_ms = value::u64_value(&params, "timeout_ms")
            .unwrap_or(DEFAULT_TIMEOUT_MS)
            .min(MAX_TIMEOUT_MS);
        let max_output = value::u64_value(&params, "max_output")
            .map(|value| value as usize)
            .unwrap_or(self.settings.max_output)
            .min(self.settings.max_output);
        let stdin = value::text(&params, "stdin").map(String::into_bytes);

        let permit = match self.slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                reply_error(&sink, reply_id, "busy", "并发执行数已达上限").await;
                return;
            }
        };
        let token = CancellationToken::new();
        self.lock_running().insert(
            call_id,
            Running {
                token: token.clone(),
                server_id,
                reply_id,
                chunks: None,
            },
        );
        let running = self.running.clone();

        tokio::spawn(async move {
            let request = bash::Request {
                command,
                cwd,
                timeout: Duration::from_millis(timeout_ms),
                stdin,
                max_output,
            };
            match bash::run(request, token).await {
                Ok(outcome) => {
                    if let Err(err) = bash::send_output(&sink, reply_id, &outcome).await {
                        debug!(error = %err, "发送输出失败");
                    }
                }
                Err(err) => {
                    let message = format!("执行命令失败: {err:#}");
                    tracing::warn!(error = %message, "执行命令失败");
                    reply_error(&sink, reply_id, "io", message).await;
                }
            }
            remove_running(&running, call_id);
            drop(permit);
        });
    }

    async fn dispatch_file(
        &self,
        reply_id: u64,
        method: &str,
        params: Value,
        sink: mpsc::Sender<Msg>,
    ) {
        let permit = match self.slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                reply_error(&sink, reply_id, "busy", "并发执行数已达上限").await;
                return;
            }
        };
        let roots = self.settings.roots.clone();
        let max_file = self.settings.max_file;
        let method = method.to_string();
        let joined =
            tokio::task::spawn_blocking(move || run_file(&method, &roots, max_file, &params)).await;
        drop(permit);
        match joined {
            Ok(Ok(report)) => reply_value(&sink, reply_id, report).await,
            Ok(Err(err)) => reply_error(&sink, reply_id, err.code, err.message).await,
            Err(err) => {
                reply_error(
                    &sink,
                    reply_id,
                    "internal",
                    format!("文件操作任务失败: {err}"),
                )
                .await;
            }
        }
    }

    async fn dispatch_upload(
        &self,
        call_id: u64,
        server_id: u64,
        reply_id: u64,
        params: Value,
        sink: mpsc::Sender<Msg>,
    ) {
        let remote_path = match require_text(&params, "remote_path") {
            Ok(path) => path,
            Err(err) => {
                reply_error(&sink, reply_id, err.code, err.message).await;
                return;
            }
        };
        let overwrite = value::bool_value(&params, "overwrite").unwrap_or(false);
        let create_parents = value::bool_value(&params, "create_parents").unwrap_or(false);
        let expected_size = value::u64_value(&params, "size");
        if let Some(size) = expected_size {
            if size > self.settings.max_file {
                reply_error(
                    &sink,
                    reply_id,
                    "too_large",
                    format!(
                        "文件声明 {size} 字节，超过上限 {} 字节",
                        self.settings.max_file
                    ),
                )
                .await;
                return;
            }
        }
        let path =
            match paths::resolve_for_write(&remote_path, &self.settings.roots, create_parents) {
                Ok(path) => path,
                Err(err) => {
                    reply_error(&sink, reply_id, err.code, err.message).await;
                    return;
                }
            };
        match std::fs::symlink_metadata(&path) {
            Ok(meta) => {
                if meta.is_dir() {
                    reply_error(
                        &sink,
                        reply_id,
                        "io",
                        format!("目标是目录: {}", path.display()),
                    )
                    .await;
                    return;
                }
                if !overwrite {
                    reply_error(
                        &sink,
                        reply_id,
                        "conflict",
                        format!("目标已存在: {}", path.display()),
                    )
                    .await;
                    return;
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                reply_error(
                    &sink,
                    reply_id,
                    "io",
                    format!("检查 {} 失败: {err}", path.display()),
                )
                .await;
                return;
            }
        }
        let permit = match self.slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                reply_error(&sink, reply_id, "busy", "并发执行数已达上限").await;
                return;
            }
        };
        let token = CancellationToken::new();
        let (chunk_tx, chunk_rx) = mpsc::channel(8);
        self.lock_running().insert(
            call_id,
            Running {
                token: token.clone(),
                server_id,
                reply_id,
                chunks: Some(chunk_tx),
            },
        );
        let running = self.running.clone();
        let max_file = self.settings.max_file;
        tokio::spawn(async move {
            upload::run(upload::Request {
                path,
                overwrite,
                expected_size,
                max_file,
                reply_id,
                sink,
                chunks: chunk_rx,
                cancel: token,
            })
            .await;
            remove_running(&running, call_id);
            drop(permit);
        });
    }

    async fn dispatch_download(
        &self,
        call_id: u64,
        server_id: u64,
        reply_id: u64,
        params: Value,
        sink: mpsc::Sender<Msg>,
    ) {
        let remote_path = match require_text(&params, "remote_path") {
            Ok(path) => path,
            Err(err) => {
                reply_error(&sink, reply_id, err.code, err.message).await;
                return;
            }
        };
        let max_file = value::u64_value(&params, "max_size")
            .unwrap_or(self.settings.max_file)
            .min(self.settings.max_file);
        let path = match paths::resolve_existing(&remote_path, &self.settings.roots) {
            Ok(path) => path,
            Err(err) => {
                reply_error(&sink, reply_id, err.code, err.message).await;
                return;
            }
        };
        let permit = match self.slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                reply_error(&sink, reply_id, "busy", "并发执行数已达上限").await;
                return;
            }
        };
        let token = CancellationToken::new();
        self.lock_running().insert(
            call_id,
            Running {
                token: token.clone(),
                server_id,
                reply_id,
                chunks: None,
            },
        );
        let running = self.running.clone();
        tokio::spawn(async move {
            download::run(download::Request {
                path,
                max_file,
                reply_id,
                sink,
                cancel: token,
            })
            .await;
            remove_running(&running, call_id);
            drop(permit);
        });
    }

    pub async fn feed(&self, msg: Msg) -> bool {
        let Some(id) = msg.id() else {
            return false;
        };
        let sender = {
            let running = self.lock_running();
            running.get(&id).and_then(|entry| entry.chunks.clone())
        };
        let Some(sender) = sender else {
            return false;
        };
        match deliver(sender, msg).await {
            Delivery::Delivered => true,
            Delivery::Closed => false,
            Delivery::Stalled => {
                debug!(id, "上传分块处理超时，取消该调用");
                self.cancel(id);
                false
            }
        }
    }

    pub async fn feed_from_server(&self, server_id: u64, msg: Msg) -> bool {
        let Some(id) = msg.id() else {
            return false;
        };
        let sender = {
            let running = self.lock_running();
            running
                .values()
                .find(|entry| {
                    entry.server_id == server_id && entry.reply_id == id && entry.chunks.is_some()
                })
                .and_then(|entry| entry.chunks.clone())
        };
        let Some(sender) = sender else {
            return false;
        };
        match deliver(sender, msg).await {
            Delivery::Delivered => true,
            Delivery::Closed => false,
            Delivery::Stalled => {
                debug!(server_id, id, "上传分块处理超时，取消该调用");
                self.cancel_by_reply(server_id, id);
                false
            }
        }
    }

    pub fn cancel(&self, call_id: u64) {
        let running = self.lock_running();
        if let Some(entry) = running.get(&call_id) {
            entry.token.cancel();
        }
    }

    pub fn cancel_by_reply(&self, server_id: u64, reply_id: u64) {
        let running = self.lock_running();
        for entry in running.values() {
            if entry.server_id == server_id && entry.reply_id == reply_id {
                entry.token.cancel();
            }
        }
    }

    pub fn cancel_by_server(&self, server_id: u64) {
        let running = self.lock_running();
        for entry in running.values() {
            if entry.server_id == server_id {
                entry.token.cancel();
            }
        }
    }

    pub fn cancel_all(&self) {
        let running = self.lock_running();
        for entry in running.values() {
            entry.token.cancel();
        }
    }

    fn lock_running(&self) -> MutexGuard<'_, HashMap<u64, Running>> {
        self.running
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn run_file(method: &str, roots: &[String], max_file: u64, params: &Value) -> ToolResult<Value> {
    match method {
        "read" => {
            let path = require_text(params, "path")?;
            let offset = value::u64_value(params, "offset").unwrap_or(0);
            let limit = value::u64_value(params, "limit").unwrap_or(files::DEFAULT_READ_LINES);
            serialize(&files::read(roots, &path, offset, limit)?)
        }
        "write" => {
            let path = require_text(params, "path")?;
            let content = require_text(params, "content")?;
            let create_parents = value::bool_value(params, "create_parents").unwrap_or(false);
            serialize(&files::write(
                roots,
                &path,
                &content,
                create_parents,
                max_file,
            )?)
        }
        "edit" => {
            let path = require_text(params, "path")?;
            let old_string = require_text(params, "old_string")?;
            let new_string = require_text(params, "new_string")?;
            if old_string.len() + new_string.len() > files::MAX_WRITE_BYTES {
                return Err(ToolError::too_large(format!(
                    "old_string 与 new_string 合计超过 {} 字节",
                    files::MAX_WRITE_BYTES
                )));
            }
            let replace_all = value::bool_value(params, "replace_all").unwrap_or(false);
            serialize(&files::edit(
                roots,
                &path,
                &old_string,
                &new_string,
                replace_all,
                max_file,
            )?)
        }
        "list" => {
            let path = require_text(params, "path")?;
            let depth = value::u64_value(params, "depth").unwrap_or(1);
            serialize(&files::list(roots, &path, depth)?)
        }
        other => Err(ToolError::new(
            "unsupported",
            format!("不支持的文件方法: {other}"),
        )),
    }
}

fn require_text(params: &Value, key: &str) -> ToolResult<String> {
    match value::text(params, key) {
        Some(text) if !text.trim().is_empty() => Ok(text),
        _ => Err(ToolError::protocol(format!("缺少 {key} 参数"))),
    }
}

fn serialize<T: serde::Serialize>(value: &T) -> ToolResult<Value> {
    value::serialize(value).map_err(|err| ToolError::internal(format!("序列化结果失败: {err:#}")))
}

fn remove_running(running: &Arc<Mutex<HashMap<u64, Running>>>, call_id: u64) {
    let mut running = running
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    running.remove(&call_id);
}

enum Delivery {
    Delivered,
    Closed,
    Stalled,
}

async fn deliver(sender: mpsc::Sender<Msg>, msg: Msg) -> Delivery {
    match tokio::time::timeout(FEED_TIMEOUT, sender.send(msg)).await {
        Ok(Ok(())) => Delivery::Delivered,
        Ok(Err(_)) => Delivery::Closed,
        Err(_) => Delivery::Stalled,
    }
}

pub(crate) async fn reply_value(sink: &mpsc::Sender<Msg>, reply_id: u64, value: Value) {
    let msg = Msg::Result {
        id: reply_id,
        ok: true,
        value: Some(value),
        error: None,
    };
    match crate::proto::frame::encode(&msg) {
        Ok(bytes) if bytes.len() <= MAX_RESULT_BYTES => {
            if sink.send(msg).await.is_err() {
                tracing::debug!("发送结果失败");
            }
        }
        Ok(bytes) => {
            reply_error(
                sink,
                reply_id,
                "too_large",
                format!(
                    "返回值 {} 字节超过单条结果上限 {MAX_RESULT_BYTES} 字节，请缩小范围",
                    bytes.len()
                ),
            )
            .await;
        }
        Err(err) => {
            reply_error(sink, reply_id, "internal", format!("编码结果失败: {err:#}")).await;
        }
    }
}

pub(crate) async fn reply_transfer(
    sink: &mpsc::Sender<Msg>,
    reply_id: u64,
    report: &transfer::TransferReport,
) {
    match value::serialize(report) {
        Ok(value) => reply_value(sink, reply_id, value).await,
        Err(err) => {
            reply_error(
                sink,
                reply_id,
                "internal",
                format!("序列化传输结果失败: {err:#}"),
            )
            .await;
        }
    }
}

async fn reply_error(
    sink: &mpsc::Sender<Msg>,
    reply_id: u64,
    code: &str,
    message: impl Into<String>,
) {
    let _ = sink
        .send(Msg::Result {
            id: reply_id,
            ok: false,
            value: None,
            error: Some(ProtoError::new(code, message)),
        })
        .await;
}
