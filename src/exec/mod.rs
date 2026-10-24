pub mod bash;
pub mod download;
pub mod error;
pub mod limits;
pub mod paths;
pub mod transfer;
pub mod upload;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use ciborium::value::Value;
use serde::de::DeserializeOwned;
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::net::Sink;
use crate::proto::params::{BashParams, DownloadParams, UploadParams};
use crate::proto::value;
use crate::proto::{Msg, ProtoError};
use error::{ToolError, ToolResult};

pub const MAX_RESULT_BYTES: usize = 60_000;
const FEED_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_GRACE: Duration = Duration::from_millis(300);

#[derive(Clone, Debug)]
pub struct Settings {
    pub roots: Vec<String>,
    pub allow_bash: bool,
    pub max_output: usize,
    pub max_file: u64,
}

struct Running {
    token: CancellationToken,
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

    pub async fn dispatch(&self, call_id: u64, method: &str, params: Value, sink: Sink) {
        debug!(call_id, method, "执行工具");
        match method {
            "bash" => self.dispatch_bash(call_id, params, sink).await,
            "upload" => self.dispatch_upload(call_id, params, sink).await,
            "download" => self.dispatch_download(call_id, params, sink).await,
            "shutdown" => self.dispatch_shutdown(call_id, sink).await,
            other => {
                reply_error(
                    &sink,
                    call_id,
                    "unsupported",
                    format!("不支持的方法: {other}"),
                )
                .await
            }
        }
    }

    async fn dispatch_shutdown(&self, reply_id: u64, sink: Sink) {
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

    async fn dispatch_bash(&self, call_id: u64, params: Value, sink: Sink) {
        if !self.settings.allow_bash {
            reply_error(&sink, call_id, "denied", "该目标已禁用 bash").await;
            return;
        }
        let params: BashParams = match parse(&params) {
            Ok(params) => params,
            Err(err) => {
                reply_error(&sink, call_id, err.code, err.message).await;
                return;
            }
        };
        if params.command.trim().is_empty() {
            reply_error(&sink, call_id, "protocol", "缺少 command 参数").await;
            return;
        }
        let cwd = match paths::ensure_allowed(params.cwd.as_deref(), &self.settings.roots) {
            Ok(path) => path,
            Err(err) => {
                reply_error(&sink, call_id, "denied", format!("{err:#}")).await;
                return;
            }
        };
        let timeout = limits::bash_timeout_ms(params.timeout_ms);
        let max_output = params
            .max_output
            .map(|value| value as usize)
            .unwrap_or(self.settings.max_output)
            .min(self.settings.max_output);
        let stdin = params.stdin.map(String::into_bytes);

        let permit = match self.slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                reply_error(&sink, call_id, "busy", "并发执行数已达上限").await;
                return;
            }
        };
        let token = CancellationToken::new();
        self.track(call_id, token.clone(), None);
        let running = self.running.clone();

        tokio::spawn(async move {
            let request = bash::Request {
                command: params.command,
                cwd,
                timeout,
                stdin,
                max_output,
            };
            match bash::run(request, token).await {
                Ok(outcome) => {
                    if let Err(err) = bash::send_output(&sink, call_id, &outcome).await {
                        debug!(error = %err, "发送输出失败");
                    }
                }
                Err(err) => {
                    let message = format!("执行命令失败: {err:#}");
                    warn!(error = %message, "执行命令失败");
                    reply_error(&sink, call_id, "io", message).await;
                }
            }
            remove_running(&running, call_id);
            drop(permit);
        });
    }

    async fn dispatch_upload(&self, call_id: u64, params: Value, sink: Sink) {
        let params: UploadParams = match parse(&params) {
            Ok(params) => params,
            Err(err) => {
                reply_error(&sink, call_id, err.code, err.message).await;
                return;
            }
        };
        let path = match paths::resolve_for_write(
            &params.remote_path,
            &self.settings.roots,
            params.create_parents,
        ) {
            Ok(path) => path,
            Err(err) => {
                reply_error(&sink, call_id, err.code, err.message).await;
                return;
            }
        };
        let permit = match self.slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                reply_error(&sink, call_id, "busy", "并发执行数已达上限").await;
                return;
            }
        };
        let token = CancellationToken::new();
        let (chunk_tx, chunk_rx) = mpsc::channel(8);
        self.track(call_id, token.clone(), Some(chunk_tx));
        let running = self.running.clone();
        let max_file = self.settings.max_file;

        tokio::spawn(async move {
            upload::run(upload::Request {
                path,
                overwrite: params.overwrite,
                expected_size: params.size,
                max_file,
                reply_id: call_id,
                sink,
                chunks: chunk_rx,
                cancel: token,
            })
            .await;
            remove_running(&running, call_id);
            drop(permit);
        });
    }

    async fn dispatch_download(&self, call_id: u64, params: Value, sink: Sink) {
        let params: DownloadParams = match parse(&params) {
            Ok(params) => params,
            Err(err) => {
                reply_error(&sink, call_id, err.code, err.message).await;
                return;
            }
        };
        let max_file = params
            .max_size
            .unwrap_or(self.settings.max_file)
            .min(self.settings.max_file);
        let path = match paths::resolve_existing(&params.remote_path, &self.settings.roots) {
            Ok(path) => path,
            Err(err) => {
                reply_error(&sink, call_id, err.code, err.message).await;
                return;
            }
        };
        let permit = match self.slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                reply_error(&sink, call_id, "busy", "并发执行数已达上限").await;
                return;
            }
        };
        let token = CancellationToken::new();
        self.track(call_id, token.clone(), None);
        let running = self.running.clone();

        tokio::spawn(async move {
            download::run(download::Request {
                path,
                max_file,
                reply_id: call_id,
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

    pub fn cancel(&self, call_id: u64) {
        let running = self.lock_running();
        if let Some(entry) = running.get(&call_id) {
            entry.token.cancel();
        }
    }

    pub fn cancel_all(&self) {
        let running = self.lock_running();
        for entry in running.values() {
            entry.token.cancel();
        }
    }

    fn track(&self, call_id: u64, token: CancellationToken, chunks: Option<mpsc::Sender<Msg>>) {
        self.lock_running()
            .insert(call_id, Running { token, chunks });
    }

    fn lock_running(&self) -> MutexGuard<'_, HashMap<u64, Running>> {
        self.running
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn parse<T: DeserializeOwned>(params: &Value) -> ToolResult<T> {
    value::deserialize(params).map_err(|err| ToolError::protocol(format!("参数不合法: {err:#}")))
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

pub(crate) async fn reply_value(sink: &Sink, reply_id: u64, value: Value) {
    let msg = Msg::Result {
        id: reply_id,
        ok: true,
        value: Some(value),
        error: None,
    };
    match crate::proto::frame::encode(&msg) {
        Ok(bytes) if bytes.len() <= MAX_RESULT_BYTES => {
            if sink.send(msg).await.is_err() {
                debug!("发送结果失败");
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

pub(crate) async fn reply_transfer(sink: &Sink, reply_id: u64, report: &transfer::TransferReport) {
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

pub(crate) async fn reply_error(
    sink: &Sink,
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
