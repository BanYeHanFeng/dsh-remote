use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use tokio::sync::{mpsc, watch};
use tokio::time::{interval, timeout, Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::{ClientConfig, GithubConfig};
use crate::key::KeyMaterial;
use crate::net::backoff::Backoff;
use crate::net::{spawn_io, IoHandles, SecureConn};
use crate::proto::{caps, EndStatus, Msg, Role, TargetEventKind, TargetInfo, PROTO_VERSION};

const HELLO_TIMEOUT: Duration = Duration::from_secs(5);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const IDLE_TIMEOUT: Duration = Duration::from_secs(45);
const REPLY_QUEUE: usize = 64;
const ROUTE_TIMEOUT: Duration = Duration::from_secs(30);
const INITIAL_CONNECT_GRACE: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub struct RemoteError {
    pub code: String,
    pub message: String,
}

impl RemoteError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for RemoteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for RemoteError {}

pub type RemoteResult<T> = std::result::Result<T, RemoteError>;

pub struct CallResult {
    pub ok: bool,
    pub exit_code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub truncated: bool,
    pub size: Option<u64>,
    pub sha256: Option<String>,
    pub stopped: Option<EndStatus>,
    pub error: Option<crate::proto::ProtoError>,
    pub value: Option<ciborium::value::Value>,
}

impl CallResult {
    fn empty() -> Self {
        Self {
            ok: false,
            exit_code: None,
            stdout: Vec::new(),
            stderr: Vec::new(),
            truncated: false,
            size: None,
            sha256: None,
            stopped: None,
            error: None,
            value: None,
        }
    }

    pub fn into_value(self) -> RemoteResult<ciborium::value::Value> {
        if self.ok {
            self.value
                .ok_or_else(|| RemoteError::new("protocol", "响应缺少 value"))
        } else {
            match self.error {
                Some(error) => Err(RemoteError::new(error.code, error.message)),
                None => Err(RemoteError::new("protocol", "远端返回了失败但没有错误信息")),
            }
        }
    }
}

#[derive(Clone)]
pub struct Session {
    inner: Arc<Inner>,
}

struct Inner {
    hub: String,
    key: KeyMaterial,
    name: String,
    roots: Vec<String>,
    allow_bash: bool,
    max_file: u64,
    github: GithubConfig,
    next_id: AtomicU64,
    state: Mutex<State>,
    targets: RwLock<Vec<TargetInfo>>,
    connected: watch::Sender<bool>,
}

struct State {
    conn: Option<Conn>,
    hub_name: Option<String>,
    next_serial: u64,
}

struct Conn {
    serial: u64,
    outbound: mpsc::Sender<Msg>,
    pending: HashMap<u64, mpsc::Sender<Msg>>,
}

impl Session {
    pub fn new(cfg: &ClientConfig) -> Self {
        let (connected, _) = watch::channel(false);
        Self {
            inner: Arc::new(Inner {
                hub: cfg.hub.clone(),
                key: cfg.key.clone(),
                name: cfg.name.clone(),
                roots: cfg.roots.clone(),
                allow_bash: cfg.allow_bash,
                max_file: cfg.max_file,
                github: cfg.github.clone(),
                next_id: AtomicU64::new(1),
                state: Mutex::new(State {
                    conn: None,
                    hub_name: None,
                    next_serial: 0,
                }),
                targets: RwLock::new(Vec::new()),
                connected,
            }),
        }
    }

    pub fn hub(&self) -> &str {
        &self.inner.hub
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn max_file(&self) -> u64 {
        self.inner.max_file
    }

    pub fn key(&self) -> &KeyMaterial {
        &self.inner.key
    }

    pub fn github(&self) -> &GithubConfig {
        &self.inner.github
    }

    pub fn is_connected(&self) -> bool {
        self.lock_state().conn.is_some()
    }

    pub async fn wait_connected(&self, limit: Duration) -> bool {
        if self.is_connected() {
            return true;
        }
        let mut rx = self.inner.connected.subscribe();
        let _ = timeout(limit, async {
            loop {
                if *rx.borrow_and_update() {
                    return;
                }
                if rx.changed().await.is_err() {
                    return;
                }
            }
        })
        .await;
        self.is_connected()
    }

    pub async fn ready(&self) {
        let first_attempt = {
            let state = self.lock_state();
            state.next_serial == 0
        };
        if first_attempt && !self.is_connected() {
            let _ = self.wait_connected(INITIAL_CONNECT_GRACE).await;
        }
    }

    pub fn hub_name(&self) -> Option<String> {
        self.lock_state().hub_name.clone()
    }

    pub fn targets_snapshot(&self) -> Vec<TargetInfo> {
        self.inner
            .targets
            .read()
            .map(|targets| targets.clone())
            .unwrap_or_default()
    }

    pub async fn connect_now(&self, shutdown: CancellationToken) -> Result<()> {
        let (inbound, outbound, serial) = self.open(&shutdown).await?;
        let session = self.clone();
        tokio::spawn(async move {
            session.serve(inbound, outbound, serial, shutdown).await;
        });
        Ok(())
    }

    pub async fn run(&self, shutdown: CancellationToken) {
        let mut backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(30));
        loop {
            if shutdown.is_cancelled() {
                break;
            }
            match self.open(&shutdown).await {
                Ok((inbound, outbound, serial)) => {
                    self.serve(inbound, outbound, serial, shutdown.clone())
                        .await;
                }
                Err(err) => warn!(error = %format!("{err:#}"), "连接中继失败"),
            }
            if shutdown.is_cancelled() {
                break;
            }
            let delay = backoff.next_delay();
            info!(seconds = delay.as_secs_f32(), "等待重连");
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(delay) => {}
            }
        }
        info!("中继连接已停止");
    }

    async fn open(
        &self,
        shutdown: &CancellationToken,
    ) -> Result<(mpsc::Receiver<Msg>, mpsc::Sender<Msg>, u64)> {
        let mut conn = SecureConn::connect(&self.inner.hub, &self.inner.key).await?;
        let hello = Msg::Hello {
            proto: PROTO_VERSION,
            role: Role::Server,
            name: self.inner.name.clone(),
            version: crate::VERSION.to_string(),
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            caps: caps(self.inner.allow_bash),
            roots: self.inner.roots.clone(),
        };
        conn.send(&hello).await?;
        let welcome = timeout(HELLO_TIMEOUT, conn.recv())
            .await
            .map_err(|_| anyhow!("等待 Welcome 超时"))??;
        let (hub_name, targets) = match welcome {
            Msg::Welcome {
                proto,
                hub_name,
                targets,
                server_online,
                ..
            } => {
                if proto != PROTO_VERSION {
                    bail!("中继协议版本不匹配: {proto}");
                }
                info!(
                    hub = %hub_name,
                    targets = %target_names(&targets),
                    server_online,
                    "已连接中继"
                );
                (hub_name, targets)
            }
            Msg::Bye { code, message } => bail!("中继拒绝连接: {code} {message}"),
            other => bail!("意外的首条响应: {other:?}"),
        };
        if shutdown.is_cancelled() {
            bail!("连接途中收到退出信号");
        }

        let IoHandles {
            inbound,
            outbound,
            reader,
            writer,
        } = spawn_io(conn);
        let serial = self.install(outbound.clone(), hub_name, targets);
        tokio::spawn(async move {
            let _ = writer.await;
            reader.abort();
        });
        Ok((inbound, outbound, serial))
    }

    async fn serve(
        &self,
        mut inbound: mpsc::Receiver<Msg>,
        outbound: mpsc::Sender<Msg>,
        serial: u64,
        shutdown: CancellationToken,
    ) {
        let mut ticker = interval(HEARTBEAT_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut last_rx = Instant::now();

        let outcome: Result<()> = loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    let _ = outbound.try_send(Msg::Bye {
                        code: "shutdown".into(),
                        message: "服务端退出".into(),
                    });
                    break Ok(());
                }
                incoming = inbound.recv() => match incoming {
                    Some(msg) => {
                        last_rx = Instant::now();
                        if let Err(err) = self.handle(serial, &outbound, msg).await {
                            break Err(err);
                        }
                    }
                    None => break Err(anyhow!("中继连接断开")),
                },
                _ = ticker.tick() => {
                    if last_rx.elapsed() > IDLE_TIMEOUT {
                        break Err(anyhow!("中继心跳超时"));
                    }
                    if outbound.try_send(Msg::Ping { nonce: rand::random() }).is_err() {
                        break Err(anyhow!("发送心跳失败"));
                    }
                }
            }
        };

        self.teardown(serial);
        match outcome {
            Ok(()) => info!("中继连接已关闭"),
            Err(err) => warn!(error = %format!("{err:#}"), "中继连接中断"),
        }
    }

    async fn handle(&self, serial: u64, outbound: &mpsc::Sender<Msg>, msg: Msg) -> Result<()> {
        match msg {
            Msg::Ping { nonce } => {
                let _ = outbound.try_send(Msg::Pong { nonce });
            }
            Msg::Pong { .. } => {}
            Msg::Bye { code, message } => bail!("中继关闭连接: {code} {message}"),
            Msg::TargetsSnapshot { targets } => {
                self.replace_targets(serial, targets);
            }
            Msg::TargetEvent { kind, target } => {
                self.apply_event(serial, kind, target);
            }
            Msg::Result { id, .. } | Msg::Chunk { id, .. } | Msg::End { id, .. } => {
                self.route(serial, outbound, id, msg).await;
            }
            other => debug!(msg = ?other, "忽略消息"),
        }
        Ok(())
    }

    async fn route(&self, serial: u64, outbound: &mpsc::Sender<Msg>, id: u64, msg: Msg) {
        let sender = {
            let state = self.lock_state();
            state
                .conn
                .as_ref()
                .filter(|conn| conn.serial == serial)
                .and_then(|conn| conn.pending.get(&id).cloned())
        };
        let Some(sender) = sender else {
            debug!(id, "丢弃没有归属的回复");
            return;
        };
        match timeout(ROUTE_TIMEOUT, sender.send(msg)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                debug!(id, "调用方已放弃，通知中继取消");
                self.unregister(serial, id);
                let _ = outbound.try_send(Msg::Cancel {
                    id,
                    reason: Some("client gone".into()),
                });
            }
            Err(_) => {
                warn!(id, "调用方处理过慢，放弃该调用");
                self.unregister(serial, id);
                let _ = outbound.try_send(Msg::Cancel {
                    id,
                    reason: Some("client stalled".into()),
                });
            }
        }
    }

    pub async fn begin(
        &self,
        target: &str,
        method: &str,
        params: ciborium::value::Value,
    ) -> RemoteResult<CallHandle> {
        self.ready().await;
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(REPLY_QUEUE);
        let (outbound, serial) = {
            let mut state = self.lock_state();
            let Some(conn) = state.conn.as_mut() else {
                return Err(RemoteError::new("offline", "尚未连接中继"));
            };
            conn.pending.insert(id, tx);
            (conn.outbound.clone(), conn.serial)
        };
        let call = Msg::Call {
            id,
            target: target.to_string(),
            method: method.to_string(),
            params,
        };
        if outbound.send(call).await.is_err() {
            self.unregister(serial, id);
            return Err(RemoteError::new("offline", "中继连接已断开"));
        }
        Ok(CallHandle {
            session: self.clone(),
            id,
            serial,
            rx,
        })
    }

    pub async fn call(
        &self,
        target: &str,
        method: &str,
        params: ciborium::value::Value,
        deadline: Duration,
    ) -> RemoteResult<CallResult> {
        self.call_cancel(target, method, params, deadline, &CancellationToken::new())
            .await
    }

    pub async fn call_cancel(
        &self,
        target: &str,
        method: &str,
        params: ciborium::value::Value,
        deadline: Duration,
        cancel: &CancellationToken,
    ) -> RemoteResult<CallResult> {
        let handle = self.begin(target, method, params).await?;
        handle.finish(deadline, cancel).await
    }

    pub async fn send_raw(&self, serial: u64, msg: Msg) -> RemoteResult<()> {
        let outbound = {
            let state = self.lock_state();
            state
                .conn
                .as_ref()
                .filter(|conn| conn.serial == serial)
                .map(|conn| conn.outbound.clone())
        };
        let Some(outbound) = outbound else {
            return Err(RemoteError::new("offline", "中继连接已断开"));
        };
        outbound
            .send(msg)
            .await
            .map_err(|_| RemoteError::new("offline", "中继连接已断开"))
    }

    fn unregister(&self, serial: u64, id: u64) {
        let mut state = self.lock_state();
        if let Some(conn) = state.conn.as_mut() {
            if conn.serial == serial {
                conn.pending.remove(&id);
            }
        }
    }

    fn install(
        &self,
        outbound: mpsc::Sender<Msg>,
        hub_name: String,
        targets: Vec<TargetInfo>,
    ) -> u64 {
        let mut state = self.lock_state();
        let serial = state.next_serial;
        state.next_serial += 1;
        state.hub_name = Some(hub_name);
        state.conn = Some(Conn {
            serial,
            outbound,
            pending: HashMap::new(),
        });
        drop(state);
        self.inner.connected.send_replace(true);
        self.replace_targets(serial, targets);
        serial
    }

    fn teardown(&self, serial: u64) {
        let mut state = self.lock_state();
        if state.conn.as_ref().map(|conn| conn.serial) == Some(serial) {
            state.conn = None;
            if let Ok(mut targets) = self.inner.targets.write() {
                targets.clear();
            }
            self.inner.connected.send_replace(false);
        }
    }

    fn replace_targets(&self, serial: u64, targets: Vec<TargetInfo>) {
        let connected = {
            let state = self.lock_state();
            state.conn.as_ref().map(|conn| conn.serial) == Some(serial)
        };
        if !connected {
            return;
        }
        if let Ok(mut current) = self.inner.targets.write() {
            debug!(count = targets.len(), "更新目标快照");
            *current = targets;
        }
    }

    fn apply_event(&self, serial: u64, kind: TargetEventKind, target: TargetInfo) {
        let connected = {
            let state = self.lock_state();
            state.conn.as_ref().map(|conn| conn.serial) == Some(serial)
        };
        if !connected {
            return;
        }
        let Ok(mut targets) = self.inner.targets.write() else {
            return;
        };
        targets.retain(|entry| entry.name != target.name);
        if kind == TargetEventKind::Online {
            targets.push(target);
            targets.sort_by(|left, right| left.name.cmp(&right.name));
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

pub struct CallHandle {
    session: Session,
    id: u64,
    serial: u64,
    rx: mpsc::Receiver<Msg>,
}

impl CallHandle {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub async fn recv(&mut self) -> Option<Msg> {
        self.rx.recv().await
    }

    pub fn try_recv(&mut self) -> Option<Msg> {
        self.rx.try_recv().ok()
    }

    pub async fn send(&self, msg: Msg) -> RemoteResult<()> {
        self.session.send_raw(self.serial, msg).await
    }

    pub async fn cancel(&self, reason: impl Into<String>) {
        let _ = self
            .session
            .send_raw(
                self.serial,
                Msg::Cancel {
                    id: self.id,
                    reason: Some(reason.into()),
                },
            )
            .await;
    }

    pub async fn finish(
        mut self,
        deadline: Duration,
        cancel: &CancellationToken,
    ) -> RemoteResult<CallResult> {
        let mut result = CallResult::empty();
        let session = self.session.clone();
        let id = self.id;
        let serial = self.serial;
        let outcome = timeout(deadline, async {
            loop {
                let msg = tokio::select! {
                    _ = cancel.cancelled() => {
                        let _ = send_cancel(&session, serial, id, "cancelled").await;
                        return Err(RemoteError::new("cancelled", "调用已取消"));
                    }
                    msg = self.rx.recv() => msg,
                };
                let Some(msg) = msg else {
                    return Err(RemoteError::new("offline", "中继连接中断，调用未完成"));
                };
                match msg {
                    Msg::Chunk { stream, data, .. } => {
                        if stream == crate::proto::STREAM_STDOUT {
                            result.stdout.extend_from_slice(&data);
                        } else {
                            result.stderr.extend_from_slice(&data);
                        }
                    }
                    Msg::End {
                        status,
                        exit_code,
                        size,
                        sha256,
                        truncated,
                        ..
                    } => {
                        result.ok = status == EndStatus::Ok;
                        result.exit_code = exit_code;
                        result.stopped = Some(status);
                        result.truncated = truncated.unwrap_or(false);
                        result.size = size;
                        result.sha256 = sha256;
                        return Ok(());
                    }
                    Msg::Result {
                        ok, value, error, ..
                    } => {
                        result.ok = ok;
                        result.value = value;
                        result.error = error;
                        return Ok(());
                    }
                    Msg::Cancel { .. } => {}
                    other => debug!(msg = ?other, "忽略消息"),
                }
            }
        })
        .await;

        match outcome {
            Ok(Ok(())) => Ok(result),
            Ok(Err(err)) => {
                let _ = send_cancel(&session, serial, id, "client error").await;
                Err(err)
            }
            Err(_) => {
                let _ = send_cancel(&session, serial, id, "timeout").await;
                Err(RemoteError::new(
                    "timeout",
                    format!("调用超过 {} 秒未完成", deadline.as_secs()),
                ))
            }
        }
    }
}

async fn send_cancel(session: &Session, serial: u64, id: u64, reason: &str) -> RemoteResult<()> {
    session
        .send_raw(
            serial,
            Msg::Cancel {
                id,
                reason: Some(reason.to_string()),
            },
        )
        .await
}

impl Drop for CallHandle {
    fn drop(&mut self) {
        self.session.unregister(self.serial, self.id);
    }
}

fn target_names(targets: &[TargetInfo]) -> String {
    targets
        .iter()
        .map(|target| target.name.as_str())
        .collect::<Vec<_>>()
        .join(",")
}
