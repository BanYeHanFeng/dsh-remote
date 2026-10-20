use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use ciborium::value::Value;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{interval, timeout, Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::HubConfig;
use crate::exec::transfer::transfer_limit;
use crate::exec::{Executor, Settings, DEFAULT_TIMEOUT_MS, MAX_TIMEOUT_MS};
use crate::net::{spawn_io, IoHandles, SecureConn};
use crate::proto::value as cbor;
use crate::proto::{
    caps, now_ms, Msg, ProtoError, Role, TargetEventKind, TargetInfo, TargetKind, PROTO_VERSION,
};

const HELLO_TIMEOUT: Duration = Duration::from_secs(5);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const IDLE_TIMEOUT: Duration = Duration::from_secs(45);
const WRITER_DRAIN: Duration = Duration::from_secs(2);
const CALL_TIMEOUT_FLOOR: Duration = Duration::from_secs(60);
const CALL_TIMEOUT_EXTRA: Duration = Duration::from_secs(30);

struct AgentEntry {
    info: TargetInfo,
    tx: mpsc::Sender<Msg>,
    conn_id: u64,
}

struct ServerEntry {
    tx: mpsc::Sender<Msg>,
}

struct Pending {
    server_id: u64,
    server_tx: mpsc::Sender<Msg>,
    server_req_id: u64,
    agent_id: u64,
    agent_tx: mpsc::Sender<Msg>,
    timer: JoinHandle<()>,
}

struct HubState {
    cfg: HubConfig,
    started: u64,
    agents: RwLock<HashMap<String, AgentEntry>>,
    servers: RwLock<HashMap<u64, ServerEntry>>,
    pending: Arc<Mutex<HashMap<u64, Pending>>>,
    next_id: AtomicU64,
    next_call: AtomicU64,
    executor: Executor,
}

impl HubState {
    fn new(cfg: HubConfig) -> Self {
        let executor = Executor::new(Settings::from_hub(&cfg));
        Self {
            cfg,
            started: now_ms(),
            agents: RwLock::new(HashMap::new()),
            servers: RwLock::new(HashMap::new()),
            pending: Arc::new(Mutex::new(HashMap::new())),
            next_id: AtomicU64::new(1),
            next_call: AtomicU64::new(1),
            executor,
        }
    }

    fn hub_info(&self) -> TargetInfo {
        TargetInfo {
            name: self.cfg.name.clone(),
            kind: TargetKind::Hub,
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            version: crate::VERSION.to_string(),
            since: self.started,
            roots: self.cfg.roots.clone(),
            caps: caps(self.cfg.allow_bash),
        }
    }

    async fn targets(&self) -> Vec<TargetInfo> {
        let mut list = vec![self.hub_info()];
        let agents = self.agents.read().await;
        let mut entries: Vec<TargetInfo> =
            agents.values().map(|entry| entry.info.clone()).collect();
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        list.extend(entries);
        list
    }

    async fn register_agent(
        &self,
        info: &TargetInfo,
        tx: &mpsc::Sender<Msg>,
        conn_id: u64,
    ) -> Result<(), ProtoError> {
        let mut agents = self.agents.write().await;
        if let Some(existing) = agents.get(&info.name) {
            if !existing.tx.is_closed() {
                return Err(ProtoError::new(
                    "duplicate_name",
                    format!("目标名 {} 已被占用", info.name),
                ));
            }
        }
        agents.insert(
            info.name.clone(),
            AgentEntry {
                info: info.clone(),
                tx: tx.clone(),
                conn_id,
            },
        );
        Ok(())
    }

    async fn remove_agent(&self, name: &str, tx: &mpsc::Sender<Msg>) -> bool {
        let mut agents = self.agents.write().await;
        let matched = agents
            .get(name)
            .map(|entry| entry.tx.same_channel(tx))
            .unwrap_or(false);
        if matched {
            agents.remove(name);
        }
        matched
    }

    async fn register_server(&self, conn_id: u64, tx: mpsc::Sender<Msg>) {
        self.servers
            .write()
            .await
            .insert(conn_id, ServerEntry { tx });
    }

    async fn remove_server(&self, conn_id: u64) {
        self.servers.write().await.remove(&conn_id);
    }

    async fn broadcast(&self, msg: Msg) {
        let targets: Vec<mpsc::Sender<Msg>> = self
            .servers
            .read()
            .await
            .values()
            .map(|entry| entry.tx.clone())
            .collect();
        for tx in targets {
            let _ = tx.try_send(msg.clone());
        }
    }

    async fn handle_call(
        &self,
        server_id: u64,
        server_tx: mpsc::Sender<Msg>,
        req_id: u64,
        target: String,
        method: String,
        params: Value,
    ) {
        if method == "targets" || target == self.cfg.name {
            self.handle_local_call(server_id, server_tx, req_id, method, params)
                .await;
        } else {
            self.route_call(server_id, server_tx, req_id, target, method, params)
                .await;
        }
    }

    async fn handle_local_call(
        &self,
        server_id: u64,
        server_tx: mpsc::Sender<Msg>,
        req_id: u64,
        method: String,
        params: Value,
    ) {
        if method == "targets" {
            match cbor::serialize(&self.targets().await) {
                Ok(value) => {
                    let _ = server_tx
                        .send(Msg::Result {
                            id: req_id,
                            ok: true,
                            value: Some(value),
                            error: None,
                        })
                        .await;
                }
                Err(err) => {
                    reply_error(
                        &server_tx,
                        req_id,
                        "internal",
                        format!("序列化目标列表失败: {err:#}"),
                    )
                    .await;
                }
            }
            return;
        }
        let call_id = self.next_call.fetch_add(1, Ordering::Relaxed);
        self.executor
            .dispatch(call_id, server_id, req_id, &method, params, server_tx)
            .await;
    }

    async fn route_call(
        &self,
        server_id: u64,
        server_tx: mpsc::Sender<Msg>,
        req_id: u64,
        target: String,
        method: String,
        params: Value,
    ) {
        let agent = self
            .agents
            .read()
            .await
            .get(&target)
            .map(|entry| (entry.tx.clone(), entry.conn_id));
        let Some((agent_tx, agent_id)) = agent else {
            reply_error(
                &server_tx,
                req_id,
                "unknown_target",
                format!("目标不在线: {target}"),
            )
            .await;
            return;
        };

        let call_id = self.next_call.fetch_add(1, Ordering::Relaxed);
        let deadline = call_timeout(&method, &params);
        let pending_map = self.pending.clone();
        let timer = tokio::spawn(async move {
            tokio::time::sleep(deadline).await;
            let entry = pending_map.lock().await.remove(&call_id);
            if let Some(entry) = entry {
                let _ = entry
                    .server_tx
                    .send(Msg::Result {
                        id: entry.server_req_id,
                        ok: false,
                        value: None,
                        error: Some(ProtoError::new("timeout", "远端执行超时")),
                    })
                    .await;
                let _ = entry
                    .agent_tx
                    .send(Msg::Cancel {
                        id: call_id,
                        reason: Some("timeout".into()),
                    })
                    .await;
            }
        });

        self.pending.lock().await.insert(
            call_id,
            Pending {
                server_id,
                server_tx: server_tx.clone(),
                server_req_id: req_id,
                agent_id,
                agent_tx: agent_tx.clone(),
                timer,
            },
        );

        let call = Msg::Call {
            id: call_id,
            target,
            method,
            params,
        };
        if agent_tx.send(call).await.is_err() {
            let entry = self.pending.lock().await.remove(&call_id);
            if let Some(entry) = entry {
                entry.timer.abort();
                drop(entry);
                reply_error(&server_tx, req_id, "offline", "目标连接已断开").await;
            }
        }
    }

    async fn forward_from_agent(&self, agent_id: u64, msg: Msg) {
        let Some(call_id) = msg.id() else {
            return;
        };
        let terminal = matches!(msg, Msg::End { .. } | Msg::Result { .. });
        let forwarded = {
            let mut pending = self.pending.lock().await;
            let Some(entry) = pending.get(&call_id) else {
                return;
            };
            if entry.agent_id != agent_id {
                return;
            }
            let server_tx = entry.server_tx.clone();
            let reply_id = entry.server_req_id;
            if terminal {
                if let Some(entry) = pending.remove(&call_id) {
                    entry.timer.abort();
                }
            }
            (server_tx, reply_id)
        };
        let _ = forwarded.0.send(msg.with_id(forwarded.1)).await;
    }

    async fn forward_from_server(&self, server_id: u64, msg: Msg) -> Option<Msg> {
        let Some(req_id) = msg.id() else {
            return Some(msg);
        };
        let target = {
            let pending = self.pending.lock().await;
            pending
                .iter()
                .find(|(_, entry)| entry.server_id == server_id && entry.server_req_id == req_id)
                .map(|(call_id, entry)| (*call_id, entry.agent_tx.clone()))
        };
        let Some((call_id, agent_tx)) = target else {
            return Some(msg);
        };
        if agent_tx.send(msg.with_id(call_id)).await.is_err() {
            debug!(req_id, "转发上行数据失败");
        }
        None
    }

    async fn cancel_call(&self, server_id: u64, req_id: u64, reason: Option<String>) {
        self.executor.cancel_by_reply(server_id, req_id);
        let found = {
            let pending = self.pending.lock().await;
            pending
                .iter()
                .find(|(_, entry)| entry.server_id == server_id && entry.server_req_id == req_id)
                .map(|(call_id, entry)| (*call_id, entry.agent_tx.clone()))
        };
        if let Some((call_id, agent_tx)) = found {
            let _ = agent_tx
                .send(Msg::Cancel {
                    id: call_id,
                    reason,
                })
                .await;
        }
    }

    async fn cleanup_connection(
        &self,
        role: Role,
        info: &TargetInfo,
        tx: &mpsc::Sender<Msg>,
        conn_id: u64,
    ) {
        match role {
            Role::Agent => {
                let removed = self.remove_agent(&info.name, tx).await;
                let dropped: Vec<Pending> = {
                    let mut pending = self.pending.lock().await;
                    let ids: Vec<u64> = pending
                        .iter()
                        .filter(|(_, entry)| entry.agent_id == conn_id)
                        .map(|(id, _)| *id)
                        .collect();
                    ids.into_iter()
                        .filter_map(|id| pending.remove(&id))
                        .collect()
                };
                for entry in dropped {
                    entry.timer.abort();
                    let _ = entry
                        .server_tx
                        .send(Msg::Result {
                            id: entry.server_req_id,
                            ok: false,
                            value: None,
                            error: Some(ProtoError::new(
                                "offline",
                                format!("目标 {} 已断开", info.name),
                            )),
                        })
                        .await;
                }
                if removed {
                    self.broadcast(Msg::TargetEvent {
                        kind: TargetEventKind::Offline,
                        target: info.clone(),
                    })
                    .await;
                    info!(name = %info.name, "目标下线");
                }
            }
            Role::Server => {
                self.remove_server(conn_id).await;
                self.executor.cancel_by_server(conn_id);
                let cancelled: Vec<(u64, Pending)> = {
                    let mut pending = self.pending.lock().await;
                    let ids: Vec<u64> = pending
                        .iter()
                        .filter(|(_, entry)| entry.server_id == conn_id)
                        .map(|(id, _)| *id)
                        .collect();
                    ids.into_iter()
                        .filter_map(|id| pending.remove(&id).map(|entry| (id, entry)))
                        .collect()
                };
                for (call_id, entry) in cancelled {
                    entry.timer.abort();
                    let _ = entry
                        .agent_tx
                        .send(Msg::Cancel {
                            id: call_id,
                            reason: Some("server disconnected".into()),
                        })
                        .await;
                }
            }
        }
    }
}

pub async fn run(cfg: HubConfig, shutdown: CancellationToken) -> Result<()> {
    let listener = TcpListener::bind(&cfg.listen)
        .await
        .with_context(|| format!("监听 {} 失败", cfg.listen))?;
    let local = listener.local_addr()?;
    info!(
        listen = %local,
        name = %cfg.name,
        key = %cfg.key.fingerprint(),
        "中继已启动"
    );
    serve(listener, cfg, shutdown).await
}

pub async fn serve(
    listener: TcpListener,
    cfg: HubConfig,
    shutdown: CancellationToken,
) -> Result<()> {
    let state = Arc::new(HubState::new(cfg));
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    let state = state.clone();
                    let token = shutdown.clone();
                    tokio::spawn(async move {
                        if let Err(err) = handle_conn(state, stream, peer, token).await {
                            warn!(%peer, error = %format!("{err:#}"), "连接结束");
                        }
                    });
                }
                Err(err) => warn!(error = %err, "接受连接失败"),
            }
        }
    }
    info!("中继停止监听");
    Ok(())
}

async fn handle_conn(
    state: Arc<HubState>,
    stream: TcpStream,
    peer: SocketAddr,
    shutdown: CancellationToken,
) -> Result<()> {
    let mut conn = SecureConn::accept(stream, &state.cfg.key)
        .await
        .context("握手失败")?;
    let hello = timeout(HELLO_TIMEOUT, conn.recv())
        .await
        .map_err(|_| anyhow!("等待 Hello 超时"))??;
    let (role, name, info) = parse_hello(hello)?;
    info!(%peer, ?role, name = %name, "握手完成");

    let IoHandles {
        mut inbound,
        outbound,
        reader,
        writer,
    } = spawn_io(conn);
    let conn_id = state.next_id.fetch_add(1, Ordering::Relaxed);

    match role {
        Role::Agent => {
            if let Err(err) = state.register_agent(&info, &outbound, conn_id).await {
                let _ = outbound.try_send(Msg::Bye {
                    code: err.code,
                    message: err.message,
                });
                drop(outbound);
                let _ = timeout(WRITER_DRAIN, writer).await;
                reader.abort();
                return Ok(());
            }
        }
        Role::Server => {
            state.register_server(conn_id, outbound.clone()).await;
        }
    }

    let welcome = Msg::Welcome {
        proto: PROTO_VERSION,
        hub_name: state.cfg.name.clone(),
        session: conn_id.to_string(),
        targets: state.targets().await,
        server_online: !state.servers.read().await.is_empty(),
    };
    if outbound.try_send(welcome).is_err() {
        state
            .cleanup_connection(role, &info, &outbound, conn_id)
            .await;
        drop(outbound);
        let _ = timeout(WRITER_DRAIN, writer).await;
        reader.abort();
        return Ok(());
    }

    if matches!(role, Role::Agent) {
        state
            .broadcast(Msg::TargetEvent {
                kind: TargetEventKind::Online,
                target: info.clone(),
            })
            .await;
        info!(name = %name, "目标上线");
    }

    let mut ticker = interval(HEARTBEAT_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_rx = Instant::now();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                let _ = outbound.try_send(Msg::Bye {
                    code: "shutdown".into(),
                    message: "中继停止".into(),
                });
                break;
            }
            incoming = inbound.recv() => match incoming {
                Some(msg) => {
                    last_rx = Instant::now();
                    if role == Role::Agent
                        && matches!(msg, Msg::Result { .. } | Msg::Chunk { .. } | Msg::End { .. })
                    {
                        state.forward_from_agent(conn_id, msg).await;
                        continue;
                    }
                    match msg {
                        Msg::Ping { nonce } => {
                            let _ = outbound.try_send(Msg::Pong { nonce });
                        }
                        Msg::Pong { .. } => {}
                        Msg::Bye { code, message } => {
                            info!(name = %name, code = %code, message = %message, "对端关闭连接");
                            break;
                        }
                        Msg::Call { id, target, method, params } => {
                            if role == Role::Server {
                                state
                                    .handle_call(conn_id, outbound.clone(), id, target, method, params)
                                    .await;
                            } else {
                                warn!(name = %name, "被控端不能发起调用");
                            }
                        }
                        Msg::Cancel { id, reason } => {
                            if role == Role::Server {
                                state.cancel_call(conn_id, id, reason).await;
                            }
                        }
                        Msg::Chunk { .. } | Msg::End { .. } => {
                            if role == Role::Server {
                                if let Some(msg) = state.forward_from_server(conn_id, msg).await {
                                    if !state.executor.feed_from_server(conn_id, msg).await {
                                        debug!(name = %name, "丢弃没有对应调用的上行数据");
                                    }
                                }
                            }
                        }
                        other => debug!(name = %name, msg = ?other, "忽略消息"),
                    }
                }
                None => {
                    debug!(name = %name, "连接读端关闭");
                    break;
                }
            },
            _ = ticker.tick() => {
                if last_rx.elapsed() > IDLE_TIMEOUT {
                    warn!(name = %name, "心跳超时");
                    break;
                }
                let nonce = rand::random::<u64>();
                if outbound.try_send(Msg::Ping { nonce }).is_err() {
                    break;
                }
            }
        }
    }

    state
        .cleanup_connection(role, &info, &outbound, conn_id)
        .await;
    drop(outbound);
    let _ = timeout(WRITER_DRAIN, writer).await;
    reader.abort();
    info!(%peer, name = %name, "连接关闭");
    Ok(())
}

fn parse_hello(msg: Msg) -> Result<(Role, String, TargetInfo)> {
    let Msg::Hello {
        proto,
        role,
        name,
        version,
        os,
        arch,
        caps,
        roots,
    } = msg
    else {
        bail!("首条消息不是 Hello");
    };
    if proto != PROTO_VERSION {
        bail!("协议版本不匹配：收到 {proto}，期望 {PROTO_VERSION}");
    }
    let name = name.trim().to_string();
    if name.is_empty() {
        bail!("目标名为空");
    }
    if name.len() > 64 {
        bail!("目标名过长: {}", name.len());
    }
    let info = TargetInfo {
        name: name.clone(),
        kind: TargetKind::Agent,
        os,
        arch,
        version,
        since: now_ms(),
        roots,
        caps,
    };
    Ok((role, name, info))
}

fn call_timeout(method: &str, params: &Value) -> Duration {
    match method {
        "bash" => {
            let requested = cbor::u64_value(params, "timeout_ms")
                .unwrap_or(DEFAULT_TIMEOUT_MS)
                .min(MAX_TIMEOUT_MS);
            Duration::from_millis(requested) + CALL_TIMEOUT_EXTRA
        }
        "upload" => transfer_limit(cbor::u64_value(params, "size").unwrap_or_default()),
        "download" => transfer_limit(cbor::u64_value(params, "max_size").unwrap_or_default()),
        _ => CALL_TIMEOUT_FLOOR,
    }
}

async fn reply_error(tx: &mpsc::Sender<Msg>, id: u64, code: &str, message: impl Into<String>) {
    let _ = tx
        .send(Msg::Result {
            id,
            ok: false,
            value: None,
            error: Some(ProtoError::new(code, message)),
        })
        .await;
}
