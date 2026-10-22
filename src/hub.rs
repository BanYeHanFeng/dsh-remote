use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use ciborium::value::Value;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio::time::{interval, timeout, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::HubConfig;
use crate::exec::{limits, Executor};
use crate::net::link::{End, Link, HELLO_TIMEOUT};
use crate::net::Sink;
use crate::proto::value as cbor;
use crate::proto::{
    now_ms, Msg, ProtoError, Role, TargetEventKind, TargetInfo, TargetKind, PROTO_VERSION,
};

const SWEEP_INTERVAL: Duration = Duration::from_secs(1);
const LOCAL_QUEUE: usize = 256;

struct AgentEntry {
    info: TargetInfo,
    tx: Sink,
    conn_id: u64,
    token: CancellationToken,
}

struct Pending {
    server_id: u64,
    server_tx: Sink,
    server_req_id: u64,
    agent_id: u64,
    agent_tx: Sink,
    deadline: Instant,
}

struct HubState {
    cfg: HubConfig,
    started: u64,
    agents: RwLock<HashMap<String, AgentEntry>>,
    servers: RwLock<HashMap<u64, Sink>>,
    pending: Mutex<HashMap<u64, Pending>>,
    next_id: AtomicU64,
    next_call: AtomicU64,
    executor: Executor,
}

impl HubState {
    fn new(cfg: HubConfig) -> Self {
        let executor = Executor::new(cfg.target.clone());
        Self {
            cfg,
            started: now_ms(),
            agents: RwLock::new(HashMap::new()),
            servers: RwLock::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
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
            roots: self.cfg.target.roots.clone(),
        }
    }

    async fn targets(&self) -> Vec<TargetInfo> {
        let mut list: Vec<TargetInfo> = self
            .agents
            .read()
            .await
            .values()
            .map(|entry| entry.info.clone())
            .collect();
        list.sort_by(|left, right| left.name.cmp(&right.name));
        list
    }

    async fn register_agent(
        &self,
        info: &TargetInfo,
        tx: &Sink,
        conn_id: u64,
        token: &CancellationToken,
    ) -> Result<Option<AgentEntry>, ProtoError> {
        if info.name == self.cfg.name {
            return Err(ProtoError::new(
                "duplicate_name",
                format!("目标名 {} 与中继自身重名", info.name),
            ));
        }
        let entry = AgentEntry {
            info: info.clone(),
            tx: tx.clone(),
            conn_id,
            token: token.clone(),
        };
        let previous = self.agents.write().await.insert(info.name.clone(), entry);
        Ok(previous.filter(|old| !old.tx.is_closed() && old.conn_id != conn_id))
    }

    async fn remove_agent(&self, name: &str, tx: &Sink) -> bool {
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

    async fn register_server(&self, conn_id: u64, tx: Sink) {
        self.servers.write().await.insert(conn_id, tx);
    }

    async fn broadcast(&self, msg: Msg) {
        let targets: Vec<Sink> = self.servers.read().await.values().cloned().collect();
        for tx in targets {
            let _ = tx.try_send(msg.clone());
        }
    }

    async fn handle_call(
        &self,
        server_id: u64,
        server_tx: Sink,
        req_id: u64,
        target: String,
        method: String,
        params: Value,
    ) {
        if method == "targets" {
            self.reply_targets(&server_tx, req_id).await;
            return;
        }
        self.route_call(server_id, server_tx, req_id, target, method, params)
            .await;
    }

    async fn reply_targets(&self, server_tx: &Sink, req_id: u64) {
        let value = match cbor::serialize(&self.targets().await) {
            Ok(value) => value,
            Err(err) => {
                reply_error(
                    server_tx,
                    req_id,
                    "internal",
                    format!("序列化目标列表失败: {err:#}"),
                );
                return;
            }
        };
        if server_tx
            .try_send(Msg::Result {
                id: req_id,
                ok: true,
                value: Some(value),
                error: None,
            })
            .is_err()
        {
            warn!(req_id, "回复目标列表失败");
        }
    }

    async fn route_call(
        &self,
        server_id: u64,
        server_tx: Sink,
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
            );
            return;
        };

        let call_id = self.next_call.fetch_add(1, Ordering::Relaxed);
        let deadline = Instant::now() + limits::deadline(&method, &params);
        self.pending.lock().await.insert(
            call_id,
            Pending {
                server_id,
                server_tx: server_tx.clone(),
                server_req_id: req_id,
                agent_id,
                agent_tx: agent_tx.clone(),
                deadline,
            },
        );

        let call = Msg::Call {
            id: call_id,
            target,
            method,
            params,
        };
        if agent_tx.try_send(call).is_err() {
            let removed = self.pending.lock().await.remove(&call_id);
            drop(removed);
            reply_error(&server_tx, req_id, "offline", "目标连接已断开或繁忙");
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
            let target = (entry.server_tx.clone(), entry.server_req_id);
            if terminal {
                pending.remove(&call_id);
            }
            target
        };
        if forwarded.0.try_send(msg.with_id(forwarded.1)).is_err() {
            debug!(call_id, "转发出站结果失败");
        }
    }

    async fn forward_from_server(&self, server_id: u64, msg: Msg) -> bool {
        let Some(req_id) = msg.id() else {
            return false;
        };
        let target = {
            let pending = self.pending.lock().await;
            pending
                .iter()
                .find(|(_, entry)| entry.server_id == server_id && entry.server_req_id == req_id)
                .map(|(call_id, entry)| (*call_id, entry.agent_tx.clone()))
        };
        let Some((call_id, agent_tx)) = target else {
            return false;
        };
        if agent_tx.try_send(msg.with_id(call_id)).is_err() {
            debug!(req_id, "转发上行数据失败");
        }
        true
    }

    async fn cancel_call(&self, server_id: u64, req_id: u64, reason: Option<String>) {
        let found = {
            let pending = self.pending.lock().await;
            pending
                .iter()
                .find(|(_, entry)| entry.server_id == server_id && entry.server_req_id == req_id)
                .map(|(call_id, entry)| (*call_id, entry.agent_tx.clone()))
        };
        if let Some((call_id, agent_tx)) = found {
            let _ = agent_tx.try_send(Msg::Cancel {
                id: call_id,
                reason,
            });
        }
    }

    async fn cleanup_connection(&self, role: Role, info: &TargetInfo, sink: &Sink, conn_id: u64) {
        match role {
            Role::Agent => {
                let removed = self.remove_agent(&info.name, sink).await;
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
                    let _ = entry.server_tx.try_send(Msg::Result {
                        id: entry.server_req_id,
                        ok: false,
                        value: None,
                        error: Some(ProtoError::new(
                            "offline",
                            format!("目标 {} 已断开", info.name),
                        )),
                    });
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
                    let _ = entry.agent_tx.try_send(Msg::Cancel {
                        id: call_id,
                        reason: Some("server disconnected".into()),
                    });
                }
            }
        }
    }

    async fn remove_server(&self, conn_id: u64) {
        self.servers.write().await.remove(&conn_id);
    }

    async fn sweep(&self, shutdown: &CancellationToken) {
        let mut ticker = interval(SWEEP_INTERVAL);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = ticker.tick() => {}
            }
            let now = Instant::now();
            let expired: Vec<(u64, Pending)> = {
                let mut pending = self.pending.lock().await;
                let ids: Vec<u64> = pending
                    .iter()
                    .filter(|(_, entry)| entry.deadline <= now)
                    .map(|(id, _)| *id)
                    .collect();
                ids.into_iter()
                    .filter_map(|id| pending.remove(&id).map(|entry| (id, entry)))
                    .collect()
            };
            for (call_id, entry) in expired {
                let _ = entry.server_tx.try_send(Msg::Result {
                    id: entry.server_req_id,
                    ok: false,
                    value: None,
                    error: Some(ProtoError::new("timeout", "远端执行超时")),
                });
                let _ = entry.agent_tx.try_send(Msg::Cancel {
                    id: call_id,
                    reason: Some("timeout".into()),
                });
            }
        }
    }

    async fn spawn_local_agent(self: &Arc<Self>, shutdown: CancellationToken) {
        let conn_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (hub_tx, mut from_hub) = Sink::pair(LOCAL_QUEUE);
        let (agent_tx, mut to_hub) = Sink::pair(LOCAL_QUEUE);
        let info = self.hub_info();
        self.agents.write().await.insert(
            info.name.clone(),
            AgentEntry {
                info,
                tx: hub_tx,
                conn_id,
                token: CancellationToken::new(),
            },
        );
        let state = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    msg = from_hub.recv() => match msg {
                        Some(Msg::Call { id, method, params, .. }) => {
                            state.executor.dispatch(id, &method, params, agent_tx.clone()).await;
                        }
                        Some(Msg::Cancel { id, .. }) => state.executor.cancel(id),
                        Some(msg @ (Msg::Chunk { .. } | Msg::End { .. })) => {
                            if !state.executor.feed(msg).await {
                                debug!("本地传输消息没有归属");
                            }
                        }
                        Some(other) => debug!(msg = ?other, "本地目标忽略消息"),
                        None => break,
                    },
                    msg = to_hub.recv() => match msg {
                        Some(msg) => state.forward_from_agent(conn_id, msg).await,
                        None => break,
                    },
                }
            }
            state.executor.cancel_all();
        });
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
    HubState::spawn_local_agent(&state, shutdown.clone()).await;
    {
        let state = state.clone();
        let token = shutdown.clone();
        tokio::spawn(async move { state.sweep(&token).await });
    }
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
    let mut link = Link::accept(stream, &state.cfg.key)
        .await
        .context("握手失败")?;
    let hello = timeout(HELLO_TIMEOUT, link.recv())
        .await
        .map_err(|_| anyhow::anyhow!("等待 Hello 超时"))??;
    let (role, name, info) = match parse_hello(hello) {
        Ok(parsed) => parsed,
        Err(err) => {
            let mismatch = format!("{err:#}").contains("协议版本不匹配");
            link.bye(
                if mismatch {
                    "proto_mismatch"
                } else {
                    "handshake"
                },
                format!("{err:#}"),
            );
            link.close().await;
            return Err(err);
        }
    };
    info!(%peer, ?role, name = %name, "握手完成");

    let conn_id = state.next_id.fetch_add(1, Ordering::Relaxed);
    let conn_token = CancellationToken::new();

    match role {
        Role::Agent => match state
            .register_agent(&info, link.sink(), conn_id, &conn_token)
            .await
        {
            Ok(previous) => {
                if let Some(old) = previous {
                    info!(
                        name = %info.name,
                        old = old.conn_id,
                        new = conn_id,
                        "顶替同名目标的旧连接"
                    );
                    old.token.cancel();
                }
            }
            Err(err) => {
                link.bye(&err.code, err.message);
                link.close().await;
                return Ok(());
            }
        },
        Role::Server => state.register_server(conn_id, link.sink().clone()).await,
    }

    let welcome = Msg::Welcome {
        proto: PROTO_VERSION,
        hub_name: state.cfg.name.clone(),
        targets: state.targets().await,
        server_online: !state.servers.read().await.is_empty(),
    };
    if link.send(welcome).await.is_err() {
        state
            .cleanup_connection(role, &info, link.sink(), conn_id)
            .await;
        link.close().await;
        return Ok(());
    }

    if role == Role::Agent {
        state
            .broadcast(Msg::TargetEvent {
                kind: TargetEventKind::Online,
                target: info.clone(),
            })
            .await;
        info!(name = %name, "目标上线");
    }

    loop {
        match link.next(&shutdown, &conn_token).await {
            Ok(msg) => {
                if role == Role::Agent
                    && matches!(
                        msg,
                        Msg::Result { .. } | Msg::Chunk { .. } | Msg::End { .. }
                    )
                {
                    state.forward_from_agent(conn_id, msg).await;
                    continue;
                }
                match msg {
                    Msg::Ping { nonce } => {
                        let _ = link.sink().try_send(Msg::Pong { nonce });
                    }
                    Msg::Pong { .. } => {}
                    Msg::Bye { code, message } => {
                        info!(name = %name, code = %code, message = %message, "对端关闭连接");
                        break;
                    }
                    Msg::Call {
                        id,
                        target,
                        method,
                        params,
                    } => {
                        if role == Role::Server {
                            state
                                .handle_call(
                                    conn_id,
                                    link.sink().clone(),
                                    id,
                                    target,
                                    method,
                                    params,
                                )
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
                    msg @ (Msg::Chunk { .. } | Msg::End { .. }) => {
                        if role == Role::Server && !state.forward_from_server(conn_id, msg).await {
                            debug!(name = %name, "丢弃没有对应调用的上行数据");
                        }
                    }
                    other => debug!(name = %name, msg = ?other, "忽略消息"),
                }
            }
            Err(End::Stopped) => {
                link.bye("replaced", format!("目标名 {} 已被新连接顶替", info.name));
                info!(name = %info.name, conn_id, "连接被同名目标顶替");
                break;
            }
            Err(End::Shutdown) => {
                link.bye("shutdown", "中继停止");
                break;
            }
            Err(end) => {
                debug!(name = %name, reason = end.reason(), "连接结束");
                break;
            }
        }
    }

    state
        .cleanup_connection(role, &info, link.sink(), conn_id)
        .await;
    link.close().await;
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
    };
    Ok((role, name, info))
}

fn reply_error(tx: &Sink, id: u64, code: &str, message: impl Into<String>) {
    if tx
        .try_send(Msg::Result {
            id,
            ok: false,
            value: None,
            error: Some(ProtoError::new(code, message)),
        })
        .is_err()
    {
        warn!(id, code, "回复失败");
    }
}
