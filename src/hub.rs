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
use crate::proto::params::{DownloadParams, TransferParams, UploadParams};
use crate::proto::value as cbor;
use crate::proto::{
    now_ms, EndStatus, Msg, ProtoError, Role, TargetEventKind, TargetInfo, TargetKind,
    PROTO_VERSION,
};

const SWEEP_INTERVAL: Duration = Duration::from_secs(1);
const LOCAL_QUEUE: usize = 256;
const PUMP_SEND_TIMEOUT: Duration = Duration::from_secs(10);

struct AgentEntry {
    info: TargetInfo,
    tx: Sink,
    conn_id: u64,
    token: CancellationToken,
}

struct Leg {
    agent_id: u64,
    agent_tx: Sink,
    call_id: u64,
}

struct CallRoute {
    server_id: u64,
    server_tx: Sink,
    server_req_id: u64,
    leg: Leg,
    deadline: Instant,
}

struct TransferRoute {
    server_id: u64,
    server_tx: Sink,
    server_req_id: u64,
    source: Leg,
    dest: Leg,
    deadline: Instant,
    next_seq: u64,
    committing: bool,
}

enum Pending {
    Call(CallRoute),
    Transfer(TransferRoute),
}

enum Decision {
    Nothing,
    Send {
        tx: Sink,
        msg: Msg,
    },
    SendThenCancel {
        tx: Sink,
        msg: Msg,
        cancels: Vec<(Sink, u64, &'static str)>,
    },
    Fail {
        server_tx: Sink,
        server_req_id: u64,
        code: &'static str,
        message: String,
        cancels: Vec<(Sink, u64, &'static str)>,
        ends: Vec<(Sink, Msg)>,
    },
}

impl Pending {
    fn server(&self) -> (u64, &Sink, u64) {
        match self {
            Pending::Call(call) => (call.server_id, &call.server_tx, call.server_req_id),
            Pending::Transfer(transfer) => (
                transfer.server_id,
                &transfer.server_tx,
                transfer.server_req_id,
            ),
        }
    }

    fn deadline(&self) -> Instant {
        match self {
            Pending::Call(call) => call.deadline,
            Pending::Transfer(transfer) => transfer.deadline,
        }
    }

    fn touches_agent(&self, conn_id: u64) -> bool {
        match self {
            Pending::Call(call) => call.leg.agent_id == conn_id,
            Pending::Transfer(transfer) => {
                transfer.source.agent_id == conn_id || transfer.dest.agent_id == conn_id
            }
        }
    }
}

#[derive(Default)]
struct Routes {
    pending: HashMap<u64, Pending>,
    legs: HashMap<u64, u64>,
}

impl Routes {
    fn track(&mut self, root: u64, pending: Pending) {
        match &pending {
            Pending::Call(call) => {
                self.legs.insert(call.leg.call_id, root);
            }
            Pending::Transfer(transfer) => {
                self.legs.insert(transfer.source.call_id, root);
                self.legs.insert(transfer.dest.call_id, root);
            }
        }
        self.pending.insert(root, pending);
    }

    fn take(&mut self, root: u64) -> Option<Pending> {
        let removed = self.pending.remove(&root)?;
        match &removed {
            Pending::Call(call) => {
                self.legs.remove(&call.leg.call_id);
            }
            Pending::Transfer(transfer) => {
                self.legs.remove(&transfer.source.call_id);
                self.legs.remove(&transfer.dest.call_id);
            }
        }
        Some(removed)
    }

    fn root_of(&self, leg_id: u64) -> Option<u64> {
        self.legs.get(&leg_id).copied()
    }

    fn owned_by_server(&self, server_id: u64, req_id: u64) -> Option<u64> {
        self.pending
            .iter()
            .find(|(_, entry)| {
                let (owner, _, request) = entry.server();
                owner == server_id && request == req_id
            })
            .map(|(root, _)| *root)
    }
}

struct HubState {
    cfg: HubConfig,
    started: u64,
    agents: RwLock<HashMap<String, AgentEntry>>,
    servers: RwLock<HashMap<u64, Sink>>,
    routes: Mutex<Routes>,
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
            routes: Mutex::new(Routes::default()),
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
        if method == "transfer" {
            self.route_transfer(server_id, server_tx, req_id, params)
                .await;
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
        let agent = self.agent_leg(&target).await;
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
        self.routes.lock().await.track(
            call_id,
            Pending::Call(CallRoute {
                server_id,
                server_tx: server_tx.clone(),
                server_req_id: req_id,
                leg: Leg {
                    agent_id,
                    agent_tx: agent_tx.clone(),
                    call_id,
                },
                deadline,
            }),
        );

        let call = Msg::Call {
            id: call_id,
            target,
            method,
            params,
        };
        if agent_tx.try_send(call).is_err() {
            let removed = self.routes.lock().await.take(call_id);
            drop(removed);
            reply_error(&server_tx, req_id, "offline", "目标连接已断开或繁忙");
        }
    }

    async fn route_transfer(&self, server_id: u64, server_tx: Sink, req_id: u64, params: Value) {
        let parsed: TransferParams = match cbor::deserialize(&params) {
            Ok(parsed) => parsed,
            Err(err) => {
                reply_error(
                    &server_tx,
                    req_id,
                    "protocol",
                    format!("transfer 参数不合法: {err:#}"),
                );
                return;
            }
        };
        if parsed.source.trim().is_empty() || parsed.dest.trim().is_empty() {
            reply_error(
                &server_tx,
                req_id,
                "protocol",
                "transfer 需要 source 与 dest",
            );
            return;
        }
        if parsed.source_path.trim().is_empty() || parsed.dest_path.trim().is_empty() {
            reply_error(
                &server_tx,
                req_id,
                "protocol",
                "transfer 需要 source_path 与 dest_path",
            );
            return;
        }
        let Some((source_tx, source_id)) = self.agent_leg(&parsed.source).await else {
            reply_error(
                &server_tx,
                req_id,
                "unknown_target",
                format!("源目标不在线: {}", parsed.source),
            );
            return;
        };
        let Some((dest_tx, dest_id)) = self.agent_leg(&parsed.dest).await else {
            reply_error(
                &server_tx,
                req_id,
                "unknown_target",
                format!("目的目标不在线: {}", parsed.dest),
            );
            return;
        };

        let upload = match cbor::serialize(&UploadParams {
            remote_path: parsed.dest_path.clone(),
            size: parsed.max_size,
            overwrite: parsed.overwrite,
            create_parents: parsed.create_parents,
        }) {
            Ok(value) => value,
            Err(err) => {
                reply_error(
                    &server_tx,
                    req_id,
                    "internal",
                    format!("序列化上传参数失败: {err:#}"),
                );
                return;
            }
        };
        let download = match cbor::serialize(&DownloadParams {
            remote_path: parsed.source_path.clone(),
            max_size: parsed.max_size,
        }) {
            Ok(value) => value,
            Err(err) => {
                reply_error(
                    &server_tx,
                    req_id,
                    "internal",
                    format!("序列化下载参数失败: {err:#}"),
                );
                return;
            }
        };

        let source_call = self.next_call.fetch_add(1, Ordering::Relaxed);
        let dest_call = self.next_call.fetch_add(1, Ordering::Relaxed);
        let deadline = Instant::now() + limits::deadline("transfer", &params);
        self.routes.lock().await.track(
            source_call,
            Pending::Transfer(TransferRoute {
                server_id,
                server_tx: server_tx.clone(),
                server_req_id: req_id,
                source: Leg {
                    agent_id: source_id,
                    agent_tx: source_tx.clone(),
                    call_id: source_call,
                },
                dest: Leg {
                    agent_id: dest_id,
                    agent_tx: dest_tx.clone(),
                    call_id: dest_call,
                },
                deadline,
                next_seq: 0,
                committing: false,
            }),
        );

        let upload_call = Msg::Call {
            id: dest_call,
            target: parsed.dest.clone(),
            method: "upload".to_string(),
            params: upload,
        };
        let download_call = Msg::Call {
            id: source_call,
            target: parsed.source.clone(),
            method: "download".to_string(),
            params: download,
        };
        let started =
            dest_tx.try_send(upload_call).is_ok() && source_tx.try_send(download_call).is_ok();
        if !started {
            let removed = self.routes.lock().await.take(source_call);
            drop(removed);
            for (tx, id) in [(&dest_tx, dest_call), (&source_tx, source_call)] {
                let _ = tx.try_send(Msg::Cancel {
                    id,
                    reason: Some("transfer aborted".to_string()),
                });
            }
            reply_error(&server_tx, req_id, "offline", "目标连接已断开或繁忙");
            return;
        }
        info!(
            source = %parsed.source,
            dest = %parsed.dest,
            "开始节点互传"
        );
    }

    async fn agent_leg(&self, name: &str) -> Option<(Sink, u64)> {
        self.agents
            .read()
            .await
            .get(name)
            .map(|entry| (entry.tx.clone(), entry.conn_id))
    }

    async fn forward_from_agent(&self, agent_id: u64, msg: Msg) {
        let Some(leg_id) = msg.id() else {
            return;
        };
        let decision = {
            let mut routes = self.routes.lock().await;
            let Some(root) = routes.root_of(leg_id) else {
                return;
            };
            let Some(entry) = routes.take(root) else {
                return;
            };
            match entry {
                Pending::Call(call) => {
                    if call.leg.agent_id != agent_id {
                        routes.track(root, Pending::Call(call));
                        return;
                    }
                    let terminal = matches!(msg, Msg::End { .. } | Msg::Result { .. });
                    let out = Decision::Send {
                        tx: call.server_tx.clone(),
                        msg: msg.with_id(call.server_req_id),
                    };
                    if !terminal {
                        routes.track(root, Pending::Call(call));
                    }
                    out
                }
                Pending::Transfer(mut transfer) => {
                    if leg_id == transfer.source.call_id {
                        if transfer.source.agent_id != agent_id {
                            routes.track(root, Pending::Transfer(transfer));
                            return;
                        }
                        match msg {
                            Msg::Chunk {
                                stream,
                                packed,
                                data,
                                ..
                            } if !transfer.committing => {
                                let seq = transfer.next_seq;
                                transfer.next_seq += 1;
                                let out = Decision::Send {
                                    tx: transfer.dest.agent_tx.clone(),
                                    msg: Msg::Chunk {
                                        id: transfer.dest.call_id,
                                        seq,
                                        stream,
                                        packed,
                                        data,
                                    },
                                };
                                routes.track(root, Pending::Transfer(transfer));
                                out
                            }
                            Msg::End {
                                status,
                                size,
                                sha256,
                                ..
                            } => {
                                if status == EndStatus::Ok {
                                    transfer.committing = true;
                                    let out = Decision::Send {
                                        tx: transfer.dest.agent_tx.clone(),
                                        msg: Msg::End {
                                            id: transfer.dest.call_id,
                                            status,
                                            size,
                                            sha256,
                                        },
                                    };
                                    routes.track(root, Pending::Transfer(transfer));
                                    out
                                } else {
                                    let message = match status {
                                        EndStatus::Cancelled => "源端取消了这次传输",
                                        _ => "源端读取文件失败",
                                    };
                                    Decision::Fail {
                                        server_tx: transfer.server_tx.clone(),
                                        server_req_id: transfer.server_req_id,
                                        code: "io",
                                        message: message.to_string(),
                                        cancels: Vec::new(),
                                        ends: vec![(
                                            transfer.dest.agent_tx.clone(),
                                            Msg::End {
                                                id: transfer.dest.call_id,
                                                status,
                                                size: None,
                                                sha256: None,
                                            },
                                        )],
                                    }
                                }
                            }
                            Msg::Result { ok, error, .. } if !ok => {
                                let (code, message) = match error {
                                    Some(error) => (remote_code(&error.code), error.message),
                                    None => ("io", "源端读取文件失败".to_string()),
                                };
                                Decision::Fail {
                                    server_tx: transfer.server_tx.clone(),
                                    server_req_id: transfer.server_req_id,
                                    code,
                                    message,
                                    cancels: vec![(
                                        transfer.dest.agent_tx.clone(),
                                        transfer.dest.call_id,
                                        "transfer aborted",
                                    )],
                                    ends: Vec::new(),
                                }
                            }
                            other => {
                                debug!(msg = ?other, "互传的源端回了意外消息");
                                routes.track(root, Pending::Transfer(transfer));
                                Decision::Nothing
                            }
                        }
                    } else {
                        if transfer.dest.agent_id != agent_id {
                            routes.track(root, Pending::Transfer(transfer));
                            return;
                        }
                        match msg {
                            Msg::Result {
                                ok, value, error, ..
                            } => {
                                let cancels = if ok {
                                    Vec::new()
                                } else {
                                    vec![(
                                        transfer.source.agent_tx.clone(),
                                        transfer.source.call_id,
                                        "transfer aborted",
                                    )]
                                };
                                Decision::SendThenCancel {
                                    tx: transfer.server_tx.clone(),
                                    msg: Msg::Result {
                                        id: transfer.server_req_id,
                                        ok,
                                        value,
                                        error,
                                    },
                                    cancels,
                                }
                            }
                            other => {
                                debug!(msg = ?other, "互传的目的端回了意外消息");
                                routes.track(root, Pending::Transfer(transfer));
                                Decision::Nothing
                            }
                        }
                    }
                }
            }
        };
        self.deliver(decision).await;
    }

    async fn deliver(&self, decision: Decision) {
        match decision {
            Decision::Nothing => {}
            Decision::Send { tx, msg } => self.forward(tx, msg).await,
            Decision::SendThenCancel { tx, msg, cancels } => {
                self.forward(tx, msg).await;
                for (tx, id, reason) in cancels {
                    let _ = tx.try_send(Msg::Cancel {
                        id,
                        reason: Some(reason.to_string()),
                    });
                }
            }
            Decision::Fail {
                server_tx,
                server_req_id,
                code,
                message,
                cancels,
                ends,
            } => {
                for (tx, msg) in ends {
                    self.forward(tx, msg).await;
                }
                for (tx, id, reason) in cancels {
                    let _ = tx.try_send(Msg::Cancel {
                        id,
                        reason: Some(reason.to_string()),
                    });
                }
                reply_error(&server_tx, server_req_id, code, message);
            }
        }
    }

    async fn forward(&self, tx: Sink, msg: Msg) {
        let id = msg.id();
        match timeout(PUMP_SEND_TIMEOUT, tx.send(msg)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => debug!(leg = ?id, "转发出站消息失败"),
            Err(_) => {
                warn!(leg = ?id, "目的端长时间不接受数据，放弃该调用");
                if let Some(leg) = id {
                    self.abort_leg(leg, "busy", "目的端长时间不接受数据").await;
                }
            }
        }
    }

    async fn abort_leg(&self, leg_id: u64, code: &'static str, message: impl Into<String>) {
        let taken = {
            let mut routes = self.routes.lock().await;
            let Some(root) = routes.root_of(leg_id) else {
                return;
            };
            routes.take(root)
        };
        let message = message.into();
        match taken {
            Some(Pending::Call(call)) => {
                let _ = call.leg.agent_tx.try_send(Msg::Cancel {
                    id: call.leg.call_id,
                    reason: Some(message.clone()),
                });
                reply_error(&call.server_tx, call.server_req_id, code, message);
            }
            Some(Pending::Transfer(transfer)) => {
                let reason = Some(message.clone());
                let _ = transfer.source.agent_tx.try_send(Msg::Cancel {
                    id: transfer.source.call_id,
                    reason: reason.clone(),
                });
                let _ = transfer.dest.agent_tx.try_send(Msg::Cancel {
                    id: transfer.dest.call_id,
                    reason,
                });
                reply_error(&transfer.server_tx, transfer.server_req_id, code, message);
            }
            None => {}
        }
    }

    async fn forward_from_server(&self, server_id: u64, msg: Msg) -> bool {
        let Some(req_id) = msg.id() else {
            return false;
        };
        let target = {
            let routes = self.routes.lock().await;
            routes.owned_by_server(server_id, req_id).and_then(|root| {
                match routes.pending.get(&root) {
                    Some(Pending::Call(call)) => {
                        Some((call.leg.agent_tx.clone(), call.leg.call_id))
                    }
                    _ => None,
                }
            })
        };
        let Some((agent_tx, call_id)) = target else {
            return false;
        };
        if agent_tx.try_send(msg.with_id(call_id)).is_err() {
            debug!(req_id, "转发上行数据失败");
        }
        true
    }

    async fn cancel_call(&self, server_id: u64, req_id: u64, reason: Option<String>) {
        let reason = reason.unwrap_or_else(|| "cancelled".to_string());
        let taken = {
            let mut routes = self.routes.lock().await;
            let Some(root) = routes.owned_by_server(server_id, req_id) else {
                return;
            };
            routes.take(root)
        };
        match taken {
            Some(Pending::Call(call)) => {
                let _ = call.leg.agent_tx.try_send(Msg::Cancel {
                    id: call.leg.call_id,
                    reason: Some(reason),
                });
            }
            Some(Pending::Transfer(transfer)) => {
                let _ = transfer.source.agent_tx.try_send(Msg::Cancel {
                    id: transfer.source.call_id,
                    reason: Some(reason.clone()),
                });
                let _ = transfer.dest.agent_tx.try_send(Msg::Cancel {
                    id: transfer.dest.call_id,
                    reason: Some(reason),
                });
            }
            None => {}
        }
    }

    async fn cleanup_connection(&self, role: Role, info: &TargetInfo, sink: &Sink, conn_id: u64) {
        match role {
            Role::Agent => {
                let removed = self.remove_agent(&info.name, sink).await;
                let dropped = self.take_where(|entry| entry.touches_agent(conn_id)).await;
                for entry in dropped {
                    self.drop_entry(entry, "offline", format!("目标 {} 已断开", info.name))
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
                let dropped = self.take_where(|entry| entry.server().0 == conn_id).await;
                for entry in dropped {
                    self.drop_entry(entry, "offline", "发起方已断开".to_string())
                        .await;
                }
            }
        }
    }

    async fn take_where(&self, keep: impl Fn(&Pending) -> bool) -> Vec<Pending> {
        let mut routes = self.routes.lock().await;
        let ids: Vec<u64> = routes
            .pending
            .iter()
            .filter(|(_, entry)| keep(entry))
            .map(|(id, _)| *id)
            .collect();
        ids.into_iter().filter_map(|id| routes.take(id)).collect()
    }

    async fn drop_entry(&self, entry: Pending, code: &'static str, message: String) {
        match entry {
            Pending::Call(call) => {
                let _ = call.leg.agent_tx.try_send(Msg::Cancel {
                    id: call.leg.call_id,
                    reason: Some(message.clone()),
                });
                reply_error(&call.server_tx, call.server_req_id, code, message);
            }
            Pending::Transfer(transfer) => {
                let reason = Some(message.clone());
                let _ = transfer.source.agent_tx.try_send(Msg::Cancel {
                    id: transfer.source.call_id,
                    reason: reason.clone(),
                });
                let _ = transfer.dest.agent_tx.try_send(Msg::Cancel {
                    id: transfer.dest.call_id,
                    reason,
                });
                reply_error(&transfer.server_tx, transfer.server_req_id, code, message);
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
            let expired = self.take_where(|entry| entry.deadline() <= now).await;
            for entry in expired {
                self.drop_entry(entry, "timeout", "远端执行超时".to_string())
                    .await;
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

fn remote_code(code: &str) -> &'static str {
    match code {
        "not_found" => "not_found",
        "denied" => "denied",
        "too_large" => "too_large",
        "conflict" => "conflict",
        "timeout" => "timeout",
        "cancelled" => "cancelled",
        _ => "io",
    }
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
