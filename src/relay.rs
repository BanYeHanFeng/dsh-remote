use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, RwLock};

use crate::fileio;
use crate::frame::{read_frame, write_frame};
use crate::protocol::{ControlMsg, ErrorCode, Failure, OpFrame, OpRequest, RelayHeader, Role};
use crate::transport::{self, Peer, Session, Stream};

pub struct RelayOptions {
    pub listen: String,
    pub psk: Vec<u8>,
}

struct NodeHandle {
    role: Role,
    session: Session,
    tags: Vec<String>,
}

pub struct Relay {
    psk: Vec<u8>,
    nodes: RwLock<HashMap<String, NodeHandle>>,
    controllers: RwLock<HashMap<u64, mpsc::Sender<ControlMsg>>>,
    next_controller: AtomicU64,
}

impl Relay {
    fn new(psk: Vec<u8>) -> Self {
        Self {
            psk,
            nodes: RwLock::new(HashMap::new()),
            controllers: RwLock::new(HashMap::new()),
            next_controller: AtomicU64::new(1),
        }
    }

    async fn register(&self, node_id: String, role: Role, session: Session, tags: Vec<String>) {
        let mut nodes = self.nodes.write().await;
        if let Some(old) = nodes.insert(
            node_id,
            NodeHandle {
                role,
                session: session.clone(),
                tags,
            },
        ) {
            if old.session.id() != session.id() {
                let stale = old.session.clone();
                tokio::spawn(async move { stale.close().await });
            }
        }
    }

    async fn unregister(&self, node_id: &str, session: &Session) -> bool {
        let mut nodes = self.nodes.write().await;
        match nodes.get(node_id) {
            Some(handle) if handle.session.id() == session.id() => {
                nodes.remove(node_id);
                true
            }
            _ => false,
        }
    }

    async fn agent_list(&self) -> Vec<(String, Vec<String>)> {
        self.nodes
            .read()
            .await
            .iter()
            .filter(|(_, handle)| handle.role == Role::Agent)
            .map(|(id, handle)| (id.clone(), handle.tags.clone()))
            .collect()
    }

    async fn agent_session(&self, node_id: &str) -> Option<Session> {
        let nodes = self.nodes.read().await;
        match nodes.get(node_id) {
            Some(handle) if handle.role == Role::Agent => Some(handle.session.clone()),
            _ => None,
        }
    }

    async fn broadcast(&self, msg: ControlMsg) {
        let controllers: Vec<mpsc::Sender<ControlMsg>> =
            self.controllers.read().await.values().cloned().collect();
        for tx in controllers {
            if tx.try_send(msg.clone()).is_err() {
                tracing::debug!("控制端通知队列已满或已断开，跳过本条通知");
            }
        }
    }

    async fn open_op(&self, node_id: &str) -> Result<Stream, Failure> {
        let session = self.agent_session(node_id).await.ok_or_else(|| {
            Failure::new(ErrorCode::NodeOffline, format!("节点 {} 不在线", node_id))
        })?;
        session.open().await.map_err(|error| {
            Failure::new(
                ErrorCode::NodeOffline,
                format!("无法连接节点 {}：{:#}", node_id, error),
            )
        })
    }

    async fn bridge_transfer(
        &self,
        src_node: &str,
        src_path: &str,
        dst_node: &str,
        dst_path: &str,
        overwrite: bool,
    ) -> Result<crate::protocol::TransferDone, Failure> {
        let mut dl = self.open_op(src_node).await?;
        write_frame(
            &mut dl,
            &OpRequest::Download {
                remote_path: src_path.to_string(),
            },
        )
        .await
        .map_err(|e| Failure::new(ErrorCode::IoError, format!("向源节点发送请求失败：{:#}", e)))?;
        let ack: OpFrame = read_frame(&mut dl).await.map_err(|e| {
            Failure::new(ErrorCode::IoError, format!("读取源节点响应失败：{:#}", e))
        })?;
        let (size, mode) = match ack {
            OpFrame::Ack { size, mode } => (size, mode),
            OpFrame::Err { code, message } => return Err(Failure { code, message }),
            _ => return Err(crate::protocol::internal("源节点返回了意料之外的响应帧")),
        };

        let mut up = self.open_op(dst_node).await?;
        write_frame(
            &mut up,
            &OpRequest::Upload {
                remote_path: dst_path.to_string(),
                size,
                mode,
                overwrite,
            },
        )
        .await
        .map_err(|e| {
            Failure::new(
                ErrorCode::IoError,
                format!("向目标节点发送请求失败：{:#}", e),
            )
        })?;
        match read_frame::<_, OpFrame>(&mut up).await.map_err(|e| {
            Failure::new(ErrorCode::IoError, format!("读取目标节点响应失败：{:#}", e))
        })? {
            OpFrame::Ack { .. } => {}
            other => {
                return Err(other.into_result().err().unwrap_or_else(|| {
                    crate::protocol::internal("目标节点返回了意料之外的响应帧")
                }))
            }
        }

        let sha = fileio::write_hashed_copy(&mut dl, &mut up, size).await?;
        let source: OpFrame = read_frame(&mut dl).await.map_err(|e| {
            Failure::new(ErrorCode::IoError, format!("读取源节点响应失败：{:#}", e))
        })?;
        if let OpFrame::Err { code, message } = source {
            return Err(Failure { code, message });
        }
        let done = read_done(&mut up).await?;
        if done.sha256 != sha {
            return Err(Failure::new(
                ErrorCode::ChecksumMismatch,
                "源节点与中继的校验值不一致",
            ));
        }
        Ok(done)
    }
}

async fn read_done(stream: &mut Stream) -> Result<crate::protocol::TransferDone, Failure> {
    let frame: OpFrame = read_frame(stream)
        .await
        .map_err(|e| Failure::new(ErrorCode::IoError, format!("读取目标节点响应失败：{:#}", e)))?;
    frame.into_result()
}

pub async fn run(opts: RelayOptions) -> Result<()> {
    let listener = transport::listen(&opts.listen).await?;
    tracing::info!(listen = %opts.listen, "中继已启动");
    let relay = Arc::new(Relay::new(opts.psk));

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(parts) => parts,
            Err(error) => {
                tracing::warn!(error = %error, "接受连接失败");
                continue;
            }
        };
        let relay = relay.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_conn(relay, stream, peer).await {
                tracing::warn!(peer = %peer, error = %format!("{error:#}"), "中继连接处理结束");
            }
        });
    }
}

async fn handle_conn(
    relay: Arc<Relay>,
    stream: tokio::net::TcpStream,
    peer: SocketAddr,
) -> Result<()> {
    let (session, info) =
        transport::accept(stream, &relay.psk, &[Role::Agent, Role::Controller]).await?;
    let result = serve_session(relay, session.clone(), info, peer).await;
    session.close().await;
    result
}

async fn serve_session(
    relay: Arc<Relay>,
    session: Session,
    info: Peer,
    peer: SocketAddr,
) -> Result<()> {
    let mut control = session.accept().await.context("读取控制流失败")?;
    read_frame::<_, ControlMsg>(&mut control)
        .await
        .context("读取控制流问候失败")?;
    match info.role {
        Role::Agent => {
            relay
                .register(
                    info.node_id.clone(),
                    Role::Agent,
                    session.clone(),
                    info.tags.clone(),
                )
                .await;
            tracing::info!(node = %info.node_id, peer = %peer, "被控端已接入");
            relay
                .broadcast(ControlMsg::NodeOnline {
                    node_id: info.node_id.clone(),
                    tags: info.tags,
                })
                .await;
            let accept = tokio::spawn(accept_loop(
                relay.clone(),
                session.clone(),
                info.node_id.clone(),
            ));
            control_loop(&session, control).await;
            accept.abort();
            if relay.unregister(&info.node_id, &session).await {
                tracing::info!(node = %info.node_id, "被控端已离线");
                relay
                    .broadcast(ControlMsg::NodeOffline {
                        node_id: info.node_id.clone(),
                    })
                    .await;
            }
            session.close().await;
        }
        Role::Controller => {
            serve_controller(relay, &info.node_id, session, control, peer).await;
        }
    }
    Ok(())
}

async fn control_loop(session: &Session, mut control: Stream) {
    tokio::select! {
        _ = async {
            while read_frame::<_, ControlMsg>(&mut control).await.is_ok() {}
        } => {}
        _ = session.closed() => {}
    }
}

async fn accept_loop(relay: Arc<Relay>, session: Session, node_id: String) {
    loop {
        let stream = match session.accept().await {
            Ok(stream) => stream,
            Err(_) => return,
        };
        tracing::debug!(node = %node_id, "收到业务流");
        let relay = relay.clone();
        tokio::spawn(async move {
            if let Err(error) = route(relay, stream).await {
                tracing::debug!(error = %format!("{error:#}"), "转发业务流失败");
            }
        });
    }
}

async fn route(relay: Arc<Relay>, mut stream: Stream) -> Result<()> {
    let header: RelayHeader = read_frame(&mut stream)
        .await
        .context("读取中继流头部失败")?;
    tracing::debug!(target = %header.target, "中继流准备转发");
    let session = match relay.agent_session(&header.target).await {
        Some(session) => session,
        None => {
            let failure = Failure::new(
                ErrorCode::NodeOffline,
                format!("节点 {} 不在线", header.target),
            );
            let _ = write_frame(&mut stream, &OpFrame::err(&failure)).await;
            return Ok(());
        }
    };
    let mut target = session.open().await.context("连接目标节点失败")?;
    let (uploaded, downloaded) = tokio::io::copy_bidirectional(&mut stream, &mut target)
        .await
        .context("双向转发失败")?;
    tracing::debug!(target = %header.target, uploaded, downloaded, "中继流转发结束");
    Ok(())
}

async fn serve_controller(
    relay: Arc<Relay>,
    node_id: &str,
    session: Session,
    control: Stream,
    peer: SocketAddr,
) {
    let (mut recv, mut send) = tokio::io::split(control);
    let id = relay.next_controller.fetch_add(1, Ordering::Relaxed);
    let (tx, mut rx) = mpsc::channel::<ControlMsg>(256);
    relay.controllers.write().await.insert(id, tx.clone());
    tracing::info!(controller = %node_id, peer = %peer, "控制端已接入");

    for (agent, tags) in relay.agent_list().await {
        if tx
            .send(ControlMsg::NodeOnline {
                node_id: agent,
                tags,
            })
            .await
            .is_err()
        {
            break;
        }
    }

    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if write_frame(&mut send, &msg).await.is_err() {
                return;
            }
        }
        let _ = send.shutdown().await;
    });
    let accept = tokio::spawn(accept_loop(
        relay.clone(),
        session.clone(),
        node_id.to_string(),
    ));

    loop {
        tokio::select! {
            incoming = read_frame::<_, ControlMsg>(&mut recv) => match incoming {
                Ok(ControlMsg::BridgeTransfer {
                    req_id,
                    src_node,
                    src_path,
                    dst_node,
                    dst_path,
                    overwrite,
                }) => {
                    let relay = relay.clone();
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        let result = relay
                            .bridge_transfer(&src_node, &src_path, &dst_node, &dst_path, overwrite)
                            .await;
                        let _ = tx.send(ControlMsg::BridgeResult { req_id, result }).await;
                    });
                }
                Ok(_) => {}
                Err(_) => break,
            },
            _ = session.closed() => break,
        }
    }

    relay.controllers.write().await.remove(&id);
    accept.abort();
    writer.abort();
    tracing::info!(controller = %node_id, "控制端已断开");
}
