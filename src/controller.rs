use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};

use crate::frame::{read_frame, write_frame};
use crate::ipc::{IpcEvent, IpcRequest, NodeInfo};
use crate::protocol::{ControlMsg, ErrorCode, Failure, RelayHeader, Role, TransferDone};
use crate::ops;
use crate::transport::{self, Stream};

pub struct ControllerOptions {
    pub listen: Option<String>,
    pub relay: Option<String>,
    pub relay_pin: Option<String>,
    pub socket: PathBuf,
    pub mcp_http: Option<String>,
    pub psk: Vec<u8>,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
}

struct NodeHandle {
    conn: quinn::Connection,
    tags: Vec<String>,
    via_relay: bool,
    since: Instant,
}

pub struct RelayLink {
    send: Mutex<quinn::SendStream>,
    pending: Mutex<HashMap<u64, oneshot::Sender<ControlMsg>>>,
    seq: AtomicU64,
}

impl RelayLink {
    pub async fn request(&self, build: impl FnOnce(u64) -> ControlMsg) -> Result<TransferDone, Failure> {
        let req_id = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(req_id, tx);
        let msg = build(req_id);
        let mut send = self.send.lock().await;
        let written = write_frame(&mut *send, &msg).await;
        drop(send);
        if let Err(error) = written {
            self.pending.lock().await.remove(&req_id);
            return Err(Failure::new(
                ErrorCode::IoError,
                format!("向中继发送请求失败：{:#}", error),
            ));
        }
        match rx.await {
            Ok(ControlMsg::BridgeResult { result, .. }) => result,
            Ok(_) => Err(Failure::new(ErrorCode::Internal, "中继返回了意料之外的响应")),
            Err(_) => Err(Failure::new(ErrorCode::IoError, "与中继的连接已断开")),
        }
    }
}

pub struct Controller {
    psk: Vec<u8>,
    nodes: RwLock<HashMap<String, NodeHandle>>,
    relay: RwLock<Option<Arc<RelayLink>>>,
}

impl Controller {
    fn new(psk: Vec<u8>) -> Self {
        Self {
            psk,
            nodes: RwLock::new(HashMap::new()),
            relay: RwLock::new(None),
        }
    }

    pub async fn register(&self, node_id: String, conn: quinn::Connection, tags: Vec<String>, via_relay: bool) {
        let mut nodes = self.nodes.write().await;
        if let Some(old) = nodes.insert(
            node_id,
            NodeHandle {
                conn: conn.clone(),
                tags,
                via_relay,
                since: Instant::now(),
            },
        ) {
            if old.conn.stable_id() != conn.stable_id() && !old.via_relay {
                old.conn.close(0u32.into(), b"replaced");
            }
        }
    }

    pub async fn unregister(&self, node_id: &str, conn: &quinn::Connection, via_relay: bool) {
        let mut nodes = self.nodes.write().await;
        if let Some(handle) = nodes.get(node_id) {
            if handle.via_relay == via_relay && handle.conn.stable_id() == conn.stable_id() {
                nodes.remove(node_id);
            }
        }
    }

    pub async fn relay_link(&self) -> Option<Arc<RelayLink>> {
        self.relay.read().await.clone()
    }

    pub async fn route(&self, node: &str) -> Result<Stream, Failure> {
        let entry = {
            let nodes = self.nodes.read().await;
            nodes.get(node).map(|handle| (handle.conn.clone(), handle.via_relay))
        };
        let (conn, via_relay) = entry.ok_or_else(|| {
            Failure::new(ErrorCode::NodeOffline, format!("节点 {} 不在线", node))
        })?;
        tracing::debug!(node = %node, via_relay, "打开业务流");
        let mut stream = transport::open(&conn).await.map_err(|error| {
            Failure::new(
                ErrorCode::NodeOffline,
                format!("无法与节点 {} 建立流：{:#}", node, error),
            )
        })?;
        if via_relay {
            write_frame(
                &mut stream,
                &RelayHeader {
                    target: node.to_string(),
                },
            )
            .await
            .map_err(|error| {
                Failure::new(ErrorCode::IoError, format!("发送路由头部失败：{:#}", error))
            })?;
        }
        Ok(stream)
    }

    pub async fn same_relay(&self, a: &str, b: &str) -> bool {
        let nodes = self.nodes.read().await;
        match (nodes.get(a), nodes.get(b)) {
            (Some(left), Some(right)) => {
                left.via_relay && right.via_relay && left.conn.stable_id() == right.conn.stable_id()
            }
            _ => false,
        }
    }

    pub async fn drop_relay_nodes(&self, conn: &quinn::Connection) {
        let mut nodes = self.nodes.write().await;
        let before = nodes.len();
        nodes.retain(|_, handle| !(handle.via_relay && handle.conn.stable_id() == conn.stable_id()));
        let dropped = before - nodes.len();
        if dropped > 0 {
            tracing::info!(count = dropped, "中继断开，已移除该中继上的节点");
        }
    }

    pub async fn list(&self, tag: Option<&str>) -> Vec<NodeInfo> {
        let nodes = self.nodes.read().await;
        let mut list: Vec<NodeInfo> = nodes
            .iter()
            .filter(|(_, handle)| match tag {
                Some(tag) => handle.tags.iter().any(|t| t == tag),
                None => true,
            })
            .map(|(id, handle)| NodeInfo {
                node_id: id.clone(),
                tags: handle.tags.clone(),
                via: if handle.via_relay { "中继" } else { "直连" }.to_string(),
                online_secs: handle.since.elapsed().as_secs(),
            })
            .collect();
        list.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        list
    }
}

pub async fn run(opts: ControllerOptions) -> Result<()> {
    let ctl = Arc::new(Controller::new(opts.psk.clone()));

    if let Some(listen) = &opts.listen {
        let server = transport::server_endpoint(listen, opts.cert.as_deref(), opts.key.as_deref())?;
        tracing::info!(
            listen = %listen,
            fingerprint = %server.fingerprint,
            "控制端 QUIC 监听已启动"
        );
        let ctl = ctl.clone();
        tokio::spawn(async move { accept_agents(ctl, server.endpoint).await });
    }

    if let Some(relay_addr) = opts.relay.clone() {
        let pin = opts.relay_pin.clone();
        let ctl = ctl.clone();
        tokio::spawn(async move { relay_loop(ctl, relay_addr, pin).await });
    }

    if let Some(addr) = opts.mcp_http.clone() {
        let socket = opts.socket.clone();
        tokio::spawn(async move {
            if let Err(error) = crate::mcp::serve_http(&addr, socket).await {
                tracing::error!(error = %format!("{error:#}"), "MCP HTTP 服务退出");
            }
        });
    }

    serve_ipc(ctl, opts.socket).await
}

async fn accept_agents(ctl: Arc<Controller>, endpoint: quinn::Endpoint) {
    while let Some(incoming) = endpoint.accept().await {
        let ctl = ctl.clone();
        tokio::spawn(async move {
            let conn = match incoming.await {
                Ok(conn) => conn,
                Err(error) => {
                    tracing::warn!(error = %error, "QUIC 握手失败");
                    return;
                }
            };
            let psk = ctl.psk.clone();
            let (_, node_id, tags, control) =
                match transport::serve_auth(&conn, &psk, &[Role::Agent]).await {
                    Ok(parts) => parts,
                    Err(error) => {
                        tracing::warn!(error = %format!("{error:#}"), "被控端认证失败");
                        return;
                    }
                };
            ctl.register(node_id.clone(), conn.clone(), tags, false).await;
            tracing::info!(node = %node_id, peer = %conn.remote_address(), "被控端已接入");
            control_loop(&conn, control).await;
            ctl.unregister(&node_id, &conn, false).await;
            tracing::info!(node = %node_id, "被控端已离线");
            conn.close(0u32.into(), b"bye");
        });
    }
}

async fn control_loop(conn: &quinn::Connection, mut control: Stream) {
    tokio::select! {
        _ = async {
            loop {
                match read_frame::<_, ControlMsg>(&mut control).await {
                    Ok(ControlMsg::Ping) => {
                        if write_frame(&mut control, &ControlMsg::Pong).await.is_err() {
                            return;
                        }
                    }
                    Ok(_) => {}
                    Err(_) => return,
                }
            }
        } => {}
        _ = conn.closed() => {}
    }
}

async fn relay_loop(ctl: Arc<Controller>, addr: String, pin: Option<String>) {
    let endpoint = match transport::client_endpoint(pin.as_deref()) {
        Ok(endpoint) => endpoint,
        Err(error) => {
            tracing::error!(error = %format!("{error:#}"), "初始化中继客户端失败");
            return;
        }
    };
    let mut backoff = 1u64;
    loop {
        match relay_session(&ctl, &endpoint, &addr).await {
            Ok(()) => {
                tracing::info!(relay = %addr, "与中继的会话已结束");
                backoff = 1;
            }
            Err(error) => {
                tracing::warn!(relay = %addr, error = %format!("{error:#}"), "连接中继失败");
                backoff = (backoff * 2).min(30);
            }
        }
        tokio::time::sleep(Duration::from_secs(backoff)).await;
    }
}

async fn relay_session(ctl: &Arc<Controller>, endpoint: &quinn::Endpoint, addr: &str) -> Result<()> {
    let peer = transport::resolve(addr).await?;
    let conn = endpoint
        .connect(peer, crate::protocol::SERVER_NAME)
        .with_context(|| format!("发起连接失败：{}", addr))?
        .await
        .context("QUIC 握手失败")?;
    let control = transport::client_auth(
        &conn,
        &ctl.psk,
        Role::Controller,
        &format!("controller-{}", std::process::id()),
        &[],
    )
    .await?;
    tracing::info!(relay = %addr, "已接入中继");

    let (mut recv, send) = control.into_inner();
    {
        let mut slot = ctl.relay.write().await;
        *slot = Some(Arc::new(RelayLink {
            send: Mutex::new(send),
            pending: Mutex::new(HashMap::new()),
            seq: AtomicU64::new(0),
        }));
    }

    let result = loop {
        let msg: ControlMsg = match read_frame(&mut recv).await {
            Ok(msg) => msg,
            Err(error) => break Err(error),
        };
        match msg {
            ControlMsg::NodeOnline { node_id, tags } => {
                ctl.register(node_id, conn.clone(), tags, true).await;
            }
            ControlMsg::NodeOffline { node_id } => {
                ctl.unregister(&node_id, &conn, true).await;
            }
            ControlMsg::BridgeResult { req_id, result } => {
                if let Some(link) = ctl.relay_link().await {
                    if let Some(tx) = link.pending.lock().await.remove(&req_id) {
                        let _ = tx.send(ControlMsg::BridgeResult { req_id, result });
                    }
                }
            }
            ControlMsg::Ping => {}
            _ => {}
        }
    };

    if let Some(link) = ctl.relay.write().await.take() {
        link.pending.lock().await.clear();
    }
    ctl.drop_relay_nodes(&conn).await;
    conn.close(0u32.into(), b"bye");
    result.map_err(|error| anyhow::anyhow!("{:#}", error))
}

async fn serve_ipc(ctl: Arc<Controller>, socket: PathBuf) -> Result<()> {
    if let Some(dir) = socket.parent() {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("创建 socket 目录失败：{}", dir.display()))?;
    }
    if socket.exists() {
        let _ = std::fs::remove_file(&socket);
    }
    let listener = UnixListener::bind(&socket)
        .with_context(|| format!("监听本地 socket 失败：{}", socket.display()))?;
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600));
    }
    tracing::info!(socket = %socket.display(), "本地接口已就绪");

    loop {
        let (stream, _) = listener.accept().await.context("接受本地连接失败")?;
        let ctl = ctl.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_ipc(ctl, stream).await {
                tracing::debug!(error = %format!("{error:#}"), "本地会话结束");
            }
        });
    }
}

async fn handle_ipc(ctl: Arc<Controller>, stream: UnixStream) -> Result<()> {
    let (mut reader, mut writer) = stream.into_split();
    let (request_tx, mut request_rx) = mpsc::channel::<IpcRequest>(4);
    let read_task = tokio::spawn(async move {
        loop {
            match read_frame::<_, IpcRequest>(&mut reader).await {
                Ok(request) => {
                    if request_tx.send(request).await.is_err() {
                        return;
                    }
                }
                Err(_) => return,
            }
        }
    });

    while let Some(request) = request_rx.recv().await {
        let (tx, mut rx) = mpsc::channel::<IpcEvent>(256);
        let ctl = ctl.clone();
        let op = tokio::spawn(async move { ops::run(&ctl, request, tx).await });

        loop {
            tokio::select! {
                event = rx.recv() => match event {
                    Some(event) => {
                        let terminal = event.is_terminal();
                        if write_frame(&mut writer, &event).await.is_err() {
                            op.abort();
                            return Ok(());
                        }
                        if terminal {
                            break;
                        }
                    }
                    None => break,
                },
                next = request_rx.recv() => match next {
                    Some(_) => tracing::warn!("本地调用方在执行期间又发送了请求，已忽略"),
                    None => {
                        tracing::debug!("本地调用方已断开，正在中止进行中的操作");
                        op.abort();
                        return Ok(());
                    }
                },
            }
        }
        let _ = op.await;
        rx.close();
        while rx.recv().await.is_some() {}
    }

    read_task.abort();
    Ok(())
}
