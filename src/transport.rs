use std::future::poll_fn;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::{anyhow, bail, Context as _, Result};
use chacha20poly1305::aead::{AeadInOut, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use hmac::{Hmac, Mac};
use rand::RngExt;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch, Mutex};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use yamux::{Connection, Mode};

use crate::protocol::{Role, PROTO_VERSION};

pub trait Io: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T> Io for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

pub type Stream = Box<dyn Io>;

/// 本地节点与远端节点共用的内存流
pub fn local_pair() -> (Stream, Stream) {
    let (a, b) = tokio::io::duplex(LOCAL_BUFFER);
    (Box::new(a), Box::new(b))
}

type Mux = Connection<Compat<tokio::io::Join<DuplexStream, RecordWriter>>>;

const MAGIC: [u8; 4] = *b"DSHR";
const MAX_HANDSHAKE: usize = 64 * 1024;
const MAX_RECORD: usize = 64 * 1024;
const MAX_PAYLOAD: usize = MAX_RECORD - 16;
const PLAIN_BUFFER: usize = 128 * 1024;
const LOCAL_BUFFER: usize = 64 * 1024;
const STREAM_QUEUE: usize = 16;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const KEEPALIVE_IDLE: Duration = Duration::from_secs(30);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Serialize, Deserialize)]
struct Hello {
    version: u32,
    role: Role,
    node_id: String,
    tags: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct Ack {
    version: u32,
}

pub struct Peer {
    pub role: Role,
    pub node_id: String,
    pub tags: Vec<String>,
}

struct Inner {
    conn: Arc<std::sync::Mutex<Mux>>,
    incoming: Mutex<mpsc::Receiver<Stream>>,
    closed: watch::Sender<bool>,
    nudge: mpsc::Sender<()>,
    peer: SocketAddr,
    id: u64,
}

#[derive(Clone)]
pub struct Session {
    inner: Arc<Inner>,
}

impl Session {
    pub fn id(&self) -> u64 {
        self.inner.id
    }

    pub fn peer(&self) -> SocketAddr {
        self.inner.peer
    }

    pub async fn open(&self) -> Result<Stream> {
        let stream = poll_fn(|cx| {
            self.inner
                .conn
                .lock()
                .expect("yamux 连接锁")
                .poll_new_outbound(cx)
        })
        .await
        .context("打开业务流失败")?;
        let _ = self.inner.nudge.try_send(());
        Ok(Box::new(stream.compat()) as Stream)
    }

    pub async fn accept(&self) -> Result<Stream> {
        self.inner
            .incoming
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| anyhow!("连接已关闭"))
    }

    pub async fn closed(&self) {
        let mut rx = self.inner.closed.subscribe();
        if *rx.borrow_and_update() {
            return;
        }
        let _ = rx.changed().await;
    }

    pub async fn close(&self) {
        let _ = poll_fn(|cx| self.inner.conn.lock().expect("yamux 连接锁").poll_close(cx)).await;
    }
}

pub async fn listen(addr: &str) -> Result<TcpListener> {
    TcpListener::bind(addr)
        .await
        .with_context(|| format!("监听 {} 失败", addr))
}

pub async fn connect(
    addr: &str,
    psk: &[u8],
    role: Role,
    node_id: &str,
    tags: &[String],
) -> Result<Session> {
    let stream = tokio::time::timeout(HANDSHAKE_TIMEOUT, TcpStream::connect(addr))
        .await
        .with_context(|| format!("连接 {} 超时", addr))?
        .with_context(|| format!("连接 {} 失败", addr))?;
    tune(&stream);
    tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        handshake_client(stream, psk, role, node_id, tags),
    )
    .await
    .with_context(|| format!("与 {} 握手超时", addr))?
}

pub async fn accept(stream: TcpStream, psk: &[u8], allowed: &[Role]) -> Result<(Session, Peer)> {
    tune(&stream);
    tokio::time::timeout(HANDSHAKE_TIMEOUT, handshake_server(stream, psk, allowed))
        .await
        .with_context(|| "握手超时")?
}

async fn handshake_client(
    mut stream: TcpStream,
    psk: &[u8],
    role: Role,
    node_id: &str,
    tags: &[String],
) -> Result<Session> {
    let peer = stream.peer_addr().context("读取对端地址失败")?;
    let mut client_nonce = [0u8; 16];
    rand::rng().fill(&mut client_nonce);

    let key = hello_key(psk);
    let hello = Hello {
        version: PROTO_VERSION,
        role,
        node_id: node_id.to_string(),
        tags: tags.to_vec(),
    };
    let mut payload = serde_json::to_vec(&hello).context("序列化握手信息失败")?;
    seal(&key, &short_nonce(&client_nonce), b"hello", &mut payload)?;

    stream.write_all(&MAGIC).await?;
    stream.write_all(&client_nonce).await?;
    write_blob(&mut stream, &payload).await?;

    let mut tag = [0u8; 1];
    stream
        .read_exact(&mut tag)
        .await
        .context("读取握手响应失败")?;
    match tag[0] {
        0 => {}
        1 => {
            let message = read_blob(&mut stream, MAX_HANDSHAKE).await?;
            bail!("{}", String::from_utf8_lossy(&message));
        }
        other => bail!("握手响应非法：{}", other),
    }

    let mut server_nonce = [0u8; 16];
    stream
        .read_exact(&mut server_nonce)
        .await
        .context("读取服务端随机数失败")?;
    let mut ack = read_blob(&mut stream, MAX_HANDSHAKE).await?;
    open(&key, &short_nonce(&server_nonce), b"ack", &mut ack)
        .context("服务端身份校验失败，预共享密钥可能不匹配")?;
    let ack: Ack = serde_json::from_slice(&ack).context("解析握手响应失败")?;
    if ack.version != PROTO_VERSION {
        bail!(
            "协议版本不匹配：服务端 {}，本机 {}",
            ack.version,
            PROTO_VERSION
        );
    }
    Ok(start(
        stream,
        psk,
        &client_nonce,
        &server_nonce,
        Mode::Client,
        peer,
    ))
}

async fn handshake_server(
    mut stream: TcpStream,
    psk: &[u8],
    allowed: &[Role],
) -> Result<(Session, Peer)> {
    let peer = stream.peer_addr().context("读取对端地址失败")?;
    let mut magic = [0u8; 4];
    stream
        .read_exact(&mut magic)
        .await
        .context("读取握手标识失败")?;
    if magic != MAGIC {
        bail!("对端不是 dsh-remote 客户端");
    }
    let mut client_nonce = [0u8; 16];
    stream
        .read_exact(&mut client_nonce)
        .await
        .context("读取客户端随机数失败")?;
    let mut payload = read_blob(&mut stream, MAX_HANDSHAKE).await?;

    let key = hello_key(psk);
    if open(&key, &short_nonce(&client_nonce), b"hello", &mut payload).is_err() {
        let _ = reply_error(&mut stream, "预共享密钥不匹配").await;
        bail!("预共享密钥不匹配");
    }
    let hello: Hello = serde_json::from_slice(&payload).context("解析握手信息失败")?;
    if hello.version != PROTO_VERSION {
        let message = format!(
            "协议版本不匹配：客户端 {}，服务端 {}",
            hello.version, PROTO_VERSION
        );
        let _ = reply_error(&mut stream, &message).await;
        bail!("{}", message);
    }
    if let Err(error) = crate::protocol::validate_node_id(&hello.node_id) {
        let message = format!("节点标识非法：{:#}", error);
        let _ = reply_error(&mut stream, &message).await;
        bail!("{}", message);
    }
    if !allowed.contains(&hello.role) {
        let _ = reply_error(&mut stream, "该角色不允许接入本服务").await;
        bail!("该角色不允许接入本服务");
    }

    let mut server_nonce = [0u8; 16];
    rand::rng().fill(&mut server_nonce);
    let mut ack = serde_json::to_vec(&Ack {
        version: PROTO_VERSION,
    })
    .context("序列化握手响应失败")?;
    seal(&key, &short_nonce(&server_nonce), b"ack", &mut ack)?;
    stream.write_all(&[0u8]).await?;
    stream.write_all(&server_nonce).await?;
    write_blob(&mut stream, &ack).await?;

    let info = Peer {
        role: hello.role,
        node_id: hello.node_id,
        tags: hello.tags,
    };
    let session = start(
        stream,
        psk,
        &client_nonce,
        &server_nonce,
        Mode::Server,
        peer,
    );
    Ok((session, info))
}

fn start(
    stream: TcpStream,
    psk: &[u8],
    client_nonce: &[u8; 16],
    server_nonce: &[u8; 16],
    mode: Mode,
    peer: SocketAddr,
) -> Session {
    let (client_to_server, server_to_client) = session_keys(psk, client_nonce, server_nonce);
    let (send_key, recv_key) = match mode {
        Mode::Client => (client_to_server, server_to_client),
        Mode::Server => (server_to_client, client_to_server),
    };

    let (read_half, write_half) = stream.into_split();
    let (plain_tx, plain_rx) = tokio::io::duplex(PLAIN_BUFFER);
    tokio::spawn(async move {
        if let Err(error) = pump_records(read_half, aead_key(&recv_key), plain_tx).await {
            tracing::debug!(error = %format!("{error:#}"), "加密链路读取结束");
        }
    });

    let io = tokio::io::join(plain_rx, RecordWriter::new(write_half, aead_key(&send_key)));
    let conn = Arc::new(std::sync::Mutex::new(Connection::new(
        io.compat(),
        yamux::Config::default(),
        mode,
    )));
    let (in_tx, in_rx) = mpsc::channel::<Stream>(STREAM_QUEUE);
    let (closed_tx, _closed_rx) = watch::channel(false);
    let (nudge_tx, mut nudge_rx) = mpsc::channel::<()>(1);
    let driver = conn.clone();
    let control = closed_tx.clone();
    tokio::spawn(async move {
        loop {
            let next = tokio::select! {
                _ = nudge_rx.recv() => continue,
                next = poll_fn(|cx| {
                    driver
                        .lock()
                        .expect("yamux 连接锁")
                        .poll_next_inbound(cx)
                }) => next,
            };
            match next {
                Some(Ok(stream)) => {
                    if in_tx
                        .send(Box::new(stream.compat()) as Stream)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Some(Err(error)) => {
                    tracing::debug!(error = %error, "yamux 连接中断");
                    break;
                }
                None => break,
            }
        }
        let _ = control.send(true);
    });

    Session {
        inner: Arc::new(Inner {
            conn,
            incoming: Mutex::new(in_rx),
            closed: closed_tx,
            nudge: nudge_tx,
            peer,
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
        }),
    }
}

fn tune(stream: &TcpStream) {
    let _ = stream.set_nodelay(true);
    let keepalive = TcpKeepalive::new()
        .with_time(KEEPALIVE_IDLE)
        .with_interval(KEEPALIVE_INTERVAL);
    if let Err(error) = SockRef::from(stream).set_tcp_keepalive(&keepalive) {
        tracing::debug!(error = %error, "设置 TCP keepalive 失败");
    }
}

async fn write_blob<W>(io: &mut W, bytes: &[u8]) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let len = u32::try_from(bytes.len()).context("握手消息过长")?;
    io.write_all(&len.to_be_bytes()).await?;
    io.write_all(bytes).await?;
    io.flush().await?;
    Ok(())
}

async fn read_blob<R>(io: &mut R, limit: usize) -> Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut head = [0u8; 4];
    io.read_exact(&mut head)
        .await
        .context("读取握手消息长度失败")?;
    let len = u32::from_be_bytes(head) as usize;
    if len == 0 || len > limit {
        bail!("握手消息长度非法：{}", len);
    }
    let mut payload = vec![0u8; len];
    io.read_exact(&mut payload)
        .await
        .context("读取握手消息失败")?;
    Ok(payload)
}

async fn reply_error(stream: &mut TcpStream, message: &str) -> Result<()> {
    stream.write_all(&[1u8]).await?;
    write_blob(stream, message.as_bytes()).await
}

fn mac(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = <Hmac<Sha256> as KeyInit>::new_from_slice(key).expect("HMAC 接受任意长度密钥");
    for part in parts {
        hasher.update(part);
    }
    let mut digest = [0u8; 32];
    digest.copy_from_slice(&hasher.finalize().into_bytes());
    digest
}

fn hello_key(psk: &[u8]) -> ChaCha20Poly1305 {
    aead_key(&mac(psk, &[b"dsh-remote/1 hello"]))
}

fn session_keys(
    psk: &[u8],
    client_nonce: &[u8; 16],
    server_nonce: &[u8; 16],
) -> ([u8; 32], [u8; 32]) {
    let prk = mac(psk, &[b"dsh-remote/1 session", client_nonce, server_nonce]);
    (mac(&prk, &[b"c2s"]), mac(&prk, &[b"s2c"]))
}

fn aead_key(bytes: &[u8; 32]) -> ChaCha20Poly1305 {
    ChaCha20Poly1305::new(&Key::from(*bytes))
}

fn short_nonce(bytes: &[u8; 16]) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&bytes[..12]);
    nonce
}

fn counter_nonce(counter: u64) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce[4..].copy_from_slice(&counter.to_be_bytes());
    nonce
}

fn seal(key: &ChaCha20Poly1305, nonce: &[u8; 12], aad: &[u8], payload: &mut Vec<u8>) -> Result<()> {
    key.encrypt_in_place(&Nonce::from(*nonce), aad, payload)
        .map_err(|_| anyhow!("加密握手消息失败"))
}

fn open(key: &ChaCha20Poly1305, nonce: &[u8; 12], aad: &[u8], payload: &mut Vec<u8>) -> Result<()> {
    key.decrypt_in_place(&Nonce::from(*nonce), aad, payload)
        .map_err(|_| anyhow!("解密握手消息失败"))
}

async fn pump_records(
    mut read_half: OwnedReadHalf,
    key: ChaCha20Poly1305,
    mut out: DuplexStream,
) -> Result<()> {
    let mut counter = 0u64;
    let mut payload = Vec::new();
    loop {
        let mut head = [0u8; 4];
        read_half
            .read_exact(&mut head)
            .await
            .context("读取加密记录长度失败")?;
        let len = u32::from_be_bytes(head) as usize;
        if !(16..=MAX_RECORD).contains(&len) {
            bail!("加密记录长度非法：{}", len);
        }
        payload.clear();
        payload.resize(len, 0);
        read_half
            .read_exact(&mut payload)
            .await
            .context("读取加密记录失败")?;
        let nonce = counter_nonce(counter);
        counter += 1;
        key.decrypt_in_place(&Nonce::from(nonce), &head, &mut payload)
            .map_err(|_| anyhow!("解密记录失败，链路可能被篡改"))?;
        out.write_all(&payload).await.context("交付明文失败")?;
    }
}

struct RecordWriter {
    inner: OwnedWriteHalf,
    key: ChaCha20Poly1305,
    counter: u64,
    pending: Vec<u8>,
    pos: usize,
    scratch: Vec<u8>,
}

impl RecordWriter {
    fn new(inner: OwnedWriteHalf, key: ChaCha20Poly1305) -> Self {
        Self {
            inner,
            key,
            counter: 0,
            pending: Vec::new(),
            pos: 0,
            scratch: Vec::new(),
        }
    }

    fn flush_pending(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.pos < self.pending.len() {
            match Pin::new(&mut self.inner).poll_write(cx, &self.pending[self.pos..]) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
                }
                Poll::Ready(Ok(written)) => self.pos += written,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }
        self.pending.clear();
        self.pos = 0;
        Poll::Ready(Ok(()))
    }

    fn seal(&mut self, buf: &[u8]) {
        let nonce = counter_nonce(self.counter);
        self.counter += 1;
        let head = ((buf.len() + 16) as u32).to_be_bytes();
        self.scratch.clear();
        self.scratch.extend_from_slice(buf);
        self.key
            .encrypt_in_place(&Nonce::from(nonce), &head, &mut self.scratch)
            .expect("AEAD 加密不会失败");
        self.pending.clear();
        self.pending.extend_from_slice(&head);
        self.pending.extend_from_slice(&self.scratch);
        self.pos = 0;
    }
}

impl AsyncWrite for RecordWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.as_mut().get_mut();
        match this.flush_pending(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let take = buf.len().min(MAX_PAYLOAD);
        this.seal(&buf[..take]);
        let _ = this.flush_pending(cx);
        Poll::Ready(Ok(take))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.as_mut().get_mut().flush_pending(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();
        match this.flush_pending(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        }
        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}
