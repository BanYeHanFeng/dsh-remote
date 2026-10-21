use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use snow::params::NoiseParams;
use snow::{Builder, TransportState};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::key::KeyMaterial;
use crate::proto::frame::decode;
use crate::proto::Msg;

pub const NOISE_PATTERN: &str = "Noise_IKpsk2_25519_ChaChaPoly_BLAKE2s";
const TAG_LEN: usize = 16;
const MAX_NOISE_MSG: usize = 65535;
const MAX_PLAIN: usize = MAX_NOISE_MSG - TAG_LEN;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

type Cipher = Arc<Mutex<TransportState>>;

pub struct SecureConn {
    reader: Reader,
    writer: Writer,
}

impl SecureConn {
    pub async fn connect(addr: &str, key: &KeyMaterial) -> Result<Self> {
        let stream = timeout(HANDSHAKE_TIMEOUT, TcpStream::connect(addr))
            .await
            .map_err(|_| anyhow!("连接 {addr} 超时"))?
            .with_context(|| format!("连接 {addr} 失败"))?;
        stream.set_nodelay(true)?;
        let (mut read, mut write) = stream.into_split();
        let params: NoiseParams = NOISE_PATTERN
            .parse()
            .map_err(|e| anyhow!("Noise 参数错误: {e}"))?;
        let private = key.static_private();
        let public = key.public_key();
        let psk = key.psk();
        let mut handshake = Builder::new(params)
            .local_private_key(&private)
            .map_err(noise_err)?
            .remote_public_key(&public)
            .map_err(noise_err)?
            .psk(2, &psk)
            .map_err(noise_err)?
            .build_initiator()
            .map_err(noise_err)?;
        let mut out = vec![0u8; MAX_NOISE_MSG];
        let written = handshake.write_message(&[], &mut out).map_err(noise_err)?;
        write_handshake(&mut write, &out[..written]).await?;
        let reply = timeout(HANDSHAKE_TIMEOUT, read_handshake(&mut read))
            .await
            .map_err(|_| anyhow!("握手响应超时"))??;
        let mut payload = vec![0u8; MAX_NOISE_MSG];
        handshake
            .read_message(&reply, &mut payload)
            .map_err(noise_err)?;
        let state = handshake.into_transport_mode().map_err(noise_err)?;
        Ok(Self::new(read, write, state))
    }

    pub async fn accept(stream: TcpStream, key: &KeyMaterial) -> Result<Self> {
        stream.set_nodelay(true)?;
        let (mut read, mut write) = stream.into_split();
        let params: NoiseParams = NOISE_PATTERN
            .parse()
            .map_err(|e| anyhow!("Noise 参数错误: {e}"))?;
        let private = key.static_private();
        let public = key.public_key();
        let psk = key.psk();
        let mut handshake = Builder::new(params)
            .local_private_key(&private)
            .map_err(noise_err)?
            .remote_public_key(&public)
            .map_err(noise_err)?
            .psk(2, &psk)
            .map_err(noise_err)?
            .build_responder()
            .map_err(noise_err)?;
        let init = timeout(HANDSHAKE_TIMEOUT, read_handshake(&mut read))
            .await
            .map_err(|_| anyhow!("等待握手超时"))??;
        let mut payload = vec![0u8; MAX_NOISE_MSG];
        handshake
            .read_message(&init, &mut payload)
            .map_err(noise_err)?;
        let mut out = vec![0u8; MAX_NOISE_MSG];
        let written = handshake.write_message(&[], &mut out).map_err(noise_err)?;
        write_handshake(&mut write, &out[..written]).await?;
        let state = handshake.into_transport_mode().map_err(noise_err)?;
        Ok(Self::new(read, write, state))
    }

    pub fn split(self) -> (Reader, Writer) {
        (self.reader, self.writer)
    }

    pub async fn send(&mut self, msg: &Msg) -> Result<()> {
        self.writer.send(msg).await
    }

    pub async fn recv(&mut self) -> Result<Msg> {
        self.reader.recv().await
    }

    fn new(read: OwnedReadHalf, write: OwnedWriteHalf, state: TransportState) -> Self {
        let cipher = Arc::new(Mutex::new(state));
        Self {
            reader: Reader {
                half: read,
                cipher: cipher.clone(),
            },
            writer: Writer {
                half: write,
                cipher,
            },
        }
    }
}

pub struct Reader {
    half: OwnedReadHalf,
    cipher: Cipher,
}

impl Reader {
    pub async fn recv(&mut self) -> Result<Msg> {
        let mut len_buf = [0u8; 2];
        self.half
            .read_exact(&mut len_buf)
            .await
            .context("读取帧长度失败")?;
        let len = u16::from_be_bytes(len_buf) as usize;
        if !(TAG_LEN + 1..=MAX_NOISE_MSG).contains(&len) {
            bail!("密文帧长度非法: {len}");
        }
        let mut cipher = vec![0u8; len];
        self.half
            .read_exact(&mut cipher)
            .await
            .context("读取帧数据失败")?;
        let mut plain = vec![0u8; len - TAG_LEN];
        let written = {
            let mut state = self.cipher.lock().map_err(|_| anyhow!("加密状态锁失效"))?;
            state.read_message(&cipher, &mut plain).map_err(noise_err)?
        };
        plain.truncate(written);
        decode(&plain)
    }
}

pub struct Writer {
    half: OwnedWriteHalf,
    cipher: Cipher,
}

impl Writer {
    pub async fn send(&mut self, msg: &Msg) -> Result<()> {
        self.send_batch(std::slice::from_ref(msg)).await.map(|_| ())
    }

    pub async fn send_batch(&mut self, msgs: &[Msg]) -> Result<usize> {
        if msgs.is_empty() {
            return Ok(0);
        }
        let mut frames = Vec::with_capacity(msgs.len().saturating_mul(96));
        let mut body = Vec::new();
        let mut cipher = Vec::new();
        for msg in msgs {
            body.clear();
            ciborium::ser::into_writer(msg, &mut body)
                .map_err(|e| anyhow!("CBOR 编码失败: {e}"))?;
            if body.is_empty() || body.len() > MAX_PLAIN {
                bail!("消息长度非法: {} 字节", body.len());
            }
            cipher.clear();
            cipher.resize(body.len() + TAG_LEN, 0);
            let written = {
                let mut state = self.cipher.lock().map_err(|_| anyhow!("加密状态锁失效"))?;
                state.write_message(&body, &mut cipher).map_err(noise_err)?
            };
            let len = u16::try_from(written).map_err(|_| anyhow!("密文过长"))?;
            frames.extend_from_slice(&len.to_be_bytes());
            frames.extend_from_slice(&cipher[..written]);
        }
        self.half.write_all(&frames).await?;
        self.half.flush().await?;
        Ok(msgs.len())
    }

    pub async fn close(&mut self) -> Result<()> {
        self.half.shutdown().await?;
        Ok(())
    }
}

fn noise_err(err: snow::Error) -> anyhow::Error {
    anyhow!("Noise 错误: {err}")
}

async fn write_handshake<W>(writer: &mut W, data: &[u8]) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let len = u16::try_from(data.len()).map_err(|_| anyhow!("握手帧过长"))?;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(data).await?;
    writer.flush().await?;
    Ok(())
}

async fn read_handshake<R>(reader: &mut R) -> Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut len_buf = [0u8; 2];
    reader.read_exact(&mut len_buf).await?;
    let len = u16::from_be_bytes(len_buf) as usize;
    if len == 0 || len > MAX_NOISE_MSG {
        bail!("握手帧长度非法: {len}");
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}
