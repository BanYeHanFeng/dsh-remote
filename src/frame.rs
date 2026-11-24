use anyhow::{bail, Context, Result};
use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

pub async fn write_frame<W, T>(io: &mut W, value: &T) -> Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let payload = serde_json::to_vec(value).context("序列化帧失败")?;
    let len = u32::try_from(payload.len()).context("帧长度超出上限")?;
    io.write_all(&len.to_be_bytes()).await?;
    io.write_all(&payload).await?;
    io.flush().await?;
    Ok(())
}

pub async fn read_frame<R, T>(io: &mut R) -> Result<T>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let payload = read_frame_bytes(io).await?;
    serde_json::from_slice(&payload).context("解析帧失败")
}

pub async fn read_frame_bytes<R>(io: &mut R) -> Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut len = [0u8; 4];
    io.read_exact(&mut len).await.context("读取帧长度失败")?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME_BYTES {
        bail!("帧长度 {} 超出上限", len);
    }
    let mut payload = vec![0u8; len];
    io.read_exact(&mut payload)
        .await
        .context("读取帧内容失败")?;
    Ok(payload)
}
