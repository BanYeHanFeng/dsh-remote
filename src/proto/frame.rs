use anyhow::{anyhow, Result};

use super::msg::Msg;

pub fn encode(msg: &Msg) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    ciborium::ser::into_writer(msg, &mut buf).map_err(|e| anyhow!("CBOR 编码失败: {e}"))?;
    Ok(buf)
}

pub fn decode(bytes: &[u8]) -> Result<Msg> {
    ciborium::de::from_reader(bytes).map_err(|e| anyhow!("CBOR 解码失败: {e}"))
}
