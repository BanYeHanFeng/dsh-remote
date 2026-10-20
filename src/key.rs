use anyhow::{bail, Context, Result};
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
use base64::Engine;
use hkdf::Hkdf;
use sha2::{Digest, Sha256};
use x25519_dalek::{PublicKey, StaticSecret};

const MIN_KEY_LEN: usize = 16;
const KEY_LEN: usize = 32;

#[derive(Clone)]
pub struct KeyMaterial {
    secret: [u8; KEY_LEN],
}

impl std::fmt::Debug for KeyMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KeyMaterial({})", self.fingerprint())
    }
}

impl KeyMaterial {
    pub fn parse(raw: &str) -> Result<Self> {
        let raw = raw.trim();
        if raw.is_empty() {
            bail!("Key 为空");
        }
        let bytes = decode_key(raw).context("Key 需要是 base64 或 hex")?;
        if bytes.len() < MIN_KEY_LEN {
            bail!(
                "Key 只有 {} 字节，至少需要 {} 字节",
                bytes.len(),
                MIN_KEY_LEN
            );
        }
        Ok(Self::from_bytes(&bytes))
    }

    pub fn from_bytes(input: &[u8]) -> Self {
        if input.len() == KEY_LEN {
            let mut secret = [0u8; KEY_LEN];
            secret.copy_from_slice(input);
            return Self { secret };
        }
        let hk = Hkdf::<Sha256>::new(Some(b"dsh-remote/v1/room"), input);
        let mut secret = [0u8; KEY_LEN];
        hk.expand(b"room", &mut secret)
            .expect("32 字节派生不会失败");
        Self { secret }
    }

    pub fn generate() -> Self {
        let raw: [u8; KEY_LEN] = rand::random();
        Self::from_bytes(&raw)
    }

    pub fn encode(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.secret)
    }

    pub fn fingerprint(&self) -> String {
        let digest = Sha256::digest(self.secret);
        hex::encode(&digest[..4])
    }

    pub fn static_private(&self) -> [u8; KEY_LEN] {
        self.derive(b"static-private")
    }

    pub fn psk(&self) -> [u8; KEY_LEN] {
        self.derive(b"psk")
    }

    pub fn public_key(&self) -> [u8; KEY_LEN] {
        let secret = StaticSecret::from(self.static_private());
        PublicKey::from(&secret).to_bytes()
    }

    fn derive(&self, label: &[u8]) -> [u8; KEY_LEN] {
        let hk = Hkdf::<Sha256>::new(Some(b"dsh-remote/v1"), &self.secret);
        let mut out = [0u8; KEY_LEN];
        hk.expand(label, &mut out).expect("32 字节派生不会失败");
        out
    }
}

fn decode_key(raw: &str) -> Result<Vec<u8>> {
    if raw.len() == 64 && raw.chars().all(|c| c.is_ascii_hexdigit()) {
        return Ok(hex::decode(raw)?);
    }
    for engine in [&URL_SAFE_NO_PAD, &STANDARD_NO_PAD, &URL_SAFE, &STANDARD] {
        if let Ok(bytes) = engine.decode(raw) {
            if !bytes.is_empty() {
                return Ok(bytes);
            }
        }
    }
    bail!("无法解码")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_key_same_material() {
        let a = KeyMaterial::parse("0123456789abcdef0123456789abcdef").unwrap();
        let b = KeyMaterial::parse("0123456789abcdef0123456789abcdef").unwrap();
        assert_eq!(a.encode(), b.encode());
        assert_eq!(a.static_private(), b.static_private());
        assert_eq!(a.psk(), b.psk());
        assert_eq!(a.public_key(), b.public_key());
    }

    #[test]
    fn different_keys_differ() {
        let a = KeyMaterial::parse("0123456789abcdef0123456789abcdef").unwrap();
        let b = KeyMaterial::parse("0123456789abcdef0123456789abcdee").unwrap();
        assert_ne!(a.derive(b"psk"), b.derive(b"psk"));
    }

    #[test]
    fn rejects_short_key() {
        assert!(KeyMaterial::parse("abc").is_err());
        assert!(KeyMaterial::parse("YWJjZGVmZ2g").is_err());
    }

    #[test]
    fn accepts_hex_and_base64() {
        let base = KeyMaterial::parse("0123456789abcdef0123456789abcdef").unwrap();
        let b64 = base.encode();
        let parsed = KeyMaterial::parse(&b64).unwrap();
        assert_eq!(base.encode(), parsed.encode());
        let hex_key = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
        assert_eq!(KeyMaterial::parse(hex_key).unwrap().secret.len(), 32);
    }

    #[test]
    fn generate_is_stable() {
        let key = KeyMaterial::generate();
        let text = key.encode();
        assert_eq!(KeyMaterial::parse(&text).unwrap().encode(), text);
    }
}
