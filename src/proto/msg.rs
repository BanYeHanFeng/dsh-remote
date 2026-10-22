use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

pub const PROTO_VERSION: u16 = 2;

pub const STREAM_STDOUT: u8 = 1;
pub const STREAM_STDERR: u8 = 2;

mod bytes_serde {
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(data: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(data)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        struct Visit;
        impl<'de> serde::de::Visitor<'de> for Visit {
            type Value = Vec<u8>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("字节串或整数数组")
            }
            fn visit_bytes<E>(self, value: &[u8]) -> Result<Vec<u8>, E> {
                Ok(value.to_vec())
            }
            fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Vec<u8>, E> {
                Ok(value)
            }
            fn visit_seq<A>(self, mut seq: A) -> Result<Vec<u8>, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(byte) = seq.next_element::<u8>()? {
                    out.push(byte);
                }
                Ok(out)
            }
        }
        deserializer.deserialize_bytes(Visit)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Server,
    Agent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetKind {
    Hub,
    Agent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetEventKind {
    Online,
    Offline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndStatus {
    Ok,
    Error,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TargetInfo {
    pub name: String,
    pub kind: TargetKind,
    pub os: String,
    pub arch: String,
    pub version: String,
    pub since: u64,
    pub roots: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProtoError {
    pub code: String,
    pub message: String,
}

impl ProtoError {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum Msg {
    Hello {
        proto: u16,
        role: Role,
        name: String,
        version: String,
        os: String,
        arch: String,
        roots: Vec<String>,
    },
    Welcome {
        proto: u16,
        hub_name: String,
        targets: Vec<TargetInfo>,
        server_online: bool,
    },
    Ping {
        nonce: u64,
    },
    Pong {
        nonce: u64,
    },
    Bye {
        code: String,
        message: String,
    },
    TargetEvent {
        kind: TargetEventKind,
        target: TargetInfo,
    },
    Call {
        id: u64,
        target: String,
        method: String,
        params: ciborium::value::Value,
    },
    Result {
        id: u64,
        ok: bool,
        value: Option<ciborium::value::Value>,
        error: Option<ProtoError>,
    },
    Chunk {
        id: u64,
        seq: u64,
        stream: u8,
        #[serde(with = "bytes_serde")]
        data: Vec<u8>,
    },
    End {
        id: u64,
        status: EndStatus,
        exit_code: Option<i32>,
        size: Option<u64>,
        sha256: Option<String>,
        truncated: Option<bool>,
    },
    Cancel {
        id: u64,
        reason: Option<String>,
    },
}

impl Msg {
    pub fn id(&self) -> Option<u64> {
        match self {
            Msg::Call { id, .. }
            | Msg::Result { id, .. }
            | Msg::Chunk { id, .. }
            | Msg::End { id, .. }
            | Msg::Cancel { id, .. } => Some(*id),
            _ => None,
        }
    }

    pub fn with_id(mut self, id: u64) -> Msg {
        if let Some(slot) = self.id_mut() {
            *slot = id;
        }
        self
    }

    fn id_mut(&mut self) -> Option<&mut u64> {
        match self {
            Msg::Call { id, .. }
            | Msg::Result { id, .. }
            | Msg::Chunk { id, .. }
            | Msg::End { id, .. }
            | Msg::Cancel { id, .. } => Some(id),
            _ => None,
        }
    }
}

pub fn target_names(targets: &[TargetInfo]) -> String {
    targets
        .iter()
        .map(|target| target.name.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn msg_roundtrip() {
        let msg = Msg::Hello {
            proto: PROTO_VERSION,
            role: Role::Agent,
            name: "nas".into(),
            version: "0.1.0".into(),
            os: "linux".into(),
            arch: "x86_64".into(),
            roots: vec!["/data".into()],
        };
        let bytes = crate::proto::frame::encode(&msg).unwrap();
        let back = crate::proto::frame::decode(&bytes).unwrap();
        assert_eq!(msg, back);
    }

    #[test]
    fn target_event_roundtrip() {
        let msg = Msg::TargetEvent {
            kind: TargetEventKind::Online,
            target: TargetInfo {
                name: "hub".into(),
                kind: TargetKind::Hub,
                os: "linux".into(),
                arch: "aarch64".into(),
                version: "0.1.0".into(),
                since: now_ms(),
                roots: vec!["/".into()],
            },
        };
        let bytes = crate::proto::frame::encode(&msg).unwrap();
        assert_eq!(msg, crate::proto::frame::decode(&bytes).unwrap());
    }

    #[test]
    fn rewrites_id() {
        let msg = Msg::End {
            id: 7,
            status: EndStatus::Ok,
            exit_code: Some(0),
            size: None,
            sha256: None,
            truncated: None,
        };
        assert_eq!(msg.with_id(9).id(), Some(9));
        assert_eq!(Msg::Ping { nonce: 1 }.with_id(9).id(), None);
    }
}
