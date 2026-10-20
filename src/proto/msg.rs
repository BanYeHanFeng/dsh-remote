use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

pub const PROTO_VERSION: u16 = 1;

pub const STREAM_STDOUT: u8 = 1;
pub const STREAM_STDERR: u8 = 2;

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
    Truncated,
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
    pub caps: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProtoError {
    pub code: String,
    pub message: String,
    pub detail: Option<String>,
}

impl ProtoError {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
            detail: None,
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
        caps: Vec<String>,
        roots: Vec<String>,
    },
    Welcome {
        proto: u16,
        hub_name: String,
        session: String,
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
    TargetsSnapshot {
        targets: Vec<TargetInfo>,
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

    pub fn with_id(self, new_id: u64) -> Msg {
        match self {
            Msg::Call {
                target,
                method,
                params,
                ..
            } => Msg::Call {
                id: new_id,
                target,
                method,
                params,
            },
            Msg::Result {
                ok, value, error, ..
            } => Msg::Result {
                id: new_id,
                ok,
                value,
                error,
            },
            Msg::Chunk {
                seq, stream, data, ..
            } => Msg::Chunk {
                id: new_id,
                seq,
                stream,
                data,
            },
            Msg::End {
                status,
                exit_code,
                size,
                sha256,
                truncated,
                ..
            } => Msg::End {
                id: new_id,
                status,
                exit_code,
                size,
                sha256,
                truncated,
            },
            Msg::Cancel { reason, .. } => Msg::Cancel { id: new_id, reason },
            other => other,
        }
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

pub fn caps(allow_bash: bool) -> Vec<String> {
    let mut caps = vec!["fs".to_string(), "transfer".to_string()];
    if allow_bash {
        caps.insert(0, "bash".to_string());
    }
    caps
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
            caps: caps(true),
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
                caps: caps(false),
            },
        };
        let bytes = crate::proto::frame::encode(&msg).unwrap();
        assert_eq!(msg, crate::proto::frame::decode(&bytes).unwrap());
    }
}
