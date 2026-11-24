use serde::{Deserialize, Serialize};

pub const PROTO_VERSION: u32 = 1;

pub fn validate_node_id(id: &str) -> anyhow::Result<()> {
    if id.trim().is_empty() {
        return Err(anyhow::anyhow!("节点标识不能为空"));
    }
    if id.len() > 128 {
        return Err(anyhow::anyhow!("节点标识过长：{}", id));
    }
    if id.contains(|c: char| c.is_whitespace() || c == ':' || c == '/') {
        return Err(anyhow::anyhow!(
            "节点标识不能包含空白字符、冒号或斜杠：{}",
            id
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Agent,
    Controller,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Agent => "agent",
            Role::Controller => "controller",
        }
    }

    pub fn zh(self) -> &'static str {
        match self {
            Role::Agent => "被控端",
            Role::Controller => "控制端",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    NodeOffline,
    AuthFailed,
    NotFound,
    NotRegularFile,
    AlreadyExists,
    PermissionDenied,
    ChecksumMismatch,
    Timeout,
    IoError,
    BadRequest,
    Internal,
}

impl ErrorCode {
    pub fn zh(self) -> &'static str {
        match self {
            ErrorCode::NodeOffline => "节点不在线",
            ErrorCode::AuthFailed => "认证失败",
            ErrorCode::NotFound => "路径不存在",
            ErrorCode::NotRegularFile => "不是普通文件",
            ErrorCode::AlreadyExists => "目标文件已存在",
            ErrorCode::PermissionDenied => "没有权限",
            ErrorCode::ChecksumMismatch => "文件校验失败",
            ErrorCode::Timeout => "操作超时",
            ErrorCode::IoError => "读写失败",
            ErrorCode::BadRequest => "请求参数有误",
            ErrorCode::Internal => "内部错误",
        }
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.zh())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Failure {
    pub code: ErrorCode,
    pub message: String,
}

pub type Res<T> = Result<T, Failure>;

impl Failure {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn io(message: impl Into<String>, error: std::io::Error) -> Self {
        Self {
            code: code_of(&error),
            message: format!("{}：{}", message.into(), error),
        }
    }

    pub fn text(&self) -> String {
        if self.message.is_empty() {
            self.code.zh().to_string()
        } else {
            format!("{}：{}", self.code.zh(), self.message)
        }
    }
}

impl From<std::io::Error> for Failure {
    fn from(error: std::io::Error) -> Self {
        Self {
            code: code_of(&error),
            message: error.to_string(),
        }
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text())
    }
}

impl std::error::Error for Failure {}

pub fn code_of(error: &std::io::Error) -> ErrorCode {
    match error.kind() {
        std::io::ErrorKind::NotFound => ErrorCode::NotFound,
        std::io::ErrorKind::PermissionDenied => ErrorCode::PermissionDenied,
        std::io::ErrorKind::AlreadyExists => ErrorCode::AlreadyExists,
        std::io::ErrorKind::TimedOut => ErrorCode::Timeout,
        _ => ErrorCode::IoError,
    }
}

pub fn internal(message: impl Into<String>) -> Failure {
    Failure::new(ErrorCode::Internal, message)
}

pub fn bad_request(message: impl Into<String>) -> Failure {
    Failure::new(ErrorCode::BadRequest, message)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum ControlMsg {
    Hello {
        node_id: String,
    },
    NodeOnline {
        node_id: String,
        tags: Vec<String>,
    },
    NodeOffline {
        node_id: String,
    },
    BridgeTransfer {
        req_id: u64,
        src_node: String,
        src_path: String,
        dst_node: String,
        dst_path: String,
        overwrite: bool,
    },
    BridgeResult {
        req_id: u64,
        result: Result<TransferDone, Failure>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferDone {
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayHeader {
    pub target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum OpRequest {
    Exec {
        command: String,
        workdir: Option<String>,
        timeout_secs: u64,
    },
    Upload {
        remote_path: String,
        size: u64,
        mode: Option<u32>,
        overwrite: bool,
    },
    Download {
        remote_path: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum OpFrame {
    Stdout {
        d: String,
    },
    Stderr {
        d: String,
    },
    Exit {
        code: Option<i32>,
        signal: Option<i32>,
        timed_out: bool,
    },
    Ack {
        size: u64,
        mode: Option<u32>,
    },
    Done {
        bytes: u64,
        sha256: String,
    },
    Err {
        code: ErrorCode,
        message: String,
    },
}

impl OpFrame {
    pub fn err(failure: &Failure) -> Self {
        OpFrame::Err {
            code: failure.code,
            message: failure.message.clone(),
        }
    }

    pub fn into_result(self) -> Res<TransferDone> {
        match self {
            OpFrame::Done { bytes, sha256 } => Ok(TransferDone { bytes, sha256 }),
            OpFrame::Err { code, message } => Err(Failure { code, message }),
            _ => Err(internal("节点返回了意料之外的响应帧")),
        }
    }
}
