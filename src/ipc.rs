use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::net::UnixStream;

use crate::frame::{read_frame, write_frame};
use crate::protocol::ErrorCode;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum IpcRequest {
    Nodes {
        tag: Option<String>,
    },
    Exec {
        node: String,
        command: String,
        workdir: Option<String>,
        timeout_secs: u64,
    },
    Upload {
        node: String,
        local_path: String,
        remote_path: String,
        overwrite: bool,
    },
    Download {
        node: String,
        remote_path: String,
        local_path: String,
        overwrite: bool,
    },
    Transfer {
        src_node: String,
        src_path: String,
        dst_node: String,
        dst_path: String,
        overwrite: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub tags: Vec<String>,
    pub via: String,
    pub online_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum IpcEvent {
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
    Nodes {
        nodes: Vec<NodeInfo>,
    },
    Done {
        bytes: u64,
        sha256: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        from: Option<(String, String)>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        to: Option<(String, String)>,
    },
    Err {
        code: ErrorCode,
        message: String,
    },
}

impl IpcEvent {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            IpcEvent::Exit { .. }
                | IpcEvent::Nodes { .. }
                | IpcEvent::Done { .. }
                | IpcEvent::Err { .. }
        )
    }
}

pub struct IpcClient {
    stream: UnixStream,
}

impl IpcClient {
    pub async fn connect(socket: &Path) -> Result<Self> {
        let stream = UnixStream::connect(socket)
            .await
            .with_context(|| format!("连接控制端失败：{}", socket.display()))?;
        Ok(Self { stream })
    }

    pub async fn send(&mut self, request: &IpcRequest) -> Result<()> {
        write_frame(&mut self.stream, request).await
    }

    pub async fn next(&mut self) -> Result<IpcEvent> {
        read_frame(&mut self.stream).await
    }
}
