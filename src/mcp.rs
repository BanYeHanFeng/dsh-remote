use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

use crate::ipc::{IpcClient, IpcEvent, IpcRequest, NodeInfo};
use crate::protocol::Failure;
use crate::text;

const DEFAULT_MAX_OUTPUT_CHARS: usize = 16000;
const PROTOCOL_VERSION: &str = "2025-06-18";

struct ExitInfo {
    code: Option<i32>,
    signal: Option<i32>,
    timed_out: bool,
    timeout_secs: u64,
}

pub async fn serve_stdio(socket: PathBuf) -> Result<()> {
    let mut reader = tokio::io::BufReader::new(tokio::io::stdin());
    let mut writer = tokio::io::stdout();
    let mut line = String::new();
    loop {
        line.clear();
        let read = reader
            .read_line(&mut line)
            .await
            .context("读取标准输入失败")?;
        if read == 0 {
            return Ok(());
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let request: Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if let Some(response) = handle(&socket, request).await {
            let mut payload = serde_json::to_string(&response)?;
            payload.push('\n');
            writer.write_all(payload.as_bytes()).await?;
            writer.flush().await?;
        }
    }
}

async fn handle(socket: &Path, request: Value) -> Option<Value> {
    let id = request.get("id").cloned();
    let method = request
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let params = request.get("params").cloned().unwrap_or(Value::Null);

    let id = id?;
    let outcome = match method.as_str() {
        "initialize" => Ok(initialize_result(&params)),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": tools_json() })),
        "tools/call" => call_tool(socket, &params).await,
        other => {
            return Some(json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {"code": -32601, "message": format!("不支持的方法：{}", other)},
            }));
        }
    };

    Some(match outcome {
        Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
        Err(message) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {"content": [{"type": "text", "text": message}], "isError": true},
        }),
    })
}

fn initialize_result(params: &Value) -> Value {
    let version = params
        .get("protocolVersion")
        .and_then(Value::as_str)
        .unwrap_or(PROTOCOL_VERSION);
    json!({
        "protocolVersion": version,
        "capabilities": {"tools": {}},
        "serverInfo": {"name": "dsh-remote", "version": env!("CARGO_PKG_VERSION")},
        "instructions": "远程节点控制与文件传输：先用 list_nodes 查看在线节点，再用 remote_bash / upload_file / download_file / transfer_file 操作。",
    })
}

fn tools_json() -> Value {
    json!([
        {
            "name": "list_nodes",
            "description": "列出当前在线的远程节点及其状态",
            "inputSchema": {
                "type": "object",
                "properties": {}
            }
        },
        {
            "name": "remote_bash",
            "description": "在指定节点执行一次性 bash -c 命令：每次都是独立进程，不保留 cd 与 export 状态，超过超时时间会被整组终止",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "node": {"type": "string", "description": "目标节点标识"},
                    "command": {"type": "string", "description": "要执行的 bash 命令"},
                    "workdir": {"type": "string", "description": "可选，命令的工作目录"},
                    "timeout_secs": {"type": "integer", "description": "可选，超时秒数，默认 300"}
                },
                "required": ["node", "command"]
            }
        },
        {
            "name": "upload_file",
            "description": "把控制端本机的文件上传到指定节点",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "node": {"type": "string", "description": "目标节点标识"},
                    "local_path": {"type": "string", "description": "控制端本机的源文件路径"},
                    "remote_path": {"type": "string", "description": "节点上的目标路径"},
                    "overwrite": {"type": "boolean", "description": "可选，是否覆盖已存在的文件，默认 true"}
                },
                "required": ["node", "local_path", "remote_path"]
            }
        },
        {
            "name": "download_file",
            "description": "从指定节点下载文件到控制端本机",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "node": {"type": "string", "description": "来源节点标识"},
                    "remote_path": {"type": "string", "description": "节点上的源文件路径"},
                    "local_path": {"type": "string", "description": "控制端本机的目标路径"},
                    "overwrite": {"type": "boolean", "description": "可选，是否覆盖已存在的文件，默认 true"}
                },
                "required": ["node", "remote_path", "local_path"]
            }
        },
        {
            "name": "transfer_file",
            "description": "在两个远程节点之间流式互传文件，数据不绕行控制端磁盘",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "src_node": {"type": "string", "description": "来源节点标识"},
                    "src_path": {"type": "string", "description": "来源节点上的文件路径"},
                    "dst_node": {"type": "string", "description": "目标节点标识"},
                    "dst_path": {"type": "string", "description": "目标节点上的路径"},
                    "overwrite": {"type": "boolean", "description": "可选，是否覆盖已存在的文件，默认 true"}
                },
                "required": ["src_node", "src_path", "dst_node", "dst_path"]
            }
        }
    ])
}

async fn call_tool(socket: &Path, params: &Value) -> Result<Value, String> {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| "缺少工具名".to_string())?;
    let args = params.get("arguments").cloned().unwrap_or(json!({}));

    let text = match name {
        "list_nodes" => list_nodes(socket).await?,
        "remote_bash" => remote_bash(socket, &args).await?,
        "upload_file" => upload_file(socket, &args).await?,
        "download_file" => download_file(socket, &args).await?,
        "transfer_file" => transfer_file(socket, &args).await?,
        other => return Err(format!("未知的工具：{}", other)),
    };
    Ok(json!({"content": [{"type": "text", "text": text}], "isError": false}))
}

fn required(args: &Value, key: &str) -> Result<String, String> {
    match args.get(key).and_then(Value::as_str) {
        Some(value) if !value.trim().is_empty() => Ok(value.to_string()),
        _ => Err(format!("参数 {} 必须是非空字符串", key)),
    }
}

fn optional(args: &Value, key: &str) -> Option<String> {
    args.get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.to_string())
}

fn flag(args: &Value, key: &str, default: bool) -> bool {
    args.get(key).and_then(Value::as_bool).unwrap_or(default)
}

async fn connect(socket: &Path) -> Result<IpcClient, String> {
    IpcClient::connect(socket)
        .await
        .map_err(|error| format!("连接控制端失败：{:#}", error))
}

async fn exchange(client: &mut IpcClient, request: &IpcRequest) -> Result<IpcEvent, String> {
    client
        .send(request)
        .await
        .map_err(|error| format!("发送请求失败：{:#}", error))?;
    client
        .next()
        .await
        .map_err(|error| format!("读取控制端响应失败：{:#}", error))
}

fn failure_text(failure: Failure) -> String {
    failure.text()
}

async fn list_nodes(socket: &Path) -> Result<String, String> {
    let mut client = connect(socket).await?;
    let event = exchange(&mut client, &IpcRequest::Nodes).await?;
    match event {
        IpcEvent::Nodes { nodes } => Ok(render_nodes(&nodes)),
        IpcEvent::Err { code, message } => Err(failure_text(Failure { code, message })),
        _ => Err("控制端返回了意料之外的响应".to_string()),
    }
}

fn render_nodes(nodes: &[NodeInfo]) -> String {
    if nodes.is_empty() {
        return text::nodes_empty();
    }
    let mut lines = vec![text::nodes_header(nodes.len())];
    for node in nodes {
        lines.push(text::node_line(&node.node_id, &node.via, node.online_secs));
    }
    lines.join("\n")
}

async fn remote_bash(socket: &Path, args: &Value) -> Result<String, String> {
    let node = required(args, "node")?;
    let command = required(args, "command")?;
    let workdir = optional(args, "workdir");
    let timeout_secs = args
        .get("timeout_secs")
        .and_then(Value::as_u64)
        .unwrap_or(300)
        .max(1);

    let mut client = connect(socket).await?;
    client
        .send(&IpcRequest::Exec {
            node,
            command,
            workdir,
            timeout_secs,
        })
        .await
        .map_err(|error| format!("发送请求失败：{:#}", error))?;

    let cap = max_output_chars();
    let mut stdout = Collector::new(cap);
    let mut stderr = Collector::new(cap);
    loop {
        let event = client
            .next()
            .await
            .map_err(|error| format!("读取控制端响应失败：{:#}", error))?;
        match event {
            IpcEvent::Stdout { d } => stdout.push(&d),
            IpcEvent::Stderr { d } => stderr.push(&d),
            IpcEvent::Exit {
                code,
                signal,
                timed_out,
            } => {
                return Ok(render_exec(
                    &stdout,
                    &stderr,
                    ExitInfo {
                        code,
                        signal,
                        timed_out,
                        timeout_secs,
                    },
                    cap,
                ));
            }
            IpcEvent::Err { code, message } => {
                return Err(failure_text(Failure { code, message }));
            }
            _ => {}
        }
    }
}

fn max_output_chars() -> usize {
    std::env::var("DSH_REMOTE_MAX_OUTPUT_CHARS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_OUTPUT_CHARS)
}

struct Collector {
    text: String,
    cap: usize,
    overflow: bool,
}

impl Collector {
    fn new(max_chars: usize) -> Self {
        Self {
            text: String::new(),
            cap: max_chars.saturating_mul(4),
            overflow: false,
        }
    }

    fn push(&mut self, chunk: &str) {
        let remaining = self.cap.saturating_sub(self.text.len());
        if remaining == 0 {
            self.overflow = true;
            return;
        }
        if chunk.len() <= remaining {
            self.text.push_str(chunk);
            return;
        }
        let mut end = remaining;
        while end > 0 && !chunk.is_char_boundary(end) {
            end -= 1;
        }
        self.text.push_str(&chunk[..end]);
        self.overflow = true;
    }

    fn clipped(&self, max_chars: usize) -> String {
        clip(&self.text, max_chars, self.overflow)
    }
}

fn render_exec(stdout: &Collector, stderr: &Collector, exit: ExitInfo, max_chars: usize) -> String {
    let mut blocks = Vec::new();
    let out = stdout.clipped(max_chars);
    if !out.is_empty() {
        blocks.push(out);
    }
    let err = stderr.clipped(max_chars);
    if !err.is_empty() {
        blocks.push(format!("{}\n{}", text::STDERR_LABEL, err));
    }
    if blocks.is_empty() {
        blocks.push(text::NO_OUTPUT.to_string());
    }
    if exit.timed_out {
        blocks.push(text::timed_out(exit.timeout_secs));
    } else if let Some(signal) = exit.signal {
        blocks.push(text::killed(signal));
    } else {
        blocks.push(text::exit_code(exit.code.unwrap_or(0)));
    }
    blocks.join("\n")
}

fn clip(value: &str, max_chars: usize, overflow: bool) -> String {
    let mut text: String = value.chars().take(max_chars).collect();
    if overflow || value.chars().count() > max_chars {
        text.push('\n');
        text.push_str(text::TRUNCATED);
    }
    text
}

async fn upload_file(socket: &Path, args: &Value) -> Result<String, String> {
    let node = required(args, "node")?;
    let local_path = required(args, "local_path")?;
    let remote_path = required(args, "remote_path")?;
    let mut client = connect(socket).await?;
    let event = exchange(
        &mut client,
        &IpcRequest::Upload {
            node: node.clone(),
            local_path,
            remote_path: remote_path.clone(),
            overwrite: flag(args, "overwrite", true),
        },
    )
    .await?;
    match event {
        IpcEvent::Done { bytes, sha256, .. } => {
            Ok(text::uploaded(&node, &remote_path, bytes, &sha256))
        }
        IpcEvent::Err { code, message } => Err(failure_text(Failure { code, message })),
        _ => Err("控制端返回了意料之外的响应".to_string()),
    }
}

async fn download_file(socket: &Path, args: &Value) -> Result<String, String> {
    let node = required(args, "node")?;
    let remote_path = required(args, "remote_path")?;
    let local_path = required(args, "local_path")?;
    let mut client = connect(socket).await?;
    let event = exchange(
        &mut client,
        &IpcRequest::Download {
            node: node.clone(),
            remote_path: remote_path.clone(),
            local_path: local_path.clone(),
            overwrite: flag(args, "overwrite", true),
        },
    )
    .await?;
    match event {
        IpcEvent::Done { bytes, sha256, .. } => Ok(text::downloaded(
            &local_path,
            &node,
            &remote_path,
            bytes,
            &sha256,
        )),
        IpcEvent::Err { code, message } => Err(failure_text(Failure { code, message })),
        _ => Err("控制端返回了意料之外的响应".to_string()),
    }
}

async fn transfer_file(socket: &Path, args: &Value) -> Result<String, String> {
    let src_node = required(args, "src_node")?;
    let src_path = required(args, "src_path")?;
    let dst_node = required(args, "dst_node")?;
    let dst_path = required(args, "dst_path")?;
    let mut client = connect(socket).await?;
    let event = exchange(
        &mut client,
        &IpcRequest::Transfer {
            src_node: src_node.clone(),
            src_path: src_path.clone(),
            dst_node: dst_node.clone(),
            dst_path: dst_path.clone(),
            overwrite: flag(args, "overwrite", true),
        },
    )
    .await?;
    match event {
        IpcEvent::Done { bytes, sha256, .. } => Ok(text::transferred(
            (&src_node, &src_path),
            (&dst_node, &dst_path),
            bytes,
            &sha256,
        )),
        IpcEvent::Err { code, message } => Err(failure_text(Failure { code, message })),
        _ => Err("控制端返回了意料之外的响应".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exit(code: i32) -> ExitInfo {
        ExitInfo {
            code: Some(code),
            signal: None,
            timed_out: false,
            timeout_secs: 300,
        }
    }

    #[test]
    fn renders_plain_output_with_exit_code() {
        let mut out = Collector::new(16);
        out.push("你好\n");
        let err = Collector::new(16);
        assert_eq!(
            render_exec(&out, &err, exit(0), 16),
            "你好\n\n[命令结束，退出码 0]"
        );
    }

    #[test]
    fn renders_no_output_notice() {
        let out = Collector::new(16);
        let err = Collector::new(16);
        assert_eq!(
            render_exec(&out, &err, exit(3), 16),
            "（这条命令没有任何输出）\n[命令结束，退出码 3]"
        );
    }

    #[test]
    fn truncates_and_marks_stderr() {
        let mut out = Collector::new(2);
        out.push("abcdef");
        let mut err = Collector::new(8);
        err.push("坏了");
        let rendered = render_exec(&out, &err, exit(0), 2);
        assert!(rendered.starts_with("ab\n<输出已截断>"));
        assert!(rendered.contains("[标准错误]\n坏了"));
        assert!(rendered.ends_with("[命令结束，退出码 0]"));
    }

    #[test]
    fn marks_timeout_and_signal() {
        let out = Collector::new(16);
        let err = Collector::new(16);
        let timed_out = ExitInfo {
            code: None,
            signal: None,
            timed_out: true,
            timeout_secs: 5,
        };
        assert!(
            render_exec(&out, &err, timed_out, 16).ends_with("[命令超过 5 秒仍未结束，已被终止]")
        );
        let killed = ExitInfo {
            code: None,
            signal: Some(9),
            timed_out: false,
            timeout_secs: 5,
        };
        assert!(render_exec(&out, &err, killed, 16).ends_with("[Shell 被信号终止：SIGKILL]"));
    }
}
