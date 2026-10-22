pub mod tools;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::session::Session;

const SUPPORTED_VERSIONS: [&str; 3] = ["2025-06-18", "2025-03-26", "2024-11-05"];
const WRITER_DRAIN: Duration = Duration::from_secs(3);
const EOF_DRAIN: Duration = Duration::from_secs(5);

const INSTRUCTIONS: &str = "\
这些工具作用于远端主机：先用 targets 查看在线目标（中继自身与各被控端），把返回的\
名字作为后续工具的 target 参数。bash 通过 sh -c 一次性执行命令，超过 timeout_ms \
会杀掉整个进程组；read/write/edit 只能访问目标上的 ROOTS 白名单目录，\
越界与符号链接逃逸都会被拒绝；超过 48 KiB 的内容用 upload/download 传文件，不要\
用 write。远端首次排错建议顺序：targets → bash（uname -a、ps、systemctl status）→ \
read（看配置文件）。工具失败时 isError 为真，错误码说明原因（unknown_target、\
offline、timeout、denied、too_large、busy、io、protocol）。需要临时算力时用 \
actions 在 GitHub Actions 上开一个一次性被控端（目标名 Actions，注册进房间后\
即可当普通 target 用），用完 actions stop 关掉（直接向节点发 shutdown，节点自行退出，\
运行保持成功），别让它白烧额度。";

pub async fn serve(session: &Session, shutdown: CancellationToken) -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<Value>(64);
    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(message) = rx.recv().await {
            let mut bytes = match serde_json::to_vec(&message) {
                Ok(bytes) => bytes,
                Err(err) => {
                    warn!(error = %err, "序列化 MCP 响应失败");
                    continue;
                }
            };
            bytes.push(b'\n');
            if stdout.write_all(&bytes).await.is_err() {
                break;
            }
            if stdout.flush().await.is_err() {
                break;
            }
            debug!(id = %message.get("id").map(|id| id.to_string()).unwrap_or_default(), "已写出响应");
        }
    });

    let pending = Arc::new(Mutex::new(HashMap::<String, CancellationToken>::new()));
    let mut calls: JoinSet<()> = JoinSet::new();
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    loop {
        let line = tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = calls.join_next(), if !calls.is_empty() => continue,
            line = lines.next_line() => match line {
                Ok(Some(line)) => line,
                Ok(None) => {
                    info!("stdin 关闭，MCP 服务端退出");
                    break;
                }
                Err(err) => {
                    warn!(error = %err, "读取 stdin 失败");
                    break;
                }
            },
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let message: Value = match serde_json::from_str(line) {
            Ok(message) => message,
            Err(err) => {
                let response = error_response(Value::Null, -32700, format!("JSON 解析失败: {err}"));
                if tx.send(response).await.is_err() {
                    break;
                }
                continue;
            }
        };
        dispatch(session, &tx, &pending, &mut calls, message).await;
    }

    let drain = async { while calls.join_next().await.is_some() {} };
    let _ = timeout(EOF_DRAIN, drain).await;
    calls.abort_all();
    if let Ok(mut pending) = pending.lock() {
        for (_, token) in pending.drain() {
            token.cancel();
        }
    }
    drop(tx);
    let _ = timeout(WRITER_DRAIN, writer).await;
    Ok(())
}

async fn dispatch(
    session: &Session,
    tx: &mpsc::Sender<Value>,
    pending: &Arc<Mutex<HashMap<String, CancellationToken>>>,
    calls: &mut JoinSet<()>,
    message: Value,
) {
    let Some(method) = message
        .get("method")
        .and_then(Value::as_str)
        .map(str::to_string)
    else {
        debug!("忽略非请求消息");
        return;
    };
    let id = message.get("id").cloned();
    let params = message.get("params").cloned().unwrap_or(Value::Null);

    let Some(id) = id else {
        match method.as_str() {
            "notifications/initialized" => info!("DSH 完成 MCP 初始化"),
            "notifications/cancelled" => {
                let key = params
                    .get("requestId")
                    .map(|value| value.to_string())
                    .unwrap_or_default();
                let token = pending.lock().ok().and_then(|mut map| map.remove(&key));
                if let Some(token) = token {
                    debug!(request = %key, "客户端取消请求");
                    token.cancel();
                }
            }
            other => debug!(method = %other, "忽略通知"),
        }
        return;
    };

    match method.as_str() {
        "initialize" => {
            let result = initialize(&params);
            let _ = tx.send(success(id, result)).await;
        }
        "ping" => {
            let _ = tx.send(success(id, json!({}))).await;
        }
        "tools/list" => {
            let _ = tx
                .send(success(id, json!({ "tools": tools::list() })))
                .await;
        }
        "tools/call" => {
            let key = id.to_string();
            let token = CancellationToken::new();
            if let Ok(mut map) = pending.lock() {
                map.insert(key.clone(), token.clone());
            }
            let session = session.clone();
            let tx = tx.clone();
            let pending = pending.clone();
            calls.spawn(async move {
                let outcome = tools::call(&session, &params, &token).await;
                if let Ok(mut map) = pending.lock() {
                    map.remove(&key);
                }
                let payload = if token.is_cancelled() {
                    tools::ToolOutput::error("调用已被客户端取消").into_value()
                } else {
                    outcome.into_value()
                };
                let _ = tx.send(success(id, payload)).await;
            });
        }
        other => {
            let response = error_response(id, -32601, format!("不支持的方法: {other}"));
            let _ = tx.send(response).await;
        }
    }
}

fn initialize(params: &Value) -> Value {
    let requested = params
        .get("protocolVersion")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let version = if SUPPORTED_VERSIONS.contains(&requested) {
        requested
    } else {
        SUPPORTED_VERSIONS[0]
    };
    if let Some(client) = params.get("clientInfo") {
        debug!(client = %client, "MCP 客户端信息");
    }
    json!({
        "protocolVersion": version,
        "capabilities": { "tools": { "listChanged": false } },
        "serverInfo": {
            "name": "dsh-remote",
            "title": "dsh-remote 远程执行网关",
            "version": crate::VERSION,
        },
        "instructions": INSTRUCTIONS,
    })
}

fn success(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn error_response(id: Value, code: i64, message: impl Into<String>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message.into() },
    })
}
