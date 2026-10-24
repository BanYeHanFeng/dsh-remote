mod common;

use std::process::Stdio;
use std::time::Duration;

use common::*;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::TcpListener;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::time::{timeout, Instant};

const REPLY_LIMIT: Duration = Duration::from_secs(30);

struct Mcp {
    child: Child,
    stdin: ChildStdin,
    lines: Lines<BufReader<ChildStdout>>,
    next_id: u64,
}

impl Mcp {
    async fn spawn(hub: &str, key: &str) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_dsh-remote"))
            .arg("server")
            .arg("--hub")
            .arg(hub)
            .arg("--key")
            .arg(key)
            .arg("--name")
            .arg("probe")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("启动 dsh-remote server 失败");
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    eprintln!("[server] {line}");
                }
            });
        }
        Self {
            child,
            stdin,
            lines: BufReader::new(stdout).lines(),
            next_id: 1,
        }
    }

    async fn notify(&mut self, method: &str, params: Value) {
        let message = json!({ "jsonrpc": "2.0", "method": method, "params": params });
        let mut line = serde_json::to_vec(&message).unwrap();
        line.push(b'\n');
        self.stdin.write_all(&line).await.unwrap();
        self.stdin.flush().await.unwrap();
    }

    async fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let message = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        let mut line = serde_json::to_vec(&message).unwrap();
        line.push(b'\n');
        self.stdin.write_all(&line).await.unwrap();
        self.stdin.flush().await.unwrap();
        self.wait_for(id).await
    }

    async fn wait_for(&mut self, id: u64) -> Value {
        loop {
            let line = timeout(REPLY_LIMIT, self.lines.next_line())
                .await
                .unwrap_or_else(|_| panic!("等待响应 {id} 超时"))
                .expect("读取 stdout 失败")
                .expect("server 提前退出");
            if line.trim().is_empty() {
                continue;
            }
            let message: Value = serde_json::from_str(&line).expect("响应不是合法 JSON");
            assert_eq!(message["jsonrpc"], "2.0");
            if message.get("id") == Some(&json!(id)) {
                return message;
            }
        }
    }

    async fn tool(&mut self, name: &str, arguments: Value) -> (String, bool) {
        let response = self
            .request(
                "tools/call",
                json!({ "name": name, "arguments": arguments }),
            )
            .await;
        let result = &response["result"];
        let text = result["content"][0]["text"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let is_error = result["isError"].as_bool().unwrap_or(false);
        (text, is_error)
    }

    async fn close(mut self) {
        drop(self.stdin);
        let _ = timeout(Duration::from_secs(5), self.child.wait()).await;
        let _ = self.child.start_kill();
    }
}

async fn connect(harness: &Harness) -> Mcp {
    let mut mcp = Mcp::spawn(&harness.addr, KEY).await;
    let response = mcp
        .request(
            "initialize",
            json!({
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": { "name": "dsh-remote-test", "version": "0" }
            }),
        )
        .await;
    assert_eq!(response["result"]["protocolVersion"], "2025-06-18");
    mcp.notify("notifications/initialized", json!({})).await;

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (text, _) = mcp.tool("targets", json!({})).await;
        if text.contains("已连接") && text.contains("nas") {
            return mcp;
        }
        if Instant::now() > deadline {
            panic!("等待 MCP 服务端连上中继超时，最后输出: {text}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn initialize_and_tools_list() {
    let harness = Harness::start("mcp", true, true, None).await;
    let mut mcp = Mcp::spawn(&harness.addr, KEY).await;

    let response = mcp
        .request(
            "initialize",
            json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": { "name": "dsh-remote-test", "version": "0" }
            }),
        )
        .await;
    let result = &response["result"];
    assert_eq!(result["protocolVersion"], "2024-11-05");
    assert_eq!(result["capabilities"]["tools"]["listChanged"], false);
    assert_eq!(result["serverInfo"]["name"], "dsh-remote");
    assert!(result["instructions"].as_str().unwrap().contains("targets"));

    let pong = mcp.request("ping", json!({})).await;
    assert!(pong["result"].is_object());

    let listing = mcp.request("tools/list", json!({})).await;
    let names: Vec<String> = listing["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        names,
        vec!["targets", "bash", "upload", "download", "actions"]
    );
    let bash = &listing["result"]["tools"][1];
    assert_eq!(
        bash["inputSchema"]["required"],
        json!(["target", "command"])
    );

    let unknown = mcp.request("resources/list", json!({})).await;
    assert_eq!(unknown["error"]["code"], -32601);

    mcp.close().await;
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tools_run_against_agent_and_hub() {
    let harness = Harness::start("mcp", true, true, None).await;
    let mut mcp = connect(&harness).await;

    let (targets, is_error) = mcp.tool("targets", json!({})).await;
    assert!(!is_error, "{targets}");
    assert!(
        targets.contains("hub") && targets.contains("nas"),
        "{targets}"
    );

    let (text, is_error) = mcp
        .tool(
            "bash",
            json!({ "target": "nas", "command": "echo mcp-ok; id -u >/dev/null" }),
        )
        .await;
    assert!(!is_error, "{text}");
    assert!(text.contains("退出码 0"), "{text}");
    assert!(text.contains("mcp-ok"), "{text}");

    let (text, is_error) = mcp
        .tool("bash", json!({ "target": "hub", "command": "exit 7" }))
        .await;
    assert!(is_error, "{text}");
    assert!(text.contains("退出码 7"), "{text}");

    let (text, is_error) = mcp
        .tool("bash", json!({ "target": "ghost", "command": "true" }))
        .await;
    assert!(is_error, "{text}");
    assert!(text.contains("unknown_target"), "{text}");

    let started = Instant::now();
    let (text, is_error) = mcp
        .tool(
            "bash",
            json!({ "target": "nas", "command": "sleep 5", "timeout_ms": 300 }),
        )
        .await;
    assert!(is_error, "{text}");
    assert!(text.contains("timeout"), "{text}");
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "超时调用应快速返回，实际 {:?}",
        started.elapsed()
    );

    mcp.close().await;
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_and_download_through_mcp() {
    let harness = Harness::start("mcp", true, true, None).await;
    let mut mcp = connect(&harness).await;

    let local_dir = harness.local("local");
    let remote_dir = harness.local("remote");
    std::fs::create_dir_all(&local_dir).unwrap();
    std::fs::create_dir_all(&remote_dir).unwrap();

    let local = local_dir.join("payload.bin");
    let payload: Vec<u8> = (0..200_000u32).map(|index| (index % 251) as u8).collect();
    std::fs::write(&local, &payload).unwrap();
    let remote = remote_dir.join("payload.bin").to_string_lossy().to_string();

    let (text, is_error) = mcp
        .tool(
            "upload",
            json!({ "target": "nas", "local_path": local.to_string_lossy(), "remote_path": remote }),
        )
        .await;
    assert!(!is_error, "{text}");
    assert!(text.contains("已上传"), "{text}");
    assert_eq!(std::fs::read(&remote).unwrap(), payload);

    let back = harness.local("payload-back.bin");
    let (text, is_error) = mcp
        .tool(
            "download",
            json!({ "target": "nas", "remote_path": remote, "local_path": back.to_string_lossy() }),
        )
        .await;
    assert!(!is_error, "{text}");
    assert!(text.contains("已下载"), "{text}");
    assert_eq!(std::fs::read(&back).unwrap(), payload);

    let (text, is_error) = mcp
        .tool(
            "download",
            json!({ "target": "nas", "remote_path": remote, "local_path": back.to_string_lossy() }),
        )
        .await;
    assert!(is_error, "{text}");
    assert!(text.contains("已存在"), "{text}");

    mcp.close().await;
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_call_returns_quickly() {
    let harness = Harness::start("mcp", true, true, None).await;
    let mut mcp = connect(&harness).await;

    let id = mcp.next_id;
    mcp.next_id += 1;
    let message = json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "tools/call",
        "params": {
            "name": "bash",
            "arguments": { "target": "nas", "command": "sleep 15", "timeout_ms": 20000 }
        }
    });
    let mut line = serde_json::to_vec(&message).unwrap();
    line.push(b'\n');
    mcp.stdin.write_all(&line).await.unwrap();
    mcp.stdin.flush().await.unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;
    mcp.notify("notifications/cancelled", json!({ "requestId": id }))
        .await;

    let started = Instant::now();
    let response = mcp.wait_for(id).await;
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "取消耗时 {:?}",
        started.elapsed()
    );
    let text = response["result"]["content"][0]["text"].as_str().unwrap();
    assert_eq!(response["result"]["isError"], true, "{text}");

    mcp.close().await;
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offline_hub_still_answers_mcp() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);

    let mut mcp = Mcp::spawn(&addr, KEY).await;
    let response = mcp
        .request(
            "initialize",
            json!({ "protocolVersion": "2025-06-18", "capabilities": {} }),
        )
        .await;
    assert_eq!(response["result"]["serverInfo"]["name"], "dsh-remote");

    let (text, is_error) = mcp.tool("targets", json!({})).await;
    assert!(is_error, "{text}");
    assert!(text.contains("未连接"), "{text}");

    let started = Instant::now();
    let (text, is_error) = mcp
        .tool("bash", json!({ "target": "hub", "command": "true" }))
        .await;
    assert!(is_error, "{text}");
    assert!(text.contains("offline"), "{text}");
    assert!(started.elapsed() < Duration::from_secs(5), "{text}");

    mcp.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_reconnects_after_hub_restart() {
    let mut harness = Harness::start("mcp", true, true, None).await;
    let mut mcp = connect(&harness).await;

    harness.stop_hub().await;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (text, _) = mcp.tool("targets", json!({})).await;
        if text.contains("未连接") {
            break;
        }
        if Instant::now() > deadline {
            panic!("中继停止后仍显示已连接: {text}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    harness.restart_hub().await;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (text, _) = mcp.tool("targets", json!({})).await;
        if text.contains("已连接") && text.contains("nas") {
            break;
        }
        if Instant::now() > deadline {
            panic!("中继重启后未恢复: {text}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let (text, is_error) = mcp
        .tool("bash", json!({ "target": "nas", "command": "echo back" }))
        .await;
    assert!(!is_error, "{text}");
    assert!(text.contains("back"), "{text}");

    mcp.close().await;
    harness.stop().await;
}
