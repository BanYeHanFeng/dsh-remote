use std::fmt::Write as _;
use std::path::Path;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::actions::{
    GhClient, StartRequest, DEFAULT_ARCH, DEFAULT_DURATION_MINUTES, MAX_DURATION_MINUTES,
};
use crate::call;
use crate::proto::now_ms;
use crate::proto::params::TransferParams;
use crate::session::{RemoteError, Session};

const START_WAIT_SECONDS: u64 = 60;
const MAX_WAIT_SECONDS: u64 = 300;
const STOP_WAIT_SECONDS: u64 = 30;
const SHUTDOWN_CALL_TIMEOUT: Duration = Duration::from_secs(10);
const WAIT_POLL: Duration = Duration::from_secs(3);

pub struct ToolOutput {
    text: String,
    is_error: bool,
}

impl ToolOutput {
    pub fn ok(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            is_error: false,
        }
    }

    pub fn error(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            is_error: true,
        }
    }

    fn mark(mut self, is_error: bool) -> Self {
        self.is_error = is_error;
        self
    }

    pub fn into_value(self) -> Value {
        json!({
            "content": [{ "type": "text", "text": self.text }],
            "isError": self.is_error,
        })
    }
}

type ToolResult = Result<ToolOutput, String>;

const TARGET_SCHEMA: &str = "目标名，取自 targets 工具返回的名字";

pub fn list() -> Value {
    json!([
        {
            "name": "targets",
            "description": "列出房间内所有在线目标（中继自身与各被控端）：类型、系统、架构、版本、在线时长与允许访问的目录。任何操作前先调用它确认目标名。",
            "inputSchema": { "type": "object", "properties": {}, "additionalProperties": false }
        },
        {
            "name": "bash",
            "description": "在目标上同一个持久的 bash 里执行命令：当前目录、导出的环境变量等状态在调用之间保留。调用这个工具时，command 参数的内容不需要做 XML 转义。想查看文件的某一段行区间（例如第 10-25 行）时，试 sed -n 10,25p /path/to/the/file。命令超过目标的 SHELL_TIMEOUT_MS（默认 300 秒）或 Shell 退出会重置 Shell；返回内容超过字符上限会被截断。交互式程序（vim、top）不可用。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "target": { "type": "string", "description": TARGET_SCHEMA },
                    "command": { "type": "string", "description": "完整 shell 命令，例如 systemctl status nginx" }
                },
                "required": ["target", "command"]
            }
        },
        {
            "name": "upload",
            "description": "把文件传到 target 节点：给 local_path 时从本机上传；给 from 时改成节点互传，由中继把 from 节点上的文件直接搬到 target 上（不经过本机磁盘）。两种方式都会先写 .part，按总字节数与 sha256 校验后原子改名，默认拒绝覆盖已有文件。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "target": { "type": "string", "description": "目的节点名" },
                    "remote_path": { "type": "string", "description": "目的节点上的路径，必须在它的 ROOTS 内" },
                    "local_path": { "type": "string", "description": "本机（运行 server 的机器）上的源文件，与 from 二选一" },
                    "from": { "type": "string", "description": "源节点名：给了它就改成节点互传，忽略 local_path" },
                    "source_path": { "type": "string", "description": "源节点上的文件路径，配合 from 使用" },
                    "overwrite": { "type": "boolean", "description": "允许覆盖目的地已有文件" },
                    "create_parents": { "type": "boolean", "description": "自动创建目的地父目录" }
                },
                "required": ["target", "remote_path"]
            }
        },
        {
            "name": "download",
            "description": "从 target 节点取文件：给 local_path 时下载到本机；给 to 时改成节点互传，由中继把 target 上的文件直接搬到 to 节点（不经过本机磁盘）。默认拒绝覆盖目的地已有文件。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "target": { "type": "string", "description": "源节点名" },
                    "remote_path": { "type": "string", "description": "源节点上的文件路径，必须在它的 ROOTS 内" },
                    "local_path": { "type": "string", "description": "本机上的目标路径，父目录必须已存在；与 to 二选一" },
                    "to": { "type": "string", "description": "目的节点名：给了它就改成节点互传，忽略 local_path" },
                    "dest_path": { "type": "string", "description": "目的节点上的路径，配合 to 使用" },
                    "overwrite": { "type": "boolean", "description": "允许覆盖目的地已有文件" }
                },
                "required": ["target", "remote_path"]
            }
        },
        {
            "name": "actions",
            "description": "把 GitHub Actions 的临时 runner 变成房间里的被控端：start 派发工作流（节点以 Actions 进房间，默认保持 60 分钟并等它上线），stop 向节点发 shutdown 让它自行退出（运行在 Actions 里保持成功，节点不在线时兜底取消运行），status 看最近的运行与在线状态。需要 server 配置 GH_REPO，并且本机 gh 已登录。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": { "type": "string", "enum": ["start", "stop", "status"], "description": "start 开节点、stop 关节点、status 查看运行" },
                    "duration_minutes": { "type": "integer", "description": format!("start 时保持多少分钟，默认 {DEFAULT_DURATION_MINUTES}，上限 {MAX_DURATION_MINUTES}") },
                    "allow_bash": { "type": "boolean", "description": "start 时是否允许 bash 工具，默认 true" },
                    "roots": { "type": "string", "description": "start 时允许访问的目录，冒号分隔，默认 /" },
                    "version": { "type": "string", "description": "start 时的二进制来源：latest 用最新发布、source 现场编译、或具体 tag" },
                    "arch": { "type": "string", "enum": ["amd64", "arm64"], "description": format!("start 时的节点架构：amd64 走 ubuntu-latest，arm64 走 ubuntu-24.04-arm（公共仓库免费），默认 {DEFAULT_ARCH}") },
                    "hub": { "type": "string", "description": "start 时覆盖中继地址，默认用本会话连接的中继" },
                    "sync_key": { "type": "boolean", "description": "start 前把房间 Key 写入仓库密钥，默认 true；关掉则要求仓库里已有该密钥" },
                    "sync_hub": { "type": "boolean", "description": "start 前把中继地址写入仓库密钥（日志里不会出现），默认 true；关掉则要求仓库里已有该密钥" },
                    "wait_seconds": { "type": "integer", "description": format!("start 后等节点上线最多多少秒，默认 {START_WAIT_SECONDS}，0 表示不等，上限 {MAX_WAIT_SECONDS}") },
                    "run_id": { "type": "integer", "description": "stop 时针对的运行号，省略则停掉全部进行中的节点" },
                    "limit": { "type": "integer", "description": "status 最多列出多少条，默认 10" }
                },
                "required": ["action"]
            }
        }
    ])
}

pub async fn call(session: &Session, params: &Value, cancel: &CancellationToken) -> ToolOutput {
    let Some(name) = params.get("name").and_then(Value::as_str) else {
        return ToolOutput::error("缺少工具名 name");
    };
    let args = match params.get("arguments") {
        Some(value) if value.is_object() => value.clone(),
        Some(Value::Null) | None => json!({}),
        Some(_) => return ToolOutput::error("arguments 必须是对象"),
    };
    let result = match name {
        "targets" => Ok(targets(session).await),
        "bash" => bash(session, &args, cancel).await,
        "upload" => upload(session, &args, cancel).await,
        "download" => download(session, &args, cancel).await,
        "actions" => actions(session, &args, cancel).await,
        other => Err(format!("未知工具: {other}")),
    };
    result.unwrap_or_else(ToolOutput::error)
}

async fn targets(session: &Session) -> ToolOutput {
    session.ready().await;
    let mut out = String::new();
    if !session.is_connected() {
        let _ = writeln!(out, "中继 {}：未连接（后台每 1-30 秒重连）", session.hub());
        let _ = writeln!(
            out,
            "当前没有可用目标。稍后重试；若一直为空，检查中继进程与网络。"
        );
        return ToolOutput::ok(out).mark(true);
    }
    let hub = session.hub_name().unwrap_or_else(|| "unknown".to_string());
    let snapshot = session.targets_snapshot();
    let _ = writeln!(
        out,
        "中继 {hub}（{}）：已连接，在线目标 {} 个",
        session.hub(),
        snapshot.len()
    );
    if snapshot.len() <= 1 {
        let _ = writeln!(out, "只有中继自身在线，没有其他被控端。");
    }
    let now = now_ms();
    let _ = writeln!(
        out,
        "{:<16} {:<6} {:<18} {:<8} {:<10} 允许目录",
        "名字", "类型", "系统/架构", "版本", "在线时长"
    );
    for target in &snapshot {
        let kind = match target.kind {
            crate::proto::TargetKind::Hub => "hub",
            crate::proto::TargetKind::Agent => "agent",
        };
        let _ = writeln!(
            out,
            "{:<16} {:<6} {:<18} {:<8} {:<10} {}",
            target.name,
            kind,
            format!("{}/{}", target.os, target.arch),
            target.version,
            human_duration(now.saturating_sub(target.since)),
            target.roots.join(":")
        );
    }
    ToolOutput::ok(out)
}

async fn bash(session: &Session, args: &Value, cancel: &CancellationToken) -> ToolResult {
    let target = require_str(args, "target")?;
    let command = require_str(args, "command")?;
    let report = call::bash(session, &target, &command, cancel)
        .await
        .map_err(|err| describe(&err))?;
    let failed = report.failed();
    Ok(ToolOutput::ok(report.output).mark(failed))
}

async fn upload(session: &Session, args: &Value, cancel: &CancellationToken) -> ToolResult {
    let target = require_str(args, "target")?;
    let remote = require_str(args, "remote_path")?;
    let overwrite = optional_bool(args, "overwrite").unwrap_or(false);
    let create_parents = optional_bool(args, "create_parents").unwrap_or(false);
    if let Some(from) = optional_str(args, "from") {
        let source_path = require_str(args, "source_path")?;
        let report = call::transfer(
            session,
            TransferParams {
                source: from.clone(),
                source_path: source_path.clone(),
                dest: target.clone(),
                dest_path: remote.clone(),
                overwrite,
                create_parents,
                max_size: Some(session.max_file()),
            },
            cancel,
        )
        .await
        .map_err(|err| describe(&err))?;
        return Ok(ToolOutput::ok(format!(
            "已传输 {from}:{source_path} → {target}:{}（{}，sha256 {}）",
            report.path,
            human_size(report.size),
            short_hash(&report.sha256)
        )));
    }
    let local = require_str(args, "local_path")?;
    let report = call::upload(
        session,
        &target,
        Path::new(&local),
        &remote,
        overwrite,
        create_parents,
        cancel,
    )
    .await
    .map_err(|err| describe(&err))?;
    Ok(ToolOutput::ok(format!(
        "已上传 {local} → {}（{}，sha256 {}）",
        report.path,
        human_size(report.size),
        short_hash(&report.sha256)
    )))
}

async fn download(session: &Session, args: &Value, cancel: &CancellationToken) -> ToolResult {
    let target = require_str(args, "target")?;
    let remote = require_str(args, "remote_path")?;
    let overwrite = optional_bool(args, "overwrite").unwrap_or(false);
    if let Some(to) = optional_str(args, "to") {
        let dest_path = require_str(args, "dest_path")?;
        let report = call::transfer(
            session,
            TransferParams {
                source: target.clone(),
                source_path: remote.clone(),
                dest: to.clone(),
                dest_path: dest_path.clone(),
                overwrite,
                create_parents: optional_bool(args, "create_parents").unwrap_or(false),
                max_size: Some(session.max_file()),
            },
            cancel,
        )
        .await
        .map_err(|err| describe(&err))?;
        return Ok(ToolOutput::ok(format!(
            "已传输 {target}:{remote} → {to}:{}（{}，sha256 {}）",
            report.path,
            human_size(report.size),
            short_hash(&report.sha256)
        )));
    }
    let local = require_str(args, "local_path")?;
    let report = call::download(
        session,
        &target,
        &remote,
        Path::new(&local),
        overwrite,
        cancel,
    )
    .await
    .map_err(|err| describe(&err))?;
    Ok(ToolOutput::ok(format!(
        "已下载 {} → {local}（{}，sha256 {}）",
        report.path,
        human_size(report.size),
        short_hash(&report.sha256)
    )))
}

async fn actions(session: &Session, args: &Value, cancel: &CancellationToken) -> ToolResult {
    let action = require_str(args, "action")?;
    let client = GhClient::new(session.github().clone());
    match action.as_str() {
        "start" => {
            let fallback = session.hub().to_string();
            let hub = optional_str(args, "hub").unwrap_or(fallback);
            if is_loopback_hub(&hub) {
                return Err(format!(
                    "中继地址 {hub} 只在 DSH 本机可达，Actions 节点连不上；用 hub 参数指定公网地址"
                ));
            }
            let roots = optional_str(args, "roots").unwrap_or_else(|| "/".to_string());
            let version = optional_str(args, "version").unwrap_or_else(|| "latest".to_string());
            let arch = optional_str(args, "arch").unwrap_or_else(|| DEFAULT_ARCH.to_string());
            let request = StartRequest {
                hub: &hub,
                duration_minutes: optional_u64(args, "duration_minutes")
                    .unwrap_or(DEFAULT_DURATION_MINUTES),
                allow_bash: optional_bool(args, "allow_bash").unwrap_or(true),
                roots: &roots,
                version: &version,
                arch: &arch,
                sync_key: optional_bool(args, "sync_key").unwrap_or(true),
                sync_hub: optional_bool(args, "sync_hub").unwrap_or(true),
            };
            let key = session.key().encode();
            let started = client
                .start(&request, &key, cancel)
                .await
                .map_err(|err| format!("{err:#}"))?;
            let wait = optional_u64(args, "wait_seconds")
                .unwrap_or(START_WAIT_SECONDS)
                .min(MAX_WAIT_SECONDS);
            let mut text = started.text;
            if wait > 0 {
                if wait_target_online(session, &started.node, wait, cancel).await {
                    let _ = writeln!(text, "节点 {} 已上线，可以直接当 target 用。", started.node);
                } else {
                    let _ = writeln!(
                        text,
                        "等了 {wait} 秒还没上线；先做别的，稍后再用 targets 看。"
                    );
                }
            }
            Ok(ToolOutput::ok(text))
        }
        "stop" => {
            let (targets, notes) = client
                .stop_targets(optional_u64(args, "run_id"), cancel)
                .await
                .map_err(|err| format!("{err:#}"))?;
            let mut text = String::new();
            for note in &notes {
                let _ = writeln!(text, "{note}");
            }
            if targets.is_empty() && notes.is_empty() {
                return Ok(ToolOutput::ok(
                    "该工作流没有进行中的运行，没有需要停止的节点。".to_string(),
                ));
            }
            let mut online: Vec<String> = Vec::new();
            let mut pending: Vec<u64> = Vec::new();
            for target in &targets {
                let result = session
                    .call_cancel(
                        &target.node,
                        "shutdown",
                        crate::proto::value::empty_map(),
                        SHUTDOWN_CALL_TIMEOUT,
                        cancel,
                    )
                    .await;
                match result {
                    Ok(reply) if reply.ok => {
                        online.push(target.node.clone());
                        pending.push(target.run_id);
                        let _ = writeln!(
                            text,
                            "已通知节点 {}（#{}）自行退出",
                            target.node, target.run_id
                        );
                    }
                    Ok(reply) => {
                        let reason = reply
                            .error
                            .as_ref()
                            .map(|err| format!("{} {}", err.code, err.message))
                            .unwrap_or_else(|| "节点没有接受关停指令".to_string());
                        let _ = writeln!(
                            text,
                            "节点 {}（#{}）没接受关停指令（{reason}），直接取消运行",
                            target.node, target.run_id
                        );
                        let _ = client.cancel_run(target.run_id, cancel).await;
                    }
                    Err(err) => {
                        let _ = writeln!(
                            text,
                            "节点 {}（#{}）不在线或没应答（{err}），直接取消运行",
                            target.node, target.run_id
                        );
                        let _ = client.cancel_run(target.run_id, cancel).await;
                    }
                }
            }
            if !pending.is_empty() {
                let wait = client
                    .await_finished(&pending, cancel)
                    .await
                    .map_err(|err| format!("{err:#}"))?;
                for (id, status) in &wait.finished {
                    let _ = writeln!(text, "#{id} 已结束：{status}");
                }
                for id in &wait.alive {
                    let _ = client.cancel_run(*id, cancel).await;
                    let _ = writeln!(
                        text,
                        "#{id} 没在 {} 秒内自行退出，已强制取消（这次运行会显示为已取消）",
                        STOP_WAIT_SECONDS
                    );
                }
                if !online.is_empty() {
                    if wait_targets_offline(session, &online, STOP_WAIT_SECONDS, cancel).await {
                        let _ = writeln!(text, "节点 {} 已从 targets 消失。", online.join("、"));
                    } else {
                        let _ = writeln!(text, "节点还在 targets 里，过几十秒再看。");
                    }
                }
                let _ = writeln!(text, "自行退出的运行在 Actions 里保持成功状态。");
            }
            Ok(ToolOutput::ok(text))
        }
        "status" => {
            let online: Vec<String> = session
                .targets_snapshot()
                .iter()
                .map(|target| target.name.clone())
                .collect();
            let limit = optional_u64(args, "limit").unwrap_or(10);
            client
                .status(limit, &online, cancel)
                .await
                .map(ToolOutput::ok)
                .map_err(|err| format!("{err:#}"))
        }
        other => Err(format!("未知 action: {other}，可用 start、stop、status")),
    }
}

async fn wait_target_online(
    session: &Session,
    name: &str,
    seconds: u64,
    cancel: &CancellationToken,
) -> bool {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    loop {
        if session
            .targets_snapshot()
            .iter()
            .any(|target| target.name == name)
        {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::select! {
            _ = cancel.cancelled() => return false,
            _ = tokio::time::sleep(WAIT_POLL) => {}
        }
    }
}

async fn wait_targets_offline(
    session: &Session,
    names: &[String],
    seconds: u64,
    cancel: &CancellationToken,
) -> bool {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    loop {
        let online = session
            .targets_snapshot()
            .iter()
            .filter(|target| names.iter().any(|name| name == &target.name))
            .count();
        if online == 0 {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::select! {
            _ = cancel.cancelled() => return false,
            _ = tokio::time::sleep(WAIT_POLL) => {}
        }
    }
}

fn is_loopback_hub(hub: &str) -> bool {
    let host = hub
        .rsplit_once(':')
        .map(|(host, _)| host)
        .unwrap_or(hub)
        .trim_matches(|ch| ch == '[' || ch == ']');
    host.is_empty()
        || host.eq_ignore_ascii_case("localhost")
        || host.starts_with("127.")
        || host == "0.0.0.0"
        || host == "::1"
}

fn require_str(args: &Value, key: &str) -> Result<String, String> {
    let text = required_str(args, key)?;
    if text.trim().is_empty() {
        return Err(format!("参数 {key} 不能为空"));
    }
    Ok(text)
}

fn required_str(args: &Value, key: &str) -> Result<String, String> {
    match args.get(key).and_then(Value::as_str) {
        Some(text) => Ok(text.to_string()),
        None => Err(format!("缺少参数 {key}")),
    }
}

fn optional_str(args: &Value, key: &str) -> Option<String> {
    args.get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

fn optional_u64(args: &Value, key: &str) -> Option<u64> {
    args.get(key).and_then(Value::as_u64)
}

fn optional_bool(args: &Value, key: &str) -> Option<bool> {
    args.get(key).and_then(Value::as_bool)
}

fn describe(err: &anyhow::Error) -> String {
    if let Some(remote) = err.downcast_ref::<RemoteError>() {
        return format!("错误 [{}]: {}", remote.code, remote.message);
    }
    format!("错误: {err:#}")
}

fn short_hash(hash: &str) -> String {
    hash.chars().take(12).collect()
}

fn human_size(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["KiB", "MiB", "GiB", "TiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64 / 1024.0;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

fn human_duration(ms: u64) -> String {
    let seconds = ms / 1000;
    if seconds < 60 {
        return format!("{seconds}s");
    }
    let minutes = seconds / 60;
    if minutes < 60 {
        return format!("{minutes}m{}s", seconds % 60);
    }
    let hours = minutes / 60;
    if hours < 24 {
        return format!("{hours}h{}m", minutes % 60);
    }
    format!("{}d{}h", hours / 24, hours % 24)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tools_list_covers_every_tool() {
        let tools = list();
        let entries = tools.as_array().unwrap();
        assert_eq!(entries.len(), 5);
        for entry in entries {
            assert!(entry.get("name").is_some());
            assert!(entry.get("description").is_some());
            assert_eq!(entry["inputSchema"]["type"], "object");
        }
    }

    #[test]
    fn formats_size() {
        assert_eq!(human_size(512), "512 B");
        assert_eq!(human_size(2048), "2.0 KiB");
        assert_eq!(human_size(3 * 1024 * 1024), "3.0 MiB");
    }

    #[test]
    fn missing_argument_is_reported() {
        let args = json!({});
        assert_eq!(require_str(&args, "target").unwrap_err(), "缺少参数 target");
        assert_eq!(
            require_str(&json!({"target": "  "}), "target").unwrap_err(),
            "参数 target 不能为空"
        );
    }
}
