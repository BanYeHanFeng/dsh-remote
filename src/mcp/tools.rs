use std::fmt::Write as _;
use std::path::Path;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::actions::{GhClient, StartRequest, DEFAULT_DURATION_MINUTES, MAX_DURATION_MINUTES};
use crate::call;
use crate::proto::{now_ms, EndStatus};
use crate::session::{RemoteError, Session};

const DEFAULT_BASH_TIMEOUT_MS: u64 = 120_000;
const MAX_BASH_TIMEOUT_MS: u64 = 600_000;
const BASH_DEADLINE_EXTRA: Duration = Duration::from_secs(15);
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
            "description": "在远端用 sh -c 一次性执行命令，返回退出码、stdout、stderr 与耗时。超过 timeout_ms 会杀掉整个进程组；交互式程序（vim、top）不可用。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "target": { "type": "string", "description": TARGET_SCHEMA },
                    "command": { "type": "string", "description": "完整 shell 命令，例如 systemctl status nginx" },
                    "cwd": { "type": "string", "description": "工作目录，必须在目标的 ROOTS 内；省略时用第一个 root" },
                    "timeout_ms": { "type": "integer", "description": "执行超时毫秒，默认 120000，上限 600000" },
                    "stdin": { "type": "string", "description": "写入进程标准输入的内容" },
                    "max_output": { "type": "integer", "description": "单路输出字节上限，默认取目标的 MAX_OUTPUT" }
                },
                "required": ["target", "command"]
            }
        },
        {
            "name": "read",
            "description": "读取远端文本文件，带行号返回，便于后续 edit。二进制文件只返回大小与 mtime，内容请用 download。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "target": { "type": "string", "description": TARGET_SCHEMA },
                    "path": { "type": "string", "description": "远端文件路径，必须在 ROOTS 内" },
                    "offset": { "type": "integer", "description": "跳过前 N 行，续读时传上次返回的 next_offset" },
                    "limit": { "type": "integer", "description": "最多读取的行数，默认 2000，上限 5000" }
                },
                "required": ["target", "path"]
            }
        },
        {
            "name": "write",
            "description": "整体写入远端文本文件：先写临时文件再原子替换，适合新建或小文件覆盖，内容上限 48 KiB，更大的文件用 upload。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "target": { "type": "string", "description": TARGET_SCHEMA },
                    "path": { "type": "string", "description": "远端文件路径，必须在 ROOTS 内" },
                    "content": { "type": "string", "description": "完整文件内容，上限 48 KiB" },
                    "create_parents": { "type": "boolean", "description": "自动创建缺失的父目录" }
                },
                "required": ["target", "path", "content"]
            }
        },
        {
            "name": "edit",
            "description": "在远端文本文件里做字面替换：默认要求 old_string 唯一匹配；replace_all 为真时替换全部。适合改配置项，避免整文件覆盖。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "target": { "type": "string", "description": TARGET_SCHEMA },
                    "path": { "type": "string", "description": "远端文件路径，必须在 ROOTS 内" },
                    "old_string": { "type": "string", "description": "要替换的原文，必须与文件内容逐字一致" },
                    "new_string": { "type": "string", "description": "替换后的文本，可为空字符串" },
                    "replace_all": { "type": "boolean", "description": "替换所有匹配，默认只替换唯一匹配" }
                },
                "required": ["target", "path", "old_string", "new_string"]
            }
        },
        {
            "name": "upload",
            "description": "把 DSH 本机的文件分块上传到远端：先写 .part，按总字节数与 sha256 校验后原子改名。默认拒绝覆盖已有文件。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "target": { "type": "string", "description": TARGET_SCHEMA },
                    "local_path": { "type": "string", "description": "本机（运行 server 的机器）上的源文件路径" },
                    "remote_path": { "type": "string", "description": "远端目标路径，必须在 ROOTS 内" },
                    "overwrite": { "type": "boolean", "description": "允许覆盖远端已有文件" },
                    "create_parents": { "type": "boolean", "description": "自动创建远端父目录" }
                },
                "required": ["target", "local_path", "remote_path"]
            }
        },
        {
            "name": "download",
            "description": "把远端文件分块下载到 DSH 本机：先写 .part，按总字节数与 sha256 校验后原子改名。默认拒绝覆盖本地已有文件。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "target": { "type": "string", "description": TARGET_SCHEMA },
                    "remote_path": { "type": "string", "description": "远端源文件路径，必须在 ROOTS 内" },
                    "local_path": { "type": "string", "description": "本机上的目标路径，父目录必须已存在" },
                    "overwrite": { "type": "boolean", "description": "允许覆盖本地已有文件" }
                },
                "required": ["target", "remote_path", "local_path"]
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
        "read" => read(session, &args, cancel).await,
        "write" => write(session, &args, cancel).await,
        "edit" => edit(session, &args, cancel).await,
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
    let timeout_ms = optional_u64(args, "timeout_ms")
        .unwrap_or(DEFAULT_BASH_TIMEOUT_MS)
        .clamp(100, MAX_BASH_TIMEOUT_MS);

    let mut params = vec![
        ("command", ciborium::value::Value::Text(command.to_string())),
        (
            "timeout_ms",
            ciborium::value::Value::Integer(timeout_ms.into()),
        ),
    ];
    if let Some(cwd) = optional_str(args, "cwd") {
        params.push(("cwd", ciborium::value::Value::Text(cwd.to_string())));
    }
    if let Some(stdin) = optional_str(args, "stdin") {
        params.push(("stdin", ciborium::value::Value::Text(stdin.to_string())));
    }
    if let Some(max_output) = optional_u64(args, "max_output") {
        params.push((
            "max_output",
            ciborium::value::Value::Integer(max_output.into()),
        ));
    }

    let started = Instant::now();
    let result = session
        .call_cancel(
            &target,
            "bash",
            crate::proto::value::map(params),
            Duration::from_millis(timeout_ms) + BASH_DEADLINE_EXTRA,
            cancel,
        )
        .await
        .map_err(|err| describe(&err.into()))?;
    let elapsed = started.elapsed().as_millis();

    let mut out = String::new();
    let exit = match result.exit_code {
        Some(code) => code.to_string(),
        None => "-".to_string(),
    };
    let _ = writeln!(out, "目标 {target}｜退出码 {exit}｜耗时 {elapsed} ms");
    if result.truncated {
        let _ = writeln!(out, "注意：输出超过上限，尾部已截断");
    }
    if !result.stdout.is_empty() {
        let _ = writeln!(out, "--- stdout ---");
        let _ = writeln!(
            out,
            "{}",
            String::from_utf8_lossy(&result.stdout).trim_end()
        );
    }
    if !result.stderr.is_empty() {
        let _ = writeln!(out, "--- stderr ---");
        let _ = writeln!(
            out,
            "{}",
            String::from_utf8_lossy(&result.stderr).trim_end()
        );
    }
    if result.ok && result.stdout.is_empty() && result.stderr.is_empty() {
        let _ = writeln!(out, "（无输出）");
    }
    if let Some(error) = &result.error {
        let _ = writeln!(out, "错误 [{}]: {}", error.code, error.message);
    } else if !result.ok {
        match result.stopped {
            Some(EndStatus::Error) => {
                let _ = writeln!(out, "错误 [timeout]: 远端超时，整个进程组已被杀掉");
            }
            Some(EndStatus::Cancelled) => {
                let _ = writeln!(out, "错误 [cancelled]: 调用已取消");
            }
            _ => {}
        }
    }

    let failed = !result.ok || result.exit_code.map(|code| code != 0).unwrap_or(false);
    Ok(ToolOutput::ok(out).mark(failed))
}

async fn read(session: &Session, args: &Value, cancel: &CancellationToken) -> ToolResult {
    let target = require_str(args, "target")?;
    let path = require_str(args, "path")?;
    let offset = optional_u64(args, "offset").unwrap_or(0);
    let limit = optional_u64(args, "limit").unwrap_or(2000).max(1);
    let report = call::read(session, &target, &path, offset, limit, cancel)
        .await
        .map_err(|err| describe(&err))?;

    if report.binary {
        return Ok(ToolOutput::ok(format!(
            "{} 是二进制文件（{}，mtime {}），只返回元信息。需要内容请用 download 拉到本地再读。",
            report.path,
            human_size(report.size),
            format_time(report.mtime)
        )));
    }

    let mut out = String::new();
    let _ = writeln!(
        out,
        "{}（{}，从第 {} 行起，最多 {} 行）",
        report.path,
        human_size(report.size),
        offset + 1,
        limit
    );
    if report.lines.is_empty() {
        let _ = writeln!(out, "（文件末尾，没有内容）");
    }
    for (index, line) in report.lines.iter().enumerate() {
        let _ = writeln!(out, "{:>6}\t{line}", offset + index as u64 + 1);
    }
    if report.truncated {
        let _ = writeln!(out, "[有超长行被截断]");
    }
    if !report.eof {
        let _ = writeln!(
            out,
            "[还有更多内容，继续读取用 offset={}]",
            report.next_offset
        );
    }
    Ok(ToolOutput::ok(out))
}

async fn write(session: &Session, args: &Value, cancel: &CancellationToken) -> ToolResult {
    let target = require_str(args, "target")?;
    let path = require_str(args, "path")?;
    let content = required_str(args, "content")?;
    let create_parents = optional_bool(args, "create_parents").unwrap_or(false);
    let report = call::write(session, &target, &path, &content, create_parents, cancel)
        .await
        .map_err(|err| describe(&err))?;
    Ok(ToolOutput::ok(format!(
        "已写入 {}（{}，sha256 {}，{}）",
        report.path,
        human_size(report.size),
        short_hash(&report.sha256),
        if report.created { "新建" } else { "覆盖" }
    )))
}

async fn edit(session: &Session, args: &Value, cancel: &CancellationToken) -> ToolResult {
    let target = require_str(args, "target")?;
    let path = require_str(args, "path")?;
    let old_string = required_str(args, "old_string")?;
    let new_string = required_str(args, "new_string")?;
    let replace_all = optional_bool(args, "replace_all").unwrap_or(false);
    let report = call::edit(
        session,
        &target,
        &path,
        &old_string,
        &new_string,
        replace_all,
        cancel,
    )
    .await
    .map_err(|err| describe(&err))?;
    Ok(ToolOutput::ok(format!(
        "已编辑 {}（替换 {} 处，{}，sha256 {}）",
        report.path,
        report.replacements,
        human_size(report.size),
        short_hash(&report.sha256)
    )))
}

async fn upload(session: &Session, args: &Value, cancel: &CancellationToken) -> ToolResult {
    let target = require_str(args, "target")?;
    let local = require_str(args, "local_path")?;
    let remote = require_str(args, "remote_path")?;
    let overwrite = optional_bool(args, "overwrite").unwrap_or(false);
    let create_parents = optional_bool(args, "create_parents").unwrap_or(false);
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
    let local = require_str(args, "local_path")?;
    let overwrite = optional_bool(args, "overwrite").unwrap_or(false);
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
            let request = StartRequest {
                hub: &hub,
                duration_minutes: optional_u64(args, "duration_minutes")
                    .unwrap_or(DEFAULT_DURATION_MINUTES),
                allow_bash: optional_bool(args, "allow_bash").unwrap_or(true),
                roots: &roots,
                version: &version,
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
                    Ok(_) => {
                        online.push(target.node.clone());
                        pending.push(target.run_id);
                        let _ = writeln!(
                            text,
                            "已通知节点 {}（#{}）自行退出",
                            target.node, target.run_id
                        );
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

fn format_time(ms: u64) -> String {
    let seconds = ms / 1000;
    let days = (seconds / 86_400) as i64;
    let rest = seconds % 86_400;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02} UTC",
        rest / 3600,
        (rest % 3600) / 60,
        rest % 60
    )
}

fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let shifted = days + 719_468;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = (shifted - era * 146_097) as u64;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era as i64 + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_pos = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_pos + 2) / 5 + 1) as u32;
    let month = if month_pos < 10 {
        month_pos + 3
    } else {
        month_pos - 9
    } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tools_list_has_eight_entries() {
        let tools = list();
        let entries = tools.as_array().unwrap();
        assert_eq!(entries.len(), 8);
        for entry in entries {
            assert!(entry.get("name").is_some());
            assert!(entry.get("description").is_some());
            assert_eq!(entry["inputSchema"]["type"], "object");
        }
    }

    #[test]
    fn formats_time() {
        assert_eq!(format_time(0), "1970-01-01 00:00:00 UTC");
        assert_eq!(format_time(1_700_000_000_000), "2023-11-14 22:13:20 UTC");
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
