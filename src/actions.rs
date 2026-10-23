use std::fmt::Write as _;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::time::{timeout, Instant};
use tokio_util::sync::CancellationToken;

use crate::config::GithubConfig;

const GH_TIMEOUT: Duration = Duration::from_secs(120);
const DISPATCH_WAIT: Duration = Duration::from_secs(45);
const POLL_INTERVAL: Duration = Duration::from_secs(2);
const RUN_FIELDS: &str = "databaseId,status,conclusion,displayTitle,createdAt,url";
const STOP_WAIT: Duration = Duration::from_secs(120);
const STOP_POLL: Duration = Duration::from_secs(5);

pub const DEFAULT_DURATION_MINUTES: u64 = 60;
pub const MAX_DURATION_MINUTES: u64 = 350;
pub const DEFAULT_NODE_NAME: &str = "Actions";
pub const DEFAULT_ARCH: &str = "amd64";
pub const RUN_NAME_PREFIX: &str = "临时被控端 ";

pub struct GhClient {
    cfg: GithubConfig,
}

#[derive(Debug)]
pub struct Started {
    pub run_id: u64,
    pub node: String,
    pub text: String,
}

#[derive(Debug)]
pub struct StopTarget {
    pub run_id: u64,
    pub node: String,
}

#[derive(Debug)]
pub struct StopWait {
    pub finished: Vec<(u64, String)>,
    pub alive: Vec<u64>,
}

pub struct StartRequest<'a> {
    pub hub: &'a str,
    pub duration_minutes: u64,
    pub allow_bash: bool,
    pub roots: &'a str,
    pub version: &'a str,
    pub arch: &'a str,
    pub sync_key: bool,
    pub sync_hub: bool,
}

struct RunSummary {
    id: u64,
    status: String,
    conclusion: String,
    title: String,
    created_at: String,
    url: String,
}

impl RunSummary {
    fn node_name(&self) -> String {
        self.title
            .strip_prefix(RUN_NAME_PREFIX)
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_string)
            .unwrap_or_else(default_node_name)
    }

    fn active(&self) -> bool {
        self.status != "completed"
    }
}

pub fn default_node_name() -> String {
    DEFAULT_NODE_NAME.to_string()
}

impl GhClient {
    pub fn new(cfg: GithubConfig) -> Self {
        Self { cfg }
    }

    pub fn workflow(&self) -> &str {
        &self.cfg.workflow
    }

    pub fn repo(&self) -> Result<&str> {
        let repo = self
            .cfg
            .repo
            .as_deref()
            .map(str::trim)
            .filter(|repo| !repo.is_empty())
            .context("未配置 GH_REPO：在 server.conf 里加 GH_REPO=owner/repo，或用环境变量 DSH_REMOTE_GH_REPO")?;
        let (owner, name) = repo
            .split_once('/')
            .ok_or_else(|| anyhow!("GH_REPO 需要 owner/repo 形式: {repo}"))?;
        if owner.is_empty() || name.is_empty() || !is_safe_token(repo) {
            bail!("GH_REPO 含非法字符: {repo}");
        }
        Ok(repo)
    }

    pub async fn start(
        &self,
        request: &StartRequest<'_>,
        key: &str,
        cancel: &CancellationToken,
    ) -> Result<Started> {
        let repo = self.repo()?.to_string();
        if request.duration_minutes < 1 || request.duration_minutes > MAX_DURATION_MINUTES {
            bail!(
                "duration_minutes 需要在 1 到 {MAX_DURATION_MINUTES} 之间，收到 {}",
                request.duration_minutes
            );
        }
        if request.roots.trim().is_empty() {
            bail!("roots 不能为空");
        }
        let arch = normalize_arch(request.arch)
            .ok_or_else(|| anyhow!("arch 只支持 amd64 或 arm64，收到 {}", request.arch.trim()))?;
        if !is_safe_token(&self.cfg.git_ref) {
            bail!("GH_REF 含非法字符: {}", self.cfg.git_ref);
        }
        if !is_safe_token(&self.cfg.workflow) {
            bail!("GH_WORKFLOW 含非法字符: {}", self.cfg.workflow);
        }
        if request.sync_hub {
            self.set_secret(&repo, &self.cfg.hub_secret, request.hub.trim(), cancel)
                .await
                .context("把中继地址写入仓库密钥失败")?;
        }
        if request.sync_key {
            self.set_secret(&repo, &self.cfg.secret, key, cancel)
                .await
                .context("把房间 Key 写入仓库密钥失败")?;
        }

        let before = self.latest_run_id(&repo, cancel).await?;
        let mut args = vec![
            "api".to_string(),
            "-X".to_string(),
            "POST".to_string(),
            format!(
                "repos/{repo}/actions/workflows/{}/dispatches",
                self.cfg.workflow
            ),
            "-f".to_string(),
            format!("ref={}", self.cfg.git_ref),
        ];
        let inputs = vec![
            ("duration", request.duration_minutes.to_string()),
            (
                "allow_bash",
                if request.allow_bash {
                    "1".to_string()
                } else {
                    "0".to_string()
                },
            ),
            ("roots", request.roots.trim().to_string()),
            ("version", request.version.trim().to_string()),
            ("arch", arch.to_string()),
        ];
        for (name, value) in inputs {
            args.push("-f".to_string());
            args.push(format!("inputs[{name}]={value}"));
        }
        self.run(&args, None, cancel)
            .await
            .context("派发工作流失败（确认工作流文件已在 GH_REF 分支上）")?;

        let run = self.wait_new_run(&repo, before, cancel).await?;
        let node = default_node_name();

        let mut out = String::new();
        let _ = writeln!(out, "已派发 Actions 临时被控端");
        let _ = writeln!(out, "仓库     {repo}");
        let _ = writeln!(
            out,
            "工作流   {}（分支 {}）",
            self.cfg.workflow, self.cfg.git_ref
        );
        let _ = writeln!(out, "运行     #{} 状态 {}", run.id, describe_status(&run));
        let _ = writeln!(out, "节点名   {node}");
        let _ = writeln!(out, "架构     {arch}");
        let _ = writeln!(
            out,
            "中继     已写入仓库密钥 {}（不在运行日志里出现）",
            self.cfg.hub_secret
        );
        let _ = writeln!(out, "寿命     {} 分钟后自动下线", request.duration_minutes);
        if !run.url.is_empty() {
            let _ = writeln!(out, "日志     {}", run.url);
        }
        let _ = writeln!(
            out,
            "接下来运行号记作 {id}；用 targets 等节点上线（通常 30-90 秒），提前下线用 actions stop，run_id 传 {id}。",
            id = run.id
        );
        Ok(Started {
            run_id: run.id,
            node,
            text: out,
        })
    }

    pub async fn stop_targets(
        &self,
        run_id: Option<u64>,
        cancel: &CancellationToken,
    ) -> Result<(Vec<StopTarget>, Vec<String>)> {
        let repo = self.repo()?.to_string();
        let runs = match run_id {
            Some(id) => vec![self.run_detail(&repo, id, cancel).await?],
            None => self
                .list_runs(&repo, 30, cancel)
                .await?
                .into_iter()
                .filter(|run| run.active())
                .collect(),
        };
        let mut targets = Vec::new();
        let mut notes = Vec::new();
        for run in runs {
            if !run.active() {
                notes.push(format!(
                    "#{} 已经结束（{}），节点不需要停。",
                    run.id,
                    describe_status(&run)
                ));
                continue;
            }
            targets.push(StopTarget {
                run_id: run.id,
                node: run.node_name(),
            });
        }
        Ok((targets, notes))
    }

    pub async fn await_finished(
        &self,
        ids: &[u64],
        cancel: &CancellationToken,
    ) -> Result<StopWait> {
        let repo = self.repo()?.to_string();
        let deadline = Instant::now() + STOP_WAIT;
        let mut pending = ids.to_vec();
        loop {
            let mut alive = Vec::new();
            for id in &pending {
                if self.run_detail(&repo, *id, cancel).await?.active() {
                    alive.push(*id);
                }
            }
            pending = alive;
            if pending.is_empty() || Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(STOP_POLL).await;
        }
        let mut finished = Vec::new();
        for id in ids.iter().filter(|id| !pending.contains(id)) {
            let run = self.run_detail(&repo, *id, cancel).await?;
            finished.push((*id, describe_status(&run)));
        }
        Ok(StopWait {
            finished,
            alive: pending,
        })
    }

    pub async fn cancel_run(&self, id: u64, cancel: &CancellationToken) -> Result<()> {
        let repo = self.repo()?.to_string();
        let args = vec![
            "run".to_string(),
            "cancel".to_string(),
            id.to_string(),
            "-R".to_string(),
            repo,
        ];
        self.run(&args, None, cancel).await.map(|_| ())
    }

    pub async fn status(
        &self,
        limit: u64,
        online: &[String],
        cancel: &CancellationToken,
    ) -> Result<String> {
        let repo = self.repo()?.to_string();
        let runs = self.list_runs(&repo, limit.clamp(1, 50), cancel).await?;
        if runs.is_empty() {
            return Ok(format!(
                "工作流 {} 还没有运行记录，用 actions start 开一个临时节点。",
                self.cfg.workflow
            ));
        }
        let now = crate::time::now_epoch();
        let mut out = String::new();
        let _ = writeln!(
            out,
            "运行号      状态          节点            在线   已运行      结论"
        );
        for run in &runs {
            let node = run.node_name();
            let live = if online.iter().any(|name| name == &node) {
                "在线"
            } else {
                "-"
            };
            let _ = writeln!(
                out,
                "{:<12} {:<13} {:<16} {:<6} {:<11} {}",
                run.id,
                run.status,
                node,
                live,
                age_text(now - created_epoch(&run.created_at)),
                if run.conclusion.is_empty() {
                    "-"
                } else {
                    &run.conclusion
                }
            );
        }
        let active = runs.iter().filter(|run| run.active()).count();
        let _ = writeln!(
            out,
            "进行中 {active} 个；停掉全部用 actions stop，只停某个用 actions stop 加 run_id。"
        );
        Ok(out)
    }

    async fn run_detail(
        &self,
        repo: &str,
        id: u64,
        cancel: &CancellationToken,
    ) -> Result<RunSummary> {
        let args = vec![
            "run".to_string(),
            "view".to_string(),
            id.to_string(),
            "-R".to_string(),
            repo.to_string(),
            "--json".to_string(),
            RUN_FIELDS.to_string(),
        ];
        let out = self.run(&args, None, cancel).await?;
        parse_run(&out).with_context(|| format!("解析运行 #{id} 失败"))
    }

    async fn set_secret(
        &self,
        repo: &str,
        name: &str,
        value: &str,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let args = vec![
            "secret".to_string(),
            "set".to_string(),
            name.to_string(),
            "-R".to_string(),
            repo.to_string(),
        ];
        self.run(&args, Some(value.to_string()), cancel)
            .await
            .map(|_| ())
    }

    async fn latest_run_id(&self, repo: &str, cancel: &CancellationToken) -> Result<Option<u64>> {
        Ok(self
            .list_runs(repo, 1, cancel)
            .await?
            .first()
            .map(|run| run.id))
    }

    async fn wait_new_run(
        &self,
        repo: &str,
        before: Option<u64>,
        cancel: &CancellationToken,
    ) -> Result<RunSummary> {
        let deadline = Instant::now() + DISPATCH_WAIT;
        let floor = before.unwrap_or(0);
        loop {
            let runs = self.list_runs(repo, 5, cancel).await?;
            if let Some(run) = runs.into_iter().find(|run| run.id > floor) {
                return Ok(run);
            }
            if Instant::now() >= deadline {
                bail!(
                    "已派发，但 {} 秒内没看到新的运行，检查 GH_REF={} 上是否有该工作流",
                    DISPATCH_WAIT.as_secs(),
                    self.cfg.git_ref
                );
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    async fn list_runs(
        &self,
        repo: &str,
        limit: u64,
        cancel: &CancellationToken,
    ) -> Result<Vec<RunSummary>> {
        let args = vec![
            "run".to_string(),
            "list".to_string(),
            "--workflow".to_string(),
            self.cfg.workflow.clone(),
            "-R".to_string(),
            repo.to_string(),
            "--limit".to_string(),
            limit.to_string(),
            "--json".to_string(),
            RUN_FIELDS.to_string(),
        ];
        let out = self.run(&args, None, cancel).await?;
        let value: Value = serde_json::from_str(&out).context("解析 gh run list 输出失败")?;
        let mut runs = Vec::new();
        for item in value.as_array().cloned().unwrap_or_default() {
            let Some(id) = item.get("databaseId").and_then(Value::as_u64) else {
                continue;
            };
            runs.push(RunSummary {
                id,
                status: text_field(&item, "status"),
                conclusion: text_field(&item, "conclusion"),
                title: text_field(&item, "displayTitle"),
                created_at: text_field(&item, "createdAt"),
                url: text_field(&item, "url"),
            });
        }
        Ok(runs)
    }

    async fn run(
        &self,
        args: &[String],
        stdin: Option<String>,
        cancel: &CancellationToken,
    ) -> Result<String> {
        let mut command = Command::new(&self.cfg.binary);
        command
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .env("GH_PROMPT_DISABLED", "1")
            .env("NO_COLOR", "1");
        if let Some(token) = &self.cfg.token {
            command.env("GH_TOKEN", token);
        }
        let mut child = command.spawn().with_context(|| {
            format!(
                "启动 {} 失败：本机需要安装 gh 并登录，或在配置里给 GH_TOKEN",
                self.cfg.binary
            )
        })?;
        if let Some(input) = stdin {
            let mut pipe = child.stdin.take().context("gh 标准输入不可用")?;
            pipe.write_all(input.as_bytes())
                .await
                .context("写入 gh 标准输入失败")?;
        }
        drop(child.stdin.take());
        let stdout = child.stdout.take().context("gh 标准输出不可用")?;
        let stderr = child.stderr.take().context("gh 标准错误不可用")?;
        let waiter = async move {
            let (out, err) = tokio::join!(read_all(stdout), read_all(stderr));
            let status = child.wait().await?;
            Ok::<_, std::io::Error>((status, out?, err?))
        };
        let (status, out, err) = tokio::select! {
            _ = cancel.cancelled() => bail!("gh 调用已取消"),
            result = timeout(GH_TIMEOUT, waiter) => match result {
                Ok(result) => result.context("等待 gh 结束失败")?,
                Err(_) => bail!("gh 执行超过 {} 秒", GH_TIMEOUT.as_secs()),
            },
        };
        if !status.success() {
            let detail = first_text(&err)
                .or_else(|| first_text(&out))
                .unwrap_or_default();
            bail!("gh {} 失败: {detail}", command_label(args));
        }
        Ok(String::from_utf8_lossy(&out).to_string())
    }
}

async fn read_all(mut pipe: impl AsyncRead + Unpin) -> std::io::Result<Vec<u8>> {
    let mut buffer = Vec::new();
    pipe.read_to_end(&mut buffer).await?;
    Ok(buffer)
}

fn first_text(bytes: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(bytes);
    let text = text.trim();
    if text.is_empty() {
        None
    } else {
        Some(text.lines().take(8).collect::<Vec<_>>().join("；"))
    }
}

fn command_label(args: &[String]) -> String {
    args.iter().take(3).cloned().collect::<Vec<_>>().join(" ")
}

fn parse_run(text: &str) -> Option<RunSummary> {
    let value: Value = serde_json::from_str(text).ok()?;
    let id = value.get("databaseId").and_then(Value::as_u64)?;
    Some(RunSummary {
        id,
        status: text_field(&value, "status"),
        conclusion: text_field(&value, "conclusion"),
        title: text_field(&value, "displayTitle"),
        created_at: text_field(&value, "createdAt"),
        url: text_field(&value, "url"),
    })
}

fn text_field(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn describe_status(run: &RunSummary) -> String {
    if run.conclusion.is_empty() {
        run.status.clone()
    } else {
        format!("{} {}", run.status, run.conclusion)
    }
}

fn normalize_arch(text: &str) -> Option<&'static str> {
    match text.trim().to_ascii_lowercase().as_str() {
        "" | "amd64" | "x86" | "x86_64" | "x64" => Some(DEFAULT_ARCH),
        "arm64" | "aarch64" => Some("arm64"),
        _ => None,
    }
}

fn is_safe_token(text: &str) -> bool {
    !text.is_empty()
        && text
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || "-_./".contains(ch))
}

fn created_epoch(stamp: &str) -> i64 {
    crate::time::parse_stamp(stamp).unwrap_or_else(crate::time::now_epoch)
}

fn age_text(seconds: i64) -> String {
    let seconds = seconds.max(0);
    if seconds < 60 {
        format!("{seconds} 秒")
    } else if seconds < 3600 {
        format!("{} 分钟", seconds / 60)
    } else if seconds < 86_400 {
        format!("{} 小时 {} 分钟", seconds / 3600, seconds % 3600 / 60)
    } else {
        format!("{} 天 {} 小时", seconds / 86_400, seconds % 86_400 / 3600)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_actions_name_when_title_has_no_suffix() {
        let run = RunSummary {
            id: 42,
            status: "in_progress".into(),
            conclusion: String::new(),
            title: "临时被控端".into(),
            created_at: "2026-01-02T03:04:05Z".into(),
            url: String::new(),
        };
        assert_eq!(run.node_name(), DEFAULT_NODE_NAME);
        assert!(run.active());
    }

    #[test]
    fn falls_back_to_default_name() {
        let run = RunSummary {
            id: 7,
            status: "completed".into(),
            conclusion: "success".into(),
            title: "其他标题".into(),
            created_at: String::new(),
            url: String::new(),
        };
        assert_eq!(run.node_name(), DEFAULT_NODE_NAME);
        assert!(!run.active());
    }

    #[test]
    fn keeps_legacy_title_suffix() {
        let run = RunSummary {
            id: 9,
            status: "in_progress".into(),
            conclusion: String::new(),
            title: "临时被控端 gh-9".into(),
            created_at: String::new(),
            url: String::new(),
        };
        assert_eq!(run.node_name(), "gh-9");
    }

    #[test]
    fn parses_timestamp() {
        assert_eq!(created_epoch("1970-01-01T00:00:00Z"), 0);
        assert_eq!(created_epoch("2023-11-14T22:13:20Z"), 1_700_000_000);
    }

    #[test]
    fn formats_age() {
        assert_eq!(age_text(9), "9 秒");
        assert_eq!(age_text(180), "3 分钟");
        assert_eq!(age_text(3_700), "1 小时 1 分钟");
        assert_eq!(age_text(90_000), "1 天 1 小时");
        assert_eq!(age_text(-5), "0 秒");
    }

    #[test]
    fn rejects_unsafe_tokens() {
        assert!(is_safe_token("ephemeral-node.yml"));
        assert!(is_safe_token("owner/repo"));
        assert!(!is_safe_token(""));
        assert!(!is_safe_token("a b"));
        assert!(!is_safe_token("a;rm"));
    }

    #[test]
    fn normalizes_arch_aliases() {
        assert_eq!(normalize_arch(""), Some(DEFAULT_ARCH));
        assert_eq!(normalize_arch("amd64"), Some("amd64"));
        assert_eq!(normalize_arch("X86_64"), Some("amd64"));
        assert_eq!(normalize_arch(" aarch64 "), Some("arm64"));
        assert_eq!(normalize_arch("arm64"), Some("arm64"));
        assert_eq!(normalize_arch("riscv64"), None);
    }
}
