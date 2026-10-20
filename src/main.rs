use std::io::{Read as _, Write};
use std::process::ExitCode;
use std::time::Duration;

use anyhow::{anyhow, Result};
use ciborium::value::Value;
use clap::Parser;
use tokio_util::sync::CancellationToken;

use dsh_remote::cli::{Cli, ClientArgs, Command, FileAction, FileArgs, RunArgs};
use dsh_remote::key::KeyMaterial;
use dsh_remote::proto::{EndStatus, TargetInfo, TargetKind};
use dsh_remote::session::Session;
use dsh_remote::{agent, call, config, hub, logging, proto, server};

#[tokio::main]
async fn main() -> ExitCode {
    if let Err(err) = run().await {
        eprintln!("错误: {err:#}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Genkey => {
            println!("{}", KeyMaterial::generate().encode());
            Ok(())
        }
        Command::Hub(args) => {
            let cfg = config::hub_config(&args)?;
            logging::init(&cfg.log)?;
            hub::run(cfg, shutdown_token()).await
        }
        Command::Agent(args) => {
            let cfg = config::client_config(&args)?;
            logging::init(&cfg.log)?;
            agent::run(cfg, shutdown_token()).await
        }
        Command::Server(args) => {
            let cfg = config::client_config(&args)?;
            logging::init(&cfg.log)?;
            let result = server::run(cfg, shutdown_token()).await;
            std::process::exit(match result {
                Ok(()) => 0,
                Err(err) => {
                    eprintln!("错误: {err:#}");
                    1
                }
            });
        }
        Command::Targets(args) => {
            let session = connect(&args).await?;
            let targets = call::targets(&session).await?;
            print_targets(&targets);
            Ok(())
        }
        Command::Run(args) => {
            let session = connect(&args.client).await?;
            run_remote(&session, &args).await
        }
        Command::File(args) => run_file(&args).await,
    }
}

async fn run_file(args: &FileArgs) -> Result<()> {
    let cancel = CancellationToken::new();
    match &args.action {
        FileAction::Read(args) => {
            let session = connect(&args.target.client).await?;
            let report = call::read(
                &session,
                &args.target.target,
                &args.path,
                args.offset,
                args.limit,
                &cancel,
            )
            .await?;
            if report.binary {
                println!(
                    "二进制文件：{} 字节，mtime {}，只返回元信息",
                    report.size, report.mtime
                );
                return Ok(());
            }
            let mut stdout = std::io::stdout().lock();
            for line in &report.lines {
                writeln!(stdout, "{line}")?;
            }
            stdout.flush()?;
            if report.truncated {
                eprintln!("[dsh-remote] 内容过长，已截断");
            }
            if !report.eof {
                eprintln!(
                    "[dsh-remote] 还有更多内容，继续读取用 --offset {}",
                    report.next_offset
                );
            }
            Ok(())
        }
        FileAction::Write(args) => {
            let session = connect(&args.target.client).await?;
            let content = read_source(args.content.as_deref(), args.from.as_deref(), args.stdin)?;
            let report = call::write(
                &session,
                &args.target.target,
                &args.path,
                &content,
                args.create_parents,
                &cancel,
            )
            .await?;
            println!(
                "已写入 {}（{} 字节，sha256 {}，{}）",
                report.path,
                report.size,
                report.sha256,
                if report.created { "新建" } else { "覆盖" }
            );
            Ok(())
        }
        FileAction::Edit(args) => {
            let session = connect(&args.target.client).await?;
            let old = read_source(args.old.as_deref(), args.old_file.as_deref(), false)?;
            let new = read_source(args.new.as_deref(), args.new_file.as_deref(), false)?;
            let report = call::edit(
                &session,
                &args.target.target,
                &args.path,
                &old,
                &new,
                args.replace_all,
                &cancel,
            )
            .await?;
            println!(
                "已编辑 {}（替换 {} 处，{} 字节，sha256 {}）",
                report.path, report.replacements, report.size, report.sha256
            );
            Ok(())
        }
        FileAction::List(args) => {
            let session = connect(&args.target.client).await?;
            let report = call::list(
                &session,
                &args.target.target,
                &args.path,
                args.depth,
                &cancel,
            )
            .await?;
            for entry in &report.entries {
                let size = entry
                    .size
                    .map(|size| size.to_string())
                    .unwrap_or_else(|| "-".to_string());
                println!("{:<7} {:>12} {}", entry.kind, size, entry.path);
            }
            if report.skipped > 0 {
                eprintln!("[dsh-remote] 跳过 {} 个越界或不可读的条目", report.skipped);
            }
            if report.truncated {
                eprintln!("[dsh-remote] 条目过多，已截断");
            }
            Ok(())
        }
        FileAction::Upload(args) => {
            let session = connect(&args.target.client).await?;
            let report = call::upload(
                &session,
                &args.target.target,
                &args.local,
                &args.remote,
                args.overwrite,
                args.create_parents,
                &cancel,
            )
            .await?;
            println!(
                "已上传 {}（{} 字节，sha256 {}）",
                report.path, report.size, report.sha256
            );
            Ok(())
        }
        FileAction::Download(args) => {
            let session = connect(&args.target.client).await?;
            let report = call::download(
                &session,
                &args.target.target,
                &args.remote,
                &args.local,
                args.overwrite,
                &cancel,
            )
            .await?;
            println!(
                "已下载 {}（{} 字节，sha256 {}）",
                report.path, report.size, report.sha256
            );
            Ok(())
        }
    }
}

async fn connect(args: &ClientArgs) -> Result<Session> {
    let cfg = config::client_config(args)?;
    logging::init(&cfg.log)?;
    call::connect(&cfg).await
}

fn read_source(
    text: Option<&str>,
    file: Option<&std::path::Path>,
    use_stdin: bool,
) -> Result<String> {
    if let Some(text) = text {
        return Ok(text.to_string());
    }
    if let Some(path) = file {
        return std::fs::read_to_string(path)
            .map_err(|err| anyhow!("读取 {} 失败: {err}", path.display()));
    }
    if use_stdin {
        let mut buffer = String::new();
        std::io::stdin().read_to_string(&mut buffer)?;
        return Ok(buffer);
    }
    Err(anyhow!("缺少内容来源"))
}

async fn run_remote(session: &Session, args: &RunArgs) -> Result<()> {
    let mut pairs: Vec<(&str, Value)> = vec![("command", Value::Text(shell_join(&args.command)))];
    if let Some(cwd) = args.cwd.as_deref() {
        pairs.push(("cwd", Value::Text(cwd.to_string())));
    }
    pairs.push(("timeout_ms", Value::Integer(args.timeout_ms.into())));
    if let Some(max_output) = args.output_limit {
        pairs.push(("max_output", Value::Integer((max_output as u64).into())));
    }

    let result = call::call(
        session,
        &args.target,
        "bash",
        proto::value::map(pairs),
        Duration::from_millis(args.timeout_ms + 5_000),
        &CancellationToken::new(),
    )
    .await?;

    std::io::stdout().write_all(&result.stdout)?;
    std::io::stdout().flush()?;
    std::io::stderr().write_all(&result.stderr)?;
    std::io::stderr().flush()?;

    if result.truncated {
        eprintln!("[dsh-remote] 输出超过上限，已截断");
    }
    if let Some(error) = &result.error {
        eprintln!("[dsh-remote] {}: {}", error.code, error.message);
    }

    let code = if result.ok {
        result.exit_code.unwrap_or(0)
    } else if let Some(code) = result.exit_code {
        code
    } else if result.stopped == Some(EndStatus::Cancelled) {
        130
    } else if result.stopped == Some(EndStatus::Error) {
        124
    } else {
        1
    };
    std::process::exit(code.clamp(0, 255));
}

fn shell_join(args: &[String]) -> String {
    args.iter()
        .map(|arg| {
            let safe = !arg.is_empty()
                && arg
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || "-_./:=@%+".contains(ch));
            if safe {
                arg.clone()
            } else {
                format!("'{}'", arg.replace('\'', "'\\''"))
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn print_targets(targets: &[TargetInfo]) {
    println!(
        "{:<16} {:<6} {:<10} {:<10} 目录",
        "名字", "类型", "系统", "架构"
    );
    for target in targets {
        let kind = match target.kind {
            TargetKind::Hub => "hub",
            TargetKind::Agent => "agent",
        };
        println!(
            "{:<16} {:<6} {:<10} {:<10} {}",
            target.name,
            kind,
            target.os,
            target.arch,
            target.roots.join(":")
        );
    }
}

fn shutdown_token() -> CancellationToken {
    let token = CancellationToken::new();
    let signal = token.clone();
    tokio::spawn(async move {
        wait_for_signal().await;
        signal.cancel();
    });
    token
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
