use std::process::ExitCode;

use anyhow::Result;
use clap::Parser;
use tokio_util::sync::CancellationToken;

use dsh_remote::cli::{Cli, ClientArgs, Command};
use dsh_remote::key::KeyMaterial;
use dsh_remote::proto::{TargetInfo, TargetKind};
use dsh_remote::session::Session;
use dsh_remote::{agent, call, config, hub, logging, server};

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
            server::run(cfg, shutdown_token()).await
        }
        Command::Targets(args) => {
            let session = connect(&args).await?;
            let targets = call::targets(&session).await?;
            print_targets(&targets);
            Ok(())
        }
    }
}

async fn connect(args: &ClientArgs) -> Result<Session> {
    let cfg = config::client_config(args)?;
    logging::init(&cfg.log)?;
    call::connect(&cfg).await
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
