use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use serde_json::json;

use dsh_remote::config;
use dsh_remote::ipc::{IpcClient, IpcEvent, IpcRequest};
use dsh_remote::protocol::Failure;
use dsh_remote::text;
use dsh_remote::{agent, controller, mcp, relay};

#[derive(Parser)]
#[command(
    name = "dsh-remote",
    version,
    about = "面向 DeepSeek Harness 的远程节点控制与文件传输工具",
    disable_help_subcommand = true
)]
struct Cli {
    #[arg(long, global = true, value_name = "FILE", help = "TOML 配置文件路径")]
    config: Option<PathBuf>,
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        help = "控制端本地 socket 路径"
    )]
    socket: Option<PathBuf>,
    #[arg(long, global = true, help = "以 JSON 输出结果")]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "启动控制端常驻服务")]
    Controller(ControllerArgs),
    #[command(about = "启动中继转发服务")]
    Relay(RelayArgs),
    #[command(about = "启动被控端，接入控制端或中继")]
    Agent(AgentArgs),
    #[command(about = "以 MCP 协议对外提供服务")]
    Mcp(McpArgs),
    #[command(about = "列出在线节点")]
    Nodes(NodesArgs),
    #[command(about = "在指定节点执行一次性 bash 命令")]
    Exec(ExecArgs),
    #[command(about = "上传本机文件到指定节点")]
    Upload(UploadArgs),
    #[command(about = "从指定节点下载文件到本机")]
    Download(DownloadArgs),
    #[command(about = "在两个节点之间互传文件")]
    Transfer(TransferArgs),
}

#[derive(Args)]
struct ControllerArgs {
    #[arg(long, value_name = "ADDR", help = "监听地址，如 0.0.0.0:7443")]
    listen: Option<String>,
    #[arg(long, value_name = "ADDR", help = "接入的中继地址")]
    relay: Option<String>,
    #[arg(
        long,
        value_name = "ADDR",
        help = "若提供则同时开启 MCP Streamable HTTP"
    )]
    mcp_http: Option<String>,
    #[arg(long, value_name = "PATH", help = "本地 socket 路径")]
    socket: Option<PathBuf>,
}

#[derive(Args)]
struct RelayArgs {
    #[arg(long, value_name = "ADDR", help = "监听地址，如 0.0.0.0:7443")]
    listen: Option<String>,
}

#[derive(Args)]
struct AgentArgs {
    #[arg(long, value_name = "ADDR", help = "控制端或中继地址")]
    connect: Option<String>,
    #[arg(long, value_name = "ID", help = "节点标识，默认取主机名")]
    id: Option<String>,
    #[arg(long = "tag", value_name = "K=V", help = "节点标签，可重复")]
    tags: Vec<String>,
}

#[derive(Args)]
struct McpArgs {
    #[arg(long, value_name = "ADDR", help = "以 Streamable HTTP 方式监听该地址")]
    http: Option<String>,
}

#[derive(Args)]
struct NodesArgs {
    #[arg(long, value_name = "K=V", help = "只看带有该标签的节点")]
    tag: Option<String>,
}

#[derive(Args)]
struct ExecArgs {
    #[arg(value_name = "NODE", help = "目标节点标识")]
    node: String,
    #[arg(long, value_name = "DIR", help = "工作目录")]
    workdir: Option<String>,
    #[arg(long, value_name = "SECS", default_value_t = 300, help = "超时秒数")]
    timeout: u64,
    #[arg(
        value_name = "COMMAND",
        required = true,
        trailing_var_arg = true,
        allow_hyphen_values = true,
        help = "要执行的命令"
    )]
    command: Vec<String>,
}

#[derive(Args)]
struct UploadArgs {
    #[arg(value_name = "LOCAL", help = "本机源文件")]
    local: String,
    #[arg(value_name = "NODE:PATH", help = "目标节点与路径")]
    target: String,
    #[arg(long, help = "目标文件已存在时不覆盖")]
    no_overwrite: bool,
}

#[derive(Args)]
struct DownloadArgs {
    #[arg(value_name = "NODE:PATH", help = "来源节点与路径")]
    source: String,
    #[arg(value_name = "LOCAL", help = "本机目标路径")]
    local: String,
    #[arg(long, help = "本机文件已存在时不覆盖")]
    no_overwrite: bool,
}

#[derive(Args)]
struct TransferArgs {
    #[arg(value_name = "SRC_NODE:PATH", help = "来源节点与路径")]
    source: String,
    #[arg(value_name = "DST_NODE:PATH", help = "目标节点与路径")]
    target: String,
    #[arg(long, help = "目标文件已存在时不覆盖")]
    no_overwrite: bool,
}

fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("初始化异步运行时失败")?;
    runtime.block_on(dispatch(cli))
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

async fn dispatch(cli: Cli) -> Result<()> {
    let socket = config::resolve_socket(cli.socket.clone(), cli.config.as_deref());
    match cli.command {
        Command::Controller(args) => run_controller(cli.config, args).await,
        Command::Relay(args) => run_relay(cli.config, args).await,
        Command::Agent(args) => run_agent(cli.config, args).await,
        Command::Mcp(args) => run_mcp(socket, args).await,
        Command::Nodes(args) => run_nodes(socket, args, cli.json).await,
        Command::Exec(args) => run_exec(socket, args, cli.json).await,
        Command::Upload(args) => run_upload(socket, args, cli.json).await,
        Command::Download(args) => run_download(socket, args, cli.json).await,
        Command::Transfer(args) => run_transfer(socket, args, cli.json).await,
    }
}

async fn run_controller(config_path: Option<PathBuf>, args: ControllerArgs) -> Result<()> {
    let file = config::load(config_path.as_deref(), "controller")?;
    let listen = config::pick_string(args.listen, file.listen.clone());
    let relay = config::pick_string(args.relay, file.relay.clone());
    if listen.is_none() && relay.is_none() {
        bail!("控制端至少要提供 --listen 或 --relay 之一");
    }
    let socket = args
        .socket
        .or(file.socket)
        .unwrap_or_else(|| PathBuf::from(config::DEFAULT_SOCKET));
    let opts = controller::ControllerOptions {
        listen: listen.map(|addr| config::with_port(&addr)),
        relay: relay.map(|addr| config::with_port(&addr)),
        socket,
        mcp_http: config::pick_string(args.mcp_http, file.mcp_http),
        psk: config::psk(file.psk)?,
    };
    controller::run(opts).await
}

async fn run_relay(config_path: Option<PathBuf>, args: RelayArgs) -> Result<()> {
    let file = config::load(config_path.as_deref(), "relay")?;
    let listen = config::require(
        config::pick_string(args.listen, file.listen),
        "中继监听地址（--listen）",
    )?;
    let opts = relay::RelayOptions {
        listen: config::with_port(&listen),
        psk: config::psk(file.psk)?,
    };
    relay::run(opts).await
}

async fn run_agent(config_path: Option<PathBuf>, args: AgentArgs) -> Result<()> {
    let file = config::load(config_path.as_deref(), "agent")?;
    let connect = config::require(
        config::pick_string(args.connect, file.connect),
        "接入地址（--connect）",
    )?;
    let id = config::pick_string(args.id, file.id).unwrap_or_else(agent::default_node_id);
    dsh_remote::protocol::validate_node_id(&id)?;
    let tags = if args.tags.is_empty() {
        file.tags
    } else {
        args.tags
    };
    let opts = agent::AgentOptions {
        connect: config::with_port(&connect),
        id,
        tags,
        psk: config::psk(file.psk)?,
    };
    agent::run(opts).await
}

async fn run_mcp(socket: PathBuf, args: McpArgs) -> Result<()> {
    match args.http {
        Some(addr) => mcp::serve_http(&addr, socket).await,
        None => mcp::serve_stdio(socket).await,
    }
}

async fn run_nodes(socket: PathBuf, args: NodesArgs, json: bool) -> Result<()> {
    let mut client = IpcClient::connect(&socket).await?;
    client.send(&IpcRequest::Nodes { tag: args.tag }).await?;
    match client.next().await? {
        IpcEvent::Nodes { nodes } => {
            if json {
                println!("{}", serde_json::to_string_pretty(&nodes)?);
                return Ok(());
            }
            if nodes.is_empty() {
                println!("{}", text::nodes_empty());
                return Ok(());
            }
            println!("{}", text::nodes_header(nodes.len()));
            for node in nodes {
                println!(
                    "{}",
                    text::node_line(&node.node_id, &node.tags, &node.via, node.online_secs)
                );
            }
            Ok(())
        }
        other => Err(unexpected(other)),
    }
}

async fn run_exec(socket: PathBuf, args: ExecArgs, json: bool) -> Result<()> {
    use std::io::Write;
    let command = args.command.join(" ");
    let mut client = IpcClient::connect(&socket).await?;
    client
        .send(&IpcRequest::Exec {
            node: args.node.clone(),
            command: command.clone(),
            workdir: args.workdir,
            timeout_secs: args.timeout,
        })
        .await?;

    let mut stdout = String::new();
    let mut stderr = String::new();
    loop {
        match client.next().await? {
            IpcEvent::Stdout { d } => {
                if json {
                    stdout.push_str(&d);
                } else {
                    print!("{}", d);
                    std::io::stdout().flush().ok();
                }
            }
            IpcEvent::Stderr { d } => {
                if json {
                    stderr.push_str(&d);
                } else {
                    eprint!("{}", d);
                    std::io::stderr().flush().ok();
                }
            }
            IpcEvent::Exit {
                code,
                signal,
                timed_out,
            } => {
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "node": args.node,
                            "command": command,
                            "stdout": stdout,
                            "stderr": stderr,
                            "exit_code": code,
                            "signal": signal,
                            "timed_out": timed_out,
                        }))?
                    );
                } else if timed_out {
                    eprintln!("{}", text::timed_out(args.timeout));
                } else if let Some(signal) = signal {
                    eprintln!("{}", text::killed(signal));
                } else {
                    eprintln!("{}", text::exit_code(code.unwrap_or(0)));
                }
                use std::io::Write as _;
                std::io::stdout().flush().ok();
                std::io::stderr().flush().ok();
                let status = if timed_out {
                    124
                } else if let Some(signal) = signal {
                    128 + signal
                } else {
                    code.unwrap_or(0)
                };
                std::process::exit(status);
            }
            IpcEvent::Err { code, message } => {
                return Err(Failure { code, message }.into());
            }
            other => return Err(unexpected(other)),
        }
    }
}

async fn run_upload(socket: PathBuf, args: UploadArgs, json: bool) -> Result<()> {
    let (node, remote_path) = split_target(&args.target)?;
    let mut client = IpcClient::connect(&socket).await?;
    client
        .send(&IpcRequest::Upload {
            node: node.clone(),
            local_path: args.local.clone(),
            remote_path: remote_path.clone(),
            overwrite: !args.no_overwrite,
        })
        .await?;
    match client.next().await? {
        IpcEvent::Done { bytes, sha256, .. } => {
            if json {
                print_json(&json!({
                    "node": node,
                    "remote_path": remote_path,
                    "local_path": args.local,
                    "bytes": bytes,
                    "sha256": sha256,
                }))?;
            } else {
                println!("{}", text::uploaded(&node, &remote_path, bytes, &sha256));
            }
            Ok(())
        }
        IpcEvent::Err { code, message } => Err(Failure { code, message }.into()),
        other => Err(unexpected(other)),
    }
}

async fn run_download(socket: PathBuf, args: DownloadArgs, json: bool) -> Result<()> {
    let (node, remote_path) = split_target(&args.source)?;
    let mut client = IpcClient::connect(&socket).await?;
    client
        .send(&IpcRequest::Download {
            node: node.clone(),
            remote_path: remote_path.clone(),
            local_path: args.local.clone(),
            overwrite: !args.no_overwrite,
        })
        .await?;
    match client.next().await? {
        IpcEvent::Done { bytes, sha256, .. } => {
            if json {
                print_json(&json!({
                    "node": node,
                    "remote_path": remote_path,
                    "local_path": args.local,
                    "bytes": bytes,
                    "sha256": sha256,
                }))?;
            } else {
                println!(
                    "{}",
                    text::downloaded(&args.local, &node, &remote_path, bytes, &sha256)
                );
            }
            Ok(())
        }
        IpcEvent::Err { code, message } => Err(Failure { code, message }.into()),
        other => Err(unexpected(other)),
    }
}

async fn run_transfer(socket: PathBuf, args: TransferArgs, json: bool) -> Result<()> {
    let (src_node, src_path) = split_target(&args.source)?;
    let (dst_node, dst_path) = split_target(&args.target)?;
    let mut client = IpcClient::connect(&socket).await?;
    client
        .send(&IpcRequest::Transfer {
            src_node: src_node.clone(),
            src_path: src_path.clone(),
            dst_node: dst_node.clone(),
            dst_path: dst_path.clone(),
            overwrite: !args.no_overwrite,
        })
        .await?;
    match client.next().await? {
        IpcEvent::Done { bytes, sha256, .. } => {
            if json {
                print_json(&json!({
                    "src_node": src_node,
                    "src_path": src_path,
                    "dst_node": dst_node,
                    "dst_path": dst_path,
                    "bytes": bytes,
                    "sha256": sha256,
                }))?;
            } else {
                println!(
                    "{}",
                    text::transferred(
                        (&src_node, &src_path),
                        (&dst_node, &dst_path),
                        bytes,
                        &sha256
                    )
                );
            }
            Ok(())
        }
        IpcEvent::Err { code, message } => Err(Failure { code, message }.into()),
        other => Err(unexpected(other)),
    }
}

fn print_json(value: &serde_json::Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn unexpected(event: IpcEvent) -> anyhow::Error {
    anyhow::anyhow!("控制端返回了意料之外的响应：{:?}", event)
}

fn split_target(value: &str) -> Result<(String, String)> {
    match value.split_once(':') {
        Some((node, path)) if !node.is_empty() && !path.is_empty() => {
            Ok((node.to_string(), path.to_string()))
        }
        _ => Err(anyhow::anyhow!(
            "目标写法应为 <节点>:<路径>，实际为 {}",
            value
        )),
    }
}
