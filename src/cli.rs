use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "dsh-remote",
    version = crate::VERSION,
    about = "单二进制远程执行网关：DSH 服务端、公网中继、被控端"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    #[command(about = "生成一个房间密钥")]
    Genkey,
    #[command(about = "启动公网中继（本机同时可被控制）")]
    Hub(Box<HubArgs>),
    #[command(about = "启动被控端，主动连接中继")]
    Agent(Box<ClientArgs>),
    #[command(about = "启动 DSH 侧服务端，MCP 走 stdio")]
    Server(Box<ClientArgs>),
    #[command(about = "列出房间内的在线目标")]
    Targets(Box<ClientArgs>),
}

#[derive(Args, Debug, Clone, Default)]
pub struct CommonArgs {
    #[arg(long, value_name = "文件", help = "KEY=VALUE 配置文件")]
    pub config: Option<PathBuf>,
    #[arg(long, value_name = "KEY", help = "房间密钥，base64 或 hex")]
    pub key: Option<String>,
    #[arg(long, value_name = "文件", help = "从文件读取房间密钥")]
    pub key_file: Option<PathBuf>,
    #[arg(long, value_name = "名字", help = "房间内唯一的目标名")]
    pub name: Option<String>,
    #[arg(
        long,
        value_delimiter = ':',
        value_name = "目录",
        help = "允许访问的目录，冒号分隔"
    )]
    pub roots: Option<Vec<String>>,
    #[arg(long, value_name = "BOOL", help = "是否允许 bash 工具")]
    pub allow_bash: Option<bool>,
    #[arg(long, help = "关闭 bash 工具")]
    pub no_bash: bool,
    #[arg(long, value_name = "字符", help = "单次 bash 返回的字符上限")]
    pub max_output_chars: Option<usize>,
    #[arg(long, value_name = "毫秒", help = "单条命令超时毫秒")]
    pub shell_timeout_ms: Option<u64>,
    #[arg(long, value_name = "字节", help = "单文件传输上限")]
    pub max_file: Option<u64>,
    #[arg(long, value_name = "路径", help = "日志目标：stderr 或文件路径")]
    pub log: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct HubArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    #[arg(long, value_name = "IP:端口", help = "监听地址")]
    pub listen: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct ClientArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    #[arg(long, value_name = "IP:端口", help = "中继地址")]
    pub hub: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_targets() {
        let cli = Cli::try_parse_from([
            "dsh-remote",
            "targets",
            "--hub",
            "127.0.0.1:1",
            "--key",
            "0123456789abcdef",
            "--name",
            "probe",
        ])
        .unwrap();
        match cli.command {
            Command::Targets(args) => {
                assert_eq!(args.common.name.as_deref(), Some("probe"));
                assert_eq!(args.hub.as_deref(), Some("127.0.0.1:1"));
            }
            other => panic!("期望 targets，收到 {other:?}"),
        }
    }

    #[test]
    fn parses_roots_list() {
        let cli = Cli::try_parse_from([
            "dsh-remote",
            "hub",
            "--listen",
            "127.0.0.1:1",
            "--roots",
            "/data:/home",
        ])
        .unwrap();
        match cli.command {
            Command::Hub(args) => {
                assert_eq!(
                    args.common.roots,
                    Some(vec!["/data".to_string(), "/home".to_string()])
                );
            }
            other => panic!("期望 hub，收到 {other:?}"),
        }
    }
}
