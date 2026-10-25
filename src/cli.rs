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
    #[command(about = "查看房间内在线目标，默认只报数量")]
    Targets(Box<TargetsArgs>),
    #[command(about = "把远端目录挂到本机，交给本地工具直接读写")]
    Mount(Box<MountArgs>),
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
    #[arg(long, value_name = "路径", help = "日志目标：stderr、文件路径或 off")]
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
pub struct MountArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    #[arg(long, value_name = "IP:端口", help = "中继地址")]
    pub hub: Option<String>,
    #[arg(long, value_name = "名字", help = "要挂载的目标名")]
    pub target: String,
    #[arg(long, value_name = "路径", help = "本机挂载点，必须是已存在的目录")]
    pub mountpoint: PathBuf,
    #[arg(long, value_name = "路径", help = "目标上的挂载根，默认 /")]
    pub remote_root: Option<String>,
    #[arg(long, help = "只读挂载")]
    pub read_only: bool,
    #[arg(long, value_name = "毫秒", help = "属性与目录缓存时间，默认 2000")]
    pub attr_ttl_ms: Option<u64>,
    #[arg(long, value_name = "字节", help = "影子文件缓存上限，默认 512MiB")]
    pub spool_limit: Option<u64>,
    #[arg(long, value_name = "路径", help = "影子文件目录，默认系统临时目录")]
    pub spool_dir: Option<PathBuf>,
    #[arg(
        long,
        help = "允许其它用户访问挂载点（需 /etc/fuse.conf 允许 user_allow_other）"
    )]
    pub allow_other: bool,
}

#[derive(Args, Debug, Clone)]
pub struct ClientArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    #[arg(long, value_name = "IP:端口", help = "中继地址")]
    pub hub: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct TargetsArgs {
    #[command(flatten)]
    pub client: ClientArgs,
    #[arg(long, help = "连名字、系统和目录一起列出，默认只报在线数量")]
    pub names: bool,
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
                assert_eq!(args.client.common.name.as_deref(), Some("probe"));
                assert_eq!(args.client.hub.as_deref(), Some("127.0.0.1:1"));
                assert!(!args.names);
            }
            other => panic!("期望 targets，收到 {other:?}"),
        }
    }

    #[test]
    fn parses_targets_names() {
        let cli = Cli::try_parse_from([
            "dsh-remote",
            "targets",
            "--hub",
            "127.0.0.1:1",
            "--key",
            "0123456789abcdef",
            "--names",
        ])
        .unwrap();
        match cli.command {
            Command::Targets(args) => assert!(args.names),
            other => panic!("期望 targets，收到 {other:?}"),
        }
    }

    #[test]
    fn parses_mount() {
        let cli = Cli::try_parse_from([
            "dsh-remote",
            "mount",
            "--hub",
            "127.0.0.1:1",
            "--key",
            "0123456789abcdef",
            "--target",
            "nas",
            "--mountpoint",
            "/mnt/nas",
            "--read-only",
        ])
        .unwrap();
        match cli.command {
            Command::Mount(args) => {
                assert_eq!(args.target, "nas");
                assert_eq!(args.mountpoint, std::path::PathBuf::from("/mnt/nas"));
                assert!(args.read_only);
                assert_eq!(args.hub.as_deref(), Some("127.0.0.1:1"));
            }
            other => panic!("期望 mount，收到 {other:?}"),
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
