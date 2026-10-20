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
    #[command(about = "向远端目标执行一条命令（调试入口）")]
    Run(Box<RunArgs>),
    #[command(about = "列出房间内的在线目标")]
    Targets(Box<ClientArgs>),
    #[command(about = "文件工具调试入口")]
    File(Box<FileArgs>),
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
    #[arg(long, value_name = "字节", help = "单次输出上限")]
    pub max_output: Option<usize>,
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

#[derive(Args, Debug, Clone)]
pub struct RunArgs {
    #[command(flatten)]
    pub client: ClientArgs,
    #[arg(long, value_name = "目标", default_value = "hub", help = "目标名")]
    pub target: String,
    #[arg(
        long,
        value_name = "毫秒",
        default_value_t = 120_000,
        help = "执行超时"
    )]
    pub timeout_ms: u64,
    #[arg(long, value_name = "目录", help = "远端工作目录")]
    pub cwd: Option<String>,
    #[arg(long = "output-limit", value_name = "字节", help = "输出上限")]
    pub output_limit: Option<usize>,
    #[arg(
        last = true,
        required = true,
        value_name = "命令",
        help = "放在 -- 之后的命令"
    )]
    pub command: Vec<String>,
}

#[derive(Args, Debug)]
pub struct FileArgs {
    #[command(subcommand)]
    pub action: FileAction,
}

#[derive(Subcommand, Debug)]
pub enum FileAction {
    #[command(about = "读取远端文本文件，带行号")]
    Read(FileReadArgs),
    #[command(about = "写入远端文本文件，临时文件加原子替换")]
    Write(FileWriteArgs),
    #[command(about = "在远端文本文件中做字面替换")]
    Edit(FileEditArgs),
    #[command(about = "列出远端目录")]
    List(FileListArgs),
    #[command(about = "上传本地文件到远端")]
    Upload(FileUploadArgs),
    #[command(about = "从远端下载文件到本地")]
    Download(FileDownloadArgs),
}

#[derive(Args, Debug, Clone)]
pub struct FileTargetArgs {
    #[command(flatten)]
    pub client: ClientArgs,
    #[arg(long, value_name = "目标", default_value = "hub", help = "目标名")]
    pub target: String,
}

#[derive(Args, Debug)]
pub struct FileReadArgs {
    #[command(flatten)]
    pub target: FileTargetArgs,
    #[arg(long, value_name = "远端路径")]
    pub path: String,
    #[arg(long, value_name = "行号", default_value_t = 0, help = "跳过前 N 行")]
    pub offset: u64,
    #[arg(
        long,
        value_name = "行数",
        default_value_t = 2000,
        help = "最多读取的行数"
    )]
    pub limit: u64,
}

#[derive(Args, Debug)]
#[command(group(
    clap::ArgGroup::new("source")
        .required(true)
        .args(["content", "from", "stdin"])
))]
pub struct FileWriteArgs {
    #[command(flatten)]
    pub target: FileTargetArgs,
    #[arg(long, value_name = "远端路径")]
    pub path: String,
    #[arg(long, value_name = "文本", group = "source", help = "直接给出内容")]
    pub content: Option<String>,
    #[arg(
        long = "from",
        value_name = "本地文件",
        group = "source",
        help = "从本地文件读取内容"
    )]
    pub from: Option<PathBuf>,
    #[arg(long, group = "source", help = "从 stdin 读取内容")]
    pub stdin: bool,
    #[arg(long, help = "自动创建父目录")]
    pub create_parents: bool,
}

#[derive(Args, Debug)]
#[command(group(
    clap::ArgGroup::new("old-source")
        .required(true)
        .args(["old", "old_file"])
))]
#[command(group(
    clap::ArgGroup::new("new-source")
        .required(true)
        .args(["new", "new_file"])
))]
pub struct FileEditArgs {
    #[command(flatten)]
    pub target: FileTargetArgs,
    #[arg(long, value_name = "远端路径")]
    pub path: String,
    #[arg(
        long = "old",
        value_name = "文本",
        group = "old-source",
        help = "要替换的原文"
    )]
    pub old: Option<String>,
    #[arg(
        long = "old-file",
        value_name = "本地文件",
        group = "old-source",
        help = "从文件读取要替换的原文"
    )]
    pub old_file: Option<PathBuf>,
    #[arg(
        long = "new",
        value_name = "文本",
        group = "new-source",
        help = "替换后的文本"
    )]
    pub new: Option<String>,
    #[arg(
        long = "new-file",
        value_name = "本地文件",
        group = "new-source",
        help = "从文件读取替换后的文本"
    )]
    pub new_file: Option<PathBuf>,
    #[arg(long, help = "替换所有匹配")]
    pub replace_all: bool,
}

#[derive(Args, Debug)]
pub struct FileListArgs {
    #[command(flatten)]
    pub target: FileTargetArgs,
    #[arg(long, value_name = "远端路径")]
    pub path: String,
    #[arg(long, value_name = "层数", default_value_t = 1, help = "递归层数")]
    pub depth: u64,
}

#[derive(Args, Debug)]
pub struct FileUploadArgs {
    #[command(flatten)]
    pub target: FileTargetArgs,
    #[arg(long, value_name = "本地文件")]
    pub local: PathBuf,
    #[arg(long, value_name = "远端路径")]
    pub remote: String,
    #[arg(long, help = "允许覆盖远端已有文件")]
    pub overwrite: bool,
    #[arg(long, help = "自动创建远端父目录")]
    pub create_parents: bool,
}

#[derive(Args, Debug)]
pub struct FileDownloadArgs {
    #[command(flatten)]
    pub target: FileTargetArgs,
    #[arg(long, value_name = "远端路径")]
    pub remote: String,
    #[arg(long, value_name = "本地文件")]
    pub local: PathBuf,
    #[arg(long, help = "允许覆盖本地已有文件")]
    pub overwrite: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_run_command() {
        let cli = Cli::try_parse_from([
            "dsh-remote",
            "run",
            "--hub",
            "127.0.0.1:1",
            "--key",
            "0123456789abcdef",
            "--target",
            "nas",
            "--",
            "echo",
            "hi",
        ])
        .unwrap();
        match cli.command {
            Command::Run(args) => {
                assert_eq!(args.target, "nas");
                assert_eq!(args.command, vec!["echo", "hi"]);
            }
            other => panic!("期望 run，收到 {other:?}"),
        }
    }

    #[test]
    fn parses_file_edit() {
        let cli = Cli::try_parse_from([
            "dsh-remote",
            "file",
            "edit",
            "--hub",
            "127.0.0.1:1",
            "--key",
            "0123456789abcdef",
            "--target",
            "nas",
            "--path",
            "/tmp/a.txt",
            "--old",
            "a",
            "--new",
            "b",
        ])
        .unwrap();
        match cli.command {
            Command::File(args) => match args.action {
                FileAction::Edit(edit) => {
                    assert_eq!(edit.target.target, "nas");
                    assert_eq!(edit.old.as_deref(), Some("a"));
                    assert_eq!(edit.new.as_deref(), Some("b"));
                }
                other => panic!("期望 edit，收到 {other:?}"),
            },
            other => panic!("期望 file，收到 {other:?}"),
        }
    }

    #[test]
    fn parses_file_write_from_stdin() {
        let cli = Cli::try_parse_from([
            "dsh-remote",
            "file",
            "write",
            "--hub",
            "127.0.0.1:1",
            "--key",
            "0123456789abcdef",
            "--path",
            "/tmp/a.txt",
            "--stdin",
        ])
        .unwrap();
        match cli.command {
            Command::File(args) => match args.action {
                FileAction::Write(write) => assert!(write.stdin),
                other => panic!("期望 write，收到 {other:?}"),
            },
            other => panic!("期望 file，收到 {other:?}"),
        }
    }
}
