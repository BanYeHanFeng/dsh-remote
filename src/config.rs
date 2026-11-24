use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::Deserialize;

pub const ETC_DIR: &str = "/etc/dsh-remote";
pub const DEFAULT_SOCKET: &str = "/run/dsh-remote/controller.sock";
pub const DEFAULT_PORT: u16 = 7443;

#[derive(Debug, Default, Deserialize)]
pub struct FileConfig {
    pub listen: Option<String>,
    pub relay: Option<String>,
    pub socket: Option<PathBuf>,
    pub mcp_http: Option<String>,
    pub psk: Option<String>,
    pub connect: Option<String>,
    pub local: Option<bool>,
    pub id: Option<String>,
}

pub fn load(explicit: Option<&Path>, role: &str) -> Result<FileConfig> {
    if let Some(path) = explicit {
        return read(path);
    }
    let default = PathBuf::from(ETC_DIR).join(format!("{}.toml", role));
    if default.exists() {
        return read(&default);
    }
    Ok(FileConfig::default())
}

fn read(path: &Path) -> Result<FileConfig> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("读取配置文件失败：{}", path.display()))?;
    let mut config: FileConfig =
        toml::from_str(&text).with_context(|| format!("解析配置文件失败：{}", path.display()))?;
    config.normalize();
    Ok(config)
}

impl FileConfig {
    fn normalize(&mut self) {
        for value in [
            &mut self.listen,
            &mut self.relay,
            &mut self.mcp_http,
            &mut self.psk,
            &mut self.connect,
            &mut self.id,
        ] {
            if value.as_deref().is_none_or(|text| text.trim().is_empty()) {
                *value = None;
            }
        }
    }
}

pub fn psk(value: Option<String>) -> Result<Vec<u8>> {
    if let Ok(env) = std::env::var("DSH_REMOTE_PSK") {
        if !env.trim().is_empty() {
            return Ok(env.trim().as_bytes().to_vec());
        }
    }
    match value {
        Some(text) if !text.trim().is_empty() => Ok(text.trim().as_bytes().to_vec()),
        _ => bail!("缺少 PSK：请在配置文件中设置 psk，或用环境变量 DSH_REMOTE_PSK 提供"),
    }
}

pub fn resolve_socket(explicit: Option<PathBuf>, config: Option<&Path>) -> PathBuf {
    if let Some(path) = explicit {
        return path;
    }
    if let Ok(file) = load(config, "controller") {
        if let Some(path) = file.socket {
            return path;
        }
    }
    PathBuf::from(DEFAULT_SOCKET)
}

pub fn pick_string(cli: Option<String>, file: Option<String>) -> Option<String> {
    match cli {
        Some(value) if !value.trim().is_empty() => Some(value),
        _ => file.filter(|value| !value.trim().is_empty()),
    }
}

pub fn require(value: Option<String>, what: &str) -> Result<String> {
    match value {
        Some(value) if !value.trim().is_empty() => Ok(value),
        _ => bail!("缺少{}，请在命令行参数或配置文件中设置", what),
    }
}

pub fn with_port(addr: &str) -> String {
    if addr
        .rsplit(':')
        .next()
        .is_some_and(|p| p.parse::<u16>().is_ok())
    {
        addr.to_string()
    } else {
        format!("{}:{}", addr, DEFAULT_PORT)
    }
}
