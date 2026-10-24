use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};

use crate::cli::{ClientArgs, CommonArgs, HubArgs, MountArgs};
use crate::exec::Settings;
use crate::key::KeyMaterial;
use crate::mount::{DEFAULT_ATTR_TTL_MS, DEFAULT_SPOOL_LIMIT};

const DEFAULT_MAX_OUTPUT_CHARS: usize = 16_000;
const DEFAULT_SHELL_TIMEOUT_MS: u64 = 300_000;
const DEFAULT_MAX_FILE: u64 = 67_108_864;
const DEFAULT_PACK_GAIN: u8 = 12;

#[derive(Clone, Debug)]
pub struct HubConfig {
    pub listen: String,
    pub key: KeyMaterial,
    pub name: String,
    pub target: Settings,
    pub log: String,
}

#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub hub: String,
    pub key: KeyMaterial,
    pub name: String,
    pub target: Settings,
    pub log: String,
    pub github: GithubConfig,
}

#[derive(Clone, Debug)]
pub struct MountConfig {
    pub client: ClientConfig,
    pub target: String,
    pub mountpoint: std::path::PathBuf,
    pub remote_root: String,
    pub read_only: bool,
    pub allow_other: bool,
    pub attr_ttl: Duration,
    pub spool_dir: Option<std::path::PathBuf>,
    pub spool_limit: u64,
}

#[derive(Clone, Debug)]
pub struct GithubConfig {
    pub repo: Option<String>,
    pub workflow: String,
    pub git_ref: String,
    pub secret: String,
    pub hub_secret: String,
    pub token: Option<String>,
    pub binary: String,
}

impl Default for GithubConfig {
    fn default() -> Self {
        Self {
            repo: None,
            workflow: "ephemeral-node.yml".to_string(),
            git_ref: "dev".to_string(),
            secret: "DSH_REMOTE_KEY".to_string(),
            hub_secret: "DSH_REMOTE_HUB".to_string(),
            token: None,
            binary: "gh".to_string(),
        }
    }
}

pub struct RawConfig {
    map: HashMap<String, String>,
}

impl RawConfig {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let mut map = HashMap::new();
        if let Some(path) = path {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("读取配置失败: {}", path.display()))?;
            for (index, line) in text.lines().enumerate() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                let Some((key, value)) = line.split_once('=') else {
                    bail!("配置文件第 {} 行缺少等号: {}", index + 1, path.display());
                };
                map.insert(key.trim().to_ascii_uppercase(), value.trim().to_string());
            }
        }
        Ok(Self { map })
    }

    fn get(&self, key: &str) -> Option<String> {
        if let Some(value) = self.map.get(key) {
            if !value.is_empty() {
                return Some(value.clone());
            }
        }
        std::env::var(format!("DSH_REMOTE_{key}"))
            .ok()
            .filter(|value| !value.is_empty())
    }
}

pub fn hub_config(args: &HubArgs) -> Result<HubConfig> {
    let raw = RawConfig::load(args.common.config.as_deref())?;
    Ok(HubConfig {
        listen: pick(args.listen.clone(), &raw, "LISTEN")
            .unwrap_or_else(|| "0.0.0.0:7000".to_string()),
        key: resolve_key(&args.common, &raw)?,
        name: resolve_name(&args.common, &raw),
        target: resolve_settings(&args.common, &raw)?,
        log: resolve_log(&args.common, &raw),
    })
}

pub fn client_config(args: &ClientArgs) -> Result<ClientConfig> {
    let raw = RawConfig::load(args.common.config.as_deref())?;
    Ok(ClientConfig {
        hub: pick(args.hub.clone(), &raw, "HUB")
            .context("缺少中继地址：用 --hub 或配置文件 HUB=")?,
        key: resolve_key(&args.common, &raw)?,
        name: resolve_name(&args.common, &raw),
        target: resolve_settings(&args.common, &raw)?,
        log: resolve_log(&args.common, &raw),
        github: resolve_github(&raw),
    })
}

pub fn mount_config(args: &MountArgs) -> Result<MountConfig> {
    let client = client_config(&ClientArgs {
        common: args.common.clone(),
        hub: args.hub.clone(),
    })?;
    if args.target.trim().is_empty() {
        bail!("缺少目标名：用 --target");
    }
    let raw = RawConfig::load(args.common.config.as_deref())?;
    let remote_root =
        pick(args.remote_root.clone(), &raw, "REMOTE_ROOT").unwrap_or_else(|| "/".to_string());
    let attr_ttl_ms = number(args.attr_ttl_ms, &raw, "ATTR_TTL_MS", DEFAULT_ATTR_TTL_MS)?;
    if attr_ttl_ms == 0 {
        bail!("ATTR_TTL_MS 必须是正整数");
    }
    let spool_limit = number(args.spool_limit, &raw, "SPOOL_LIMIT", DEFAULT_SPOOL_LIMIT)?;
    if spool_limit < DEFAULT_MAX_FILE {
        bail!("SPOOL_LIMIT 不能小于单文件上限 {DEFAULT_MAX_FILE} 字节");
    }
    Ok(MountConfig {
        client,
        target: args.target.clone(),
        mountpoint: args.mountpoint.clone(),
        remote_root,
        read_only: args.read_only,
        allow_other: args.allow_other,
        attr_ttl: Duration::from_millis(attr_ttl_ms),
        spool_dir: args.spool_dir.clone(),
        spool_limit,
    })
}

fn resolve_settings(common: &CommonArgs, raw: &RawConfig) -> Result<Settings> {
    let shell_timeout_ms = number(
        common.shell_timeout_ms,
        raw,
        "SHELL_TIMEOUT_MS",
        DEFAULT_SHELL_TIMEOUT_MS,
    )?;
    if shell_timeout_ms == 0 {
        bail!("SHELL_TIMEOUT_MS 必须是正整数");
    }
    Ok(Settings {
        roots: resolve_roots(common, raw),
        allow_bash: resolve_allow_bash(common, raw),
        max_output_chars: number(
            common.max_output_chars,
            raw,
            "MAX_OUTPUT_CHARS",
            DEFAULT_MAX_OUTPUT_CHARS,
        )?,
        shell_timeout: Duration::from_millis(shell_timeout_ms),
        max_file: number(common.max_file, raw, "MAX_FILE", DEFAULT_MAX_FILE)?,
        pack: flag(raw, "PACK", true),
        pack_gain: number(None, raw, "PACK_GAIN", DEFAULT_PACK_GAIN)?,
    })
}

fn resolve_github(raw: &RawConfig) -> GithubConfig {
    let defaults = GithubConfig::default();
    GithubConfig {
        repo: raw.get("GH_REPO"),
        workflow: raw.get("GH_WORKFLOW").unwrap_or(defaults.workflow),
        git_ref: raw.get("GH_REF").unwrap_or(defaults.git_ref),
        secret: raw.get("GH_SECRET").unwrap_or(defaults.secret),
        hub_secret: raw.get("GH_HUB_SECRET").unwrap_or(defaults.hub_secret),
        token: raw.get("GH_TOKEN"),
        binary: raw.get("GH_BIN").unwrap_or(defaults.binary),
    }
}

fn pick(cli: Option<String>, raw: &RawConfig, key: &str) -> Option<String> {
    cli.filter(|value| !value.trim().is_empty())
        .or_else(|| raw.get(key))
}

fn resolve_log(common: &CommonArgs, raw: &RawConfig) -> String {
    pick(common.log.clone(), raw, "LOG").unwrap_or_else(|| "stderr".to_string())
}

fn resolve_key(common: &CommonArgs, raw: &RawConfig) -> Result<KeyMaterial> {
    if let Some(value) = common
        .key
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        return KeyMaterial::parse(value);
    }
    if let Some(path) = &common.key_file {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("读取 Key 文件失败: {}", path.display()))?;
        return KeyMaterial::parse(&text);
    }
    if let Some(value) = raw.get("KEY") {
        return KeyMaterial::parse(&value);
    }
    bail!("缺少 Key：用 --key、--key-file 或配置文件的 KEY=")
}

fn resolve_name(common: &CommonArgs, raw: &RawConfig) -> String {
    pick(common.name.clone(), raw, "NAME").unwrap_or_else(default_name)
}

fn default_name() -> String {
    hostname::get()
        .ok()
        .and_then(|value| value.into_string().ok())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "dsh-remote".to_string())
}

fn resolve_roots(common: &CommonArgs, raw: &RawConfig) -> Vec<String> {
    let mut roots = common
        .roots
        .clone()
        .or_else(|| {
            raw.get("ROOTS").map(|value| {
                value
                    .split(':')
                    .map(|item| item.trim().to_string())
                    .collect::<Vec<_>>()
            })
        })
        .unwrap_or_else(|| vec!["/".to_string()]);
    roots.retain(|root| !root.is_empty());
    if roots.is_empty() {
        roots.push("/".to_string());
    }
    roots
}

fn resolve_allow_bash(common: &CommonArgs, raw: &RawConfig) -> bool {
    if common.no_bash {
        return false;
    }
    if let Some(value) = common.allow_bash {
        return value;
    }
    if let Some(value) = raw.get("ALLOW_BASH") {
        return !(value == "0" || value.eq_ignore_ascii_case("false"));
    }
    true
}

fn flag(raw: &RawConfig, key: &str, default: bool) -> bool {
    match raw.get(key) {
        Some(text) => !matches!(
            text.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "no" | "off"
        ),
        None => default,
    }
}

fn number<T>(cli: Option<T>, raw: &RawConfig, key: &str, default: T) -> Result<T>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    if let Some(value) = cli {
        return Ok(value);
    }
    match raw.get(key) {
        Some(text) => text.parse().map_err(|err| anyhow!("{key} 的值非法: {err}")),
        None => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn temp_config(text: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "dsh-remote-test-{}-{}.conf",
            std::process::id(),
            rand::random::<u32>()
        ));
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(text.as_bytes()).unwrap();
        path
    }

    #[test]
    fn parses_conf_file() {
        let path = temp_config(
            "# 注释\nLISTEN=127.0.0.1:7000\nKEY=0123456789abcdef0123456789abcdef\nNAME=hub\nROOTS=/data:/home\nALLOW_BASH=0\n",
        );
        let raw = RawConfig::load(Some(&path)).unwrap();
        assert_eq!(raw.get("NAME").as_deref(), Some("hub"));
        assert_eq!(raw.get("ALLOW_BASH").as_deref(), Some("0"));
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn rejects_bad_line() {
        let path = temp_config("KEY\n");
        assert!(RawConfig::load(Some(&path)).is_err());
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn settings_bundle_fields() {
        let common = CommonArgs {
            roots: Some(vec!["/data".into(), "/home".into()]),
            max_output_chars: Some(1024),
            shell_timeout_ms: Some(5_000),
            ..Default::default()
        };
        let raw = RawConfig::load(None).unwrap();
        let settings = resolve_settings(&common, &raw).unwrap();
        assert_eq!(settings.roots, vec!["/data", "/home"]);
        assert_eq!(settings.max_output_chars, 1024);
        assert_eq!(settings.shell_timeout, Duration::from_secs(5));
        assert_eq!(settings.max_file, DEFAULT_MAX_FILE);
        assert!(settings.pack);
        assert_eq!(settings.pack_gain, DEFAULT_PACK_GAIN);
    }

    #[test]
    fn rejects_zero_shell_timeout() {
        let common = CommonArgs {
            shell_timeout_ms: Some(0),
            ..Default::default()
        };
        let raw = RawConfig::load(None).unwrap();
        assert!(resolve_settings(&common, &raw).is_err());
    }
}
