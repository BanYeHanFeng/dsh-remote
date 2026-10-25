use std::path::Path;

use anyhow::Result;
use tracing_subscriber::EnvFilter;

pub fn init(spec: &str) -> Result<()> {
    let spec = spec.trim();
    if spec.eq_ignore_ascii_case("off") || spec.eq_ignore_ascii_case("none") {
        return Ok(());
    }
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("dsh_remote=info"));
    if spec.is_empty() || spec == "stderr" {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .init();
        return Ok(());
    }
    let path = Path::new(spec);
    let dir = path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(dir)?;
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("dsh-remote.log");
    let appender = tracing_appender::rolling::never(dir, name);
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(appender)
        .with_ansi(false)
        .init();
    Ok(())
}
