use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::ClientConfig;
use crate::exec::Executor;
use crate::net::backoff::Backoff;
use crate::net::link::{End, Link, HELLO_TIMEOUT};
use crate::proto::{target_names, Msg, Role, PROTO_VERSION};

const REPLACED_RETRY: Duration = Duration::from_secs(60);
const REPLACED_JITTER_MS: u64 = 15_000;

#[derive(Debug)]
struct Replaced;

impl std::fmt::Display for Replaced {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("同名目标已被新连接顶替")
    }
}

impl std::error::Error for Replaced {}

pub async fn run(cfg: ClientConfig, shutdown: CancellationToken) -> Result<()> {
    let mut backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(30));
    loop {
        if shutdown.is_cancelled() {
            break;
        }
        let mut replaced = false;
        match connect_once(&cfg, &shutdown, &mut backoff).await {
            Ok(()) => info!("中继连接已关闭"),
            Err(err) => {
                replaced = err.downcast_ref::<Replaced>().is_some();
                if replaced {
                    warn!(
                        error = %format!("{err:#}"),
                        "连接被同名目标顶替，检查是否重复启动了实例"
                    );
                } else {
                    warn!(error = %format!("{err:#}"), "连接中继失败");
                }
            }
        }
        if shutdown.is_cancelled() {
            break;
        }
        let delay = if replaced {
            REPLACED_RETRY + Duration::from_millis(rand::random_range(0..=REPLACED_JITTER_MS))
        } else {
            backoff.next_delay()
        };
        info!(seconds = delay.as_secs_f32(), "等待重连");
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(delay) => {}
        }
    }
    info!("被控端退出");
    Ok(())
}

async fn connect_once(
    cfg: &ClientConfig,
    shutdown: &CancellationToken,
    backoff: &mut Backoff,
) -> Result<()> {
    let mut link = Link::connect(&cfg.hub, &cfg.key).await?;
    let hello = Msg::Hello {
        proto: PROTO_VERSION,
        role: Role::Agent,
        name: cfg.name.clone(),
        version: crate::VERSION.to_string(),
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        roots: cfg.target.roots.clone(),
    };
    link.send(hello).await?;
    let welcome = timeout(HELLO_TIMEOUT, link.recv())
        .await
        .map_err(|_| anyhow!("等待 Welcome 超时"))??;
    match welcome {
        Msg::Welcome {
            proto,
            hub_name,
            targets,
            ..
        } => {
            if proto != PROTO_VERSION {
                bail!("中继协议版本不匹配: {proto}");
            }
            backoff.reset();
            info!(
                hub = %hub_name,
                targets = %target_names(&targets),
                "已注册到中继"
            );
        }
        Msg::Bye { code, message } if code == "replaced" => {
            return Err(anyhow::Error::new(Replaced).context(format!("中继拒绝连接: {message}")));
        }
        Msg::Bye { code, message } => bail!("中继拒绝连接: {code} {message}"),
        other => bail!("意外的首条响应: {other:?}"),
    }

    let executor = Executor::with_shutdown(cfg.target.clone(), shutdown.clone());
    let never = CancellationToken::new();

    let result = loop {
        match link.next(shutdown, &never).await {
            Ok(msg) => match msg {
                Msg::Ping { nonce } => {
                    let _ = link.sink().try_send(Msg::Pong { nonce });
                }
                Msg::Pong { .. } => {}
                Msg::Bye { code, message } if code == "replaced" => {
                    break Err(
                        anyhow::Error::new(Replaced).context(format!("中继关闭连接: {message}"))
                    );
                }
                Msg::Bye { code, message } => {
                    break Err(anyhow!("中继关闭连接: {code} {message}"));
                }
                Msg::Call {
                    id, method, params, ..
                } => {
                    executor
                        .dispatch(id, &method, params, link.sink().clone())
                        .await;
                }
                msg @ (Msg::Chunk { .. } | Msg::End { .. }) => {
                    let id = msg.id();
                    if !executor.feed(msg).await {
                        debug!(?id, "忽略未匹配的传输消息");
                    }
                }
                Msg::Cancel { id, .. } => executor.cancel(id),
                other => debug!(msg = ?other, "忽略消息"),
            },
            Err(End::Shutdown) => {
                link.bye("shutdown", "被控端退出");
                break Ok(());
            }
            Err(end) => break Err(anyhow!("中继连接中断：{}", end.reason())),
        }
    };

    executor.cancel_all();
    link.close().await;
    result
}
