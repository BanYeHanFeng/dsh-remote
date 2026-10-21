use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use tokio::time::{interval, timeout, Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::ClientConfig;
use crate::exec::{Executor, Settings};
use crate::net::backoff::Backoff;
use crate::net::{spawn_io, IoHandles, SecureConn};
use crate::proto::{caps, Msg, Role, TargetInfo, PROTO_VERSION};

const HELLO_TIMEOUT: Duration = Duration::from_secs(5);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const IDLE_TIMEOUT: Duration = Duration::from_secs(45);
const WRITER_DRAIN: Duration = Duration::from_secs(2);
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
    let mut conn = SecureConn::connect(&cfg.hub, &cfg.key).await?;
    let hello = Msg::Hello {
        proto: PROTO_VERSION,
        role: Role::Agent,
        name: cfg.name.clone(),
        version: crate::VERSION.to_string(),
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        caps: caps(cfg.allow_bash),
        roots: cfg.roots.clone(),
    };
    conn.send(&hello).await?;
    let welcome = timeout(HELLO_TIMEOUT, conn.recv())
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

    let IoHandles {
        mut inbound,
        outbound,
        reader,
        writer,
    } = spawn_io(conn);
    let executor = Executor::with_shutdown(Settings::from_client(cfg), shutdown.clone());
    let mut ticker = interval(HEARTBEAT_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_rx = Instant::now();

    let result = loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                let _ = outbound.try_send(Msg::Bye {
                    code: "shutdown".into(),
                    message: "被控端退出".into(),
                });
                break Ok(());
            }
            incoming = inbound.recv() => match incoming {
                Some(msg) => {
                    last_rx = Instant::now();
                    match msg {
                        Msg::Ping { nonce } => {
                            let _ = outbound.try_send(Msg::Pong { nonce });
                        }
                        Msg::Pong { .. } => {}
                        Msg::TargetsSnapshot { targets } => {
                            debug!(count = targets.len(), "目标快照");
                        }
                        Msg::TargetEvent { kind, target } => {
                            debug!(kind = ?kind, name = %target.name, "目标事件");
                        }
                        Msg::Bye { code, message } if code == "replaced" => {
                            break Err(anyhow::Error::new(Replaced)
                                .context(format!("中继关闭连接: {message}")));
                        }
                        Msg::Bye { code, message } => {
                            break Err(anyhow!("中继关闭连接: {code} {message}"));
                        }
                        Msg::Call { id, method, params, .. } => {
                            executor
                                .dispatch(id, 0, id, &method, params, outbound.clone())
                                .await;
                        }
                        msg @ (Msg::Chunk { .. } | Msg::End { .. }) => {
                            let id = msg.id();
                            if !executor.feed(msg).await {
                                debug!(?id, "忽略未匹配的传输消息");
                            }
                        }
                        Msg::Cancel { id, .. } => {
                            executor.cancel(id);
                        }
                        other => debug!(msg = ?other, "忽略消息"),
                    }
                }
                None => break Err(anyhow!("中继连接断开")),
            },
            _ = ticker.tick() => {
                if last_rx.elapsed() > IDLE_TIMEOUT {
                    break Err(anyhow!("中继心跳超时"));
                }
                let nonce = rand::random::<u64>();
                if outbound.try_send(Msg::Ping { nonce }).is_err() {
                    break Err(anyhow!("发送心跳失败"));
                }
            }
        }
    };

    drop(outbound);
    let _ = timeout(WRITER_DRAIN, writer).await;
    reader.abort();
    executor.cancel_all();
    result
}

fn target_names(targets: &[TargetInfo]) -> String {
    targets
        .iter()
        .map(|target| target.name.as_str())
        .collect::<Vec<_>>()
        .join(",")
}
