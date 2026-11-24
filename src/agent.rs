use std::time::Duration;

use anyhow::{Context, Result};

use crate::frame::{read_frame, write_frame};
use crate::node;
use crate::protocol::Role;
use crate::transport::{self, Stream};

pub struct AgentOptions {
    pub connect: String,
    pub id: String,
    pub tags: Vec<String>,
    pub psk: Vec<u8>,
}

pub async fn run(opts: AgentOptions) -> Result<()> {
    let mut backoff = 1u64;
    loop {
        match session(&opts).await {
            Ok(()) => {
                tracing::info!("与控制端的会话已结束，准备重连");
                backoff = 1;
            }
            Err(error) => {
                tracing::warn!(error = %format!("{error:#}"), "连接控制端失败");
                backoff = (backoff * 2).min(30);
            }
        }
        tokio::time::sleep(Duration::from_secs(backoff)).await;
    }
}

async fn session(opts: &AgentOptions) -> Result<()> {
    let session = transport::connect(&opts.connect, &opts.psk, Role::Agent, &opts.id, &opts.tags)
        .await
        .with_context(|| format!("接入 {} 失败", opts.connect))?;
    let mut control = session.open().await.context("打开控制流失败")?;
    write_frame(
        &mut control,
        &crate::protocol::ControlMsg::Hello {
            node_id: opts.id.clone(),
        },
    )
    .await
    .context("发送控制流问候失败")?;
    tracing::info!(node = %opts.id, peer = %session.peer(), "已接入控制端");

    let mut control_task = tokio::spawn(control_loop(control));
    loop {
        tokio::select! {
            accepted = session.accept() => match accepted {
                Ok(stream) => {
                    tokio::spawn(async move {
                        if let Err(error) = node::serve(stream).await {
                            tracing::warn!(error = %format!("{error:#}"), "处理远程操作失败");
                        }
                    });
                }
                Err(_) => break,
            },
            _ = &mut control_task => break,
            _ = session.closed() => {
                tracing::info!("与控制端的连接已关闭");
                break;
            }
        }
    }
    control_task.abort();
    session.close().await;
    Ok(())
}

async fn control_loop(mut control: Stream) {
    while read_frame::<_, crate::protocol::ControlMsg>(&mut control)
        .await
        .is_ok()
    {}
}

pub fn default_node_id() -> String {
    hostname::get()
        .ok()
        .and_then(|name| name.into_string().ok())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| format!("node-{}", std::process::id()))
}
