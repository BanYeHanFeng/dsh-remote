use std::time::Duration;

use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::ClientConfig;
use crate::mcp;
use crate::session::Session;

const CONNECTION_DRAIN: Duration = Duration::from_secs(3);

pub async fn run(cfg: ClientConfig, shutdown: CancellationToken) -> Result<()> {
    let session = Session::new(&cfg);
    info!(name = %cfg.name, "启动 MCP 服务端（stdio）");

    let connection_shutdown = shutdown.child_token();
    let connection = tokio::spawn({
        let session = session.clone();
        let token = connection_shutdown.clone();
        async move { session.run(token).await }
    });

    let result = mcp::serve(&session, shutdown.clone()).await;

    connection_shutdown.cancel();
    shutdown.cancel();
    let _ = tokio::time::timeout(CONNECTION_DRAIN, connection).await;
    info!("MCP 服务端退出");
    result
}
