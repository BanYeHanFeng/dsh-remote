use std::time::Duration;

use anyhow::Result;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{interval, timeout, Instant, Interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::key::KeyMaterial;
use crate::net::{spawn_io, SecureConn, Sink};
use crate::proto::Msg;

pub const HELLO_TIMEOUT: Duration = Duration::from_secs(5);
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(45);
const DRAIN: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum End {
    Shutdown,
    Stopped,
    Closed,
    Idle,
    Stall,
}

impl End {
    pub fn reason(self) -> &'static str {
        match self {
            End::Shutdown => "本端退出",
            End::Stopped => "连接被顶替",
            End::Closed => "对端断开",
            End::Idle => "心跳超时",
            End::Stall => "发送心跳失败",
        }
    }
}

pub struct Link {
    inbound: mpsc::Receiver<Msg>,
    sink: Sink,
    reader: JoinHandle<()>,
    writer: JoinHandle<()>,
    ticker: Interval,
    last_rx: Instant,
}

impl Link {
    pub async fn connect(addr: &str, key: &KeyMaterial) -> Result<Self> {
        Ok(Self::wrap(SecureConn::connect(addr, key).await?))
    }

    pub async fn accept(stream: TcpStream, key: &KeyMaterial) -> Result<Self> {
        Ok(Self::wrap(SecureConn::accept(stream, key).await?))
    }

    fn wrap(conn: SecureConn) -> Self {
        let handles = spawn_io(conn);
        let mut ticker = interval(HEARTBEAT_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        Self {
            inbound: handles.inbound,
            sink: handles.outbound,
            reader: handles.reader,
            writer: handles.writer,
            ticker,
            last_rx: Instant::now(),
        }
    }

    pub fn sink(&self) -> &Sink {
        &self.sink
    }

    pub async fn send(&self, msg: Msg) -> Result<()> {
        self.sink.send(msg).await
    }

    pub async fn recv(&mut self) -> Result<Msg> {
        self.inbound
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("连接已关闭"))
    }

    pub async fn next(
        &mut self,
        shutdown: &CancellationToken,
        stop: &CancellationToken,
    ) -> std::result::Result<Msg, End> {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return Err(End::Shutdown),
                _ = stop.cancelled() => return Err(End::Stopped),
                incoming = self.inbound.recv() => match incoming {
                    Some(msg) => {
                        self.last_rx = Instant::now();
                        return Ok(msg);
                    }
                    None => return Err(End::Closed),
                },
                _ = self.ticker.tick() => {
                    if self.last_rx.elapsed() > IDLE_TIMEOUT {
                        return Err(End::Idle);
                    }
                    if self.sink.try_send(Msg::Ping { nonce: rand::random() }).is_err() {
                        return Err(End::Stall);
                    }
                }
            }
        }
    }

    pub fn bye(&self, code: &str, message: impl Into<String>) {
        let _ = self.sink.try_send(Msg::Bye {
            code: code.to_string(),
            message: message.into(),
        });
    }

    pub async fn close(self) {
        drop(self.sink);
        let _ = timeout(DRAIN, self.writer).await;
        self.reader.abort();
        debug!("连接已排空");
    }
}
