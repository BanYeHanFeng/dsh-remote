pub mod backoff;
pub mod noise;

use anyhow::{anyhow, Result};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::debug;

use crate::proto::Msg;
pub use noise::SecureConn;

pub const CONTROL_QUEUE: usize = 64;
pub const DATA_QUEUE: usize = 256;
const BATCH_LIMIT: usize = 64;

#[derive(Clone)]
pub struct Sink {
    control: mpsc::Sender<Msg>,
    data: mpsc::Sender<Msg>,
}

fn ordered_with_data(msg: &Msg) -> bool {
    matches!(
        msg,
        Msg::Chunk { .. } | Msg::End { .. } | Msg::Result { .. }
    )
}

impl Sink {
    fn channel(&self, msg: &Msg) -> &mpsc::Sender<Msg> {
        if ordered_with_data(msg) {
            &self.data
        } else {
            &self.control
        }
    }

    pub async fn send(&self, msg: Msg) -> Result<()> {
        self.channel(&msg)
            .send(msg)
            .await
            .map_err(|_| anyhow!("连接已关闭"))
    }

    pub fn try_send(&self, msg: Msg) -> Result<()> {
        self.channel(&msg).try_send(msg).map_err(|err| match err {
            mpsc::error::TrySendError::Full(_) => anyhow!("发送队列已满"),
            mpsc::error::TrySendError::Closed(_) => anyhow!("连接已关闭"),
        })
    }

    pub fn is_closed(&self) -> bool {
        self.control.is_closed() || self.data.is_closed()
    }

    pub fn same_channel(&self, other: &Sink) -> bool {
        self.control.same_channel(&other.control)
    }
}

pub struct IoHandles {
    pub inbound: mpsc::Receiver<Msg>,
    pub outbound: Sink,
    pub reader: JoinHandle<()>,
    pub writer: JoinHandle<()>,
}

pub fn spawn_io(conn: SecureConn) -> IoHandles {
    let (in_tx, inbound) = mpsc::channel(DATA_QUEUE);
    let (control_tx, mut control_rx) = mpsc::channel(CONTROL_QUEUE);
    let (data_tx, mut data_rx) = mpsc::channel(DATA_QUEUE);
    let (mut reader, mut writer) = conn.split();

    let reader_task = tokio::spawn(async move {
        loop {
            match reader.recv().await {
                Ok(msg) => {
                    if in_tx.send(msg).await.is_err() {
                        break;
                    }
                }
                Err(err) => {
                    debug!(error = %err, "读取连接结束");
                    break;
                }
            }
        }
    });

    let writer_task = tokio::spawn(async move {
        let mut batch: Vec<Msg> = Vec::with_capacity(BATCH_LIMIT);
        let mut control_open = true;
        loop {
            if control_open {
                tokio::select! {
                    biased;
                    msg = control_rx.recv() => match msg {
                        Some(msg) => batch.push(msg),
                        None => control_open = false,
                    },
                    msg = data_rx.recv() => match msg {
                        Some(msg) => batch.push(msg),
                        None => break,
                    },
                }
            } else {
                match data_rx.recv().await {
                    Some(msg) => batch.push(msg),
                    None => break,
                }
            }
            while let Ok(msg) = control_rx.try_recv() {
                batch.push(msg);
            }
            while batch.len() < BATCH_LIMIT {
                match data_rx.try_recv() {
                    Ok(msg) => batch.push(msg),
                    Err(_) => break,
                }
            }
            let count = batch.len();
            if let Err(err) = writer.send_batch(&batch).await {
                debug!(error = %err, count, "写入连接结束");
                break;
            }
            batch.clear();
        }
        let _ = writer.close().await;
    });

    IoHandles {
        inbound,
        outbound: Sink {
            control: control_tx,
            data: data_tx,
        },
        reader: reader_task,
        writer: writer_task,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{EndStatus, STREAM_STDOUT};

    fn sink_pair() -> (Sink, mpsc::Receiver<Msg>, mpsc::Receiver<Msg>) {
        let (control_tx, control_rx) = mpsc::channel(4);
        let (data_tx, data_rx) = mpsc::channel(1);
        (
            Sink {
                control: control_tx,
                data: data_tx,
            },
            control_rx,
            data_rx,
        )
    }

    #[tokio::test]
    async fn control_never_waits_for_bulk_data() {
        let (sink, mut control, mut data) = sink_pair();
        let chunk = Msg::Chunk {
            id: 1,
            seq: 0,
            stream: STREAM_STDOUT,
            data: vec![7; 16],
        };
        sink.try_send(chunk).unwrap();
        assert!(sink.try_send(Msg::Ping { nonce: 9 }).is_ok());
        assert!(matches!(control.try_recv(), Ok(Msg::Ping { nonce: 9 })));
        assert!(matches!(data.try_recv(), Ok(Msg::Chunk { .. })));
    }

    #[tokio::test]
    async fn end_stays_behind_its_chunks() {
        let (sink, _control, _data) = sink_pair();
        let chunk = Msg::Chunk {
            id: 1,
            seq: 0,
            stream: STREAM_STDOUT,
            data: vec![7; 16],
        };
        let end = Msg::End {
            id: 1,
            status: EndStatus::Ok,
            exit_code: None,
            size: Some(16),
            sha256: None,
            truncated: Some(false),
        };
        sink.try_send(chunk).unwrap();
        assert!(sink.try_send(end).is_err());
    }
}
