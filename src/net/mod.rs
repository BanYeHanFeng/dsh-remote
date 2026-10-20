pub mod backoff;
pub mod noise;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::debug;

use crate::proto::Msg;
pub use noise::SecureConn;

pub struct IoHandles {
    pub inbound: mpsc::Receiver<Msg>,
    pub outbound: mpsc::Sender<Msg>,
    pub reader: JoinHandle<()>,
    pub writer: JoinHandle<()>,
}

pub fn spawn_io(conn: SecureConn) -> IoHandles {
    let (in_tx, inbound) = mpsc::channel(64);
    let (outbound, mut out_rx) = mpsc::channel(64);
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
        while let Some(msg) = out_rx.recv().await {
            if let Err(err) = writer.send(&msg).await {
                debug!(error = %err, "写入连接结束");
                break;
            }
        }
        let _ = writer.close().await;
    });

    IoHandles {
        inbound,
        outbound,
        reader: reader_task,
        writer: writer_task,
    }
}
