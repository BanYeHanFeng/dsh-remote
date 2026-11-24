use std::sync::Arc;
use std::time::Duration;

use dsh_remote::frame::{read_frame, write_frame};
use dsh_remote::protocol::{ControlMsg, OpFrame, OpRequest, RelayHeader, Role};
use dsh_remote::relay::{LocalNode, Relay};
use dsh_remote::transport;

const LOCAL_ID: &str = "relay-host";

async fn start_relay(psk: &[u8]) -> String {
    let listener = transport::listen("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let relay = Arc::new(Relay::new(
        psk.to_vec(),
        Some(LocalNode {
            id: LOCAL_ID.to_string(),
            tags: vec!["relay".to_string()],
        }),
    ));
    tokio::spawn(relay.serve(listener));
    addr
}

async fn connect_controller(addr: &str, psk: &[u8]) -> (transport::Session, transport::Stream) {
    let session = transport::connect(addr, psk, Role::Controller, "ctl", &[])
        .await
        .unwrap();
    let mut control = session.open().await.unwrap();
    write_frame(
        &mut control,
        &ControlMsg::Hello {
            node_id: "ctl".to_string(),
        },
    )
    .await
    .unwrap();
    (session, control)
}

#[tokio::test]
async fn relay_runs_command_on_local_node() {
    let psk = b"testpsk".to_vec();
    let addr = start_relay(&psk).await;
    let (session, mut control) = connect_controller(&addr, &psk).await;

    let online: ControlMsg = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut control))
        .await
        .expect("等待节点上线通知超时")
        .unwrap();
    match online {
        ControlMsg::NodeOnline { node_id, tags } => {
            assert_eq!(node_id, LOCAL_ID);
            assert_eq!(tags, vec!["relay".to_string()]);
        }
        other => panic!("意外的通知：{:?}", other),
    }

    let mut op = session.open().await.unwrap();
    write_frame(
        &mut op,
        &RelayHeader {
            target: LOCAL_ID.to_string(),
        },
    )
    .await
    .unwrap();
    write_frame(
        &mut op,
        &OpRequest::Exec {
            command: "printf 'relay-local' ".to_string(),
            workdir: None,
            timeout_secs: 10,
        },
    )
    .await
    .unwrap();

    let mut stdout = String::new();
    let exit = loop {
        let frame: OpFrame = tokio::time::timeout(Duration::from_secs(10), read_frame(&mut op))
            .await
            .expect("等待执行结果超时")
            .unwrap();
        match frame {
            OpFrame::Stdout { d } => stdout.push_str(&d),
            OpFrame::Stderr { .. } => {}
            exit @ OpFrame::Exit { .. } => break exit,
            other => panic!("意外的帧：{:?}", other),
        }
    };
    assert_eq!(stdout, "relay-local");
    assert!(
        matches!(
            exit,
            OpFrame::Exit {
                code: Some(0),
                timed_out: false,
                ..
            }
        ),
        "实际退出：{:?}",
        exit
    );
    session.close().await;
}

#[tokio::test]
async fn relay_downloads_file_from_local_node() {
    let psk = b"testpsk".to_vec();
    let addr = start_relay(&psk).await;
    let (session, _control) = connect_controller(&addr, &psk).await;

    let path = std::env::temp_dir().join(format!("dsh-relay-local-{}", std::process::id()));
    std::fs::write(&path, b"payload").unwrap();

    let mut op = session.open().await.unwrap();
    write_frame(
        &mut op,
        &RelayHeader {
            target: LOCAL_ID.to_string(),
        },
    )
    .await
    .unwrap();
    write_frame(
        &mut op,
        &OpRequest::Download {
            remote_path: path.to_string_lossy().to_string(),
        },
    )
    .await
    .unwrap();

    let ack: OpFrame = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut op))
        .await
        .expect("等待下载应答超时")
        .unwrap();
    assert!(
        matches!(ack, OpFrame::Ack { size: 7, .. }),
        "实际应答：{:?}",
        ack
    );

    let mut body = vec![0u8; 7];
    tokio::io::AsyncReadExt::read_exact(&mut op, &mut body)
        .await
        .unwrap();
    assert_eq!(body, b"payload");
    let done: OpFrame = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut op))
        .await
        .expect("等待下载完成超时")
        .unwrap();
    assert!(
        matches!(done, OpFrame::Done { bytes: 7, .. }),
        "实际结果：{:?}",
        done
    );

    let _ = std::fs::remove_file(&path);
    session.close().await;
}
