use std::time::Duration;

use dsh_remote::frame::{read_frame, write_frame};
use dsh_remote::protocol::{ControlMsg, Role};
use dsh_remote::transport;

#[tokio::test]
async fn handshake_and_multiplexing() {
    let listener = transport::listen("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let psk = b"testpsk".to_vec();

    let server_psk = psk.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (session, peer) = transport::accept(stream, &server_psk, &[Role::Agent])
            .await
            .unwrap();
        assert_eq!(peer.role, Role::Agent);
        assert_eq!(peer.node_id, "node-a");

        let mut control = session.accept().await.unwrap();
        let hello: ControlMsg = read_frame(&mut control).await.unwrap();
        write_frame(&mut control, &hello).await.unwrap();

        let mut op = session.accept().await.unwrap();
        let echo: ControlMsg = read_frame(&mut op).await.unwrap();
        write_frame(&mut op, &echo).await.unwrap();
        session
    });

    let session = transport::connect(&addr.to_string(), &psk, Role::Agent, "node-a")
        .await
        .unwrap();
    let hello = ControlMsg::Hello {
        node_id: "node-a".to_string(),
    };
    let msg = ControlMsg::NodeOnline {
        node_id: "node-a".to_string(),
    };

    let mut control = session.open().await.unwrap();
    write_frame(&mut control, &hello).await.unwrap();
    let echoed: ControlMsg = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut control))
        .await
        .expect("等待控制流回显超时")
        .unwrap();
    assert!(matches!(echoed, ControlMsg::Hello { .. }));

    let mut op = session.open().await.unwrap();
    write_frame(&mut op, &msg).await.unwrap();
    let back: ControlMsg = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut op))
        .await
        .expect("等待业务流回显超时")
        .unwrap();
    assert!(matches!(back, ControlMsg::NodeOnline { .. }));

    session.close().await;
    let _ = tokio::time::timeout(Duration::from_secs(5), server).await;
}

#[tokio::test]
async fn handshake_rejects_wrong_psk() {
    let listener = transport::listen("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let _ = transport::accept(stream, b"right", &[Role::Agent]).await;
    });

    let error = transport::connect(&addr.to_string(), b"wrong", Role::Agent, "node-a")
        .await
        .err()
        .expect("应当握手失败");
    let message = format!("{:#}", error);
    assert!(
        message.contains("预共享密钥不匹配"),
        "实际错误：{}",
        message
    );
    let _ = tokio::time::timeout(Duration::from_secs(5), server).await;
}

#[tokio::test]
async fn handshake_rejects_unexpected_role() {
    let listener = transport::listen("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let _ = transport::accept(stream, b"psk", &[Role::Agent]).await;
    });

    let error = transport::connect(&addr.to_string(), b"psk", Role::Controller, "ctl")
        .await
        .err()
        .expect("应当拒绝该角色");
    let message = format!("{:#}", error);
    assert!(message.contains("角色"), "实际错误：{}", message);
    let _ = tokio::time::timeout(Duration::from_secs(5), server).await;
}
