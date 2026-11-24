use std::time::Duration;

use dsh_remote::frame::{read_frame, write_frame};
use dsh_remote::protocol::{ControlMsg, Role};
use dsh_remote::transport;

#[tokio::test]
async fn auth_and_stream_roundtrip() {
    let server = transport::server_endpoint("127.0.0.1:0", None, None).unwrap();
    let addr = server.endpoint.local_addr().unwrap();
    let psk = b"testpsk".to_vec();

    let psk2 = psk.clone();
    let server_task = tokio::spawn(async move {
        let incoming = server.endpoint.accept().await.unwrap();
        let conn = incoming.await.unwrap();
        let (role, node_id, tags, mut control) = transport::serve_auth(&conn, &psk2, &[Role::Agent])
            .await
            .unwrap();
        assert_eq!(role, Role::Agent);
        assert_eq!(node_id, "node-a");
        assert!(tags.is_empty());
        match read_frame::<_, ControlMsg>(&mut control).await.unwrap() {
            ControlMsg::Ping => write_frame(&mut control, &ControlMsg::Pong).await.unwrap(),
            _ => panic!("意外消息"),
        }
        let mut op = transport::accept(&conn).await.unwrap();
        let echo: ControlMsg = read_frame(&mut op).await.unwrap();
        write_frame(&mut op, &echo).await.unwrap();
        let _keep = server.fingerprint;
        tokio::time::sleep(Duration::from_secs(2)).await;
    });

    let client = transport::client_endpoint(None).unwrap();
    let conn = client.connect(addr, "dsh-remote").unwrap().await.unwrap();
    let mut control = transport::client_auth(&conn, &psk, Role::Agent, "node-a", &[])
        .await
        .unwrap();
    write_frame(&mut control, &ControlMsg::Ping).await.unwrap();
    let pong: ControlMsg = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut control))
        .await
        .expect("等待 Pong 超时")
        .unwrap();
    assert!(matches!(pong, ControlMsg::Pong));

    let mut op = transport::open(&conn).await.unwrap();
    write_frame(&mut op, &ControlMsg::Ping).await.unwrap();
    let echoed: ControlMsg = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut op))
        .await
        .expect("等待回显超时")
        .unwrap();
    assert!(matches!(echoed, ControlMsg::Ping));

    server_task.abort();
}

#[tokio::test]
async fn auth_rejects_wrong_psk() {
    let server = transport::server_endpoint("127.0.0.1:0", None, None).unwrap();
    let addr = server.endpoint.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let incoming = server.endpoint.accept().await.unwrap();
        let conn = incoming.await.unwrap();
        let _ = transport::serve_auth(&conn, b"right", &[Role::Agent]).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    });

    let client = transport::client_endpoint(None).unwrap();
    let conn = client.connect(addr, "dsh-remote").unwrap().await.unwrap();
    let result = transport::client_auth(&conn, b"wrong", Role::Agent, "node-a", &[]).await;
    let message = format!("{:#}", result.err().expect("应当认证失败"));
    assert!(message.contains("认证失败"), "实际错误：{}", message);
    server_task.abort();
}
