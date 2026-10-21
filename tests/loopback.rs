use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ciborium::value::Value;
use dsh_remote::agent;
use dsh_remote::config::{ClientConfig, GithubConfig, HubConfig};
use dsh_remote::hub;
use dsh_remote::key::KeyMaterial;
use dsh_remote::net::SecureConn;
use dsh_remote::proto::{caps, Msg, Role, TargetInfo, PROTO_VERSION};
use tokio::net::TcpListener;
use tokio::time::{timeout, Instant};
use tokio_util::sync::CancellationToken;

fn key() -> KeyMaterial {
    KeyMaterial::parse("0123456789abcdef0123456789abcdef").unwrap()
}

fn hello(role: Role, name: &str) -> Msg {
    Msg::Hello {
        proto: PROTO_VERSION,
        role,
        name: name.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        caps: caps(true),
        roots: vec!["/".to_string()],
    }
}

fn hub_config(addr: &str, key: &KeyMaterial) -> HubConfig {
    HubConfig {
        listen: addr.to_string(),
        key: key.clone(),
        name: "hub".to_string(),
        roots: vec!["/".to_string()],
        allow_bash: true,
        max_output: 262_144,
        max_file: 67_108_864,
        log: "stderr".to_string(),
    }
}

fn client_config(addr: &str, key: &KeyMaterial, name: &str) -> ClientConfig {
    ClientConfig {
        hub: addr.to_string(),
        key: key.clone(),
        name: name.to_string(),
        roots: vec!["/".to_string()],
        allow_bash: true,
        max_output: 262_144,
        max_file: 67_108_864,
        log: "stderr".to_string(),
        github: GithubConfig::default(),
    }
}

#[tokio::test]
async fn handshake_and_ping() {
    let key = key();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let responder_key = key.clone();
    let responder = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut conn = SecureConn::accept(stream, &responder_key).await.unwrap();
        let msg = conn.recv().await.unwrap();
        assert_eq!(msg, Msg::Ping { nonce: 7 });
        conn.send(&Msg::Pong { nonce: 7 }).await.unwrap();
    });

    let mut client = SecureConn::connect(&addr.to_string(), &key).await.unwrap();
    client.send(&Msg::Ping { nonce: 7 }).await.unwrap();
    assert_eq!(client.recv().await.unwrap(), Msg::Pong { nonce: 7 });
    responder.await.unwrap();
}

#[tokio::test]
async fn wrong_key_rejected() {
    let good = key();
    let bad = KeyMaterial::parse("another-key-0123456789abcdef").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let responder = tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            let _ = SecureConn::accept(stream, &good).await;
        }
    });

    let attempt = timeout(
        Duration::from_secs(3),
        SecureConn::connect(&addr.to_string(), &bad),
    )
    .await;
    assert!(matches!(attempt, Ok(Err(_)) | Err(_)));
    responder.await.unwrap();
}

async fn wait_for_target(addr: &str, key: &KeyMaterial, name: &str) -> Vec<TargetInfo> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(mut conn) = SecureConn::connect(addr, key).await {
            if conn.send(&hello(Role::Server, "test-server")).await.is_ok() {
                if let Ok(Ok(Msg::Welcome { targets, .. })) =
                    timeout(Duration::from_secs(2), conn.recv()).await
                {
                    if targets.iter().any(|target| target.name == name) {
                        return targets;
                    }
                }
            }
        }
        if Instant::now() > deadline {
            panic!("等待目标 {name} 上线超时");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_registers_agent() {
    let key = key();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();

    let hub_task = tokio::spawn(hub::serve(
        listener,
        hub_config(&addr.to_string(), &key),
        shutdown.clone(),
    ));
    let agent_task = tokio::spawn(agent::run(
        client_config(&addr.to_string(), &key, "nas"),
        shutdown.clone(),
    ));

    let targets = wait_for_target(&addr.to_string(), &key, "nas").await;
    assert!(targets.iter().any(|target| target.name == "hub"));
    assert!(targets
        .iter()
        .any(|target| target.name == "nas" && target.kind == dsh_remote::proto::TargetKind::Agent));

    shutdown.cancel();
    let _ = timeout(Duration::from_secs(3), agent_task).await;
    let _ = timeout(Duration::from_secs(3), hub_task).await;
}

async fn recv_kind<F>(conn: &mut SecureConn, want: F, label: &str) -> Msg
where
    F: Fn(&Msg) -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match timeout(left, conn.recv()).await {
            Ok(Ok(msg)) => {
                if want(&msg) {
                    return msg;
                }
            }
            Ok(Err(err)) => panic!("等待 {label} 时连接出错: {err}"),
            Err(_) => panic!("等待 {label} 超时"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_agent_name_replaced() {
    let key = key();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();

    let hub_task = tokio::spawn(hub::serve(
        listener,
        hub_config(&addr.to_string(), &key),
        shutdown.clone(),
    ));

    let mut first = SecureConn::connect(&addr.to_string(), &key).await.unwrap();
    first.send(&hello(Role::Agent, "nas")).await.unwrap();
    recv_kind(
        &mut first,
        |msg| matches!(msg, Msg::Welcome { .. }),
        "首个 Welcome",
    )
    .await;

    let mut second = SecureConn::connect(&addr.to_string(), &key).await.unwrap();
    second.send(&hello(Role::Agent, "nas")).await.unwrap();
    recv_kind(
        &mut second,
        |msg| matches!(msg, Msg::Welcome { .. }),
        "顶替方 Welcome",
    )
    .await;

    match recv_kind(&mut first, |msg| matches!(msg, Msg::Bye { .. }), "replaced").await {
        Msg::Bye { code, .. } => assert_eq!(code, "replaced"),
        other => panic!("期望 replaced，收到 {other:?}"),
    }

    let targets = wait_for_target(&addr.to_string(), &key, "nas").await;
    assert_eq!(
        targets.iter().filter(|target| target.name == "nas").count(),
        1
    );

    drop(first);
    drop(second);
    shutdown.cancel();
    let _ = timeout(Duration::from_secs(3), hub_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_fails_pending_calls() {
    let key = key();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();

    let hub_task = tokio::spawn(hub::serve(
        listener,
        hub_config(&addr.to_string(), &key),
        shutdown.clone(),
    ));

    let mut agent = SecureConn::connect(&addr.to_string(), &key).await.unwrap();
    agent.send(&hello(Role::Agent, "nas")).await.unwrap();
    recv_kind(
        &mut agent,
        |msg| matches!(msg, Msg::Welcome { .. }),
        "agent Welcome",
    )
    .await;

    let mut server = SecureConn::connect(&addr.to_string(), &key).await.unwrap();
    server
        .send(&hello(Role::Server, "test-server"))
        .await
        .unwrap();
    recv_kind(
        &mut server,
        |msg| matches!(msg, Msg::Welcome { .. }),
        "server Welcome",
    )
    .await;

    server
        .send(&Msg::Call {
            id: 42,
            target: "nas".to_string(),
            method: "roots".to_string(),
            params: Value::Map(Vec::new()),
        })
        .await
        .unwrap();
    recv_kind(
        &mut agent,
        |msg| matches!(msg, Msg::Call { .. }),
        "转发的调用",
    )
    .await;

    let mut successor = SecureConn::connect(&addr.to_string(), &key).await.unwrap();
    successor.send(&hello(Role::Agent, "nas")).await.unwrap();
    recv_kind(
        &mut successor,
        |msg| matches!(msg, Msg::Welcome { .. }),
        "后继 Welcome",
    )
    .await;

    match recv_kind(
        &mut server,
        |msg| matches!(msg, Msg::Result { .. }),
        "offline 结果",
    )
    .await
    {
        Msg::Result { id, ok, error, .. } => {
            assert_eq!(id, 42);
            assert!(!ok);
            assert_eq!(error.unwrap().code, "offline");
        }
        other => panic!("期望 offline 结果，收到 {other:?}"),
    }

    drop(agent);
    drop(successor);
    drop(server);
    shutdown.cancel();
    let _ = timeout(Duration::from_secs(3), hub_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_name_conflicts_with_hub() {
    let key = key();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();

    let hub_task = tokio::spawn(hub::serve(
        listener,
        hub_config(&addr.to_string(), &key),
        shutdown.clone(),
    ));

    let mut conn = SecureConn::connect(&addr.to_string(), &key).await.unwrap();
    conn.send(&hello(Role::Agent, "hub")).await.unwrap();
    let reply = timeout(Duration::from_secs(2), conn.recv())
        .await
        .unwrap()
        .unwrap();
    match reply {
        Msg::Bye { code, .. } => assert_eq!(code, "duplicate_name"),
        other => panic!("期望 duplicate_name，收到 {other:?}"),
    }

    drop(conn);
    shutdown.cancel();
    let _ = timeout(Duration::from_secs(3), hub_task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agent_reconnects_after_drop() {
    let key = key();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = accepted.clone();

    let fake_hub = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            counter.fetch_add(1, Ordering::SeqCst);
            drop(stream);
        }
    });

    let shutdown = CancellationToken::new();
    let agent_task = tokio::spawn(agent::run(
        client_config(&addr.to_string(), &key, "nas"),
        shutdown.clone(),
    ));

    let deadline = Instant::now() + Duration::from_secs(6);
    while accepted.load(Ordering::SeqCst) < 2 {
        if Instant::now() > deadline {
            panic!("被控端没有重连");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    shutdown.cancel();
    let _ = timeout(Duration::from_secs(3), agent_task).await;
    fake_hub.abort();
}
