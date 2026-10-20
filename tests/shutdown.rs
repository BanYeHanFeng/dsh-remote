use std::time::Duration;

use dsh_remote::agent;
use dsh_remote::config::{ClientConfig, GithubConfig, HubConfig};
use dsh_remote::hub;
use dsh_remote::key::KeyMaterial;
use dsh_remote::net::SecureConn;
use dsh_remote::proto::{caps, value, Msg, Role, TargetEventKind, PROTO_VERSION};
use dsh_remote::session::Session;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

fn key() -> KeyMaterial {
    KeyMaterial::parse("0123456789abcdef0123456789abcdef").unwrap()
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

fn hello(role: Role, name: &str) -> Msg {
    Msg::Hello {
        proto: PROTO_VERSION,
        role,
        name: name.to_string(),
        version: "test".to_string(),
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        caps: caps(true),
        roots: vec!["/".to_string()],
    }
}

async fn watch(
    addr: String,
    key: KeyMaterial,
    wanted: &'static str,
) -> (
    tokio::sync::oneshot::Receiver<()>,
    mpsc::UnboundedReceiver<TargetEventKind>,
) {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut conn = match timeout(Duration::from_secs(5), SecureConn::connect(&addr, &key)).await
        {
            Ok(Ok(conn)) => conn,
            _ => return,
        };
        if conn.send(&hello(Role::Server, "watcher")).await.is_err() {
            return;
        }
        match conn.recv().await {
            Ok(Msg::Welcome { targets, .. }) => {
                for target in targets {
                    if target.name == wanted {
                        let _ = tx.send(TargetEventKind::Online);
                    }
                }
            }
            _ => return,
        }
        let _ = ready_tx.send(());
        while let Ok(msg) = conn.recv().await {
            if let Msg::TargetEvent { kind, target } = msg {
                if target.name == wanted && tx.send(kind).is_err() {
                    return;
                }
            }
        }
    });
    (ready_rx, rx)
}

async fn wait_kind(events: &mut mpsc::UnboundedReceiver<TargetEventKind>, wanted: TargetEventKind) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match timeout(deadline - tokio::time::Instant::now(), events.recv()).await {
            Ok(Some(kind)) if kind == wanted => return,
            Ok(Some(_)) => continue,
            _ => panic!("等待目标事件超时"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_stops_agent_without_reconnect() {
    let key = key();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let shutdown = CancellationToken::new();

    let hub_task = tokio::spawn(hub::serve(
        listener,
        hub_config(&addr, &key),
        shutdown.clone(),
    ));
    let (ready, mut events) = watch(addr.clone(), key.clone(), "nas").await;
    ready.await.unwrap();
    let agent_task = tokio::spawn(agent::run(
        client_config(&addr, &key, "nas"),
        shutdown.clone(),
    ));
    wait_kind(&mut events, TargetEventKind::Online).await;

    let session = Session::new(&client_config(&addr, &key, "test-server"));
    let session_task = tokio::spawn({
        let session = session.clone();
        let shutdown = shutdown.clone();
        async move { session.run(shutdown).await }
    });
    session.wait_connected(Duration::from_secs(5)).await;

    let result = session
        .call_cancel(
            "nas",
            "shutdown",
            value::empty_map(),
            Duration::from_secs(10),
            &CancellationToken::new(),
        )
        .await;
    assert!(
        result.is_ok(),
        "关停调用失败: {}",
        result.err().map(|err| err.to_string()).unwrap_or_default()
    );

    let ended = timeout(Duration::from_secs(5), agent_task).await;
    assert!(ended.is_ok(), "被控端收到关停后没有退出");

    let mut extra = 0;
    while let Ok(Some(kind)) = timeout(Duration::from_millis(300), events.recv()).await {
        if kind == TargetEventKind::Online {
            extra += 1;
        }
    }
    assert_eq!(extra, 0, "关停后又被控端重新上线了 {extra} 次");

    shutdown.cancel();
    session_task.abort();
    let _ = timeout(Duration::from_secs(3), hub_task).await;
}
