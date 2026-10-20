use std::time::Duration;

use ciborium::value::Value;
use dsh_remote::agent;
use dsh_remote::call;
use dsh_remote::config::{ClientConfig, GithubConfig, HubConfig};
use dsh_remote::hub;
use dsh_remote::key::KeyMaterial;
use dsh_remote::proto::value;
use dsh_remote::proto::EndStatus;
use dsh_remote::session::Session;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::{timeout, Instant};
use tokio_util::sync::CancellationToken;

struct Harness {
    addr: String,
    key: KeyMaterial,
    shutdown: CancellationToken,
    hub: JoinHandle<anyhow::Result<()>>,
    agent: JoinHandle<anyhow::Result<()>>,
}

impl Harness {
    fn client(&self) -> ClientConfig {
        ClientConfig {
            hub: self.addr.clone(),
            key: self.key.clone(),
            name: "dsh".to_string(),
            roots: vec!["/".to_string()],
            allow_bash: true,
            max_output: 262_144,
            max_file: 67_108_864,
            log: "stderr".to_string(),
            github: GithubConfig::default(),
        }
    }

    async fn session(&self) -> Session {
        call::connect(&self.client()).await.unwrap()
    }

    async fn stop(self) {
        self.shutdown.cancel();
        let _ = timeout(Duration::from_secs(3), self.agent).await;
        let _ = timeout(Duration::from_secs(3), self.hub).await;
    }
}

async fn remote_call(
    harness: &Harness,
    target: &str,
    method: &str,
    params: Value,
    limit: Duration,
) -> call::RemoteResult<call::CallResult> {
    let session = harness.session().await;
    call::call(
        &session,
        target,
        method,
        params,
        limit,
        &CancellationToken::new(),
    )
    .await
}

async fn start(agent_allow_bash: bool, roots: Vec<String>) -> Harness {
    let key = KeyMaterial::parse("0123456789abcdef0123456789abcdef").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let shutdown = CancellationToken::new();

    let hub_cfg = HubConfig {
        listen: addr.clone(),
        key: key.clone(),
        name: "hub".to_string(),
        roots: roots.clone(),
        allow_bash: true,
        max_output: 262_144,
        max_file: 67_108_864,
        log: "stderr".to_string(),
    };
    let agent_cfg = ClientConfig {
        hub: addr.clone(),
        key: key.clone(),
        name: "nas".to_string(),
        roots,
        allow_bash: agent_allow_bash,
        max_output: 262_144,
        max_file: 67_108_864,
        log: "stderr".to_string(),
        github: GithubConfig::default(),
    };

    let hub = tokio::spawn(hub::serve(listener, hub_cfg, shutdown.clone()));
    let agent = tokio::spawn(agent::run(agent_cfg, shutdown.clone()));
    let harness = Harness {
        addr,
        key,
        shutdown,
        hub,
        agent,
    };
    wait_ready(&harness).await;
    harness
}

async fn wait_ready(harness: &Harness) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(session) = call::connect(&harness.client()).await {
            if let Ok(targets) = call::targets(&session).await {
                if targets.iter().any(|target| target.name == "nas") {
                    return;
                }
            }
        }
        if Instant::now() > deadline {
            panic!("等待被控端上线超时");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn bash_params(command: &str) -> Value {
    value::map(vec![
        ("command", Value::Text(command.to_string())),
        ("timeout_ms", Value::Integer(5_000u64.into())),
    ])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bash_on_agent() {
    let harness = start(true, vec!["/tmp".to_string()]).await;
    let result = remote_call(
        &harness,
        "nas",
        "bash",
        bash_params("echo hello"),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert!(result.ok, "error: {:?}", result.error);
    assert_eq!(result.exit_code, Some(0));
    assert_eq!(String::from_utf8_lossy(&result.stdout), "hello\n");
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bash_on_hub() {
    let harness = start(true, vec!["/tmp".to_string()]).await;
    let result = remote_call(
        &harness,
        "hub",
        "bash",
        bash_params("pwd"),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert!(result.ok, "error: {:?}", result.error);
    assert_eq!(String::from_utf8_lossy(&result.stdout).trim(), "/tmp");
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn targets_lists_hub_and_agent() {
    let harness = start(true, vec!["/tmp".to_string()]).await;
    let targets = call::targets(&harness.session().await).await.unwrap();
    assert!(targets.iter().any(|target| target.name == "hub"));
    assert!(targets.iter().any(|target| target.name == "nas"));
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bash_timeout_returns_error_end() {
    let harness = start(true, vec!["/tmp".to_string()]).await;
    let params = value::map(vec![
        ("command", Value::Text("sleep 3".to_string())),
        ("timeout_ms", Value::Integer(200u64.into())),
    ]);
    let result = remote_call(&harness, "nas", "bash", params, Duration::from_secs(10))
        .await
        .unwrap();
    assert!(!result.ok);
    assert_eq!(result.stopped, Some(EndStatus::Error));
    assert_eq!(result.exit_code, None);
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bash_disabled_is_denied() {
    let harness = start(false, vec!["/tmp".to_string()]).await;
    let result = remote_call(
        &harness,
        "nas",
        "bash",
        bash_params("echo hello"),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert!(!result.ok);
    let error = result.error.unwrap();
    assert_eq!(error.code, "denied");
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cwd_outside_roots_is_denied() {
    let harness = start(true, vec!["/tmp".to_string()]).await;
    let params = value::map(vec![
        ("command", Value::Text("pwd".to_string())),
        ("cwd", Value::Text("/etc".to_string())),
    ]);
    let result = remote_call(&harness, "nas", "bash", params, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(!result.ok);
    assert_eq!(result.error.unwrap().code, "denied");
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nonzero_exit_code_is_reported() {
    let harness = start(true, vec!["/tmp".to_string()]).await;
    let result = remote_call(
        &harness,
        "nas",
        "bash",
        bash_params("exit 3"),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert!(result.ok);
    assert_eq!(result.exit_code, Some(3));
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_target_is_reported() {
    let harness = start(true, vec!["/tmp".to_string()]).await;
    let result = remote_call(
        &harness,
        "ghost",
        "bash",
        bash_params("echo hello"),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert!(!result.ok);
    assert_eq!(result.error.unwrap().code, "unknown_target");
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stdin_is_forwarded() {
    let harness = start(true, vec!["/tmp".to_string()]).await;
    let params = value::map(vec![
        ("command", Value::Text("cat".to_string())),
        ("stdin", Value::Text("pipe-data".to_string())),
        ("timeout_ms", Value::Integer(5_000u64.into())),
    ]);
    let result = remote_call(&harness, "nas", "bash", params, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(result.ok, "error: {:?}", result.error);
    assert_eq!(String::from_utf8_lossy(&result.stdout), "pipe-data");
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_makes_agent_exit() {
    let mut harness = start(true, vec!["/tmp".to_string()]).await;
    let result = remote_call(
        &harness,
        "nas",
        "shutdown",
        value::empty_map(),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert!(result.ok, "error: {:?}", result.error);
    let value = result.into_value().unwrap();
    assert_eq!(value::bool_value(&value, "stopping"), Some(true));
    timeout(Duration::from_secs(5), &mut harness.agent)
        .await
        .expect("被控端没有在 5 秒内退出")
        .expect("被控端任务 panic")
        .expect("被控端返回错误");
    harness.shutdown.cancel();
    let _ = timeout(Duration::from_secs(3), &mut harness.hub).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_on_hub_is_unsupported() {
    let harness = start(true, vec!["/tmp".to_string()]).await;
    let result = remote_call(
        &harness,
        "hub",
        "shutdown",
        value::empty_map(),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert!(!result.ok);
    let err = result.error.unwrap();
    assert_eq!(err.code, "unsupported");
    harness.stop().await;
}
