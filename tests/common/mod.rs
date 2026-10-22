#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use ciborium::value::Value;
use dsh_remote::agent;
use dsh_remote::call;
use dsh_remote::config::{ClientConfig, GithubConfig, HubConfig};
use dsh_remote::exec::Settings;
use dsh_remote::hub;
use dsh_remote::key::KeyMaterial;
use dsh_remote::proto::{value, Msg, Role, PROTO_VERSION};
use dsh_remote::session::Session;
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::{timeout, Instant};
use tokio_util::sync::CancellationToken;

pub const KEY: &str = "0123456789abcdef0123456789abcdef";
pub const AGENT: &str = "nas";

pub fn key() -> KeyMaterial {
    KeyMaterial::parse(KEY).unwrap()
}

pub fn no_cancel() -> CancellationToken {
    CancellationToken::new()
}

pub fn settings(roots: Vec<String>, allow_bash: bool) -> Settings {
    Settings {
        roots,
        allow_bash,
        max_output: 262_144,
        max_file: 67_108_864,
    }
}

pub fn hub_config(addr: &str, roots: Vec<String>, allow_bash: bool) -> HubConfig {
    HubConfig {
        listen: addr.to_string(),
        key: key(),
        name: "hub".to_string(),
        target: settings(roots, allow_bash),
        log: "stderr".to_string(),
    }
}

pub fn client_config(addr: &str, name: &str, roots: Vec<String>, allow_bash: bool) -> ClientConfig {
    ClientConfig {
        hub: addr.to_string(),
        key: key(),
        name: name.to_string(),
        target: settings(roots, allow_bash),
        log: "stderr".to_string(),
        github: GithubConfig::default(),
    }
}

pub fn hello(role: Role, name: &str) -> Msg {
    Msg::Hello {
        proto: PROTO_VERSION,
        role,
        name: name.to_string(),
        version: "test".to_string(),
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        roots: vec!["/".to_string()],
    }
}

pub fn bash_params(command: &str, timeout_ms: u64) -> Value {
    value::map(vec![
        ("command", Value::Text(command.to_string())),
        ("timeout_ms", Value::Integer(timeout_ms.into())),
    ])
}

pub fn sha256_file(path: &Path) -> String {
    hex::encode(Sha256::digest(std::fs::read(path).unwrap()))
}

pub fn write_pattern(path: &Path, size: u64) {
    use std::io::Write;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let mut file = std::fs::File::create(path).unwrap();
    let mut buffer = vec![0u8; 1024 * 1024];
    for (index, byte) in buffer.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    let mut left = size;
    while left > 0 {
        let take = left.min(buffer.len() as u64) as usize;
        file.write_all(&buffer[..take]).unwrap();
        left -= take as u64;
    }
    file.sync_all().unwrap();
}

pub struct Harness {
    pub addr: String,
    pub root: PathBuf,
    pub roots: Vec<String>,
    pub hub_allow_bash: bool,
    pub agent_allow_bash: bool,
    pub hub_shutdown: CancellationToken,
    pub agent_shutdown: CancellationToken,
    pub hub: Option<JoinHandle<anyhow::Result<()>>>,
    pub agent: Option<JoinHandle<anyhow::Result<()>>>,
}

impl Harness {
    pub async fn start(
        tag: &str,
        hub_allow_bash: bool,
        agent_allow_bash: bool,
        roots: Option<Vec<String>>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let root = std::env::temp_dir().join(format!(
            "dsh-remote-{tag}-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let roots = roots.unwrap_or_else(|| vec![root.to_string_lossy().to_string()]);
        let mut harness = Self {
            addr,
            root,
            roots,
            hub_allow_bash,
            agent_allow_bash,
            hub_shutdown: CancellationToken::new(),
            agent_shutdown: CancellationToken::new(),
            hub: None,
            agent: None,
        };
        harness
            .spawn_hub(Some(listener), &harness.addr.clone())
            .await;
        harness.spawn_agent().await;
        harness.wait_ready().await;
        harness
    }

    async fn spawn_hub(&mut self, listener: Option<TcpListener>, addr: &str) {
        let listener = match listener {
            Some(listener) => listener,
            None => TcpListener::bind(addr).await.unwrap(),
        };
        self.hub_shutdown = CancellationToken::new();
        self.hub = Some(tokio::spawn(hub::serve(
            listener,
            hub_config(addr, self.roots.clone(), self.hub_allow_bash),
            self.hub_shutdown.clone(),
        )));
    }

    async fn spawn_agent(&mut self) {
        self.agent_shutdown = CancellationToken::new();
        self.agent = Some(tokio::spawn(agent::run(
            client_config(&self.addr, AGENT, self.roots.clone(), self.agent_allow_bash),
            self.agent_shutdown.clone(),
        )));
    }

    pub fn client(&self) -> ClientConfig {
        client_config(&self.addr, "dsh", self.roots.clone(), true)
    }

    pub fn remote(&self, name: &str) -> String {
        self.root.join(name).to_string_lossy().to_string()
    }

    pub fn local(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    pub async fn session(&self) -> Session {
        call::connect(&self.client()).await.unwrap()
    }

    pub async fn stop_agent(&mut self) {
        self.agent_shutdown.cancel();
        if let Some(handle) = self.agent.take() {
            let _ = timeout(Duration::from_secs(3), handle).await;
        }
    }

    pub async fn stop_hub(&mut self) {
        self.hub_shutdown.cancel();
        if let Some(handle) = self.hub.take() {
            let _ = timeout(Duration::from_secs(3), handle).await;
        }
    }

    pub async fn restart_hub(&mut self) {
        let addr = self.addr.clone();
        self.spawn_hub(None, &addr).await;
    }

    pub async fn wait_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(session) = call::connect(&self.client()).await {
                if let Ok(targets) = call::targets(&session).await {
                    if targets.iter().any(|target| target.name == AGENT) {
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

    pub async fn stop(mut self) {
        self.stop_agent().await;
        self.stop_hub().await;
        std::fs::remove_dir_all(&self.root).ok();
    }
}

pub async fn remote_call(
    harness: &Harness,
    target: &str,
    method: &str,
    params: Value,
    limit: Duration,
) -> call::RemoteResult<call::CallResult> {
    let session = harness.session().await;
    call::call(&session, target, method, params, limit, &no_cancel()).await
}

pub fn error_code(error: &anyhow::Error) -> String {
    error
        .downcast_ref::<call::RemoteError>()
        .map(|error| error.code.clone())
        .unwrap_or_else(|| format!("{error:#}"))
}
