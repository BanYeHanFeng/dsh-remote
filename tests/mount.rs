mod common;

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use common::*;
use dsh_remote::config::MountConfig;
use dsh_remote::mount;
use tokio_util::sync::CancellationToken;

const READY_LIMIT: Duration = Duration::from_secs(10);
const SETTLE_LIMIT: Duration = Duration::from_secs(10);
const ATTR_TTL: Duration = Duration::from_millis(100);

struct Mounted {
    mountpoint: PathBuf,
    token: CancellationToken,
    handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Mounted {
    async fn start(harness: &Harness, read_only: bool) -> Option<Self> {
        let mountpoint = std::env::temp_dir().join(format!(
            "dsh-remote-mnt-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&mountpoint).unwrap();
        let cfg = MountConfig {
            client: harness.client(),
            target: AGENT.to_string(),
            mountpoint: mountpoint.clone(),
            remote_root: harness.root.to_string_lossy().to_string(),
            read_only,
            allow_other: false,
            attr_ttl: ATTR_TTL,
            spool_dir: None,
            spool_limit: 64 * 1024 * 1024,
        };
        let token = CancellationToken::new();
        let handle = tokio::spawn(mount::run(cfg, token.clone()));
        if !wait_ready(&mountpoint).await {
            let outcome = handle.await.unwrap_or_else(|err| {
                panic!("挂载任务异常退出: {err}");
            });
            eprintln!("跳过 FUSE 用例: 挂载不可用: {outcome:?}");
            std::fs::remove_dir_all(&mountpoint).ok();
            return None;
        }
        Some(Self {
            mountpoint,
            token,
            handle,
        })
    }

    fn path(&self, name: &str) -> PathBuf {
        self.mountpoint.join(name)
    }

    async fn stop(self) {
        self.token.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(10), self.handle).await;
        std::fs::remove_dir_all(&self.mountpoint).ok();
    }
}

async fn wait_ready(mountpoint: &Path) -> bool {
    let deadline = Instant::now() + READY_LIMIT;
    while Instant::now() < deadline {
        if is_mounted(mountpoint) && std::fs::read_dir(mountpoint).is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

fn is_mounted(mountpoint: &Path) -> bool {
    let wanted = mountpoint.to_string_lossy().to_string();
    std::fs::read_to_string("/proc/self/mounts")
        .unwrap_or_default()
        .lines()
        .any(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            fields.len() >= 2 && fields[0].starts_with("dsh-remote:") && fields[1] == wanted
        })
}

async fn wait_until<F: Fn() -> bool>(condition: F) -> bool {
    let deadline = Instant::now() + SETTLE_LIMIT;
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    condition()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn edits_files_through_the_mount() {
    let harness = Harness::start("mount-rw", true, true, None).await;
    std::fs::write(harness.local("hello.txt"), b"first\n").unwrap();
    let Some(mounted) = Mounted::start(&harness, false).await else {
        harness.stop().await;
        return;
    };

    assert!(wait_until(|| mounted.path("hello.txt").exists()).await);
    assert_eq!(
        std::fs::read(mounted.path("hello.txt")).unwrap(),
        b"first\n"
    );
    assert_eq!(
        std::fs::metadata(mounted.path("hello.txt")).unwrap().len(),
        6
    );

    std::fs::write(mounted.path("hello.txt"), b"second\n").unwrap();
    let remote = harness.local("hello.txt");
    assert!(
        wait_until(|| std::fs::read(&remote)
            .map(|data| data == b"second\n")
            .unwrap_or(false))
        .await,
        "远端内容没有跟着回传: {:?}",
        std::fs::read(&remote)
    );

    std::fs::write(mounted.path("new.txt"), b"created\n").unwrap();
    let new_remote = harness.local("new.txt");
    assert!(wait_until(|| new_remote.exists()).await);
    assert_eq!(std::fs::read(&new_remote).unwrap(), b"created\n");

    std::fs::create_dir(mounted.path("dir")).unwrap();
    assert!(wait_until(|| harness.local("dir").is_dir()).await);

    std::fs::rename(mounted.path("new.txt"), mounted.path("dir/moved.txt")).unwrap();
    assert!(wait_until(|| harness.local("dir/moved.txt").exists()).await);
    assert!(!new_remote.exists());

    let names: Vec<String> = std::fs::read_dir(&mounted.mountpoint)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .collect();
    assert!(names.contains(&"hello.txt".to_string()), "{names:?}");
    assert!(names.contains(&"dir".to_string()), "{names:?}");

    std::os::unix::fs::symlink("hello.txt", mounted.path("ln.txt")).unwrap();
    let link_remote = harness.local("ln.txt");
    assert!(wait_until(|| link_remote.is_symlink()).await);
    assert_eq!(
        std::fs::read_link(&link_remote).unwrap().to_string_lossy(),
        "hello.txt"
    );
    assert_eq!(
        std::fs::read_link(mounted.path("ln.txt"))
            .unwrap()
            .to_string_lossy(),
        "hello.txt"
    );
    assert_eq!(std::fs::read(mounted.path("ln.txt")).unwrap(), b"second\n");
    std::fs::remove_file(mounted.path("ln.txt")).unwrap();
    assert!(wait_until(|| !link_remote.exists() && !link_remote.is_symlink()).await);

    std::fs::remove_file(mounted.path("dir/moved.txt")).unwrap();
    assert!(wait_until(|| !harness.local("dir/moved.txt").exists()).await);

    std::fs::write(mounted.path("dir/keep.txt"), b"keep\n").unwrap();
    assert!(wait_until(|| harness.local("dir/keep.txt").exists()).await);
    assert!(std::fs::remove_dir(mounted.path("dir")).is_err());
    assert!(harness.local("dir").is_dir());
    std::fs::remove_file(mounted.path("dir/keep.txt")).unwrap();
    assert!(wait_until(|| !harness.local("dir/keep.txt").exists()).await);
    assert!(std::fs::remove_dir(mounted.path("dir")).is_ok());
    assert!(wait_until(|| !harness.local("dir").exists()).await);

    std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(mounted.path("hello.txt"))
        .unwrap()
        .write_all(b"x")
        .unwrap();
    assert!(
        wait_until(|| std::fs::read(&remote)
            .map(|data| data == b"x")
            .unwrap_or(false))
        .await,
        "截断后远端内容不对: {:?}",
        std::fs::read(&remote)
    );

    std::fs::write(&remote, b"edited-remotely\n").unwrap();
    tokio::time::sleep(ATTR_TTL + Duration::from_millis(200)).await;
    assert_eq!(
        std::fs::read(mounted.path("hello.txt")).unwrap(),
        b"edited-remotely\n"
    );

    mounted.stop().await;
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn git_can_commit_through_the_mount() {
    if Command::new("git").arg("--version").output().is_err() {
        eprintln!("跳过 git 用例: 没有 git");
        return;
    }
    let harness = Harness::start("mount-git", true, true, None).await;
    let Some(mounted) = Mounted::start(&harness, false).await else {
        harness.stop().await;
        return;
    };
    let repo = mounted.path("repo");
    std::fs::create_dir(&repo).unwrap();
    let git = |args: &[&str]| {
        let output = Command::new("git")
            .args(args)
            .current_dir(&repo)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?} 失败: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).to_string()
    };
    git(&["init", "-q", "."]);
    std::fs::write(repo.join("a.txt"), b"hello\n").unwrap();
    git(&["add", "a.txt"]);
    git(&[
        "-c",
        "user.email=a@b",
        "-c",
        "user.name=dsh",
        "-c",
        "commit.gpgsign=false",
        "commit",
        "-qm",
        "第一次提交",
    ]);
    assert_eq!(git(&["status", "--porcelain"]), "");

    std::fs::write(repo.join("a.txt"), b"changed\n").unwrap();
    assert!(git(&["status", "--porcelain"]).contains("a.txt"));
    git(&["checkout", "--", "a.txt"]);
    assert_eq!(std::fs::read(repo.join("a.txt")).unwrap(), b"hello\n");
    assert_eq!(git(&["status", "--porcelain"]), "");

    assert!(wait_until(|| harness.local("repo/a.txt").exists()).await);
    let remote = harness.local("repo/a.txt");
    assert!(
        wait_until(|| std::fs::read(&remote)
            .map(|data| data == b"hello\n")
            .unwrap_or(false))
        .await,
        "工作区被 checkout 还原后远端还没跟上: {:?}",
        std::fs::read(&remote)
    );
    assert!(harness.local("repo/.git/HEAD").exists());

    mounted.stop().await;
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_only_mount_refuses_writes() {
    let harness = Harness::start("mount-ro", true, true, None).await;
    std::fs::write(harness.local("a.txt"), b"keep\n").unwrap();
    let Some(mounted) = Mounted::start(&harness, true).await else {
        harness.stop().await;
        return;
    };

    assert!(wait_until(|| mounted.path("a.txt").exists()).await);
    assert_eq!(std::fs::read(mounted.path("a.txt")).unwrap(), b"keep\n");
    assert!(std::fs::write(mounted.path("a.txt"), b"nope\n").is_err());
    assert!(std::fs::write(mounted.path("b.txt"), b"nope\n").is_err());
    assert_eq!(std::fs::read(harness.local("a.txt")).unwrap(), b"keep\n");
    assert!(!harness.local("b.txt").exists());

    mounted.stop().await;
    harness.stop().await;
}
