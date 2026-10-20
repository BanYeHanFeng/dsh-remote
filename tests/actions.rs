#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use dsh_remote::actions::{GhClient, StartRequest, MAX_DURATION_MINUTES};
use dsh_remote::config::GithubConfig;
use tokio_util::sync::CancellationToken;

const SCRIPT: &str = r#"#!/bin/sh
printf '%s\n' "$*" >> "$FAKE_GH_LOG"
state="$FAKE_GH_STATE"
case "$1 $2" in
  "secret set")
    cat >> "$FAKE_GH_STDIN"
    exit 0
    ;;
  "api -X")
    case "$3 $4" in
      "POST "*"/dispatches") touch "$state.dispatched" ;;
    esac
    exit 0
    ;;
  "run list")
    if [ -f "$state.dispatched" ]; then
      printf '%s\n' '[{"databaseId":2222,"status":"queued","conclusion":"","displayTitle":"临时被控端 gh-2222","createdAt":"2026-01-02T03:04:05Z","url":"https://example.com/run/2222"}]'
    else
      printf '%s\n' '[{"databaseId":1111,"status":"completed","conclusion":"success","displayTitle":"临时被控端 gh-1111","createdAt":"2026-01-01T00:00:00Z","url":"https://example.com/run/1111"}]'
    fi
    exit 0
    ;;
  "run view")
    if [ -f "$state.stopped" ]; then
      status=completed
      conclusion=success
    else
      status=in_progress
      conclusion=
    fi
    printf '{"databaseId":%s,"status":"%s","conclusion":"%s","displayTitle":"临时被控端 gh-%s","createdAt":"2026-01-02T03:04:05Z","url":"https://example.com/run/%s"}\n' "$3" "$status" "$conclusion" "$3" "$3"
    exit 0
    ;;
  "run cancel")
    printf '%s\n' "$3" >> "$state.cancelled"
    exit 0
    ;;
esac
exit 1
"#;

fn unique_dir() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir =
        std::env::temp_dir().join(format!("dsh-remote-actions-{}-{nanos}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn config(dir: &Path) -> GithubConfig {
    GithubConfig {
        repo: Some("owner/repo".to_string()),
        workflow: "ephemeral-node.yml".to_string(),
        git_ref: "dev".to_string(),
        secret: "DSH_REMOTE_KEY".to_string(),
        hub_secret: "DSH_REMOTE_HUB".to_string(),
        token: None,
        binary: dir.join("gh").to_string_lossy().to_string(),
    }
}

fn request<'a>(hub: &'a str) -> StartRequest<'a> {
    StartRequest {
        hub,
        name: None,
        duration_minutes: 15,
        allow_bash: true,
        roots: "/",
        version: "latest",
        sync_key: true,
        sync_hub: true,
    }
}

#[tokio::test]
async fn dispatches_syncs_and_stops_gracefully() {
    let dir = unique_dir();
    let gh = dir.join("gh");
    fs::write(&gh, SCRIPT).unwrap();
    fs::set_permissions(&gh, fs::Permissions::from_mode(0o755)).unwrap();
    std::env::set_var("FAKE_GH_LOG", dir.join("calls.log"));
    std::env::set_var("FAKE_GH_STDIN", dir.join("secret.txt"));
    std::env::set_var("FAKE_GH_STATE", dir.join("state"));

    let client = GhClient::new(config(&dir));
    let cancel = CancellationToken::new();
    let started = client
        .start(&request("1.2.3.4:7000"), "room-key-123456", &cancel)
        .await
        .unwrap();
    assert_eq!(started.run_id, 2222);
    assert_eq!(started.node, "gh-2222");
    let out = &started.text;
    assert!(out.contains("#2222"), "{out}");
    assert!(out.contains("15 分钟后自动下线"), "{out}");

    let secrets = fs::read_to_string(dir.join("secret.txt")).unwrap();
    assert!(secrets.contains("room-key-123456"), "{secrets}");
    assert!(secrets.contains("1.2.3.4:7000"), "{secrets}");

    let calls = fs::read_to_string(dir.join("calls.log")).unwrap();
    assert!(calls.contains("secret set DSH_REMOTE_HUB -R owner/repo"));
    assert!(calls.contains("secret set DSH_REMOTE_KEY -R owner/repo"));
    assert!(calls
        .contains("api -X POST repos/owner/repo/actions/workflows/ephemeral-node.yml/dispatches"));
    assert!(calls.contains("ref=dev"));
    assert!(!calls.contains("inputs[hub]"), "{calls}");
    assert!(calls.contains("inputs[duration]=15"));
    assert!(calls.contains("inputs[allow_bash]=1"));
    assert!(calls.contains("inputs[version]=latest"));

    let online = vec!["gh-2222".to_string()];
    let status = client.status(10, &online, &cancel).await.unwrap();
    assert!(status.contains("2222"), "{status}");
    assert!(status.contains("在线"), "{status}");

    let (targets, notes) = client.stop_targets(Some(3333), &cancel).await.unwrap();
    assert!(notes.is_empty(), "{notes:?}");
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].run_id, 3333);
    assert_eq!(targets[0].node, "gh-3333");

    let (targets, notes) = client.stop_targets(None, &cancel).await.unwrap();
    assert!(notes.is_empty(), "{notes:?}");
    let nodes: Vec<&str> = targets.iter().map(|target| target.node.as_str()).collect();
    assert_eq!(nodes, ["gh-2222"]);

    fs::write(dir.join("state.stopped"), "").unwrap();
    let wait = client.await_finished(&[2222, 3333], &cancel).await.unwrap();
    assert!(wait.alive.is_empty(), "{:?}", wait.alive);
    let finished: Vec<(u64, &str)> = wait
        .finished
        .iter()
        .map(|(id, status)| (*id, status.as_str()))
        .collect();
    assert_eq!(
        finished,
        [(2222, "completed success"), (3333, "completed success")]
    );

    client.cancel_run(1111, &cancel).await.unwrap();
    let calls = fs::read_to_string(dir.join("calls.log")).unwrap();
    assert!(!calls.contains("git/refs"), "{calls}");
    assert!(calls.contains("run cancel 1111"), "{calls}");

    fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn requires_repo_and_sane_duration() {
    let dir = unique_dir();
    let mut cfg = config(&dir);
    cfg.repo = None;
    let client = GhClient::new(cfg);
    let cancel = CancellationToken::new();
    let err = client
        .start(&request("1.2.3.4:7000"), "room-key-123456", &cancel)
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("GH_REPO"), "{err:#}");

    let mut cfg = config(&dir);
    cfg.binary = dir.join("missing-gh").to_string_lossy().to_string();
    let client = GhClient::new(cfg);
    let too_long = StartRequest {
        hub: "1.2.3.4:7000",
        name: None,
        duration_minutes: MAX_DURATION_MINUTES + 1,
        allow_bash: true,
        roots: "/",
        version: "latest",
        sync_key: false,
        sync_hub: false,
    };
    let err = client
        .start(&too_long, "room-key-123456", &cancel)
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("duration_minutes"), "{err:#}");
    fs::remove_dir_all(&dir).ok();
}
