mod common;

use std::path::Path;
use std::time::Duration;

use common::*;
use dsh_remote::call;
use dsh_remote::net::SecureConn;
use dsh_remote::proto::value;
use dsh_remote::proto::{EndStatus, Msg, Role};
use tokio::time::{timeout, Instant};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_read_edit() {
    let harness = Harness::start("files", false, false, None).await;
    let cfg = harness.session().await;
    let path = harness.remote("notes/readme.txt");

    let report = call::write(&cfg, "nas", &path, "alpha\nbeta\n", true, &no_cancel())
        .await
        .unwrap();
    assert!(report.created);
    assert_eq!(report.size, 11);
    assert_eq!(report.sha256.len(), 64);

    let read = call::read(&cfg, "nas", &path, 0, 100, &no_cancel())
        .await
        .unwrap();
    assert!(!read.binary);
    assert_eq!(read.lines, vec!["1\talpha", "2\tbeta"]);
    assert!(read.eof);

    let edited = call::edit(&cfg, "nas", &path, "beta", "gamma", false, &no_cancel())
        .await
        .unwrap();
    assert_eq!(edited.replacements, 1);
    let read = call::read(&cfg, "nas", &path, 0, 100, &no_cancel())
        .await
        .unwrap();
    assert_eq!(read.lines, vec!["1\talpha", "2\tgamma"]);

    let paged = call::read(&cfg, "nas", &path, 1, 1, &no_cancel())
        .await
        .unwrap();
    assert_eq!(paged.lines, vec!["2\tgamma"]);
    assert!(paged.eof);
    assert_eq!(paged.next_offset, 2);

    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_on_hub() {
    let harness = Harness::start("files", false, false, None).await;
    let cfg = harness.session().await;
    let path = harness.remote("hub.txt");
    call::write(&cfg, "hub", &path, "from hub", false, &no_cancel())
        .await
        .unwrap();
    let read = call::read(&cfg, "hub", &path, 0, 10, &no_cancel())
        .await
        .unwrap();
    assert_eq!(read.lines, vec!["1\tfrom hub"]);
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_and_download_on_hub() {
    let harness = Harness::start("files", false, false, None).await;
    let cfg = harness.session().await;
    let local = harness.root.join("hub-local.bin");
    write_pattern(&local, 512 * 1024 + 7);
    let remote = harness.remote("hub-remote.bin");
    let expected = sha256_file(&local);

    let report = call::upload(&cfg, "hub", &local, &remote, false, false, &no_cancel())
        .await
        .unwrap();
    assert_eq!(report.sha256, expected);

    let downloaded = harness.root.join("hub-downloaded.bin");
    let report = call::download(&cfg, "hub", &remote, &downloaded, false, &no_cancel())
        .await
        .unwrap();
    assert_eq!(report.sha256, expected);
    assert_eq!(sha256_file(&downloaded), expected);
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn path_outside_roots_is_denied() {
    let harness = Harness::start("files", false, false, None).await;
    let cfg = harness.session().await;
    let error = call::read(&cfg, "nas", "/etc/passwd", 0, 10, &no_cancel())
        .await
        .unwrap_err();
    assert_eq!(error_code(&error), "denied");
    let error = call::write(&cfg, "nas", "/etc/dsh-remote-m3", "x", false, &no_cancel())
        .await
        .unwrap_err();
    assert_eq!(error_code(&error), "denied");
    harness.stop().await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn symlink_escape_is_denied() {
    let harness = Harness::start("files", false, false, None).await;
    let link = harness.root.join("escape");
    std::os::unix::fs::symlink("/etc", &link).unwrap();
    let cfg = harness.session().await;
    let target = link.join("passwd");
    let error = call::read(&cfg, "nas", target.to_str().unwrap(), 0, 10, &no_cancel())
        .await
        .unwrap_err();
    assert_eq!(error_code(&error), "denied");
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn edit_requires_unique_match() {
    let harness = Harness::start("files", false, false, None).await;
    let cfg = harness.session().await;
    let path = harness.remote("dup.txt");
    call::write(&cfg, "nas", &path, "one one", false, &no_cancel())
        .await
        .unwrap();
    let error = call::edit(&cfg, "nas", &path, "one", "two", false, &no_cancel())
        .await
        .unwrap_err();
    assert_eq!(error_code(&error), "conflict");
    let report = call::edit(&cfg, "nas", &path, "one", "two", true, &no_cancel())
        .await
        .unwrap();
    assert_eq!(report.replacements, 2);
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binary_read_returns_metadata_only() {
    let harness = Harness::start("files", false, false, None).await;
    let cfg = harness.session().await;
    let path = harness.root.join("blob.bin");
    std::fs::write(&path, [0u8, 1, 2, 3, 0, 5]).unwrap();
    let read = call::read(&cfg, "nas", path.to_str().unwrap(), 0, 10, &no_cancel())
        .await
        .unwrap();
    assert!(read.binary);
    assert!(read.lines.is_empty());
    assert_eq!(read.size, 6);
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_and_download_roundtrip() {
    let harness = Harness::start("files", false, false, None).await;
    let cfg = harness.session().await;
    let local = harness.root.join("local.bin");
    write_pattern(&local, 1024 * 1024 + 123);
    let remote = harness.root.join("sub/remote.bin");
    let remote_text = remote.to_string_lossy().to_string();

    let report = call::upload(&cfg, "nas", &local, &remote_text, false, true, &no_cancel())
        .await
        .unwrap();
    assert_eq!(report.size, 1024 * 1024 + 123);
    assert_eq!(report.sha256, sha256_file(&local));

    let downloaded = harness.root.join("downloaded.bin");
    let report = call::download(&cfg, "nas", &remote_text, &downloaded, false, &no_cancel())
        .await
        .unwrap();
    assert_eq!(report.size, 1024 * 1024 + 123);
    assert_eq!(report.sha256, sha256_file(&local));
    assert_eq!(sha256_file(&downloaded), sha256_file(&local));
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "64 MiB 传输在 debug 构建下约需 2 分钟，验收时用 --ignored 运行"]
async fn upload_download_64_mib() {
    let harness = Harness::start("files", false, false, None).await;
    let cfg = harness.session().await;
    let local = harness.root.join("big-local.bin");
    let size = 64 * 1024 * 1024;
    write_pattern(&local, size);
    let remote = harness.remote("big-remote.bin");
    let expected = sha256_file(&local);

    let report = call::upload(&cfg, "nas", &local, &remote, false, false, &no_cancel())
        .await
        .unwrap();
    assert_eq!(report.size, size);
    assert_eq!(report.sha256, expected);

    let downloaded = harness.root.join("big-downloaded.bin");
    let report = call::download(&cfg, "nas", &remote, &downloaded, false, &no_cancel())
        .await
        .unwrap();
    assert_eq!(report.size, size);
    assert_eq!(report.sha256, expected);
    assert_eq!(sha256_file(&downloaded), expected);
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_overwrite_is_opt_in() {
    let harness = Harness::start("files", false, false, None).await;
    let cfg = harness.session().await;
    let local = harness.root.join("local.txt");
    std::fs::write(&local, "hello").unwrap();
    let remote = harness.remote("remote.txt");
    call::upload(&cfg, "nas", &local, &remote, false, false, &no_cancel())
        .await
        .unwrap();
    let error = call::upload(&cfg, "nas", &local, &remote, false, false, &no_cancel())
        .await
        .unwrap_err();
    assert_eq!(error_code(&error), "conflict");
    call::upload(&cfg, "nas", &local, &remote, true, false, &no_cancel())
        .await
        .unwrap();
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_does_not_overwrite_by_default() {
    let harness = Harness::start("files", false, false, None).await;
    let cfg = harness.session().await;
    let remote = harness.remote("remote.txt");
    call::write(&cfg, "nas", &remote, "remote", false, &no_cancel())
        .await
        .unwrap();
    let local = harness.root.join("existing.txt");
    std::fs::write(&local, "keep").unwrap();
    let error = call::download(&cfg, "nas", &remote, &local, false, &no_cancel())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("已存在"), "错误: {error:#}");
    assert_eq!(std::fs::read_to_string(&local).unwrap(), "keep");
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_rejects_oversized_content() {
    let harness = Harness::start("files", false, false, None).await;
    let cfg = harness.session().await;
    let content = "x".repeat(dsh_remote::exec::files::MAX_WRITE_BYTES + 1);
    let error = call::write(
        &cfg,
        "nas",
        &harness.remote("big.txt"),
        &content,
        false,
        &no_cancel(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("upload"), "错误: {error:#}");
    harness.stop().await;
}

async fn raw_server(harness: &Harness) -> SecureConn {
    let mut conn = SecureConn::connect(&harness.addr, &key()).await.unwrap();
    conn.send(&hello(Role::Server, "raw")).await.unwrap();
    let welcome = timeout(Duration::from_secs(3), conn.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(welcome, Msg::Welcome { .. }));
    conn
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_checksum_mismatch_is_rejected() {
    let harness = Harness::start("files", false, false, None).await;
    let mut conn = raw_server(&harness).await;
    let remote = harness.remote("bad.bin");
    conn.send(&Msg::Call {
        id: 1,
        target: "nas".to_string(),
        method: "upload".to_string(),
        params: value::map(vec![
            ("remote_path", ciborium::value::Value::Text(remote.clone())),
            ("size", ciborium::value::Value::Integer(5u64.into())),
            ("overwrite", ciborium::value::Value::Bool(false)),
        ]),
    })
    .await
    .unwrap();
    conn.send(&Msg::Chunk {
        id: 1,
        seq: 0,
        stream: dsh_remote::proto::STREAM_STDOUT,
        data: b"hello".to_vec(),
    })
    .await
    .unwrap();
    conn.send(&Msg::End {
        id: 1,
        status: EndStatus::Ok,
        exit_code: None,
        size: Some(5),
        sha256: Some("0".repeat(64)),
        truncated: Some(false),
    })
    .await
    .unwrap();

    loop {
        let msg = timeout(Duration::from_secs(5), conn.recv())
            .await
            .unwrap()
            .unwrap();
        match msg {
            Msg::Ping { nonce } => conn.send(&Msg::Pong { nonce }).await.unwrap(),
            Msg::Result {
                ok, error, id: 1, ..
            } => {
                assert!(!ok);
                assert_eq!(error.unwrap().code, "protocol");
                break;
            }
            _ => {}
        }
    }

    assert!(!Path::new(&remote).exists());
    let leftovers: Vec<String> = std::fs::read_dir(&harness.root)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .filter(|name| name.contains(".part"))
        .collect();
    assert!(leftovers.is_empty(), "残留临时文件: {leftovers:?}");
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_consumer_does_not_stall_other_calls() {
    let harness = Harness::start("files", false, false, None).await;
    let session = harness.session().await;
    let local = harness.root.join("parallel.bin");
    let size = 16 * 1024 * 1024;
    write_pattern(&local, size);
    let remote = harness.remote("sub/parallel.bin");
    call::upload(&session, "nas", &local, &remote, false, true, &no_cancel())
        .await
        .unwrap();

    let mut slow = session
        .begin(
            "nas",
            "download",
            value::map(vec![(
                "remote_path",
                ciborium::value::Value::Text(remote.clone()),
            )]),
        )
        .await
        .unwrap();
    let slow_task = tokio::spawn(async move {
        let mut seen = 0usize;
        while let Some(msg) = slow.recv().await {
            seen += 1;
            if matches!(msg, Msg::End { .. }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        seen
    });

    let base_started = Instant::now();
    let baseline_path = harness.root.join("parallel-baseline.bin");
    call::download(
        &session,
        "nas",
        &remote,
        &baseline_path,
        false,
        &no_cancel(),
    )
    .await
    .unwrap();
    let baseline = base_started.elapsed();

    let fast_local = harness.root.join("parallel-fast.bin");
    let started = Instant::now();
    let report = call::download(&session, "nas", &remote, &fast_local, false, &no_cancel())
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(report.size, size);
    assert_eq!(sha256_file(&fast_local), sha256_file(&local));
    assert!(
        elapsed < baseline * 2 + Duration::from_secs(2),
        "慢消费者拖住了其它调用：基线 {baseline:?}，实际 {elapsed:?}"
    );
    let seen = slow_task.await.unwrap();
    assert!(seen > 0);
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn call_ends_when_hub_disappears() {
    let mut harness = Harness::start("files", false, false, None).await;
    let session = harness.session().await;
    let local = harness.root.join("drop.bin");
    write_pattern(&local, 2 * 1024 * 1024);
    let remote = harness.remote("sub/drop.bin");
    call::upload(&session, "nas", &local, &remote, false, true, &no_cancel())
        .await
        .unwrap();

    let mut handle = session
        .begin(
            "nas",
            "download",
            value::map(vec![(
                "remote_path",
                ciborium::value::Value::Text(remote.clone()),
            )]),
        )
        .await
        .unwrap();
    assert!(handle.recv().await.is_some());
    harness.stop_hub().await;
    let outcome = timeout(Duration::from_secs(10), handle.recv()).await;
    assert!(
        matches!(outcome, Ok(None)),
        "中继断开后调用应当立刻结束，实际 {outcome:?}"
    );
    harness.stop().await;
}
