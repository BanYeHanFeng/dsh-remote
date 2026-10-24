mod common;

use std::time::Duration;

use common::*;
use dsh_remote::call;
use dsh_remote::proto::params::FsParams;
use dsh_remote::proto::value;

#[tokio::test]
async fn lists_and_stats_remote_paths() {
    let harness = Harness::start("fs-list", true, true, None).await;
    std::fs::write(harness.local("b.txt"), b"hello").unwrap();
    std::fs::create_dir(harness.local("sub")).unwrap();
    let session = harness.session().await;
    let cancel = no_cancel();
    let root = harness.root.to_string_lossy().to_string();

    let listed = call::fs_list(&session, AGENT, &root, 0, &cancel)
        .await
        .unwrap();
    let names: Vec<_> = listed
        .entries
        .iter()
        .map(|entry| entry.name.clone())
        .collect();
    assert_eq!(names, vec!["b.txt", "sub"]);
    assert_eq!(listed.next, None);
    assert_eq!(listed.entries[0].size, 5);
    assert_eq!(listed.entries[1].kind, "dir");

    let file = format!("{root}/b.txt");
    let entry = call::fs_stat(&session, AGENT, &file, &cancel)
        .await
        .unwrap();
    assert_eq!(entry.kind, "file");
    assert_eq!(entry.size, 5);
    assert!(entry.mtime_ms > 0);

    let dir = call::fs_stat(&session, AGENT, &root, &cancel)
        .await
        .unwrap();
    assert_eq!(dir.kind, "dir");
    harness.stop().await;
}

#[tokio::test]
async fn list_pages_through_the_hub() {
    let harness = Harness::start("fs-page", true, true, None).await;
    for index in 0..5 {
        std::fs::write(harness.local(&format!("f{index}.txt")), b"x").unwrap();
    }
    let session = harness.session().await;
    let cancel = no_cancel();
    let root = harness.root.to_string_lossy().to_string();

    let first = call::fs_list(&session, AGENT, &root, 0, &cancel)
        .await
        .unwrap();
    assert_eq!(first.entries.len(), 5);

    let params = FsParams {
        op: "list".into(),
        path: root.clone(),
        offset: Some(0),
        limit: Some(2),
        ..FsParams::default()
    };
    let page = call::fs_call(&session, AGENT, params.clone(), &cancel)
        .await
        .unwrap()
        .into_value()
        .unwrap();
    let page: dsh_remote::proto::params::FsList = value::deserialize(&page).unwrap();
    assert_eq!(page.entries.len(), 2);
    assert_eq!(page.entries[0].name, "f0.txt");
    assert_eq!(page.next, Some(2));

    let params = FsParams {
        offset: page.next,
        ..params
    };
    let next = call::fs_call(&session, AGENT, params, &cancel)
        .await
        .unwrap()
        .into_value()
        .unwrap();
    let next: dsh_remote::proto::params::FsList = value::deserialize(&next).unwrap();
    assert_eq!(next.entries.len(), 2);
    assert_eq!(next.entries[0].name, "f2.txt");
    harness.stop().await;
}

#[tokio::test]
async fn mkdir_rename_remove_round_trip() {
    let harness = Harness::start("fs-mutate", true, true, None).await;
    let session = harness.session().await;
    let cancel = no_cancel();
    let root = harness.root.to_string_lossy().to_string();
    let dir = format!("{root}/sub");

    let created = call::fs_call(
        &session,
        AGENT,
        FsParams {
            op: "mkdir".into(),
            path: dir.clone(),
            ..FsParams::default()
        },
        &cancel,
    )
    .await
    .unwrap()
    .into_value()
    .unwrap();
    let created: dsh_remote::proto::params::FsEntry = value::deserialize(&created).unwrap();
    assert_eq!(created.kind, "dir");
    assert!(harness.local("sub").is_dir());

    std::fs::write(harness.local("sub/old.txt"), b"data").unwrap();
    let renamed = call::fs_call(
        &session,
        AGENT,
        FsParams {
            op: "rename".into(),
            path: format!("{dir}/old.txt"),
            to_path: format!("{dir}/new.txt"),
            overwrite: false,
            ..FsParams::default()
        },
        &cancel,
    )
    .await
    .unwrap()
    .into_value()
    .unwrap();
    let renamed: dsh_remote::proto::params::FsEntry = value::deserialize(&renamed).unwrap();
    assert_eq!(renamed.size, 4);
    assert!(harness.local("sub/new.txt").exists());
    assert!(!harness.local("sub/old.txt").exists());

    std::fs::write(harness.local("sub/other.txt"), b"keep").unwrap();
    let clash = call::fs_call(
        &session,
        AGENT,
        FsParams {
            op: "rename".into(),
            path: format!("{dir}/new.txt"),
            to_path: format!("{dir}/other.txt"),
            overwrite: false,
            ..FsParams::default()
        },
        &cancel,
    )
    .await
    .unwrap();
    let error = clash.into_value().unwrap_err();
    assert_eq!(error.code, "conflict");

    call::fs_call(
        &session,
        AGENT,
        FsParams {
            op: "remove".into(),
            path: format!("{dir}/new.txt"),
            ..FsParams::default()
        },
        &cancel,
    )
    .await
    .unwrap()
    .into_value()
    .unwrap();
    assert!(!harness.local("sub/new.txt").exists());

    let busy = call::fs_call(
        &session,
        AGENT,
        FsParams {
            op: "remove".into(),
            path: dir.clone(),
            ..FsParams::default()
        },
        &cancel,
    )
    .await
    .unwrap();
    assert_eq!(busy.into_value().unwrap_err().code, "not_empty");
    std::fs::remove_file(harness.local("sub/other.txt")).unwrap();

    call::fs_call(
        &session,
        AGENT,
        FsParams {
            op: "remove".into(),
            path: dir.clone(),
            ..FsParams::default()
        },
        &cancel,
    )
    .await
    .unwrap()
    .into_value()
    .unwrap();
    assert!(!harness.local("sub").exists());
    harness.stop().await;
}

#[tokio::test]
async fn setattr_updates_mode_and_mtime() {
    let harness = Harness::start("fs-setattr", true, true, None).await;
    std::fs::write(harness.local("a.txt"), b"hello").unwrap();
    let session = harness.session().await;
    let cancel = no_cancel();
    let path = format!("{}/a.txt", harness.root.to_string_lossy());

    let entry = call::fs_call(
        &session,
        AGENT,
        FsParams {
            op: "setattr".into(),
            path: path.clone(),
            mode: Some(0o640),
            mtime_ms: Some(1_700_000_000_000),
            ..FsParams::default()
        },
        &cancel,
    )
    .await
    .unwrap()
    .into_value()
    .unwrap();
    let entry: dsh_remote::proto::params::FsEntry = value::deserialize(&entry).unwrap();
    assert_eq!(entry.perm, 0o640);
    assert_eq!(entry.mtime_ms, 1_700_000_000_000);
    harness.stop().await;
}

#[tokio::test]
async fn denies_paths_outside_roots() {
    let harness = Harness::start("fs-deny", true, true, None).await;
    let session = harness.session().await;
    let cancel = no_cancel();

    let error = call::fs_stat(&session, AGENT, "/etc/passwd", &cancel)
        .await
        .unwrap_err();
    assert_eq!(error.code, "denied");

    let escape = format!("{}/../escape.txt", harness.root.to_string_lossy());
    let error = call::fs_stat(&session, AGENT, &escape, &cancel)
        .await
        .unwrap_err();
    assert_eq!(error.code, "denied");
    harness.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn reports_symlinks_without_following_them() {
    let harness = Harness::start("fs-link", true, true, None).await;
    std::os::unix::fs::symlink("target.txt", harness.local("link")).unwrap();
    let session = harness.session().await;
    let cancel = no_cancel();
    let link = format!("{}/link", harness.root.to_string_lossy());

    let entry = call::fs_stat(&session, AGENT, &link, &cancel)
        .await
        .unwrap();
    assert_eq!(entry.kind, "symlink");
    let target = call::fs_readlink(&session, AGENT, &link, &cancel)
        .await
        .unwrap();
    assert_eq!(target.target, "target.txt");
    harness.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn creates_symlinks_on_the_target() {
    let harness = Harness::start("fs-symlink", true, true, None).await;
    std::fs::write(harness.local("target.txt"), b"x").unwrap();
    let session = harness.session().await;
    let cancel = no_cancel();
    let link = format!("{}/link", harness.root.to_string_lossy());

    let entry = call::fs_call(
        &session,
        AGENT,
        FsParams {
            op: "symlink".into(),
            path: link.clone(),
            target: "target.txt".into(),
            ..FsParams::default()
        },
        &cancel,
    )
    .await
    .unwrap()
    .into_value()
    .unwrap();
    let entry: dsh_remote::proto::params::FsEntry = value::deserialize(&entry).unwrap();
    assert_eq!(entry.kind, "symlink");
    assert!(harness.local("link").is_symlink());
    harness.stop().await;
}

#[tokio::test]
async fn default_call_timeout_covers_fs_calls() {
    let harness = Harness::start("fs-timeout", true, true, None).await;
    let session = harness.session().await;
    let params = FsParams {
        op: "list".into(),
        path: harness.root.to_string_lossy().to_string(),
        ..FsParams::default()
    };
    let encoded = value::serialize(&params).unwrap();
    let deadline = dsh_remote::exec::limits::deadline("fs", &encoded);
    assert!(deadline >= Duration::from_secs(30));
    assert!(deadline <= Duration::from_secs(60));
    drop(session);
    harness.stop().await;
}
