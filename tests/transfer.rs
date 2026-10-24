mod common;

use common::*;
use dsh_remote::call;
use dsh_remote::proto::params::TransferParams;

fn transfer_params(source: &str, source_path: &str, dest: &str, dest_path: &str) -> TransferParams {
    TransferParams {
        source: source.to_string(),
        source_path: source_path.to_string(),
        dest: dest.to_string(),
        dest_path: dest_path.to_string(),
        overwrite: false,
        create_parents: false,
        max_size: Some(67_108_864),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfers_between_hub_and_agent() {
    let harness = Harness::start("transfer", true, true, None).await;
    let session = harness.session().await;

    let payload: Vec<u8> = (0..900_000u32).map(|index| (index % 251) as u8).collect();
    let source = harness.local("source.bin");
    std::fs::write(&source, &payload).unwrap();
    let source = source.to_string_lossy().to_string();
    let dest = harness.remote("copied.bin");

    let report = call::transfer(
        &session,
        transfer_params("hub", &source, "nas", &dest),
        &no_cancel(),
    )
    .await
    .unwrap();
    assert_eq!(report.size, payload.len() as u64);
    assert_eq!(std::fs::read(&dest).unwrap(), payload);
    assert_eq!(report.sha256, sha256_file(std::path::Path::new(&dest)));

    let back = harness.local("back.bin");
    let back_text = back.to_string_lossy().to_string();
    let report = call::transfer(
        &session,
        transfer_params("nas", &dest, "hub", &back_text),
        &no_cancel(),
    )
    .await
    .unwrap();
    assert_eq!(report.size, payload.len() as u64);
    assert_eq!(std::fs::read(&back).unwrap(), payload);
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfers_within_one_agent() {
    let harness = Harness::start("transfer", true, true, None).await;
    let session = harness.session().await;
    let source = harness.remote("a.txt");
    std::fs::write(&source, b"hello transfer").unwrap();
    let dest = harness.remote("b.txt");
    let report = call::transfer(
        &session,
        transfer_params("nas", &source, "nas", &dest),
        &no_cancel(),
    )
    .await
    .unwrap();
    assert_eq!(report.size, 14);
    assert_eq!(std::fs::read(&dest).unwrap(), b"hello transfer");
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfer_reports_missing_source() {
    let harness = Harness::start("transfer", true, true, None).await;
    let session = harness.session().await;
    let error = call::transfer(
        &session,
        transfer_params(
            "nas",
            &harness.remote("ghost.bin"),
            "hub",
            &harness.remote("x.bin"),
        ),
        &no_cancel(),
    )
    .await
    .unwrap_err();
    assert_eq!(error_code(&error), "not_found", "{error}");
    assert!(!harness.local("x.bin").exists(), "失败时不该留下目标文件");
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfer_refuses_to_overwrite_by_default() {
    let harness = Harness::start("transfer", true, true, None).await;
    let session = harness.session().await;
    let source = harness.remote("source.txt");
    std::fs::write(&source, b"new").unwrap();
    let dest = harness.remote("existing.txt");
    std::fs::write(&dest, b"keep").unwrap();

    let error = call::transfer(
        &session,
        transfer_params("nas", &source, "hub", &dest),
        &no_cancel(),
    )
    .await
    .unwrap_err();
    assert_eq!(error_code(&error), "conflict", "{error}");
    assert_eq!(std::fs::read(&dest).unwrap(), b"keep");

    let mut params = transfer_params("nas", &source, "hub", &dest);
    params.overwrite = true;
    call::transfer(&session, params, &no_cancel())
        .await
        .unwrap();
    assert_eq!(std::fs::read(&dest).unwrap(), b"new");
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfer_reports_unknown_target() {
    let harness = Harness::start("transfer", true, true, None).await;
    let session = harness.session().await;
    let error = call::transfer(
        &session,
        transfer_params("ghost", "/tmp/a", "nas", &harness.remote("b")),
        &no_cancel(),
    )
    .await
    .unwrap_err();
    assert_eq!(error_code(&error), "unknown_target", "{error}");
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfer_creates_parent_directories() {
    let harness = Harness::start("transfer", true, true, None).await;
    let session = harness.session().await;
    let source = harness.remote("source.txt");
    std::fs::write(&source, b"nested").unwrap();
    let dest = harness.remote("deep/inside/file.txt");

    let error = call::transfer(
        &session,
        transfer_params("nas", &source, "hub", &dest),
        &no_cancel(),
    )
    .await
    .unwrap_err();
    assert_eq!(error_code(&error), "not_found", "{error}");

    let mut params = transfer_params("nas", &source, "hub", &dest);
    params.create_parents = true;
    call::transfer(&session, params, &no_cancel())
        .await
        .unwrap();
    assert_eq!(std::fs::read(&dest).unwrap(), b"nested");
    harness.stop().await;
}
