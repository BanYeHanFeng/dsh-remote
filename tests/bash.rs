mod common;

use std::time::Duration;

use ciborium::value::Value;
use common::*;
use dsh_remote::call;
use dsh_remote::proto::value;
use dsh_remote::proto::EndStatus;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bash_on_agent() {
    let harness = Harness::start("bash", true, true, Some(vec!["/tmp".to_string()])).await;
    let result = remote_call(
        &harness,
        "nas",
        "bash",
        bash_params("echo hello", 5_000),
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
    let harness = Harness::start("bash", true, true, Some(vec!["/tmp".to_string()])).await;
    let result = remote_call(
        &harness,
        "hub",
        "bash",
        bash_params("pwd", 5_000),
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
    let harness = Harness::start("bash", true, true, Some(vec!["/tmp".to_string()])).await;
    let targets = call::targets(&harness.session().await).await.unwrap();
    assert!(targets.iter().any(|target| target.name == "hub"));
    assert!(targets.iter().any(|target| target.name == "nas"));
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bash_timeout_returns_error_end() {
    let harness = Harness::start("bash", true, true, Some(vec!["/tmp".to_string()])).await;
    let params = value::map(vec![
        ("command", Value::Text("sleep 3".to_string())),
        ("timeout_ms", Value::Integer(200u64.into())),
    ]);
    let result = remote_call(&harness, "nas", "bash", params, Duration::from_secs(10))
        .await
        .unwrap();
    assert!(!result.ok);
    assert_eq!(result.status, Some(EndStatus::Error));
    assert_eq!(result.exit_code, None);
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bash_disabled_is_denied() {
    let harness = Harness::start("bash", true, false, Some(vec!["/tmp".to_string()])).await;
    let result = remote_call(
        &harness,
        "nas",
        "bash",
        bash_params("echo hello", 5_000),
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
    let harness = Harness::start("bash", true, true, Some(vec!["/tmp".to_string()])).await;
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
    let harness = Harness::start("bash", true, true, Some(vec!["/tmp".to_string()])).await;
    let result = remote_call(
        &harness,
        "nas",
        "bash",
        bash_params("exit 3", 5_000),
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
    let harness = Harness::start("bash", true, true, Some(vec!["/tmp".to_string()])).await;
    let result = remote_call(
        &harness,
        "ghost",
        "bash",
        bash_params("echo hello", 5_000),
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
    let harness = Harness::start("bash", true, true, Some(vec!["/tmp".to_string()])).await;
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
    let mut harness = Harness::start("bash", true, true, Some(vec!["/tmp".to_string()])).await;
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
    let stopping: ciborium::value::Value = value::deserialize(&value).unwrap();
    assert_eq!(stopping, value::map(vec![("stopping", Value::Bool(true))]));
    let handle = harness.agent.take().unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("被控端没有在 5 秒内退出")
        .expect("被控端任务 panic")
        .expect("被控端返回错误");
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_on_hub_is_unsupported() {
    let harness = Harness::start("bash", true, true, Some(vec!["/tmp".to_string()])).await;
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
