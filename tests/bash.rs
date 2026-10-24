mod common;

use std::time::Duration;

use ciborium::value::Value;
use common::*;
use dsh_remote::call;
use dsh_remote::proto::value;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bash_on_agent() {
    let harness = Harness::start("bash", true, true, Some(vec!["/tmp".to_string()])).await;
    let result = remote_call(
        &harness,
        "nas",
        "bash",
        bash_params("echo hello"),
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    let report = bash_report(&result);
    assert_eq!(report.exit_code, Some(0));
    assert!(!report.timed_out);
    assert_eq!(report.output, "hello\n[命令结束，退出码 0]");
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bash_on_hub() {
    let harness = Harness::start("bash", true, true, Some(vec!["/tmp".to_string()])).await;
    let result = remote_call(
        &harness,
        "hub",
        "bash",
        bash_params("pwd"),
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    let report = bash_report(&result);
    assert!(report.output.contains("/tmp"), "{}", report.output);
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shell_keeps_state_between_calls() {
    let harness = Harness::start("bash", true, true, Some(vec!["/tmp".to_string()])).await;
    let dir = std::env::temp_dir().join(format!(
        "dsh-remote-state-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let first = remote_call(
        &harness,
        "nas",
        "bash",
        bash_params(&format!(
            "cd {} && export DSH_REMOTE_STATE=42",
            dir.display()
        )),
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(bash_report(&first).exit_code, Some(0));

    let second = remote_call(
        &harness,
        "nas",
        "bash",
        bash_params("pwd; echo $DSH_REMOTE_STATE"),
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    let report = bash_report(&second);
    assert!(
        report.output.contains(&dir.display().to_string()),
        "{}",
        report.output
    );
    assert!(report.output.contains("42"), "{}", report.output);
    std::fs::remove_dir_all(dir).ok();
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
async fn timeout_kills_the_shell_and_returns_partial_output() {
    let harness = Harness::start_tuned(
        "bash",
        true,
        true,
        Some(vec!["/tmp".to_string()]),
        MAX_OUTPUT_CHARS,
        Duration::from_millis(500),
    )
    .await;
    let started = std::time::Instant::now();
    let result = remote_call(
        &harness,
        "nas",
        "bash",
        bash_params("echo before; sleep 30"),
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    let report = bash_report(&result);
    assert!(report.timed_out, "{}", report.output);
    assert!(report.output.contains("before"), "{}", report.output);
    assert!(report.output.contains("[命令超时]"), "{}", report.output);
    assert!(
        report.output.contains("持久 Shell 已重置"),
        "{}",
        report.output
    );
    assert!(started.elapsed() < Duration::from_secs(5));

    let again = remote_call(
        &harness,
        "nas",
        "bash",
        bash_params("echo recovered"),
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    let report = bash_report(&again);
    assert!(report.output.contains("recovered"), "{}", report.output);
    assert_eq!(report.exit_code, Some(0));
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn output_is_truncated_to_the_configured_limit() {
    let harness = Harness::start_tuned(
        "bash",
        true,
        true,
        Some(vec!["/tmp".to_string()]),
        32,
        SHELL_TIMEOUT,
    )
    .await;
    let result = remote_call(
        &harness,
        "nas",
        "bash",
        bash_params("printf 'x%.0s' $(seq 1 200)"),
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    let report = bash_report(&result);
    assert!(report.output.contains("<输出已截断>"), "{}", report.output);
    assert_eq!(report.output.matches('x').count(), 32, "{}", report.output);
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shell_exit_is_reported_and_reset() {
    let harness = Harness::start("bash", true, true, Some(vec!["/tmp".to_string()])).await;
    let result = remote_call(
        &harness,
        "nas",
        "bash",
        bash_params("echo bye; exit 4"),
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    let report = bash_report(&result);
    assert!(report.output.contains("bye"), "{}", report.output);
    assert!(report.output.contains("Shell 已退出"), "{}", report.output);

    let again = remote_call(
        &harness,
        "nas",
        "bash",
        bash_params("echo back"),
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    let report = bash_report(&again);
    assert_eq!(report.exit_code, Some(0));
    assert!(report.output.contains("back"), "{}", report.output);
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bash_disabled_is_denied() {
    let harness = Harness::start("bash", true, false, Some(vec!["/tmp".to_string()])).await;
    let result = remote_call(
        &harness,
        "nas",
        "bash",
        bash_params("echo hello"),
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert!(!result.ok);
    let error = result.error.unwrap();
    assert_eq!(error.code, "denied");
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nonzero_exit_code_is_reported() {
    let harness = Harness::start("bash", true, true, Some(vec!["/tmp".to_string()])).await;
    let result = remote_call(
        &harness,
        "nas",
        "bash",
        bash_params("sh -c 'exit 3'"),
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    let report = bash_report(&result);
    assert_eq!(report.exit_code, Some(3));
    assert_eq!(report.output, "[命令结束，退出码 3]");
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stderr_is_merged_into_the_output() {
    let harness = Harness::start("bash", true, true, Some(vec!["/tmp".to_string()])).await;
    let result = remote_call(
        &harness,
        "nas",
        "bash",
        bash_params("echo out; echo err 1>&2"),
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    let report = bash_report(&result);
    assert!(report.output.contains("out"), "{}", report.output);
    assert!(report.output.contains("err"), "{}", report.output);
    harness.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_target_is_reported() {
    let harness = Harness::start("bash", true, true, Some(vec!["/tmp".to_string()])).await;
    let result = remote_call(
        &harness,
        "ghost",
        "bash",
        bash_params("echo hello"),
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert!(!result.ok);
    assert_eq!(result.error.unwrap().code, "unknown_target");
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
        Duration::from_secs(10),
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
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert!(!result.ok);
    let err = result.error.unwrap();
    assert_eq!(err.code, "unsupported");
    harness.stop().await;
}
