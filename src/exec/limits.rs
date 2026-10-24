use std::time::Duration;

use ciborium::value::Value;

use crate::exec::transfer::transfer_limit;
use crate::proto::params::{BashParams, DownloadParams, TransferParams, UploadParams};
use crate::proto::value;

pub const BASH_DEFAULT_TIMEOUT_MS: u64 = 300_000;
pub const BASH_MAX_TIMEOUT_MS: u64 = 600_000;
const BASH_MIN_TIMEOUT_MS: u64 = 100;
const BASH_MARGIN: Duration = Duration::from_secs(30);
const TRANSFER_MARGIN: Duration = Duration::from_secs(10);
const FS_TIMEOUT: Duration = Duration::from_secs(30);
const CALL_TIMEOUT_FLOOR: Duration = Duration::from_secs(60);

pub fn bash_timeout_ms(requested: Option<u64>) -> Duration {
    let requested = requested
        .unwrap_or(BASH_DEFAULT_TIMEOUT_MS)
        .clamp(BASH_MIN_TIMEOUT_MS, BASH_MAX_TIMEOUT_MS);
    Duration::from_millis(requested)
}

pub fn deadline(method: &str, params: &Value) -> Duration {
    match method {
        "bash" => {
            let requested = value::deserialize::<BashParams>(params)
                .ok()
                .and_then(|params| params.timeout_ms);
            bash_timeout_ms(requested) + BASH_MARGIN
        }
        "upload" => {
            let size = value::deserialize::<UploadParams>(params)
                .ok()
                .and_then(|params| params.size)
                .unwrap_or_default();
            transfer_limit(size) + TRANSFER_MARGIN
        }
        "download" => {
            let size = value::deserialize::<DownloadParams>(params)
                .ok()
                .and_then(|params| params.max_size)
                .unwrap_or_default();
            transfer_limit(size) + TRANSFER_MARGIN
        }
        "fs" => FS_TIMEOUT,
        "transfer" => {
            let size = value::deserialize::<TransferParams>(params)
                .ok()
                .and_then(|params| params.max_size)
                .unwrap_or_default();
            transfer_limit(size) + TRANSFER_MARGIN
        }
        _ => CALL_TIMEOUT_FLOOR,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bash_deadline_follows_request() {
        let params = value::serialize(&BashParams {
            command: "true".into(),
            timeout_ms: Some(1_000),
        })
        .unwrap();
        assert_eq!(deadline("bash", &params), Duration::from_secs(31));
    }

    #[test]
    fn bash_default_timeout_matches_shell_side() {
        assert_eq!(
            bash_timeout_ms(None),
            Duration::from_millis(BASH_DEFAULT_TIMEOUT_MS)
        );
    }

    #[test]
    fn fs_deadline_is_fixed() {
        assert_eq!(deadline("fs", &value::empty_map()), Duration::from_secs(30));
    }

    #[test]
    fn unknown_method_gets_floor() {
        assert_eq!(deadline("targets", &value::empty_map()), CALL_TIMEOUT_FLOOR);
    }

    #[test]
    fn timeout_is_clamped() {
        assert_eq!(bash_timeout_ms(Some(0)), Duration::from_millis(100));
        assert_eq!(bash_timeout_ms(Some(u64::MAX)), Duration::from_secs(600));
    }
}
