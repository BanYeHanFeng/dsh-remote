use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct BashParams {
    pub command: String,
    pub cwd: Option<String>,
    pub stdin: Option<String>,
    pub timeout_ms: Option<u64>,
    pub max_output: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ReadParams {
    pub path: String,
    pub offset: u64,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct WriteParams {
    pub path: String,
    pub content: String,
    pub create_parents: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct EditParams {
    pub path: String,
    pub old_string: String,
    pub new_string: String,
    pub replace_all: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct UploadParams {
    pub remote_path: String,
    pub size: Option<u64>,
    pub overwrite: bool,
    pub create_parents: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct DownloadParams {
    pub remote_path: String,
    pub max_size: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_fields_fall_back() {
        let params: BashParams =
            crate::proto::value::deserialize(&crate::proto::value::empty_map()).unwrap();
        assert!(params.command.is_empty());
        assert_eq!(params.timeout_ms, None);
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let raw = crate::proto::value::map(vec![
            ("path", ciborium::value::Value::Text("/tmp/a".into())),
            ("future", ciborium::value::Value::Bool(true)),
        ]);
        let params: ReadParams = crate::proto::value::deserialize(&raw).unwrap();
        assert_eq!(params.path, "/tmp/a");
        assert_eq!(params.offset, 0);
    }
}
