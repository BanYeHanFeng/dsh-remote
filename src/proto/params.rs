use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct BashParams {
    pub command: String,
    pub timeout_ms: Option<u64>,
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct FsParams {
    pub op: String,
    pub path: String,
    pub to_path: String,
    pub target: String,
    pub create_parents: bool,
    pub overwrite: bool,
    pub mode: Option<u32>,
    pub mtime_ms: Option<u64>,
    pub atime_ms: Option<u64>,
    pub offset: Option<u64>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct FsEntry {
    pub name: String,
    pub kind: String,
    pub size: u64,
    pub mtime_ms: u64,
    pub atime_ms: u64,
    pub ctime_ms: u64,
    pub perm: u32,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u32,
}

impl FsEntry {
    pub fn is_dir(&self) -> bool {
        self.kind == "dir"
    }

    pub fn is_symlink(&self) -> bool {
        self.kind == "symlink"
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct FsList {
    pub entries: Vec<FsEntry>,
    pub next: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct FsLink {
    pub target: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TransferParams {
    pub source: String,
    pub source_path: String,
    pub dest: String,
    pub dest_path: String,
    pub overwrite: bool,
    pub create_parents: bool,
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
        let params: UploadParams = crate::proto::value::deserialize(&raw).unwrap();
        assert_eq!(params.remote_path, "");
        assert!(!params.overwrite);
    }

    #[test]
    fn fs_params_only_need_op_and_path() {
        let raw = crate::proto::value::map(vec![
            ("op", ciborium::value::Value::Text("stat".into())),
            ("path", ciborium::value::Value::Text("/data/a".into())),
        ]);
        let params: FsParams = crate::proto::value::deserialize(&raw).unwrap();
        assert_eq!(params.op, "stat");
        assert_eq!(params.path, "/data/a");
        assert!(!params.create_parents);
        assert_eq!(params.limit, None);
    }
}
