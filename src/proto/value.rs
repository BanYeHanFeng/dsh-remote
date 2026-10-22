use anyhow::{anyhow, Result};
use ciborium::value::Value;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub fn empty_map() -> Value {
    Value::Map(Vec::new())
}

pub fn map(pairs: Vec<(&str, Value)>) -> Value {
    Value::Map(
        pairs
            .into_iter()
            .map(|(key, value)| (Value::Text(key.to_string()), value))
            .collect(),
    )
}

pub fn serialize<T: Serialize + ?Sized>(value: &T) -> Result<Value> {
    Value::serialized(value).map_err(|err| anyhow!("CBOR 序列化失败: {err}"))
}

pub fn deserialize<T: DeserializeOwned>(value: &Value) -> Result<T> {
    value
        .deserialized()
        .map_err(|err| anyhow!("CBOR 反序列化失败: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_targets() {
        let targets = vec![crate::proto::TargetInfo {
            name: "hub".into(),
            kind: crate::proto::TargetKind::Hub,
            os: "linux".into(),
            arch: "x86_64".into(),
            version: "0.1.0".into(),
            since: 1,
            roots: vec!["/".into()],
        }];
        let value = serialize(&targets).unwrap();
        let back: Vec<crate::proto::TargetInfo> = deserialize(&value).unwrap();
        assert_eq!(back, targets);
    }
}
