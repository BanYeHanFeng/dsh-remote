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

pub fn get<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    let Value::Map(entries) = value else {
        return None;
    };
    entries.iter().find_map(|(entry, item)| match entry {
        Value::Text(text) if text == key => Some(item),
        _ => None,
    })
}

pub fn text(value: &Value, key: &str) -> Option<String> {
    match get(value, key)? {
        Value::Text(text) => Some(text.clone()),
        _ => None,
    }
}

pub fn bool_value(value: &Value, key: &str) -> Option<bool> {
    match get(value, key)? {
        Value::Bool(flag) => Some(*flag),
        _ => None,
    }
}

pub fn u64_value(value: &Value, key: &str) -> Option<u64> {
    match get(value, key)? {
        Value::Integer(number) => u64::try_from(i128::from(*number)).ok(),
        _ => None,
    }
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
    fn reads_fields() {
        let params = map(vec![
            ("command", Value::Text("df -h".into())),
            ("timeout_ms", Value::Integer(1500u64.into())),
            ("flag", Value::Bool(true)),
        ]);
        assert_eq!(text(&params, "command").as_deref(), Some("df -h"));
        assert_eq!(u64_value(&params, "timeout_ms"), Some(1500));
        assert_eq!(bool_value(&params, "flag"), Some(true));
        assert!(text(&params, "missing").is_none());
    }

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
            caps: vec!["bash".into()],
        }];
        let value = serialize(&targets).unwrap();
        let back: Vec<crate::proto::TargetInfo> = deserialize(&value).unwrap();
        assert_eq!(back, targets);
    }
}
