pub const TRUNCATED: &str =
    "<输出已截断> 为节省上下文只返回了一部分输出，建议用 grep -n / head / tail 缩小范围再重试";
pub const STDERR_LABEL: &str = "[标准错误]";
pub const NO_OUTPUT: &str = "（这条命令没有任何输出）";

pub fn exit_code(code: i32) -> String {
    format!("[命令结束，退出码 {}]", code)
}

pub fn timed_out(secs: u64) -> String {
    format!("[命令超过 {} 秒仍未结束，已被终止]", secs)
}

pub fn killed(signal: i32) -> String {
    format!("[Shell 被信号终止：{}]", signal_name(signal))
}

pub fn uploaded(node: &str, path: &str, bytes: u64, sha256: &str) -> String {
    format!(
        "已上传至 {}:{}（{}，SHA-256: {}）",
        node,
        path,
        human_size(bytes),
        short_sha(sha256)
    )
}

pub fn downloaded(local: &str, node: &str, path: &str, bytes: u64, sha256: &str) -> String {
    format!(
        "已下载至 {}（来自 {}:{}，{}，SHA-256: {}）",
        local,
        node,
        path,
        human_size(bytes),
        short_sha(sha256)
    )
}

pub fn transferred(src: (&str, &str), dst: (&str, &str), bytes: u64, sha256: &str) -> String {
    format!(
        "已传输 {}:{} → {}:{}（{}，SHA-256: {}）",
        src.0,
        src.1,
        dst.0,
        dst.1,
        human_size(bytes),
        short_sha(sha256)
    )
}

pub fn node_line(node_id: &str, tags: &[String], via: &str, online_secs: u64) -> String {
    if tags.is_empty() {
        format!(
            "- {}（{}，已在线 {}）",
            node_id,
            via,
            human_duration(online_secs)
        )
    } else {
        format!(
            "- {}（{}，标签：{}，已在线 {}）",
            node_id,
            via,
            tags.join(","),
            human_duration(online_secs)
        )
    }
}

pub fn nodes_empty() -> String {
    "当前没有任何在线节点".to_string()
}

pub fn nodes_header(count: usize) -> String {
    format!("共有 {} 个在线节点：", count)
}

pub fn short_sha(sha256: &str) -> String {
    sha256.chars().take(12).collect()
}

pub fn human_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} B", bytes)
    } else {
        format!("{:.1} {}", value, UNITS[unit])
    }
}

pub fn human_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{} 秒", secs)
    } else if secs < 3600 {
        format!("{} 分钟", secs / 60)
    } else if secs < 86400 {
        format!("{} 小时", secs / 3600)
    } else {
        format!("{} 天", secs / 86400)
    }
}

pub fn signal_name(signal: i32) -> String {
    let name = match signal {
        libc::SIGHUP => "SIGHUP",
        libc::SIGINT => "SIGINT",
        libc::SIGQUIT => "SIGQUIT",
        libc::SIGILL => "SIGILL",
        libc::SIGTRAP => "SIGTRAP",
        libc::SIGABRT => "SIGABRT",
        libc::SIGBUS => "SIGBUS",
        libc::SIGFPE => "SIGFPE",
        libc::SIGKILL => "SIGKILL",
        libc::SIGSEGV => "SIGSEGV",
        libc::SIGPIPE => "SIGPIPE",
        libc::SIGALRM => "SIGALRM",
        libc::SIGTERM => "SIGTERM",
        _ => return format!("SIG{}", signal),
    };
    name.to_string()
}

#[derive(Default)]
pub struct ChunkDecoder {
    carry: Vec<u8>,
}

impl ChunkDecoder {
    pub fn push(&mut self, bytes: &[u8]) -> String {
        if self.carry.is_empty() {
            return split_valid(bytes, &mut self.carry);
        }
        let mut merged = std::mem::take(&mut self.carry);
        merged.extend_from_slice(bytes);
        split_valid(&merged, &mut self.carry)
    }

    pub fn finish(&mut self) -> String {
        let rest = std::mem::take(&mut self.carry);
        String::from_utf8_lossy(&rest).into_owned()
    }
}

fn split_valid(bytes: &[u8], carry: &mut Vec<u8>) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) => text.to_string(),
        Err(error) => {
            let valid = error.valid_up_to();
            let tail = &bytes[valid..];
            let complete = match error.error_len() {
                Some(len) => {
                    let mut out = String::from_utf8_lossy(&bytes[..valid + len]).into_owned();
                    out.push_str(&String::from_utf8_lossy(&tail[len..]));
                    return out;
                }
                None => valid,
            };
            carry.extend_from_slice(&bytes[complete..]);
            String::from_utf8_lossy(&bytes[..complete]).into_owned()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_split_utf8() {
        let text = "中文输出";
        let bytes = text.as_bytes();
        let mut decoder = ChunkDecoder::default();
        let mut out = String::new();
        for chunk in bytes.chunks(2) {
            out.push_str(&decoder.push(chunk));
        }
        out.push_str(&decoder.finish());
        assert_eq!(out, text);
    }

    #[test]
    fn formats_sizes() {
        assert_eq!(human_size(0), "0 B");
        assert_eq!(human_size(1536), "1.5 KiB");
    }
}
