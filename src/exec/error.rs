use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolError {
    pub code: &'static str,
    pub message: String,
}

pub type ToolResult<T> = Result<T, ToolError>;

impl ToolError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn denied(message: impl Into<String>) -> Self {
        Self::new("denied", message)
    }

    pub fn io(message: impl Into<String>) -> Self {
        Self::new("io", message)
    }

    pub fn too_large(message: impl Into<String>) -> Self {
        Self::new("too_large", message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new("not_found", message)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new("conflict", message)
    }

    pub fn protocol(message: impl Into<String>) -> Self {
        Self::new("protocol", message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new("internal", message)
    }
}

impl fmt::Display for ToolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ToolError {}

impl From<std::io::Error> for ToolError {
    fn from(err: std::io::Error) -> Self {
        ToolError::io(format!("文件操作失败: {err}"))
    }
}
