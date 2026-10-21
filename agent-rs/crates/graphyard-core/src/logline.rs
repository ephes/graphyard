//! Log records produced by the pure code, for the binary's logger.
//!
//! The pure crates return the operator log lines of PLAN.md 3.6 as values
//! instead of logging, so fixtures can pin their text and order; the I/O
//! crates hand them to the `log` facade. Values interpolated into a line are
//! passed through [`escape_log_value`] (D-10).

use std::fmt;

/// The level of a record (Python's `logging` level names).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogLevel {
    Info,
    Warning,
    Error,
}

impl LogLevel {
    /// Python's `levelname`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Info => "INFO",
            Self::Warning => "WARNING",
            Self::Error => "ERROR",
        }
    }
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One log record: level and the bare message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEvent {
    pub level: LogLevel,
    pub message: String,
}

impl LogEvent {
    pub fn warning(message: impl Into<String>) -> Self {
        Self {
            level: LogLevel::Warning,
            message: message.into(),
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            level: LogLevel::Error,
            message: message.into(),
        }
    }
}

/// D-10: `\n` and `\r` in a value interpolated into a log line become the
/// two-character escapes `\n` and `\r`, so a value cannot forge a line.
pub fn escape_log_value(value: &str) -> String {
    value.replace('\n', "\\n").replace('\r', "\\r")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_log_value_escapes_line_breaks_only() {
        assert_eq!(escape_log_value("a\nb\rc\td\\n"), "a\\nb\\rc\td\\n");
        assert_eq!(escape_log_value("plain"), "plain");
    }

    #[test]
    fn levels_use_python_names() {
        assert_eq!(LogEvent::warning("w").level.as_str(), "WARNING");
        assert_eq!(LogEvent::error("e").level.to_string(), "ERROR");
        assert_eq!(LogLevel::Info.as_str(), "INFO");
    }
}
