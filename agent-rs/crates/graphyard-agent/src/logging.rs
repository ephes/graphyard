//! The stderr logger (PLAN.md 3.6, 4.3): production `LOGGING` is a root
//! `StreamHandler` without a formatter at level INFO, so every record is the
//! bare message on one stderr line. The graphyard crates log at INFO and
//! above; other crates (reqwest, rustls, hyper) only warnings and errors,
//! which keeps their debug chatter out of the journal.

use std::io::Write as _;

use log::{Level, LevelFilter, Log, Metadata, Record};

struct StderrLogger;

fn level_for(target: &str) -> Level {
    if target.starts_with("graphyard") {
        Level::Info
    } else {
        Level::Warn
    }
}

impl Log for StderrLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= level_for(metadata.target())
    }

    fn log(&self, record: &Record<'_>) {
        if self.enabled(record.metadata()) {
            // One write per record, so lines of concurrent writers never
            // interleave; a closed stderr is ignored.
            let line = format!("{}\n", record.args());
            let _ = std::io::stderr().lock().write_all(line.as_bytes());
        }
    }

    fn flush(&self) {
        let _ = std::io::stderr().flush();
    }
}

static LOGGER: StderrLogger = StderrLogger;

/// Install the logger (once per process; later calls are ignored).
pub fn install() {
    if log::set_logger(&LOGGER).is_ok() {
        log::set_max_level(LevelFilter::Info);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graphyard_targets_log_info_others_only_warnings() {
        let info = |target| {
            StderrLogger.enabled(
                &Metadata::builder()
                    .level(Level::Info)
                    .target(target)
                    .build(),
            )
        };
        let warn = |target| {
            StderrLogger.enabled(
                &Metadata::builder()
                    .level(Level::Warn)
                    .target(target)
                    .build(),
            )
        };
        assert!(info("graphyard_collect::http::client"));
        assert!(info("graphyard_agent"));
        assert!(!info("reqwest::connect"));
        assert!(warn("rustls::conn"));
        assert!(
            !StderrLogger.enabled(
                &Metadata::builder()
                    .level(Level::Debug)
                    .target("graphyard_store")
                    .build()
            )
        );
    }
}
