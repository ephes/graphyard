//! Capture of `log` records per test thread (each `#[tokio::test]` runs its
//! current-thread runtime on its own thread, where the HTTP layer logs).

use std::cell::RefCell;
use std::sync::Once;

struct ThreadLogger;

thread_local! {
    static LINES: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

impl log::Log for ThreadLogger {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.level() <= log::Level::Info
    }

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            let line = format!("{}", record.args());
            LINES.with(|lines| lines.borrow_mut().push(line));
        }
    }

    fn flush(&self) {}
}

static LOGGER: ThreadLogger = ThreadLogger;
static INIT: Once = Once::new();

/// Start capturing on this thread; earlier lines are dropped.
pub(super) fn capture() {
    INIT.call_once(|| {
        log::set_logger(&LOGGER).expect("no other logger in the test binary");
        log::set_max_level(log::LevelFilter::Info);
    });
    LINES.with(|lines| lines.borrow_mut().clear());
}

/// The lines logged on this thread since [`capture`].
pub(super) fn lines() -> Vec<String> {
    LINES.with(|lines| lines.borrow().clone())
}
