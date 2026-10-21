//! The DB thread owning the agent's only connection (PLAN.md 4.1, 5.2).

use std::path::Path;
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::Duration;

use graphyard_core::settings::Settings;
use num_bigint::BigInt;
use num_traits::ToPrimitive;
use rusqlite::{Connection, OpenFlags};

use crate::error::StoreError;

type Job = Box<dyn FnOnce(&mut Connection) + Send>;

enum Message {
    Job(Job),
    Stop,
}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Message::Job(_) => "Job",
            Message::Stop => "Stop",
        })
    }
}

/// Cloneable handle to the DB thread. Every operation is a closure run on
/// that thread; the caller awaits its reply. An operation that was sent
/// runs to completion even if the awaiting future is dropped (a cancelled
/// caller never cuts a statement or transaction short; the result is then
/// discarded).
#[derive(Clone, Debug)]
pub struct Store {
    messages: mpsc::Sender<Message>,
    thread: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl Store {
    /// Open the existing database read-write (never create it, D-2), apply
    /// the connection setup of Django's SQLite backend and the pragmas of
    /// `apps.py:_configure_sqlite_connection` (PLAN.md 3.3, 5.2), and start
    /// the DB thread.
    ///
    /// # Errors
    /// The SQLite open error (a missing file is `unable to open database
    /// file`), or [`StoreError::Io`] when the thread cannot be spawned. A
    /// failing graphyard pragma is logged, not returned (as in Python).
    pub fn open(path: &Path, settings: &Settings) -> Result<Store, StoreError> {
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let mut connection = connect(path, flags, settings)?;
        apply_graphyard_pragmas(&connection, settings);
        let (messages, inbox) = mpsc::channel::<Message>();
        let thread = std::thread::Builder::new()
            .name("graphyard-db".to_owned())
            .spawn(move || {
                // A panicking job unwinds this thread: the connection is
                // dropped (rolling back an open transaction) and every
                // waiting and later caller gets `ThreadGone`.
                while let Ok(Message::Job(job)) = inbox.recv() {
                    job(&mut connection);
                }
            })
            .map_err(StoreError::Io)?;
        Ok(Store {
            messages,
            thread: Arc::new(Mutex::new(Some(thread))),
        })
    }

    /// Run `operation` on the DB thread and await its result.
    pub(crate) async fn call<T, F>(&self, operation: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, StoreError> + Send + 'static,
    {
        let (reply, response) = tokio::sync::oneshot::channel();
        let job: Job = Box::new(move |connection| {
            // The receiver is gone when the caller was cancelled; the
            // operation has completed either way.
            let _ = reply.send(operation(connection));
        });
        self.messages
            .send(Message::Job(job))
            .map_err(|_| StoreError::ThreadGone)?;
        response.await.map_err(|_| StoreError::ThreadGone)?
    }

    /// Stop the DB thread after the operation it is running (and the ones
    /// queued before this call) and close the connection. Blocks until the
    /// thread has finished, which a statement waiting on a lock bounds by
    /// `busy_timeout` (PLAN.md 4.1); call it outside the async runtime.
    /// Later operations on any clone fail with [`StoreError::ThreadGone`].
    pub fn close(self) {
        // The thread may already be gone; then there is nothing to stop.
        let _ = self.messages.send(Message::Stop);
        let handle = self
            .thread
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(handle) = handle {
            // A panic of the thread was already reported to its caller.
            let _ = handle.join();
        }
    }
}

/// Django's `DatabaseWrapper.get_new_connection`: `sqlite3.connect(timeout=
/// GRAPHYARD_SQLITE_TIMEOUT_SECONDS)`, then `PRAGMA foreign_keys = ON` and
/// `PRAGMA legacy_alter_table = OFF`.
fn connect(path: &Path, flags: OpenFlags, settings: &Settings) -> Result<Connection, StoreError> {
    let connection = Connection::open_with_flags(path, flags)?;
    connection.busy_timeout(connect_timeout(&settings.sqlite_timeout_seconds))?;
    connection.execute_batch("PRAGMA foreign_keys = ON; PRAGMA legacy_alter_table = OFF")?;
    Ok(connection)
}

/// `sqlite3.connect(timeout=seconds)`: a non-positive timeout disables the
/// busy handler; the value is capped at what SQLite's `int` milliseconds
/// hold. (The `busy_timeout` pragma replaces it right away.)
fn connect_timeout(seconds: &BigInt) -> Duration {
    let cap = i64::from(i32::MAX);
    let millis: BigInt = seconds * 1000;
    let millis = match millis.to_i64() {
        Some(ms) => ms.clamp(0, cap),
        None if millis.sign() == num_bigint::Sign::Minus => 0,
        None => cap,
    };
    Duration::from_millis(u64::try_from(millis).unwrap_or(0))
}

/// A read-only connection for `--check-schema`: Django's connection setup
/// and the `busy_timeout` pragma; no journal or synchronous change (they
/// would write).
pub(crate) fn open_read_only(path: &Path, settings: &Settings) -> Result<Connection, StoreError> {
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let connection = connect(path, flags, settings)?;
    let [busy_timeout, _, _] = settings.sqlite_pragmas().statements();
    run_pragma(&connection, &busy_timeout)?;
    Ok(connection)
}

fn run_pragma(connection: &Connection, sql: &str) -> rusqlite::Result<()> {
    let mut statement = connection.prepare(sql)?;
    let mut rows = statement.query([])?;
    while rows.next()?.is_some() {}
    Ok(())
}

/// `apps.py:_configure_sqlite_connection` (PLAN.md 3.3): the warnings for
/// invalid modes, then `busy_timeout`, `journal_mode`, `synchronous` in
/// that order; the first failing pragma is logged and the rest skipped.
fn apply_graphyard_pragmas(connection: &Connection, settings: &Settings) {
    let pragmas = settings.sqlite_pragmas();
    for warning in &pragmas.warnings {
        log::warn!("{warning}");
    }
    let applied = pragmas
        .statements()
        .iter()
        .try_for_each(|statement| run_pragma(connection, statement));
    if let Err(err) = applied {
        log::error!("Failed to apply SQLite pragmas on new DB connection: {err}");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn settings(vars: &[(&str, &str)]) -> Settings {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect();
        Settings::from_lookup(|name| vars.get(name).cloned()).unwrap()
    }

    fn scalar(connection: &Connection, sql: &str) -> String {
        connection
            .query_row(sql, [], |row| row.get::<_, rusqlite::types::Value>(0))
            .map(|value| match value {
                rusqlite::types::Value::Integer(i) => i.to_string(),
                rusqlite::types::Value::Text(t) => t,
                other => format!("{other:?}"),
            })
            .unwrap()
    }

    #[test]
    fn connect_timeout_is_clamped() {
        assert_eq!(connect_timeout(&30.into()), Duration::from_secs(30));
        assert_eq!(connect_timeout(&(-5).into()), Duration::ZERO);
        let cap = Duration::from_millis(u64::try_from(i32::MAX).unwrap());
        assert_eq!(connect_timeout(&i64::MAX.into()), cap);
        let huge: BigInt = "1".repeat(40).parse().unwrap();
        assert_eq!(connect_timeout(&huge), cap);
        assert_eq!(connect_timeout(&-huge), Duration::ZERO);
    }

    #[tokio::test]
    async fn open_applies_the_pragmas_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.sqlite3");
        Connection::open(&path).unwrap();
        let settings = settings(&[
            ("GRAPHYARD_SQLITE_TIMEOUT_SECONDS", "1"),
            ("GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS", "1234"),
            ("GRAPHYARD_SQLITE_JOURNAL_MODE", " wal "),
            ("GRAPHYARD_SQLITE_SYNCHRONOUS", "full"),
        ]);
        let store = Store::open(&path, &settings).unwrap();
        let values = store
            .call(|connection| {
                Ok([
                    scalar(connection, "PRAGMA busy_timeout"),
                    scalar(connection, "PRAGMA journal_mode"),
                    scalar(connection, "PRAGMA synchronous"),
                    scalar(connection, "PRAGMA foreign_keys"),
                    scalar(connection, "PRAGMA legacy_alter_table"),
                ])
            })
            .await
            .unwrap();
        assert_eq!(values, ["1234", "wal", "2", "1", "0"]);
        store.close();
    }

    #[tokio::test]
    async fn invalid_modes_fall_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.sqlite3");
        Connection::open(&path).unwrap();
        let settings = settings(&[
            ("GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS", "-10"),
            ("GRAPHYARD_SQLITE_JOURNAL_MODE", "fast"),
            ("GRAPHYARD_SQLITE_SYNCHRONOUS", "sometimes"),
        ]);
        let store = Store::open(&path, &settings).unwrap();
        let values = store
            .call(|connection| {
                Ok([
                    scalar(connection, "PRAGMA busy_timeout"),
                    scalar(connection, "PRAGMA journal_mode"),
                    scalar(connection, "PRAGMA synchronous"),
                ])
            })
            .await
            .unwrap();
        // NORMAL is 1.
        assert_eq!(values, ["0", "wal", "1"]);
        store.close();
    }

    #[tokio::test]
    async fn a_failing_pragma_is_logged_and_the_rest_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.sqlite3");
        // Another connection holds an exclusive lock: switching to WAL fails
        // with `database is locked` (busy timeout 0), so `synchronous` is
        // never applied, as in Python.
        let holder = Connection::open(&path).unwrap();
        holder
            .execute_batch("CREATE TABLE t (id INTEGER); BEGIN EXCLUSIVE")
            .unwrap();
        let settings = settings(&[
            ("GRAPHYARD_SQLITE_TIMEOUT_SECONDS", "0"),
            ("GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS", "0"),
            ("GRAPHYARD_SQLITE_SYNCHRONOUS", "OFF"),
        ]);
        let store = Store::open(&path, &settings).unwrap();
        holder.execute_batch("COMMIT").unwrap();
        let values = store
            .call(|connection| {
                Ok([
                    scalar(connection, "PRAGMA busy_timeout"),
                    scalar(connection, "PRAGMA journal_mode"),
                    scalar(connection, "PRAGMA synchronous"),
                ])
            })
            .await
            .unwrap();
        // FULL (2) is SQLite's default; OFF would be 0.
        assert_eq!(values, ["0", "delete", "2"]);
        store.close();
    }

    #[tokio::test]
    async fn a_panicking_operation_kills_the_thread() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.sqlite3");
        Connection::open(&path).unwrap();
        let store = Store::open(&path, &settings(&[])).unwrap();
        assert_eq!(store.call(|_| Ok(1)).await.unwrap(), 1);
        let died = store
            .call::<(), _>(|_| panic!("operation panicked"))
            .await
            .unwrap_err();
        assert!(matches!(died, StoreError::ThreadGone));
        let later = store.call(|_| Ok(2)).await.unwrap_err();
        assert!(matches!(later, StoreError::ThreadGone));
        store.close();
    }

    #[tokio::test]
    async fn close_finishes_the_running_operation_and_stops_the_thread() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.sqlite3");
        Connection::open(&path).unwrap();
        let store = Store::open(&path, &settings(&[])).unwrap();
        let clone = store.clone();
        let (started, wait_started) = std::sync::mpsc::channel();
        let slow = tokio::spawn({
            let store = store.clone();
            async move {
                store
                    .call(move |connection| {
                        started.send(()).unwrap();
                        std::thread::sleep(Duration::from_millis(200));
                        connection
                            .execute_batch("CREATE TABLE finished (id INTEGER)")
                            .map_err(StoreError::from)
                    })
                    .await
            }
        });
        tokio::task::yield_now().await;
        wait_started.recv().unwrap();
        // The slow operation is running; dropping its caller does not cut it
        // short and close waits for it.
        slow.abort();
        store.close();
        assert!(matches!(
            clone.call(|_| Ok(())).await.unwrap_err(),
            StoreError::ThreadGone
        ));
        let check = Connection::open(&path).unwrap();
        let tables: i64 = check
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name = 'finished'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tables, 1);
    }

    #[test]
    fn open_never_creates_the_database() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.sqlite3");
        assert!(Store::open(&path, &settings(&[])).is_err());
        assert!(open_read_only(&path, &settings(&[])).is_err());
        assert!(!path.exists());
    }
}
