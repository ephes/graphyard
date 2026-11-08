//! `pipeline_heartbeat`: `services.py:record_heartbeat` with
//! `min_update_interval_seconds=0` (PLAN.md 5.4).
//!
//! Django 6's `update_or_create(name=..., defaults)` inside one
//! `BEGIN IMMEDIATE` transaction: load the row (all columns, datetimes
//! converted); a missing row is inserted; an existing row gets an UPDATE of
//! every field `update_or_create` saves, which is the `defaults` plus every
//! field whose class overrides `pre_save`: `updated_at` (auto_now, set to
//! `now`) and `last_success` (a `DateTimeField`), so after a failure the
//! loaded `last_success` is written back in Django's text form (an
//! unparseable stored value becomes NULL). Three attempts, retrying only on
//! `database is locked` after 50 ms and 100 ms.
//! Rust additionally fences status/details by observation time under the writer
//! lock (first commit wins ties), while retaining the maximum successful time.

use std::time::Duration;

use chrono::{DateTime, Utc};
use graphyard_core::StatusLevel;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use crate::actor::Store;
use crate::error::StoreError;
use crate::rows::{datetime_column, now_text};

const ATTEMPTS: u32 = 3;

/// One `record_heartbeat` call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeartbeatRecord {
    pub name: String,
    pub status: StatusLevel,
    pub last_error: String,
    /// The `details` dict in insertion order (`json.dumps` keeps it).
    pub details: Vec<(String, i64)>,
    pub success: bool,
}

/// `json.dumps(details)` with the default separators and `ensure_ascii`.
pub fn details_json(details: &[(String, i64)]) -> String {
    let items: Vec<String> = details
        .iter()
        .map(|(key, value)| format!("{}: {value}", json_string(key)))
        .collect();
    format!("{{{}}}", items.join(", "))
}

/// A JSON string as `json.dumps` (`ensure_ascii=True`) writes it.
fn json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            ' '..='\u{7e}' => out.push(c),
            _ => {
                let mut units = [0u16; 2];
                for unit in c.encode_utf16(&mut units) {
                    out.push_str(&format!("\\u{unit:04x}"));
                }
            }
        }
    }
    out.push('"');
    out
}

/// One attempt: the `update_or_create` transaction.
fn write_heartbeat(
    connection: &mut Connection,
    record: &HeartbeatRecord,
    now: &str,
) -> Result<(), StoreError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let existing: Option<(i64, Option<String>, Option<String>)> = transaction
        .query_row(
            "SELECT id, CAST(last_success AS TEXT), CAST(updated_at AS TEXT) \
             FROM pipeline_heartbeat WHERE name = ?",
            [&record.name],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let details = details_json(&record.details);
    match existing {
        None => {
            let last_success = record.success.then_some(now);
            transaction.execute(
                "INSERT INTO pipeline_heartbeat (name, status, last_success, last_error, \
                 details, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                params![
                    record.name,
                    record.status.as_str(),
                    last_success,
                    record.last_error,
                    details,
                    now
                ],
            )?;
        }
        Some((id, stored_last_success, stored_updated_at)) => {
            // Django converts every loaded datetime (an out-of-range value
            // fails the call here) and writes `last_success` back.
            let loaded_last_success = datetime_column(stored_last_success.as_deref())?;
            let loaded_updated_at = datetime_column(stored_updated_at.as_deref())?;
            let observed = datetime_column(Some(now))?.expect("validated clock timestamp");
            let last_success = loaded_last_success
                .into_iter()
                .chain(record.success.then_some(observed))
                .max()
                .map(|value| value.to_db_text())
                .transpose()?;
            // The writer reservation serializes this comparison with other
            // processes. Equal-time status/details use first-commit-wins;
            // late successful evidence may still advance last_success.
            if loaded_updated_at.is_some_and(|updated| observed <= updated) {
                if record.success && loaded_last_success.is_none_or(|last| observed > last) {
                    transaction.execute(
                        "UPDATE pipeline_heartbeat SET last_success = ? WHERE id = ?",
                        params![last_success, id],
                    )?;
                }
                transaction.commit()?;
                return Ok(());
            }
            let changed = transaction.execute(
                "UPDATE pipeline_heartbeat SET status = ?, last_success = ?, last_error = ?, \
                 details = ?, updated_at = ? WHERE id = ?",
                params![
                    record.status.as_str(),
                    last_success,
                    record.last_error,
                    details,
                    now,
                    id
                ],
            )?;
            if changed == 0 {
                return Err(StoreError::NotUpdated);
            }
        }
    }
    transaction.commit()?;
    Ok(())
}

impl Store {
    /// `record_heartbeat(name, status=..., last_error=..., details=...,
    /// success=...)` at `now` (Python reads the clock twice, for
    /// `last_success` and `updated_at`; the store writes one instant, P2).
    /// The retry sleeps are await points, so a shutdown is never held up by
    /// them.
    ///
    /// # Errors
    /// The last attempt's error; errors other than `database is locked` are
    /// not retried.
    pub async fn record_heartbeat(
        &self,
        record: HeartbeatRecord,
        now: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        let now = now_text(now)?;
        let mut attempt = 1;
        loop {
            let job_record = record.clone();
            let job_now = now.clone();
            let result = self
                .call(move |connection| write_heartbeat(connection, &job_record, &job_now))
                .await;
            match result {
                Err(err) if attempt < ATTEMPTS && err.is_database_locked() => {
                    tokio::time::sleep(Duration::from_millis(50 * u64::from(attempt))).await;
                    attempt += 1;
                }
                other => return other,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_common as common;

    #[tokio::test]
    async fn delayed_heartbeats_preserve_newer_projection_and_maximum_success() {
        use std::future::Future;
        use std::task::{Context, Poll, Waker};

        for delayed_success in [false, true] {
            let db = common::TestDb::new();
            let older = db.store();
            let newer = db.store();
            let (entered, waiting) = std::sync::mpsc::channel();
            let (release, gate) = std::sync::mpsc::channel();
            let blocker = tokio::spawn({
                let store = older.clone();
                async move {
                    store
                        .call(move |_| {
                            entered.send(()).unwrap();
                            gate.recv_timeout(Duration::from_secs(10)).unwrap();
                            Ok(())
                        })
                        .await
                        .unwrap();
                }
            });
            tokio::task::yield_now().await;
            waiting.recv_timeout(Duration::from_secs(10)).unwrap();
            let record = |success, count| HeartbeatRecord {
                name: "synthetic".into(),
                status: if success {
                    StatusLevel::Ok
                } else {
                    StatusLevel::Warning
                },
                last_error: if success {
                    String::new()
                } else {
                    "synthetic failure".into()
                },
                details: vec![("count".into(), count)],
                success,
            };
            let t0 = common::now("2026-11-08 12:00:00");
            let t1 = common::now("2026-11-08 12:01:00");
            // Capture and enqueue the old timestamp while another connection owns
            // the writer lock. Its actor remains gated until the fresh commit.
            let holder = db.raw();
            holder.execute_batch("BEGIN IMMEDIATE").unwrap();
            let mut delayed = Box::pin(older.record_heartbeat(record(delayed_success, 1), t0));
            assert!(matches!(
                delayed
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            ));
            holder.execute_batch("COMMIT").unwrap();
            newer
                .record_heartbeat(record(!delayed_success, 2), t1)
                .await
                .unwrap();
            release.send(()).unwrap();
            delayed.await.unwrap();
            blocker.await.unwrap();

            let snapshot = || {
                db.raw().query_row(
                "SELECT status, last_error, details, updated_at, last_success FROM pipeline_heartbeat",
                [], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?, row.get::<_, String>(3)?, row.get::<_, Option<String>>(4)?)),
            ).unwrap()
            };
            let expected = record(!delayed_success, 2);
            let row = snapshot();
            assert_eq!(row.0, expected.status.as_str());
            assert_eq!(row.1, expected.last_error);
            assert_eq!(row.2, details_json(&expected.details));
            assert_eq!(row.3, "2026-11-08 12:01:00");
            assert_eq!(
                row.4.as_deref(),
                Some(if delayed_success {
                    "2026-11-08 12:00:00"
                } else {
                    "2026-11-08 12:01:00"
                })
            );
            // Equal timestamps retain the first committed status/details.
            older
                .record_heartbeat(record(delayed_success, 3), t1)
                .await
                .unwrap();
            let equal = snapshot();
            assert_eq!(
                (&equal.0, &equal.1, &equal.2, &equal.3),
                (&row.0, &row.1, &row.2, &row.3)
            );
            assert_eq!(equal.4.as_deref(), Some("2026-11-08 12:01:00"));
            newer
                .record_heartbeat(record(true, 4), common::now("2026-11-08 12:02:00"))
                .await
                .unwrap();
            let fresh = snapshot();
            assert_eq!(fresh.0, "ok");
            assert_eq!(fresh.2, "{\"count\": 4}");
            assert_eq!(fresh.3, "2026-11-08 12:02:00");
            assert_eq!(fresh.4.as_deref(), Some("2026-11-08 12:02:00"));
            older.close();
            newer.close();
        }
    }

    #[test]
    fn details_render_like_json_dumps() {
        assert_eq!(details_json(&[]), "{}");
        assert_eq!(
            details_json(&[("evaluated".to_owned(), 2), ("failed".to_owned(), 0)]),
            r#"{"evaluated": 2, "failed": 0}"#
        );
        assert_eq!(
            details_json(&[("\u{fc}\"\\\n\u{1}\u{1f600}".to_owned(), -1)]),
            r#"{"\u00fc\"\\\n\u0001\ud83d\ude00": -1}"#
        );
    }
}
