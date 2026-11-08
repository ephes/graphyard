//! The metrics and conditions runs (PLAN.md 3.4, 3.5):
//! `services.py:run_metric_collection_specs_once`,
//! `_run_single_metric_collection_spec`, `evaluate_conditions_once` and the
//! heartbeats they record. Pinned by `fixtures/runs.json`.
//!
//! The executors, the condition samples and the clock are parameters, so
//! the run rules are tested with fakes; [`crate::collectors`] has the real
//! ones.

use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use chrono::{DateTime, SubsecRound, Utc};
use futures_util::FutureExt as _;
use num_bigint::BigInt;
use serde_json::Value;

use graphyard_collect::executors::{ExecutorError, ExecutorResult, RunState};
use graphyard_core::conditions::{ConditionRule, evaluate_condition};
use graphyard_core::diagnostics::{collector_failure, error_category};
use graphyard_core::pycompat::{int_digit_limit_error, json_loads};
use graphyard_core::{
    ConditionEvaluationRun, MetricCollectionSpecRun, MetricSample, SpecOutcome, SpecType,
    StatusLevel,
};
use graphyard_influx::{ConditionQuery, QueryError};
use graphyard_store::{ConditionRow, HeartbeatRecord, SpecRow, SpecRunUpdate, Store, StoreError};

/// The UTC wall clock (`timezone.now()`, `datetime.now(UTC)`).
pub trait Clock {
    fn now(&self) -> DateTime<Utc>;
}

/// The system clock at Python's microsecond precision.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now().trunc_subsecs(6)
    }
}

/// `_SPEC_EXECUTORS[spec_type](spec)` for the five known spec types.
pub trait SpecExecutors {
    fn execute(
        &self,
        spec_type: SpecType,
        name: &str,
        config: &Value,
        run: &mut RunState,
    ) -> impl Future<Output = ExecutorResult>;
}

/// `influx.query_condition_window(condition, now=now)`.
pub trait ConditionSamples {
    fn query_condition_window(
        &self,
        condition: &ConditionQuery<'_>,
        now: DateTime<Utc>,
    ) -> impl Future<Output = Result<Vec<MetricSample>, QueryError>>;
}

/// Why a whole run failed (stderr `Metric collection failed: <err>` /
/// `Condition eval failed: <err>`, no heartbeat).
#[derive(Debug)]
pub enum RunError {
    /// `ValueError("No matching enabled metric collection specs")`.
    NoMatchingSpecs,
    /// `next_run_time` beyond SQLite's INTEGER range: sqlite3's
    /// `OverflowError`.
    NextRunTimeOverflow,
    /// A load, update or heartbeat failure.
    Store(StoreError),
}

impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoMatchingSpecs => f.write_str("No matching enabled metric collection specs"),
            Self::NextRunTimeOverflow => {
                f.write_str("Python int too large to convert to SQLite INTEGER")
            }
            Self::Store(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for RunError {}

impl From<StoreError> for RunError {
    fn from(err: StoreError) -> Self {
        Self::Store(err)
    }
}

impl RunError {
    /// `type(err).__name__` of the Python exception.
    pub fn python_class(&self) -> &'static str {
        match self {
            Self::NoMatchingSpecs => "ValueError",
            Self::NextRunTimeOverflow => "OverflowError",
            Self::Store(err) => err.python_class(),
        }
    }
}

/// Decode a stored JSON column (`config`, `tags_filter`) like Django's
/// `JSONField` loader where the two parsers agree; D-11 otherwise: an
/// integer token beyond CPython's digit limit is the `ValueError` Python
/// raises while loading, a document `serde_json` rejects (lone surrogate
/// escapes, nesting beyond 127 levels) is `<field> is not valid JSON: <err>`.
/// Both affect only this spec or condition.
///
/// # Errors
/// The message stored for the spec or condition.
pub fn load_stored_json(field: &str, text: &str) -> Result<Value, String> {
    if let Some(err) = int_digit_limit_error(text) {
        return Err(err.to_string());
    }
    json_loads(text).map_err(|err| format!("{field} is not valid JSON: {err}"))
}

fn critical(message: impl Into<String>) -> SpecOutcome {
    SpecOutcome::new(StatusLevel::Critical, 0, 0, message)
}

/// `_run_single_metric_collection_spec(spec)`: the executor's outcome, with
/// every failure isolated as a `critical` outcome. A panicking executor is
/// isolated like one that raises (class `panic`), so one bad spec can never
/// crash the agent and keep its own schedule stuck on "due".
pub async fn run_single_spec<E: SpecExecutors>(
    executors: &E,
    run: &mut RunState,
    spec: &SpecRow,
) -> SpecOutcome {
    let Some(spec_type) = SpecType::parse(&spec.spec_type) else {
        return critical(format!("unsupported spec_type: {}", spec.spec_type));
    };
    let config = match load_stored_json("config", &spec.config) {
        Ok(config) => config,
        Err(message) => return critical(message),
    };
    let result = AssertUnwindSafe(executors.execute(spec_type, &spec.name, &config, run))
        .catch_unwind()
        .await;
    let class = match result {
        Ok(Ok(outcome)) => return outcome,
        Ok(Err(ExecutorError::Config(err))) => return critical(err.0),
        Ok(Err(ExecutorError::Raised { class, .. })) => class,
        Err(_) => "panic".to_owned(),
    };
    let category = error_category(&class);
    log::error!(
        "Collector failure spec_id={} category={}",
        spec.id,
        category
    );
    critical(collector_failure(category))
}

fn count(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// `run_metric_collection_specs_once(due_only=...)`.
///
/// # Errors
/// See [`RunError`]; the specs processed before the failure keep their
/// rows, no heartbeat is written.
pub async fn run_metric_specs<E: SpecExecutors, C: Clock>(
    store: &Store,
    executors: &E,
    clock: &C,
    due_only: bool,
) -> Result<MetricCollectionSpecRun, RunError> {
    // `_subject_mapping_warning_keys.clear()`: a new state per run.
    let mut state = RunState::new();
    let now = clock.now();
    let now_ts = now.timestamp();
    let specs = store.load_specs(due_only.then_some(now_ts)).await?;
    if specs.is_empty() {
        if due_only {
            return Ok(MetricCollectionSpecRun::default());
        }
        return Err(RunError::NoMatchingSpecs);
    }

    let mut run = MetricCollectionSpecRun::default();
    let mut errors: Vec<String> = Vec::new();
    for spec in &specs {
        let run_at = if due_only { clock.now() } else { now };
        let run_ts = run_at.timestamp();
        let lease_seconds = spec.interval_seconds.clamp(3, 300);
        let mut claim_until = if due_only {
            let until = run_ts
                .checked_add(lease_seconds)
                .ok_or(RunError::NextRunTimeOverflow)?;
            if !store
                .claim_due_spec(spec.id, spec.next_run_time, run_ts, until)
                .await?
            {
                continue;
            }
            Some(until)
        } else {
            None
        };
        let outcome = if let Some(until) = claim_until.as_mut() {
            // Keep ownership through collection, the sink and registry publication.
            // Dropping this run also drops renewal; recovery stays bounded.
            let execution = run_single_spec(executors, &mut state, spec);
            tokio::pin!(execution);
            loop {
                tokio::select! {
                    biased;
                    outcome = &mut execution => break Some(outcome),
                    _ = tokio::time::sleep(Duration::from_secs((lease_seconds / 3) as u64)) => {
                        let renewed = clock.now().timestamp()
                            .checked_add(lease_seconds)
                            .ok_or(RunError::NextRunTimeOverflow)?
                            .max(until.checked_add(1).ok_or(RunError::NextRunTimeOverflow)?);
                        if !store.renew_spec_claim(spec.clone(), *until, renewed).await? {
                            break None;
                        }
                        *until = renewed;
                    }
                }
            }
        } else {
            Some(run_single_spec(executors, &mut state, spec).await)
        };
        let Some(outcome) = outcome else {
            log::info!("Collection superseded spec_id={}", spec.id);
            continue;
        };
        let next_run_time = run_ts
            .checked_add(spec.interval_seconds.max(1))
            .ok_or(RunError::NextRunTimeOverflow)?;
        let update = SpecRunUpdate {
            id: spec.id,
            last_run_at: run_at,
            last_status: outcome.status,
            last_error: outcome.message.clone(),
            next_run_time,
        };
        if !store
            .finish_claimed_spec(
                spec.clone(),
                update,
                claim_until.unwrap_or(spec.next_run_time),
            )
            .await?
        {
            log::info!("Collection superseded spec_id={}", spec.id);
            continue;
        }
        run.total += 1;
        run.ingested += outcome.ingested;
        run.skipped += outcome.skipped;
        match outcome.status {
            StatusLevel::Critical => {
                run.failed += 1;
                errors.push(format!("{}: {}", spec.name, outcome.message));
            }
            StatusLevel::Warning => {
                run.warning += 1;
                if !outcome.message.is_empty() {
                    errors.push(format!("{}: {}", spec.name, outcome.message));
                }
            }
            StatusLevel::Ok => {}
        }
    }

    if run.total == 0 {
        return Ok(run);
    }
    let (status, success) = if run.failed > 0 && run.total == run.failed {
        (StatusLevel::Critical, false)
    } else if run.failed > 0 || run.warning > 0 {
        (StatusLevel::Warning, true)
    } else {
        (StatusLevel::Ok, true)
    };
    let details = [
        ("total", run.total),
        ("failed", run.failed),
        ("warning", run.warning),
        ("ingested", run.ingested),
        ("skipped", run.skipped),
    ];
    store
        .record_heartbeat(
            HeartbeatRecord {
                name: "metric_collectors".to_owned(),
                status,
                last_error: errors
                    .iter()
                    .take(5)
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join("; "),
                details: details
                    .iter()
                    .map(|(key, value)| ((*key).to_owned(), count(*value)))
                    .collect(),
                success,
            },
            clock.now(),
        )
        .await?;
    Ok(run)
}

/// `evaluate_condition(condition)` with its own `now`; errors retain only a category.
async fn evaluate<S: ConditionSamples>(
    condition: &ConditionRow,
    samples: &S,
    now: DateTime<Utc>,
    stale_warning_seconds: &BigInt,
) -> Result<graphyard_core::ConditionEvaluation, &'static str> {
    let tags_filter =
        load_stored_json("tags_filter", &condition.tags_filter).map_err(|_| "ValueError")?;
    let query = ConditionQuery {
        id: condition.id,
        name: &condition.name,
        metric_name: &condition.metric_name,
        host_filter: &condition.host_filter,
        service_filter: &condition.service_filter,
        subject_type_filter: &condition.subject_type_filter,
        subject_id_filter: &condition.subject_id_filter,
        tags_filter: &tags_filter,
        window_minutes: condition.window_minutes,
        breach_minutes: condition.breach_minutes,
        stale_after_seconds: condition.stale_after_seconds,
    };
    let rule = ConditionRule {
        metric_name: condition.metric_name.clone(),
        operator: condition.operator.clone(),
        warning_threshold: condition.warning_threshold,
        critical_threshold: condition.critical_threshold,
        breach_minutes: condition.breach_minutes,
        stale_after_seconds: condition.stale_after_seconds,
        alert_when_stale: condition.alert_when_stale,
    };
    let mut source = |now| samples.query_condition_window(&query, now);
    evaluate_condition(&rule, stale_warning_seconds, now, &mut source)
        .await
        .map_err(|err| match err {
            graphyard_core::EvaluationError::Query(err) => err.python_class(),
            graphyard_core::EvaluationError::Evaluation(err) => err.python_class(),
        })
}

/// `evaluate_conditions_once(condition_id=...)`: the agent's runs pass
/// `None`, `graphyard-agent evaluate-conditions --condition-id N` the id.
/// The heartbeat is written for a single-condition run too, also when the
/// id selects nothing.
///
/// # Errors
/// A failed load or heartbeat fails the run; per-condition publication failures
/// are isolated and reported in safe diagnostics.
pub async fn run_conditions<S: ConditionSamples, C: Clock>(
    store: &Store,
    samples: &S,
    clock: &C,
    stale_warning_seconds: &BigInt,
    condition_id: Option<&BigInt>,
) -> Result<ConditionEvaluationRun, RunError> {
    let conditions = match condition_id.map(i64::try_from) {
        None => store.load_conditions(None).await?,
        Some(Ok(id)) => store.load_conditions(Some(id)).await?,
        // `filter(id=...)` with a value outside the 64-bit range of the
        // `BigAutoField` is Django's empty result: no query, no rows.
        Some(Err(_)) => Vec::new(),
    };
    let mut run = ConditionEvaluationRun::default();
    for condition in &conditions {
        run.total += 1;
        let attempted_at = clock.now();
        let (stage, saved) =
            match evaluate(condition, samples, attempted_at, stale_warning_seconds).await {
                Ok(evaluation) => (
                    "publication",
                    store
                        .update_condition_success(condition.clone(), evaluation)
                        .await
                        .map_err(|err| err.python_class()),
                ),
                Err(category) => ("evaluation", Err(category)),
            };
        if let Err(category) = saved {
            run.failed += 1;
            let category = error_category(category);
            let failure_published = store
                .update_condition_failure(
                    condition.clone(),
                    StatusLevel::Critical,
                    format!("evaluation error: {category}"),
                    attempted_at,
                )
                .await
                .unwrap_or(false);
            log::error!(
                "Condition failure condition_id={} stage={} category={} failure_published={}",
                condition.id,
                stage,
                category,
                failure_published
            );
        }
    }
    let ok = run.failed == 0;
    store
        .record_heartbeat(
            HeartbeatRecord {
                name: "condition_evaluator".to_owned(),
                status: if ok {
                    StatusLevel::Ok
                } else {
                    StatusLevel::Warning
                },
                last_error: if ok {
                    String::new()
                } else {
                    format!("{} condition(s) failed", run.failed)
                },
                details: vec![
                    ("evaluated".to_owned(), count(run.total)),
                    ("failed".to_owned(), count(run.failed)),
                ],
                success: ok,
            },
            clock.now(),
        )
        .await?;
    Ok(run)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_json_follows_d11() {
        assert_eq!(
            load_stored_json("config", r#"{"a": [1, 2.5]}"#).unwrap()["a"][1],
            serde_json::json!(2.5)
        );
        let huge = format!(r#"{{"n": {}}}"#, "1".repeat(4301));
        let err = load_stored_json("config", &huge).unwrap_err();
        assert!(
            err.starts_with("Exceeds the limit (4300 digits) for integer string conversion"),
            "{err}"
        );
        let at_limit = format!(r#"{{"n": {}}}"#, "1".repeat(4300));
        assert!(load_stored_json("config", &at_limit).is_ok());
        let surrogate = load_stored_json("tags_filter", r#"{"unused": "\ud800"}"#).unwrap_err();
        assert!(
            surrogate.starts_with("tags_filter is not valid JSON: "),
            "{surrogate}"
        );
        let deep = format!("{}{}", "[".repeat(200), "]".repeat(200));
        assert!(
            load_stored_json("config", &deep)
                .unwrap_err()
                .starts_with("config is not valid JSON: ")
        );
    }

    #[test]
    fn run_errors_carry_python_texts() {
        assert_eq!(
            RunError::NoMatchingSpecs.to_string(),
            "No matching enabled metric collection specs"
        );
        assert_eq!(RunError::NoMatchingSpecs.python_class(), "ValueError");
        assert_eq!(
            RunError::NextRunTimeOverflow.to_string(),
            "Python int too large to convert to SQLite INTEGER"
        );
        assert_eq!(
            RunError::Store(StoreError::NotUpdated).to_string(),
            "Save with update_fields did not affect any rows."
        );
        assert_eq!(
            RunError::Store(StoreError::NotUpdated).python_class(),
            "NotUpdated"
        );
    }
}
