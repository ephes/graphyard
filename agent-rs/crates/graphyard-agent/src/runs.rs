//! The metrics and conditions runs (PLAN.md 3.4, 3.5):
//! `services.py:run_metric_collection_specs_once`,
//! `_run_single_metric_collection_spec`, `evaluate_conditions_once` and the
//! heartbeats they record. Pinned by `fixtures/runs.json`.
//!
//! The executors, the condition samples and the clock are parameters, so
//! the run rules are tested with fakes; [`crate::collectors`] has the real
//! ones.

use std::cell::Cell;
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use futures_util::FutureExt as _;
use graphyard_core::utc::UtcDateTime;
use num_bigint::BigInt;
use serde_json::Value;

use graphyard_collect::executors::{ExecutorError, ExecutorResult, RunState};
use graphyard_core::conditions::{ConditionRule, evaluate_condition, validate_condition_duration};
use graphyard_core::diagnostics::{collector_failure, error_category};
use graphyard_core::pycompat::{int_digit_limit_error, json_loads};
use graphyard_core::{
    ConditionEvaluationRun, MetricCollectionSpecRun, MetricSample, SpecOutcome, SpecType,
    StatusLevel,
};
use graphyard_influx::{ConditionQuery, QueryError};
use graphyard_store::{
    ConditionRow, DueSpecScan, HeartbeatRecord, PendingSpecIssue, SpecRow, SpecRunUpdate, Store,
    StoreError,
};

/// The UTC wall clock (`timezone.now()`, `datetime.now(UTC)`).
pub trait Clock {
    fn now(&self) -> UtcDateTime;
}

/// The system clock at Python's microsecond precision.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> UtcDateTime {
        UtcDateTime::now()
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
        now: UtcDateTime,
    ) -> impl Future<Output = Result<Vec<MetricSample>, QueryError>>;
}

/// Run errors (stderr `Metric collection failed: <err>` /
/// `Condition eval failed: <err>`).
#[derive(Debug)]
pub enum RunError {
    /// `ValueError("No matching enabled metric collection specs")`.
    NoMatchingSpecs,
    /// `next_run_time` beyond SQLite's INTEGER range: sqlite3's
    /// `OverflowError` text. Raised per spec only; [`run_metric_specs`]
    /// counts it as a failed spec and never returns it.
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

const MAX_HEARTBEAT_ERRORS: usize = 5;

fn retain_heartbeat_error(errors: &mut Vec<String>, name: &str, outcome: &SpecOutcome) {
    if errors.len() < MAX_HEARTBEAT_ERRORS
        && (outcome.status == StatusLevel::Critical
            || (outcome.status == StatusLevel::Warning && !outcome.message.is_empty()))
    {
        errors.push(format!("{name}: {}", outcome.message));
    }
}

/// `run_metric_collection_specs_once(due_only=...)`.
///
/// # Errors
/// See [`RunError`]. A store error stops the batch and leaves earlier
/// completions committed. If any outcome was counted, including a schedule
/// overflow, try to publish a partial heartbeat before returning the original
/// store error.
pub async fn run_metric_specs<E: SpecExecutors, C: Clock>(
    store: &Store,
    executors: &E,
    clock: &C,
    due_only: bool,
) -> Result<MetricCollectionSpecRun, RunError> {
    run_metric_specs_until_shutdown(store, executors, clock, due_only, &Cell::new(false)).await
}

pub(crate) async fn run_metric_specs_until_shutdown<E: SpecExecutors, C: Clock>(
    store: &Store,
    executors: &E,
    clock: &C,
    due_only: bool,
    stopping: &Cell<bool>,
) -> Result<MetricCollectionSpecRun, RunError> {
    // `_subject_mapping_warning_keys.clear()`: a new state per run.
    let mut state = RunState::new();
    let now = clock.now();
    let mut due_scan = DueSpecScan::new(now.timestamp());
    let mut manual_specs = if due_only {
        Vec::new().into_iter()
    } else {
        let specs = store.load_manual_specs().await?;
        if specs.is_empty() {
            return Err(RunError::NoMatchingSpecs);
        }
        specs.into_iter()
    };

    let mut run = MetricCollectionSpecRun::default();
    let mut errors: Vec<String> = Vec::new();
    let mut store_error = None;
    let mut interrupted = false;
    while !manual_specs.as_slice().is_empty() || (due_only && due_scan.has_pending()) {
        if stopping.get() {
            interrupted = true;
            break;
        }
        let next = if due_only {
            due_scan.next(store).await
        } else {
            Ok(manual_specs.next())
        };
        let spec = match next {
            Ok(Some(Ok(spec))) => spec,
            Ok(Some(Err(PendingSpecIssue::Superseded))) => continue,
            Ok(Some(Err(PendingSpecIssue::Invalid { id, reason }))) => {
                log::error!("Collector admission failed spec_id={id}: {reason}");
                run.total += 1;
                run.failed += 1;
                if errors.len() < MAX_HEARTBEAT_ERRORS {
                    errors.push(format!("spec_id={id}: {reason}"));
                }
                continue;
            }
            Ok(None) => break,
            Err(err) if run.total > 0 => {
                store_error = Some(err);
                break;
            }
            Err(err) => return Err(err.into()),
        };
        if stopping.get() {
            interrupted = true;
            break;
        }
        let run_at = if due_only { clock.now() } else { now };
        let outcome =
            match run_metric_spec(store, executors, clock, &mut state, &spec, run_at, due_only)
                .await
            {
                Ok(Some(outcome)) => outcome,
                Ok(None) => continue,
                Err(RunError::NextRunTimeOverflow) => {
                    log::error!("Collection schedule overflow spec_id={}", spec.id);
                    run.total += 1;
                    run.failed += 1;
                    if errors.len() < MAX_HEARTBEAT_ERRORS {
                        errors.push(format!(
                            "spec_id={}: next run time exceeds SQLite INTEGER range",
                            spec.id
                        ));
                    }
                    continue;
                }
                Err(RunError::Store(err)) if run.total > 0 => {
                    store_error = Some(err);
                    break;
                }
                Err(err) => return Err(err),
            };
        run.total += 1;
        run.ingested += outcome.ingested;
        run.skipped += outcome.skipped;
        match outcome.status {
            StatusLevel::Critical => run.failed += 1,
            StatusLevel::Warning => run.warning += 1,
            StatusLevel::Ok => {}
        }
        retain_heartbeat_error(&mut errors, &spec.name, &outcome);
    }

    if run.total == 0 {
        return Ok(run);
    }
    let partial = store_error.is_some() || interrupted;
    let status = if run.failed == run.total {
        StatusLevel::Critical
    } else if partial || run.failed > 0 || run.warning > 0 {
        StatusLevel::Warning
    } else {
        StatusLevel::Ok
    };
    let mut details: Vec<_> = [
        ("total", run.total),
        ("failed", run.failed),
        ("warning", run.warning),
        ("ingested", run.ingested),
        ("skipped", run.skipped),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), count(value)))
    .collect();
    if partial {
        details.push(("partial".to_owned(), 1));
        let reason = if interrupted {
            "agent shutdown"
        } else {
            "collector store failure"
        };
        errors.insert(0, format!("partial run: {reason}"));
    }
    let heartbeat = store
        .record_heartbeat(
            HeartbeatRecord {
                name: "metric_collectors".to_owned(),
                status,
                last_error: errors.join("; "),
                details,
                success: status != StatusLevel::Critical && !partial,
            },
            clock.now(),
        )
        .await;
    if let Some(err) = store_error {
        if let Err(heartbeat_err) = heartbeat {
            log::warn!(
                "Partial collector heartbeat publication failed category={}",
                heartbeat_err.python_class()
            );
        }
        return Err(err.into());
    }
    heartbeat?;
    Ok(run)
}

async fn run_metric_spec<E: SpecExecutors, C: Clock>(
    store: &Store,
    executors: &E,
    clock: &C,
    state: &mut RunState,
    spec: &SpecRow,
    run_at: UtcDateTime,
    due_only: bool,
) -> Result<Option<SpecOutcome>, RunError> {
    let run_ts = run_at.timestamp();
    let next_run_time = run_ts
        .checked_add(spec.interval_seconds.max(1))
        .ok_or(RunError::NextRunTimeOverflow)?;
    let lease_seconds = spec.interval_seconds.clamp(3, 300);
    let claim = spec.claim_identity();
    let mut claim_until = if due_only {
        let until = run_ts
            .checked_add(lease_seconds)
            .ok_or(RunError::NextRunTimeOverflow)?;
        if !store
            .claim_due_spec(claim.clone(), spec.next_run_time, run_ts, until)
            .await?
        {
            return Ok(None);
        }
        Some(until)
    } else {
        None
    };
    let outcome = if let Some(until) = claim_until.as_mut() {
        // Keep ownership through collection, the sink and registry publication.
        // Dropping this run also drops renewal; recovery stays bounded.
        let execution = run_single_spec(executors, state, spec);
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
                    if !store.renew_spec_claim(claim.clone(), *until, renewed).await? {
                        break None;
                    }
                    *until = renewed;
                }
            }
        }
    } else {
        Some(run_single_spec(executors, state, spec).await)
    };
    let Some(outcome) = outcome else {
        log::info!("Collection superseded spec_id={}", spec.id);
        return Ok(None);
    };
    let update = SpecRunUpdate {
        id: spec.id,
        last_run_at: run_at,
        last_status: outcome.status,
        last_error: outcome.message.clone(),
        next_run_time,
    };
    if !store
        .finish_claimed_spec(claim, update, claim_until.unwrap_or(spec.next_run_time))
        .await?
    {
        log::info!("Collection superseded spec_id={}", spec.id);
        return Ok(None);
    }
    Ok(Some(outcome))
}

/// `evaluate_condition(condition)` with its own `now`; errors retain only a category.
async fn evaluate<S: ConditionSamples>(
    condition: &ConditionRow,
    samples: &S,
    now: UtcDateTime,
    stale_warning_seconds: &BigInt,
) -> Result<graphyard_core::ConditionEvaluation, &'static str> {
    validate_condition_duration(condition.window_minutes).map_err(|_| "ValueError")?;
    validate_condition_duration(condition.breach_minutes).map_err(|_| "ValueError")?;
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
/// Only a full run writes the global heartbeat.
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
    if condition_id.is_none() {
        let ok = run.failed == 0;
        let no_work = run.total == 0;
        let mut details = vec![
            ("evaluated".to_owned(), count(run.total)),
            ("failed".to_owned(), count(run.failed)),
        ];
        if no_work {
            details.push(("no_work".to_owned(), 1));
        }
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
                    details,
                    success: ok && !no_work,
                },
                clock.now(),
            )
            .await?;
    }
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
    fn heartbeat_error_accumulation_stops_at_five() {
        let mut errors = Vec::new();
        for id in 0..256 {
            retain_heartbeat_error(&mut errors, &format!("spec-{id}"), &critical("failed"));
        }
        assert_eq!(errors.len(), 5);
        assert_eq!(errors[0], "spec-0: failed");
        assert_eq!(errors[4], "spec-4: failed");
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
