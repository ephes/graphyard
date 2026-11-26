//! Condition evaluation (PLAN.md section 7, P1).
//!
//! Python sources: `services.py:evaluate_condition`, `_evaluate_series`,
//! `_is_breached_for_duration`, `_series_key`, `_group_samples_by_series`,
//! `_series_label`, `_STATUS_SEVERITY` and
//! `models.py:ConditionDefinition.stale_limit_seconds`. Pinned by
//! `fixtures/conditions.json` (`fixtures/gen_conditions.py`), which covers
//! every `evaluate_condition` test of `tests/test_condition_evaluation.py`.
//!
//! The evaluation is pure: the samples come from a [`SampleSource`] (the
//! InfluxDB condition query in the agent, a stub in tests), `now` and the
//! global staleness setting are passed in. Time is computed on Python's
//! microsecond timeline: `now` and every sample timestamp are read with
//! [`UtcDateTime::micros`] (the type has no sub-microsecond part, like a
//! Python `datetime`), and the `timedelta` arithmetic raises where
//! CPython raises ([`crate::pycompat::py_timedelta_minutes`],
//! [`crate::pycompat::py_datetime_add`]).
//!
//! Python quirks kept on purpose: an unknown operator fails only when a
//! threshold is actually compared (`KeyError` of `_operator_fn`), the anchor
//! carry is bounded by the global staleness setting even when the condition
//! has its own allowance, and `stale_after_seconds = 0` means "use the
//! global setting".

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;

use crate::utc::UtcDateTime;
use indexmap::IndexMap;
use num_bigint::BigInt;

use crate::model::{ComparisonOperator, ConditionEvaluation, MetricSample, StatusLevel};
use crate::pycompat::{
    PyError, py_datetime_add, py_float_repr, py_timedelta_minutes, py_total_seconds,
};

/// Inclusive practical ceiling shared with Python's condition_duration module.
pub const MAX_CONDITION_MINUTES: i64 = 10080;
pub const DURATION_ERROR: &str = "Must be an integer between 1 and 10080 minutes.";

pub fn validate_condition_duration(value: i64) -> Result<(), PyError> {
    if !(1..=MAX_CONDITION_MINUTES).contains(&value) {
        return Err(PyError::value_error(DURATION_ERROR));
    }
    Ok(())
}

/// The fields of a `ConditionDefinition` that the evaluation reads (the
/// query filters belong to the [`SampleSource`]).
#[derive(Clone, Debug, PartialEq)]
pub struct ConditionRule {
    pub metric_name: String,
    /// The stored text; an unknown operator is the `KeyError` of
    /// `_operator_fn`, raised only when a threshold is compared.
    pub operator: String,
    pub warning_threshold: Option<f64>,
    pub critical_threshold: Option<f64>,
    pub breach_minutes: i64,
    pub stale_after_seconds: Option<i64>,
    pub alert_when_stale: bool,
}

impl ConditionRule {
    /// `ConditionDefinition.stale_limit_seconds()`: the condition's own
    /// allowance when it is set and not 0, else
    /// `settings.CONDITION_DATA_STALE_WARNING_SECONDS`.
    pub fn stale_limit_seconds(&self, stale_warning_seconds: &BigInt) -> BigInt {
        match self.stale_after_seconds {
            Some(seconds) if seconds != 0 => BigInt::from(seconds),
            _ => stale_warning_seconds.clone(),
        }
    }
}

/// Where `evaluate_condition` gets its samples:
/// `influx.query_condition_window(condition, now=now_utc)` for the
/// condition the source was built for.
pub trait SampleSource {
    type Error;

    /// The samples of the condition window ending at `now`, in the order
    /// the query returned them.
    fn query_condition_window(
        &mut self,
        now: UtcDateTime,
    ) -> impl Future<Output = Result<Vec<MetricSample>, Self::Error>>;
}

impl<F, Fut, E> SampleSource for F
where
    F: FnMut(UtcDateTime) -> Fut,
    Fut: Future<Output = Result<Vec<MetricSample>, E>>,
{
    type Error = E;

    fn query_condition_window(
        &mut self,
        now: UtcDateTime,
    ) -> impl Future<Output = Result<Vec<MetricSample>, E>> {
        self(now)
    }
}

/// Why `evaluate_condition` raised. `Display` is `str(err)`, the text after
/// `evaluation error: ` in the condition's message (PLAN.md 3.5).
#[derive(Debug, PartialEq)]
pub enum EvaluationError<E> {
    /// The sample query failed.
    Query(E),
    /// The evaluation itself raised (unknown operator, datetime overflow).
    Evaluation(PyError),
}

impl<E: fmt::Display> fmt::Display for EvaluationError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Query(err) => err.fmt(f),
            Self::Evaluation(err) => err.fmt(f),
        }
    }
}

impl<E: fmt::Debug + fmt::Display> std::error::Error for EvaluationError<E> {}

/// `services.py:evaluate_condition(condition, now=now)`: query the samples
/// for `now` (truncated to microseconds, as a Python `datetime`), then
/// [`evaluate_samples`]. `stale_warning_seconds` is
/// `settings.CONDITION_DATA_STALE_WARNING_SECONDS`.
pub async fn evaluate_condition<S: SampleSource>(
    rule: &ConditionRule,
    stale_warning_seconds: &BigInt,
    now: UtcDateTime,
    source: &mut S,
) -> Result<ConditionEvaluation, EvaluationError<S::Error>> {
    validate_condition_duration(rule.breach_minutes).map_err(EvaluationError::Evaluation)?;
    let samples = source
        .query_condition_window(now)
        .await
        .map_err(EvaluationError::Query)?;
    evaluate_samples(rule, &samples, now, stale_warning_seconds)
        .map_err(EvaluationError::Evaluation)
}

/// The body of `evaluate_condition` after the query: no samples, one series
/// or several series (worst status wins).
pub fn evaluate_samples(
    rule: &ConditionRule,
    samples: &[MetricSample],
    now: UtcDateTime,
    stale_warning_seconds: &BigInt,
) -> Result<ConditionEvaluation, PyError> {
    validate_condition_duration(rule.breach_minutes)?;
    let now_micros = now.micros();
    let evaluated_at = now;
    let evaluation = |status, message, last_value| ConditionEvaluation {
        status,
        message,
        last_value,
        evaluated_at,
    };

    if samples.is_empty() {
        return Ok(if rule.alert_when_stale {
            evaluation(
                StatusLevel::Warning,
                "No samples available in condition window".to_owned(),
                None,
            )
        } else {
            evaluation(
                StatusLevel::Ok,
                "No samples available in condition window, not alerting (intermittent)".to_owned(),
                None,
            )
        });
    }

    let grouped = group_samples_by_series(samples)?;
    let keys: Vec<_> = grouped.keys().collect();
    let mut results = Vec::with_capacity(grouped.len());
    for (index, series) in grouped.values().enumerate() {
        let latest = series[series.len() - 1].ts;
        let (status, message, last_value) =
            evaluate_series(rule, series, now_micros, stale_warning_seconds)?;
        results.push(SeriesResult {
            index,
            latest,
            status,
            message,
            last_value,
        });
    }

    if results.len() == 1 {
        let result = results.remove(0);
        return Ok(evaluation(
            result.status,
            result.message,
            Some(result.last_value),
        ));
    }

    // Worst status first; among equals the oldest latest sample, then the
    // structural series key (independent of display escaping).
    results.sort_by(|a, b| {
        b.status
            .severity()
            .cmp(&a.status.severity())
            .then(a.latest.cmp(&b.latest))
            .then_with(|| keys[a.index].cmp(keys[b.index]))
    });
    let total = results.len();
    let not_ok = results
        .iter()
        .filter(|result| result.status != StatusLevel::Ok)
        .count();

    let worst = &results[0];
    if worst.status == StatusLevel::Ok {
        // `max(results, key=(latest, structural_key))`.
        let mut newest = worst;
        for result in &results[1..] {
            if (result.latest, keys[result.index]) > (newest.latest, keys[newest.index]) {
                newest = result;
            }
        }
        return Ok(evaluation(
            newest.status,
            format!("{} ({total} series)", newest.message),
            Some(newest.last_value),
        ));
    }
    Ok(evaluation(
        worst.status,
        format!(
            "{} [series: {}] ({not_ok} of {total} series not OK)",
            worst.message,
            series_label(&keys, keys[worst.index])
        ),
        Some(worst.last_value),
    ))
}

/// A sample reduced to what the series evaluation reads.
#[derive(Clone, Copy, Debug)]
struct Point {
    /// Microseconds since the Unix epoch.
    ts: i64,
    value: f64,
}

/// `_series_key`: the sorted `(name, value)` pairs of a sample.
type SeriesKey<'a> = Vec<(&'a str, &'a str)>;

struct SeriesResult {
    /// Position of the series in query order (also its label's index).
    index: usize,
    latest: i64,
    status: StatusLevel,
    message: String,
    last_value: f64,
}

/// `_series_key`: the metric, every non-empty canonical dimension, then the
/// custom tags without overriding a dimension (`setdefault`), sorted by name.
fn series_key(sample: &MetricSample) -> SeriesKey<'_> {
    let mut dimensions: BTreeMap<&str, &str> = BTreeMap::new();
    dimensions.insert("metric", &sample.metric);
    let canonical: [(&str, Option<&str>); 9] = [
        ("host", Some(sample.host.as_str())),
        ("service", sample.service.as_deref()),
        ("subject_type", sample.subject_type.as_deref()),
        ("subject_id", sample.subject_id.as_deref()),
        ("source_system", sample.source_system.as_deref()),
        ("source_instance", sample.source_instance.as_deref()),
        ("source_entity_id", sample.source_entity_id.as_deref()),
        ("collector_service", sample.collector_service.as_deref()),
        ("collector_host", sample.collector_host.as_deref()),
    ];
    for (name, value) in canonical {
        if let Some(value) = value.filter(|value| !value.is_empty()) {
            dimensions.insert(name, value);
        }
    }
    for (name, value) in &sample.tags {
        dimensions.entry(name).or_insert(value);
    }
    dimensions.into_iter().collect()
}

/// `_group_samples_by_series`: series in order of first appearance, each
/// sorted by time; conflicting equal-time values refuse the entire observation.
fn group_samples_by_series(
    samples: &[MetricSample],
) -> Result<IndexMap<SeriesKey<'_>, Vec<Point>>, PyError> {
    let mut grouped: IndexMap<SeriesKey<'_>, Vec<Point>> = IndexMap::new();
    for sample in samples {
        grouped.entry(series_key(sample)).or_default().push(Point {
            ts: sample.ts.micros(),
            value: sample.value,
        });
    }
    for series in grouped.values_mut() {
        series.sort_by_key(|point| point.ts);
        if series
            .windows(2)
            .any(|pair| pair[0].ts == pair[1].ts && pair[0].value != pair[1].value)
        {
            return Err(PyError::value_error(
                "Conflicting values for same series and timestamp",
            ));
        }
    }
    Ok(grouped)
}

#[cfg(test)]
thread_local! { static DIMENSION_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

fn dimension<'a>(key: &SeriesKey<'a>, name: &str) -> Option<&'a str> {
    #[cfg(test)]
    DIMENSION_READS.with(|count| count.set(count.get() + 1));
    key.binary_search_by(|(candidate, _)| (*candidate).cmp(name))
        .ok()
        .map(|index| key[index].1)
}

/// `_series_label`: JSON-quoted names/values of the dimensions that differ;
/// with nothing varying (one series) every name but `metric`. Missing values
/// render as `null`, distinct from the quoted empty string.
fn series_label(keys: &[&SeriesKey<'_>], selected: &SeriesKey<'_>) -> String {
    // One visit per present dimension; absence varies whenever its count is
    // below the series count. Sparse keys never require an N-by-union matrix.
    let mut dimensions: BTreeMap<&str, (&str, usize, bool)> = BTreeMap::new();
    for key in keys {
        for &(name, value) in key.iter() {
            let entry = dimensions.entry(name).or_insert((value, 0, false));
            entry.1 += 1;
            entry.2 |= entry.0 != value;
        }
    }
    let varying = dimensions
        .values()
        .any(|(_, count, differs)| *count != keys.len() || *differs);
    dimensions
        .iter()
        .filter(|(name, (_, count, differs))| {
            if varying {
                *count != keys.len() || *differs
            } else {
                **name != "metric"
            }
        })
        .map(|(name, _)| {
            format!(
                "{}={}",
                serde_json::to_string(name).expect("string serialization"),
                serde_json::to_string(&dimension(selected, name))
                    .expect("optional string serialization")
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// `_evaluate_series` on one time-ordered, non-empty series.
fn evaluate_series(
    rule: &ConditionRule,
    series: &[Point],
    now: i64,
    stale_warning_seconds: &BigInt,
) -> Result<(StatusLevel, String, f64), PyError> {
    let latest = series[series.len() - 1];
    // `int((now - latest.ts).total_seconds())`.
    let stale_seconds = py_total_seconds(micros_between(now, latest.ts)).trunc() as i64;
    if BigInt::from(stale_seconds) > rule.stale_limit_seconds(stale_warning_seconds) {
        return Ok(if rule.alert_when_stale {
            (
                StatusLevel::Warning,
                format!("Latest sample is stale ({stale_seconds}s old)"),
                latest.value,
            )
        } else {
            (
                StatusLevel::Ok,
                format!(
                    "Latest sample is stale ({stale_seconds}s old), not alerting (intermittent)"
                ),
                latest.value,
            )
        });
    }

    if rule.critical_threshold.is_some() || rule.warning_threshold.is_some() {
        let operator = ComparisonOperator::parse(&rule.operator)
            .ok_or_else(|| PyError::key_error(&rule.operator))?;
        let window =
            continuous_breach_window(series, rule.breach_minutes, now, stale_warning_seconds)?;
        for (status, threshold) in [
            (StatusLevel::Critical, rule.critical_threshold),
            (StatusLevel::Warning, rule.warning_threshold),
        ] {
            let Some(threshold) = threshold else {
                continue;
            };
            if window.is_some_and(|points| {
                points
                    .iter()
                    .all(|point| operator.compare(point.value, threshold))
            }) {
                return Ok((
                    status,
                    format!(
                        "{} {} {} for {}m",
                        rule.metric_name,
                        rule.operator,
                        py_float_repr(threshold),
                        rule.breach_minutes
                    ),
                    latest.value,
                ));
            }
        }
    }
    Ok((
        StatusLevel::Ok,
        "Condition is within thresholds".to_owned(),
        latest.value,
    ))
}

/// Select a continuous slice of the sorted query result for threshold checks.
/// A sample at the window start takes precedence over the last earlier
/// sample (the anchor). Without a usable anchor, the first in-window
/// sample must lie within one minute of the window start.
fn continuous_breach_window<'a>(
    values: &'a [Point],
    breach_minutes: i64,
    now: i64,
    max_carry_seconds: &BigInt,
) -> Result<Option<&'a [Point]>, PyError> {
    let window_start = py_datetime_add(now, -py_timedelta_minutes(breach_minutes)?)?;
    let start = values.partition_point(|point| point.ts < window_start);
    let (before, window) = values.split_at(start);
    let covers_start = window.first().is_some_and(|point| point.ts == window_start);
    let anchor = before.last().filter(|candidate| {
        !covers_start
            && total_seconds_at_most(
                micros_between(window.first().map_or(now, |point| point.ts), candidate.ts),
                max_carry_seconds,
            )
    });
    if anchor.is_none() {
        let Some(first) = window.first() else {
            return Ok(None);
        };
        let grace_start = py_datetime_add(window_start, py_timedelta_minutes(1)?)?;
        if first.ts > grace_start {
            return Ok(None);
        }
    }

    // Anchor admission already checked the carry to the first point (or now).
    if window.windows(2).any(|pair| {
        !total_seconds_at_most(micros_between(pair[1].ts, pair[0].ts), max_carry_seconds)
    }) || window
        .last()
        .is_some_and(|last| !total_seconds_at_most(micros_between(now, last.ts), max_carry_seconds))
    {
        return Ok(None);
    }
    Ok(Some(&values[start - usize::from(anchor.is_some())..]))
}

/// `later - earlier` in microseconds. Both come from Python-range datetimes
/// in practice; other instants saturate instead of overflowing.
fn micros_between(later: i64, earlier: i64) -> i64 {
    later.saturating_sub(earlier)
}

#[cfg(test)]
thread_local! { static CARRY_CHECKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

/// `timedelta.total_seconds() <= limit` with Python's exact float/int
/// comparison: for a float `x` and an integer `n`, `x <= n` exactly when
/// `ceil(x) <= n`.
fn total_seconds_at_most(micros: i64, limit: &BigInt) -> bool {
    #[cfg(test)]
    CARRY_CHECKS.with(|count| count.set(count.get() + 1));
    // |total_seconds| < 2**63 / 10**6, so the ceiling fits an i64.
    BigInt::from(py_total_seconds(micros).ceil() as i64) <= *limit
}

#[cfg(test)]
mod tests;
