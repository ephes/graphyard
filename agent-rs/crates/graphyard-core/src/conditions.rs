//! Condition evaluation (PLAN.md section 7, P1).
//!
//! Python sources: `services.py:evaluate_condition`, `_evaluate_series`,
//! `_is_breached_for_duration`, `_series_key`, `_group_samples_by_series`,
//! `_series_labels`, `_STATUS_SEVERITY` and
//! `models.py:ConditionDefinition.stale_limit_seconds`. Pinned by
//! `fixtures/conditions.json` (`fixtures/gen_conditions.py`), which covers
//! every `evaluate_condition` test of `tests/test_condition_evaluation.py`.
//!
//! The evaluation is pure: the samples come from a [`SampleSource`] (the
//! InfluxDB condition query in the agent, a stub in tests), `now` and the
//! global staleness setting are passed in. Time is computed on Python's
//! microsecond timeline: `now` and every sample timestamp are read with
//! [`DateTime::timestamp_micros`] (sub-microsecond parts are dropped, a
//! Python `datetime` has none), and the `timedelta` arithmetic raises where
//! CPython raises ([`crate::pycompat::py_timedelta_minutes`],
//! [`crate::pycompat::py_datetime_add`]).
//!
//! Python quirks kept on purpose: an unknown operator fails only when a
//! threshold is actually compared (`KeyError` of `_operator_fn`), the anchor
//! carry is bounded by the global staleness setting even when the condition
//! has its own allowance, and `stale_after_seconds = 0` means "use the
//! global setting".

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;
use std::future::Future;

use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use num_bigint::BigInt;

use crate::model::{ComparisonOperator, ConditionEvaluation, MetricSample, StatusLevel};
use crate::pycompat::{
    PyError, py_datetime_add, py_float_repr, py_timedelta_minutes, py_total_seconds,
};

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
        now: DateTime<Utc>,
    ) -> impl Future<Output = Result<Vec<MetricSample>, Self::Error>>;
}

impl<F, Fut, E> SampleSource for F
where
    F: FnMut(DateTime<Utc>) -> Fut,
    Fut: Future<Output = Result<Vec<MetricSample>, E>>,
{
    type Error = E;

    fn query_condition_window(
        &mut self,
        now: DateTime<Utc>,
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
    now: DateTime<Utc>,
    source: &mut S,
) -> Result<ConditionEvaluation, EvaluationError<S::Error>> {
    let now_micros = now.timestamp_micros();
    let now = micros_to_datetime(now_micros);
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
    now: DateTime<Utc>,
    stale_warning_seconds: &BigInt,
) -> Result<ConditionEvaluation, PyError> {
    let now_micros = now.timestamp_micros();
    let evaluated_at = micros_to_datetime(now_micros);
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

    let grouped = group_samples_by_series(samples);
    let keys: Vec<&SeriesKey> = grouped.keys().collect();
    let labels = series_labels(&keys);
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
    // label (a stable sort, like Python's `list.sort`).
    results.sort_by(|a, b| {
        b.status
            .severity()
            .cmp(&a.status.severity())
            .then(a.latest.cmp(&b.latest))
            .then_with(|| labels[a.index].cmp(&labels[b.index]))
    });
    let total = results.len();
    let not_ok = results
        .iter()
        .filter(|result| result.status != StatusLevel::Ok)
        .count();

    let worst = &results[0];
    if worst.status == StatusLevel::Ok {
        // `max(results, key=(latest, label))`: the first maximal element.
        let mut newest = worst;
        for result in &results[1..] {
            if (result.latest, &labels[result.index]) > (newest.latest, &labels[newest.index]) {
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
            worst.message, labels[worst.index]
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
type SeriesKey = Vec<(String, String)>;

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
fn series_key(sample: &MetricSample) -> SeriesKey {
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
    dimensions
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect()
}

/// `_group_samples_by_series`: series in order of first appearance, each
/// sorted by time (stable, so equal timestamps keep the query order).
fn group_samples_by_series(samples: &[MetricSample]) -> IndexMap<SeriesKey, Vec<Point>> {
    let mut grouped: IndexMap<SeriesKey, Vec<Point>> = IndexMap::new();
    for sample in samples {
        grouped.entry(series_key(sample)).or_default().push(Point {
            ts: sample.ts.timestamp_micros(),
            value: sample.value,
        });
    }
    for series in grouped.values_mut() {
        series.sort_by_key(|point| point.ts);
    }
    grouped
}

fn dimension<'a>(key: &'a SeriesKey, name: &str) -> Option<&'a str> {
    key.binary_search_by(|(candidate, _)| candidate.as_str().cmp(name))
        .ok()
        .map(|index| key[index].1.as_str())
}

/// `_series_labels`: `name=value` of the names whose value (or absence)
/// differs between the series; with nothing varying (one series) every name
/// but `metric`. A missing name renders as `name=`.
fn series_labels(keys: &[&SeriesKey]) -> Vec<String> {
    let all_names: BTreeSet<&str> = keys
        .iter()
        .flat_map(|key| key.iter().map(|(name, _)| name.as_str()))
        .collect();
    let varying: Vec<&str> = all_names
        .iter()
        .copied()
        .filter(|name| {
            keys.iter()
                .map(|key| dimension(key, name))
                .collect::<HashSet<_>>()
                .len()
                > 1
        })
        .collect();
    let names: Vec<&str> = if varying.is_empty() {
        all_names
            .iter()
            .copied()
            .filter(|name| *name != "metric")
            .collect()
    } else {
        varying
    };
    keys.iter()
        .map(|key| {
            names
                .iter()
                .map(|name| format!("{name}={}", dimension(key, name).unwrap_or("")))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .collect()
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

    for (status, threshold) in [
        (StatusLevel::Critical, rule.critical_threshold),
        (StatusLevel::Warning, rule.warning_threshold),
    ] {
        let Some(threshold) = threshold else {
            continue;
        };
        if is_breached_for_duration(
            series,
            &rule.operator,
            threshold,
            rule.breach_minutes,
            now,
            stale_warning_seconds,
        )? {
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
    Ok((
        StatusLevel::Ok,
        "Condition is within thresholds".to_owned(),
        latest.value,
    ))
}

/// `_is_breached_for_duration(values, operator, threshold, breach_minutes,
/// now, max_carry_seconds=settings.CONDITION_DATA_STALE_WARNING_SECONDS)`.
///
/// The breach window is `[now - breach_minutes, now]`. The last sample
/// before it (the anchor) covers the window start when it is at most
/// `max_carry_seconds` older and no sample lies exactly at the start; the
/// anchor and every in-window sample must breach. Without an anchor the
/// first in-window sample must lie within one minute of the window start.
fn is_breached_for_duration(
    values: &[Point],
    operator: &str,
    threshold: f64,
    breach_minutes: i64,
    now: i64,
    max_carry_seconds: &BigInt,
) -> Result<bool, PyError> {
    if values.is_empty() {
        return Ok(false);
    }
    let operator =
        ComparisonOperator::parse(operator).ok_or_else(|| PyError::key_error(operator))?;
    let window_start = py_datetime_add(now, -py_timedelta_minutes(breach_minutes)?)?;
    let window_values: Vec<Point> = values
        .iter()
        .copied()
        .filter(|point| point.ts >= window_start)
        .collect();

    let covers_start = window_values
        .first()
        .is_some_and(|point| point.ts == window_start);
    let anchor = if covers_start {
        None
    } else {
        values
            .iter()
            .rev()
            .find(|point| point.ts < window_start)
            .copied()
            .filter(|candidate| {
                total_seconds_at_most(
                    micros_between(window_start, candidate.ts),
                    max_carry_seconds,
                )
            })
    };

    let breached = |point: &Point| operator.compare(point.value, threshold);
    match anchor {
        Some(anchor) => Ok(breached(&anchor) && window_values.iter().all(breached)),
        None => {
            let Some(first) = window_values.first() else {
                return Ok(false);
            };
            let grace_start = py_datetime_add(window_start, py_timedelta_minutes(1)?)?;
            if first.ts > grace_start {
                return Ok(false);
            }
            Ok(window_values.iter().all(breached))
        }
    }
}

/// `later - earlier` in microseconds. Both come from Python-range datetimes
/// in practice; out-of-range chrono values saturate instead of overflowing.
fn micros_between(later: i64, earlier: i64) -> i64 {
    later.saturating_sub(earlier)
}

/// `timedelta.total_seconds() <= limit` with Python's exact float/int
/// comparison: for a float `x` and an integer `n`, `x <= n` exactly when
/// `ceil(x) <= n`.
fn total_seconds_at_most(micros: i64, limit: &BigInt) -> bool {
    // |total_seconds| < 2**63 / 10**6, so the ceiling fits an i64.
    BigInt::from(py_total_seconds(micros).ceil() as i64) <= *limit
}

fn micros_to_datetime(micros: i64) -> DateTime<Utc> {
    // The count comes from `timestamp_micros` of a chrono value.
    DateTime::from_timestamp_micros(micros).unwrap_or_default()
}

#[cfg(test)]
mod tests;
