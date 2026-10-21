//! The scheduler loop of `start_agent.py:handle` (PLAN.md 3.2, P1).
//!
//! Monotonic clock (tokio's, which tests pause and advance). Both loops run
//! sequentially in one task: a slow metrics run delays the conditions run.
//! A deadline that would leave `Instant`'s range (an interval of more than
//! about 2^63 seconds) means "never again", as Python's float arithmetic
//! does in effect.

use std::future::Future;
use std::time::Duration;

use num_bigint::BigInt;
use num_traits::ToPrimitive;
use tokio::time::Instant;

use crate::cli::Loops;

/// The two runs the loop schedules.
pub trait Runner {
    /// `_run_metric_specs(due_only=...)`.
    fn metrics(&mut self, due_only: bool) -> impl Future<Output = ()>;
    /// `_run_condition_eval()`.
    fn conditions(&mut self) -> impl Future<Output = ()>;
}

const MIN_SLEEP: Duration = Duration::from_millis(200);
const MAX_SLEEP: Duration = Duration::from_secs(1);

/// An interval in seconds as a duration (`None`: beyond `Duration`).
fn interval(seconds: &BigInt) -> Option<Duration> {
    seconds.to_u64().map(Duration::from_secs)
}

fn add(at: Instant, interval: Option<Duration>) -> Option<Instant> {
    interval.and_then(|interval| at.checked_add(interval))
}

/// `next += interval`, and `monotonic() + interval` when that is already
/// in the past.
fn reschedule(next: Instant, interval: Option<Duration>) -> Option<Instant> {
    match add(next, interval) {
        Some(next) if next < Instant::now() => add(Instant::now(), interval),
        other => other,
    }
}

/// `_run_enabled_loops`: metrics (`due_only=False`) then conditions.
pub async fn run_once<R: Runner>(runner: &mut R, loops: &Loops) {
    if loops.run_metrics {
        runner.metrics(false).await;
    }
    if loops.run_conditions {
        runner.conditions().await;
    }
}

/// The endless loop; `None` is Python's `float("inf")`.
pub async fn run_forever<R: Runner>(runner: &mut R, loops: &Loops) {
    let metrics_interval = interval(&loops.metrics_interval);
    let condition_interval = interval(&loops.condition_interval);
    let start = Instant::now();
    let mut next_metrics = loops.run_metrics.then_some(start);
    let mut next_conditions = loops.run_conditions.then_some(start);
    loop {
        let now = Instant::now();
        if let Some(next) = next_metrics
            && now >= next
        {
            runner.metrics(true).await;
            next_metrics = reschedule(next, metrics_interval);
        }
        if let Some(next) = next_conditions
            && now >= next
        {
            runner.conditions().await;
            next_conditions = reschedule(next, condition_interval);
        }
        let until = match (next_metrics, next_conditions) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        // `max(0.2, min(1.0, until - monotonic()))`.
        let sleep = until.map_or(MAX_SLEEP, |until| {
            until
                .saturating_duration_since(Instant::now())
                .clamp(MIN_SLEEP, MAX_SLEEP)
        });
        tokio::time::sleep(sleep).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Records the (virtual) second of every run; a run takes `metrics_cost`
    /// or `conditions_cost` of virtual time.
    struct Recorder {
        start: Instant,
        calls: Vec<(&'static str, f64)>,
        metrics_cost: Duration,
        conditions_cost: Duration,
    }

    impl Recorder {
        fn new(metrics_cost: u64, conditions_cost: u64) -> Self {
            Self {
                start: Instant::now(),
                calls: Vec::new(),
                metrics_cost: Duration::from_millis(metrics_cost),
                conditions_cost: Duration::from_millis(conditions_cost),
            }
        }

        fn at(&self) -> f64 {
            (Instant::now() - self.start).as_secs_f64()
        }
    }

    impl Runner for Recorder {
        async fn metrics(&mut self, due_only: bool) {
            let name = if due_only { "metrics" } else { "metrics-all" };
            self.calls.push((name, self.at()));
            tokio::time::sleep(self.metrics_cost).await;
        }

        async fn conditions(&mut self) {
            self.calls.push(("conditions", self.at()));
            tokio::time::sleep(self.conditions_cost).await;
        }
    }

    fn loops(metrics: Option<u64>, conditions: Option<u64>) -> Loops {
        Loops {
            run_metrics: metrics.is_some(),
            metrics_interval: BigInt::from(metrics.unwrap_or(60)),
            run_conditions: conditions.is_some(),
            condition_interval: BigInt::from(conditions.unwrap_or(60)),
            run_once: false,
        }
    }

    async fn run_for(recorder: &mut Recorder, loops: &Loops, millis: u64) {
        let _ =
            tokio::time::timeout(Duration::from_millis(millis), run_forever(recorder, loops)).await;
    }

    fn rounded(calls: &[(&'static str, f64)]) -> Vec<(&'static str, f64)> {
        calls
            .iter()
            .map(|(name, at)| (*name, (at * 10.0).round() / 10.0))
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn both_loops_start_at_once_and_repeat_on_their_interval() {
        let mut recorder = Recorder::new(0, 0);
        run_for(&mut recorder, &loops(Some(5), Some(3)), 9500).await;
        // Sleeps are capped at 1 s, so a run starts within a second of its
        // due time; with instant runs the 1 s ticks land exactly on it.
        assert_eq!(
            rounded(&recorder.calls),
            [
                ("metrics", 0.0),
                ("conditions", 0.0),
                ("conditions", 3.0),
                ("metrics", 5.0),
                ("conditions", 6.0),
                ("conditions", 9.0),
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_metrics_run_delays_conditions_and_keeps_its_cadence() {
        // Metrics take 2.5 s: the conditions run of t=0 waits for it (the
        // loops are sequential), and the next metrics run stays on the
        // `next += interval` grid.
        let mut recorder = Recorder::new(2500, 0);
        run_for(&mut recorder, &loops(Some(4), Some(4)), 9000).await;
        assert_eq!(
            rounded(&recorder.calls),
            [
                ("metrics", 0.0),
                ("conditions", 2.5),
                ("metrics", 4.0),
                ("conditions", 6.5),
                ("metrics", 8.0),
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_overrunning_run_restarts_the_grid_after_it_ends() {
        // A 7 s metrics run with a 3 s interval: `next += 3` is already in
        // the past, so the next run is due 3 s after the run ended.
        let mut recorder = Recorder::new(7000, 0);
        run_for(&mut recorder, &loops(Some(3), None), 18000).await;
        assert_eq!(
            rounded(&recorder.calls),
            [("metrics", 0.0), ("metrics", 10.0)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_sleep_is_at_least_200_ms_and_at_most_one_second() {
        // An interval of 1 s with runs of 0.9 s: the next run is due 0.1 s
        // after the run ends, but the loop sleeps at least 0.2 s.
        let mut recorder = Recorder::new(900, 0);
        run_for(&mut recorder, &loops(Some(1), None), 3000).await;
        assert_eq!(
            rounded(&recorder.calls),
            [("metrics", 0.0), ("metrics", 1.1), ("metrics", 2.2)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_disabled_loop_never_runs_and_a_huge_interval_runs_once() {
        let mut recorder = Recorder::new(0, 0);
        let mut huge = loops(None, Some(1));
        huge.condition_interval = BigInt::from(10).pow(30);
        run_for(&mut recorder, &huge, 5000).await;
        assert_eq!(rounded(&recorder.calls), [("conditions", 0.0)]);
    }

    #[tokio::test(start_paused = true)]
    async fn run_once_runs_metrics_without_the_due_filter_then_conditions() {
        let mut recorder = Recorder::new(0, 0);
        run_once(&mut recorder, &loops(Some(60), Some(60))).await;
        assert_eq!(
            rounded(&recorder.calls),
            [("metrics-all", 0.0), ("conditions", 0.0)]
        );
        let mut recorder = Recorder::new(0, 0);
        run_once(&mut recorder, &loops(None, Some(60))).await;
        assert_eq!(rounded(&recorder.calls), [("conditions", 0.0)]);
        let mut recorder = Recorder::new(0, 0);
        run_once(&mut recorder, &loops(Some(60), None)).await;
        assert_eq!(rounded(&recorder.calls), [("metrics-all", 0.0)]);
    }
}
