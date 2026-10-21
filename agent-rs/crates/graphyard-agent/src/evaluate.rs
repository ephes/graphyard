//! `graphyard-agent evaluate-conditions [--condition-id N]` (PLAN.md 11.4,
//! slice S15a): a port of `manage.py evaluate_conditions`
//! (`evaluate_conditions.py:Command.handle`).
//!
//! One run of `evaluate_conditions_once(condition_id=...)` ([`run_conditions`]):
//! the enabled conditions (only the given id with `--condition-id`) ordered
//! by name, the per-condition writes and the `condition_evaluator`
//! heartbeat exactly as the agent's conditions run (PLAN.md 3.5). Then the
//! command's report: stdout `Condition evaluation completed (<total>).` and
//! exit 0, or stdout `Condition evaluation completed with failures
//! (<failed>/<total>).`, stderr `CommandError: <failed> condition
//! evaluation(s) failed.` and exit 1 (P1). A failure outside a condition
//! (load, failure update, heartbeat) exits 1 with one stderr line where
//! Python prints a traceback (P2). The database is opened and its schema
//! verified as at the agent's startup (D-2): a missing database exits 1 and
//! is never created.
//!
//! The command installs no signal handlers: SIGINT and SIGTERM end it by
//! their default action, as they end the Python command (SIGTERM by the
//! default action, SIGINT through the uncaught `KeyboardInterrupt`). SQLite's
//! transactions keep the database consistent either way.

use std::path::Path;
use std::process::ExitCode;

use graphyard_core::ConditionEvaluationRun;
use graphyard_core::logline::escape_log_value;
use graphyard_core::settings::Settings;
use graphyard_influx::QueryClient;

use crate::app::{SHUTDOWN_GRACE, open_verified, print_stderr, print_stdout};
use crate::cli::EvaluateConditionsArgs;
use crate::collectors::InfluxSamples;
use crate::runs::{SystemClock, run_conditions};

/// The command's report of a finished run: the stdout line and, when a
/// condition failed, the `CommandError` message (`handle`, P1).
pub fn report(run: &ConditionEvaluationRun) -> (String, Option<String>) {
    if run.failed > 0 {
        (
            format!(
                "Condition evaluation completed with failures ({}/{}).",
                run.failed, run.total
            ),
            Some(format!("{} condition evaluation(s) failed.", run.failed)),
        )
    } else {
        (
            format!("Condition evaluation completed ({}).", run.total),
            None,
        )
    }
}

/// Open and verify the database, run the evaluation, report; the exit code.
async fn evaluate(
    db: &Path,
    condition_id: Option<&num_bigint::BigInt>,
    settings: &Settings,
) -> ExitCode {
    let (store, _report) = match open_verified(db, settings).await {
        Ok(opened) => opened,
        Err(code) => return code,
    };
    let samples = InfluxSamples(QueryClient::new(settings));
    let run = run_conditions(
        &store,
        &samples,
        &SystemClock,
        &settings.condition_data_stale_warning_seconds,
        condition_id,
    )
    .await;
    // Every statement has completed; close the connection before reporting.
    store.close();
    match run {
        Ok(run) => {
            let (summary, failure) = report(&run);
            print_stdout(&summary);
            match failure {
                Some(message) => {
                    print_stderr(&format!("CommandError: {message}"));
                    ExitCode::FAILURE
                }
                None => ExitCode::SUCCESS,
            }
        }
        Err(err) => {
            print_stderr(&format!(
                "Condition evaluation failed: {}: {}",
                err.python_class(),
                escape_log_value(&err.to_string())
            ));
            ExitCode::FAILURE
        }
    }
}

/// The subcommand (the environment is already read, PLAN.md 3.1).
pub fn evaluate_conditions(args: &EvaluateConditionsArgs, settings: &Settings) -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            print_stderr(&format!("cannot start the runtime: {err}"));
            return ExitCode::FAILURE;
        }
    };
    let code = runtime.block_on(evaluate(&args.db, args.condition_id.as_ref(), settings));
    runtime.shutdown_timeout(SHUTDOWN_GRACE);
    code
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(total: u64, failed: u64) -> ConditionEvaluationRun {
        ConditionEvaluationRun { total, failed }
    }

    #[test]
    fn report_follows_handle() {
        assert_eq!(
            report(&run(3, 0)),
            ("Condition evaluation completed (3).".to_owned(), None)
        );
        assert_eq!(
            report(&run(0, 0)),
            ("Condition evaluation completed (0).".to_owned(), None)
        );
        assert_eq!(
            report(&run(3, 1)),
            (
                "Condition evaluation completed with failures (1/3).".to_owned(),
                Some("1 condition evaluation(s) failed.".to_owned())
            )
        );
    }
}
