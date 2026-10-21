//! The command line (PLAN.md 3.1, `start_agent.py:Command.add_arguments`,
//! `handle`).
//!
//! The Python flags keep argparse's behaviour where clap offers it: unique
//! prefixes of long flags are accepted (`allow_abbrev`), a repeated flag
//! takes the last value, an interval is Python's `int()` of the argument
//! (whitespace, sign and `_` accepted) and may be negative so that the
//! validation, not the parser, rejects it. Bad flag syntax is clap's usage
//! error with exit 2 (D-7). The Rust-only flags are `--db`,
//! `--check-schema`, `--version` and `--influx-write-file`.
//!
//! The subcommand `evaluate-conditions` (PLAN.md 11.4, S15a) ports
//! `manage.py evaluate_conditions` (`evaluate_conditions.py:add_arguments`):
//! `--condition-id` is `type=int` like the intervals, plus the Rust-only
//! `--db`. The agent's flags and the subcommand exclude each other.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use graphyard_core::pycompat::py_int;
use graphyard_core::settings::Settings;
use num_bigint::BigInt;

/// `type=int`: Python's `int()` on the argument.
fn parse_int(text: &str) -> Result<BigInt, String> {
    py_int(text).map_err(|_| format!("invalid int value: {text:?}"))
}

#[derive(Clone, Debug, Parser)]
#[command(
    name = "graphyard-agent",
    about = "Run the long-lived graphyard agent loop (metric collection specs + condition evaluation)",
    disable_version_flag = true,
    args_override_self = true,
    infer_long_args = true,
    args_conflicts_with_subcommands = true,
    disable_help_subcommand = true
)]
pub struct Cli {
    /// A one-shot command instead of the agent loop.
    #[command(subcommand)]
    pub command: Option<Command>,
    /// Seconds between metric collection scheduler ticks (default:
    /// GRAPHYARD_METRIC_COLLECTION_INTERVAL_SECONDS)
    #[arg(long, value_parser = parse_int, allow_negative_numbers = true)]
    pub metrics_interval: Option<BigInt>,
    /// Seconds between condition evaluation runs (default:
    /// GRAPHYARD_CONDITION_EVAL_INTERVAL_SECONDS)
    #[arg(long, value_parser = parse_int, allow_negative_numbers = true)]
    pub condition_interval: Option<BigInt>,
    /// Disable metric collection loop
    #[arg(long)]
    pub disable_metrics: bool,
    /// Deprecated alias for --disable-metrics
    #[arg(long)]
    pub disable_ha: bool,
    /// Disable condition evaluation loop
    #[arg(long)]
    pub disable_conditions: bool,
    /// Run enabled loops once and exit
    #[arg(long)]
    pub run_once: bool,
    /// The SQLite database of the Django app
    #[arg(long, value_name = "PATH", default_value = "db.sqlite3")]
    pub db: PathBuf,
    /// Verify the database schema read-only, print one line and exit 0 or 1
    #[arg(long)]
    pub check_schema: bool,
    /// Print `graphyard-agent <version> (<commit>)` and exit
    #[arg(long)]
    pub version: bool,
    /// Shadow mode: append every InfluxDB write batch to PATH as line
    /// protocol instead of sending it (queries still go to InfluxDB)
    #[arg(long, value_name = "PATH")]
    pub influx_write_file: Option<PathBuf>,
}

/// The subcommands.
#[derive(Clone, Debug, Subcommand)]
pub enum Command {
    /// Evaluate enabled conditions and persist their status
    #[command(args_override_self = true, infer_long_args = true)]
    EvaluateConditions(EvaluateConditionsArgs),
}

/// `evaluate_conditions.py:Command.add_arguments` plus `--db`.
#[derive(Clone, Debug, Args)]
pub struct EvaluateConditionsArgs {
    /// Evaluate only one condition
    #[arg(long, value_parser = parse_int, allow_negative_numbers = true)]
    pub condition_id: Option<BigInt>,
    /// The SQLite database of the Django app
    #[arg(long, value_name = "PATH", default_value = "db.sqlite3")]
    pub db: PathBuf,
}

/// The validated loop settings of `handle`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Loops {
    pub run_metrics: bool,
    pub metrics_interval: BigInt,
    pub run_conditions: bool,
    pub condition_interval: BigInt,
    pub run_once: bool,
}

impl Cli {
    /// `handle`'s validation, in its order; the error is the `CommandError`
    /// message. A disabled loop's interval is not validated.
    ///
    /// # Errors
    /// The `CommandError` text.
    pub fn loops(&self, settings: &Settings) -> Result<Loops, &'static str> {
        let metrics_interval = self
            .metrics_interval
            .clone()
            .unwrap_or_else(|| settings.metric_collection_interval_seconds.clone());
        let condition_interval = self
            .condition_interval
            .clone()
            .unwrap_or_else(|| settings.condition_eval_interval_seconds.clone());
        let run_metrics = !(self.disable_metrics || self.disable_ha);
        let run_conditions = !self.disable_conditions;
        let zero = BigInt::from(0);
        if !run_metrics && !run_conditions {
            return Err("At least one loop must be enabled");
        }
        if run_metrics && metrics_interval <= zero {
            return Err("--metrics-interval must be greater than 0");
        }
        if run_conditions && condition_interval <= zero {
            return Err("--condition-interval must be greater than 0");
        }
        Ok(Loops {
            run_metrics,
            metrics_interval,
            run_conditions,
            condition_interval,
            run_once: self.run_once,
        })
    }
}

/// Python's `repr(bool)`.
fn py_bool(value: bool) -> &'static str {
    if value { "True" } else { "False" }
}

impl Loops {
    /// The start line (stdout, P1).
    pub fn start_line(&self) -> String {
        format!(
            "Starting graphyard-agent (metrics={}@{}s, conditions={}@{}s)",
            py_bool(self.run_metrics),
            self.metrics_interval,
            py_bool(self.run_conditions),
            self.condition_interval
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(vars: &[(&str, &str)]) -> Settings {
        Settings::from_lookup(|name| {
            vars.iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_owned())
        })
        .unwrap()
    }

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("graphyard-agent").chain(args.iter().copied()))
    }

    fn loops(args: &[&str]) -> Result<Loops, &'static str> {
        parse(args).unwrap().loops(&settings(&[]))
    }

    #[test]
    fn defaults_come_from_the_settings() {
        let cli = parse(&[]).unwrap();
        let env = settings(&[
            ("GRAPHYARD_METRIC_COLLECTION_INTERVAL_SECONDS", "30"),
            ("GRAPHYARD_CONDITION_EVAL_INTERVAL_SECONDS", "45"),
        ]);
        let loops = cli.loops(&env).unwrap();
        assert_eq!(
            loops.start_line(),
            "Starting graphyard-agent (metrics=True@30s, conditions=True@45s)"
        );
        assert!(!loops.run_once);
        assert_eq!(cli.db, PathBuf::from("db.sqlite3"));
        assert_eq!(
            loops_with(&[], &[]).start_line(),
            "Starting graphyard-agent (metrics=True@60s, conditions=True@60s)"
        );
    }

    fn loops_with(args: &[&str], vars: &[(&str, &str)]) -> Loops {
        parse(args).unwrap().loops(&settings(vars)).unwrap()
    }

    #[test]
    fn validation_order_and_messages_follow_handle() {
        assert_eq!(
            loops(&["--disable-metrics", "--disable-conditions", "--run-once"]),
            Err("At least one loop must be enabled")
        );
        assert_eq!(
            loops(&[
                "--disable-ha",
                "--disable-conditions",
                "--metrics-interval",
                "0"
            ]),
            Err("At least one loop must be enabled")
        );
        assert_eq!(
            loops(&["--metrics-interval", "0", "--condition-interval", "0"]),
            Err("--metrics-interval must be greater than 0")
        );
        assert_eq!(
            loops(&["--condition-interval", "-5"]),
            Err("--condition-interval must be greater than 0")
        );
        // A disabled loop's interval is not validated.
        let only_conditions = loops_with(&["--disable-ha", "--metrics-interval=-1"], &[]);
        assert_eq!(
            only_conditions.start_line(),
            "Starting graphyard-agent (metrics=False@-1s, conditions=True@60s)"
        );
        let only_metrics = loops_with(
            &[
                "--disable-conditions",
                "--condition-interval",
                "0",
                "--run-once",
            ],
            &[],
        );
        assert!(only_metrics.run_once);
        assert_eq!(
            only_metrics.start_line(),
            "Starting graphyard-agent (metrics=True@60s, conditions=False@0s)"
        );
    }

    #[test]
    fn intervals_are_python_ints() {
        let loops = loops_with(
            &["--metrics-interval", " 1_0 ", "--condition-interval", "+7"],
            &[],
        );
        assert_eq!(loops.metrics_interval, BigInt::from(10));
        assert_eq!(loops.condition_interval, BigInt::from(7));
        let huge = "9".repeat(30);
        let loops = loops_with(&["--metrics-interval", &huge], &[]);
        assert_eq!(loops.metrics_interval.to_string(), huge);
        for bad in ["5s", "1.5", "", "1__0"] {
            let err = parse(&["--metrics-interval", bad]).unwrap_err();
            assert_eq!(err.exit_code(), 2, "{bad:?}");
        }
    }

    #[test]
    fn argparse_conveniences() {
        // Unique prefixes and repeated flags (last one wins).
        let loops = loops_with(
            &[
                "--metrics-int",
                "5",
                "--metrics-interval",
                "6",
                "--run-on",
                "--run-once",
            ],
            &[],
        );
        assert_eq!(loops.metrics_interval, BigInt::from(6));
        assert!(loops.run_once);
        // Ambiguous prefix, unknown flag, missing value: usage errors (2).
        for args in [
            &["--disable"][..],
            &["--verbosity", "2"],
            &["--metrics-interval"],
            &["extra"],
        ] {
            assert_eq!(parse(args).unwrap_err().exit_code(), 2, "{args:?}");
        }
    }

    #[test]
    fn rust_only_flags() {
        let cli = parse(&[
            "--db",
            "/var/db.sqlite3",
            "--check-schema",
            "--influx-write-file",
            "/tmp/writes.lp",
            "--version",
        ])
        .unwrap();
        assert_eq!(cli.db, PathBuf::from("/var/db.sqlite3"));
        assert!(cli.check_schema && cli.version);
        assert_eq!(cli.influx_write_file, Some(PathBuf::from("/tmp/writes.lp")));
        assert!(cli.command.is_none());
    }

    fn evaluate(args: &[&str]) -> Result<EvaluateConditionsArgs, clap::Error> {
        let cli = parse(&[&["evaluate-conditions"][..], args].concat())?;
        let Some(Command::EvaluateConditions(args)) = cli.command else {
            panic!("no subcommand parsed");
        };
        Ok(args)
    }

    #[test]
    fn evaluate_conditions_takes_an_optional_python_int_id_and_the_db() {
        let args = evaluate(&[]).unwrap();
        assert_eq!(args.condition_id, None);
        assert_eq!(args.db, PathBuf::from("db.sqlite3"));
        let args = evaluate(&["--condition-id", " 7 ", "--db", "/var/db.sqlite3"]).unwrap();
        assert_eq!(args.condition_id, Some(BigInt::from(7)));
        assert_eq!(args.db, PathBuf::from("/var/db.sqlite3"));
        // argparse: a negative number is a value, prefixes are accepted and
        // the last of repeated flags wins; the id may exceed 64 bits.
        let args = evaluate(&["--condition", "-3"]).unwrap();
        assert_eq!(args.condition_id, Some(BigInt::from(-3)));
        let huge = "9".repeat(30);
        let args = evaluate(&["--condition-id=1", "--condition-id", &huge]).unwrap();
        assert_eq!(args.condition_id.unwrap().to_string(), huge);
        for bad in [
            &["--condition-id", "x"][..],
            &["--condition-id", "1.0"],
            &["--condition-id"],
            &["--run-once"],
            &["--no-color"],
            &["7"],
        ] {
            assert_eq!(evaluate(bad).unwrap_err().exit_code(), 2, "{bad:?}");
        }
    }

    #[test]
    fn the_agent_flags_and_the_subcommand_exclude_each_other() {
        for args in [
            &["--run-once", "evaluate-conditions"][..],
            &["--db", "x", "evaluate-conditions"],
            &["--version", "evaluate-conditions"],
            &["evaluate_conditions"],
            &["evaluate"],
            &["help"],
        ] {
            assert_eq!(parse(args).unwrap_err().exit_code(), 2, "{args:?}");
        }
    }
}
