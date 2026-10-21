//! Startup, the run reports and shutdown (PLAN.md 3.1, 3.6, 3.7, 4.1, D-1,
//! D-2, D-8).
//!
//! One `current_thread` tokio runtime; the scheduler runs in the main task
//! and a signal future beside it. On SIGTERM or SIGINT the main task is
//! dropped (it stops at its next await point; an SQLite statement that was
//! sent completes on the DB thread), `main` waits for the DB thread to
//! finish and close the connection, prints the SIGINT line, and ends the
//! runtime with a 500 ms grace, abandoning `getaddrinfo` calls stuck in the
//! blocking pool. Exit 0 either way.

use std::future::Future;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser as _;
use num_bigint::BigInt;

use graphyard_collect::http::HttpContext;
use graphyard_core::logline::escape_log_value;
use graphyard_core::settings::Settings;
use graphyard_influx::{InfluxWriter, QueryClient};
use graphyard_store::{Store, check_schema};

use crate::cli::{Cli, Command, Loops};
use crate::collectors::{AgentSink, Collectors, InfluxSamples};
use crate::runs::{
    Clock, ConditionSamples, SpecExecutors, SystemClock, run_conditions, run_metric_specs,
};
use crate::scheduler::{self, Runner};

/// `--version` text after the program name: the package version and the
/// git commit `build.rs` embeds.
pub const VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("GRAPHYARD_AGENT_GIT_COMMIT"),
    ")"
);

/// The stdout line of a SIGINT (P1).
pub const INTERRUPTED_LINE: &str = "graphyard-agent interrupted, exiting";

/// The runtime's grace for blocking work at shutdown (PLAN.md 4.1).
pub(crate) const SHUTDOWN_GRACE: Duration = Duration::from_millis(500);

/// One stdout line, flushed at once (D-8); a closed stdout is ignored.
pub fn print_stdout(line: &str) {
    let mut stdout = std::io::stdout().lock();
    let _ = writeln!(stdout, "{line}");
    let _ = stdout.flush();
}

/// One stderr line (the command's `self.stderr.write`).
pub fn print_stderr(line: &str) {
    let mut stderr = std::io::stderr().lock();
    let _ = writeln!(stderr, "{line}");
    let _ = stderr.flush();
}

/// The signal that ended the agent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shutdown {
    /// SIGINT: Python's `KeyboardInterrupt` handler.
    Interrupt,
    /// SIGTERM.
    Terminate,
}

/// Register the production signal handlers (SIGTERM, SIGINT) and return
/// the future that resolves on the first signal. [`run_agent`] calls it
/// inside the runtime before it opens the database, so a signal during the
/// startup is already handled.
///
/// # Errors
/// When a handler cannot be registered.
#[cfg(unix)]
pub fn signals() -> std::io::Result<impl Future<Output = Shutdown>> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    Ok(async move {
        tokio::select! {
            _ = terminate.recv() => Shutdown::Terminate,
            _ = interrupt.recv() => Shutdown::Interrupt,
        }
    })
}

/// The production signal future: Ctrl-C only (development builds).
///
/// # Errors
/// Never; the handler is registered when the future is first polled.
#[cfg(not(unix))]
pub fn signals() -> std::io::Result<impl Future<Output = Shutdown>> {
    Ok(async {
        let _ = tokio::signal::ctrl_c().await;
        Shutdown::Interrupt
    })
}

/// The command's runs with their report lines (`_run_metric_specs`,
/// `_run_condition_eval`): the summary on stdout, or an error record and
/// the stderr line; a failed run never stops the loop.
pub struct Agent<E, S, C> {
    pub store: Store,
    pub executors: E,
    pub samples: S,
    pub clock: C,
    /// `GRAPHYARD_CONDITION_DATA_STALE_WARNING_SECONDS`.
    pub stale_warning_seconds: BigInt,
}

impl<E: SpecExecutors, S: ConditionSamples, C: Clock> Runner for Agent<E, S, C> {
    async fn metrics(&mut self, due_only: bool) {
        match run_metric_specs(&self.store, &self.executors, &self.clock, due_only).await {
            Ok(run) => print_stdout(&format!(
                "Metric specs: total={} failed={} warning={} ingested={} skipped={}",
                run.total, run.failed, run.warning, run.ingested, run.skipped
            )),
            Err(err) => {
                // `logger.exception` ends with the exception line; one line
                // here (P2), then the command's stderr line (P1).
                log::error!(
                    "Metric collection failed: {}: {}",
                    err.python_class(),
                    escape_log_value(&err.to_string())
                );
                print_stderr(&format!("Metric collection failed: {err}"));
            }
        }
    }

    async fn conditions(&mut self) {
        let run = run_conditions(
            &self.store,
            &self.samples,
            &self.clock,
            &self.stale_warning_seconds,
            None,
        )
        .await;
        match run {
            Ok(run) => print_stdout(&format!(
                "Condition eval: evaluated={} failed={}",
                run.total, run.failed
            )),
            Err(err) => {
                log::error!(
                    "Condition evaluation failed: {}: {}",
                    err.python_class(),
                    escape_log_value(&err.to_string())
                );
                print_stderr(&format!("Condition eval failed: {err}"));
            }
        }
    }
}

/// What [`run_agent`] needs besides the signal future.
pub struct AgentConfig {
    pub db: PathBuf,
    pub loops: Loops,
    pub settings: Settings,
    pub http: HttpContext,
    pub writer: InfluxWriter,
}

pub(crate) fn database_error(db: &Path, err: impl std::fmt::Display) -> ExitCode {
    print_stderr(&format!("cannot open database {}: {err}", db.display()));
    ExitCode::FAILURE
}

/// `--check-schema` (PLAN.md 5.1): read-only, one line, exit 0 or 1.
pub fn check_schema_only(db: &Path, settings: &Settings) -> ExitCode {
    match check_schema(db, settings) {
        Ok(report) if report.is_ok() => {
            print_stdout(&report.to_string());
            ExitCode::SUCCESS
        }
        Ok(report) => {
            print_stderr(&report.to_string());
            ExitCode::FAILURE
        }
        Err(err) => database_error(db, err),
    }
}

fn describe_loop(enabled: bool, interval: &BigInt) -> String {
    if enabled {
        format!("every {interval}s")
    } else {
        "disabled".to_owned()
    }
}

/// Open the database off the runtime thread (a locked database can hold
/// the open up to the busy timeout) and verify the schema (D-2); the error
/// is the exit code after the reported failure. The passing schema report
/// is returned for the startup log.
pub(crate) async fn open_verified(
    db: &Path,
    settings: &Settings,
) -> Result<(Store, String), ExitCode> {
    let (path, open_settings) = (db.to_path_buf(), settings.clone());
    let opened = tokio::task::spawn_blocking(move || Store::open(&path, &open_settings)).await;
    let store = match opened {
        Ok(Ok(store)) => store,
        Ok(Err(err)) => return Err(database_error(db, err)),
        Err(err) => return Err(database_error(db, err)),
    };
    match store.verify_schema().await {
        Ok(report) if report.is_ok() => Ok((store, report.to_string())),
        Ok(report) => {
            print_stderr(&report.to_string());
            store.close();
            Err(ExitCode::FAILURE)
        }
        Err(err) => {
            store.close();
            Err(database_error(db, err))
        }
    }
}

/// [`open_verified`] plus the agent's three startup log lines.
async fn open_and_verify(db: &Path, settings: &Settings, loops: &Loops) -> Result<Store, ExitCode> {
    let (store, report) = open_verified(db, settings).await?;
    log::info!("graphyard-agent {VERSION}");
    log::info!("{report}");
    log::info!(
        "metrics {}, conditions {}{}",
        describe_loop(loops.run_metrics, &loops.metrics_interval),
        describe_loop(loops.run_conditions, &loops.condition_interval),
        if loops.run_once { ", run once" } else { "" }
    );
    Ok(store)
}

/// End the runtime with the grace for blocking work and report the signal.
fn finish(runtime: tokio::runtime::Runtime, signal: Option<Shutdown>, code: ExitCode) -> ExitCode {
    if signal == Some(Shutdown::Interrupt) {
        print_stdout(INTERRUPTED_LINE);
    }
    runtime.shutdown_timeout(SHUTDOWN_GRACE);
    code
}

/// Register the signal handlers (`shutdown`, called inside the runtime),
/// open the database, verify the schema (D-2) and run the loops until they
/// end (`--run-once`) or a signal arrives. A signal during the startup
/// ends the agent the same way: the open or the schema read in flight is
/// abandoned (both only read or set pragmas), exit 0.
pub fn run_agent<F, Fut>(config: AgentConfig, shutdown: F) -> ExitCode
where
    F: FnOnce() -> std::io::Result<Fut>,
    Fut: Future<Output = Shutdown>,
{
    let AgentConfig {
        db,
        loops,
        settings,
        http,
        writer,
    } = config;
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
    let registered = {
        let _context = runtime.enter();
        shutdown()
    };
    let mut shutdown = match registered {
        Ok(shutdown) => Box::pin(shutdown),
        Err(err) => {
            print_stderr(&format!("cannot register the signal handlers: {err}"));
            return ExitCode::FAILURE;
        }
    };

    let started = runtime.block_on(async {
        tokio::select! {
            biased;
            signal = &mut shutdown => Err(signal),
            store = open_and_verify(&db, &settings, &loops) => Ok(store),
        }
    });
    let store = match started {
        Ok(Ok(store)) => store,
        Ok(Err(code)) => return finish(runtime, None, code),
        Err(signal) => return finish(runtime, Some(signal), ExitCode::SUCCESS),
    };

    let settings = Arc::new(settings);
    let mut agent = Agent {
        store: store.clone(),
        executors: Collectors {
            http,
            sink: AgentSink {
                settings: settings.clone(),
                writer: Arc::new(writer),
                store: store.clone(),
            },
        },
        samples: InfluxSamples(QueryClient::new(&settings)),
        clock: SystemClock,
        stale_warning_seconds: settings.condition_data_stale_warning_seconds.clone(),
    };
    let ended = runtime.block_on(async move {
        let work = async {
            if loops.run_once {
                scheduler::run_once(&mut agent, &loops).await;
            } else {
                scheduler::run_forever(&mut agent, &loops).await;
            }
        };
        tokio::select! {
            biased;
            signal = &mut shutdown => Some(signal),
            () = work => None,
        }
    });
    // The main task is gone: wait for the statement the DB thread runs.
    store.close();
    finish(runtime, ended, ExitCode::SUCCESS)
}

/// The binary: parse, validate, start (PLAN.md 3.1); or the one-shot
/// `evaluate-conditions` (PLAN.md 11.4).
pub fn main_entry() -> ExitCode {
    let parsed = Cli::try_parse();
    if let Ok(cli) = &parsed
        && cli.version
    {
        print_stdout(&format!("graphyard-agent {VERSION}"));
        return ExitCode::SUCCESS;
    }
    crate::logging::install();
    // Python reads the settings while importing them, before argparse.
    let settings = match Settings::from_process_env() {
        Ok(settings) => settings,
        Err(err) => {
            print_stderr(&err.to_string());
            return ExitCode::FAILURE;
        }
    };
    let cli = match parsed {
        Ok(cli) => cli,
        Err(err) => err.exit(),
    };
    if let Some(Command::EvaluateConditions(args)) = cli.command {
        return crate::evaluate::evaluate_conditions(&args, &settings);
    }
    if cli.check_schema {
        return check_schema_only(&cli.db, &settings);
    }
    let loops = match cli.loops(&settings) {
        Ok(loops) => loops,
        Err(message) => {
            print_stderr(&format!("CommandError: {message}"));
            return ExitCode::FAILURE;
        }
    };
    print_stdout(&loops.start_line());
    let writer = match &cli.influx_write_file {
        Some(path) => InfluxWriter::to_file(path).map_err(|err| {
            format!(
                "cannot open the --influx-write-file {}: {err}",
                path.display()
            )
        }),
        None => {
            InfluxWriter::new().map_err(|err| format!("cannot build the InfluxDB client: {err}"))
        }
    };
    let writer = match writer {
        Ok(writer) => writer,
        Err(message) => {
            print_stderr(&message);
            return ExitCode::FAILURE;
        }
    };
    let config = AgentConfig {
        db: cli.db,
        loops,
        settings,
        http: HttpContext::new(),
        writer,
    };
    run_agent(config, signals)
}
