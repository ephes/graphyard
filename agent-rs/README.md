# graphyard-agent (Rust)

A Rust port of the graphyard agent (`manage.py start_agent`, run by
`graphyard-agent.service`). The contract is [`PLAN.md`](PLAN.md); the Python
agent in `src/graphyard` stays the reference implementation and the rollback
target. Nothing runs this code in production yet: the deployment selects it
only through a systemd drop-in that later slices add (PLAN.md 8.3), and
deploys ship `agent-rs/` only as a release archive.

Status: slices S1 (scaffold), S2, S3a, S3b, S4a, S5, S6 and S10a. S1: the workspace, the toolchain pin,
the cargo-deny policy, the CI `rust` job, the `just rs-*` recipes, the shared
model types (`graphyard_core::model`) and the fixture pipeline with its drift
test. S2: the CPython compatibility helpers (`graphyard_core::pycompat`), the
environment settings with django-environ semantics and the SQLite pragma
values (`graphyard_core::settings`), and the typed spec config knobs
(`graphyard_core::spec_config`), each pinned by its fixture. S4a: the metric
point normalisation (`graphyard_core::normalize`), the line protocol of
`influxdb-client` 1.50.0 and the pure part of `write_points`
(`graphyard_core::line_protocol`, log records as values in
`graphyard_core::logline`), and the InfluxDB write client with the lazy
configuration check (`graphyard_influx::InfluxWriter`,
`graphyard_influx::config`), pinned by `normalize.json` and
`line_protocol.json`; the write tests replay every fixture case against fake
servers. S5: the
condition evaluation (`graphyard_core::conditions`: `evaluate_condition`
over a `SampleSource`, `evaluate_samples` on samples already read), pinned
by `conditions.json`. The
`graphyard-agent` binary is a placeholder that exits 1 until slice S8; it
answers only `--version`.

Slice S4b adds the condition queries in `graphyard-influx` (PLAN.md 6.5):
the lookback window of a condition, the v2 Flux text and request, the
decoding of InfluxDB's annotated CSV, the v3 SQL request with its byte cap
and deadline, and the `auto` fallback, plus `datetime.fromisoformat`,
`base64.b64decode` and `int / int` in `graphyard_core::pycompat`. Pinned by
`flux.json`, `sql_v3.json` and `isoformat.json`; the end-to-end cases run
against a loopback fake InfluxDB that replays the responses the fixture
generator served to the Python code.

Slice S10a adds the release pipeline (PLAN.md 8.2): the workflow
`.github/workflows/agent-rs-release.yml`, the checks in
`scripts/release_checks.py` and the commit stamp of `--version`
(`crates/graphyard-agent/build.rs`); see [Releases](#releases).

Slice S6 adds the collectors' HTTP layer in `graphyard-collect::http`
(PLAN.md 6.1, 6.2): httpx 0.28.1 as `services.py` configures it, on hyper's
HTTP/1.1 connection API. One `HttpClient` per spec execution with httpx's
request headers, auth and a cookie jar; connections as httpcore opens them
(`getaddrinfo` through libc on the blocking pool, every address in turn, the
timeout per connect, TLS handshake and socket read/write); httpx's URL model
(`HttpxUrl`: `urlparse`, the redirect join, request target and `Host`), its
`raise_for_status` texts, decoders and exception classes; the total deadline
with the abandoned-lookup cap (`run_with_deadline`), the JSON reader, the
page-probe redirect loop and automatic redirects. Pinned by
`http_errors.json` and fake-server tests; the executors follow in S7a/S7b.

Slice S3a adds the store core in `graphyard-store` (PLAN.md 5): the DB thread
owning the connection, Django's connection setup and the graphyard pragmas,
the schema contract and `check_schema`, Django's datetime column conversion,
spec and condition loads and updates, and the heartbeat upsert with its lock
retry.

Slice S3b adds the registry touch (`Store::touch_registry`, PLAN.md 5.5): the
batched `touch_registry_from_points` of graphyard `4d5baf1` without a bound
subject, which reads the registry rows once per call and writes only missing,
changed or stale (60 s) rows in one transaction, pinned by the
`registry_cases` of `store_ops.json` including Django's statement sequence.

The slice table is PLAN.md section 12; the test coverage per slice is
[`TESTMAP.md`](TESTMAP.md).

## Layout

```
agent-rs/
  Cargo.toml            workspace; release profile (thin LTO, stripped)
  Cargo.lock            committed; every build uses --locked
  rust-toolchain.toml   pinned toolchain 1.99.0 (also the MSRV)
  deny.toml             cargo-deny policy (licenses, advisories, sources)
  PLAN.md               the contract
  TESTMAP.md            Python test -> Rust counterpart, per slice
  crates/
    graphyard-core/     pure logic, no I/O: model types (S1); pycompat,
                        settings, spec_config (S2); normalize, line_protocol,
                        logline (S4a); conditions (S5); JSON path, mappings
                        (S7a, S7b)
    graphyard-store/    rusqlite store: DB thread, schema contract, datetime
                        codec, specs, conditions, heartbeats (S3a); registry
                        (S3b)
    graphyard-influx/   InfluxDB write client and config check (S4a),
                        query clients (S4b)
    graphyard-collect/  HTTP layer (S6: `http`) and the five executors
                        (S7a/S7b)
    graphyard-agent/    the binary: CLI, scheduler, runs, signals (S8)
  fixtures/
    generate.py         fixture driver (`just rs-fixtures`)
    gen_*.py            builder modules, one or more per slice
    *.json              generated fixtures, embedded by the Rust tests
    schema/             the Django schema contract (gen_schema.py): schema.sql,
                        from which every store test builds its database, and
                        agent_tables.json
  scripts/
    release_checks.py   release workflow checks (version, GLIBC floor, smoke, pack)
```

## Requirements

- The pinned toolchain via rustup: `rustup toolchain install` in `agent-rs/`
  reads `rust-toolchain.toml`. A `cargo` that is not the rustup proxy (for
  example Homebrew's) ignores the pin; put `~/.cargo/bin` first in `PATH`.
- `cargo-deny` 0.20.2 for `just rs-deny` (CI installs the pinned release
  and checks its sha256).
- `uv` for the fixture generator (the Python project's environment).
- Docker only for `just rs-build-linux`.

## Commands

From the repository root:

| Recipe | What it does |
| --- | --- |
| `just rs-test` | `cargo test --workspace --locked` |
| `just rs-lint` | `cargo fmt --check` and `cargo clippy -D warnings` |
| `just rs-fmt` | format the workspace |
| `just rs-deny` | `cargo deny --locked check` |
| `just rs-fixtures` | regenerate every fixture from the Python code |
| `just rs-build` | release build for this host |
| `just rs-build-linux` | release build for macmini (`x86_64-unknown-linux-gnu`, glibc 2.36 in `rust:1.99.0-bookworm`), output in `target/docker-linux-amd64` |

`just test`, `just lint` and `just typecheck` run the Python checks and then
the Rust ones. The parity harness recipe `rs-parity` arrives with slice S9a.

## Fixtures

The Python code is the oracle (PLAN.md 9.2). `fixtures/generate.py`
discovers every `fixtures/gen_*.py` module, calls the builders of its
`GENERATORS` mapping (fixture file name → function) under the Django test
settings, and writes the files (JSON, and `schema/schema.sql` as text). It
re-executes itself with `PYTHONHASHSEED=0`, removes `DJANGO_*`, `GRAPHYARD_*` and `INFLUX_*`
variables and keeps django-environ from reading `src/django/.env`, so a
developer environment cannot change a fixture. Files are rendered with
`indent=1`, ASCII escapes, the key order the builder wrote, and no NaN or
infinity (builders encode non-finite floats explicitly).

```bash
just rs-fixtures                                        # rewrite all fixtures
uv run python agent-rs/fixtures/generate.py --check     # exit 1 on drift
uv run python agent-rs/fixtures/generate.py --list      # fixture names
```

`tests/test_agent_rs_fixtures.py` (part of `uv run pytest`) runs `--check`
and fails on a stale, missing or orphaned fixture, so a Python change that
alters pinned behaviour cannot merge without regenerated fixtures and the
matching Rust change. A slice adds a fixture by adding a builder module (or
a `GENERATORS` entry in its existing one), running `just rs-fixtures` and
loading the file in Rust with `include_str!`.

Rust tests parse fixtures with `serde_json` (`preserve_order`,
`arbitrary_precision`) through the helpers in
`crates/graphyard-core/src/fixture.rs` (the store tests through
`crates/graphyard-store/tests/common`).

## Schema contract

The agent runs no DDL; Django's migrations own the schema (PLAN.md 5.1).
`fixtures/gen_schema.py` migrates a temporary database and writes
`fixtures/schema/schema.sql` and `fixtures/schema/agent_tables.json` (the
columns of the six agent tables and the minimum graphyard migration, the last
one whose operations touch an agent model). `tests/test_agent_rs_schema_contract.py`
fails when a migration changes them; then run `just rs-fixtures`, update
`REQUIRED_COLUMNS` / `MIN_GRAPHYARD_MIGRATION` in
`crates/graphyard-store/src/schema.rs` (its tests compare them with
`agent_tables.json`) and the store code that uses the changed columns.

## Releases

`.github/workflows/agent-rs-release.yml` builds and publishes the binary
(PLAN.md 8.1, 8.2). Deploys install only such a release archive, pinned by
version and sha256; nothing builds on the host.

- **Release**: push a tag `agent-rs-v<version>` on a commit of `main`, where
  `<version>` equals the `graphyard-agent` crate version (the workspace
  `version` in `Cargo.toml`, semver without build metadata). A version with
  a pre-release part (`0.2.0-rc.1`) becomes a GitHub pre-release.
- **Dry build**: run the workflow by hand (`gh workflow run
  agent-rs-release.yml --ref <branch>`, the workflow file must be on the
  default branch); it runs every check and uploads the archive as a workflow
  artifact, without a tag check and without a release.

The `build` job runs on `ubuntu-22.04` (glibc 2.35, the floor below
macmini's 2.39) without a cargo cache: `cargo fmt --check`, clippy, `cargo
test --locked`, `cargo build --release --locked --target
x86_64-unknown-linux-gnu`, then `scripts/release_checks.py`:

| Check | Rule |
| --- | --- |
| `version` | the crate version from `cargo metadata`; on a tag push the tag must be `agent-rs-v<that version>` |
| `glibc-floor` | `objdump -T`/`-p`: no `GLIBC_*` symbol version above 2.35, `NEEDED` only glibc and libgcc (`libc`, `libm`, `libgcc_s`, `ld-linux-x86-64`, ...) |
| `smoke` | `graphyard-agent --version` prints exactly `graphyard-agent <version> (<workflow commit>)` and exits 0 |
| `pack` | deterministic `graphyard-agent-<version>-x86_64-unknown-linux-gnu.tar.gz` (`graphyard-agent` 0755 and `LICENSE` 0644 at the root, root:root, commit time as mtime) and its `.sha256` in `sha256sum` format |

The `fixtures` job runs the fixture drift test and the release check tests
(`tests/test_agent_rs_fixtures.py`, `tests/test_agent_rs_release_checks.py`)
with the locked Python environment. The `release` job (the only one with
`contents: write`) needs both, verifies the checksum again and creates the
GitHub release with the archive and its `.sha256` file; the job summary and
the release notes name the commit and the sha256 the deployment pins.

`--version` names the git commit `build.rs` embeds: the
`GRAPHYARD_AGENT_GIT_COMMIT` environment variable when set (the workflow sets
it to its commit, `just rs-build-linux` to the checkout's `HEAD` because the
container sees no `.git`), else `git rev-parse HEAD`, else `unknown`; a value
that is not 7 to 40 lowercase hex digits fails the build.
