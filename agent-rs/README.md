# graphyard-agent (Rust)

A Rust port of the graphyard agent (`manage.py start_agent`, run by
`graphyard-agent.service`). The contract is [`PLAN.md`](PLAN.md); the Python
agent in `src/graphyard` stays the reference implementation and the rollback
target. Nothing runs this code in production yet: the deployment selects it
only through a systemd drop-in that later slices add (PLAN.md 8.3), and
deploys ship `agent-rs/` only as a release archive.

Status: slice S1 (scaffold): the workspace, the toolchain pin, the
cargo-deny policy, the CI `rust` job, the `just rs-*` recipes, the shared
model types (`graphyard_core::model`) and the fixture pipeline with its drift
test. The `graphyard-agent` binary is a placeholder that exits 1 until slice
S8. The slice table is PLAN.md section 12; the test coverage per slice is
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
                        settings, spec_config, normalisation, line protocol,
                        JSON path, mappings, condition evaluation (S2-S7)
    graphyard-store/    rusqlite store, schema contract, registry (S3a/S3b)
    graphyard-influx/   InfluxDB write and query clients (S4a/S4b)
    graphyard-collect/  HTTP layer and the five executors (S6/S7a/S7b)
    graphyard-agent/    the binary: CLI, scheduler, runs, signals (S8)
  fixtures/
    generate.py         fixture driver (`just rs-fixtures`)
    gen_*.py            builder modules, one or more per slice
    *.json              generated fixtures, embedded by the Rust tests
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
settings, and writes the JSON files. It re-executes itself with
`PYTHONHASHSEED=0`, removes `DJANGO_*`, `GRAPHYARD_*` and `INFLUX_*`
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
`crates/graphyard-core/src/fixture.rs`.
