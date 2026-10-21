# Justfile for graphyard development

# Path to your local ops-control clone
OPS_CONTROL := env_var_or_default("OPS_CONTROL", "/Users/jochen/workspaces/ws-ops-misc/ops-control")
HOST := env_var_or_default("HOST", "macmini")

default:
    @just --list

install:
    uv sync

# Lint (Python pre-commit hooks, then the Rust agent)
lint:
    uvx pre-commit run --all-files
    just rs-lint

# Type check (mypy, then cargo check of the Rust agent)
typecheck:
    uv run mypy
    uv run mypy --check-untyped-defs scripts/inventory_transport_probe.py scripts/inventory_sender_probe.py
    cd agent-rs && cargo check --workspace --all-targets --locked

# Run all tests (Python suite incl. the Rust fixture drift test, then the Rust agent)
test:
    uv run pytest
    just rs-test

# Opt-in real Vector/Graphyard transport test; fresh DB and loopback listeners only.
[positional-arguments]
test-inventory-transport *ARGS:
    uv run python scripts/inventory_transport_probe.py "$@"

manage *ARGS:
    cd src/django && uv run python manage.py {{ARGS}}

dev:
    uvx honcho start

# Deploy via ops-control shorthand.
deploy:
    #!/usr/bin/env bash
    set -euo pipefail
    graphyard_root="$(git rev-parse --show-toplevel)"
    default_projects_root="$(dirname "$graphyard_root")"
    if [[ -n "${PROJECTS_ROOT:-}" && -d "${PROJECTS_ROOT}/graphyard" ]]; then
        projects_root="$PROJECTS_ROOT"
    else
        projects_root="$default_projects_root"
    fi
    cd {{OPS_CONTROL}}
    PROJECTS_ROOT="$projects_root" just deploy graphyard {{HOST}}

# Opt-in direct HTTPS sender contract; no Vector or production database involved.
[positional-arguments]
test-inventory-sender *ARGS:
    uv run python scripts/inventory_sender_probe.py "$@"

# ========= Rust agent (agent-rs/, see agent-rs/README.md) =========

# Rust agent tests
rs-test:
    cd agent-rs && cargo test --workspace --locked

# Rust agent lint: rustfmt check and clippy with warnings as errors
rs-lint:
    cd agent-rs && cargo fmt --all --check
    cd agent-rs && cargo clippy --workspace --all-targets --locked -- -D warnings

# Format the Rust agent
rs-fmt:
    cd agent-rs && cargo fmt --all

# Rust agent supply-chain check (licenses, advisories, sources; needs cargo-deny, see agent-rs/README.md)
rs-deny:
    cd agent-rs && cargo deny --locked check

# Regenerate the Rust agent fixtures from the Python implementation (agent-rs/PLAN.md 9.2)
rs-fixtures:
    uv run python agent-rs/fixtures/generate.py

# Offline parity harness: Python and Rust agents on the same corpus and fakes (agent-rs/PLAN.md 9.4)
rs-parity *ARGS:
    cd agent-rs && cargo build --locked
    uv run python agent-rs/parity/run_parity.py --rust-binary agent-rs/target/debug/graphyard-agent {{ARGS}}

# Release build of the Rust agent for this host
rs-build:
    cd agent-rs && cargo build --release --locked

# Release build for macmini (x86_64 glibc) in Docker; output in agent-rs/target/docker-linux-amd64
rs-build-linux:
    docker run --rm --platform linux/amd64 \
      -v "{{justfile_directory()}}/agent-rs:/work" -w /work \
      -v graphyard-agent-rs-cargo-registry:/usr/local/cargo/registry \
      -e CARGO_TARGET_DIR=/work/target/docker-linux-amd64 \
      -e GRAPHYARD_AGENT_GIT_COMMIT="$(git -C "{{justfile_directory()}}" rev-parse HEAD)" \
      rust:1.99.0-bookworm \
      cargo build --release --locked --target x86_64-unknown-linux-gnu
