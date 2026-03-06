# Justfile for graphyard development

# Path to your local ops-control clone
OPS_CONTROL := env_var_or_default("OPS_CONTROL", "/Users/jochen/workspaces/ws-ops-misc/ops-control")
HOST := env_var_or_default("HOST", "macmini")

default:
    @just --list

install:
    uv sync

lint:
    uvx pre-commit run --all-files

typecheck:
    uv run mypy

test:
    uv run pytest

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
