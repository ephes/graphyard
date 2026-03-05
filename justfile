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
    cd {{OPS_CONTROL}} && just deploy graphyard {{HOST}}
