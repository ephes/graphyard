# Justfile for graphyard development

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
