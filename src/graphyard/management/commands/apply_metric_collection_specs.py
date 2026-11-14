from __future__ import annotations

import io
import json
import os
import sqlite3
import stat
from pathlib import Path
from typing import Any

import ijson

from django.core.exceptions import ValidationError
from django.core.management.base import BaseCommand, CommandError
from django.db import IntegrityError, OperationalError, transaction
from django.utils import timezone

from graphyard.models import MetricCollectionSpec, MetricCollectionSpecType


MAX_SPEC_FILE_BYTES = 2 * 1024 * 1024
MAX_SPEC_FILE_DEPTH = 64
MAX_SPEC_FILE_NODES = 100_000
MAX_SPEC_FILE_SPECS = 1_000
# Match inventory admission: the Python backend avoids native integer callback failures.
STREAMING_JSON = ijson.get_backend("python")
DEFINITION_FIELDS = ("enabled", "spec_type", "interval_seconds", "config")
APPLY_CONFLICT = "Spec application conflicted; retry the command"


def _retryable_storage_error(error: OperationalError) -> bool:
    cause = error.__cause__
    sqlite_code = getattr(cause, "sqlite_errorcode", None)
    if isinstance(sqlite_code, int) and (sqlite_code & 0xFF) in (
        sqlite3.SQLITE_BUSY,
        sqlite3.SQLITE_LOCKED,
    ):
        return True
    sqlstate = getattr(cause, "sqlstate", None) or getattr(cause, "pgcode", None)
    return sqlstate in {"40001", "40P01", "55P03"}


def _read_specs_file(path: Path) -> str:
    descriptor = os.open(
        path, os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_BINARY", 0)
    )
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            detail = " (path is a directory)" if stat.S_ISDIR(metadata.st_mode) else ""
            raise CommandError(f"Spec file must be a regular file{detail}: {path}")
        if metadata.st_size > MAX_SPEC_FILE_BYTES:
            raise CommandError(f"Spec file exceeds {MAX_SPEC_FILE_BYTES} bytes: {path}")
        content = bytearray()
        while len(content) <= MAX_SPEC_FILE_BYTES:
            chunk = os.read(
                descriptor, min(65536, MAX_SPEC_FILE_BYTES + 1 - len(content))
            )
            if not chunk:
                return content.decode("utf-8")
            content.extend(chunk)
        raise CommandError(f"Spec file exceeds {MAX_SPEC_FILE_BYTES} bytes: {path}")
    finally:
        os.close(descriptor)


def _admit_spec_shape(content: str, path: Path) -> None:
    """Count events without building lists/dicts, including ignored fields."""
    depth = nodes = specs = 0
    spec_array_depth: int | None = None
    root_member_is_specs = False
    try:
        for event, value in STREAMING_JSON.basic_parse(
            io.BytesIO(content.encode("utf-8")), buf_size=4096
        ):
            if event in ("end_map", "end_array"):
                depth -= 1
                if spec_array_depth is not None and depth < spec_array_depth:
                    spec_array_depth = None
                continue
            nodes += 1
            if nodes > MAX_SPEC_FILE_NODES:
                raise CommandError(
                    f"JSON node limit exceeds {MAX_SPEC_FILE_NODES} in {path}"
                )
            if event == "map_key":
                if depth == 1:
                    root_member_is_specs = value == "metric_collection_specs"
                continue
            if depth > MAX_SPEC_FILE_DEPTH:
                raise CommandError(f"JSON nesting too deep in {path}")
            if depth == spec_array_depth:
                specs += 1
                if specs > MAX_SPEC_FILE_SPECS:
                    raise CommandError(
                        f"Spec definition limit exceeds {MAX_SPEC_FILE_SPECS} in {path}"
                    )
            if event == "start_array" and (
                depth == 0 or (depth == 1 and root_member_is_specs)
            ):
                spec_array_depth = depth + 1
            if event in ("start_map", "start_array"):
                depth += 1
    except ijson.JSONError as err:
        # The streaming backend wraps Python's integer digit-limit ValueError.
        # Classify only its fixed prefix; never forward parser text or input.
        cause = err.__context__
        if isinstance(cause, ValueError) and str(cause).startswith(
            "Exceeds the limit ("
        ):
            raise CommandError(
                f"JSON integer exceeds the decoder limit in {path}"
            ) from None
        raise CommandError(f"Invalid JSON in {path}") from None
    except ArithmeticError:
        raise CommandError(f"Invalid JSON number in {path}") from None


def _unique_spec_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for position, (key, value) in enumerate(pairs, start=1):
        if key in result:
            raise CommandError(f"Duplicate JSON object key at member {position}")
        result[key] = value
    return result


def _load_specs_file(path: Path) -> list[dict[str, Any]]:
    try:
        content = _read_specs_file(path)
    except FileNotFoundError:
        raise CommandError(f"Spec file not found: {path}") from None
    except UnicodeDecodeError:
        raise CommandError(f"Spec file is not valid UTF-8: {path}") from None
    except OSError as err:
        if isinstance(err, IsADirectoryError):
            detail = "path is a directory"
        elif isinstance(err, PermissionError):
            detail = "permission denied"
        else:
            detail = "filesystem read failed"
        raise CommandError(f"Cannot read spec file {path}: {detail}") from None
    except ValueError:
        # For example an embedded NUL byte in the path.
        raise CommandError("Cannot read spec file: invalid path") from None
    _admit_spec_shape(content, path)
    try:
        payload = json.loads(content, object_pairs_hook=_unique_spec_object)
    except json.JSONDecodeError as err:
        raise CommandError(
            f"Invalid JSON in {path} at line {err.lineno} column {err.colno}"
        ) from None
    except RecursionError:
        raise CommandError(f"JSON nesting too deep in {path}") from None
    except ValueError as err:
        if str(err).startswith("Exceeds the limit ("):
            raise CommandError(
                f"JSON integer exceeds the decoder limit in {path}"
            ) from None
        raise CommandError(f"Invalid JSON in {path}") from None

    raw_specs: object
    if isinstance(payload, list):
        raw_specs = payload
    elif isinstance(payload, dict):
        raw_specs = payload.get("metric_collection_specs")
    else:
        raw_specs = None

    if not isinstance(raw_specs, list):
        raise CommandError(
            "Spec file must contain a JSON list or an object with a "
            "'metric_collection_specs' list"
        )

    normalized_specs: list[dict[str, Any]] = []
    valid_types = {item[0] for item in MetricCollectionSpecType.CHOICES}
    seen_names: set[str] = set()

    for idx, raw_spec in enumerate(raw_specs):
        if not isinstance(raw_spec, dict):
            raise CommandError(f"Spec at index {idx} must be an object")

        name = raw_spec.get("name")
        if not isinstance(name, str) or not name.strip():
            raise CommandError(f"Spec at index {idx} has invalid name")

        spec_type = raw_spec.get("spec_type")
        if not isinstance(spec_type, str) or spec_type not in valid_types:
            raise CommandError(f"Spec at index {idx} has invalid spec_type")

        interval_seconds = raw_spec.get("interval_seconds", 60)
        if (
            not isinstance(interval_seconds, int)
            or isinstance(interval_seconds, bool)
            or interval_seconds <= 0
        ):
            raise CommandError(f"Spec at index {idx} has invalid interval_seconds")

        enabled = raw_spec.get("enabled", True)
        if not isinstance(enabled, bool):
            raise CommandError(f"Spec at index {idx} has invalid enabled flag")

        config = raw_spec.get("config", {})
        if not isinstance(config, dict):
            raise CommandError(f"Spec at index {idx} has invalid config")

        normalized_name = name.strip()
        if normalized_name in seen_names:
            raise CommandError(f"Spec at index {idx} has a duplicated name in {path}")
        seen_names.add(normalized_name)

        normalized_specs.append(
            {
                "name": normalized_name,
                "enabled": enabled,
                "spec_type": spec_type,
                "interval_seconds": interval_seconds,
                "config": config,
            }
        )

    return normalized_specs


def _validate_spec(spec: MetricCollectionSpec, index: int) -> None:
    try:
        spec.full_clean(validate_unique=False)
        return
    except ValidationError as error:
        # Only publish known field identities. Model messages and params can
        # contain arbitrary config values; even unknown field names are private.
        invalid: list[str] = []
        field_errors = getattr(error, "error_dict", {})
        for field in ("name", *DEFINITION_FIELDS):
            if field not in field_errors:
                continue
            config_fields = []
            if field == "config":
                for key in (
                    "request_timeout_seconds",
                    "total_timeout_seconds",
                    "verify_tls",
                    "follow_redirects",
                    "max_body_bytes",
                ):
                    if any(
                        isinstance(item.message, str)
                        and item.message.startswith(f"config.{key} ")
                        for item in field_errors[field]
                    ):
                        config_fields.append(f"config.{key}")
            invalid.extend(config_fields or [field])
    # Raise outside the handler so no raw ValidationError remains in the chain.
    fields = ", ".join(invalid) or "definition"
    raise CommandError(
        f"Spec at index {index} has invalid {fields} (validation failed)"
    )


class Command(BaseCommand):
    help = "Create or update MetricCollectionSpec rows from a JSON file"

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--file",
            required=True,
            help="Path to a JSON file containing metric collection spec definitions",
        )
        parser.add_argument(
            "--prune",
            action="store_true",
            help=(
                "Delete existing MetricCollectionSpec rows whose names are not present "
                "in the desired file"
            ),
        )

    def handle(self, *args, **options) -> None:
        del args
        path = Path(str(options["file"])).expanduser()
        specs = _load_specs_file(path)
        prune = bool(options["prune"])

        created = 0
        updated = 0
        unchanged = 0
        deleted = 0
        result_lines: list[str] = []

        if prune and not specs:
            raise CommandError(
                "Refusing to prune with an empty desired spec set; omit --prune or "
                "provide at least one metric collection spec"
            )

        desired_names = {spec["name"] for spec in specs}

        try:
            with transaction.atomic():
                candidates = MetricCollectionSpec.objects.all()
                if not prune:
                    candidates = candidates.filter(name__in=desired_names)
                # Capture definitions once. Prune only rows present in this snapshot;
                # concurrent inserts are left for the next explicit reconciliation.
                existing = {spec.name: spec for spec in candidates}
                for index, desired in enumerate(specs):
                    spec = existing.get(desired["name"])
                    was_created = spec is None

                    if was_created:
                        spec = MetricCollectionSpec(
                            name=desired["name"],
                            enabled=desired["enabled"],
                            spec_type=desired["spec_type"],
                            interval_seconds=desired["interval_seconds"],
                            config=desired["config"],
                        )
                        _validate_spec(spec, index)
                        _, inserted = MetricCollectionSpec.objects.get_or_create(
                            name=spec.name,
                            defaults={
                                field: getattr(spec, field)
                                for field in DEFINITION_FIELDS
                            },
                        )
                        if not inserted:
                            raise CommandError(APPLY_CONFLICT)
                        created += 1
                        result_lines.append(f"created {spec.name}")
                        continue

                    assert spec is not None
                    expected = {
                        "pk": spec.pk,
                        "name": spec.name,
                        "updated_at": spec.updated_at,
                        **{field: getattr(spec, field) for field in DEFINITION_FIELDS},
                    }
                    current = MetricCollectionSpec.objects.filter(**expected)
                    changed_fields: list[str] = []
                    for field_name in DEFINITION_FIELDS:
                        desired_value = desired[field_name]
                        if getattr(spec, field_name) != desired_value:
                            setattr(spec, field_name, desired_value)
                            changed_fields.append(field_name)

                    if changed_fields:
                        if spec.next_run_time != 0:
                            spec.next_run_time = 0
                            changed_fields.append("next_run_time")
                        _validate_spec(spec, index)
                        values = {
                            field: getattr(spec, field) for field in changed_fields
                        }
                        if current.update(**values, updated_at=timezone.now()) != 1:
                            raise CommandError(APPLY_CONFLICT)
                        updated += 1
                        result_lines.append(f"updated {spec.name}")
                    else:
                        # Re-validate rows saved before config validation existed so
                        # an apply never reports a malformed legacy spec as accepted.
                        _validate_spec(spec, index)
                        # Retain the verified row until the operation commits.
                        if not current.select_for_update().exists():
                            raise CommandError(APPLY_CONFLICT)
                        unchanged += 1
                        result_lines.append(f"unchanged {spec.name}")

                if prune:
                    for stale_name, stale_spec in sorted(existing.items()):
                        if stale_name in desired_names:
                            continue
                        current = MetricCollectionSpec.objects.filter(
                            pk=stale_spec.pk,
                            name=stale_name,
                            updated_at=stale_spec.updated_at,
                            **{
                                field: getattr(stale_spec, field)
                                for field in DEFINITION_FIELDS
                            },
                        )
                        if not current.select_for_update().exists():
                            raise CommandError(APPLY_CONFLICT)
                        current.delete()
                        deleted += 1
                        result_lines.append(f"deleted {stale_name}")

        except (IntegrityError, OperationalError) as err:
            if isinstance(err, OperationalError) and _retryable_storage_error(err):
                raise CommandError(APPLY_CONFLICT) from err
            category = (
                "IntegrityError"
                if isinstance(err, IntegrityError)
                else "OperationalError"
            )
            raise CommandError(f"Spec application storage failure: {category}") from err

        # Keep this summary line's key=value shape stable; Ansible changed_when
        # logic in ops-library depends on created=/updated=/deleted= tokens here.
        changed = created + updated + deleted
        for line in result_lines:
            self.stdout.write(line)
        self.stdout.write(
            f"summary total={len(specs)} created={created} updated={updated} "
            f"unchanged={unchanged} deleted={deleted} changed={changed}"
        )
