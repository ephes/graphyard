from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from django.core.exceptions import ValidationError
from django.core.management.base import BaseCommand, CommandError

from graphyard.models import MetricCollectionSpec, MetricCollectionSpecType


def _load_specs_file(path: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as err:
        raise CommandError(f"Spec file not found: {path}") from err
    except json.JSONDecodeError as err:
        raise CommandError(f"Invalid JSON in {path}: {err}") from err

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

    for idx, raw_spec in enumerate(raw_specs):
        if not isinstance(raw_spec, dict):
            raise CommandError(f"Spec at index {idx} must be an object")

        name = raw_spec.get("name")
        if not isinstance(name, str) or not name.strip():
            raise CommandError(f"Spec at index {idx} has invalid name")

        spec_type = raw_spec.get("spec_type")
        if not isinstance(spec_type, str) or spec_type not in valid_types:
            raise CommandError(
                f"Spec {name!r} has invalid spec_type {spec_type!r}"
            )

        interval_seconds = raw_spec.get("interval_seconds", 60)
        if (
            not isinstance(interval_seconds, int)
            or isinstance(interval_seconds, bool)
            or interval_seconds <= 0
        ):
            raise CommandError(
                f"Spec {name!r} has invalid interval_seconds {interval_seconds!r}"
            )

        enabled = raw_spec.get("enabled", True)
        if not isinstance(enabled, bool):
            raise CommandError(f"Spec {name!r} has invalid enabled flag {enabled!r}")

        config = raw_spec.get("config", {})
        if not isinstance(config, dict):
            raise CommandError(f"Spec {name!r} has invalid config {config!r}")

        normalized_specs.append(
            {
                "name": name.strip(),
                "enabled": enabled,
                "spec_type": spec_type,
                "interval_seconds": interval_seconds,
                "config": config,
            }
        )

    return normalized_specs


class Command(BaseCommand):
    help = "Create or update MetricCollectionSpec rows from a JSON file"

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--file",
            required=True,
            help="Path to a JSON file containing metric collection spec definitions",
        )

    def handle(self, *args, **options) -> None:
        del args
        path = Path(str(options["file"])).expanduser()
        specs = _load_specs_file(path)

        created = 0
        updated = 0
        unchanged = 0

        for desired in specs:
            spec = MetricCollectionSpec.objects.filter(name=desired["name"]).first()
            was_created = spec is None

            if was_created:
                spec = MetricCollectionSpec(
                    name=desired["name"],
                    enabled=desired["enabled"],
                    spec_type=desired["spec_type"],
                    interval_seconds=desired["interval_seconds"],
                    config=desired["config"],
                )
                try:
                    spec.full_clean()
                except ValidationError as err:
                    raise CommandError(str(err)) from err
                spec.save()
                created += 1
                self.stdout.write(f"created {spec.name}")
                continue

            assert spec is not None
            changed_fields: list[str] = []
            for field_name in ("enabled", "spec_type", "interval_seconds", "config"):
                desired_value = desired[field_name]
                if getattr(spec, field_name) != desired_value:
                    setattr(spec, field_name, desired_value)
                    changed_fields.append(field_name)

            if changed_fields:
                if spec.next_run_time != 0:
                    spec.next_run_time = 0
                    changed_fields.append("next_run_time")
                try:
                    spec.full_clean()
                except ValidationError as err:
                    raise CommandError(str(err)) from err
                spec.save(update_fields=changed_fields + ["updated_at"])
                updated += 1
                self.stdout.write(f"updated {spec.name}")
            else:
                unchanged += 1
                self.stdout.write(f"unchanged {spec.name}")

        # Keep this summary line's key=value shape stable; Ansible changed_when
        # logic in ops-library depends on created=/updated= tokens here.
        changed = created + updated
        self.stdout.write(
            f"summary total={len(specs)} created={created} updated={updated} "
            f"unchanged={unchanged} changed={changed}"
        )
