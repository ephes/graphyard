from __future__ import annotations

from django.core.exceptions import ValidationError
from django.core.management.base import BaseCommand, CommandError

from graphyard.models import ComparisonOperator, ConditionDefinition, SubjectType


def _validate_threshold(name: str, value: float) -> None:
    if value < 0 or value > 1:
        raise CommandError(f"{name} must be between 0 and 1 for ratio metrics")


class Command(BaseCommand):
    help = "Create or update a host.filesystem_used_ratio condition for disk usage alerting"

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--host", required=True, help="Host id (for subject and host filter)"
        )
        parser.add_argument(
            "--mountpoint",
            default="/",
            help="Filesystem mountpoint tag filter (default: /)",
        )
        parser.add_argument(
            "--no-mountpoint-filter",
            action="store_true",
            help="Seed condition without a mountpoint tag filter",
        )
        parser.add_argument(
            "--name",
            default="",
            help="Condition name override (default is derived from host/mountpoint)",
        )
        parser.add_argument(
            "--operator",
            choices=[
                ComparisonOperator.GT,
                ComparisonOperator.GTE,
                ComparisonOperator.LT,
                ComparisonOperator.LTE,
            ],
            default=ComparisonOperator.GTE,
            help="Comparison operator (default: gte)",
        )
        parser.add_argument(
            "--warning-threshold",
            type=float,
            default=0.80,
            help="Warning threshold ratio (default: 0.80)",
        )
        parser.add_argument(
            "--critical-threshold",
            type=float,
            default=0.90,
            help="Critical threshold ratio (default: 0.90)",
        )
        parser.add_argument(
            "--window-minutes",
            type=int,
            default=30,
            help="Evaluation lookback window in minutes (default: 30)",
        )
        parser.add_argument(
            "--breach-minutes",
            type=int,
            default=5,
            help="Required continuous breach duration in minutes (default: 5)",
        )

    def handle(self, *args, **options) -> None:
        del args
        host: str = str(options["host"]).strip()
        mountpoint: str = str(options["mountpoint"]).strip()
        no_mountpoint_filter: bool = bool(options["no_mountpoint_filter"])
        warning_threshold: float = float(options["warning_threshold"])
        critical_threshold: float = float(options["critical_threshold"])
        window_minutes: int = int(options["window_minutes"])
        breach_minutes: int = int(options["breach_minutes"])
        operator: str = str(options["operator"])

        if not host:
            raise CommandError("--host must not be empty")
        if not no_mountpoint_filter and not mountpoint:
            raise CommandError(
                "--mountpoint must not be empty unless --no-mountpoint-filter is used"
            )
        if window_minutes <= 0:
            raise CommandError("--window-minutes must be greater than 0")
        if breach_minutes <= 0:
            raise CommandError("--breach-minutes must be greater than 0")
        if breach_minutes > window_minutes:
            raise CommandError(
                "--breach-minutes must be less than or equal to --window-minutes"
            )

        _validate_threshold("warning-threshold", warning_threshold)
        _validate_threshold("critical-threshold", critical_threshold)
        if warning_threshold > critical_threshold:
            raise CommandError(
                "--warning-threshold must be less than or equal to --critical-threshold"
            )

        name_override = str(options["name"]).strip()
        mountpoint_name = "*" if no_mountpoint_filter else mountpoint
        condition_name = (
            name_override or f"Host filesystem usage ({host} {mountpoint_name})"
        )

        tags_filter = {}
        if not no_mountpoint_filter:
            tags_filter = {"mountpoint": mountpoint}

        defaults: dict[str, object] = {
            "enabled": True,
            "metric_name": "host.filesystem_used_ratio",
            "host_filter": host,
            "subject_type_filter": SubjectType.HOST,
            "subject_id_filter": host,
            "service_filter": "",
            "tags_filter": tags_filter,
            "operator": operator,
            "warning_threshold": warning_threshold,
            "critical_threshold": critical_threshold,
            "window_minutes": window_minutes,
            "breach_minutes": breach_minutes,
        }

        condition, created = ConditionDefinition.objects.get_or_create(
            name=condition_name,
            defaults=defaults,
        )
        if not created:
            for field_name, field_value in defaults.items():
                setattr(condition, field_name, field_value)

        try:
            condition.full_clean()
        except ValidationError as err:
            raise CommandError(str(err)) from err
        condition.save()

        action = "Created" if created else "Updated"
        self.stdout.write(self.style.SUCCESS(f"{action} condition"))
        self.stdout.write(f"id={condition.id}")
        self.stdout.write(f"name={condition.name}")
        self.stdout.write(f"host={host}")
        self.stdout.write(f"mountpoint={mountpoint_name}")
