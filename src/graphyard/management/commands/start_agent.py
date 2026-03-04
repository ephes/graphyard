from __future__ import annotations

import logging
import time

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from graphyard.services import (
    evaluate_conditions_once,
    run_metric_collection_specs_once,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (
        "Run the long-lived graphyard agent loop "
        "(metric collection specs + condition evaluation)"
    )

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--metrics-interval",
            type=int,
            default=settings.GRAPHYARD_METRIC_COLLECTION_INTERVAL_SECONDS,
            help="Seconds between metric collection scheduler ticks",
        )
        parser.add_argument(
            "--condition-interval",
            type=int,
            default=settings.GRAPHYARD_CONDITION_EVAL_INTERVAL_SECONDS,
            help="Seconds between condition evaluation runs",
        )
        parser.add_argument(
            "--disable-metrics",
            action="store_true",
            help="Disable metric collection loop",
        )
        parser.add_argument(
            "--disable-ha",
            action="store_true",
            help="Deprecated alias for --disable-metrics",
        )
        parser.add_argument(
            "--disable-conditions",
            action="store_true",
            help="Disable condition evaluation loop",
        )
        parser.add_argument(
            "--run-once",
            action="store_true",
            help="Run enabled loops once and exit",
        )

    def handle(self, *args, **options) -> None:
        metrics_interval = int(options["metrics_interval"])
        condition_interval = int(options["condition_interval"])
        run_metrics = not bool(options["disable_metrics"] or options["disable_ha"])
        run_conditions = not bool(options["disable_conditions"])
        run_once = bool(options["run_once"])

        if not run_metrics and not run_conditions:
            raise CommandError("At least one loop must be enabled")
        if run_metrics and metrics_interval <= 0:
            raise CommandError("--metrics-interval must be greater than 0")
        if run_conditions and condition_interval <= 0:
            raise CommandError("--condition-interval must be greater than 0")

        self.stdout.write(
            self.style.SUCCESS(
                "Starting graphyard-agent "
                f"(metrics={run_metrics}@{metrics_interval}s, "
                f"conditions={run_conditions}@{condition_interval}s)"
            )
        )

        if run_once:
            self._run_enabled_loops(
                run_metrics=run_metrics, run_conditions=run_conditions
            )
            return

        now = time.monotonic()
        next_metrics_run = now if run_metrics else float("inf")
        next_condition_run = now if run_conditions else float("inf")

        try:
            while True:
                now = time.monotonic()
                if run_metrics and now >= next_metrics_run:
                    self._run_metric_specs()
                    next_metrics_run += metrics_interval
                    if next_metrics_run < time.monotonic():
                        next_metrics_run = time.monotonic() + metrics_interval

                if run_conditions and now >= next_condition_run:
                    self._run_condition_eval()
                    next_condition_run += condition_interval
                    if next_condition_run < time.monotonic():
                        next_condition_run = time.monotonic() + condition_interval

                sleep_until = min(next_metrics_run, next_condition_run)
                sleep_seconds = max(0.2, min(1.0, sleep_until - time.monotonic()))
                time.sleep(sleep_seconds)
        except KeyboardInterrupt:
            self.stdout.write(
                self.style.WARNING("graphyard-agent interrupted, exiting")
            )

    def _run_enabled_loops(self, *, run_metrics: bool, run_conditions: bool) -> None:
        if run_metrics:
            self._run_metric_specs(due_only=False)
        if run_conditions:
            self._run_condition_eval()

    def _run_metric_specs(self, *, due_only: bool = True) -> None:
        try:
            run = run_metric_collection_specs_once(due_only=due_only)
            self.stdout.write(
                "Metric specs: "
                f"total={run.total} failed={run.failed} warning={run.warning} "
                f"ingested={run.ingested} skipped={run.skipped}"
            )
        except Exception as err:  # noqa: BLE001
            logger.exception("Metric collection failed")
            self.stderr.write(f"Metric collection failed: {err}")

    def _run_condition_eval(self) -> None:
        try:
            run = evaluate_conditions_once()
            self.stdout.write(
                f"Condition eval: evaluated={run.total} failed={run.failed}"
            )
        except Exception as err:  # noqa: BLE001
            logger.exception("Condition evaluation failed")
            self.stderr.write(f"Condition eval failed: {err}")
