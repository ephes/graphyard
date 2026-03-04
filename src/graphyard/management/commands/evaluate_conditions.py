from __future__ import annotations

from django.core.management.base import BaseCommand

from graphyard.services import evaluate_conditions_once


class Command(BaseCommand):
    help = "Evaluate enabled conditions and persist their status"

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--condition-id", type=int, help="Evaluate only one condition"
        )

    def handle(self, *args, **options) -> None:
        condition_id: int | None = options.get("condition_id")

        run = evaluate_conditions_once(condition_id=condition_id)
        if run.failed:
            self.stdout.write(
                self.style.WARNING(
                    f"Condition evaluation completed with failures ({run.failed}/{run.total})."
                )
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(f"Condition evaluation completed ({run.total}).")
            )
