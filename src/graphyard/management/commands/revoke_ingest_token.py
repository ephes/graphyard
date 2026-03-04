from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from graphyard.models import IngestToken


class Command(BaseCommand):
    help = "Revoke (disable) an ingest token by id or name"

    def add_arguments(self, parser) -> None:
        parser.add_argument("--id", type=int, help="Token id")
        parser.add_argument("--name", help="Token name")

    def handle(self, *args, **options) -> None:
        token_id: int | None = options.get("id")
        name: str | None = options.get("name")

        if token_id is None and not name:
            raise CommandError("Provide --id or --name")

        queryset = IngestToken.objects.filter(enabled=True)
        if token_id is not None:
            queryset = queryset.filter(id=token_id)
        if name:
            queryset = queryset.filter(name=name)

        tokens = list(queryset)
        if not tokens:
            raise CommandError("No matching enabled token found")

        for token in tokens:
            token.revoke()
            self.stdout.write(
                self.style.SUCCESS(f"Revoked token id={token.id} name={token.name}")
            )
