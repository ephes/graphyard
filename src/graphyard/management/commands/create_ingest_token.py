from __future__ import annotations

import secrets

from django.core.management.base import BaseCommand, CommandError

from graphyard.models import IngestToken


class Command(BaseCommand):
    help = "Create a new ingest bearer token (stored hashed only)"

    def add_arguments(self, parser) -> None:
        parser.add_argument("--name", required=True, help="Token name (e.g. host id)")
        parser.add_argument(
            "--rotate",
            action="store_true",
            help="Revoke an existing token with the same name before creating a new one",
        )

    def handle(self, *args, **options) -> None:
        name: str = options["name"]
        rotate: bool = options["rotate"]

        existing = IngestToken.objects.filter(name=name, enabled=True).first()
        if existing is not None:
            if not rotate:
                raise CommandError(
                    f"Enabled token with name '{name}' exists. Use --rotate to replace it."
                )
            existing.revoke()

        plaintext_token = secrets.token_urlsafe(32)
        ingest_token = IngestToken(name=name)
        ingest_token.set_token(plaintext_token)
        ingest_token.save()

        self.stdout.write(self.style.SUCCESS("Created ingest token"))
        self.stdout.write(f"id={ingest_token.id}")
        self.stdout.write(f"name={ingest_token.name}")
        self.stdout.write(f"token={plaintext_token}")
        self.stdout.write(
            "Store this token securely now. Graphyard only stores a hash and cannot show it again."
        )
