from __future__ import annotations

import secrets

from django.core.management.base import BaseCommand, CommandError
from django.db import IntegrityError, transaction

from graphyard.models import IngestToken


def retired_token_name(token: IngestToken) -> str:
    suffix = f" (retired #{token.pk})"
    max_length = IngestToken._meta.get_field("name").max_length or 128
    return f"{token.name[: max_length - len(suffix)]}{suffix}"


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

        plaintext_token = secrets.token_urlsafe(32)
        ingest_token = IngestToken(name=name)
        ingest_token.set_token(plaintext_token)
        try:
            # One transaction: either the new token exists and the previous one
            # is revoked, or nothing changed and the previous one stays valid.
            with transaction.atomic():
                previous = (
                    IngestToken.objects.select_for_update().filter(name=name).first()
                )
                if previous is not None:
                    if previous.enabled:
                        if not rotate:
                            raise CommandError(
                                f"Enabled token with name '{name}' exists. Use --rotate to replace it."
                            )
                        previous.revoke()
                    # Names are unique: the retired row keeps its history under
                    # a derived name and frees the name for the new token.
                    previous.name = retired_token_name(previous)
                    previous.save(update_fields=["name"])
                ingest_token.save()
        except IntegrityError as err:
            raise CommandError(
                f"Could not create token '{name}'; no token was changed: {err}"
            ) from err

        self.stdout.write(self.style.SUCCESS("Created ingest token"))
        self.stdout.write(f"id={ingest_token.id}")
        self.stdout.write(f"name={ingest_token.name}")
        self.stdout.write(f"token={plaintext_token}")
        self.stdout.write(
            "Store this token securely now. Graphyard only stores a hash and cannot show it again."
        )
