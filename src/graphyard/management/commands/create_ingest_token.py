from __future__ import annotations

import secrets

from django.core.exceptions import ValidationError
from django.core.management.base import BaseCommand, CommandError
from django.db import IntegrityError, transaction

from graphyard.models import IngestToken
from graphyard.token_scope import (
    is_valid_stored_scope,
    parse_host_scope_text,
)


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
        parser.add_argument(
            "--host-scope",
            action="append",
            default=None,
            metavar="HOSTS",
            help=(
                "Host identifiers the token may write metrics for (comma-separated, "
                "repeatable): exact names, or a prefix with one trailing '*'. "
                "Omitted: a replaced token keeps its scope, a new token is unrestricted."
            ),
        )
        parser.add_argument(
            "--unrestricted",
            action="store_true",
            help="Create the token without a host scope even if the replaced token had one",
        )

    def handle(self, *args, **options) -> None:
        name: str = options["name"]
        rotate: bool = options["rotate"]
        scope_args: list[str] | None = options.get("host_scope")
        unrestricted: bool = bool(options.get("unrestricted"))

        host_scope: list[str] | None = None
        if scope_args is not None:
            if unrestricted:
                raise CommandError("Use either --host-scope or --unrestricted")
            try:
                host_scope = parse_host_scope_text(",".join(scope_args))
            except ValidationError as err:
                raise CommandError("; ".join(err.messages)) from err
            if not host_scope:
                raise CommandError(
                    "--host-scope needs at least one entry; use --unrestricted "
                    "for a token without a scope"
                )
        elif unrestricted:
            host_scope = []

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
                    ingest_token.bound_subject_id = previous.bound_subject_id
                    previous.name = retired_token_name(previous)
                    previous.save(update_fields=["name"])
                    if host_scope is None:
                        # Replacing a token must never silently widen it.
                        if not is_valid_stored_scope(previous.host_scope):
                            raise CommandError(
                                f"The host scope stored for '{name}' is invalid. "
                                "Pass --host-scope or --unrestricted."
                            )
                        host_scope = previous.host_scope
                ingest_token.host_scope = [] if host_scope is None else host_scope
                ingest_token.save()
        except IntegrityError as err:
            raise CommandError(
                f"Could not create token '{name}'; no token was changed: {err}"
            ) from err

        self.stdout.write(self.style.SUCCESS("Created ingest token"))
        self.stdout.write(f"id={ingest_token.id}")
        self.stdout.write(f"name={ingest_token.name}")
        scope = ingest_token.host_scope
        self.stdout.write(
            "host_scope="
            + (",".join(str(entry) for entry in scope) if scope else "unrestricted")
        )
        self.stdout.write(f"token={plaintext_token}")
        self.stdout.write(
            "Store this token securely now. Graphyard only stores a hash and cannot show it again."
        )
