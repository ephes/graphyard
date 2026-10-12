from __future__ import annotations

from datetime import timedelta

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from graphyard.models import IngestToken, IngestTokenHostUsage
from graphyard.token_scope import (
    MAX_HOST_SCOPE_ENTRIES,
    host_in_scope,
    is_scoped,
    is_valid_stored_scope,
)


class Command(BaseCommand):
    help = (
        "Report, per ingest token, the host identifiers it wrote metrics for in "
        "the last N days (read-only). Use it to choose host scopes."
    )

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--days", type=int, default=30, help="Look back this many days (default 30)"
        )
        parser.add_argument(
            "--include-disabled",
            action="store_true",
            help="Also list revoked/disabled tokens",
        )

    def handle(self, *args, **options) -> None:
        days: int = options["days"]
        if days < 1:
            raise CommandError("--days must be at least 1")
        cutoff = timezone.now() - timedelta(days=days)

        tokens = IngestToken.objects.order_by("name")
        if not options["include_disabled"]:
            tokens = tokens.filter(enabled=True)

        usage_by_token: dict[int, list[IngestTokenHostUsage]] = {}
        for usage in IngestTokenHostUsage.objects.filter(
            last_seen_at__gte=cutoff
        ).order_by("host_id"):
            usage_by_token.setdefault(usage.token_id, []).append(usage)

        self.stdout.write(
            f"Hosts written per ingest token in the last {days} day(s). "
            "Usage is recorded from the release that added host scopes onwards; "
            "last_seen is updated at most once per hour."
        )
        for token in tokens:
            scope = token.host_scope
            if not is_scoped(scope):
                scope_text = "unrestricted"
            elif is_valid_stored_scope(scope):
                scope_text = ",".join(str(entry) for entry in scope)
            else:
                scope_text = "invalid (allows nothing)"
            state = "enabled" if token.enabled else "disabled"
            self.stdout.write("")
            self.stdout.write(
                f"token id={token.pk} name={token.name} {state} host_scope={scope_text}"
            )
            usages = usage_by_token.get(token.pk, [])
            if not usages:
                self.stdout.write("  (no recorded writes in this period)")
                continue
            for usage in usages:
                note = ""
                if is_scoped(scope) and not host_in_scope(scope, usage.host_id):
                    note = "  outside current scope"
                self.stdout.write(
                    f"  {usage.host_id}  last_seen={usage.last_seen_at.isoformat(timespec='seconds')}"
                    f"  first_seen={usage.first_seen_at.isoformat(timespec='seconds')}{note}"
                )
            if len(usages) > MAX_HOST_SCOPE_ENTRIES:
                self.stdout.write(
                    f"  {len(usages)} identifiers; a scope holds at most "
                    f"{MAX_HOST_SCOPE_ENTRIES} entries, so choose prefixes "
                    "(for example 'web-*') or split the token"
                )
            else:
                self.stdout.write(
                    "  suggested --host-scope "
                    + ",".join(usage.host_id for usage in usages)
                )
