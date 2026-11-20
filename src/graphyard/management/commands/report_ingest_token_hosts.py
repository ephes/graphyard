from __future__ import annotations

from datetime import timedelta

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from graphyard.models import IngestToken
from graphyard.token_reports import shell_operand, terminal_text, token_usage_batches
from graphyard.token_scope import MAX_HOST_SCOPE_ENTRIES, admit_host_scope


MAX_LOOKBACK_DAYS = 36500


class Command(BaseCommand):
    help = (
        "Report, per ingest token, the host identifiers it wrote metrics for in "
        "the last N days (read-only). Use it to choose host scopes."
    )

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--days", type=int, default=30, help="Look back 1–36500 days (default 30)"
        )
        parser.add_argument(
            "--include-disabled",
            action="store_true",
            help="Also list revoked/disabled tokens",
        )

    def handle(self, *args, **options) -> None:
        days: int = options["days"]
        if not 1 <= days <= MAX_LOOKBACK_DAYS:
            raise CommandError("--days must be between 1 and 36500")
        try:
            cutoff = timezone.now() - timedelta(days=days)
        except (OverflowError, ValueError) as err:
            raise CommandError("--days exceeds the supported datetime range") from err

        tokens = IngestToken.objects.order_by("name")
        if not options["include_disabled"]:
            tokens = tokens.filter(enabled=True)

        tokens = tokens.values("id", "name", "enabled", "host_scope")

        self.stdout.write(
            f"Hosts written per ingest token in the last {days} day(s). "
            "Usage is recorded from the release that added host scopes onwards; "
            "last_seen is updated at most once per hour."
        )
        for token, usages in token_usage_batches(tokens, cutoff):
            scope = token["host_scope"]
            matcher = admit_host_scope(scope)
            if matcher.unrestricted:
                scope_text = "unrestricted"
            elif matcher.valid:
                scope_text = ",".join(scope)
            else:
                scope_text = "invalid (allows nothing)"
            state = "enabled" if token["enabled"] else "disabled"
            self.stdout.write("")
            self.stdout.write(
                f"token id={token['id']} name={terminal_text(token['name'])} {state} host_scope={terminal_text(scope_text)}"
            )
            if not usages:
                self.stdout.write("  (no recorded writes in this period)")
                continue
            for usage in usages:
                note = ""
                if not matcher.allows(usage["host_id"]):
                    note = "  outside current scope"
                self.stdout.write(
                    f"  {terminal_text(usage['host_id'])}  last_seen={usage['last_seen_at'].isoformat(timespec='seconds')}"
                    f"  first_seen={usage['first_seen_at'].isoformat(timespec='seconds')}{note}"
                )
            if len(usages) > MAX_HOST_SCOPE_ENTRIES:
                self.stdout.write(
                    f"  {len(usages)} identifiers; a scope holds at most "
                    f"{MAX_HOST_SCOPE_ENTRIES} entries, so choose prefixes "
                    "(for example 'web-*') or split the token"
                )
            else:
                operand = shell_operand(",".join(usage["host_id"] for usage in usages))
                if operand is None:
                    self.stdout.write("  no suggestion: NUL cannot be a shell argument")
                else:
                    self.stdout.write("  suggested --host-scope " + operand)
