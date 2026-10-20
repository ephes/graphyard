from datetime import timedelta

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from graphyard.models import IngestToken, IngestTokenHostUsage


class Command(BaseCommand):
    help = "Report recent subject/host identifiers by token NAME and propose single-host bindings (read-only)."

    def add_arguments(self, parser):
        parser.add_argument("--days", type=int, default=30)

    def handle(self, *args, **options):
        days = options["days"]
        if not 1 <= days <= 36500:
            raise CommandError("--days must be between 1 and 36500")
        cutoff = timezone.now() - timedelta(days=days)
        self.stdout.write(
            f"Source: ingest_token_host_usage.last_seen_at in the last {days} days; "
            "records normalized subject_id AND collector_host from accepted ingests. "
            "Updated at most hourly, capped at 500 identifiers per token. "
            "No history before host-usage recording began; registry last-seen alone cannot identify a token. "
            "Proposals require owner review and are not proof of exclusive ownership. "
            "Names are stored token names; disabled/retired usage stays with that row, "
            "not its replacement."
        )
        for token in IngestToken.objects.only(
            "name", "enabled", "bound_subject_id"
        ).order_by("name"):
            usages = list(
                IngestTokenHostUsage.objects.filter(
                    token=token, last_seen_at__gte=cutoff
                ).order_by("host_id")
            )
            self.stdout.write(
                f"name={token.name} enabled={token.enabled} binding={token.bound_subject_id or 'unbound'}"
            )
            for usage in usages:
                self.stdout.write(
                    f"  {usage.host_id} last_seen={usage.last_seen_at.isoformat()}"
                )
            if len(usages) == 1:
                self.stdout.write(f"  proposed binding={usages[0].host_id}")
            elif usages:
                self.stdout.write(
                    "  no proposal: multiple subject/host identifiers; split producers/tokens before binding"
                )
            else:
                self.stdout.write("  no proposal: no recorded writes in this period")
