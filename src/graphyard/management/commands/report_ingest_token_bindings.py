from django.core.management.base import BaseCommand

from graphyard.models import IngestToken
from graphyard.token_reports import (
    open_token_usage_batches,
    report_cutoff,
    terminal_text,
)


class Command(BaseCommand):
    help = "Report recent subject/host identifiers by token NAME and propose single-host bindings (read-only)."

    def add_arguments(self, parser):
        parser.add_argument("--days", type=int, default=30)

    def handle(self, *args, **options):
        days = options["days"]
        cutoff = report_cutoff(days)
        tokens = IngestToken.objects.order_by("name").values(
            "id", "name", "enabled", "bound_subject_id"
        )

        with open_token_usage_batches(tokens, cutoff) as batches:
            self.stdout.write(
                f"Source: ingest_token_host_usage.last_seen_at in the last {days} days; "
                "records normalized subject_id AND collector_host from accepted ingests. "
                "Updated at most hourly, capped at 500 identifiers per token. "
                "No history before host-usage recording began; registry last-seen alone cannot identify a token. "
                "Proposals require owner review and are not proof of exclusive ownership. "
                "Names are stored token names; disabled/retired usage stays with that row, "
                "not its replacement."
            )
            for token, usages in batches:
                self.stdout.write(
                    f"name={terminal_text(token['name'])} enabled={token['enabled']} binding={terminal_text(token['bound_subject_id'] or 'unbound')}"
                )
                for usage in usages:
                    self.stdout.write(
                        f"  {terminal_text(usage['host_id'])} last_seen={usage['last_seen_at'].isoformat()}"
                    )
                if len(usages) == 1:
                    self.stdout.write(
                        f"  proposed binding={terminal_text(usages[0]['host_id'])}"
                    )
                elif usages:
                    self.stdout.write(
                        "  no proposal: multiple subject/host identifiers; split producers/tokens before binding"
                    )
                else:
                    self.stdout.write(
                        "  no proposal: no recorded writes in this period"
                    )
