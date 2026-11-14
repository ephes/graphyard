"""Bounded, read-only token metadata/usage projections (no credential columns)."""

from itertools import islice

from graphyard.models import IngestTokenHostUsage


REPORT_BATCH_SIZE = 100


def token_usage_batches(tokens, cutoff):
    rows = tokens.iterator(chunk_size=REPORT_BATCH_SIZE)
    while batch := list(islice(rows, REPORT_BATCH_SIZE)):
        usages = {token["id"]: [] for token in batch}
        for usage in (
            IngestTokenHostUsage.objects.filter(
                token_id__in=usages, last_seen_at__gte=cutoff
            )
            .order_by("token_id", "host_id")
            .values("token_id", "host_id", "first_seen_at", "last_seen_at")
            .iterator(chunk_size=REPORT_BATCH_SIZE)
        ):
            usages[usage["token_id"]].append(usage)
        for token in batch:
            yield token, usages[token["id"]]
