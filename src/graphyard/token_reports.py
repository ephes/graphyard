"""Bounded, read-only token metadata/usage projections (no credential columns)."""

from datetime import datetime
from itertools import islice
import json
import shlex
from tempfile import TemporaryFile

from django.core.management.base import CommandError

from graphyard.database import read_snapshot

from graphyard.models import IngestTokenHostUsage


REPORT_BATCH_SIZE = 100
MAX_REPORT_BYTES = 64 * 1024 * 1024
MAX_USAGE_PER_TOKEN = 500


def _token_usage_rows(tokens, cutoff):
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
            rows_for_token = usages[usage["token_id"]]
            if len(rows_for_token) >= MAX_USAGE_PER_TOKEN:
                raise CommandError(
                    "Token audit exceeds 500 identifiers per token; narrow --days"
                )
            rows_for_token.append(usage)
        for token in batch:
            yield token, usages[token["id"]]


def token_usage_batches(tokens, cutoff):
    """Read one generation, then yield outside the DB transaction.

    Private, automatically removed disk spool: RAM stays bounded by the original
    100-token/500-usage projections, and total encoded output is capped at 64 MiB.
    Only the caller's credential-free values projection enters this file.
    """
    with TemporaryFile() as spool:
        with read_snapshot():
            for token, usages in _token_usage_rows(tokens, cutoff):
                record = (
                    json.dumps(
                        (token, usages), default=lambda value: value.isoformat()
                    ).encode("utf-8")
                    + b"\n"
                )
                if spool.tell() + len(record) > MAX_REPORT_BYTES:
                    raise CommandError("Token audit exceeds 64 MiB; narrow --days")
                spool.write(record)
        spool.seek(0)
        for record in spool:
            token, usages = json.loads(record)
            for usage in usages:
                for field in ("first_seen_at", "last_seen_at"):
                    usage[field] = datetime.fromisoformat(usage[field])
            yield token, usages


def terminal_text(value):
    """Escape C0/DEL/C1 controls only at the human presentation boundary."""
    return "".join(
        f"\\x{ord(char):02x}" if ord(char) < 32 or 127 <= ord(char) <= 159 else char
        for char in str(value)
    )


def shell_operand(value):
    """One literal operand: POSIX quoting, or Bash/zsh ANSI-C for controls.

    ANSI-C octal UTF-8 bytes preserve even trailing newlines without exposing
    terminal controls. NUL has no shell-argument representation.
    """
    if "\0" in value:
        return None
    if terminal_text(value) == value:
        return shlex.quote(value)
    return "$'" + "".join(f"\\{byte:03o}" for byte in value.encode("utf-8")) + "'"
