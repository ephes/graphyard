"""Synthetic hostile identities stay data in terminal output and shell operands."""

from io import StringIO
import subprocess

import pytest
from django.core.management import call_command
from django.db import connection
from django.utils import timezone

from graphyard.models import IngestToken, IngestTokenHostUsage


@pytest.mark.django_db
@pytest.mark.parametrize(
    "command", ["report_ingest_token_hosts", "report_ingest_token_bindings"]
)
def test_terminal_controls_never_reach_operator_output(command):
    name = "invented\x1b]0;spoof\x07\r\n\t\x85\x9b"
    host = "invented\x1b[2J\n\r\x7f\x9b;$(printf hacked)'\"\n"
    token = IngestToken.objects.create(
        name=name, token_hash="never-print", bound_subject_id=host
    )
    now = timezone.now()
    IngestTokenHostUsage.objects.create(
        token=token, host_id=host, first_seen_at=now, last_seen_at=now
    )
    output = StringIO()
    call_command(command, stdout=output)
    text = output.getvalue()
    assert all(ord(c) >= 32 and not 127 <= ord(c) <= 159 for c in text if c != "\n"), (
        repr(text)
    )
    assert "\\x1b" in text and "\\x0a" in text
    if command.endswith("_hosts"):
        operand = next(
            line.split("suggested --host-scope ", 1)[1]
            for line in text.splitlines()
            if "suggested --host-scope " in line
        )
        decoded = subprocess.run(
            ["bash", "-c", "set -- " + operand + '; test "$#" = 1 && printf %s "$1"'],
            check=True,
            capture_output=True,
        ).stdout.decode()
        assert decoded == host
    token.refresh_from_db()
    assert token.name == name and token.bound_subject_id == host
    assert token.host_usage.get().host_id == host


@pytest.mark.django_db
def test_printable_shell_syntax_is_one_literal_operand():
    import shlex

    host = "invented';$(printf hacked) with space"
    token = IngestToken.objects.create(name="invented", token_hash="never-print")
    now = timezone.now()
    IngestTokenHostUsage.objects.create(
        token=token, host_id=host, first_seen_at=now, last_seen_at=now
    )
    output = StringIO()
    call_command("report_ingest_token_hosts", stdout=output)
    operand = next(
        line.split("suggested --host-scope ", 1)[1]
        for line in output.getvalue().splitlines()
        if "suggested --host-scope " in line
    )
    assert shlex.split(operand) == [host]


@pytest.mark.django_db
def test_nul_cannot_mint_a_lossy_shell_operand():
    if connection.vendor == "postgresql":
        pytest.skip("PostgreSQL text cannot store NUL; the SQLite case covers it")
    token = IngestToken.objects.create(name="invented", token_hash="never-print")
    now = timezone.now()
    IngestTokenHostUsage.objects.create(
        token=token, host_id="invented\0suffix", first_seen_at=now, last_seen_at=now
    )
    output = StringIO()
    call_command("report_ingest_token_hosts", stdout=output)
    assert "\\x00" in output.getvalue() and "\0" not in output.getvalue()
    assert "suggested --host-scope" not in output.getvalue()
