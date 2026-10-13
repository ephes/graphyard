import pytest
from django.contrib.auth.hashers import make_password
from django.test import RequestFactory

from graphyard.auth import authenticate_ingest_token
from graphyard.models import (
    IngestToken,
    LEGACY_FAST_TOKEN_HASH_PREFIXES,
    _fast_token_hash,
)

pytestmark = pytest.mark.django_db


def request(value):
    return RequestFactory().post("/v1/metrics", HTTP_AUTHORIZATION=f"Bearer {value}")


@pytest.mark.parametrize(
    "supplied,expected", [("fast-2", "fast2"), ("wrong", None), ("old", "legacy")]
)
def test_lookup_skips_unrelated_fast_rows(monkeypatch, supplied, expected):
    IngestToken.objects.create(name="legacy", token_hash=make_password("old"))
    for index in range(5):
        IngestToken.objects.create(
            name=f"fast{index}", token_hash=_fast_token_hash(f"fast-{index}")
        )
    checked = []
    original = IngestToken.check_token

    def check(self, value):
        checked.append(self.name)
        return original(self, value)

    monkeypatch.setattr(IngestToken, "check_token", check)
    result = authenticate_ingest_token(request(supplied))
    assert (result.name if result else None) == expected
    assert checked == (["fast2"] if supplied == "fast-2" else ["legacy"])
    if supplied == "old":
        assert IngestToken.objects.get(name="legacy").uses_fast_token_hash()


@pytest.mark.parametrize("prefix", LEGACY_FAST_TOKEN_HASH_PREFIXES)
def test_old_fast_prefix_still_upgrades(prefix):
    row = IngestToken.objects.create(
        name="old-fast", token_hash=_fast_token_hash("old", prefix=prefix)
    )
    assert authenticate_ingest_token(request("old")).pk == row.pk
    row.refresh_from_db()
    assert not row.needs_fast_hash_prefix_upgrade()


def test_system_check_reports_only_enabled_legacy_rows():
    from graphyard.checks import legacy_ingest_tokens

    row = IngestToken.objects.create(name="legacy", token_hash=make_password("old"))
    assert legacy_ingest_tokens(None) == []
    assert [w.id for w in legacy_ingest_tokens(None, databases=["default"])] == [
        "graphyard.W001"
    ]
    authenticate_ingest_token(request("old"))
    assert legacy_ingest_tokens(None, databases=["default"]) == []
    IngestToken.objects.filter(pk=row.pk).update(
        token_hash=make_password("old"), enabled=False
    )
    assert legacy_ingest_tokens(None, databases=["default"]) == []


def test_legacy_warning_registered_with_django():
    import io
    from django.core.management import call_command

    IngestToken.objects.create(name="legacy", token_hash=make_password("old"))
    output = io.StringIO()
    call_command("check", databases=["default"], stderr=output)
    assert "graphyard.W001" in output.getvalue()


@pytest.mark.parametrize("error_name", ["OperationalError", "ProgrammingError"])
def test_legacy_check_tolerates_unmigrated_database(monkeypatch, error_name):
    from django import db
    from django.db.models.query import QuerySet
    from graphyard.checks import legacy_ingest_tokens

    def unavailable(self):
        raise getattr(db, error_name)("no such table")

    monkeypatch.setattr(QuerySet, "exists", unavailable)
    assert legacy_ingest_tokens(None, databases=["default"]) == []
