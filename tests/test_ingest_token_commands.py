from __future__ import annotations

import io

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from graphyard.management.commands import create_ingest_token
from graphyard.models import IngestToken


def create_token(name: str, **options: object) -> str:
    stdout = io.StringIO()
    call_command("create_ingest_token", name=name, stdout=stdout, **options)
    for line in stdout.getvalue().splitlines():
        if line.startswith("token="):
            return line.removeprefix("token=")
    raise AssertionError("command did not print a token")


@pytest.mark.django_db
def test_rotate_replaces_token_and_keeps_revoked_history():
    old_secret = create_token("macmini")
    old = IngestToken.objects.get()

    new_secret = create_token("macmini", rotate=True)

    old.refresh_from_db()
    assert old.enabled is False
    assert old.revoked_at is not None
    assert old.name == f"macmini (retired #{old.pk})"
    assert old.check_token(old_secret)
    current = IngestToken.objects.get(name="macmini")
    assert current.pk != old.pk
    assert current.enabled is True
    assert current.check_token(new_secret)
    assert not current.check_token(old_secret)
    assert IngestToken.objects.filter(enabled=True).count() == 1


@pytest.mark.django_db
def test_rotate_can_be_repeated():
    create_token("macmini")
    create_token("macmini", rotate=True)
    newest_secret = create_token("macmini", rotate=True)

    assert IngestToken.objects.count() == 3
    assert IngestToken.objects.filter(enabled=True).count() == 1
    assert IngestToken.objects.get(name="macmini").check_token(newest_secret)


@pytest.mark.django_db
def test_failed_rotation_changes_nothing_and_old_token_stays_valid(monkeypatch):
    old_secret = create_token("macmini")
    create_token("blocker")
    # Force the unique constraint to fail in the middle of the rotation.
    monkeypatch.setattr(
        create_ingest_token, "retired_token_name", lambda token: "blocker"
    )

    with pytest.raises(CommandError, match="no token was changed"):
        create_token("macmini", rotate=True)

    assert IngestToken.objects.count() == 2
    old = IngestToken.objects.get(name="macmini")
    assert old.enabled is True
    assert old.revoked_at is None
    assert old.check_token(old_secret)


@pytest.mark.django_db
def test_failed_insert_of_new_token_rolls_the_revocation_back(monkeypatch):
    old_secret = create_token("macmini")
    old = IngestToken.objects.get()
    real_save = IngestToken.save

    def failing_insert(self, *args, **kwargs):
        if self.pk is None:
            raise RuntimeError("insert exploded")
        return real_save(self, *args, **kwargs)

    monkeypatch.setattr(IngestToken, "save", failing_insert)

    with pytest.raises(RuntimeError, match="insert exploded"):
        create_token("macmini", rotate=True)

    old.refresh_from_db()
    assert old.name == "macmini"
    assert old.enabled is True
    assert old.revoked_at is None
    assert old.check_token(old_secret)
    assert IngestToken.objects.count() == 1


@pytest.mark.django_db
def test_create_without_rotate_refuses_enabled_token_and_changes_nothing():
    old_secret = create_token("macmini")

    with pytest.raises(CommandError, match="Use --rotate"):
        create_token("macmini")

    old = IngestToken.objects.get()
    assert old.name == "macmini"
    assert old.enabled is True
    assert old.check_token(old_secret)


@pytest.mark.django_db
def test_create_reuses_name_of_revoked_token():
    create_token("macmini")
    call_command("revoke_ingest_token", name="macmini", stdout=io.StringIO())
    revoked = IngestToken.objects.get()

    new_secret = create_token("macmini")

    revoked.refresh_from_db()
    assert revoked.enabled is False
    assert revoked.name == f"macmini (retired #{revoked.pk})"
    assert IngestToken.objects.get(name="macmini").check_token(new_secret)


@pytest.mark.django_db
def test_retired_name_fits_the_name_column():
    long_name = "n" * 128
    create_token(long_name)
    old = IngestToken.objects.get()

    create_token(long_name, rotate=True)

    old.refresh_from_db()
    assert len(old.name) == 128
    assert old.name.endswith(f" (retired #{old.pk})")
    assert IngestToken.objects.get(name=long_name).enabled is True
