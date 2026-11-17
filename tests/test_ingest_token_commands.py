from __future__ import annotations

import io
import traceback

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


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("name", "label"),
    [
        (
            "synthetic\x1b]0;spoof\x07\r\n\t\x7f\x85\x9b",
            r"synthetic\x1b]0;spoof\x07\x0d\x0a\x09\x7f\x85\x9b",
        ),
        ("n" * 128, "n" * 128),
        ("n" * 124 + "\nend", "n" * 124 + "\\..."),
        ("\x1b" * 128, r"\x1b" * 31 + "\\..."),
    ],
)
def test_issuance_name_label_is_bounded_and_inert(name, label, monkeypatch):
    secret = "synthetic-exact-credential"
    monkeypatch.setattr(create_ingest_token.secrets, "token_urlsafe", lambda _: secret)
    stdout = io.StringIO()
    call_command("create_ingest_token", name=name, stdout=stdout, no_color=True)
    token = IngestToken.objects.get()
    assert token.name == name
    assert token.check_token(secret)
    assert stdout.getvalue().splitlines() == [
        "Created ingest token",
        f"id={token.pk}",
        f"name={label}",
        "host_scope=unrestricted",
        f"token={secret}",
        "Store this token securely now. Graphyard only stores a hash and cannot show it again.",
    ]
    assert stdout.getvalue().count(secret) == 1
    assert len(label) <= 128


@pytest.mark.django_db
@pytest.mark.parametrize("failure", ["enabled", "scope"])
def test_issuance_errors_use_inert_name_labels(failure):
    name = "synthetic\n\x1b[2J" + "n" * 100
    old_secret = create_token(name)
    old = IngestToken.objects.get()
    if failure == "scope":
        old.host_scope = ["invalid**invalid"]
        old.save(update_fields=["host_scope"])
    stdout = io.StringIO()
    with pytest.raises(CommandError) as caught:
        call_command(
            "create_ingest_token", name=name, rotate=failure != "enabled", stdout=stdout
        )
    message = str(caught.value)
    assert r"synthetic\x0a\x1b[2J" in message
    assert "\n" not in message and "\x1b" not in message
    assert stdout.getvalue() == ""
    old.refresh_from_db()
    assert old.name == name and old.enabled and old.check_token(old_secret)


@pytest.mark.django_db
@pytest.mark.parametrize("stage", ["revoke", "retire", "insert"])
@pytest.mark.parametrize("entrypoint", ["call", "cli", "traceback"])
def test_issuance_conflict_is_private_and_rolls_back(stage, entrypoint, monkeypatch):
    from django.db import IntegrityError

    old_secret = create_token("synthetic", host_scope=["edge"])
    old = IngestToken.objects.get()
    old.bound_subject_id = "edge"
    old.save(update_fields=["bound_subject_id"])
    original = IngestToken.objects.values().get()
    secret = "synthetic-unpublished-credential"
    private = "PRIVATE-SQL-BOUND-VALUES-STORAGE-SENTINEL"
    monkeypatch.setattr(create_ingest_token.secrets, "token_urlsafe", lambda _: secret)
    real_save = IngestToken.save

    def conflict(self, *args, **kwargs):
        if (
            (stage == "insert" and self.pk is None)
            or (
                stage == "revoke"
                and kwargs.get("update_fields") == ["enabled", "revoked_at"]
            )
            or (stage == "retire" and kwargs.get("update_fields") == ["name"])
        ):
            raise IntegrityError(private)
        return real_save(self, *args, **kwargs)

    monkeypatch.setattr(IngestToken, "save", conflict)
    # run_from_argv closes every connection afterwards; keep the test's open.
    monkeypatch.setattr("django.db.connections.close_all", lambda: None)
    stdout, stderr = io.StringIO(), io.StringIO()
    command = create_ingest_token.Command(stdout=stdout, stderr=stderr, no_color=True)
    with pytest.raises(SystemExit if entrypoint == "cli" else CommandError) as caught:
        if entrypoint == "call":
            call_command(command, name="synthetic", rotate=True)
        else:
            argv = [
                "manage.py",
                "create_ingest_token",
                "--name",
                "synthetic",
                "--rotate",
            ]
            if entrypoint == "traceback":
                argv.append("--traceback")
            command.run_from_argv(argv)
    rendered = "".join(traceback.format_exception(caught.value))
    assert private not in stdout.getvalue() + stderr.getvalue() + rendered
    assert secret not in stdout.getvalue() + stderr.getvalue() + rendered
    assert stdout.getvalue() == ""
    if entrypoint == "cli":
        assert caught.value.code == 1
        assert stderr.getvalue() == (
            "CommandError: Token issuance conflicted; no token was changed. Retry the command.\n"
        )
    else:
        assert str(caught.value) == (
            "Token issuance conflicted; no token was changed. Retry the command."
        )
        assert caught.value.__cause__ is None
    assert IngestToken.objects.values().get() == original
    old.refresh_from_db()
    assert old.check_token(old_secret)
