"""Descriptor admission tests: no platform devices or FIFO operations required."""

import json
import os
import stat
from types import SimpleNamespace

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from graphyard.management.commands import apply_metric_collection_specs as command


@pytest.mark.parametrize(
    "kind", [stat.S_IFIFO, stat.S_IFCHR, stat.S_IFDIR, stat.S_IFSOCK]
)
def test_nonregular_spec_descriptor_is_refused_before_read_or_database(
    monkeypatch, kind
):
    opened, closed = [], []

    def open_file(path, flags):
        opened.append((path, flags))
        return 123

    monkeypatch.setattr(os, "open", open_file)
    monkeypatch.setattr(
        os, "fstat", lambda fd: SimpleNamespace(st_mode=kind, st_size=0)
    )
    monkeypatch.setattr(os, "close", closed.append)

    def read(*args):
        pytest.fail("nonregular descriptor must never be read")

    monkeypatch.setattr(os, "read", read)
    with pytest.raises(CommandError, match="regular file"):
        call_command(
            "apply_metric_collection_specs", file="synthetic-input", prune=True
        )
    assert len(opened) == 1
    assert opened[0][1] & getattr(os, "O_NONBLOCK", 0) == getattr(os, "O_NONBLOCK", 0)
    assert closed == [123]


@pytest.mark.parametrize("kind", ["oversized", "growing", "deep", "invalid"])
def test_bad_spec_file_has_bounded_private_error_and_no_database(
    tmp_path, monkeypatch, kind
):
    path = tmp_path / "synthetic.json"
    private = "PRIVATE-SYNTHETIC-CONTENT"
    limit = 128
    if kind == "deep":
        path.write_text(
            '[{"name":"synthetic","spec_type":"http_page_probe",'
            '"config":{"url":"https://example.test/","extra":'
            + "[" * 100
            + "0"
            + "]" * 100
            + "}}]"
        )
    elif kind == "invalid":
        path.write_text(private)
    else:
        monkeypatch.setattr(command, "MAX_SPEC_FILE_BYTES", limit, raising=False)
        path.write_text(
            json.dumps(
                [
                    {
                        "name": private,
                        "spec_type": "http_page_probe",
                        "config": {"url": "https://example.test/"},
                    }
                ]
            )
            + " " * limit
        )
    if kind == "growing":
        # Metadata was small at open time; the actual read must still be capped.
        real_stat, real_read = os.fstat, os.read
        reads = []

        def fstat(fd):
            value = real_stat(fd)
            return SimpleNamespace(st_mode=value.st_mode, st_size=1)

        def read(fd, size):
            reads.append(size)
            return real_read(fd, size)

        monkeypatch.setattr(os, "fstat", fstat)
        monkeypatch.setattr(os, "read", read)
    with pytest.raises(CommandError) as error:
        call_command("apply_metric_collection_specs", file=str(path), prune=True)
    assert private not in str(error.value)
    assert len(str(error.value)) < len(str(path)) + 120
    if kind == "growing":
        assert sum(reads) <= limit + 1


@pytest.mark.django_db
def test_exact_limit_spec_file_is_accepted(tmp_path, monkeypatch):
    path = tmp_path / "synthetic.json"
    payload = json.dumps(
        [
            {
                "name": "synthetic",
                "spec_type": "http_page_probe",
                "config": {"url": "https://example.test/"},
            }
        ]
    ).encode()
    path.write_bytes(payload)
    monkeypatch.setattr(command, "MAX_SPEC_FILE_BYTES", len(payload), raising=False)
    call_command("apply_metric_collection_specs", file=str(path))


@pytest.mark.django_db
def test_spec_file_uses_the_opened_regular_descriptor(tmp_path, monkeypatch):
    backing = tmp_path / "backing.json"
    backing.write_text(
        json.dumps(
            [
                {
                    "name": "synthetic-link",
                    "spec_type": "http_page_probe",
                    "config": {"url": "https://example.test/"},
                }
            ]
        )
    )
    alias = tmp_path / "synthetic-link.json"
    original = os.open
    opened = []

    def open_alias(path, flags):
        assert path == alias
        opened.append(path)
        return original(backing, flags)

    # Model a followed symlink without requiring OS-specific symlink permissions.
    monkeypatch.setattr(os, "open", open_alias)
    call_command("apply_metric_collection_specs", file=str(alias))
    assert opened == [alias]


def test_decoder_depth_exhaustion_is_normalized_before_database(tmp_path, monkeypatch):
    path = tmp_path / "synthetic.json"
    path.write_text("[]")
    calls = []

    def exhausted(content):
        calls.append(True)
        raise RecursionError("private synthetic content")

    monkeypatch.setattr(command.json, "loads", exhausted)
    with pytest.raises(CommandError, match="nesting too deep") as error:
        call_command("apply_metric_collection_specs", file=str(path), prune=True)
    assert calls == [True]
    assert "private synthetic" not in str(error.value)


@pytest.mark.django_db
@pytest.mark.parametrize("nested_levels", [61, 62])
def test_spec_file_depth_boundary(tmp_path, django_assert_num_queries, nested_levels):
    path = tmp_path / "synthetic.json"
    extra = 0
    for _ in range(nested_levels):
        extra = [extra]
    path.write_text(
        json.dumps(
            [
                {
                    "name": "synthetic",
                    "spec_type": "http_page_probe",
                    "config": {"url": "https://example.test/", "extra": extra},
                }
            ]
        )
    )
    # Root list -> spec -> config -> extra uses three edges; 61 more reaches 64.
    if nested_levels == 61:
        call_command("apply_metric_collection_specs", file=str(path))
    else:
        with (
            django_assert_num_queries(0),
            pytest.raises(CommandError, match="nesting too deep"),
        ):
            call_command("apply_metric_collection_specs", file=str(path))
