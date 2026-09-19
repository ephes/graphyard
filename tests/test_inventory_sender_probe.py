"""Failure evidence must remain useful without echoing credentials or peer output."""

import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

scripts = Path(__file__).resolve().parents[1] / "scripts"
spec = importlib.util.spec_from_file_location(
    "inventory_sender_probe", scripts / "inventory_sender_probe.py"
)
assert spec is not None and spec.loader is not None
probe = importlib.util.module_from_spec(spec)
sys.path.insert(0, str(scripts))
try:
    spec.loader.exec_module(probe)
finally:
    sys.path.pop(0)


def test_failed_exit_retains_safe_authored_diagnostic():
    completed = subprocess.CompletedProcess([], 2, "private stdout", "private stderr")
    with pytest.raises(probe.ProbeFailure) as error:
        probe.checked_sender_result(completed, 0, "synthetic-secret")
    assert probe.failure_description(error.value) == "Unexpected sender exit code 2"


def test_credential_leak_is_checked_before_exit_code():
    completed = subprocess.CompletedProcess([], 2, "synthetic-secret", "")
    with pytest.raises(probe.ProbeFailure) as error:
        probe.checked_sender_result(completed, 0, "synthetic-secret")
    assert probe.failure_description(error.value) == "Credential leaked"


def test_untrusted_exception_text_is_not_reflected():
    assert (
        probe.failure_description(ValueError("private exception content"))
        == "ValueError"
    )


@pytest.mark.parametrize(
    "stdout,stderr", [("synthetic-secret", ""), ("", "synthetic-secret")]
)
def test_secret_in_either_stream_fails(stdout, stderr):
    completed = subprocess.CompletedProcess([], 0, stdout, stderr)
    with pytest.raises(probe.ProbeFailure, match="Credential leaked"):
        probe.checked_sender_result(completed, 0, "synthetic-secret")


def test_successful_result_is_parsed():
    completed = subprocess.CompletedProcess([], 0, '{"reports":[]}', "")
    assert probe.checked_sender_result(completed, 0, "synthetic-secret") == {
        "reports": []
    }


def test_main_failure_uses_tested_diagnostic_helper(tmp_path, monkeypatch):
    library = tmp_path / "library"
    files = library / "roles/software_estate/files"
    files.mkdir(parents=True)
    (files / "send.py").touch()
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    monkeypatch.setattr(sys, "argv", ["probe", "--library", str(library)])
    monkeypatch.setitem(sys.modules, "emit", SimpleNamespace())
    monkeypatch.setattr(probe.tempfile, "mkdtemp", lambda *args, **kwargs: str(root))
    monkeypatch.setattr(
        probe, "configure", Mock(side_effect=probe.ProbeFailure("Database isolation"))
    )
    diagnostic = Mock(wraps=probe.failure_description)
    monkeypatch.setattr(probe, "failure_description", diagnostic)
    assert probe.main() == 1
    diagnostic.assert_called_once()
    result = json.loads((root / "result.json").read_text())
    assert result["error"] == "Database isolation"
