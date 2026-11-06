"""Synthetic export/auditor outcomes; no advisory service needed for unit tests."""

from pathlib import Path
import runpy
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "audit_python.py"


@pytest.mark.parametrize("export_exit,audit_exit", [(0, 0), (19, 0), (0, 23)])
def test_audit_propagates_failures_and_cleans_export(
    monkeypatch: pytest.MonkeyPatch, export_exit: int, audit_exit: int
) -> None:
    calls: list[list[str]] = []
    requirements: Path | None = None

    def run(command: list[str], *, cwd: Path, check: bool) -> None:
        nonlocal requirements
        assert cwd == ROOT
        assert check is True
        calls.append(command)
        if command[:2] == ["uv", "export"]:
            assert {
                "--frozen",
                "--all-groups",
                "--no-emit-project",
                "--no-hashes",
            } <= set(command)
            requirements = Path(command[command.index("--output-file") + 1])
            requirements.write_text("filelock==4.2.1\n", encoding="utf-8")
            code = export_exit
        else:
            assert requirements is not None
            assert requirements.read_text(encoding="utf-8") == "filelock==4.2.1\n"
            assert command == [
                "uvx",
                "--from",
                "pip-audit==2.10.1",
                "pip-audit",
                "--disable-pip",
                "--no-deps",
                "-r",
                str(requirements),
            ]
            code = audit_exit
        if code:
            raise subprocess.CalledProcessError(code, command)

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(SystemExit) as result:
        runpy.run_path(str(SCRIPT), run_name="__main__")
    assert result.value.code == (export_exit or audit_exit)
    assert len(calls) == (1 if export_exit else 2)
    assert requirements is not None and not requirements.exists()


def test_ci_runs_the_local_audit_command() -> None:
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert "run: uv run --frozen python scripts/audit_python.py" in workflow
