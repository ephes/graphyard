"""Drift guard for the Rust agent's fixtures (agent-rs/PLAN.md 9.2).

The fixtures in ``agent-rs/fixtures`` are generated from the Python
implementation and pin the behaviour the Rust port must reproduce. A Python
change that alters any of them must regenerate the files (``just
rs-fixtures``), which then shows the Rust side what to change.

The generator runs as its own process, exactly as ``just rs-fixtures`` does:
it fixes ``PYTHONHASHSEED``, scrubs the environment and sets up Django with
the test settings, independent of this pytest process.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

GENERATOR = (
    Path(__file__).resolve().parents[1] / "agent-rs" / "fixtures" / "generate.py"
)


def _generate(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(GENERATOR), *args],
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )


def _fixture_names() -> list[str]:
    result = _generate("--list")
    assert result.returncode == 0, result.stderr
    names = result.stdout.split()
    assert "model.json" in names
    return names


def test_committed_fixtures_match_the_python_implementation() -> None:
    result = _generate("--check")
    assert result.returncode == 0, (
        "stale Rust agent fixtures: run `just rs-fixtures` and update the Rust "
        f"side to match\n{result.stderr}"
    )


def test_drift_check_detects_a_hand_edited_fixture(tmp_path: Path) -> None:
    fixtures_dir = GENERATOR.parent
    for name in _fixture_names():
        (tmp_path / name).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(fixtures_dir / name, tmp_path / name)
    clean = _generate("--check", "--dir", str(tmp_path))
    assert clean.returncode == 0, clean.stderr

    edited = tmp_path / "model.json"
    text = edited.read_text(encoding="utf-8")
    assert '"critical"' in text
    edited.write_text(text.replace('"critical"', '"kritisch"', 1), encoding="utf-8")
    result = _generate("--check", "--dir", str(tmp_path))
    assert result.returncode == 1
    assert "stale: model.json" in result.stderr

    edited.unlink()
    (tmp_path / "leftover.json").write_text("{}\n", encoding="utf-8")
    result = _generate("--check", "--dir", str(tmp_path))
    assert result.returncode == 1
    assert "stale: model.json" in result.stderr
    assert "orphan: leftover.json" in result.stderr
