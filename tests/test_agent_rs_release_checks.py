"""Release checks of the Rust agent (agent-rs/PLAN.md section 8.2).

``agent-rs/scripts/release_checks.py`` gates the release workflow: the tag must
match the crate version, the binary must not need glibc newer than the floor
or libraries outside glibc/libgcc, ``--version`` must name the version and
commit, and the archive must be deterministic with exactly two files.
"""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import os
import subprocess
import sys
import tarfile
from pathlib import Path
from types import ModuleType

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "agent-rs" / "scripts" / "release_checks.py"
COMMIT = "27cf52e7bd2525d6f8ba4b2f9be42a880c2f6e1a"

# Shapes of binutils 2.38 (ubuntu-22.04) output: `objdump -T` then `objdump -p`.
OBJDUMP_OK = """
graphyard-agent:     file format elf64-x86-64

DYNAMIC SYMBOL TABLE:
0000000000000000      DF *UND*\t0000000000000000 (GLIBC_2.2.5) free
0000000000000000      DF *UND*\t0000000000000000 (GLIBC_2.34) __libc_start_main
0000000000000000      DF *UND*\t0000000000000000  GLIBC_2.18  __cxa_thread_atexit_impl
0000000000000000      DF *UND*\t0000000000000000 (GCC_4.2.0) _Unwind_Backtrace
0000000000000000  w   D  *UND*\t0000000000000000  Base        __gmon_start__

graphyard-agent:     file format elf64-x86-64

Dynamic Section:
  NEEDED               libgcc_s.so.1
  NEEDED               libm.so.6
  NEEDED               libc.so.6
  NEEDED               ld-linux-x86-64.so.2
  FLAGS                0x0000000000000008

Version References:
  required from libgcc_s.so.1:
    0x0b792650 0x00 13 GCC_3.0
    0x0d696914 0x00 12 GCC_4.2.0
  required from libc.so.6:
    0x06969194 0x00 05 GLIBC_2.14
    0x069691b5 0x00 04 GLIBC_2.35
    0x09691a75 0x00 02 GLIBC_2.2.5
"""


@pytest.fixture(scope="module")
def checks() -> ModuleType:
    spec = importlib.util.spec_from_file_location("agent_rs_release_checks", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def metadata(*packages: tuple[str, str]) -> dict:
    return {
        "packages": [{"name": name, "version": version} for name, version in packages]
    }


def test_version_comes_from_the_agent_package(checks: ModuleType) -> None:
    data = metadata(("graphyard-core", "9.9.9"), ("graphyard-agent", "0.1.0"))
    assert checks.package_version(data) == "0.1.0"
    assert (
        checks.package_version(metadata(("graphyard-agent", "1.2.3-rc.1")))
        == "1.2.3-rc.1"
    )


@pytest.mark.parametrize(
    "version", ["0.1", "01.0.0", "1.0.0+build.5", "1.0.0-", "v1.0.0"]
)
def test_version_must_be_semver_without_build_metadata(
    checks: ModuleType, version: str
) -> None:
    with pytest.raises(checks.CheckError, match="not semver"):
        checks.package_version(metadata(("graphyard-agent", version)))


def test_version_requires_exactly_one_agent_package(checks: ModuleType) -> None:
    with pytest.raises(checks.CheckError, match="found 0"):
        checks.package_version(metadata(("graphyard-core", "0.1.0")))


def test_tag_must_equal_the_crate_version(checks: ModuleType) -> None:
    checks.check_tag("agent-rs-v0.1.0", "0.1.0")
    for tag in ("agent-rs-v0.1.1", "agent-rs-0.1.0", "v0.1.0", "agent-rs-v0.1.0-rc.1"):
        with pytest.raises(checks.CheckError, match="expected 'agent-rs-v0.1.0'"):
            checks.check_tag(tag, "0.1.0")


def test_glibc_floor_accepts_the_ubuntu_22_04_build(checks: ModuleType) -> None:
    assert checks.glibc_versions(OBJDUMP_OK) == {
        "2.2.5",
        "2.14",
        "2.18",
        "2.34",
        "2.35",
    }
    assert checks.needed_libraries(OBJDUMP_OK) == [
        "libgcc_s.so.1",
        "libm.so.6",
        "libc.so.6",
        "ld-linux-x86-64.so.2",
    ]
    assert checks.check_glibc(OBJDUMP_OK, "2.35") == "2.35"


def test_glibc_floor_compares_numerically(checks: ModuleType) -> None:
    # 2.4 < 2.14 < 2.35 numerically, not as strings.
    text = OBJDUMP_OK.replace("GLIBC_2.35", "GLIBC_2.4")
    with pytest.raises(
        checks.CheckError, match="newer than 2.14: GLIBC_2.18, GLIBC_2.34"
    ):
        checks.check_glibc(text, "2.14")
    assert checks.check_glibc(text, "2.34") == "2.34"


@pytest.mark.parametrize(
    ("symbol", "message"),
    [
        ("GLIBC_2.36", "newer than 2.35: GLIBC_2.36"),
        ("GLIBC_2.39", "newer than 2.35: GLIBC_2.39"),
        ("GLIBC_PRIVATE", "unexpected glibc symbol version GLIBC_PRIVATE"),
    ],
)
def test_glibc_floor_rejects_newer_or_private_versions(
    checks: ModuleType, symbol: str, message: str
) -> None:
    text = (
        OBJDUMP_OK
        + f"0000000000000000      DF *UND*\t0000000000000000 ({symbol}) foo\n"
    )
    with pytest.raises(checks.CheckError, match=message):
        checks.check_glibc(text, "2.35")


def test_glibc_floor_rejects_foreign_libraries_and_static_binaries(
    checks: ModuleType,
) -> None:
    with pytest.raises(checks.CheckError, match="outside glibc/libgcc: libssl.so.3"):
        checks.check_glibc(OBJDUMP_OK + "  NEEDED               libssl.so.3\n", "2.35")
    without_needed = "\n".join(
        line for line in OBJDUMP_OK.splitlines() if "NEEDED" not in line
    )
    with pytest.raises(checks.CheckError, match="no NEEDED entries"):
        checks.check_glibc(without_needed, "2.35")
    with pytest.raises(checks.CheckError, match="no GLIBC symbol versions"):
        checks.check_glibc("statically linked\n", "2.35")
    with pytest.raises(checks.CheckError, match="not a glibc version"):
        checks.check_glibc(OBJDUMP_OK, "2.35a")


def test_version_output_must_name_version_and_commit(checks: ModuleType) -> None:
    checks.check_version_output(f"graphyard-agent 0.1.0 ({COMMIT})\n", "0.1.0", COMMIT)
    for stdout in (
        "graphyard-agent 0.1.0\n",
        f"graphyard-agent 0.1.0 ({COMMIT})",
        "graphyard-agent 0.1.0 (unknown)\n",
        f"graphyard-agent 0.1.1 ({COMMIT})\n",
    ):
        with pytest.raises(checks.CheckError, match="--version printed"):
            checks.check_version_output(stdout, "0.1.0", COMMIT)


def test_archive_is_deterministic_and_holds_binary_and_license(
    checks: ModuleType,
) -> None:
    binary, license_text = b"\x7fELF binary", b"MIT License\n"
    first = checks.build_archive(binary, license_text, 1_700_000_000)
    assert first == checks.build_archive(binary, license_text, 1_700_000_000)
    assert first != checks.build_archive(binary, license_text, 1_700_000_001)
    checks.verify_archive(first, binary, license_text)
    with tarfile.open(fileobj=io.BytesIO(first), mode="r:gz") as archive:
        members = {member.name: member for member in archive.getmembers()}
    assert set(members) == {"LICENSE", "graphyard-agent"}
    assert members["graphyard-agent"].mode == 0o755
    assert members["LICENSE"].mode == 0o644
    assert {(m.uid, m.gid, m.uname, m.gname, m.mtime) for m in members.values()} == {
        (0, 0, "root", "root", 1_700_000_000)
    }
    with pytest.raises(checks.CheckError, match="differs from its source"):
        checks.verify_archive(first, b"other", license_text)
    assert checks.archive_name("0.1.0", "x86_64-unknown-linux-gnu") == (
        "graphyard-agent-0.1.0-x86_64-unknown-linux-gnu.tar.gz"
    )


def run_script(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-I", str(SCRIPT), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def test_cli_version_and_tag(tmp_path: Path) -> None:
    path = tmp_path / "metadata.json"
    path.write_text(
        json.dumps(metadata(("graphyard-agent", "0.1.0"))), encoding="utf-8"
    )
    result = run_script("version", "--metadata", str(path), "--tag", "agent-rs-v0.1.0")
    assert (result.returncode, result.stdout) == (0, "0.1.0\n")
    result = run_script("version", "--metadata", str(path), "--tag", "agent-rs-v0.2.0")
    assert result.returncode == 1
    assert "release check failed: tag 'agent-rs-v0.2.0'" in result.stderr


def test_cli_pack_writes_archive_and_sha256sum_line(tmp_path: Path) -> None:
    binary = tmp_path / "graphyard-agent"
    binary.write_bytes(b"binary")
    license_path = tmp_path / "LICENSE"
    license_path.write_bytes(b"MIT\n")
    out = tmp_path / "dist"
    args = [
        "--binary",
        str(binary),
        "--license",
        str(license_path),
        "--version",
        "0.1.0",
    ]
    args += [
        "--target",
        "x86_64-unknown-linux-gnu",
        "--mtime",
        "1700000000",
        "--out-dir",
        str(out),
    ]
    result = run_script("pack", *args)
    name = "graphyard-agent-0.1.0-x86_64-unknown-linux-gnu.tar.gz"
    assert (result.returncode, result.stdout) == (0, f"{name}\n")
    data = (out / name).read_bytes()
    assert (out / f"{name}.sha256").read_text(encoding="utf-8") == (
        f"{hashlib.sha256(data).hexdigest()}  {name}\n"
    )


@pytest.mark.skipif(os.name != "posix", reason="POSIX shell script fake")
def test_cli_smoke_runs_the_binary(tmp_path: Path) -> None:
    fake = tmp_path / "graphyard-agent"
    fake.write_text(
        f"#!/bin/sh\necho 'graphyard-agent 0.1.0 ({COMMIT})'\n", encoding="utf-8"
    )
    fake.chmod(0o755)
    base = ["smoke", "--binary", str(fake), "--version"]
    assert run_script(*base, "0.1.0", "--commit", COMMIT).returncode == 0
    assert run_script(*base, "0.1.1", "--commit", COMMIT).returncode == 1
    result = run_script(*base, "0.1.0", "--commit", COMMIT[:12])
    assert result.returncode == 1
    assert "40 lowercase hex digits" in result.stderr


@pytest.mark.skipif(os.name != "posix", reason="POSIX shell script fake")
def test_cli_smoke_rejects_a_failing_binary(tmp_path: Path) -> None:
    # The S1 placeholder binary exits 1 with a message on stderr; the smoke
    # check must report that, not a traceback.
    fake = tmp_path / "graphyard-agent"
    fake.write_text(
        f"#!/bin/sh\necho 'graphyard-agent 0.1.0 ({COMMIT})'\necho broken >&2\nexit 1\n",
        encoding="utf-8",
    )
    fake.chmod(0o755)
    result = run_script(
        "smoke", "--binary", str(fake), "--version", "0.1.0", "--commit", COMMIT
    )
    assert result.returncode == 1
    assert "release check failed: --version exited with 1, stderr 'broken'" in (
        result.stderr
    )
    assert "Traceback" not in result.stderr


def test_smoke_reports_a_hanging_binary(
    checks: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def hang(*args: object, **kwargs: object) -> None:
        raise subprocess.TimeoutExpired(cmd="graphyard-agent", timeout=1)

    monkeypatch.setattr(checks.subprocess, "run", hang)
    binary = str(tmp_path / "graphyard-agent")
    assert (
        checks.main(
            ["smoke", "--binary", binary, "--version", "0.1.0", "--commit", COMMIT]
        )
        == 1
    )


@pytest.mark.parametrize(
    ("flag", "value", "message"),
    [
        ("--version", "../0.1.0", "is not semver"),
        ("--version", "0.1.0+build", "is not semver"),
        ("--target", "x86_64/../linux", "is not a target triple"),
        ("--target", "linux", "is not a target triple"),
    ],
)
def test_cli_pack_rejects_odd_names(
    tmp_path: Path, flag: str, value: str, message: str
) -> None:
    binary = tmp_path / "graphyard-agent"
    binary.write_bytes(b"binary")
    license_path = tmp_path / "LICENSE"
    license_path.write_bytes(b"MIT\n")
    options = {"--version": "0.1.0", "--target": "x86_64-unknown-linux-gnu"}
    options[flag] = value
    args = ["--binary", str(binary), "--license", str(license_path)]
    for name, option in options.items():
        args += [name, option]
    args += ["--mtime", "1700000000", "--out-dir", str(tmp_path / "dist")]
    result = run_script("pack", *args)
    assert result.returncode == 1
    assert message in result.stderr
    assert not (tmp_path / "dist").exists()


def test_release_workflow_runs_every_check() -> None:
    workflow = (REPO / ".github" / "workflows" / "agent-rs-release.yml").read_text(
        encoding="utf-8"
    )
    for needle in (
        "runs-on: ubuntu-22.04",
        'GLIBC_FLOOR: "2.35"',
        "release_checks.py version",
        "release_checks.py glibc-floor",
        "release_checks.py smoke",
        "release_checks.py pack",
        "cargo test --workspace --locked",
        "tests/test_agent_rs_fixtures.py",
        "GRAPHYARD_AGENT_GIT_COMMIT: ${{ github.sha }}",
        "needs: [build, fixtures]",
        'cargo build --release --locked --target "$TARGET"',
        "sha256sum --check",
    ):
        assert needle in workflow, needle
