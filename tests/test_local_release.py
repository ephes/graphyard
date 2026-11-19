"""The local release program (``<crate dir>/scripts/local_release.py``).

The same test file runs in mailgun-relay, opsgate, graphyard and nyxmon: it
checks this repository's ``local-release.json`` against ``release_checks.py``
and drives both build modes and the publication with fakes (no Docker, no
network, no GitHub).
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
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

REPO = next(
    path
    for path in Path(__file__).resolve().parents
    if (path / "pyproject.toml").exists()
)
SCRIPT = next(REPO.glob("*/scripts/local_release.py"))
CRATE = SCRIPT.parents[1]
COMMIT = "0123456789abcdef0123456789abcdef01234567"
OTHER = "fedcba9876543210fedcba9876543210fedcba98"


def load(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def release() -> ModuleType:
    return load("local_release_under_test", SCRIPT)


@pytest.fixture(scope="module")
def checks() -> ModuleType:
    return load(
        "release_checks_for_local_release", CRATE / "scripts" / "release_checks.py"
    )


def linux_data(**changes: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "github_repository": "ephes/example",
        "binary": "example",
        "tag_prefix": "rs-v",
        "target": "x86_64-unknown-linux-gnu",
        "mode": "linux-container",
        "commit_env": "EXAMPLE_GIT_COMMIT",
        "glibc_floor": "2.35",
        "checks": ["cargo fmt --check", "cargo test --locked"],
        "check_env": {"EXAMPLE_REQUIRE": "1"},
    }
    data.update(changes)
    return data


def macos_data(**changes: Any) -> dict[str, Any]:
    data = linux_data(
        target="aarch64-apple-darwin",
        mode="macos-native",
        max_minos="14.0",
        macho_arch="arm64",
    )
    del data["glibc_floor"]
    data.update(changes)
    return data


# --- configuration -------------------------------------------------------------------


def test_repository_config_matches_release_checks(
    release: ModuleType, checks: ModuleType
) -> None:
    config = release.load_config()
    assert config.github_repository.startswith("ephes/")
    assert config.archive("1.2.3") == checks.archive_name("1.2.3", config.target)
    checks.check_tag(config.tag("1.2.3"), "1.2.3")
    assert any(check.startswith("cargo fmt") for check in config.checks)
    assert any(
        check.startswith("cargo clippy") and "-D warnings" in check
        for check in config.checks
    )
    assert any(
        check.startswith("cargo test") and "--locked" in check
        for check in config.checks
    )
    assert (CRATE / "rust-toolchain.toml").is_file()
    assert (REPO / "LICENSE").is_file()


def test_no_tag_triggered_release_workflow() -> None:
    """Releases are local; no workflow builds or publishes on a tag push."""
    workflows = REPO / ".github" / "workflows"
    for path in sorted(workflows.glob("*.y*ml")) if workflows.is_dir() else []:
        text = path.read_text(encoding="utf-8")
        assert "tags:" not in text, path.name
        assert "gh release" not in text, path.name


@pytest.mark.parametrize(
    ("data", "message"),
    [
        (linux_data(extra=1), "unknown keys extra"),
        (linux_data(mode="windows"), "mode is missing or malformed"),
        (linux_data(tag_prefix="v"), "tag_prefix"),
        (linux_data(glibc_floor=None), "needs glibc_floor"),
        (linux_data(max_minos="14.0"), "needs glibc_floor only"),
        (
            linux_data(target="aarch64-unknown-linux-gnu"),
            "builds x86_64-unknown-linux-gnu",
        ),
        (linux_data(apt_packages=["tmux; rm"]), "not an apt package name"),
        (linux_data(checks=[]), "checks must name"),
        (linux_data(check_env={"lower": "1"}), "check_env"),
        (linux_data(host_checks=["pytest"]), "host_checks"),
        (linux_data(source_paths=["../x", "LICENSE"]), "leaves the repository"),
        (linux_data(source_paths=["LICENSE"]), "crate dir and LICENSE"),
        (macos_data(apt_packages=["tmux"]), "no glibc_floor or apt_packages"),
        (macos_data(macho_arch=None), "needs max_minos and macho_arch"),
        (macos_data(target="x86_64-unknown-linux-gnu"), "apple-darwin target"),
    ],
)
def test_config_refusals(
    release: ModuleType, data: dict[str, Any], message: str
) -> None:
    with pytest.raises(release.ReleaseError, match=message):
        release.parse_config(data)


# --- scripts -----------------------------------------------------------------------------


def test_linux_release_script_runs_every_check(release: ModuleType) -> None:
    config = release.parse_config(linux_data())
    script = release.release_script(config)
    for needle in (
        'version --metadata "$OUT/metadata.json" --tag "$TAG"',
        "cargo build --release --locked --target x86_64-unknown-linux-gnu",
        'glibc-floor --max 2.35 "target/x86_64-unknown-linux-gnu/release/example"',
        'smoke --binary "target/x86_64-unknown-linux-gnu/release/example"',
        '--version "$VERSION" --commit "$COMMIT"',
        "pack --binary",
        "--license ../LICENSE",
        '--mtime "$MTIME"',
        'sha256sum --check "${archive}.sha256"',
    ):
        assert needle in script, needle
    assert release.check_script(config) == "cargo fmt --check\ncargo test --locked\n"


def test_macos_release_script_runs_every_check(release: ModuleType) -> None:
    config = release.parse_config(macos_data())
    script = release.release_script(config, python="/opt/python 3/bin/python3")
    assert "'/opt/python 3/bin/python3' -I scripts/release_checks.py macho" in script
    assert (
        '--max-minos 14.0 --arch arm64 "target/aarch64-apple-darwin/release/example"'
        in script
    )
    assert 'shasum -a 256 --check "${archive}.sha256"' in script
    assert "glibc-floor" not in script


def test_dockerfile_pins_rustup_and_refuses_bad_packages(release: ModuleType) -> None:
    text = release.dockerfile(
        "ubuntu@sha256:" + "a" * 64, ["python3"], "linux/amd64", "rust"
    )
    assert text.startswith("FROM --platform=linux/amd64 ubuntu@sha256:")
    assert release.RUSTUP["linux/amd64"][1] in text
    assert "sha256sum --check" in text
    assert "cargo fetch --locked" in text
    assert release.RUSTUP_VERSION == "1.29.2"
    # The trust store check runs before curl fetches rustup.
    trust = text.index("dpkg --compare-versions")
    assert trust < text.index("RUN curl")
    # In the same RUN as the APT install: a failed check leaves no cached layer.
    assert "rm -rf /var/lib/apt/lists/* \\\n    && dpkg --compare-versions" in text
    assert "ge 20260816ubuntu0.22.04.1" in text
    for root in release.BLOCKED_ROOTS:
        assert root in text
    assert "\n" not in release.TRUST_CHECK
    assert "WORKDIR /work/rust" in text
    with pytest.raises(release.ReleaseError, match="not an apt package name"):
        release.dockerfile("ubuntu", ["bad name"], "linux/amd64", "rust")
    with pytest.raises(release.ReleaseError, match="no rustup-init"):
        release.dockerfile("ubuntu", [], "linux/riscv64", "rust")


# --- fakes -------------------------------------------------------------------------------


class FakeGit:
    def __init__(self, repo: Path, *, clean: bool = True) -> None:
        self.repo = repo
        self.clean = clean

    def is_clean(self) -> bool:
        return self.clean

    def head(self) -> str:
        return COMMIT

    def commit_time(self, commit: str) -> int:
        return 1_700_000_000

    def archive(self, commit: str, paths: Sequence[str], output: Path) -> None:
        with tarfile.open(output, "w") as tar:
            data = (
                b'[toolchain]\nchannel = "1.99.0"\ncomponents = ["rustfmt", "clippy"]\n'
            )
            info = tarfile.TarInfo(f"{CRATE.name}/rust-toolchain.toml")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))


def write_dist(out: Path, archive: str) -> None:
    dist = out / "dist"
    dist.mkdir(parents=True)
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        info = tarfile.TarInfo("example")
        info.size = 3
        info.mode = 0o755
        tar.addfile(info, io.BytesIO(b"bin"))
    (dist / archive).write_bytes(buffer.getvalue())
    digest = hashlib.sha256(buffer.getvalue()).hexdigest()
    (dist / f"{archive}.sha256").write_text(f"{digest}  {archive}\n", encoding="utf-8")
    (out / "checks.log").write_text("glibc OK\nsmoke OK\n", encoding="utf-8")


class FakeDocker:
    def __init__(self) -> None:
        self.images: list[tuple[str, str]] = []
        self.jobs: list[tuple[str, str, dict[str, str]]] = []
        self.removed: list[str] = []

    def preflight(self) -> str:
        return "linux/arm64"

    def pinned(self, image: str) -> str:
        return "ubuntu@sha256:" + "b" * 64

    def build_image(self, tag: str, context: Path, platform: str) -> None:
        assert (context / "source.tar").is_file()
        assert (
            f"FROM --platform={platform} ubuntu@sha256:"
            in (context / "Dockerfile").read_text()
        )
        self.images.append((tag, platform))

    def job(
        self,
        image: str,
        name: str,
        script: str,
        env: Mapping[str, str],
        out_dir: Path,
        platform: str,
    ) -> None:
        self.jobs.append((platform, script, dict(env)))
        (out_dir / "versions.txt").write_text(
            "python3=3.10\nrustc 1.99.0\n", encoding="utf-8"
        )
        if "cargo build --release" in script:
            write_dist(
                out_dir, f"example-{env['VERSION']}-x86_64-unknown-linux-gnu.tar.gz"
            )

    def remove_image(self, tag: str) -> None:
        self.removed.append(tag)


class FakeHost:
    def __init__(self) -> None:
        self.scripts: list[tuple[str, dict[str, str]]] = []

    def shell(self, script: str, cwd: Path, env: Mapping[str, str]) -> None:
        assert (cwd / "rust-toolchain.toml").is_file()
        self.scripts.append((script, dict(env)))
        if "cargo build --release" in script:
            write_dist(
                Path(env["OUT"]),
                f"example-{env['VERSION']}-aarch64-apple-darwin.tar.gz",
            )

    def output(self, argv: Sequence[str], cwd: Path, env: Mapping[str, str]) -> str:
        return {
            "rustc": "rustc 1.99.0 (abc 2026-10-01)\n",
            "sw_vers": "15.5\n",
            "rustup": "/fake/toolchains/1.99.0/bin/cargo\n",
        }[argv[0]]


class FakeGitHub:
    def __init__(self, *, on_main: bool = True) -> None:
        self.main_ok = on_main
        self.tags: dict[str, str] = {}
        self.tagged: list[tuple[str, str]] = []
        self.releases: dict[str, dict[str, bytes]] = {}
        self.ids: dict[str, int] = {}
        self.drafts: set[str] = set()
        self.prereleases: set[str] = set()
        self.created: list[tuple[str, bool]] = []
        self.uploaded: list[str] = []
        self.created_as_draft = False
        self.corrupt = False
        self.replace_after_download = False
        self.swap_asset_after_download = False
        self.no_digests = False
        self.asset_generation = 0

    def release(self, tag: str) -> Any:
        if tag not in self.releases:
            return None
        data = self.releases[tag]
        return SimpleNamespace(
            id=self.ids.setdefault(tag, 100 + len(self.ids)),
            assets={
                name: "" if self.no_digests else hashlib.sha256(blob).hexdigest()
                for name, blob in data.items()
            },
            asset_ids={name: f"{name}-{self.asset_generation}" for name in data},
            draft=tag in self.drafts,
            prerelease=tag in self.prereleases,
        )

    def check_cli(self) -> None:
        pass

    def on_main(self, commit: str) -> bool:
        return self.main_ok

    def tag(self, tag: str) -> str | None:
        return self.tags.get(tag)

    def create_tag(self, tag: str, commit: str, message: str) -> None:
        self.tagged.append((tag, commit))
        self.tags[tag] = commit

    def create(
        self, tag: str, title: str, notes: Path, files: list[Path], prerelease: bool
    ) -> None:
        assert notes.read_text(encoding="utf-8").startswith(title)
        self.created.append((tag, prerelease))
        self.releases[tag] = {path.name: path.read_bytes() for path in files}
        if prerelease:
            self.prereleases.add(tag)
        if self.created_as_draft:
            self.drafts.add(tag)

    def upload(self, tag: str, files: list[Path]) -> None:
        self.uploaded.extend(path.name for path in files)
        self.releases[tag].update({path.name: path.read_bytes() for path in files})

    def download(self, tag: str, directory: Path) -> None:
        for name, data in self.releases[tag].items():
            (directory / name).write_bytes(b"x" + data if self.corrupt else data)
        if self.replace_after_download:
            self.ids[tag] = 999
        if self.swap_asset_after_download:
            self.asset_generation += 1


def no_host_checks(config: Any, repo: Path, log: Any) -> None:
    pass


@pytest.fixture
def which_rustup(
    monkeypatch: pytest.MonkeyPatch, release: ModuleType
) -> Iterator[None]:
    monkeypatch.setattr(release.shutil, "which", lambda name: "/fake/cargo/bin/rustup")
    yield


# --- build -------------------------------------------------------------------------------


def test_build_refuses_dirty_unpushed_taken_and_untaken(
    release: ModuleType, tmp_path: Path
) -> None:
    config = release.parse_config(linux_data())
    common: dict[str, Any] = {
        "config": config,
        "docker": FakeDocker(),
        "version": "1.2.3",
        "run_host_checks": no_host_checks,
        "log": lambda line: None,
    }
    with pytest.raises(release.ReleaseError, match="clean checkout"):
        release.build(
            False,
            tmp_path / "a",
            git=FakeGit(REPO, clean=False),
            github=FakeGitHub(),
            **common,
        )
    with pytest.raises(release.ReleaseError, match="push it first"):
        release.build(
            False,
            tmp_path / "a",
            git=FakeGit(REPO),
            github=FakeGitHub(on_main=False),
            **common,
        )
    taken = FakeGitHub()
    taken.tags["rs-v1.2.3"] = COMMIT
    with pytest.raises(release.ReleaseError, match="already released"):
        release.build(False, tmp_path / "a", git=FakeGit(REPO), github=taken, **common)
    with pytest.raises(release.ReleaseError, match="drop --compare-released"):
        release.build(
            True, tmp_path / "a", git=FakeGit(REPO), github=FakeGitHub(), **common
        )
    (tmp_path / "full").mkdir()
    (tmp_path / "full" / "x").write_text("x")
    with pytest.raises(release.ReleaseError, match="not empty"):
        release.build(
            False, tmp_path / "full", git=FakeGit(REPO), github=FakeGitHub(), **common
        )


def test_linux_build_and_publish(release: ModuleType, tmp_path: Path) -> None:
    config = release.parse_config(linux_data())
    docker = FakeDocker()
    git = FakeGit(REPO)
    out = release.build(
        False,
        tmp_path / "out",
        config=config,
        git=git,
        github=FakeGitHub(),
        docker=docker,
        version="1.2.3",
        run_host_checks=no_host_checks,
        log=lambda line: None,
    )
    archive = "example-1.2.3-x86_64-unknown-linux-gnu.tar.gz"
    assert sorted(path.name for path in out.iterdir()) == sorted(
        [archive, f"{archive}.sha256", "build.json", "notes.md"]
    )
    assert [platform for _, platform in docker.images] == ["linux/arm64", "linux/amd64"]
    assert sorted(docker.removed) == sorted(tag for tag, _ in docker.images)
    (check_platform, check, check_env), (build_platform, build, build_env) = docker.jobs
    assert check_platform == "linux/arm64" and "cargo test --locked" in check
    assert (
        check_env["EXAMPLE_REQUIRE"] == "1" and check_env["CARGO_NET_OFFLINE"] == "true"
    )
    assert build_platform == "linux/amd64" and "glibc-floor --max 2.35" in build
    assert "EXAMPLE_REQUIRE" not in build_env
    assert build_env["EXAMPLE_GIT_COMMIT"] == COMMIT and build_env["TAG"] == "rs-v1.2.3"
    assert build_env["MTIME"] == "1700000000"
    record = json.loads((out / "build.json").read_text())
    assert record["commit"] == COMMIT and record["compare_released"] is False
    notes = (out / "notes.md").read_text()
    assert f"sha256 of {archive}: {record['sha256']}" in notes
    assert "glibc >= 2.35" in notes

    github = FakeGitHub()
    lines: list[str] = []
    release.publish(out, config=config, github=github, log=lines.append)
    assert github.tagged == [("rs-v1.2.3", COMMIT)]
    assert github.created == [("rs-v1.2.3", False)]
    assert lines[-1] == "verified 2 assets of rs-v1.2.3 byte for byte"
    # Repeatable: the second run changes nothing and verifies again.
    lines.clear()
    release.publish(out, config=config, github=github, log=lines.append)
    assert lines[0] == "tag rs-v1.2.3 already on GitHub"
    assert "already holds every asset" in lines[1]
    assert len(github.tagged) == 1 and len(github.created) == 1


def test_linux_build_names_survive_reused_pid(
    release: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    retained = set()

    class RetainingDocker(FakeDocker):
        def job(self, image: str, name: str, *args: Any) -> None:
            assert name not in retained
            retained.add(name)
            super().job(image, name, *args)

    docker = RetainingDocker()
    invocation_ids = iter(["abcd1234", "abcd5678"])
    monkeypatch.setattr(release.os, "getpid", lambda: 4242)
    monkeypatch.setattr(release.secrets, "token_hex", lambda size: next(invocation_ids))
    config = release.parse_config(linux_data())
    for invocation in ("first", "second"):
        release.build(
            False,
            tmp_path / invocation,
            config=config,
            git=FakeGit(REPO),
            github=FakeGitHub(),
            docker=docker,
            version="1.2.3",
            run_host_checks=no_host_checks,
            log=lambda line: None,
        )
    assert retained == {
        f"example-release-{phase}-4242-{suffix}"
        for phase in ("checks", "build")
        for suffix in ("abcd1234", "abcd5678")
    }
    assert set(docker.removed) == {tag for tag, _ in docker.images}


@pytest.mark.parametrize("foreign", [False, True])
def test_docker_job_retires_only_owned_container_after_failed_create(
    release: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, foreign: bool
) -> None:
    name = "example-release-build-4242-abcd1234"
    container_id = "a" * 64
    containers = {container_id: "foreign"} if foreign else {}
    observed: list[str] = []

    def run(argv: Sequence[str], **kwargs: Any) -> str:
        if argv[1] == "create":
            if not foreign and "--label" in argv:
                containers[container_id] = argv[argv.index("--label") + 1]
            raise release.ReleaseError("create result unavailable")
        pytest.fail(f"unexpected Docker command: {argv}")

    def cleanup(argv: Sequence[str], *, read_output: bool = False) -> str | None:
        if read_output:
            assert argv[1:3] == ["container", "ls"]
            ownership = next(arg for arg in argv if arg.startswith("label="))
            observed.append(ownership)
            return "".join(
                f"{cid} {name}\n"
                for cid, label in containers.items()
                if label == ownership.removeprefix("label=")
            )
        assert list(argv[:3]) == ["docker", "rm", "-f"]
        containers.pop(argv[3])
        return None

    monkeypatch.setattr(release, "run", run)
    monkeypatch.setattr(release, "cleanup", cleanup)
    with pytest.raises(release.ReleaseError, match="create result unavailable"):
        release.Docker().job("image", name, "script", {}, tmp_path, "linux/amd64")
    assert len(observed) == 1
    assert containers == ({container_id: "foreign"} if foreign else {})


@pytest.mark.parametrize("failure", ["stall", "missing", "nonzero"])
def test_docker_cleanup_preserves_the_job_error(
    release: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    failure: str,
) -> None:
    container = "a" * 64

    def run(argv: Sequence[str], **kwargs: Any) -> str:
        if argv[1] == "create":
            return container
        assert list(argv) == ["docker", "start", "-a", container]
        raise release.ReleaseError("primary build failure")

    monkeypatch.setattr(release, "run", run)
    monkeypatch.setattr(release, "DOCKER_CLEANUP_TIMEOUT", 0.1, raising=False)
    monkeypatch.setenv("PATH", str(tmp_path))
    if failure != "missing":
        executable = tmp_path / "docker"
        body = "exec /bin/sleep 1" if failure == "stall" else "echo noise >&2; exit 7"
        executable.write_text(f"#!/bin/sh\n{body}\n")
        executable.chmod(0o755)

    with pytest.raises(release.ReleaseError, match="primary build failure"):
        release.Docker().job("image", "example", "script", {}, tmp_path, "linux/amd64")

    diagnostics = capsys.readouterr().err
    assert "docker kill" not in diagnostics
    assert f"release cleanup failed: docker rm -f {container}" in diagnostics
    assert {
        "stall": "did not finish within 0.1 s",
        "missing": "could not start",
        "nonzero": "exited 7",
    }[failure] in diagnostics
    assert "noise" not in diagnostics


@pytest.mark.parametrize("flood", [False, True])
def test_cleanup_bounds_container_lookup_output(
    release: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    flood: bool,
) -> None:
    executable = tmp_path / "docker"
    reply = f"{'a' * 64} example"
    if flood:
        executable.write_text("#!/bin/sh\nexec /usr/bin/head -c 2097152 /dev/zero\n")
    else:
        executable.write_text(f"#!/bin/sh\nprintf '%s\\n' '{reply}'\n")
    executable.chmod(0o755)
    monkeypatch.setenv("PATH", str(tmp_path))
    monkeypatch.setattr(release, "DOCKER_CLEANUP_TIMEOUT", 0.2 if flood else 5)

    container = release.Docker().find_owned_container("example", "job=owner")
    assert container == (None if flood else "a" * 64)
    diagnostics = capsys.readouterr().err
    if flood:
        assert "release cleanup failed: docker container ls" in diagnostics
        assert "\0" not in diagnostics
    else:
        assert diagnostics == ""


@pytest.mark.parametrize("lookup", ["absent", "present", "failed"])
def test_remove_image_skips_tags_that_were_never_built(
    release: ModuleType, monkeypatch: pytest.MonkeyPatch, lookup: str
) -> None:
    tag = "example-local-release:key-linux-amd64-commit-4242-abcd1234"
    calls: list[list[str]] = []

    def cleanup(argv: Sequence[str], *, read_output: bool = False) -> str | None:
        calls.append(list(argv))
        if read_output:
            assert list(argv) == ["docker", "image", "ls", "--quiet", tag]
            return {"absent": "", "present": "sha256:abc\n", "failed": None}[lookup]
        return None

    monkeypatch.setattr(release, "cleanup", cleanup)
    release.Docker().remove_image(tag)
    removal = ["docker", "image", "rm", tag]
    assert (removal in calls) == (lookup != "absent")


def test_macos_build(release: ModuleType, tmp_path: Path, which_rustup: None) -> None:
    config = release.parse_config(macos_data())
    host = FakeHost()
    out = release.build(
        False,
        tmp_path / "out",
        config=config,
        git=FakeGit(REPO),
        github=FakeGitHub(),
        host=host,
        version="1.2.3-rc.1",
        run_host_checks=no_host_checks,
        log=lambda line: None,
    )
    scripts = [script for script, _ in host.scripts]
    assert scripts[0] == (
        "rustup toolchain install 1.99.0 --profile minimal --component rustfmt"
        " --component clippy && rustup target add --toolchain 1.99.0 aarch64-apple-darwin"
    )
    assert scripts[1] == "cargo fetch --locked"
    assert scripts[2] == release.check_script(config)
    assert "macho --max-minos 14.0 --arch arm64" in scripts[3]
    check_env, build_env = host.scripts[2][1], host.scripts[3][1]
    assert check_env["EXAMPLE_REQUIRE"] == "1" and "EXAMPLE_REQUIRE" not in build_env
    assert build_env["MACOSX_DEPLOYMENT_TARGET"] == "14.0"
    assert build_env["RUSTUP_TOOLCHAIN"] == "1.99.0"
    # The pinned toolchain's own bin first, then rustup's directory.
    assert build_env["PATH"].startswith("/fake/toolchains/1.99.0/bin:/fake/cargo/bin")
    assert build_env["CARGO_NET_OFFLINE"] == "true"
    assert "macOS >= 14.0" in (out / "notes.md").read_text()

    github = FakeGitHub()
    release.publish(out, config=config, github=github, log=lambda line: None)
    assert github.created == [("rs-v1.2.3-rc.1", True)]


def test_pinned_toolchain_reads_rust_toolchain_toml(
    release: ModuleType, tmp_path: Path
) -> None:
    (tmp_path / "rust-toolchain.toml").write_text(
        '[toolchain]\nchannel = "1.99.0"\ncomponents = [\n    "rustfmt",\n    "clippy",\n]\n'
    )
    assert release.pinned_toolchain(tmp_path) == ("1.99.0", ["rustfmt", "clippy"])
    (tmp_path / "rust-toolchain.toml").write_text('[toolchain]\nchannel = "1.99; rm"\n')
    with pytest.raises(release.ReleaseError, match="no valid channel"):
        release.pinned_toolchain(tmp_path)
    (tmp_path / "rust-toolchain.toml").write_text('[toolchain]\nchannel = "--help"\n')
    with pytest.raises(release.ReleaseError, match="no valid channel"):
        release.pinned_toolchain(tmp_path)
    (tmp_path / "rust-toolchain.toml").write_text(
        '[toolchain]\nchannel = "1.99.0"\ncomponents = ["--force"]\n'
    )
    with pytest.raises(release.ReleaseError, match="invalid components"):
        release.pinned_toolchain(tmp_path)


def test_pinned_cargo_environment_wins_over_ambient_toolchains(
    release: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "rust-toolchain.toml").write_text('[toolchain]\nchannel = "1.99.0"\n')
    monkeypatch.setenv("RUSTUP_TOOLCHAIN", "1.100.0")
    monkeypatch.setattr(release.shutil, "which", lambda name: "/fake/bin/rustup")
    calls: list[list[str]] = []
    installed = {"yes": True}

    def run(argv: Sequence[str], **kwargs: Any) -> str:
        calls.append(list(argv))
        assert kwargs["env"]["RUSTUP_TOOLCHAIN"] == "1.99.0"
        if argv[1] == "which" and not installed["yes"]:
            raise release.ReleaseError("toolchain not installed")
        if argv[1] == "toolchain":
            installed["yes"] = True
        return "/fake/toolchains/1.99.0/bin/cargo\n"

    monkeypatch.setattr(release, "run", run)
    env = release.pinned_cargo_environment(tmp_path)
    assert env["RUSTUP_TOOLCHAIN"] == "1.99.0"
    assert env["PATH"].startswith("/fake/toolchains/1.99.0/bin")
    assert [call[1] for call in calls] == ["which"]  # installed: no network
    calls.clear()
    installed["yes"] = False
    release.pinned_cargo_environment(tmp_path)
    assert [call[1] for call in calls] == ["which", "toolchain", "which"]


def test_macos_build_refuses_an_unpinned_toolchain(
    release: ModuleType, tmp_path: Path, which_rustup: None
) -> None:
    class OtherRustc(FakeHost):
        def output(self, argv: Sequence[str], cwd: Path, env: Mapping[str, str]) -> str:
            if argv[0] == "rustup":
                return "/fake/toolchains/1.99.0/bin/cargo\n"
            return "rustc 1.100.0 (Homebrew)\n"

    with pytest.raises(release.ReleaseError, match="not the pinned toolchain"):
        release.build(
            False,
            tmp_path / "out",
            config=release.parse_config(macos_data()),
            git=FakeGit(REPO),
            github=FakeGitHub(),
            host=OtherRustc(),
            version="1.2.3",
            run_host_checks=no_host_checks,
            log=lambda line: None,
        )


# --- publish -----------------------------------------------------------------------------


@pytest.fixture
def built(release: ModuleType, tmp_path: Path) -> Path:
    return Path(
        release.build(
            False,
            tmp_path / "out",
            config=release.parse_config(linux_data()),
            git=FakeGit(REPO),
            github=FakeGitHub(),
            docker=FakeDocker(),
            version="1.2.3",
            run_host_checks=no_host_checks,
            log=lambda line: None,
        )
    )


def publish(release: ModuleType, out: Path, github: Any = None) -> None:
    release.publish(
        out,
        config=release.parse_config(linux_data()),
        github=github or FakeGitHub(),
        log=lambda line: None,
    )


def test_publish_refuses_a_dry_run_and_changed_builds(
    release: ModuleType, built: Path
) -> None:
    record = json.loads((built / "build.json").read_text())
    (built / "build.json").write_text(json.dumps({**record, "compare_released": True}))
    with pytest.raises(release.ReleaseError, match="never published"):
        publish(release, built)
    (built / "build.json").write_text(json.dumps({**record, "tag": "rs-v9.9.9"}))
    with pytest.raises(release.ReleaseError, match="inconsistent"):
        publish(release, built)
    (built / "build.json").write_text(json.dumps(record))
    (built / "stray.txt").write_text("x")
    with pytest.raises(
        release.ReleaseError, match=r"not part of the release: stray\.txt"
    ):
        publish(release, built)
    (built / "stray.txt").unlink()
    archive = built / record["archive"]
    archive.write_bytes(archive.read_bytes() + b"\0")
    with pytest.raises(release.ReleaseError, match="does not name sha256"):
        publish(release, built)


@pytest.mark.parametrize("kind", ["missing", "directory", "fifo"])
def test_publish_refuses_invalid_notes_before_tagging(
    release: ModuleType, built: Path, kind: str
) -> None:
    class RequirePreflight(FakeGitHub):
        def create_tag(self, tag: str, commit: str, message: str) -> None:
            pytest.fail("publish attempted to create a tag before checking notes")

    github = RequirePreflight()
    notes = built / "notes.md"
    notes.unlink()
    if kind == "directory":
        notes.mkdir()
    elif kind == "fifo":
        os.mkfifo(notes)
    with pytest.raises(release.ReleaseError, match=r"notes\.md.*regular"):
        publish(release, built, github)
    assert github.tagged == github.created == []


def test_publish_uses_notes_staged_before_tagging(
    release: ModuleType, built: Path
) -> None:
    class ChangeNotesOnTag(FakeGitHub):
        def create_tag(self, tag: str, commit: str, message: str) -> None:
            super().create_tag(tag, commit, message)
            (built / "notes.md").unlink()

    github = ChangeNotesOnTag()
    publish(release, built, github)
    assert github.created == [("rs-v1.2.3", False)]


def test_publish_never_moves_a_tag(release: ModuleType, built: Path) -> None:
    github = FakeGitHub()
    github.tags["rs-v1.2.3"] = OTHER
    with pytest.raises(release.ReleaseError, match=f"points at {OTHER}"):
        publish(release, built, github=github)
    with pytest.raises(release.ReleaseError, match="is not on main on GitHub"):
        publish(release, built, github=FakeGitHub(on_main=False))


def test_publish_completes_and_checks_a_partial_release(
    release: ModuleType, built: Path
) -> None:
    record = json.loads((built / "build.json").read_text())
    archive = record["archive"]
    github = FakeGitHub()
    github.releases["rs-v1.2.3"] = {archive: (built / archive).read_bytes()}
    publish(release, built, github=github)
    assert github.uploaded == [f"{archive}.sha256"]
    github.releases["rs-v1.2.3"]["other.tar.gz"] = b"x"
    with pytest.raises(release.ReleaseError, match=r"should not have: other\.tar\.gz"):
        publish(release, built, github=github)
    del github.releases["rs-v1.2.3"]["other.tar.gz"]
    github.corrupt = True
    with pytest.raises(release.ReleaseError, match="differs from the local file"):
        publish(release, built, github=github)


@pytest.mark.parametrize("difference", ["none", "last byte", "length"])
def test_downloaded_assets_are_compared_in_bounded_reads(
    release: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    difference: str,
) -> None:
    archive = tmp_path / "large.tar.gz"

    def write_sparse(path: Path, size: int, last: bytes) -> None:
        with path.open("wb") as handle:
            handle.seek(size - 1)
            handle.write(last)

    size = 16 * 1024 * 1024 + 1
    write_sparse(archive, size, b"a")
    digest = release.sha256_file(archive)

    class SparseGitHub(FakeGitHub):
        def release(self, tag: str) -> Any:
            return release.Release(
                100, {archive.name: digest}, {archive.name: "asset-1"}, False, False
            )

        def download(self, tag: str, directory: Path) -> None:
            write_sparse(
                directory / archive.name,
                size - (difference == "length"),
                b"b" if difference == "last byte" else b"a",
            )

    original_open = Path.open

    def bounded_open(path: Path, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
        handle = original_open(path, mode, *args, **kwargs)
        if mode == "rb" and path.name == archive.name:
            read = handle.read

            def bounded_read(size: int = -1) -> bytes:
                assert 0 < size <= 1024 * 1024
                return read(size)

            monkeypatch.setattr(handle, "read", bounded_read)
        return handle

    monkeypatch.setattr(Path, "open", bounded_open)
    args = (SparseGitHub(), "rs-v1.2.3", "title", tmp_path / "notes.md")
    if difference == "none":
        assert release.ensure_release(*args, {archive.name: archive})[-1] == (
            "verified 1 assets of rs-v1.2.3 byte for byte"
        )
    else:
        with pytest.raises(release.ReleaseError, match="differs from the local file"):
            release.ensure_release(*args, {archive.name: archive})


def test_ensure_release_refuses_a_different_digest(
    release: ModuleType, tmp_path: Path
) -> None:
    path = tmp_path / "a.tar.gz"
    path.write_bytes(b"a")

    class Digests(FakeGitHub):
        def release(self, tag: str) -> Any:
            return SimpleNamespace(
                id=1,
                assets={"a.tar.gz": "0" * 64},
                asset_ids={"a.tar.gz": "x"},
                draft=False,
                prerelease=False,
            )

    with pytest.raises(release.ReleaseError, match="has sha256 0000"):
        release.ensure_release(Digests(), "t", "t", path, {"a.tar.gz": path})


def test_publish_refuses_a_draft_left_by_an_interrupted_create(
    release: ModuleType, built: Path
) -> None:
    record = json.loads((built / "build.json").read_text())
    archive = record["archive"]
    github = FakeGitHub()
    github.releases["rs-v1.2.3"] = {archive: (built / archive).read_bytes()}
    github.drafts.add("rs-v1.2.3")
    with pytest.raises(release.ReleaseError, match="is a draft"):
        publish(release, built, github=github)
    assert github.uploaded == [] and github.created == []


def test_publish_refuses_a_release_replaced_meanwhile_or_flagged_wrong(
    release: ModuleType, built: Path
) -> None:
    github = FakeGitHub()
    github.replace_after_download = True
    with pytest.raises(release.ReleaseError, match="changed during the verification"):
        publish(release, built, github=github)
    github = FakeGitHub()
    github.swap_asset_after_download = True
    with pytest.raises(release.ReleaseError, match="changed during the verification"):
        publish(release, built, github=github)
    github = FakeGitHub()
    github.created_as_draft = True
    with pytest.raises(release.ReleaseError, match="is not published"):
        publish(release, built, github=github)
    github = FakeGitHub()
    github.no_digests = True
    with pytest.raises(release.ReleaseError, match="reports sha256 none"):
        publish(release, built, github=github)
    github = FakeGitHub()
    publish(release, built, github=github)
    github.prereleases.add("rs-v1.2.3")
    with pytest.raises(release.ReleaseError, match="expected no prerelease"):
        publish(release, built, github=github)


def test_publish_refuses_a_partial_release_with_the_wrong_prerelease_flag(
    release: ModuleType, built: Path
) -> None:
    record = json.loads((built / "build.json").read_text())
    archive = record["archive"]
    github = FakeGitHub()
    github.releases["rs-v1.2.3"] = {archive: (built / archive).read_bytes()}
    github.prereleases.add("rs-v1.2.3")
    with pytest.raises(release.ReleaseError, match="settle it by hand"):
        publish(release, built, github=github)
    assert github.uploaded == []  # refused before anything was uploaded


def test_publish_checks_main_also_for_an_existing_tag(
    release: ModuleType, built: Path
) -> None:
    github = FakeGitHub(on_main=False)
    github.tags["rs-v1.2.3"] = COMMIT
    with pytest.raises(release.ReleaseError, match="is not on main on GitHub"):
        publish(release, built, github=github)


def test_build_never_mixes_into_an_output_filled_meanwhile(
    release: ModuleType, tmp_path: Path
) -> None:
    out = tmp_path / "out"

    class Racing(FakeDocker):
        def job(
            self,
            image: str,
            name: str,
            script: str,
            env: Mapping[str, str],
            out_dir: Path,
            platform: str,
        ) -> None:
            super().job(image, name, script, env, out_dir, platform)
            if "cargo build --release" in script:
                out.mkdir()
                (out / "build.json").write_text("other build")

    with pytest.raises(release.ReleaseError, match="filled by another build"):
        release.build(
            False,
            out,
            config=release.parse_config(linux_data()),
            git=FakeGit(REPO),
            github=FakeGitHub(),
            docker=Racing(),
            version="1.2.3",
            run_host_checks=no_host_checks,
            log=lambda line: None,
        )
    assert [path.name for path in out.iterdir()] == ["build.json"]
    assert [path.name for path in tmp_path.iterdir()] == ["out"]


class FakeApi:
    """Stands in for `run` behind `GitHub.api`: answers per API path."""

    def __init__(self, answers: dict[str, Any]) -> None:
        self.answers = answers
        self.calls: list[list[str]] = []

    def __call__(self, argv: Sequence[str], **kwargs: Any) -> str:
        self.calls.append(list(argv))
        assert list(argv[:4]) == ["gh", "api", "--hostname", "github.com"]
        path = next(arg for arg in argv[4:] if arg.startswith("repos/"))
        answer = self.answers.get(path)
        if answer is None:
            raise ApiNotFoundError(f"gh api {path} exited 1: gh: Not Found (HTTP 404)")
        return json.dumps(answer)


class ApiNotFoundError(Exception):
    pass


@pytest.fixture
def api(release: ModuleType, monkeypatch: pytest.MonkeyPatch) -> Any:
    def install(answers: dict[str, Any]) -> FakeApi:
        fake = FakeApi(answers)

        def run(argv: Sequence[str], **kwargs: Any) -> str:
            try:
                return fake(argv, **kwargs)
            except ApiNotFoundError as error:
                raise release.ReleaseError(str(error)) from None

        monkeypatch.setattr(release, "run", run)
        return fake

    return install


REPO_API = "repos/ephes/example"


def test_github_tag_needs_an_annotated_tag_on_a_commit(
    release: ModuleType, api: Any
) -> None:
    github = release.GitHub("ephes/example")
    api({})
    assert github.tag("v1") is None
    ref = {"ref": "refs/tags/v1", "object": {"type": "tag", "sha": OTHER}}
    api(
        {
            f"{REPO_API}/git/ref/tags/v1": ref,
            f"{REPO_API}/git/tags/{OTHER}": {
                "object": {"type": "commit", "sha": COMMIT}
            },
        }
    )
    assert github.tag("v1") == COMMIT
    light = {"ref": "refs/tags/v1", "object": {"type": "commit", "sha": COMMIT}}
    api({f"{REPO_API}/git/ref/tags/v1": light})
    with pytest.raises(release.ReleaseError, match="not an annotated tag"):
        github.tag("v1")
    api(
        {
            f"{REPO_API}/git/ref/tags/v1": ref,
            f"{REPO_API}/git/tags/{OTHER}": {"object": {"type": "tree", "sha": COMMIT}},
        }
    )
    with pytest.raises(release.ReleaseError, match="does not point at a commit"):
        github.tag("v1")
    # A prefix match (GitHub answers refs/tags/v10 for v1 in some cases) is refused.
    api({f"{REPO_API}/git/ref/tags/v1": {**ref, "ref": "refs/tags/v10"}})
    with pytest.raises(release.ReleaseError, match="not an annotated tag"):
        github.tag("v1")


def test_github_on_main_and_create_tag(release: ModuleType, api: Any) -> None:
    github = release.GitHub("ephes/example")
    main = {"ref": "refs/heads/main", "object": {"type": "commit", "sha": OTHER}}
    heads = f"{REPO_API}/git/ref/heads/main"
    compare = f"{REPO_API}/compare/{COMMIT}...{OTHER}"
    for status, expected in (("ahead", True), ("identical", True), ("diverged", False)):
        api({heads: main, compare: {"status": status}})
        assert github.on_main(COMMIT) is expected
    api({heads: main})
    assert github.on_main(COMMIT) is False
    # Only the exact branch ref counts (a bare `main` could resolve to a tag).
    for ref in (
        {**main, "ref": "refs/heads/main-old"},
        {**main, "object": {"type": "tag"}},
        {**main, "object": None},
        {**main, "object": {"type": "commit", "sha": "nope"}},
        {**main, "object": {"type": "commit", "sha": 10**39}},
        [main],
    ):
        fake = api({heads: ref})
        with pytest.raises(release.ReleaseError, match="no branch main"):
            github.on_main(COMMIT)
        assert len(fake.calls) == 1  # refused before any compare request
    api({})
    with pytest.raises(release.ReleaseError, match="no branch main"):
        github.on_main(COMMIT)
    fake = api({f"{REPO_API}/git/tags": {"sha": OTHER}, f"{REPO_API}/git/refs": {}})
    github.create_tag("v1", COMMIT, "example 1")
    tag_call, ref_call = fake.calls
    assert ["--method", "POST", f"{REPO_API}/git/tags"] == tag_call[4:7]
    assert "object=" + COMMIT in tag_call and "type=commit" in tag_call
    assert ["-f", "ref=refs/tags/v1", "-f", f"sha={OTHER}"] == ref_call[7:]
    api({f"{REPO_API}/git/tags": {"sha": "nope"}})
    with pytest.raises(release.ReleaseError, match="no tag object"):
        github.create_tag("v1", COMMIT, "example 1")


def test_ensure_tag_creates_checks_and_never_moves(release: ModuleType) -> None:
    github = FakeGitHub()
    assert (
        release.ensure_tag(github, "v1", COMMIT, "m")
        == f"created tag v1 -> {COMMIT} on GitHub"
    )
    assert release.ensure_tag(github, "v1", COMMIT, "m") == "tag v1 already on GitHub"
    with pytest.raises(release.ReleaseError, match=f"points at {COMMIT}"):
        release.ensure_tag(github, "v1", OTHER, "m")
    assert github.tagged == [("v1", COMMIT)]


@pytest.mark.parametrize(
    ("command", "budget"),
    [("api", 60), ("view", 60), ("create", 1800), ("upload", 1800), ("download", 1800)],
)
def test_github_network_commands_report_timeout(
    release: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    command: str,
    budget: int,
) -> None:
    def stall(argv: Sequence[str], *, timeout: float | None, **kwargs: Any) -> Any:
        assert timeout == budget
        assert argv[0] == "gh" and command in argv
        raise subprocess.TimeoutExpired(argv, timeout)

    github = release.GitHub("ephes/example")
    tag = "rs-v1.2.3"
    monkeypatch.setattr(release.subprocess, "run", stall)
    with pytest.raises(release.ReleaseError, match=f"did not finish within {budget} s"):
        if command == "api":
            github.api(REPO_API)
        elif command == "view":
            github.release(tag)
        elif command == "create":
            github.create(tag, "title", tmp_path / "notes.md", [], False)
        elif command == "upload":
            github.upload(tag, [tmp_path / "archive"])
        else:
            github.download(tag, tmp_path)


def test_publish_reconciles_upload_committed_before_timeout(
    release: ModuleType, built: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    record = json.loads((built / "build.json").read_text())
    archive = record["archive"]
    tag = record["tag"]
    client = release.GitHub("ephes/example")

    class PartialGitHub(FakeGitHub):
        def upload(self, tag: str, files: list[Path]) -> None:
            super().upload(tag, files)
            client.upload(tag, files)

    def lose_upload_response(
        argv: Sequence[str], *, timeout: float, **kwargs: Any
    ) -> Any:
        assert list(argv[:3]) == ["gh", "release", "upload"]
        assert timeout == 1800
        raise subprocess.TimeoutExpired(argv, timeout)

    github = PartialGitHub()
    github.releases[tag] = {archive: (built / archive).read_bytes()}
    monkeypatch.setattr(release.subprocess, "run", lose_upload_response)
    with pytest.raises(release.ReleaseError, match="did not finish within 1800 s"):
        publish(release, built, github)
    publish(release, built, github)
    assert github.tagged == [(tag, COMMIT)]
    assert github.created == []
    assert github.uploaded == [f"{archive}.sha256"]


def test_gh_version_and_api_host_are_checked(
    release: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    github = release.GitHub("ephes/example")

    def setup(version: str, code: int, stdout: str = "", stderr: str = "") -> None:
        monkeypatch.setattr(release, "run", lambda argv, **kwargs: version)

        def config(argv: Sequence[str], **kwargs: Any) -> Any:
            assert list(argv) == [
                "gh",
                "config",
                "get",
                "--host",
                "github.com",
                "api_host",
            ]
            return subprocess.CompletedProcess(argv, code, stdout, stderr)

        monkeypatch.setattr(release.subprocess, "run", config)

    setup("gh version 2.104.0 (2026)", 0, "\n")
    github.check_cli()
    # gh 2.74.0 reports an unset key as an error.
    setup("gh version 2.74.0 (2025)", 1, stderr='could not find key "api_host"\n')
    github.check_cli()
    setup("gh version 2.73.9 (2025)", 0)
    with pytest.raises(release.ReleaseError, match=r"2\.74\.0 or newer"):
        github.check_cli()
    setup("gh version 2.104.0 (2026)", 0, "api.example.test\n")
    with pytest.raises(release.ReleaseError, match="api_host override"):
        github.check_cli()
    setup("gh version 2.104.0 (2026)", 2, stderr="broken config")
    with pytest.raises(release.ReleaseError, match="exited 2"):
        github.check_cli()

    def missing(argv: Sequence[str], **kwargs: Any) -> Any:
        raise FileNotFoundError(2, "No such file or directory", "gh")

    monkeypatch.setattr(release.subprocess, "run", missing)
    with pytest.raises(release.ReleaseError, match="cannot run gh"):
        github.check_cli()


def test_main_reports_refusals(
    release: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def refuse(*args: Any, **kwargs: Any) -> None:
        raise release.ReleaseError("nope")

    monkeypatch.setattr(release, "build", refuse)
    assert release.main(["build"]) == 1
    assert "release failed: nope" in capsys.readouterr().err
