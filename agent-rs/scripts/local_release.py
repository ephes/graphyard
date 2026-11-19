#!/usr/bin/env python3
"""Local release build and publication of the Rust binary (README "Release").

It replaces the tag-triggered GitHub Actions release workflow: the same
checks run on this machine, and ``release_checks.py`` next to this file still
makes every release decision (tag and version, glibc floor or Mach-O checks,
smoke test, deterministic archive). This file only orchestrates. The same file
is used by mailgun-relay, opsgate, graphyard and nyxmon; what differs lives in
``../local-release.json``. Stdlib only.

Commands (run from a clean checkout of ``main``, see the README):

- ``build [--compare-released]``: build the crate's current version from the
  checked-out commit into ``<crate dir>/target/local-release/<binary>-<version>/``
  (archive, ``.sha256``, ``build.json``, ``notes.md``). Publishes nothing.
- ``publish``: create the annotated tag on the build commit through the
  GitHub API, create the GitHub release with exactly the archive and its
  ``.sha256``, then download both again and compare them byte for byte.
  Repeatable until that end state is reached.

Modes (``local-release.json`` ``mode``):

- ``linux-container``: the workspace checks (fmt, clippy, tests) run in an
  ``ubuntu:22.04`` container of the Docker engine's own platform; the release
  build, glibc floor check, smoke test and archive run in a ``linux/amd64``
  ``ubuntu:22.04`` container (glibc 2.35, as the old runner). Only the image
  phase has network; jobs run with ``--network none`` and resource limits.
- ``macos-native``: the release target is the Mac itself
  (``aarch64-apple-darwin``); everything runs on the host in a temporary
  extract of the build commit, with the pinned toolchain from rustup.

Every remote fact (is the commit on ``main``, does the tag exist) is read
from, and the tag is created on, the configured GitHub repository through
``gh api --hostname github.com``; nothing is pushed with git. A version is
*taken* when its tag exists there. ``build`` refuses a
taken version unless ``--compare-released`` is given (a dry run that
``publish`` refuses). Nothing here moves or deletes a tag or replaces a
release asset.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import shlex
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import tomllib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SCRIPTS = Path(__file__).resolve().parent
CRATE = SCRIPTS.parent
REPO = CRATE.parent
CONFIG_PATH = CRATE / "local-release.json"

MAIN = "main"
AMD64 = "linux/amd64"
MODES = ("linux-container", "macos-native")
RUNNER_IMAGE = "ubuntu:22.04"
BASE_PACKAGES = (
    "binutils",
    "ca-certificates",
    "curl",
    "gcc",
    "git",
    "libc6-dev",
    "python3",
)
# 1.29.2 fixes CVE-2026-111943 (certificate verification of its downloads).
RUSTUP_VERSION = "1.29.2"
# rustup-init per container platform: target triple and sha256.
RUSTUP = {
    AMD64: (
        "x86_64-unknown-linux-gnu",
        # pragma: allowlist nextline secret
        "5be191e8c70a5007074254cb74d78dbfdd25cd5a61c27de7b8e5410122ec24f4",
    ),
    "linux/arm64": (
        "aarch64-unknown-linux-gnu",
        # pragma: allowlist nextline secret
        "a9c168dbea81a84671c6b7a5c1795cb0eb699020110d107ddfca427bd797e697",
    ),
}
# The OS trust store the image downloads rustup and the toolchain with: at
# least the ca-certificates of USN-8944-1 for ubuntu 22.04, and neither root
# of the 2026-10-21 incident (SHA-256 of the DER certificate), as the hosted
# release job's scripts/ci_rustup.py required.
CA_CERTIFICATES_MINIMUM = "20260816ubuntu0.22.04.1"
BLOCKED_ROOTS = (
    "d7a7a0fb5d7e2731d771e9484ebcdef71d5f0c3e0a2948782bc83ee0ea699ef4",  # pragma: allowlist secret
    "86a1ecba089c4a8d3bbe2734c612ba341d813e043cf9e8a862cd5c57a36bbe6b",  # pragma: allowlist secret
)
TRUST_CHECK = (  # one line: it goes into a Dockerfile RUN instruction
    "import hashlib, re, ssl, sys; "
    "text = open('/etc/ssl/certs/ca-certificates.crt').read(); "
    "pems = re.findall('-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----', text, re.S); "
    "found = {hashlib.sha256(ssl.PEM_cert_to_DER_cert(pem)).hexdigest() for pem in pems}; "
    "blocked = sorted(found & set(sys.argv[1:])); "
    "sys.exit('the CA bundle is empty or holds an incident root: ' + ' '.join(blocked)) "
    "if not pems or blocked else None"
)
MEMORY = "8g"
PIDS = "4096"
IMAGE_TIMEOUT = 3600
JOB_TIMEOUT = 5400
CONTAINER_CREATE_TIMEOUT = 60
CONTAINER_ID = re.compile(r"[0-9a-f]{64}")
DIST = "local-release"
BUILD_JSON = "build.json"
NOTES = "notes.md"

APT_PACKAGE = re.compile(r"[a-z0-9][a-z0-9+.-]+")
COMMIT = re.compile(r"[0-9a-f]{40}")
VERSION = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?")
NAME = re.compile(r"[a-z][a-z0-9-]*")
ENV_NAME = re.compile(r"[A-Z][A-Z0-9_]*")
TARGET = re.compile(r"[a-z0-9_]+-[a-z0-9_]+-[a-z0-9_]+(?:-[a-z0-9_]+)?")
REPOSITORY = re.compile(r"[A-Za-z0-9-]+/[A-Za-z0-9._-]+")
DOTTED = re.compile(r"[0-9]+\.[0-9]+")

Log = Callable[[str], None]


class ReleaseError(Exception):
    """A release step refused or failed; the message is the reason."""


# --- configuration -------------------------------------------------------------


@dataclass(frozen=True)
class Config:
    github_repository: str
    binary: str
    tag_prefix: str
    target: str
    mode: str
    commit_env: str
    checks: tuple[str, ...]
    check_env: dict[str, str]
    host_checks: tuple[tuple[str, ...], ...]
    source_paths: tuple[str, ...]
    apt_packages: tuple[str, ...]
    glibc_floor: str | None
    max_minos: str | None
    macho_arch: str | None

    def tag(self, version: str) -> str:
        return f"{self.tag_prefix}{version}"

    def archive(self, version: str) -> str:
        return f"{self.binary}-{version}-{self.target}.tar.gz"


def _strings(
    data: Mapping[str, Any], key: str, default: Sequence[str] = ()
) -> tuple[str, ...]:
    value = data.get(key, list(default))
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item for item in value
    ):
        raise ReleaseError(
            f"local-release.json: {key} must be a list of non-empty strings"
        )
    return tuple(value)


def _optional(data: Mapping[str, Any], key: str) -> str | None:
    value = data.get(key)
    if value is not None and (
        not isinstance(value, str) or not DOTTED.fullmatch(value)
    ):
        raise ReleaseError(f"local-release.json: {key} must be a version such as 2.35")
    return value


def parse_config(data: Any) -> Config:
    if not isinstance(data, dict):
        raise ReleaseError("local-release.json: not an object")
    known = {
        "comment",
        "github_repository",
        "binary",
        "tag_prefix",
        "target",
        "mode",
        "commit_env",
        "checks",
        "check_env",
        "host_checks",
        "source_paths",
        "apt_packages",
        "glibc_floor",
        "max_minos",
        "macho_arch",
    }
    unknown = sorted(set(data) - known)
    if unknown:
        raise ReleaseError(f"local-release.json: unknown keys {', '.join(unknown)}")
    patterns = {
        "github_repository": REPOSITORY,
        "binary": NAME,
        "tag_prefix": re.compile(r"[a-z][a-z0-9-]*-v"),
        "target": TARGET,
        "mode": re.compile("|".join(MODES)),
        "commit_env": ENV_NAME,
    }
    for key, pattern in patterns.items():
        value = data.get(key)
        if not isinstance(value, str) or not pattern.fullmatch(value):
            raise ReleaseError(f"local-release.json: {key} is missing or malformed")
    check_env = data.get("check_env", {})
    if not isinstance(check_env, dict) or not all(
        isinstance(key, str) and ENV_NAME.fullmatch(key) and isinstance(value, str)
        for key, value in check_env.items()
    ):
        raise ReleaseError(
            "local-release.json: check_env must map variable names to strings"
        )
    host_checks = data.get("host_checks", [])
    if not isinstance(host_checks, list) or not all(
        isinstance(argv, list) and argv and all(isinstance(arg, str) for arg in argv)
        for argv in host_checks
    ):
        raise ReleaseError(
            "local-release.json: host_checks must be a list of argv lists"
        )
    crate_dir = CRATE.name
    config = Config(
        github_repository=data["github_repository"],
        binary=data["binary"],
        tag_prefix=data["tag_prefix"],
        target=data["target"],
        mode=data["mode"],
        commit_env=data["commit_env"],
        checks=_strings(data, "checks"),
        check_env=dict(check_env),
        host_checks=tuple(tuple(argv) for argv in host_checks),
        source_paths=_strings(data, "source_paths", (crate_dir, "LICENSE")),
        apt_packages=_strings(data, "apt_packages"),
        glibc_floor=_optional(data, "glibc_floor"),
        max_minos=_optional(data, "max_minos"),
        macho_arch=data.get("macho_arch"),
    )
    if not config.checks:
        raise ReleaseError("local-release.json: checks must name the workspace checks")
    for package in config.apt_packages:
        if not APT_PACKAGE.fullmatch(package):
            raise ReleaseError(
                f"local-release.json: {package!r} is not an apt package name"
            )
    for path in config.source_paths:
        if path.startswith("/") or ".." in Path(path).parts:
            raise ReleaseError(
                f"local-release.json: source path {path!r} leaves the repository"
            )
    if crate_dir not in config.source_paths or "LICENSE" not in config.source_paths:
        raise ReleaseError(
            "local-release.json: source_paths must hold the crate dir and LICENSE"
        )
    if config.mode == "linux-container":
        if config.glibc_floor is None or config.max_minos or config.macho_arch:
            raise ReleaseError(
                "local-release.json: linux-container needs glibc_floor only"
            )
        if config.target != "x86_64-unknown-linux-gnu":
            raise ReleaseError(
                "local-release.json: linux-container builds x86_64-unknown-linux-gnu"
            )
    else:
        if config.glibc_floor or config.apt_packages:
            raise ReleaseError(
                "local-release.json: macos-native has no glibc_floor or apt_packages"
            )
        if config.max_minos is None or config.macho_arch not in ("arm64", "x86_64"):
            raise ReleaseError(
                "local-release.json: macos-native needs max_minos and macho_arch"
            )
        if not config.target.endswith("-apple-darwin"):
            raise ReleaseError(
                "local-release.json: macos-native builds an apple-darwin target"
            )
    return config


def load_config(path: Path = CONFIG_PATH) -> Config:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise ReleaseError(f"cannot read {path}: {error}") from None
    return parse_config(data)


# --- processes -------------------------------------------------------------------


def run(
    argv: Sequence[str],
    *,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    timeout: float | None = None,
    capture: bool = True,
) -> str:
    """Run a command; return its stdout, raise ReleaseError on failure."""
    try:
        result = subprocess.run(  # noqa: S603 - argv lists only
            list(argv),
            cwd=cwd,
            env=dict(env) if env is not None else None,
            timeout=timeout,
            check=False,
            text=True,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.PIPE if capture else None,
        )
    except subprocess.TimeoutExpired:
        raise ReleaseError(
            f"{shlex.join(argv)} did not finish within {timeout} s"
        ) from None
    except OSError as error:
        raise ReleaseError(f"cannot run {argv[0]}: {error}") from None
    if result.returncode != 0:
        detail = (result.stderr or "").strip() if capture else ""
        message = f"{shlex.join(argv)} exited {result.returncode}"
        raise ReleaseError(message + (f": {detail}" if detail else ""))
    return result.stdout if capture else ""


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def same_file_bytes(first: Path, second: Path) -> bool:
    with first.open("rb") as left, second.open("rb") as right:
        while True:
            block = left.read(1 << 20)
            if block != right.read(1 << 20):
                return False
            if not block:
                return True


def quietly(argv: Sequence[str]) -> None:
    """Best-effort cleanup: the outcome is ignored."""
    subprocess.run(list(argv), check=False, capture_output=True)  # noqa: S603 - argv lists only


# --- git -------------------------------------------------------------------------


class Git:
    """The local checkout: only read. Tags are created on GitHub through its
    API (`GitHub.create_tag`), never pushed with git, so no git remote, URL
    rewrite or configuration can send a tag anywhere else."""

    def __init__(self, repo: Path) -> None:
        self.repo = repo

    def __call__(self, *args: str) -> str:
        return run(["git", "-C", str(self.repo), *args])

    def head(self) -> str:
        return self("rev-parse", "HEAD").strip()

    def is_clean(self) -> bool:
        return self("status", "--porcelain", "--untracked-files=normal") == ""

    def commit_time(self, commit: str) -> int:
        return int(self("log", "-1", "--format=%ct", commit).strip())

    def archive(self, commit: str, paths: Sequence[str], output: Path) -> None:
        self("archive", "--format=tar", "-o", str(output), commit, *paths)


# --- GitHub ----------------------------------------------------------------------


@dataclass(frozen=True)
class Release:
    id: int
    assets: dict[str, str]
    asset_ids: dict[str, str]
    draft: bool
    prerelease: bool


GH_MINIMUM = (2, 74, 0)


class GitHub:
    """The `gh` calls on releases; tests replace it with a fake. The
    repository is always qualified with github.com, so `GH_HOST` cannot send
    a call to another host."""

    def __init__(self, repository: str) -> None:
        self.repository = repository
        self.qualified = f"github.com/{repository}"

    def release(self, tag: str) -> Release | None:
        """The release's id, assets (name -> sha256, '' when GitHub reports
        none), draft and prerelease state, or None without a release."""
        fields = "databaseId,assets,isDraft,isPrerelease"
        argv = ["gh", "release", "view", tag, "-R", self.qualified, "--json", fields]
        try:
            data = json.loads(run(argv))
        except ReleaseError as error:
            if "release not found" in str(error):
                return None
            raise
        assets = {
            asset["name"]: (asset.get("digest") or "").removeprefix("sha256:")
            for asset in data["assets"]
        }
        return Release(
            id=int(data["databaseId"]),
            assets=assets,
            asset_ids={asset["name"]: str(asset["id"]) for asset in data["assets"]},
            draft=bool(data["isDraft"]),
            prerelease=bool(data["isPrerelease"]),
        )

    def api(self, *args: str) -> Any:
        """`gh api` on github.com (never `GH_HOST`); None for HTTP 404."""
        try:
            text = run(["gh", "api", "--hostname", "github.com", *args])
        except ReleaseError as error:
            if "HTTP 404" in str(error):
                return None
            raise
        return json.loads(text) if text.strip() else {}

    def on_main(self, commit: str) -> bool:
        """Whether the commit is on the repository's main branch on GitHub:
        `refs/heads/main` resolved exactly (a bare `main` could name a tag),
        then compared by commit."""
        ref = self.api(f"repos/{self.repository}/git/ref/heads/{MAIN}")
        ref = ref if isinstance(ref, dict) else {}
        target = ref.get("object")
        target = target if isinstance(target, dict) else {}
        head = target.get("sha")
        if (
            ref.get("ref") != f"refs/heads/{MAIN}"
            or target.get("type") != "commit"
            or not isinstance(head, str)
            or not COMMIT.fullmatch(head)
        ):
            raise ReleaseError(f"no branch {MAIN} on github.com/{self.repository}")
        data = self.api(f"repos/{self.repository}/compare/{commit}...{head}")
        return data is not None and data.get("status") in ("ahead", "identical")

    def tag(self, tag: str) -> str | None:
        """The commit the annotated tag points at on GitHub, or None without
        the tag; refuses a lightweight tag or one on anything but a commit."""
        ref = self.api(f"repos/{self.repository}/git/ref/tags/{tag}")
        if ref is None:
            return None
        target = ref.get("object", {})
        if ref.get("ref") != f"refs/tags/{tag}" or target.get("type") != "tag":
            raise ReleaseError(
                f"tag {tag} on GitHub is not an annotated tag; settle it by hand"
            )
        tag_object = self.api(f"repos/{self.repository}/git/tags/{target['sha']}")
        tagged = (tag_object or {}).get("object", {})
        if tagged.get("type") != "commit" or not COMMIT.fullmatch(
            str(tagged.get("sha"))
        ):
            raise ReleaseError(f"tag {tag} on GitHub does not point at a commit")
        return str(tagged["sha"])

    def create_tag(self, tag: str, commit: str, message: str) -> None:
        """Create the annotated tag object and its ref; creating an existing
        ref fails, so a tag is never moved."""
        created = self.api(
            "--method",
            "POST",
            f"repos/{self.repository}/git/tags",
            "-f",
            f"tag={tag}",
            "-f",
            f"message={message}",
            "-f",
            f"object={commit}",
            "-f",
            "type=commit",
        )
        sha = str((created or {}).get("sha", ""))
        if not COMMIT.fullmatch(sha):
            raise ReleaseError(f"GitHub created no tag object for {tag}")
        self.api(
            "--method",
            "POST",
            f"repos/{self.repository}/git/refs",
            "-f",
            f"ref=refs/tags/{tag}",
            "-f",
            f"sha={sha}",
        )

    def check_cli(self) -> None:
        """gh 2.74.0 or newer (older clients report no asset digests), and no
        `api_host` override for github.com in gh's configuration, which would
        send the API calls (and the token) to another server."""
        argv = ["gh", "config", "get", "--host", "github.com", "api_host"]
        try:
            result = subprocess.run(  # noqa: S603 - argv list only
                argv, check=False, capture_output=True, text=True, timeout=600
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise ReleaseError(f"cannot run gh: {error}") from None
        if result.returncode == 0:
            override = result.stdout.strip()
        elif "could not find key" in result.stderr:
            override = ""  # older gh: an unset key is an error
        else:
            raise ReleaseError(f"{shlex.join(argv)} exited {result.returncode}")
        if override:
            raise ReleaseError(
                "gh has an api_host override for github.com; refusing to publish"
            )
        text = run(["gh", "--version"])
        match = re.match(r"gh version ([0-9]+)\.([0-9]+)\.([0-9]+)", text)
        if match is None or tuple(map(int, match.groups())) < GH_MINIMUM:
            raise ReleaseError(
                f"gh {'.'.join(map(str, GH_MINIMUM))} or newer is needed (asset digests)"
            )

    def create(
        self, tag: str, title: str, notes: Path, files: list[Path], prerelease: bool
    ) -> None:
        argv = ["gh", "release", "create", tag, *map(str, files), "-R", self.qualified]
        argv += ["--verify-tag", "--title", title, "--notes-file", str(notes)]
        if prerelease:
            argv.append("--prerelease")
        run(argv)

    def upload(self, tag: str, files: list[Path]) -> None:
        # Never --clobber: an existing asset is never replaced.
        run(["gh", "release", "upload", tag, *map(str, files), "-R", self.qualified])

    def download(self, tag: str, directory: Path) -> None:
        run(
            [
                "gh",
                "release",
                "download",
                tag,
                "-R",
                self.qualified,
                "-D",
                str(directory),
            ]
        )


def check_assets(tag: str, release: Release, expected: Mapping[str, str]) -> None:
    """No asset beyond the expected ones, and none with another sha256."""
    unexpected = sorted(set(release.assets) - set(expected))
    if unexpected:
        raise ReleaseError(
            f"release {tag} has assets it should not have: {', '.join(unexpected)}"
        )
    for name, digest in sorted(release.assets.items()):
        if digest and digest != expected[name]:
            raise ReleaseError(
                f"release {tag}: asset {name} has sha256 {digest}, expected {expected[name]}"
            )


def check_complete(
    tag: str, before: Release, now: Release | None, expected: Mapping[str, str]
) -> Release:
    """`now` is the same release as `before`, holding the same asset ids, and
    each asset's sha256 as GitHub reports it is the expected one."""
    if now is None or now.id != before.id or now.asset_ids != before.asset_ids:
        raise ReleaseError(f"release {tag} changed during the verification")
    if sorted(now.assets) != sorted(expected):
        raise ReleaseError(
            f"release {tag} holds {sorted(now.assets)}, expected {sorted(expected)}"
        )
    for name, digest in sorted(now.assets.items()):
        if digest != expected[name]:
            raise ReleaseError(
                f"release {tag}: GitHub reports sha256 {digest or 'none'} for {name}, "
                f"expected {expected[name]}"
            )
    return now


def ensure_release(
    github: GitHub,
    tag: str,
    title: str,
    notes: Path,
    files: dict[str, Path],
    prerelease: bool = False,
) -> list[str]:
    """Bring the release to exactly `files` (name -> local path), download
    every asset and compare it with the local file, then make sure that very
    release (by id) is published with the expected prerelease state and
    still holds exactly those assets. Returns the steps taken."""
    steps = []
    expected = {name: sha256_file(path) for name, path in files.items()}
    release = github.release(tag)
    if release is not None and release.draft:
        # Left by an interrupted `gh release create`. Publishing it here would
        # leave a gap between checking its assets and publishing them.
        raise ReleaseError(
            f"release {tag} is a draft (an interrupted publish?); check it, delete the "
            "draft by hand (the tag stays) and run publish again"
        )
    if release is None:
        github.create(
            tag, title, notes, [files[name] for name in sorted(files)], prerelease
        )
        steps.append(f"created release {tag} with {len(files)} assets")
    else:
        check_assets(tag, release, expected)
        if release.prerelease != prerelease:
            # Checked before any upload: an archive never lands in a release
            # with the wrong prerelease flag.
            raise ReleaseError(
                f"release {tag} is {'a' if release.prerelease else 'not a'} prerelease, "
                f"expected {'a' if prerelease else 'no'} prerelease; settle it by hand"
            )
        missing = sorted(set(files) - set(release.assets))
        if missing:
            github.upload(tag, [files[name] for name in missing])
            steps.append(f"uploaded {', '.join(missing)}")
        else:
            steps.append(f"release {tag} already holds every asset")
    before = github.release(tag)
    if before is None:
        raise ReleaseError(f"release {tag} is gone")
    check_complete(tag, before, before, expected)
    with tempfile.TemporaryDirectory(prefix="release-verify-") as directory:
        github.download(tag, Path(directory))
        found = {path.name: path for path in Path(directory).iterdir()}
        if sorted(found) != sorted(files):
            raise ReleaseError(
                f"release {tag} holds {sorted(found)}, expected {sorted(files)}"
            )
        for name, path in sorted(found.items()):
            if not same_file_bytes(path, files[name]):
                raise ReleaseError(
                    f"release {tag}: downloaded {name} differs from the local file"
                )
    steps.append(f"verified {len(files)} assets of {tag} byte for byte")
    # The downloaded assets must still be the ones on the release: same
    # release and asset ids, GitHub's sha256 of each the expected one.
    after = check_complete(tag, before, github.release(tag), expected)
    if after.draft:
        raise ReleaseError(f"release {tag} is not published")
    if after.prerelease != prerelease:
        raise ReleaseError(
            f"release {tag} is {'a' if after.prerelease else 'not a'} prerelease, "
            f"expected {'a' if prerelease else 'no'} prerelease"
        )
    return steps


def ensure_tag(github: GitHub, tag: str, commit: str, message: str) -> str:
    """Afterwards the annotated tag on GitHub points at the commit, which is
    on GitHub's main; never moves a tag."""
    if not github.on_main(commit):
        raise ReleaseError(f"{commit} is not on {MAIN} on GitHub")
    existing = github.tag(tag)
    if existing is not None:
        if existing != commit:
            raise ReleaseError(
                f"tag {tag} on GitHub points at {existing}, not {commit}"
            )
        return f"tag {tag} already on GitHub"
    github.create_tag(tag, commit, message)
    if github.tag(tag) != commit:
        raise ReleaseError(
            f"tag {tag} on GitHub does not point at {commit} after creating it"
        )
    return f"created tag {tag} -> {commit} on GitHub"


# --- build scripts -----------------------------------------------------------------


def versions_script(packages: Sequence[str]) -> str:
    return (
        "mkdir -p /out\n"
        f"{{ dpkg-query -W -f '${{Package}}=${{Version}}\\n' {' '.join(packages)}; "
        "rustc --version; } > /out/versions.txt\n"
    )


def check_script(config: Config) -> str:
    """The old workflow's fmt, clippy and test steps (`checks`), offline."""
    return "\n".join(config.checks) + "\n"


def release_script(config: Config, python: str = "python3") -> str:
    """Version and tag check, release build, binary checks, smoke test and the
    archive with its .sha256 file, as the old workflow's build job. Expects
    VERSION, TAG, COMMIT, MTIME and OUT in the environment; run in the crate dir."""
    binary = f'"target/{config.target}/release/{config.binary}"'
    checks = f"{shlex.quote(python)} -I scripts/release_checks.py"
    if config.mode == "linux-container":
        binary_check = f"{checks} glibc-floor --max {config.glibc_floor} {binary}"
        verify = 'sha256sum --check "${archive}.sha256"'
    else:
        binary_check = f"{checks} macho --max-minos {config.max_minos} --arch {config.macho_arch} {binary}"
        verify = 'shasum -a 256 --check "${archive}.sha256"'
    return f"""mkdir -p "$OUT/dist"
cargo metadata --format-version 1 --no-deps --locked > "$OUT/metadata.json"
version=$({checks} version --metadata "$OUT/metadata.json" --tag "$TAG")
if [ "$version" != "$VERSION" ]; then
    echo "crate version $version is not $VERSION" >&2
    exit 1
fi
cargo build --release --locked --target {config.target}
{binary_check} | tee -a "$OUT/checks.log"
{checks} smoke --binary {binary} --version "$VERSION" --commit "$COMMIT" | tee -a "$OUT/checks.log"
archive=$({checks} pack --binary {binary} --license ../LICENSE --version "$VERSION" \\
    --target {config.target} --mtime "$MTIME" --out-dir "$OUT/dist")
if [ "$archive" != "{config.binary}-${{VERSION}}-{config.target}.tar.gz" ]; then
    echo "pack wrote $archive" >&2
    exit 1
fi
(cd "$OUT/dist" && {verify})
"""


def dockerfile(
    base: str, packages: Sequence[str], platform: str, crate_dir: str
) -> str:
    """The builder image: base pinned by digest, apt packages, pinned and
    sha256-checked rustup, the source (`git archive` of the build commit), the
    pinned toolchain and the locked dependencies. The only phase with network;
    it runs no code of the crates or their dependencies."""
    for package in packages:
        if not APT_PACKAGE.fullmatch(package):
            raise ReleaseError(f"{package!r} is not an apt package name")
    if platform not in RUSTUP:
        raise ReleaseError(f"no rustup-init for platform {platform}")
    triple, rustup_sha256 = RUSTUP[platform]
    url = f"https://static.rust-lang.org/rustup/archive/{RUSTUP_VERSION}/{triple}/rustup-init"
    return f"""FROM --platform={platform} {base}
ENV DEBIAN_FRONTEND=noninteractive \\
    CARGO_HOME=/opt/cargo \\
    RUSTUP_HOME=/opt/rustup \\
    CARGO_TERM_COLOR=never \\
    PATH=/opt/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
RUN apt-get update \\
    && apt-get install -y --no-install-recommends {" ".join(packages)} \\
    && rm -rf /var/lib/apt/lists/* \\
    && dpkg --compare-versions "$(dpkg-query -W -f '${{Version}}' ca-certificates)" \\
        ge {CA_CERTIFICATES_MINIMUM} \\
    && python3 -I -c {shlex.quote(TRUST_CHECK)} {" ".join(BLOCKED_ROOTS)}
RUN curl --proto '=https' --tlsv1.2 -sSfL -o /tmp/rustup-init {url} \\
    && echo "{rustup_sha256}  /tmp/rustup-init" | sha256sum --check \\
    && chmod 0755 /tmp/rustup-init \\
    && /tmp/rustup-init -y --no-modify-path --profile minimal --default-toolchain none \\
    && rm /tmp/rustup-init
COPY source.tar /tmp/source.tar
RUN mkdir /work && tar -C /work -xf /tmp/source.tar && rm /tmp/source.tar
WORKDIR /work/{crate_dir}
RUN rustup toolchain install \\
    && rustup default "$(sed -n 's/^channel = "\\(.*\\)"$/\\1/p' rust-toolchain.toml)" \\
    && rustc --version && cargo fetch --locked
"""


# --- engines -------------------------------------------------------------------------


class Docker:
    """The docker calls of a linux-container build; tests replace it with a fake."""

    def preflight(self) -> str:
        """Require working linux/amd64 containers; return the engine's own platform."""
        argv = [
            "docker",
            "run",
            "--rm",
            "--platform",
            AMD64,
            RUNNER_IMAGE,
            "uname",
            "-m",
        ]
        arch = run(argv, timeout=600).strip()
        if arch != "x86_64":
            raise ReleaseError(
                f"a {AMD64} container reports {arch!r}, not x86_64 (README)"
            )
        native = run(
            ["docker", "version", "-f", "{{.Server.Os}}/{{.Server.Arch}}"]
        ).strip()
        if native not in RUSTUP:
            raise ReleaseError(f"unsupported Docker engine platform {native!r}")
        return native

    def pinned(self, image: str) -> str:
        """Pull the image and return it as <name>@<digest>."""
        run(
            ["docker", "pull", "--quiet", "--platform", AMD64, image],
            timeout=IMAGE_TIMEOUT,
        )
        inspect = [
            "docker",
            "image",
            "inspect",
            "--format",
            "{{json .RepoDigests}}",
            image,
        ]
        digests = json.loads(run(inspect))
        name = image.split(":", 1)[0]
        for digest in digests or []:
            if digest.split("@", 1)[0] in (
                name,
                f"docker.io/library/{name}",
                f"library/{name}",
            ):
                return f"{name}@{digest.split('@', 1)[1]}"
        raise ReleaseError(f"no repository digest for {image}")

    def build_image(self, tag: str, context: Path, platform: str) -> None:
        argv = [
            "docker",
            "build",
            "--platform",
            platform,
            "--network",
            "default",
            "-t",
            tag,
        ]
        run([*argv, str(context)], timeout=IMAGE_TIMEOUT, capture=False)

    def find_owned_container(self, name: str, ownership: str) -> str | None:
        argv = [
            "docker",
            "container",
            "ls",
            "--all",
            "--no-trunc",
            "--filter",
            f"label={ownership}",
            "--format",
            "{{.ID}} {{.Names}}",
        ]
        try:
            result = run(argv, timeout=CONTAINER_CREATE_TIMEOUT).strip()
        except ReleaseError as error:
            print(f"release cleanup failed: {error}", file=sys.stderr)
            return None
        container, _, found_name = result.partition(" ")
        if found_name == name and CONTAINER_ID.fullmatch(container):
            return container
        return None

    def job(
        self,
        image: str,
        name: str,
        script: str,
        env: Mapping[str, str],
        out_dir: Path,
        platform: str,
    ) -> None:
        """One build-phase job: no network, no mounts, memory, process and time
        limits; /out comes back with `docker cp`."""
        ownership = f"com.graphyard.local-release.job={secrets.token_hex(16)}"
        argv = [
            "docker",
            "create",
            "--name",
            name,
            "--label",
            ownership,
            "--platform",
            platform,
            "--network",
            "none",
        ]
        argv += ["--memory", MEMORY, "--memory-swap", MEMORY, "--pids-limit", PIDS]
        for key, value in sorted(env.items()):
            argv += ["-e", f"{key}={value}"]
        argv += [image, "bash", "-euo", "pipefail", "-c", script]
        container = None
        try:
            result = run(argv, timeout=CONTAINER_CREATE_TIMEOUT).strip()
            if not CONTAINER_ID.fullmatch(result):
                raise ReleaseError(f"docker create returned no container ID for {name}")
            container = result
            try:
                run(
                    ["docker", "start", "-a", container],
                    timeout=JOB_TIMEOUT,
                    capture=False,
                )
            except ReleaseError:
                quietly(["docker", "kill", container])
                raise
            run(["docker", "cp", f"{container}:/out/.", str(out_dir)])
        finally:
            if container is None:
                container = self.find_owned_container(name, ownership)
            if container is not None:
                quietly(["docker", "rm", "-f", container])

    def remove_image(self, tag: str) -> None:
        quietly(["docker", "image", "rm", tag])


class Host:
    """The host calls of a macos-native build; tests replace it with a fake."""

    def shell(self, script: str, cwd: Path, env: Mapping[str, str]) -> None:
        argv = ["bash", "-euo", "pipefail", "-c", script]
        run(argv, cwd=cwd, env=env, timeout=JOB_TIMEOUT, capture=False)

    def output(self, argv: Sequence[str], cwd: Path, env: Mapping[str, str]) -> str:
        return run(argv, cwd=cwd, env=env, timeout=IMAGE_TIMEOUT)


def host_checks(config: Config, repo: Path, log: Log) -> None:
    """The repository's own checks that the old workflow ran besides the crate
    (graphyard: fixture drift); on the host, in the clean checkout."""
    for argv in config.host_checks:
        log(f"== host check: {shlex.join(argv)}")
        run(argv, cwd=repo, timeout=JOB_TIMEOUT, capture=False)


def pinned_toolchain(crate: Path) -> tuple[str, list[str]]:
    """The channel and the components rust-toolchain.toml pins."""
    try:
        data = tomllib.loads(
            (crate / "rust-toolchain.toml").read_text(encoding="utf-8")
        )
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ReleaseError(
            f"cannot read {crate}/rust-toolchain.toml: {error}"
        ) from None
    toolchain = data.get("toolchain")
    channel = toolchain.get("channel") if isinstance(toolchain, dict) else None
    components = toolchain.get("components", []) if isinstance(toolchain, dict) else []
    if not isinstance(channel, str) or not re.fullmatch(
        r"[0-9A-Za-z][0-9A-Za-z._-]*", channel
    ):
        raise ReleaseError(f"{crate}/rust-toolchain.toml names no valid channel")
    if not isinstance(components, list) or not all(
        isinstance(name, str) and re.fullmatch(r"[a-z0-9][a-z0-9-]*", name)
        for name in components
    ):
        raise ReleaseError(f"{crate}/rust-toolchain.toml lists invalid components")
    return channel, list(components)


def pinned_channel(crate: Path) -> str:
    return pinned_toolchain(crate)[0]


def pinned_cargo_environment(crate: Path) -> dict[str, str]:
    """The environment for the pinned toolchain: `RUSTUP_TOOLCHAIN` set to
    the pin (an ambient override cannot win) and the toolchain's own bin
    directory first on PATH, so `cargo` is that toolchain whatever else PATH
    holds. The toolchain is installed only when it is missing. Without rustup
    the environment is unchanged."""
    env = dict(os.environ)
    rustup = shutil.which("rustup")
    if rustup is None:
        return env
    channel = pinned_channel(crate)
    env["RUSTUP_TOOLCHAIN"] = channel
    which = [rustup, "which", "--toolchain", channel, "cargo"]
    try:
        cargo = run(which, cwd=crate, env=env).strip()
    except ReleaseError:
        run(
            [rustup, "toolchain", "install", channel],
            cwd=crate,
            env=env,
            timeout=IMAGE_TIMEOUT,
        )
        cargo = run(which, cwd=crate, env=env).strip()
    env["PATH"] = f"{Path(cargo).parent}{os.pathsep}{env.get('PATH', '')}"
    return env


def crate_version(crate: Path) -> str:
    argv = ["cargo", "metadata", "--format-version", "1", "--no-deps", "--locked"]
    metadata = run(argv, cwd=crate, env=pinned_cargo_environment(crate))
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
        handle.write(metadata)
    try:
        script = str(crate / "scripts" / "release_checks.py")
        return run(
            [sys.executable, "-I", script, "version", "--metadata", handle.name]
        ).strip()
    finally:
        os.unlink(handle.name)


def build_linux(
    config: Config,
    git: Git,
    docker: Docker,
    commit: str,
    version: str,
    jobs: Path,
    log: Log,
) -> dict[str, Any]:
    native = docker.preflight()
    context = jobs / "context"
    context.mkdir()
    git.archive(commit, config.source_paths, context / "source.tar")
    packages = [
        *BASE_PACKAGES,
        *(p for p in config.apt_packages if p not in BASE_PACKAGES),
    ]
    base = docker.pinned(RUNNER_IMAGE)
    run_id = f"{os.getpid()}-{secrets.token_hex(4)}"
    # The version only as a short digest: tags are limited to 128 characters.
    version_key = hashlib.sha256(version.encode()).hexdigest()[:12]
    built: dict[str, str] = {}
    env = {
        "CARGO_NET_OFFLINE": "true",
        "COMMIT": commit,
        config.commit_env: commit,
        "MTIME": str(git.commit_time(commit)),
        "OUT": "/out",
        "TAG": config.tag(version),
        "VERSION": version,
    }
    try:
        for platform in dict.fromkeys([native, AMD64]):
            (context / "Dockerfile").write_text(
                dockerfile(base, packages, platform, CRATE.name), encoding="utf-8"
            )
            slug = platform.replace("/", "-")
            # One tag per invocation: a concurrent build of the same commit never
            # removes this build's images.
            built[platform] = (
                f"{config.binary}-local-release:{version_key}-{slug}-{commit[:12]}-{run_id}"
            )
            log(f"== image {built[platform]} from {base} ({platform})")
            docker.build_image(built[platform], context, platform)
        log(f"== checks on {native}: {'; '.join(config.checks)}")
        check_out = jobs / "checks"
        check_out.mkdir()
        docker.job(
            built[native],
            f"{config.binary}-release-checks-{run_id}",
            versions_script(packages) + check_script(config),
            {**env, **config.check_env},
            check_out,
            native,
        )
        log(f"== release build on {AMD64}")
        build_out = jobs / "build"
        build_out.mkdir()
        docker.job(
            built[AMD64],
            f"{config.binary}-release-build-{run_id}",
            versions_script(packages) + release_script(config),
            env,
            build_out,
            AMD64,
        )
    finally:
        for tag in built.values():
            docker.remove_image(tag)
    return {
        "dist": build_out / "dist",
        "checks_log": read_text(build_out / "checks.log"),
        "environment": {
            "image": base,
            "check_platform": native,
            "build_platform": AMD64,
            f"packages and toolchain ({native})": read_text(check_out / "versions.txt"),
            f"packages and toolchain ({AMD64})": read_text(build_out / "versions.txt"),
        },
    }


def build_macos(
    config: Config,
    git: Git,
    host: Host,
    commit: str,
    version: str,
    jobs: Path,
    log: Log,
) -> dict[str, Any]:
    source = jobs / "source"
    source.mkdir()
    archive = jobs / "source.tar"
    git.archive(commit, config.source_paths, archive)
    with tarfile.open(archive) as tar:
        tar.extractall(source, filter="data")
    crate = source / CRATE.name
    rustup = shutil.which("rustup")
    if rustup is None:
        raise ReleaseError("rustup is not on PATH (README)")
    out = jobs / "out"
    out.mkdir()
    env = {
        **os.environ,
        "PATH": f"{Path(rustup).parent}{os.pathsep}{os.environ.get('PATH', '')}",
        "CARGO_TARGET_DIR": str(crate / "target"),
        "CARGO_TERM_COLOR": "never",
        "MACOSX_DEPLOYMENT_TARGET": config.max_minos or "",
        "COMMIT": commit,
        config.commit_env: commit,
        "MTIME": str(git.commit_time(commit)),
        "OUT": str(out),
        "TAG": config.tag(version),
        "VERSION": version,
    }
    log("== pinned toolchain and locked dependencies (network)")
    channel = pinned_channel(crate)
    env["RUSTUP_TOOLCHAIN"] = channel  # no ambient toolchain override wins
    components = "".join(f" --component {name}" for name in pinned_toolchain(crate)[1])
    host.shell(
        f"rustup toolchain install {channel} --profile minimal{components}"
        f" && rustup target add --toolchain {channel} {config.target}",
        crate,
        env,
    )
    # The toolchain's own bin directory first: its cargo, rustc, rustfmt and
    # clippy whatever else PATH holds.
    cargo = host.output(
        ["rustup", "which", "--toolchain", channel, "cargo"], crate, env
    )
    env["PATH"] = f"{Path(cargo.strip()).parent}{os.pathsep}{env['PATH']}"
    toolchain = host.output(["rustc", "--version"], crate, env).strip()
    if not toolchain.startswith(f"rustc {channel} "):
        raise ReleaseError(f"{toolchain!r} is not the pinned toolchain {channel}")
    host.shell("cargo fetch --locked", crate, env)
    offline = {**env, "CARGO_NET_OFFLINE": "true"}
    log(f"== checks: {'; '.join(config.checks)}")
    host.shell(check_script(config), crate, {**offline, **config.check_env})
    log(f"== release build for {config.target}")
    host.shell(release_script(config, python=sys.executable), crate, offline)
    macos = host.output(["sw_vers", "-productVersion"], crate, env).strip()
    return {
        "dist": out / "dist",
        "checks_log": (out / "checks.log").read_text(encoding="utf-8"),
        "environment": {"host": f"macOS {macos}", "toolchain": toolchain},
    }


def default_out_dir(config: Config, version: str) -> Path:
    return CRATE / "target" / DIST / f"{config.binary}-{version}"


def build(
    compare_released: bool,
    out_dir: Path | None = None,
    *,
    config: Config | None = None,
    git: Git | None = None,
    github: GitHub | None = None,
    docker: Docker | None = None,
    host: Host | None = None,
    version: str | None = None,
    run_host_checks: Callable[[Config, Path, Log], None] = host_checks,
    log: Log = print,
) -> Path:
    config = config or load_config()
    git = git or Git(REPO)
    github = github or GitHub(config.github_repository)
    if not git.is_clean():
        raise ReleaseError(
            "the worktree has changes or untracked files; build from a clean checkout"
        )
    github.check_cli()
    commit = git.head()
    if not github.on_main(commit):
        raise ReleaseError(f"HEAD {commit} is not on {MAIN} on GitHub; push it first")
    version = version or crate_version(CRATE)
    if not VERSION.fullmatch(version):
        raise ReleaseError(f"crate version {version!r} is not semver")
    tag = config.tag(version)
    taken = github.tag(tag) is not None
    if taken and not compare_released:
        raise ReleaseError(
            f"{config.binary} {version} is already released (tag {tag}); "
            "bump the version, or pass --compare-released for a dry run"
        )
    if compare_released and not taken:
        raise ReleaseError(
            f"{config.binary} {version} is not released; drop --compare-released"
        )
    out = out_dir or default_out_dir(config, version)
    if out.exists() and any(out.iterdir()):
        raise ReleaseError(f"{out} is not empty; remove it first")
    run_host_checks(config, git.repo, log)
    jobs = Path(tempfile.mkdtemp(prefix=f"release-{config.binary}-"))
    try:
        if config.mode == "linux-container":
            result = build_linux(
                config, git, docker or Docker(), commit, version, jobs, log
            )
        else:
            result = build_macos(
                config, git, host or Host(), commit, version, jobs, log
            )
        archive = config.archive(version)
        dist: Path = result["dist"]
        found = sorted(path.name for path in dist.iterdir())
        if found != [archive, f"{archive}.sha256"]:
            raise ReleaseError(
                f"the build wrote {found}, expected {archive} and its .sha256"
            )
        # Everything goes into a private staging directory first, which then
        # replaces the (missing or empty) output directory in one rename: a
        # concurrent build cannot mix its files into this one.
        out.parent.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=f".{out.name}-", dir=out.parent))
        try:
            for name in found:
                shutil.copyfile(dist / name, staging / name)
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise
    finally:
        shutil.rmtree(jobs, ignore_errors=True)
    try:
        digest = check_sha256_file(staging, archive)
        record = {
            "binary": config.binary,
            "version": version,
            "tag": tag,
            "commit": commit,
            "target": config.target,
            "mode": config.mode,
            "compare_released": compare_released,
            "archive": archive,
            "sha256": digest,
            "checks": list(config.checks),
            "environment": result["environment"],
        }
        (staging / BUILD_JSON).write_text(
            json.dumps(record, indent=2) + "\n", encoding="utf-8"
        )
        notes = release_notes(config, record, result["checks_log"])
        (staging / NOTES).write_text(notes, encoding="utf-8")
        install_dir(staging, out)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    log(f"{digest}  {archive}")
    with tarfile.open(out / archive, "r:gz") as tar:
        for member in tar.getmembers():
            log(f"    {member.name} ({oct(member.mode)}, {member.size} bytes)")
    if compare_released:
        log(
            f"dry run of the released {config.binary} {version}: {out} cannot be published"
        )
    return out


def install_dir(staging: Path, out: Path) -> None:
    """Move the staged build to `out` in one rename; `rename` replaces only a
    missing or empty directory, so a build that got there first is kept."""
    try:
        os.rename(staging, out)
    except OSError as error:
        raise ReleaseError(
            f"{out} was filled by another build meanwhile: {error}"
        ) from None


def check_sha256_file(directory: Path, archive: str) -> str:
    """The archive's .sha256 file must be the sha256sum line of the archive."""
    digest = sha256_file(directory / archive)
    text = (directory / f"{archive}.sha256").read_text(encoding="utf-8")
    if text != f"{digest}  {archive}\n":
        raise ReleaseError(f"{archive}.sha256 does not name sha256 {digest}")
    return digest


def release_notes(config: Config, record: Mapping[str, Any], checks_log: str) -> str:
    if config.mode == "linux-container":
        floor = f"glibc >= {config.glibc_floor}"
        where = (
            "local linux/amd64 containers without network during the build; workspace checks "
            f"in a {record['environment']['check_platform']} container"
        )
    else:
        floor = f"macOS >= {config.max_minos}"
        where = f"natively on {record['environment']['host']}"
    lines = [
        f"{config.binary} {record['version']}, built from {record['commit']} for "
        f"{config.target} ({floor}) by {CRATE.name}/scripts/local_release.py ({where}).",
        "",
        f"sha256 of {record['archive']}: {record['sha256']}",
        "",
        "Checks: " + "; ".join(f"`{check}`" for check in config.checks) + ".",
        "",
        "```",
        checks_log.rstrip("\n"),
        "```",
        "",
        "Build environment:",
    ]
    for key, value in sorted(record["environment"].items()):
        if "\n" in value.strip():
            lines.append(f"- {key}:")
            lines.extend(f"  - {line}" for line in value.splitlines() if line)
        else:
            lines.append(f"- {key}: {value.strip()}")
    return "\n".join(lines) + "\n"


# --- publish -------------------------------------------------------------------------


def stage_notes(source: Path, destination: Path) -> None:
    try:
        # Nonblocking open lets us reject a FIFO even if the path changes.
        fd = os.open(source, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as notes:
            if not stat.S_ISREG(os.fstat(notes.fileno()).st_mode):
                raise OSError("not a regular file")
            with destination.open("wb") as staged:
                shutil.copyfileobj(notes, staged)
    except OSError as error:
        raise ReleaseError(
            f"cannot stage release notes {source}: {error}; "
            "publish requires a readable regular notes file"
        ) from None


def publish(
    out_dir: Path | None = None,
    *,
    config: Config | None = None,
    github: GitHub | None = None,
    version: str | None = None,
    log: Log = print,
) -> None:
    config = config or load_config()
    github = github or GitHub(config.github_repository)
    github.check_cli()
    out = out_dir or default_out_dir(config, version or crate_version(CRATE))
    try:
        record = json.loads((out / BUILD_JSON).read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise ReleaseError(f"no build in {out}: {error}") from None
    if record.get("binary") != config.binary or record.get("target") != config.target:
        raise ReleaseError(f"{out} holds a build of something else")
    if record.get("compare_released"):
        raise ReleaseError(
            f"{out} is a --compare-released dry run; it is never published"
        )
    version, tag, commit, archive = (
        record["version"],
        record["tag"],
        record["commit"],
        record["archive"],
    )
    consistent = (
        VERSION.fullmatch(version)
        and tag == config.tag(version)
        and archive == config.archive(version)
        and COMMIT.fullmatch(commit)
    )
    if not consistent:
        raise ReleaseError(f"{out}/{BUILD_JSON} is inconsistent")
    if check_sha256_file(out, archive) != record["sha256"]:
        raise ReleaseError(f"{archive} changed since the build")
    files = {archive: out / archive, f"{archive}.sha256": out / f"{archive}.sha256"}
    extra = sorted(
        {path.name for path in out.iterdir()} - set(files) - {BUILD_JSON, NOTES}
    )
    if extra:
        raise ReleaseError(
            f"{out} holds files that are not part of the release: {', '.join(extra)}"
        )
    with tempfile.TemporaryDirectory(prefix="release-notes-") as directory:
        notes = Path(directory) / NOTES
        stage_notes(out / NOTES, notes)
        log(ensure_tag(github, tag, commit, f"{config.binary} {version}"))
        title = f"{config.binary} {version}"
        for step in ensure_release(
            github, tag, title, notes, files, prerelease="-" in version
        ):
            log(step)


# --- command line --------------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    build_parser = commands.add_parser(
        "build", help="build a release (publishes nothing)"
    )
    build_parser.add_argument(
        "--compare-released", action="store_true", help="dry run of a released version"
    )
    build_parser.add_argument("--out-dir", type=Path)
    publish_parser = commands.add_parser(
        "publish", help="tag and publish what build produced"
    )
    publish_parser.add_argument("--out-dir", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "build":
            build(args.compare_released, args.out_dir)
        else:
            publish(args.out_dir)
    except ReleaseError as error:
        print(f"release failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
