"""Audit the frozen runtime and development graph without installing it."""

from pathlib import Path
import subprocess
import tempfile


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(prefix="graphyard-audit-") as directory:
        requirements = str(Path(directory) / "requirements.txt")
        commands = [
            [
                "uv",
                "export",
                "--quiet",
                "--frozen",
                "--all-groups",
                "--no-hashes",
                "--no-emit-project",
                "--output-file",
                requirements,
            ],
            [
                "uvx",
                "--from",
                "pip-audit==2.10.1",
                "pip-audit",
                "--disable-pip",
                "--no-deps",
                "-r",
                requirements,
            ],
        ]
        for command in commands:
            try:
                subprocess.run(command, cwd=root, check=True)
            except subprocess.CalledProcessError as error:
                return error.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
