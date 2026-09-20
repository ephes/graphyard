import json
import os
import stat
import fcntl
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from graphyard.inventory_releases import definitions, refresh


class Command(BaseCommand):
    help = "Refresh explicitly registered public release metadata; never connect to inventory hosts."

    def add_arguments(self, parser):
        parser.add_argument("--file", required=True, type=Path)
        parser.add_argument("--check", action="store_true")
        parser.add_argument("--force", action="store_true")

    def handle(self, *args, **options):
        try:
            raw = options["file"].read_bytes()
            if len(raw) > 1024 * 1024:
                raise ValueError("Registry too large")
            data = json.loads(raw)
            definitions(data)
        except (OSError, ValueError, TypeError) as exc:
            raise CommandError("Invalid release registry") from exc
        if options["check"]:
            self.stdout.write("Release registry valid; no requests or database changes")
            return
        fd = os.open(
            settings.BASE_DIR / ".inventory-releases.lock",
            os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW,
            0o600,
        )
        with os.fdopen(fd, "r+") as lock:
            info = os.fstat(lock.fileno())
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid()
                or info.st_nlink != 1
                or info.st_mode & 0o077
            ):
                raise CommandError("Unsafe release refresh lock")
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise CommandError("Release refresh already running") from exc
            failures = refresh(data, force=options["force"])
        self.stdout.write(f"Release refresh completed; {failures} failed sources")
        if failures:
            raise CommandError("Some release sources could not be checked")
