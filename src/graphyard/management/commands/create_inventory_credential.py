"""Explicitly enroll one host and issue a host-bound, write-only credential."""

import hashlib
import secrets

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from graphyard.models import HostRegistry, InventoryCredential, InventoryHost


class Command(BaseCommand):
    help = "Enroll a registered host for inventory and issue a write-only credential"

    def add_arguments(self, parser):
        parser.add_argument("--host", required=True)
        parser.add_argument("--rotate", action="store_true")
        parser.add_argument(
            "--intermittent",
            action="store_true",
            default=None,
            help="Show stale data without freshness alerts",
        )
        parser.add_argument("--warning-after-seconds", type=int, default=None)

    def handle(self, *args, **options):
        if (
            options["warning_after_seconds"] is not None
            and options["warning_after_seconds"] < 60
        ):
            raise CommandError("warning-after-seconds must be at least 60")
        with transaction.atomic():
            try:
                registered = HostRegistry.objects.get(
                    host_id=options["host"], enabled=True
                )
            except HostRegistry.DoesNotExist as exc:
                raise CommandError("Register and enable this host first") from exc
            host, _ = InventoryHost.objects.get_or_create(host=registered)
            existing = InventoryCredential.objects.filter(host=host, enabled=True)
            if existing.exists() and not options["rotate"]:
                raise CommandError("Host already has a credential; use --rotate")
            existing.update(enabled=False)
            if options["warning_after_seconds"] is not None:
                host.warning_after_seconds = options["warning_after_seconds"]
            if options["intermittent"] is not None:
                host.alert_when_stale = not options["intermittent"]
            host.save()
            plaintext = secrets.token_urlsafe(32)
            credential = InventoryCredential.objects.create(
                host=host, digest=hashlib.sha256(plaintext.encode()).hexdigest()
            )
        # Print only after commit; operator captures stdout securely.
        self.stdout.write(f"{credential.pk}.{plaintext}")
