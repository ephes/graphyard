"""Operator warnings for ingest credentials still using password hashing."""

from django.core.checks import Warning, Tags, register
from django.db import OperationalError, ProgrammingError
from django.db.models import Q

from .models import FAST_TOKEN_HASH_PREFIXES, IngestToken


@register(Tags.database)
def legacy_ingest_tokens(app_configs, **kwargs):
    # Do not open unrequested databases or fail initial setup before migrations.
    if "default" not in (kwargs.get("databases") or []):
        return []
    prefixes = Q()
    for prefix in FAST_TOKEN_HASH_PREFIXES:
        prefixes |= Q(token_hash__startswith=f"{prefix}$")
    try:
        present = IngestToken.objects.filter(enabled=True).exclude(prefixes).exists()
    except (OperationalError, ProgrammingError):
        return []
    if not present:
        return []
    return [
        Warning(
            "Enabled ingest tokens still use legacy password hashes.",
            hint="Rotate unused tokens; successful authentication upgrades active tokens automatically.",
            id="graphyard.W001",
        )
    ]
