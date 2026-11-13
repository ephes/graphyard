"""Resolve one database for an operator credential transaction."""

from django.core.management.base import CommandError
from django.db import router


def credential_database(models, requested):
    if requested is not None:
        return requested
    aliases = {router.db_for_write(model) for model in models}
    if len(aliases) != 1:
        raise CommandError(
            "Credential models route to different databases; select --database explicitly"
        )
    return aliases.pop()
