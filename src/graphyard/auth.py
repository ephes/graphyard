from __future__ import annotations

from datetime import timedelta
import logging
from typing import Optional

from django.db import OperationalError
from django.http import HttpRequest
from django.utils import timezone

from .models import IngestToken

logger = logging.getLogger(__name__)


BEARER_PREFIX = "Bearer "
LAST_USED_UPDATE_INTERVAL = timedelta(minutes=1)


def extract_bearer_token(request: HttpRequest) -> Optional[str]:
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith(BEARER_PREFIX):
        return None
    token = auth_header[len(BEARER_PREFIX) :].strip()
    if not token:
        return None
    return token


def authenticate_ingest_token(request: HttpRequest) -> Optional[IngestToken]:
    raw_token = extract_bearer_token(request)
    if raw_token is None:
        return None

    now = timezone.now()
    for ingest_token in IngestToken.objects.filter(enabled=True):
        if ingest_token.check_token(raw_token):
            update_fields: list[str] = []

            if (
                not ingest_token.uses_fast_token_hash()
                or ingest_token.needs_fast_hash_prefix_upgrade()
            ):
                ingest_token.set_token(raw_token)
                update_fields.append("token_hash")

            if (
                ingest_token.last_used_at is None
                or now - ingest_token.last_used_at >= LAST_USED_UPDATE_INTERVAL
            ):
                ingest_token.last_used_at = now
                update_fields.append("last_used_at")

            if update_fields:
                try:
                    ingest_token.save(update_fields=update_fields)
                except OperationalError as err:
                    if "database is locked" not in str(err).lower():
                        raise
                    logger.warning(
                        "Skipping ingest token metadata update due to SQLite lock: token=%s fields=%s",
                        ingest_token.name,
                        ",".join(update_fields),
                    )
            return ingest_token

    return None
