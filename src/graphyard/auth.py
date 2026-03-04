from __future__ import annotations

from typing import Optional

from django.http import HttpRequest

from .models import IngestToken


BEARER_PREFIX = "Bearer "


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

    for ingest_token in IngestToken.objects.filter(enabled=True):
        if ingest_token.check_token(raw_token):
            ingest_token.mark_used()
            return ingest_token

    return None
