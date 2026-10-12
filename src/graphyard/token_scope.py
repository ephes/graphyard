"""Optional host scope for ingest tokens.

A scope is a list of entries. Each entry is an exact host identifier or a
prefix followed by one trailing ``*``. An empty scope means the token is not
restricted. Entries and candidates are compared in the canonical subject-id
form (lowercase, every run of other characters folded to ``_``), so
``mac-mini.lan`` and ``mac_mini_lan`` name the same host, as they already do
for ``subject_id``.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

from django.core.exceptions import ValidationError

MAX_HOST_SCOPE_ENTRIES = 64
MAX_HOST_SCOPE_ENTRY_LENGTH = 128
_HOST_SCOPE_ENTRY_PATTERN = re.compile(r"[a-z0-9][a-z0-9._-]*\*?")
_NON_IDENTIFIER_RUN = re.compile(r"[^a-z0-9]+")


def canonical_host_identifier(raw: str) -> str:
    return _NON_IDENTIFIER_RUN.sub("_", raw.strip().lower()).strip("_")


def _is_valid_entry(entry: object) -> bool:
    return (
        isinstance(entry, str)
        and len(entry) <= MAX_HOST_SCOPE_ENTRY_LENGTH
        and _HOST_SCOPE_ENTRY_PATTERN.fullmatch(entry) is not None
    )


def clean_host_scope(raw_entries: Iterable[object]) -> list[str]:
    """Return the validated, lowercased, de-duplicated scope or raise."""
    entries: list[str] = []
    for raw in raw_entries:
        entry = raw.strip().lower() if isinstance(raw, str) else raw
        if not _is_valid_entry(entry):
            raise ValidationError(
                "Invalid host scope entry %(entry)r: use a host identifier of "
                "letters, digits, '.', '_' and '-', optionally followed by one "
                "trailing '*'.",
                params={"entry": raw},
            )
        assert isinstance(entry, str)
        if entry not in entries:
            entries.append(entry)
    if len(entries) > MAX_HOST_SCOPE_ENTRIES:
        raise ValidationError(
            f"A host scope may hold at most {MAX_HOST_SCOPE_ENTRIES} entries."
        )
    return entries


def parse_host_scope_text(text: str) -> list[str]:
    """Parse entries separated by commas and/or whitespace."""
    return clean_host_scope(part for part in re.split(r"[\s,]+", text) if part)


def validate_host_scope(value: object) -> None:
    """Model field validator: the stored value must already be clean."""
    if not isinstance(value, list):
        raise ValidationError("Host scope must be a list of entries.")
    if clean_host_scope(value) != value:
        raise ValidationError(
            "Host scope entries must be lowercase, trimmed and unique."
        )


def is_scoped(scope: object) -> bool:
    """Only an empty list means unrestricted; anything else restricts.

    That includes ``None``: SQLite stores a JSON null in the column, and a
    damaged value must never widen a token.
    """
    return not (isinstance(scope, list) and not scope)


def is_valid_stored_scope(scope: object) -> bool:
    """Exactly what ``validate_host_scope`` accepts, as a boolean."""
    try:
        validate_host_scope(scope)
    except ValidationError:
        return False
    return True


def host_in_scope(scope: object, identifier: str) -> bool:
    """Whether ``identifier`` is allowed by ``scope``.

    Fails closed: a stored scope that is not a list of valid entries allows
    nothing, so a damaged row can never widen a token.
    """
    if not is_scoped(scope):
        return True
    if not is_valid_stored_scope(scope):
        return False
    assert isinstance(scope, list)
    candidate = canonical_host_identifier(identifier)
    if not candidate:
        return False
    for entry in scope:
        if entry.endswith("*"):
            # Keep a trailing separator of the prefix: "web-*" must not match "web".
            prefix = _NON_IDENTIFIER_RUN.sub("_", entry[:-1])
            if candidate.startswith(prefix):
                return True
        elif candidate == canonical_host_identifier(entry):
            return True
    return False
