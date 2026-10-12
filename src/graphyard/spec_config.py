"""Typed parsing and validation of metric collection spec ``config`` values.

The collectors and ``MetricCollectionSpec.clean()`` share these helpers, so a
config that the admin or ``apply_metric_collection_specs`` accepts is one the
agent can run, and a malformed value fails the same way in both places.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping

MAX_TIMEOUT_SECONDS = 86400.0
DEFAULT_REQUEST_TIMEOUT_SECONDS = 10.0
DEFAULT_PAGE_PROBE_MAX_BODY_BYTES = 10 * 1024 * 1024
DEFAULT_PAGE_PROBE_TOTAL_TIMEOUT_FACTOR = 3


class SpecConfigError(ValueError):
    """A spec config value has the wrong type or range."""


def _describe(raw: object) -> str:
    try:
        text = repr(raw)
    except ValueError:  # e.g. an int too long to convert to text
        return f"<{type(raw).__name__}>"
    return text if len(text) <= 64 else f"{text[:61]}..."


def positive_float_config(
    config: Mapping[str, object], key: str, default: float
) -> float:
    """Return a finite number > 0. Numbers and numeric strings are accepted."""
    raw = config.get(key, default)
    value: float | None = None
    if isinstance(raw, int | float | str) and not isinstance(raw, bool):
        try:
            value = float(raw.strip() if isinstance(raw, str) else raw)
        except (OverflowError, ValueError):
            value = None
    if value is None or not math.isfinite(value) or value <= 0:
        raise SpecConfigError(
            f"config.{key} must be a number greater than 0, got {_describe(raw)}"
        )
    return value


def positive_int_config(config: Mapping[str, object], key: str, default: int) -> int:
    """Return an integer >= 1. Integers and ASCII digit strings are accepted."""
    if key not in config:
        return default
    raw = config[key]
    value: int | None = None
    if isinstance(raw, int) and not isinstance(raw, bool):
        value = raw
    elif isinstance(raw, str) and re.fullmatch(r"[0-9]{1,18}", raw.strip()):
        value = int(raw.strip())
    if value is None or value < 1:
        raise SpecConfigError(
            f"config.{key} must be a positive integer, got {_describe(raw)}"
        )
    return value


def bool_config(config: Mapping[str, object], key: str, default: bool) -> bool:
    """Return a real JSON boolean. Strings such as ``"false"`` are rejected."""
    if key not in config:
        return default
    raw = config[key]
    if not isinstance(raw, bool):
        raise SpecConfigError(
            f"config.{key} must be true or false, got {_describe(raw)}"
        )
    return raw


def request_timeout_config(config: Mapping[str, object]) -> float:
    value = positive_float_config(
        config, "request_timeout_seconds", DEFAULT_REQUEST_TIMEOUT_SECONDS
    )
    return _supported_timeout("request_timeout_seconds", value)


def page_probe_total_timeout_config(
    config: Mapping[str, object], request_timeout_seconds: float
) -> float:
    """Whole-probe deadline; defaults to three request timeouts."""
    value = positive_float_config(
        config,
        "total_timeout_seconds",
        request_timeout_seconds * DEFAULT_PAGE_PROBE_TOTAL_TIMEOUT_FACTOR,
    )
    return _supported_timeout("total_timeout_seconds", value)


def _supported_timeout(key: str, value: float) -> float:
    """Reject timeouts that Python transport/wait primitives cannot represent."""
    if value > MAX_TIMEOUT_SECONDS:
        raise SpecConfigError(f"config.{key} must be <= {MAX_TIMEOUT_SECONDS} seconds")
    return value


def validate_spec_config(spec_type: str, config: object) -> list[str]:
    """Return human-readable problems with a spec config (empty when valid).

    Only checks the typed knobs the collectors parse; required string fields
    such as ``url`` keep being reported by the collectors at run time.
    """
    if not isinstance(config, dict):
        return ["config must be an object"]

    errors: list[str] = []

    def total_timeout_check() -> float:
        if spec_type not in {"http_page_probe", "http_json_metric"}:
            return positive_float_config(config, "total_timeout_seconds", 1.0)
        try:
            request_timeout = request_timeout_config(config)
        except SpecConfigError:
            # Report the request error once, while still checking an explicit total.
            request_timeout = DEFAULT_REQUEST_TIMEOUT_SECONDS
        return page_probe_total_timeout_config(config, request_timeout)

    checks = (
        lambda: request_timeout_config(config),
        total_timeout_check,
        lambda: bool_config(config, "verify_tls", True),
        lambda: bool_config(config, "follow_redirects", True),
        lambda: positive_int_config(
            config, "max_body_bytes", DEFAULT_PAGE_PROBE_MAX_BODY_BYTES
        ),
    )
    for check in checks:
        try:
            check()
        except SpecConfigError as err:
            errors.append(str(err))
    return errors
