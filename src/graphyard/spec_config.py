"""Typed parsing and validation of metric collection spec ``config`` values.

The collectors and ``MetricCollectionSpec.clean()`` share these helpers, so a
config that the admin or ``apply_metric_collection_specs`` accepts is one the
agent can run, and a malformed value fails the same way in both places.
"""

from __future__ import annotations

import math
from collections.abc import Mapping

DEFAULT_REQUEST_TIMEOUT_SECONDS = 10.0
DEFAULT_PAGE_PROBE_TOTAL_TIMEOUT_SECONDS = 30.0
DEFAULT_PAGE_PROBE_MAX_BODY_BYTES = 10 * 1024 * 1024


class SpecConfigError(ValueError):
    """A spec config value has the wrong type or range."""


def positive_float_config(
    config: Mapping[str, object], key: str, default: float
) -> float:
    """Return a finite number > 0. Numbers and numeric strings are accepted."""
    if key not in config:
        return default
    raw = config[key]
    if isinstance(raw, bool) or raw is None:
        raise SpecConfigError(
            f"config.{key} must be a number greater than 0, got {raw!r}"
        )
    if isinstance(raw, int | float):
        value = float(raw)
    elif isinstance(raw, str):
        try:
            value = float(raw.strip())
        except ValueError:
            raise SpecConfigError(
                f"config.{key} must be a number greater than 0, got {raw!r}"
            ) from None
    else:
        raise SpecConfigError(
            f"config.{key} must be a number greater than 0, got {raw!r}"
        )
    if not math.isfinite(value) or value <= 0:
        raise SpecConfigError(
            f"config.{key} must be a number greater than 0, got {raw!r}"
        )
    return value


def positive_int_config(config: Mapping[str, object], key: str, default: int) -> int:
    """Return an integer >= 1. Integers and digit strings are accepted."""
    if key not in config:
        return default
    raw = config[key]
    value: int | None = None
    if isinstance(raw, bool) or raw is None:
        value = None
    elif isinstance(raw, int):
        value = raw
    elif isinstance(raw, str) and raw.strip().isdigit():
        value = int(raw.strip())
    if value is None or value < 1:
        raise SpecConfigError(f"config.{key} must be a positive integer, got {raw!r}")
    return value


def bool_config(config: Mapping[str, object], key: str, default: bool) -> bool:
    """Return a real JSON boolean. Strings such as ``"false"`` are rejected."""
    if key not in config:
        return default
    raw = config[key]
    if not isinstance(raw, bool):
        raise SpecConfigError(f"config.{key} must be true or false, got {raw!r}")
    return raw


def request_timeout_config(config: Mapping[str, object]) -> float:
    return positive_float_config(
        config, "request_timeout_seconds", DEFAULT_REQUEST_TIMEOUT_SECONDS
    )


def validate_spec_config(spec_type: str, config: object) -> list[str]:
    """Return human-readable problems with a spec config (empty when valid).

    Only checks the typed knobs the collectors parse; required string fields
    such as ``url`` keep being reported by the collectors at run time.
    """
    del spec_type  # The typed keys mean the same thing for every collector.
    if not isinstance(config, dict):
        return ["config must be an object"]

    errors: list[str] = []
    checks = (
        lambda: request_timeout_config(config),
        lambda: bool_config(config, "verify_tls", True),
        lambda: bool_config(config, "follow_redirects", True),
        lambda: positive_float_config(
            config,
            "total_timeout_seconds",
            DEFAULT_PAGE_PROBE_TOTAL_TIMEOUT_SECONDS,
        ),
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
