"""Fixture ``spec_config.json``: typed spec config knobs (slice S2).

Pins ``graphyard-core::spec_config`` (agent-rs/PLAN.md 6.3) against every
helper of ``spec_config.py`` on a config corpus. Configs are JSON texts
decoded with ``json.loads``, exactly as Django's ``JSONField`` hands them to
the collectors, so Rust parses the same text. Floats in arguments and
results are written as ``repr`` text (``inf``/``nan`` included); results are
``{"value": ...}`` or ``{"error": <SpecConfigError message>}``.

The corpus includes the configs of the S2 rows of TESTMAP.md
(``test_metric_collection_spec_isolation.py``: ``test_full_clean_*``,
``test_validate_spec_config_rejects_non_object``,
``test_model_validates_derived_wait_limit``,
``test_derived_timeout_defaults_are_validated``,
``test_invalid_timeouts_report_each_key_once``, the ``1e308`` timeouts of
``test_impractical_timeout_rejected_before_collection`` and
``test_explicit_oversized_total_rejected_before_transport``).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from functools import partial
from typing import Any

from graphyard import spec_config
from graphyard.models import MetricCollectionSpecType
from graphyard.spec_config import (
    SpecConfigError,
    bool_config,
    page_probe_total_timeout_config,
    positive_float_config,
    positive_int_config,
    request_timeout_config,
    validate_spec_config,
)

# Raw JSON values for one key, shared by the per-helper corpora.
RAW_VALUES = [
    "null",
    "true",
    "false",
    "0",
    "-0",
    "1",
    "-1",
    "7",
    "15",
    "1.5",
    "0.0",
    "-0.0",
    "-2.5",
    "0.001",
    "1e-300",
    "5e-324",
    "86400",
    "86400.0",
    "86400.00000000001",
    "43200.0",
    "86401",
    "1e308",
    "1e309",
    "-1e309",
    "1" + "0" * 400,
    "123456789012345678",
    "1234567890123456789",
    "99999999999999999999999",
    '""',
    '" "',
    '"15"',
    '" 15 "',
    '"\\t2.5\\n"',
    '"\\u00a015\\u2003"',
    '"\\u001c15\\u001f"',
    '"1_0"',
    '"1__0"',
    '"5s"',
    '"abc"',
    '"inf"',
    '"-inf"',
    '"nan"',
    '"Infinity"',
    '"1e3"',
    '"0"',
    '"000"',
    '"0010"',
    '"-5"',
    '"+5"',
    '"123456789012345678"',
    '"1234567890123456789"',
    '"\\u00b2"',
    '"true"',
    '"false"',
    '"it\'s"',
    '"' + "x" * 80 + '"',
    "[]",
    "[1]",
    "{}",
    '{"seconds": 5}',
    "[" + ", ".join(str(n) for n in range(30)) + "]",
]

FLOAT_DEFAULTS = [10.0, 1.0, 30.0, 0.5, 0.0, -1.0, float("inf"), float("nan")]

TOTAL_REQUEST_TIMEOUTS = [10.0, 0.5, 15.0, 28800.0, 28800.000000000004, 43200.0, 1e308]

TOTAL_CONFIGS = [
    "{}",
    '{"total_timeout_seconds": 45.0}',
    '{"total_timeout_seconds": 30}',
    '{"total_timeout_seconds": "60"}',
    '{"total_timeout_seconds": 86400}',
    '{"total_timeout_seconds": 86401}',
    '{"total_timeout_seconds": 1e308}',
    '{"total_timeout_seconds": 0}',
    '{"total_timeout_seconds": "abc"}',
    '{"total_timeout_seconds": null}',
    '{"total_timeout_seconds": true}',
]

SPEC_TYPES = [value for value, _label in MetricCollectionSpecType.CHOICES] + [
    "unknown_type"
]

VALIDATE_CONFIGS = [
    "{}",
    "[]",
    "null",
    '"config"',
    "42",
    '{"url": "https://example.test/", "subject_id": "example"}',
    '{"url": "https://example.test/", "subject_id": "example", '
    '"request_timeout_seconds": "15", "max_body_bytes": 1024, '
    '"verify_tls": false, "follow_redirects": true}',
    '{"request_timeout_seconds": "5s"}',
    '{"request_timeout_seconds": null}',
    '{"request_timeout_seconds": 0}',
    '{"request_timeout_seconds": true}',
    '{"verify_tls": "false"}',
    '{"follow_redirects": "true"}',
    '{"max_body_bytes": 1.5}',
    '{"request_timeout_seconds": 1' + "0" * 400 + "}",
    '{"max_body_bytes": "\\u00b2"}',
    '{"max_body_bytes": "\\u0661"}',
    '{"request_timeout_seconds": 43200.0}',
    '{"request_timeout_seconds": 43200.0, "total_timeout_seconds": 30.0}',
    '{"request_timeout_seconds": "5s", "total_timeout_seconds": "abc"}',
    '{"request_timeout_seconds": 1e308}',
    '{"request_timeout_seconds": 1e308, "total_timeout_seconds": 30.0}',
    '{"total_timeout_seconds": 1e308}',
    '{"request_timeout_seconds": 28800.000000000004}',
    '{"request_timeout_seconds": 100000, "total_timeout_seconds": 0, '
    '"verify_tls": 1, "follow_redirects": null, "max_body_bytes": 0}',
    '{"total_timeout_seconds": "nan", "max_body_bytes": "  42  "}',
]


def _float_text(value: float) -> str:
    return repr(value)


def _outcome(call: Callable[[], object]) -> dict[str, Any]:
    try:
        value = call()
    except SpecConfigError as exc:
        return {"error": str(exc)}
    if isinstance(value, float):
        return {"value": _float_text(value)}
    if isinstance(value, bool):
        return {"value": value}
    assert isinstance(value, int)
    return {"value": str(value)}


def _keyed_configs() -> list[str]:
    """``{}`` (key missing) plus one config per raw value under ``knob``."""
    return ["{}"] + ['{"other": 1, "knob": ' + raw + "}" for raw in RAW_VALUES]


def _positive_float() -> list[dict[str, Any]]:
    cases = []
    for text in _keyed_configs():
        for default in FLOAT_DEFAULTS if text == "{}" else [10.0]:
            config = json.loads(text)
            cases.append(
                {
                    "config": text,
                    "key": "knob",
                    "default": _float_text(default),
                    **_outcome(partial(positive_float_config, config, "knob", default)),
                }
            )
    return cases


def _positive_int() -> list[dict[str, Any]]:
    cases = []
    for text in _keyed_configs():
        config = json.loads(text)
        cases.append(
            {
                "config": text,
                "key": "knob",
                "default": "1024",
                **_outcome(partial(positive_int_config, config, "knob", 1024)),
            }
        )
    return cases


def _bool() -> list[dict[str, Any]]:
    cases = []
    for text in _keyed_configs():
        config = json.loads(text)
        for default in [True, False] if text == "{}" else [True]:
            cases.append(
                {
                    "config": text,
                    "key": "knob",
                    "default": default,
                    **_outcome(partial(bool_config, config, "knob", default)),
                }
            )
    return cases


def _request_timeout() -> list[dict[str, Any]]:
    cases = []
    for text in _keyed_configs():
        text = text.replace('"knob"', '"request_timeout_seconds"')
        config = json.loads(text)
        cases.append(
            {"config": text, **_outcome(partial(request_timeout_config, config))}
        )
    return cases


def _total_timeout() -> list[dict[str, Any]]:
    cases = []
    for text in TOTAL_CONFIGS:
        config = json.loads(text)
        for request_timeout in TOTAL_REQUEST_TIMEOUTS:
            cases.append(
                {
                    "config": text,
                    "request_timeout_seconds": _float_text(request_timeout),
                    **_outcome(
                        partial(
                            page_probe_total_timeout_config, config, request_timeout
                        )
                    ),
                }
            )
    return cases


def _validate() -> list[dict[str, Any]]:
    return [
        {
            "spec_type": spec_type,
            "config": text,
            "errors": validate_spec_config(spec_type, json.loads(text)),
        }
        for text in VALIDATE_CONFIGS
        for spec_type in SPEC_TYPES
    ]


def build_spec_config() -> dict[str, Any]:
    return {
        "constants": {
            "MAX_TIMEOUT_SECONDS": _float_text(spec_config.MAX_TIMEOUT_SECONDS),
            "DEFAULT_REQUEST_TIMEOUT_SECONDS": _float_text(
                spec_config.DEFAULT_REQUEST_TIMEOUT_SECONDS
            ),
            "DEFAULT_PAGE_PROBE_MAX_BODY_BYTES": spec_config.DEFAULT_PAGE_PROBE_MAX_BODY_BYTES,
            "DEFAULT_PAGE_PROBE_TOTAL_TIMEOUT_FACTOR": (
                spec_config.DEFAULT_PAGE_PROBE_TOTAL_TIMEOUT_FACTOR
            ),
        },
        "positive_float_config": _positive_float(),
        "positive_int_config": _positive_int(),
        "bool_config": _bool(),
        "request_timeout_config": _request_timeout(),
        "page_probe_total_timeout_config": _total_timeout(),
        "validate_spec_config": _validate(),
    }


GENERATORS = {"spec_config.json": build_spec_config}
