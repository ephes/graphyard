"""Fixture ``pycompat.json``: the CPython built-ins the agent mirrors (slice S2).

Pins ``graphyard-core::pycompat`` (agent-rs/PLAN.md 4.4) on a value corpus:
``repr(float)``/``format(x, "g")`` (floats given as IEEE bit patterns, so the
input is exact), ``float()`` and ``int()`` of strings, ``str()``/``repr()``/
``type().__name__``/``float()`` of JSON values decoded by ``json.loads``,
``spec_config._describe``, ``str.strip()``/``str.split()``, ``==`` between
JSON values and the integer digit limit of ``json.loads`` (D-11).

Strings stay ASCII apart from whitespace and printable letters: non-ASCII
digits are an intentional divergence (D-6) and the ``repr`` of other
non-printable code points is P2 (PLAN.md 2.3).
"""

from __future__ import annotations

import json
import struct
import sys
from typing import Any

from graphyard.spec_config import _describe

FLOATS = [
    0.0,
    -0.0,
    1.0,
    -1.0,
    0.1,
    0.2,
    0.1 + 0.2,
    1.5,
    2.5,
    10.0,
    21.0,
    30.0,
    86400.0,
    43200.0,
    129600.0,
    259200.0,
    0.5,
    1e-4,
    1e-5,
    0.00012345678,
    123456.0,
    1234567.0,
    999999.5,
    9999995.0,
    0.000099999995,
    1e15,
    1e16,
    1e17,
    123456789012345678.0,
    9007199254740993.0,
    1.7976931348623157e308,
    5e-324,
    2.2250738585072014e-308,
    1 / 3,
    2 / 3,
    100.0 / 3,
    1e22,
    1e23,
    -123.456,
    3.0000000000000004,
    float("inf"),
    float("-inf"),
    float("nan"),
]

FLOAT_STRINGS = [
    "1",
    "1.5",
    " 15 ",
    "\t2.5\n",
    "+3",
    "-3",
    "1_000.5",
    "1__0",
    "_1",
    "1_",
    "1e5",
    "1E-5",
    "1e",
    ".5",
    "5.",
    ".",
    "",
    " ",
    "inf",
    "-Infinity",
    "+INF",
    "nan",
    "-NaN",
    "infinit",
    "1e400",
    "-1e400",
    "1e-400",
    "5s",
    "abc",
    "0x10",
    "1,5",
    "\x1c1",
    "1\x1f",
    "\u00a015\u2003",
    "1 2",
    "0.1",
    "00012",
    "9007199254740993",
    "'quoted'",
    'say "hi"',
]

INT_STRINGS = [
    "0",
    "-0",
    "60",
    " 60 ",
    "\n30\t",
    "+5",
    "-5",
    "1_000",
    "1__000",
    "_1",
    "1_",
    "+_1",
    "00010",
    "",
    " ",
    "1.0",
    "1e3",
    "abc",
    "60s",
    "--1",
    "0x10",
    "\x1c1",
    "\u00a07\u3000",
    "99999999999999999999999999999",
    "-99999999999999999999999999999",
    "9" * 4300,
    "9" * 4301,
    "1_" * 2150 + "1",
    "x" * 300,
]

VALUES = [
    "null",
    "true",
    "false",
    "0",
    "-0",
    "7",
    "-7",
    "12345678901234567890123",
    "0.0",
    "-0.0",
    "1.0",
    "1.5",
    "1e16",
    "1E2",
    "1e400",
    "-1e400",
    "1e-400",
    "123456789012345678",
    "0.30000000000000004",
    '""',
    '"text"',
    '"it\'s"',
    '"say \\"hi\\""',
    '"both \' and \\""',
    '"back\\\\slash"',
    '"tab\\tnew\\nline\\rcr"',
    '"\\u0000\\u0007\\u001b\\u007f"',
    '"\\u001c\\u001f"',
    '"caf\\u00e9 \\u20ac \\ud83d\\ude00"',
    '"nbsp\\u00a0ls\\u2028"',
    '"\\u0085"',
    "[]",
    "{}",
    '[1, 2.0, "x", null, true]',
    '{"a": 1, "b": [false, {"c": "d"}], "e": 1.5}',
    '{"z": 1, "a": 2}',
    '"' + "x" * 63 + '"',
    '"' + "x" * 62 + '"',
    '"' + "x" * 100 + '"',
    "[" + ", ".join(["1"] * 40) + "]",
    '{"url": "https://example.test/a/very/long/path/that/goes/on/and/on"}',
    "1" + "0" * 400,
    # Objects that serde_json's arbitrary_precision would read as numbers.
    '{"$serde_json::private::Number": "5"}',
    '{"\\u0024serde_json::private::Number": "5"}',
    '[{"$serde_json::private::Number": "1.5"}, 2]',
    '{"$serde_json::private::Number": "5", "b": 1}',
    '{"a": {"$serde_json::private::Number": "x"}}',
    '{"$serde_json::private::RawValue": "5"}',
    # A repeated key keeps its first position and its last value.
    '{"a": 1, "b": 2, "a": 3}',
]

STRIP_STRINGS = [
    "",
    "   ",
    " a ",
    "\t\n\x0b\x0c\r a b \r\n",
    "\x1c\x1d\x1e\x1fa\x1f",
    "\u00a0a\u2003b\u3000",
    "\u0085a\u2028",
    "a  b\tc\n\nd",
    "\u200ba\u200b",
    "abc",
]

EQ_PAIRS = [
    ("1", "1"),
    ("1", "1.0"),
    ("1", "true"),
    ("1.0", "true"),
    ("0", "false"),
    ("0", "-0.0"),
    ("0.0", "-0.0"),
    ("2", "2.5"),
    ("1", '"1"'),
    ("null", "null"),
    ("null", "false"),
    ('"a"', '"a"'),
    ('"a"', '"A"'),
    ("9007199254740993", "9007199254740992.0"),
    ("9007199254740992", "9007199254740992.0"),
    ("12345678901234567890123", "1.2345678901234568e22"),
    ("1e400", "1e400"),
    ("1" + "0" * 400, "1e400"),
    ("[1, 2]", "[1.0, true]"),
    ("[1, 2]", "[2, 1]"),
    ("[1]", "[1, 1]"),
    ('{"a": 1, "b": 2}', '{"b": 2.0, "a": true}'),
    ('{"a": 1}', '{"a": 1, "b": 2}'),
    ('{"a": 1}', '{"b": 1}'),
    ("[]", "{}"),
    ('""', "[]"),
]

INT_LIMIT_DOCUMENTS = [
    "1" * 4300,
    "1" * 4301,
    "-" + "1" * 4301,
    '{"a": "' + "1" * 5000 + '", "b": 1.' + "1" * 5000 + "}",
    '{"a": [1, ' + "2" * 4301 + ", " + "3" * 4302 + "]}",
    "1" + "0" * 4300 + "e1",
]


def _bits(value: float) -> str:
    return struct.pack(">d", value).hex()


def _error(exc: Exception) -> dict[str, str]:
    return {"class": type(exc).__name__, "message": str(exc)}


def _float_result(call: Any) -> dict[str, Any]:
    try:
        value = call()
    except (TypeError, ValueError, OverflowError) as exc:
        return {"error": _error(exc)}
    return {"bits": _bits(value), "repr": repr(value)}


def _floats() -> list[dict[str, str]]:
    return [
        {
            "bits": _bits(value),
            "repr": repr(value),
            "str": str(value),
            "format_g": format(value, "g"),
        }
        for value in FLOATS
    ]


def _int_parse(text: str) -> dict[str, Any]:
    try:
        value = int(text)
    except ValueError as exc:
        return {"input": text, "error": _error(exc)}
    return {"input": text, "value": str(value)}


def _value(text: str) -> dict[str, Any]:
    value = json.loads(text)
    return {
        "json": text,
        "type": type(value).__name__,
        "str": str(value),
        "repr": repr(value),
        "describe": _describe(value),
        "float": _float_result(lambda: float(value)),
    }


def _describe_beyond_int_limit() -> list[dict[str, str]]:
    """``_describe`` where ``repr`` raises: integers beyond 4300 digits.

    ``json.loads`` itself rejects such documents (D-11), so the limit is
    lifted while decoding and restored before ``_describe`` runs.
    """
    texts = ["1" + "0" * 4300, '{"a": [1, ' + "2" * 4301 + "]}", "[" + "3" * 4400 + "]"]
    cases = []
    default_limit = sys.get_int_max_str_digits()
    for text in texts:
        sys.set_int_max_str_digits(0)
        try:
            value = json.loads(text)
        finally:
            sys.set_int_max_str_digits(default_limit)
        cases.append({"json": text, "describe": _describe(value)})
    return cases


def _int_limit(text: str) -> dict[str, Any]:
    try:
        json.loads(text)
    except ValueError as exc:
        return {"json": text, "error": _error(exc)}
    return {"json": text, "error": None}


def build_pycompat() -> dict[str, Any]:
    return {
        "max_int_str_digits": sys.get_int_max_str_digits(),
        "floats": _floats(),
        "float_parse": [
            {"input": text, **_float_result(lambda text=text: float(text))}
            for text in FLOAT_STRINGS
        ],
        "int_parse": [_int_parse(text) for text in INT_STRINGS],
        "values": [_value(text) for text in VALUES],
        "describe_beyond_int_limit": _describe_beyond_int_limit(),
        "strip_split": [
            {"input": text, "strip": text.strip(), "split": text.split()}
            for text in STRIP_STRINGS
        ],
        "eq": [
            {"left": left, "right": right, "eq": json.loads(left) == json.loads(right)}
            for left, right in EQ_PAIRS
        ],
        "int_limit": [_int_limit(text) for text in INT_LIMIT_DOCUMENTS],
    }


GENERATORS = {"pycompat.json": build_pycompat}
