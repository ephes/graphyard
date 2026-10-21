"""Fixture ``json_path.json``: the metric path of ``http_json_metric`` (S7a).

Pins ``graphyard-core::json_path`` (agent-rs/PLAN.md 6.3) against
``services.py:_resolve_json_path``, including the paths of the 24
production ``http_json_metric`` specs on macmini.

Encoding:

* payloads are JSON texts, decoded with ``json.loads`` here and with
  ``pycompat::json_loads`` in Rust, as the executor receives them;
* a found value is its ``repr`` (``pycompat::py_repr`` in Rust); ``None``
  (not found, which includes a JSON ``null`` at the path) is ``null``;
* errors are ``{"class": <exception class>, "message": str(exc)}``;
* ``divergence`` names the PLAN.md section 2.4 entry of a case whose Rust
  result differs on purpose; ``rust`` is then the Rust result.
"""

from __future__ import annotations

import json
from typing import Any

from graphyard import services

PRODUCTION_PAYLOAD = json.dumps(
    {
        "filesystems_by_name": {"rootfs": {"used_ratio": 0.41}},
        "ipmi": {
            name: {"value": value}
            for name, value in [
                ("FAN1_1", 1120),
                ("FAN2_1", 980.0),
                ("FAN3_1", "1040"),
                ("FAN6_1", None),
                ("TEMP_CARD_SIDE", 41.5),
                ("TEMP_CPU", 55),
                ("TEMP_DDR5_A1", 38.25),
                ("TEMP_DDR5_B1", 37.75),
                ("TEMP_FCH", 47),
                ("TEMP_MB", 36),
            ]
        },
        "lm_sensors": {
            "cpu_tctl": {"value": 55.625},
            "gpu_edge": {"value": 44.0},
            "nvme_boot_composite": {"value": 39.85},
            "nvme_fast_composite": {"value": 41.85},
        },
        "pools": {"fast": {"cap_ratio": 0.27}, "tank": {"cap_ratio": 0.63}},
        "sysfs": {
            "cpu_package": {"value": 52.0},
            "fan1": {"value": 1200},
            "nvme_composite": {"value": 40.85},
            "pch_temperature": {"value": 48.0},
        },
        "usb": {
            "device_present_value": 1,
            "last_known_used_ratio": 0.5,
            "pool_imported_value": 0,
        },
    }
)

PRODUCTION_PATHS = [
    "$.filesystems_by_name.rootfs.used_ratio",
    "$.ipmi.FAN1_1.value",
    "$.ipmi.FAN2_1.value",
    "$.ipmi.FAN3_1.value",
    "$.ipmi.FAN6_1.value",
    "$.ipmi.TEMP_CARD_SIDE.value",
    "$.ipmi.TEMP_CPU.value",
    "$.ipmi.TEMP_DDR5_A1.value",
    "$.ipmi.TEMP_DDR5_B1.value",
    "$.ipmi.TEMP_FCH.value",
    "$.ipmi.TEMP_MB.value",
    "$.lm_sensors.cpu_tctl.value",
    "$.lm_sensors.gpu_edge.value",
    "$.lm_sensors.nvme_boot_composite.value",
    "$.lm_sensors.nvme_fast_composite.value",
    "$.pools.fast.cap_ratio",
    "$.pools.tank.cap_ratio",
    "$.sysfs.cpu_package.value",
    "$.sysfs.fan1.value",
    "$.sysfs.nvme_composite.value",
    "$.sysfs.pch_temperature.value",
    "$.usb.device_present_value",
    "$.usb.last_known_used_ratio",
    "$.usb.pool_imported_value",
]

NESTED = json.dumps(
    {
        "queue": {"depth": 12, "items": [{"id": "a", "n": 1.5}, {"id": "b", "n": -2}]},
        "list": [10, [20, 30], {"0": "key zero"}],
        "": {"": "empty keys"},
        "a.b": 1,
        "flag": True,
        "off": False,
        "nothing": None,
        "text": "42",
        "big": 10**30,
        "zero": 0,
        "unicode": {"ä": "umlaut"},
        "dup": 1,
    }
).replace('"dup": 1', '"dup": 1, "dup": 2')

CASES: list[tuple[str, str]] = [
    (PRODUCTION_PAYLOAD, path) for path in PRODUCTION_PATHS
] + [
    (NESTED, "$"),
    ("null", "$"),
    ("[1, 2]", "$"),
    ("7", "$"),
    (NESTED, "$."),
    (NESTED, "$.."),
    (NESTED, "$.queue"),
    (NESTED, "$.queue.depth"),
    (NESTED, "$..queue...depth.."),
    (NESTED, "$.queue.missing"),
    (NESTED, "$.queue.depth.deeper"),
    (NESTED, "$.queue.items.0.n"),
    (NESTED, "$.queue.items.1.id"),
    (NESTED, "$.queue.items.2.id"),
    (NESTED, "$.queue.items.00.n"),
    (NESTED, "$.queue.items.-1.n"),
    (NESTED, "$.queue.items.+1.n"),
    (NESTED, "$.queue.items. 1.n"),
    (NESTED, "$.queue.items.1_0.n"),
    (NESTED, "$.queue.items.first"),
    (NESTED, "$.queue.items.99999999999999999999999999"),
    (NESTED, "$.list.1.0"),
    (NESTED, "$.list.2.0"),
    (NESTED, "$.list.0.0"),
    (NESTED, "$.a.b"),
    (NESTED, "$.flag"),
    (NESTED, "$.off"),
    (NESTED, "$.nothing"),
    (NESTED, "$.text"),
    (NESTED, "$.big"),
    (NESTED, "$.zero"),
    (NESTED, "$.unicode.ä"),
    (NESTED, "$.dup"),
    (NESTED, "queue.depth"),
    (NESTED, "$queue"),
    (NESTED, " $.queue"),
    (NESTED, "$ .queue"),
    (NESTED, ""),
    (NESTED, "$.QUEUE.depth"),
    ("[[1, [2, 3]]]", "$.0.1.1"),
    ("[1, 2]", "$.queue"),
    ('"text"', "$.0"),
    ("[1, 2]", "$.1" + "0" * 4300),
    ("[1, 2]", "$.1" + "0" * 4299),
    ('{"k": [1]}', "$.k.0" + "0" * 4300),
    ('{"' + "1" * 4301 + '": 5}', "$." + "1" * 4301),
]

# Unicode digit segments: ``str.isdigit`` is true, Rust takes ASCII digits
# only (PLAN.md D-6) and finds nothing.
DIVERGENT: list[tuple[str, str, str]] = [
    ("[10, 20, 30, 40]", "$.٣", "D-6"),
    ("[10, 20, 30, 40]", "$.²", "D-6"),
]


def _resolve(payload_text: str, path: str) -> dict[str, Any]:
    case: dict[str, Any] = {"payload": payload_text, "path": path}
    try:
        found = services._resolve_json_path(json.loads(payload_text), path)
    except (TypeError, ValueError, OverflowError) as exc:
        case["error"] = {"class": type(exc).__name__, "message": str(exc)}
        return case
    case["found"] = None if found is None else repr(found)
    return case


def build_json_path() -> dict[str, Any]:
    cases = [_resolve(payload, path) for payload, path in CASES]
    for payload, path, divergence in DIVERGENT:
        case = _resolve(payload, path)
        case["divergence"] = divergence
        case["rust"] = {"found": None}
        cases.append(case)
    return {"cases": cases}


GENERATORS = {"json_path.json": build_json_path}
