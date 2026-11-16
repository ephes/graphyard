"""Admission of controller evidence before collection or tag expansion."""

import io
import json

import ijson


MAX_NODES = 100_000
MAX_DEPTH = 32
MAX_ARRAY_ITEMS = 1024
MAX_TAG_CHARS = 256
STREAMING_JSON = ijson.get_backend("python")


class UnifiEvidenceError(ValueError):
    pass


def decode_unifi_json(body):
    # Containers and member names count as nodes. Keep only one bounded stack
    # of keys/counts; no device/port lists are constructed by this first pass.
    text = body.decode("utf-8-sig")
    stack = []
    nodes = 0
    try:
        for event, value in STREAMING_JSON.basic_parse(
            io.BytesIO(text.encode("utf-8"))
        ):
            if event in ("end_map", "end_array"):
                stack.pop()
                continue
            nodes += 1
            if nodes > MAX_NODES or len(stack) > MAX_DEPTH:
                raise UnifiEvidenceError("UniFi JSON work limit")
            if event == "map_key":
                keys, _, _ = stack[-1]
                if value in keys:
                    raise UnifiEvidenceError("UniFi duplicate JSON member")
                keys.add(value)
                stack[-1][2] = value
                continue
            if stack:
                keys, count, key = stack[-1]
                if keys is None:
                    stack[-1][1] = count + 1
                    if count >= MAX_ARRAY_ITEMS:
                        raise UnifiEvidenceError("UniFi array limit")
                elif key in {"name", "mac", "speed", "rc"}:
                    if (
                        event in ("start_map", "start_array")
                        or len(str(value)) > MAX_TAG_CHARS
                    ):
                        raise UnifiEvidenceError("UniFi scalar metadata required")
            if event in ("start_map", "start_array"):
                # Root container is level one in both implementations.
                if len(stack) >= MAX_DEPTH:
                    raise UnifiEvidenceError("UniFi JSON depth limit")
                stack.append([set() if event == "start_map" else None, 0, None])
    except (ijson.JSONError, ArithmeticError):
        raise ValueError("Invalid UniFi JSON") from None
    return json.loads(text)
