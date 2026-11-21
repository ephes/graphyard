"""Admission of controller evidence before collection or tag expansion."""

from decimal import Decimal
import io
import json
import sys

import ijson


# Sized for real multi-device sites: a large switch record (48+ port_table
# rows with all counters) is roughly 6,000-7,000 nodes, so the node budget
# admits well over a hundred such devices while still bounding expansion.
MAX_NODES = 1_000_000
MAX_DEPTH = 32
MAX_ARRAY_ITEMS = 4096
MAX_TAG_CHARS = 256
STREAMING_JSON = ijson.get_backend("python")
REFUSED = "UniFi evidence refused"


class UnifiEvidenceError(ValueError):
    pass


def int_digit_limit_exceeded(text):
    """Whether ``json.loads`` would refuse an integer literal for its digit count.

    Mirrors the Rust ``int_digit_limit_error`` scan so the check is independent
    of where the literal appears relative to an admission refusal.
    """
    limit = sys.get_int_max_str_digits()
    if not limit:
        return False
    index = 0
    length = len(text)
    while index < length:
        char = text[index]
        if char == '"':
            index += 1
            while index < length and text[index] != '"':
                index += 2 if text[index] == "\\" else 1
            index += 1
        elif char == "-" or "0" <= char <= "9":
            digits = 0
            is_integer = True
            while index < length:
                char = text[index]
                if "0" <= char <= "9":
                    digits += 1
                elif char in ".eE":
                    is_integer = False
                elif char not in "-+":
                    break
                index += 1
            if is_integer and digits > limit:
                return True
        else:
            index += 1
    return False


def _decoder_error(text):
    """Raise the error json.loads reports for a body that was not admitted.

    Deep nesting can exhaust Python's recursion before the syntax error is
    found; it is unparseable like serde_json's own nesting limit in Rust.
    """
    try:
        json.loads(text)
    except RecursionError:
        raise json.JSONDecodeError("Nesting too deep", text, 0) from None


def _rendered(value):
    # json.loads renders non-integer numbers as floats, not exact decimals.
    return str(float(value)) if isinstance(value, Decimal) else str(value)


def _admit(events):
    # Containers and member names count as nodes. Keep only one bounded stack
    # of keys/counts; no device/port lists are constructed by this pass.
    stack = []
    nodes = 0
    for event, value in events:
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
                    or len(_rendered(value)) > MAX_TAG_CHARS
                ):
                    raise UnifiEvidenceError("UniFi scalar metadata required")
        if event in ("start_map", "start_array"):
            # Root container is level one in both implementations.
            if len(stack) >= MAX_DEPTH:
                raise UnifiEvidenceError("UniFi JSON depth limit")
            stack.append([set() if event == "start_map" else None, 0, None])


def decode_unifi_json(body):
    # Undecodable text, syntax errors and over-long integers are reported by
    # json.loads itself, exactly like the unbounded decoder used before, and
    # take precedence over admission refusals wherever they appear (the Rust
    # collector checks the whole document's syntax and digits first).
    # UTF-8 only, like the Rust collector: json.loads(bytes) would also accept
    # UTF-16/32, which is unparseable here (UnicodeDecodeError) instead.
    text = body.decode("utf-8-sig")
    if int_digit_limit_exceeded(text):
        _decoder_error(text)
        raise ValueError("Exceeds the limit for integer string conversion")
    events = STREAMING_JSON.basic_parse(io.BytesIO(text.encode("utf-8")))
    try:
        try:
            _admit(events)
        except UnifiEvidenceError:
            # Finish the syntax check without building anything, so a later
            # syntax error is reported as unparseable instead of refused.
            for _ in events:
                pass
            raise
    except (ijson.JSONError, ArithmeticError):
        # Report the decoder's own error. A document json.loads accepts but the
        # strict pass does not (NaN/Infinity) is never returned unadmitted; it is
        # unparseable, as the Rust decoder (RFC 8259 only) reports it.
        _decoder_error(text)
        raise json.JSONDecodeError("Non-standard JSON", text, 0) from None
    return json.loads(text)
