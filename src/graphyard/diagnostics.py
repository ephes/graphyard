"""Bounded error metadata for persistence and operator logs."""

import httpx


# Never emit arbitrary exception names or messages from plugins/backends.
SAFE_ERROR_TYPES = frozenset(
    {
        "ConnectError",
        "ConnectTimeout",
        "ReadTimeout",
        "WriteTimeout",
        "PoolTimeout",
        "ReadError",
        "WriteError",
        "RemoteProtocolError",
        "LocalProtocolError",
        "UnsupportedProtocol",
        "DecodingError",
        "TooManyRedirects",
        "HTTPStatusError",
        "InvalidURL",
        "_PageProbeLimitExceeded",
        "_PageProbeDeadlineExceeded",
        "JSONDecodeError",
        "UnicodeDecodeError",
        "ValueError",
        "TypeError",
        "KeyError",
        "IndexError",
        "AttributeError",
        "OverflowError",
        "RuntimeError",
        "OSError",
        "DatabaseError",
        "OperationalError",
        "IntegrityError",
        "InfluxConfigurationError",
        "InfluxTransferError",
        "ApiException",
        "panic",
    }
)


def error_category(err: Exception) -> str:
    name = type(err).__name__
    return name if name in SAFE_ERROR_TYPES else "Exception"


SAFE_VALIDATION_MESSAGES = frozenset(
    {
        "UniFi redirect outside controller origin refused",
        "UniFi compressed response refused",
        "UniFi controller URL must be absolute HTTP or HTTPS",
    }
)


def collector_failure(err: Exception) -> str:
    if type(err) is ValueError and str(err) in SAFE_VALIDATION_MESSAGES:
        return str(err)
    if isinstance(err, httpx.HTTPStatusError):
        return f"collector HTTP error: status={int(err.response.status_code)}"
    return f"collector failure: {error_category(err)}"
