"""Limit failed logins (owner login and admin login).

Failures are counted in the ``login_throttle`` cache per (username, client address) and per client address.
Once a counter reaches its limit, further attempts for that key are refused for the rest of the window
without checking the password. A successful login clears the (username, client address) counter.

It hooks into authentication itself (a backend that stops the login, plus the ``user_login_failed`` and
``user_logged_in`` signals), so every login form that passes the request is covered. Calls without a request
(management commands, ``Client.login`` in tests) are not limited. Bearer tokens are not affected.

The counters live in each server process (local-memory cache), so with several workers the effective limit
is the configured limit times the number of workers.
"""

from __future__ import annotations

import hashlib
import ipaddress

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.signals import user_logged_in, user_login_failed
from django.core.cache import caches
from django.core.exceptions import PermissionDenied
from django.dispatch import receiver

CACHE_ALIAS = "login_throttle"
MAX_USERNAME = 150  # longer names are cut for the key; they cannot belong to an account


def client_address(request) -> str:
    """The address failures are counted for.

    ``LOGIN_THROTTLE_PROXY_HOPS`` says how many reverse proxies sit in front of the app. With ``n`` hops the
    n-th entry from the right of ``X-Forwarded-For`` is the address the outermost trusted proxy saw; anything
    left of it was sent by the client and is ignored. With 0 hops, or when the header is missing, too short or
    not an address, the socket address is used."""
    remote = request.META.get("REMOTE_ADDR", "") or "unknown"
    hops = settings.LOGIN_THROTTLE_PROXY_HOPS
    if hops <= 0:
        return remote
    parts = [
        part.strip()
        for part in request.META.get("HTTP_X_FORWARDED_FOR", "").split(",")
        if part.strip()
    ]
    if len(parts) < hops:
        return remote
    candidate = parts[-hops]
    try:
        return str(ipaddress.ip_address(candidate))
    except ValueError:
        return remote


def _keys(request, username: str) -> tuple[str, str]:
    address = client_address(request)
    name = (username or "")[:MAX_USERNAME].casefold()
    pair = hashlib.sha256(f"{name}\x00{address}".encode()).hexdigest()
    return f"pair:{pair}", f"addr:{hashlib.sha256(address.encode()).hexdigest()}"


def _count(key: str) -> int:
    value = caches[CACHE_ALIAS].get(key, 0)
    return value if isinstance(value, int) else 0


def is_locked(request, username: str) -> bool:
    if request is None or settings.LOGIN_FAILURE_LIMIT <= 0:
        return False
    pair, address = _keys(request, username)
    return (
        _count(pair) >= settings.LOGIN_FAILURE_LIMIT
        or _count(address) >= settings.LOGIN_ADDRESS_FAILURE_LIMIT
    )


def _increment(key: str) -> None:
    cache = caches[CACHE_ALIAS]
    # add() starts the window; incr() keeps the expiry of the existing entry, so the window is fixed.
    if cache.add(key, 1, timeout=settings.LOGIN_FAILURE_WINDOW_S):
        return
    try:
        cache.incr(key)
    except ValueError:  # expired between add() and incr()
        cache.add(key, 1, timeout=settings.LOGIN_FAILURE_WINDOW_S)


def record_failure(request, username: str) -> None:
    if request is None or settings.LOGIN_FAILURE_LIMIT <= 0:
        return
    # Locked attempts must not create fresh keys and evict active counters.
    if is_locked(request, username):
        return
    for key in _keys(request, username):
        _increment(key)


def reset(request, username: str) -> None:
    if request is None:
        return
    caches[CACHE_ALIAS].delete(_keys(request, username)[0])


class LoginThrottleBackend:
    """First authentication backend: stops a login attempt while its counters are at the limit.

    ``PermissionDenied`` ends ``authenticate()`` before the password backends run, so a locked-out attempt
    costs no password hash and cannot succeed even with the right password.

    It only gates logins and never owns a session: it deliberately has no ``get_user`` (and no permission
    methods), so Django never records it as the backend a user logged in with."""

    def authenticate(self, request, username=None, password=None, **kwargs):
        if username is None:
            username = kwargs.get(get_user_model().USERNAME_FIELD)
        if (
            request is not None
            and isinstance(username, str)
            and is_locked(request, username)
        ):
            raise PermissionDenied("Too many failed login attempts.")
        return None


@receiver(user_login_failed, dispatch_uid="graphyard.login_throttle.failed")
def _on_login_failed(sender, credentials, request=None, **kwargs):
    username = (
        credentials.get("username")
        or credentials.get(get_user_model().USERNAME_FIELD)
        or ""
    )
    record_failure(request, username if isinstance(username, str) else "")


@receiver(user_logged_in, dispatch_uid="graphyard.login_throttle.logged_in")
def _on_logged_in(sender, request, user, **kwargs):
    reset(request, user.get_username())
