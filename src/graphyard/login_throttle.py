"""Reserve owner/admin password attempts in fixed, generation-scoped cache windows.

Failures and pending verifications share pair/address limits. Atomic increments
reserve before hashing. Failure settlement increments before releasing pending,
so races can refuse extra work but cannot admit extra password checks. Success
clears pair failures while releasing only its own reservations. Generation keys
make late completions harmless after expiry. Local-memory and Memcached atomic
add/increment/decrement are the supported cache boundary, with per-process limits
by default. Signals cover unwrapped callers without double-counting this backend.
"""

from __future__ import annotations

import hashlib
import ipaddress
import uuid

from asgiref.sync import sync_to_async
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.backends import ModelBackend
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


def _raw_count(key: str) -> int:
    value = caches[CACHE_ALIAS].get(key, 0)
    return value if isinstance(value, int) else 0


def _window(key: str, *, create: bool = False) -> str | None:
    cache = caches[CACHE_ALIAS]
    pointer = f"window:{key}"
    generation = cache.get(pointer)
    if generation is None and create:
        candidate = uuid.uuid4().hex
        if cache.add(pointer, candidate, timeout=settings.LOGIN_FAILURE_WINDOW_S):
            generation = candidate
        else:
            generation = cache.get(pointer)
    return generation if isinstance(generation, str) else None


def _count(key: str) -> int:
    generation = _window(key)
    if generation is None:
        return 0
    prefix = f"{key}:{generation}"
    return _raw_count(f"{prefix}:pending") + _raw_count(f"{prefix}:failed")


def is_locked(request, username: str) -> bool:
    if request is None or settings.LOGIN_FAILURE_LIMIT <= 0:
        return False
    pair, address = _keys(request, username)
    return (
        _count(pair) >= settings.LOGIN_FAILURE_LIMIT
        or _count(address) >= settings.LOGIN_ADDRESS_FAILURE_LIMIT
    )


def _increment(key: str) -> int:
    cache = caches[CACHE_ALIAS]
    # add starts this generation's counter; incr never refreshes its expiry.
    for _ in range(3):
        if cache.add(key, 1, timeout=settings.LOGIN_FAILURE_WINDOW_S):
            return 1
        try:
            return cache.incr(key)
        except ValueError:
            continue  # expired/evicted between add and incr
    raise PermissionDenied("Login throttle unavailable.")


def _release(key: str) -> None:
    try:
        caches[CACHE_ALIAS].decr(key)
    except ValueError:
        pass  # expired generation; never touch a replacement window


def _reserve(request, username: str) -> list[tuple[str, str]]:
    if is_locked(request, username):
        raise PermissionDenied("Too many failed login attempts.")
    pair, address = _keys(request, username)
    reserved: list[tuple[str, str]] = []
    try:
        # Address first: a locked address cannot churn arbitrary username keys.
        for key, limit in [
            (address, settings.LOGIN_ADDRESS_FAILURE_LIMIT),
            (pair, settings.LOGIN_FAILURE_LIMIT),
        ]:
            generation = _window(key, create=True)
            if generation is None:
                raise PermissionDenied("Login throttle unavailable.")
            prefix = f"{key}:{generation}"
            pending, failed = f"{prefix}:pending", f"{prefix}:failed"
            count = _increment(pending)
            reserved.append((pending, failed))
            # Reserve first, then observe failures. Failure settlement adds its
            # failure before releasing pending, so races may overcount, never
            # admit an extra password verification.
            if count + _raw_count(failed) > limit:
                raise PermissionDenied("Too many failed login attempts.")
        return reserved
    except BaseException:
        for pending, _ in reserved:
            _release(pending)
        raise


def _settle(reserved: list[tuple[str, str]], *, success: bool) -> None:
    try:
        if success:
            # Only clear pair failures; other pending attempts keep their slots.
            caches[CACHE_ALIAS].delete(reserved[-1][1])
        else:
            for _, failed in reserved:
                _increment(failed)
    finally:
        for pending, _ in reserved:
            _release(pending)


def record_failure(request, username: str) -> None:
    if request is None or settings.LOGIN_FAILURE_LIMIT <= 0:
        return
    try:
        reserved = _reserve(request, username)
    except PermissionDenied:
        return
    _settle(reserved, success=False)


def reset(request, username: str) -> None:
    if request is None:
        return
    pair = _keys(request, username)[0]
    generation = _window(pair)
    if generation is not None:
        caches[CACHE_ALIAS].delete(f"{pair}:{generation}:failed")


class LoginThrottleBackend(ModelBackend):
    """Own password admission and settlement for owner/admin authentication.

    ModelBackend remains configured for existing sessions; a failed password
    raises PermissionDenied here so it is never checked by that fallback.
    """

    def authenticate(self, request, username=None, password=None, **kwargs):
        if username is None:
            username = kwargs.get(get_user_model().USERNAME_FIELD)
        reserved = []
        if request is not None and settings.LOGIN_FAILURE_LIMIT > 0:
            request._graphyard_throttle_handled = True
            reserved = _reserve(request, username if isinstance(username, str) else "")
        try:
            user = super().authenticate(
                request, username=username, password=password, **kwargs
            )
        except BaseException:
            for pending, _ in reserved:
                _release(pending)
            raise
        if reserved:
            _settle(reserved, success=user is not None)
        if user is None:
            raise PermissionDenied("Invalid login.")
        return user

    async def aauthenticate(self, request, username=None, password=None, **kwargs):
        # ModelBackend has its own async password path; route it through the
        # same admission/settlement boundary rather than inheriting a bypass.
        return await sync_to_async(self.authenticate, thread_sensitive=True)(
            request, username=username, password=password, **kwargs
        )


@receiver(user_login_failed, dispatch_uid="graphyard.login_throttle.failed")
def _on_login_failed(sender, credentials, request=None, **kwargs):
    if request is not None and getattr(request, "_graphyard_throttle_handled", False):
        return
    username = (
        credentials.get("username")
        or credentials.get(get_user_model().USERNAME_FIELD)
        or ""
    )
    record_failure(request, username if isinstance(username, str) else "")


@receiver(user_logged_in, dispatch_uid="graphyard.login_throttle.logged_in")
def _on_logged_in(sender, request, user, **kwargs):
    if request is None or not getattr(request, "_graphyard_throttle_handled", False):
        reset(request, user.get_username())
