"""WSGI wrapper for Granian with trusted proxy headers."""

import os

from django.core.wsgi import get_wsgi_application
from granian.utils.proxies import wrap_wsgi_with_proxy_headers

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.production")

_app = get_wsgi_application()
application = wrap_wsgi_with_proxy_headers(_app, trusted_hosts=["127.0.0.1", "::1"])
