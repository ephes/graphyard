"""Pin the production entrypoint that supplies login throttle client addresses."""

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("forwarded", ["198.51.100.42", "203.0.113.7, 198.51.100.42"])
@pytest.mark.parametrize(
    "peer,expected",
    [
        ("127.0.0.1", "198.51.100.42"),
        ("::1", "198.51.100.42"),
        ("203.0.113.9", "203.0.113.9"),
    ],
)
def test_production_wsgi_wrapper_client_address(peer, expected, forwarded, monkeypatch):
    seen = []

    def app(environ, start_response):
        seen.append(environ["REMOTE_ADDR"])
        return [b"ok"]

    monkeypatch.setattr("django.core.wsgi.get_wsgi_application", lambda: app)
    spec = importlib.util.spec_from_file_location(
        "graphyard_wsgi_proxy_test", ROOT / "src/django/config/wsgi_proxy.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    result = module.application(
        {"REMOTE_ADDR": peer, "HTTP_X_FORWARDED_FOR": forwarded},
        lambda *args: None,
    )
    assert result == [b"ok"]
    assert seen == [expected]


def test_production_service_and_docs_require_proxy_entrypoint():
    service = (ROOT / "deploy/systemd/graphyard-web.service").read_text()
    start = next(line for line in service.splitlines() if line.startswith("ExecStart="))
    assert "--interface wsgi" in start
    assert start.split()[-1] == "config.wsgi_proxy:application"
    for path in ["README.md", "docs/deployment.md"]:
        text = (ROOT / path).read_text()
        assert "Required production entrypoint: `config.wsgi_proxy:application`" in text
        assert "REMOTE_ADDR" in text
        assert "GRAPHYARD_LOGIN_THROTTLE_PROXY_HOPS" in text
    runtime = (
        (ROOT / "README.md")
        .read_text()
        .split("## Production Runtime Pattern (systemd)", 1)[1]
        .split("## Validation Commands", 1)[0]
    )
    assert "config.wsgi_proxy:application" in runtime
    assert "docs/deployment.md" in runtime
