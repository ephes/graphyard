#!/usr/bin/env python3
"""Exercise real Graphyard + Vector on loopback, using a new private SQLite DB."""

from __future__ import annotations

import argparse
import hashlib
import http.client
import io
import json
import os
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from uuid import uuid4
from wsgiref.simple_server import WSGIRequestHandler, make_server

import httpx

REPO = Path(__file__).resolve().parents[1]


class ProbeFailure(RuntimeError):
    """An assertion with a caller-authored message safe for diagnostic output."""


def require(condition, message):
    if not condition:
        raise ProbeFailure(message)


def configure(root, monitor_credential):
    """Never load project .env, existing settings or an existing database."""
    import django
    from django.conf import settings

    require(not settings.configured, "Run this probe in its own Python process")
    sys.path[:0] = [str(REPO / "src"), str(REPO / "src/django")]
    settings.configure(
        SECRET_KEY=secrets.token_urlsafe(48),
        DEBUG=False,
        ALLOWED_HOSTS=["127.0.0.1"],
        ROOT_URLCONF="config.urls",
        ADMIN_URL="admin/",
        LOGIN_URL="/login/",
        LOGIN_REDIRECT_URL="/inventory/",
        USE_TZ=True,
        INSTALLED_APPS=[
            "django.contrib.admin",
            "django.contrib.auth",
            "django.contrib.contenttypes",
            "django.contrib.sessions",
            "django.contrib.messages",
            "django.contrib.staticfiles",
            "graphyard.apps.GraphyardConfig",
        ],
        MIDDLEWARE=[
            "django.middleware.security.SecurityMiddleware",
            "django.contrib.sessions.middleware.SessionMiddleware",
            "django.middleware.common.CommonMiddleware",
            "django.middleware.csrf.CsrfViewMiddleware",
            "django.contrib.auth.middleware.AuthenticationMiddleware",
            "django.contrib.messages.middleware.MessageMiddleware",
        ],
        TEMPLATES=[
            {
                "BACKEND": "django.template.backends.django.DjangoTemplates",
                "APP_DIRS": True,
                "OPTIONS": {
                    "context_processors": [
                        "django.template.context_processors.request",
                        "django.contrib.auth.context_processors.auth",
                        "django.contrib.messages.context_processors.messages",
                    ]
                },
            }
        ],
        DATABASES={
            "default": {
                "ENGINE": "django.db.backends.sqlite3",
                "NAME": root / "probe.sqlite3",
                "OPTIONS": {"timeout": 10, "transaction_mode": "IMMEDIATE"},
            }
        },
        GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS=10000,
        GRAPHYARD_SQLITE_JOURNAL_MODE="WAL",
        GRAPHYARD_SQLITE_SYNCHRONOUS="FULL",
        GRAPHYARD_INVENTORY_MONITOR_TOKEN=monitor_credential,
        DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
        STATIC_URL="/static/",
    )
    django.setup()
    from django.core.management import call_command
    from django.db import connection

    call_command("migrate", verbosity=0, interactive=False, stdout=io.StringIO())
    with connection.cursor() as cursor:
        require(cursor.execute("PRAGMA synchronous").fetchone()[0] == 2, "SQLite FULL")
    require(
        Path(connection.settings_dict["NAME"]).resolve() == root / "probe.sqlite3",
        "Database isolation",
    )


class QuietWSGIHandler(WSGIRequestHandler):
    def setup(self):
        self.request.settimeout(10)
        super().setup()

    def log_message(self, fmt, *args):
        pass


class FaultProxy(ThreadingHTTPServer):
    """Inject transport faults; successful requests always reach real Graphyard."""

    daemon_threads = False

    def __init__(self, backend_port):
        super().__init__(("127.0.0.1", 0), ForwardRequest)
        self.backend_port = backend_port
        self.lock = threading.Lock()
        self.reject = False
        self.drop_reply = False
        self.attempts: Counter[str] = Counter()
        self.receipts: list[dict[str, Any]] = []
        self.content_types: set[str] = set()
        self.expected_auth = ""
        self.errors: list[dict[str, str]] = []
        self.http_statuses: list[int] = []


class ForwardRequest(BaseHTTPRequestHandler):
    def setup(self):
        self.request.settimeout(10)
        super().setup()

    def log_message(self, *args):
        pass

    def do_POST(self):
        proxy = self.server
        assert isinstance(proxy, FaultProxy)
        connection = None
        stage = "read request"
        try:
            require(self.path == "/v1/inventory", "Unexpected proxy path")
            length = int(self.headers.get("Content-Length", "0"))
            require(0 < length <= 8 * 1024 * 1024, "Invalid request size")
            body = self.rfile.read(length)
            value = json.loads(body)
            if isinstance(value, list):
                require(len(value) == 1, "Expected exactly one report per HTTP request")
                value = value[0]
            require(isinstance(value, dict), "Expected an inventory object")
            require(
                isinstance(value.get("snapshot_id"), str), "Missing snapshot identity"
            )
            report = value
            with proxy.lock:
                proxy.attempts[report["snapshot_id"]] += 1
                proxy.content_types.add(self.headers.get("Content-Type", ""))
                require(
                    self.headers.get("Authorization") == proxy.expected_auth,
                    "Vector did not resolve the configured writer secret",
                )
                reject = proxy.reject
            if reject:
                self.send_response(503)
                self.end_headers()
                return
            stage = "forward to Graphyard"
            connection = http.client.HTTPConnection(
                "127.0.0.1", proxy.backend_port, timeout=10
            )
            connection.request(
                "POST",
                "/v1/inventory",
                body,
                {
                    "Content-Type": self.headers.get("Content-Type", ""),
                    "Authorization": self.headers.get("Authorization", ""),
                },
            )
            response = connection.getresponse()
            stage = "read Graphyard response"
            with proxy.lock:
                proxy.http_statuses.append(response.status)
            payload = response.read()
            try:
                receipt_body = json.loads(payload)
            except (ValueError, UnicodeDecodeError):
                raise ProbeFailure(
                    f"Graphyard returned non-JSON HTTP {response.status}"
                ) from None
            require(isinstance(receipt_body, dict), "Expected a JSON response object")
            with proxy.lock:
                proxy.receipts.append(
                    {
                        "id": report["snapshot_id"],
                        "status": response.status,
                        "body": receipt_body,
                    }
                )
                drop = proxy.drop_reply and response.status == 200
                if drop:
                    proxy.drop_reply = False
            if drop:
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            self.send_response(response.status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except Exception as exc:
            with proxy.lock:
                proxy.errors.append(
                    {
                        "stage": stage,
                        "error": str(exc)
                        if isinstance(exc, ProbeFailure)
                        else type(exc).__name__,
                    }
                )
            self.close_connection = True
        finally:
            if connection:
                connection.close()


def fixture():
    return {
        "schema_version": 1,
        "host": "transport-probe",
        "snapshot_id": str(uuid4()),
        "observed_at": (datetime.now(UTC) - timedelta(minutes=5)).isoformat(),
        "collector": "synthetic-transport-test",
        "categories": {
            name: {"status": "ok", "items": []}
            for name in ("packages", "units", "containers", "applications")
        },
        "gaps": [],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", type=Path, default=REPO.parent / "ops-library")
    parser.add_argument(
        "--report", type=Path, help="Optional private local collector report"
    )
    parser.add_argument(
        "--vector-threads",
        type=int,
        help="Optional diagnostic worker count; default uses Vector's own default",
    )
    args = parser.parse_args()
    require(
        args.vector_threads is None or args.vector_threads > 0,
        "Vector worker count must be positive",
    )
    vector = shutil.which("vector")
    if vector is None:
        raise RuntimeError("Vector must already be installed")
    vector_command = [vector]
    if args.vector_threads is not None:
        vector_command += ["--threads", str(args.vector_threads)]
    library = args.library.resolve() / "roles/software_estate/files"
    require((library / "emit.py").is_file(), "Matching ops-library checkout required")
    # The only production imports supplying the spool writer/config generator.
    sys.path.insert(0, str(library))
    import emit
    from vector_inventory_config import configuration, secret_directory

    root = Path(tempfile.mkdtemp(prefix="graphyard-inventory-probe-")).resolve()
    old_umask = os.umask(0o077)
    result: dict[str, Any] = {"status": "running", "checks": [], "directory": str(root)}
    process = None
    servers = []
    workers = []
    secret_paths: list[Path] = []
    log = (root / "vector.log").open("wb")
    print(f"Private artifacts: {root}", flush=True)

    def stop(force=False):
        nonlocal process
        if process is not None:
            if process.poll() is None:
                process.kill() if force else process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
            process = None

    def check(name):
        result["checks"].append(name)
        print("PASS:", name, flush=True)

    def wait_for(predicate, label, timeout=60):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with proxy.lock:
                require(not proxy.errors, "Proxy failure: " + str(proxy.errors))
            if process is not None:
                require(process.poll() is None, "Vector exited unexpectedly")
            if predicate():
                return
            time.sleep(0.1)
        raise TimeoutError(label)

    try:
        monitor_credential = secrets.token_urlsafe(40)
        configure(root, monitor_credential)
        from django.contrib.auth import get_user_model
        from django.core.management import call_command
        from django.core.wsgi import get_wsgi_application
        from graphyard.inventory import canonical, validate
        from graphyard.models import (
            HostRegistry,
            InventoryCredential,
            InventorySnapshot,
        )
        from django.utils import timezone

        backend = make_server(
            "127.0.0.1", 0, get_wsgi_application(), handler_class=QuietWSGIHandler
        )
        proxy = FaultProxy(backend.server_port)
        for server in (backend, proxy):
            servers.append(server)
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            workers.append(worker)
        url = f"http://127.0.0.1:{backend.server_port}"
        endpoint = f"http://127.0.0.1:{proxy.server_port}/v1/inventory"
        result["vector"] = subprocess.check_output(
            [vector, "--version"], text=True
        ).strip()
        result["backend_url"] = url
        result["vector_threads"] = args.vector_threads or "default"
        check("isolated migrated SQLite FULL database and loopback receiver")

        def enroll(host):
            HostRegistry.objects.create(host_id=host)
            output = io.StringIO()
            call_command("create_inventory_credential", host=host, stdout=output)
            return output.getvalue().strip()

        def pipeline(name, credential):
            proxy.expected_auth = "Bearer " + credential
            folder = root / name
            folder.mkdir(mode=0o700)
            spool, data = folder / "outbox", folder / "vector-data"
            spool.mkdir(mode=0o700)
            data.mkdir(mode=0o700)
            secret_dir = secret_directory(data)
            secret_dir.mkdir(mode=0o700)
            writer_path = secret_dir / "writer"
            secret_paths.append(writer_path)
            config = configuration(spool, data, endpoint)
            target = folder / "vector.json"
            target.write_text(json.dumps(config))
            env = {
                k: v
                for k, v in os.environ.items()
                if not k.startswith("VECTOR_")
                and k
                not in (
                    "SOFTWARE_ESTATE_WRITE_CREDENTIAL",
                    "HTTP_PROXY",
                    "HTTPS_PROXY",
                    "ALL_PROXY",
                    "http_proxy",
                    "https_proxy",
                    "all_proxy",
                )
            }
            writer_path.write_text(credential + "\n")
            completed = subprocess.run(
                [vector, "validate", "--no-environment", str(target)],
                env=env,
                stdout=log,
                stderr=log,
                timeout=30,
            )
            require(
                completed.returncode == 0, "Generated Vector configuration validation"
            )
            return spool, target, env

        def start(target, env):
            return subprocess.Popen(
                vector_command + ["--config", str(target)],
                env=env,
                stdout=log,
                stderr=log,
            )

        def stored(report):
            return InventorySnapshot.objects.filter(
                snapshot_id=report["snapshot_id"]
            ).exists()

        def attempted(report, count):
            with proxy.lock:
                return proxy.attempts[report["snapshot_id"]] >= count

        def confirmed(report, duplicate=None):
            with proxy.lock:
                return any(
                    r["id"] == report["snapshot_id"]
                    and r["status"] == 200
                    and (duplicate is None or r["body"]["duplicate"] is duplicate)
                    for r in proxy.receipts
                )

        credential = enroll("transport-probe")
        with httpx.Client(base_url=url, trust_env=False, timeout=15) as client:
            require(
                client.get(
                    "/v1/inventory/status",
                    headers={"Authorization": "Bearer " + monitor_credential},
                ).status_code
                == 200,
                "Monitor credential must read status",
            )
            require(
                client.get(
                    "/v1/inventory/status",
                    headers={"Authorization": "Bearer " + credential},
                ).status_code
                == 401,
                "Writer must not read status",
            )
        check("working monitor credential reads status while writer is denied")
        spool, target, env = pipeline("synthetic", credential)
        writer_path = secret_paths[-1]
        for secret_state in ("missing", "empty"):
            if secret_state == "missing":
                writer_path.unlink()
            else:
                writer_path.write_text("")
            try:
                invalid_secret = subprocess.run(
                    vector_command + ["--config", str(target)],
                    env=env,
                    stdout=log,
                    stderr=log,
                    timeout=10,
                )
            except subprocess.TimeoutExpired as exc:
                raise ProbeFailure(
                    f"Vector did not reject the {secret_state} writer secret within 10 seconds"
                ) from exc
            require(
                invalid_secret.returncode != 0,
                f"{secret_state} writer secret must reject startup",
            )
        writer_path.write_text(credential + "\n")
        check("missing and empty writer secrets prevent Vector startup")
        large = fixture()
        large["categories"]["packages"]["items"] = [
            {"name": f"Größe-测试-{i}", "version": "1.2.3", "metadata": "x" * 180}
            for i in range(22000)
        ]
        source = emit.write_report(large, spool)
        require(source.stat().st_size > 4 * 1024 * 1024, "Large report fixture")
        process = start(target, env)
        wait_for(lambda: confirmed(large), "large report delivery")
        require(
            InventorySnapshot.objects.get(snapshot_id=large["snapshot_id"]).report
            == large,
            "Full report JSON equality",
        )
        result["large_report_bytes"] = source.stat().st_size
        check("generated Vector pipeline delivers >4 MiB Unicode report exactly")

        retry = fixture()
        retry["observed_at"] = (datetime.now(UTC) - timedelta(minutes=4)).isoformat()
        with proxy.lock:
            proxy.reject = True
        retry_source = emit.write_report(retry, spool)
        wait_for(lambda: attempted(retry, 2), "503 retries")
        require(not stored(retry), "503 must not store a report")
        time.sleep(2)  # Allow the disk buffer synchronization interval before SIGKILL.
        stop(force=True)
        retry_source.unlink()
        with proxy.lock:
            proxy.reject = False
        process = start(target, env)
        wait_for(
            lambda: confirmed(retry), "disk-buffer recovery without source", timeout=90
        )
        require(stored(retry) and not retry_source.exists(), "Disk recovery evidence")
        check(
            "503 retry and Vector SIGKILL recovery after deleting only the synthetic source"
        )

        lost = fixture()
        lost["observed_at"] = (datetime.now(UTC) - timedelta(minutes=3)).isoformat()
        with proxy.lock:
            proxy.drop_reply = True
        emit.write_report(lost, spool)
        wait_for(lambda: confirmed(lost, duplicate=True), "lost response retry")
        require(
            InventorySnapshot.objects.filter(snapshot_id=lost["snapshot_id"]).count()
            == 1,
            "Duplicate report inserted",
        )
        check(
            "lost committed response retries against Graphyard without a duplicate row"
        )

        failed = fixture()
        failed["observed_at"] = (datetime.now(UTC) - timedelta(minutes=2)).isoformat()
        failed["categories"]["packages"] = {
            "status": "error",
            "items": [],
            "error": "synthetic failure",
        }
        emit.write_report(failed, spool)
        wait_for(lambda: confirmed(failed), "failed category delivery")
        from graphyard.models import InventoryCategory

        category = InventoryCategory.objects.get(
            host__host__host_id="transport-probe", name="packages"
        )
        require(
            str(category.latest_success.snapshot_id) == lost["snapshot_id"],
            "Last good category lost",
        )
        require(
            str(category.latest_attempt.snapshot_id) == failed["snapshot_id"],
            "Failed attempt missing",
        )
        check(
            "failed category preserves last successful state through actual transport"
        )
        stop()

        with httpx.Client(base_url=url, trust_env=False, timeout=15) as client:
            require(
                client.get("/inventory/").status_code == 302,
                "Anonymous inventory access",
            )
            wrong = fixture()
            wrong["host"] = "wrong-host"
            require(
                client.post(
                    "/v1/inventory",
                    json=wrong,
                    headers={"Authorization": "Bearer " + credential},
                ).status_code
                == 403,
                "Host binding",
            )
            InventoryCredential.objects.filter(
                host__host__host_id="transport-probe"
            ).update(enabled=False)
            require(
                client.post(
                    "/v1/inventory",
                    json=fixture(),
                    headers={"Authorization": "Bearer " + credential},
                ).status_code
                == 401,
                "Credential revocation",
            )
            password = secrets.token_urlsafe(32)
            get_user_model().objects.create_user(
                username="probe-reader", password=password
            )
            require(client.get("/login/").status_code == 200, "Login page")
            response = client.post(
                "/login/",
                data={
                    "username": "probe-reader",
                    "password": password,
                    "csrfmiddlewaretoken": client.cookies["csrftoken"],
                },
            )
            require(response.status_code == 302, "Real session login")
            check("real HTTP login, private pages, host-bound writer and revocation")

            if args.report:
                require(
                    args.report.stat().st_size <= emit.MAX_BYTES,
                    "Local report too large",
                )
                local = json.loads(args.report.read_text())
                validate(local, timezone.now())
                require(
                    local["host"] != "transport-probe",
                    "Real report uses reserved synthetic host",
                )
                local_credential = enroll(local["host"])
                local_spool, local_target, local_env = pipeline(
                    "local", local_credential
                )
                emit.write_report(local, local_spool)
                process = start(local_target, local_env)
                wait_for(lambda: confirmed(local), "local report delivery")
                download = client.get(
                    f"/inventory/{local['host']}/{local['snapshot_id']}.json"
                )
                require(
                    download.status_code == 200 and download.json() == local,
                    "Authenticated report round trip",
                )
                detail = client.get(f"/inventory/{local['host']}/")
                require(
                    detail.status_code == 200 and local["host"] in detail.text,
                    "Local host detail",
                )
                (root / "local-inventory.html").write_text(detail.text)
                result["local_report"] = {
                    "host": local["host"],
                    "bytes": len(canonical(local)),
                    "sha256": hashlib.sha256(canonical(local)).hexdigest(),
                    "categories": {
                        k: {"status": v["status"], "items": len(v["items"])}
                        for k, v in local["categories"].items()
                    },
                }
                check(
                    "fresh local collector report delivered and downloaded unchanged via authenticated UI"
                )
                stop()
            overview = client.get("/inventory/")
            require(
                overview.status_code == 200 and "transport-probe" in overview.text,
                "Inventory overview",
            )
            (root / "overview.html").write_text(overview.text)
        result["status"] = "passed"
    except BaseException as exc:
        result["status"] = "failed"
        result["error"] = type(exc).__name__ + ": " + str(exc)
        if "proxy" in locals():
            with proxy.lock:
                result["content_types"] = sorted(proxy.content_types)
                result["http_statuses"] = list(proxy.http_statuses)
                result["proxy_errors"] = [dict(error) for error in proxy.errors]
        raise
    finally:
        stop()
        for server in reversed(servers):
            server.shutdown()
            server.server_close()
        for worker in workers:
            worker.join(timeout=5)
        if "django.db" in sys.modules:
            from django.db import connections

            connections.close_all()
        log.close()
        for path in secret_paths:
            path.unlink(missing_ok=True)
        result["processes_stopped"] = process is None and all(
            not w.is_alive() for w in workers
        )
        if not result["processes_stopped"]:
            result["status"] = "failed"
            result["cleanup_error"] = "Probe listener cleanup incomplete"
            result.setdefault("error", result["cleanup_error"])
        (root / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        os.umask(old_umask)
        print(f"Result: {root / 'result.json'}", flush=True)
        require(result["processes_stopped"], "Probe listener cleanup incomplete")


if __name__ == "__main__":
    main()
