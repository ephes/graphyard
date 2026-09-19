#!/usr/bin/env python3
"""Opt-in direct sender integration: fresh SQLite FULL DB and loopback only."""

from __future__ import annotations

import argparse
import io
import json
import os
import secrets
import subprocess
import sys
import tempfile
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from wsgiref.simple_server import make_server

from inventory_transport_probe import (
    FaultProxy,
    ProbeFailure,
    QuietWSGIHandler,
    configure,
    fixture,
    require,
)


def checked_sender_result(completed, expected, credential):
    require(credential not in completed.stdout + completed.stderr, "Credential leaked")
    require(
        completed.returncode == expected,
        f"Unexpected sender exit code {completed.returncode}",
    )
    return json.loads(completed.stdout)


def failure_description(exc):
    return str(exc) if isinstance(exc, ProbeFailure) else type(exc).__name__


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--library",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "ops-library",
    )
    args = parser.parse_args()
    files = args.library.resolve() / "roles/software_estate/files"
    require((files / "send.py").is_file(), "Direct sender source required")
    sys.path.insert(0, str(files))
    try:
        import emit
    finally:
        sys.path.remove(str(files))

    root = Path(tempfile.mkdtemp(prefix="graphyard-direct-sender-")).resolve()
    old_umask = os.umask(0o077)
    result: dict[str, Any] = {"status": "running", "checks": []}
    servers = []
    workers = []
    credential_file = root / "writer"
    print(f"Private artifacts: {root}", flush=True)

    def check(name):
        result["checks"].append(name)
        print("PASS:", name, flush=True)

    try:
        configure(root, secrets.token_urlsafe(40))
        from django.core.management import call_command
        from django.core.wsgi import get_wsgi_application
        from graphyard.models import (
            HostRegistry,
            InventoryCategory,
            InventoryCredential,
            InventorySnapshot,
        )

        HostRegistry.objects.create(host_id="transport-probe")
        output = io.StringIO()
        call_command(
            "create_inventory_credential", host="transport-probe", stdout=output
        )
        credential = output.getvalue().strip()
        credential_file.write_text(credential + "\n")
        credential_file.chmod(0o600)
        backend = make_server(
            "127.0.0.1", 0, get_wsgi_application(), handler_class=QuietWSGIHandler
        )
        proxy = FaultProxy(backend.server_port)
        proxy.expected_auth = "Bearer " + credential
        for server in (backend, proxy):
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            try:
                worker.start()
            except Exception:
                server.server_close()
                raise
            servers.append(server)
            workers.append(worker)
        endpoint = f"http://127.0.0.1:{proxy.server_port}/v1/inventory"
        spool = root / "outbox"
        spool.mkdir(mode=0o700)
        command = [
            sys.executable,
            str(files / "send.py"),
            "--spool",
            str(spool),
            "--endpoint",
            endpoint,
            "--credential-file",
            str(credential_file),
            "--allow-loopback-http",
        ]
        check("isolated migrated SQLite FULL database and real loopback Graphyard")

        def run(expected):
            completed = subprocess.run(
                command, text=True, capture_output=True, timeout=130
            )
            outcome = checked_sender_result(completed, expected, credential)
            require(not proxy.errors, "Fault proxy error")
            return outcome

        def due_again():
            # Simulate time passing only in this private test fixture. First verify
            # another real process respects the persisted backoff.
            before = sum(proxy.attempts.values())
            run(1)
            require(sum(proxy.attempts.values()) == before, "Retry delay was ignored")
            path = spool / ".delivery.json"
            state = json.loads(path.read_text())
            for item in state.values():
                if item["status"] == "retry":
                    item["retry_at"] = 0
            path.write_text(json.dumps(state))

        large = fixture()
        large["categories"]["packages"]["items"] = [
            {"name": f"unicode-{n}", "description": "Größe雪" * 150}
            for n in range(5000)
        ]
        path = emit.write_report(large, spool)
        require(path.stat().st_size > 5 * 1024 * 1024, "Large report must exceed 5 MiB")
        run(0)
        require(not path.exists(), "Acknowledged report was not retired")
        require(
            InventorySnapshot.objects.get(snapshot_id=large["snapshot_id"]).report
            == large,
            "Unicode report altered",
        )
        check(
            "large Unicode report stored byte-equivalently and retired after acknowledgment"
        )

        retry = fixture()
        path = emit.write_report(retry, spool)
        with proxy.lock:
            proxy.reject = True
        run(1)
        require(path.exists(), "503 lost report")
        require(
            not InventorySnapshot.objects.filter(
                snapshot_id=retry["snapshot_id"]
            ).exists(),
            "503 injection did not execute",
        )
        due_again()
        with proxy.lock:
            proxy.reject = False
        run(0)
        require(not path.exists(), "Recovery did not retire report")
        check("503 preserves report; new process respects durable delay then recovers")

        lost = fixture()
        path = emit.write_report(lost, spool)
        with proxy.lock:
            proxy.drop_reply = True
        run(1)
        require(path.exists(), "Lost reply deleted unconfirmed report")
        row = InventorySnapshot.objects.get(snapshot_id=lost["snapshot_id"])
        received = row.received_at
        due_again()
        delivery = run(0)
        require(
            delivery["reports"][0]["reason"] == "duplicate",
            "Replay was not acknowledged as duplicate",
        )
        require(
            InventorySnapshot.objects.filter(snapshot_id=lost["snapshot_id"]).count()
            == 1,
            "Replay duplicated storage",
        )
        row.refresh_from_db()
        require(row.received_at == received, "Replay refreshed timestamp")
        check(
            "lost committed reply replays same identity once without refreshing inventory age"
        )

        newer = fixture()
        newer["observed_at"] = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
        newer["categories"]["packages"]["items"] = [{"name": "retained"}]
        emit.write_report(newer, spool)
        run(0)
        error = fixture()
        error["observed_at"] = datetime.now(UTC).isoformat()
        error["categories"]["packages"] = {
            "status": "error",
            "items": [],
            "error": "synthetic error",
        }
        emit.write_report(error, spool)
        run(0)
        old = fixture()
        emit.write_report(old, spool)
        run(0)
        category = InventoryCategory.objects.get(name="packages")
        require(
            str(category.latest_success.snapshot_id) == newer["snapshot_id"],
            "Late/error report replaced latest good state",
        )
        require(
            str(category.latest_attempt.snapshot_id) == error["snapshot_id"],
            "Late report replaced newest attempt",
        )
        removed = fixture()
        removed["observed_at"] = datetime.now(UTC).isoformat()
        emit.write_report(removed, spool)
        run(0)
        category.refresh_from_db()
        require(
            category.latest_success.report["categories"]["packages"]["items"] == [],
            "Package removal missing",
        )
        check(
            "out-of-order/error retention and complete package removal through HTTP sender"
        )

        wrong = fixture()
        wrong["host"] = "another-host"
        path = emit.write_report(wrong, spool)
        run(1)
        require(path.exists(), "Wrong-host report lost")
        require(403 in proxy.http_statuses, "Host binding rejection not exercised")
        valid = fixture()
        valid_path = emit.write_report(valid, spool)
        run(1)
        require(
            not valid_path.exists() and path.exists(),
            "Blocked report obstructed valid delivery",
        )
        check("real 403 host rejection retained without obstructing other reports")

        InventoryCredential.objects.update(enabled=False)
        revoked = fixture()
        revoked_path = emit.write_report(revoked, spool)
        run(1)
        require(revoked_path.exists(), "Revoked-writer report lost")
        require(401 in proxy.http_statuses, "Revocation rejection not exercised")
        check("real revoked credential rejected and report retained")
        result["status"] = "passed"
    except Exception as exc:
        result["status"] = "failed"
        result["error"] = failure_description(exc)
        # Only caller-authored assertion text, never peer bodies/stdout/stderr.
        print("FAIL:", result["error"], flush=True)
    finally:
        credential_file.unlink(missing_ok=True)
        for server in reversed(servers):
            server.shutdown()
            server.server_close()
        for worker in workers:
            worker.join(timeout=5)
        result["listeners_stopped"] = all(not worker.is_alive() for worker in workers)
        if not result["listeners_stopped"] or credential_file.exists():
            result["status"] = "failed"
            result["cleanup_error"] = "Incomplete cleanup"
        (root / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        os.umask(old_umask)
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
