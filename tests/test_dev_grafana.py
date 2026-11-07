"""Local startup contracts with synthetic Docker inventory, without a daemon."""

import os
from pathlib import Path
import runpy
import shlex

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "dev_grafana.py"


def test_procfile_uses_safe_launcher():
    command = next(
        line.partition(":")[2].strip()
        for line in (ROOT / "Procfile").read_text().splitlines()
        if line.startswith("grafana:")
    )
    assert shlex.split(command) == ["uv", "run", "python", "scripts/dev_grafana.py"]


@pytest.mark.parametrize("existing", [None, "other-checkout", "this-checkout-stale"])
def test_startup_never_removes_existing_containers(monkeypatch, tmp_path, existing):
    calls = []
    inventory = {"graphyard-grafana": existing} if existing else {}
    original_inventory = inventory.copy()
    checkout = tmp_path / "checkout with spaces"
    checkout.mkdir()
    monkeypatch.chdir(checkout)

    def docker(file, args):
        assert file == "docker"
        calls.append(args)
        assert args[:2] == ["docker", "run"]
        name = args[args.index("--name") + 1]
        if name in inventory:
            raise SystemExit(125)  # Docker refuses a conflicting name.
        inventory[name] = str(checkout)
        raise SystemExit(0)

    monkeypatch.setattr(os, "execvp", docker)
    with pytest.raises(SystemExit) as result:
        runpy.run_path(str(SCRIPT), run_name="__main__")
    assert result.value.code == (125 if existing else 0)
    assert len(calls) == 1
    if existing:
        assert inventory == original_inventory
    args = calls[0]
    assert "--rm" in args
    assert args[args.index("-p") + 1] == "127.0.0.1:3000:3000"
    assert f"{checkout}/.dev/grafana:/var/lib/grafana" in args
    assert (checkout / ".dev/grafana").is_dir()


def test_grafana_identity_matches_datasource_and_runbook(monkeypatch, tmp_path):
    commands = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(os, "execvp", lambda _, args: commands.append(args))
    runpy.run_path(str(SCRIPT), run_name="__main__")
    args = commands[0]
    env = dict(args[i + 1].split("=", 1) for i, arg in enumerate(args) if arg == "-e")
    provisioning = (
        ROOT / "deploy/grafana/provisioning/datasources/graphyard.yaml"
    ).read_text()
    for key, value in env.items():
        provisioning = provisioning.replace("${" + key + "}", value)
    datasource = yaml.safe_load(provisioning)["datasources"][0]
    assert datasource["jsonData"] == {
        "version": "InfluxQL",
        "dbName": "graphyard",
        "httpMode": "POST",
    }
    assert datasource["user"] == "dev"
    assert datasource["secureJsonData"]["password"] == "graphyard-dev"
    assert datasource["url"] == "http://graphyard-influxdb:8086"
    assert "--add-host=graphyard-influxdb:host-gateway" in args
    runbook = (ROOT / "docs/influx_grafana.md").read_text()
    assert "--db graphyard --rp autogen" in runbook
    assert "--username dev --password graphyard-dev" in runbook
    # A documented query must validate data and credentials, not merely HTTP 200.
    assert 'SELECT "value" FROM "graphyard_metrics"' in runbook
    assert "local.bootstrap" in runbook
    assert "results" in runbook
