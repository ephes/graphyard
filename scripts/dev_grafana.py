"""Start local Grafana; Docker atomically refuses any existing same-name container."""

import os
from pathlib import Path
import sys


def main() -> None:
    root = Path.cwd()
    (root / ".dev/grafana").mkdir(parents=True, exist_ok=True)
    args = [
        "docker",
        "run",
        "--rm",
        "--name",
        "graphyard-grafana",
        "-p",
        "127.0.0.1:3000:3000",
        "--add-host=graphyard-influxdb:host-gateway",
        "-e",
        "GF_SECURITY_ADMIN_USER=admin",
        "-e",
        "GF_SECURITY_ADMIN_PASSWORD=admin",
        "-e",
        "GF_USERS_ALLOW_SIGN_UP=false",
        "-e",
        "GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/etc/grafana/provisioning/dashboards-json/overview/graphyard-home.json",
        "-e",
        "GRAPHYARD_INFLUX_DB=graphyard",
        "-e",
        "GRAPHYARD_INFLUX_USER=dev",
        "-e",
        "GRAPHYARD_INFLUX_PASSWORD=graphyard-dev",
        "-v",
        f"{root}/.dev/grafana:/var/lib/grafana",
        "-v",
        f"{root}/deploy/grafana/provisioning:/etc/grafana/provisioning",
        "-v",
        f"{root}/deploy/grafana/dashboards:/etc/grafana/provisioning/dashboards-json",
        "grafana/grafana-oss",
        "sh",
        "-c",
        "grafana cli --homepath /usr/share/grafana admin reset-admin-password "
        '"$GF_SECURITY_ADMIN_PASSWORD" || true; exec /run.sh',
    ]
    try:
        os.execvp("docker", args)
    except FileNotFoundError:
        sys.exit("docker not found (install Docker or run Grafana separately on :3000)")


if __name__ == "__main__":
    main()
