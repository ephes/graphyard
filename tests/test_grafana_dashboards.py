from __future__ import annotations

import json
from pathlib import Path

import pytest


GRAFANA_DASHBOARDS_ROOT = (
    Path(__file__).resolve().parents[1] / "deploy" / "grafana" / "dashboards"
)
DASHBOARD_PATHS = sorted(GRAFANA_DASHBOARDS_ROOT.glob("*/*.json"))


@pytest.mark.parametrize(
    "dashboard_path",
    DASHBOARD_PATHS,
    ids=lambda path: str(path.relative_to(GRAFANA_DASHBOARDS_ROOT)),
)
def test_provisioned_grafana_dashboards_are_valid_json(dashboard_path: Path) -> None:
    payload = json.loads(dashboard_path.read_text())

    assert payload["title"]
    assert payload["uid"]


def test_provisioned_grafana_dashboard_uids_are_unique() -> None:
    seen_uids: dict[str, Path] = {}

    for dashboard_path in DASHBOARD_PATHS:
        payload = json.loads(dashboard_path.read_text())
        uid = payload["uid"]

        assert uid not in seen_uids, (
            f"Duplicate Grafana dashboard uid {uid!r} in "
            f"{dashboard_path.relative_to(GRAFANA_DASHBOARDS_ROOT)} and "
            f"{seen_uids[uid].relative_to(GRAFANA_DASHBOARDS_ROOT)}"
        )
        seen_uids[uid] = dashboard_path


@pytest.mark.parametrize(
    ("dashboard_name", "dashboard_title", "subject_id", "extra_metrics"),
    [
        (
            "graphyard-macmini-infrastructure.json",
            "Macmini",
            "macmini",
            ("host.temperature_celsius", "host.fan_rpm"),
        ),
        (
            "graphyard-fractal-infrastructure.json",
            "Fractal",
            "fractal",
            (
                "host.temperature_celsius",
                "host.fan_rpm",
                "host.storage_filesystem_used_ratio",
                "host.storage_pool_used_ratio",
            ),
        ),
        (
            "graphyard-staging-infrastructure.json",
            "Staging",
            "staging",
            (),
        ),
        (
            "graphyard-production-infrastructure.json",
            "Production",
            "production",
            (),
        ),
        (
            "graphyard-marina-infrastructure.json",
            "Marina",
            "marina",
            (),
        ),
    ],
)
def test_host_dashboards_are_host_pinned_and_cover_device_metrics(
    dashboard_name: str,
    dashboard_title: str,
    subject_id: str,
    extra_metrics: tuple[str, ...],
) -> None:
    dashboard_path = GRAFANA_DASHBOARDS_ROOT / "host-infrastructure" / dashboard_name
    payload = json.loads(dashboard_path.read_text())

    queries = [
        target["query"]
        for panel in payload["panels"]
        for target in panel.get("targets", [])
        if "query" in target
    ]
    combined = "\n".join(queries)
    filesystem_panel = next(
        panel for panel in payload["panels"] if panel["title"] == "Filesystem Used"
    )
    latest_filesystem_panel = next(
        panel
        for panel in payload["panels"]
        if panel["title"] == "Latest Filesystem Values"
    )

    assert payload["title"] == dashboard_title
    assert "\"subject_type\" = 'host'" in combined
    assert f"\"subject_id\" = '{subject_id}'" in combined
    assert "host.cpu_seconds_total" in combined
    assert "host.load1" in combined
    assert "host.disk_read_bytes_total" in combined
    assert "host.disk_written_bytes_total" in combined
    assert "host.network_receive_bytes_total" in combined
    assert "host.network_transmit_bytes_total" in combined
    assert "host.filesystem_used_ratio" in combined
    assert subject_id in filesystem_panel.get("description", "")
    assert latest_filesystem_panel["fieldConfig"]["defaults"]["unit"] == "percentunit"
    for metric_name in extra_metrics:
        assert metric_name in combined


@pytest.mark.parametrize(
    ("dashboard_name", "dashboard_title", "subject_id"),
    [
        (
            "graphyard-fritz-box-7590-ax.json",
            "FRITZ!Box 7590 AX",
            "fritz_box_7590_ax",
        ),
        (
            "graphyard-usw-pro-xg-8-poe.json",
            "USW Pro XG 8 PoE",
            "usw_pro_xg_8_poe",
        ),
    ],
)
def test_network_device_dashboards_are_subject_pinned(
    dashboard_name: str, dashboard_title: str, subject_id: str
) -> None:
    dashboard_path = GRAFANA_DASHBOARDS_ROOT / "device-network" / dashboard_name
    payload = json.loads(dashboard_path.read_text())

    queries = [
        target["query"]
        for panel in payload["panels"]
        for target in panel.get("targets", [])
        if "query" in target
    ]
    combined = "\n".join(queries)

    assert payload["title"] == dashboard_title
    assert "\"subject_type\" = 'network_device'" in combined
    assert f"\"subject_id\" = '{subject_id}'" in combined
    assert "network_device.network_receive_bytes_per_second" in combined
    assert "network_device.network_transmit_bytes_per_second" in combined
    assert "\"device_class\" = 'temperature'" in combined
