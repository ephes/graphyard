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
