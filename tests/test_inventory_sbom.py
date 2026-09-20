import copy
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
from cyclonedx.schema import SchemaVersion
from cyclonedx.validation.json import JsonStrictValidator

from graphyard.inventory_sbom import EXPORT_VERSION, Unavailable, build, eligible


@pytest.fixture
def snapshot():
    return SimpleNamespace(
        snapshot_id=uuid4(),
        digest="a" * 64,
        report={
            "host": "studio",
            "observed_at": "2026-09-19T08:00:00Z",
            "collector": "software-estate/1",
            "categories": {
                "applications": {
                    "status": "ok",
                    "items": [
                        {
                            "id": "demo",
                            "status": "ok",
                            "items": {
                                "git": {
                                    "status": "ok",
                                    "items": {"commit": "abc", "dirty": True},
                                },
                                "python": {
                                    "status": "ok",
                                    "items": [
                                        {
                                            "name": "Example_Package",
                                            "version": "1.2+local",
                                            "requires": [
                                                'other>=2; extra == "optional"'
                                            ],
                                            "license": "Not validated as SPDX",
                                        }
                                    ],
                                },
                            },
                        }
                    ],
                },
            },
        },
    )


def test_validated_stable_incomplete_export_preserves_metadata(snapshot):
    document = build(snapshot, "demo")
    validator = JsonStrictValidator(SchemaVersion.V1_6)
    assert validator.validate_str(json.dumps(document)) is None
    assert build(snapshot, "demo") == document
    assert document["metadata"]["tools"]["components"] == [
        {
            "type": "application",
            "name": "graphyard-python-sbom",
            "version": EXPORT_VERSION,
        }
    ]
    package = document["components"][0]
    assert package["purl"] == "pkg:pypi/example-package@1.2%2Blocal"
    metadata = json.loads(package["properties"][0]["value"])
    assert metadata["requires"] == ['other>=2; extra == "optional"']
    assert metadata["license"] == "Not validated as SPDX"
    assert "licenses" not in package and "hashes" not in package
    assert "dependencies" not in document
    assert document["compositions"][0]["aggregate"] == "incomplete"
    props = {p["name"]: p["value"] for p in document["metadata"]["properties"]}
    assert props["graphyard:observed-at"] == snapshot.report["observed_at"]
    assert props["graphyard:source-report-sha256"] == snapshot.digest
    assert props["graphyard:snapshot-id"] == str(snapshot.snapshot_id)
    assert json.loads(props["graphyard:source-evidence"])["git"]["items"]["dirty"]
    assert "timestamp" not in document["metadata"]  # Never invent a new observation.
    different = copy.deepcopy(snapshot)
    different.digest = "b" * 64
    assert build(different, "demo")["serialNumber"] != document["serialNumber"]


def test_partial_category_and_failed_app_can_export_successful_python(snapshot):
    category = snapshot.report["categories"]["applications"]
    category["partial_items"] = category["items"]
    category["items"] = []
    category["status"] = "error"
    category["partial_items"][0]["status"] = "error"
    document = build(snapshot, "demo")
    props = {p["name"]: p["value"] for p in document["metadata"]["properties"]}
    assert props["graphyard:application-category-status"] == "error"
    assert props["graphyard:application-probe-status"] == "error"
    assert len(document["components"]) == 1


@pytest.mark.parametrize(
    "value",
    [
        None,
        {},
        [None],
        [{"name": "ok"}],
        [{"name": "../bad", "version": "1"}],
        [{"name": "ok", "version": ""}],
        [{"name": "ok", "version": "1\n"}],
        [{"name": "ok", "version": "1", "requires": None}],
        [{"name": "ok", "version": "1", "requires": [None]}],
        [
            {"name": "EXAMPLE_package", "version": "1"},
            {"name": "example.package", "version": "2"},
        ],
    ],
)
def test_malformed_and_ambiguous_packages_refuse_export(snapshot, value):
    app = snapshot.report["categories"]["applications"]["items"][0]
    app["items"]["python"]["items"] = value
    assert eligible(app) is False
    with pytest.raises(Unavailable):
        build(snapshot, "demo")


def test_missing_or_duplicate_application_refuses_export(snapshot):
    with pytest.raises(Unavailable):
        build(snapshot, "missing")
    apps = snapshot.report["categories"]["applications"]["items"]
    apps.append(copy.deepcopy(apps[0]))
    with pytest.raises(Unavailable):
        build(snapshot, "demo")


def test_failed_python_refuses_export_and_empty_inventory_is_incomplete(snapshot):
    app = snapshot.report["categories"]["applications"]["items"][0]
    app["items"]["python"]["status"] = "error"
    assert not eligible(app)
    with pytest.raises(Unavailable):
        build(snapshot, "demo")
    app["items"]["python"] = {"status": "ok", "items": []}
    document = build(snapshot, "demo")
    assert document["components"] == []
    assert document["compositions"][0]["aggregate"] == "incomplete"
    assert (
        JsonStrictValidator(SchemaVersion.V1_6).validate_str(json.dumps(document))
        is None
    )


@pytest.mark.parametrize(
    "category",
    [
        None,
        {},
        {"status": "ok", "items": None},
        {"status": "ok", "items": {}},
        {"status": "ok", "items": [None]},
        {"status": "error", "partial_items": None},
        {"status": 42, "items": []},
    ],
)
def test_malformed_stored_category_refuses_export(snapshot, category):
    snapshot.report["categories"]["applications"] = category
    with pytest.raises(Unavailable):
        build(snapshot, "demo")


@pytest.mark.parametrize("field", ["host", "observed_at", "collector", "categories"])
def test_missing_or_invalid_stored_provenance_refuses_export(snapshot, field):
    for value in [None, 42, False, ""]:
        snapshot.report[field] = value
        with pytest.raises(Unavailable):
            build(snapshot, "demo")
    del snapshot.report[field]
    with pytest.raises(Unavailable):
        build(snapshot, "demo")


def test_missing_stored_application_category_refuses_export(snapshot):
    del snapshot.report["categories"]["applications"]
    with pytest.raises(Unavailable):
        build(snapshot, "demo")
