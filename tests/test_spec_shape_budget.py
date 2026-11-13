"""Bound shape before the tree decoder, using deterministic synthetic budgets."""

import json

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from graphyard.management.commands import apply_metric_collection_specs as command


@pytest.mark.parametrize(
    "payload,reason,nodes",
    [
        ("[" * 66 + "0" + "]" * 66, "nesting too deep", 100),
        (json.dumps({"ignored": [[] for _ in range(21)]}), "node limit", 20),
        (json.dumps([{} for _ in range(4)]), "definition limit", 100),
        (
            json.dumps({"metric_collection_specs": [{} for _ in range(4)]}),
            "definition limit",
            100,
        ),
    ],
)
def test_shape_budgets_run_before_tree_decoder(
    tmp_path, monkeypatch, payload, reason, nodes
):
    monkeypatch.setattr(command, "MAX_SPEC_FILE_NODES", nodes, raising=False)
    monkeypatch.setattr(command, "MAX_SPEC_FILE_SPECS", 3, raising=False)

    def no_tree(*args, **kwargs):
        pytest.fail("over-budget input reached full JSON materialization")

    monkeypatch.setattr(command.json, "loads", no_tree)
    path = tmp_path / "synthetic.json"
    path.write_text(payload)
    with pytest.raises(CommandError, match=reason):
        call_command("apply_metric_collection_specs", file=str(path))


@pytest.mark.parametrize("wrapped", [False, True])
def test_exact_shape_limits_allow_normalization(tmp_path, monkeypatch, wrapped):
    monkeypatch.setattr(command, "MAX_SPEC_FILE_SPECS", 2, raising=False)
    specs = [
        {"name": name, "spec_type": "http_page_probe", "config": {"extra": [1]}}
        for name in ("first", "second")
    ]
    payload = {"metric_collection_specs": specs} if wrapped else specs
    # One root + two specs of 10 nodes each; envelope adds two (key and list).
    monkeypatch.setattr(
        command, "MAX_SPEC_FILE_NODES", 23 if wrapped else 21, raising=False
    )
    path = tmp_path / "synthetic.json"
    path.write_text(json.dumps(payload))
    assert len(command._load_specs_file(path)) == 2
