"""Synthetic JSON admission regressions for the spec reconciliation command."""

import io
import sys

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from graphyard.models import MetricCollectionSpec


SPEC = '{"name":"synthetic","spec_type":"http_page_probe","config":{"url":"https://example.test/"}}'
DUPLICATES = [
    SPEC.replace('"name":"synthetic"', '"name":"synthetic","name":"synthetic"'),
    SPEC.replace('"spec_type":', '"enabled":false,"enabled":true,"spec_type":'),
    SPEC.replace('"spec_type":', '"spec_type":"private-type","spec_type":'),
    SPEC.replace('"config":', '"config":{"private":"synthetic-secret"},"config":'),
    SPEC.replace('"url":', '"extra":{"private":1,"priva\\u0074e":2},"url":'),
]


@pytest.mark.django_db
@pytest.mark.parametrize(
    "payload",
    [
        *("[" + spec + "]" for spec in DUPLICATES),
        '{"metric_collection_specs":[],"metric_collection_specs":[' + SPEC + "]}",
    ],
)
def test_duplicate_members_refuse_before_reads_or_mutation(
    tmp_path, django_assert_num_queries, payload
):
    MetricCollectionSpec.objects.create(name="keep", spec_type="http_page_probe")
    before = list(MetricCollectionSpec.objects.values())
    path = tmp_path / "synthetic.json"
    path.write_text(payload)
    stdout, stderr = io.StringIO(), io.StringIO()
    with (
        django_assert_num_queries(0),
        pytest.raises(
            CommandError, match="Duplicate JSON object key at member"
        ) as error,
    ):
        call_command(
            "apply_metric_collection_specs",
            file=str(path),
            prune=True,
            stdout=stdout,
            stderr=stderr,
        )
    assert len(str(error.value)) < len(str(path)) + 100
    assert "private" not in str(error.value)
    assert "synthetic-secret" not in str(error.value)
    assert stdout.getvalue() == stderr.getvalue() == ""
    assert list(MetricCollectionSpec.objects.values()) == before


@pytest.mark.parametrize("field", ['"ignored":', '"config":{"nested":'])
def test_integer_digit_limit_is_a_controlled_error_before_database(tmp_path, field):
    path = tmp_path / "synthetic.json"
    path.write_text("[{" + field + "9" * 5000 + ("}}]" if "config" in field else "}]"))
    previous = sys.get_int_max_str_digits()
    sys.set_int_max_str_digits(4300)
    try:
        with pytest.raises(
            CommandError, match="integer exceeds the decoder limit"
        ) as error:
            call_command("apply_metric_collection_specs", file=str(path), prune=True)
    finally:
        sys.set_int_max_str_digits(previous)
    assert len(str(error.value)) < len(str(path)) + 100


def test_repeated_records_retain_duplicate_spec_name_validation(tmp_path):
    path = tmp_path / "synthetic.json"
    path.write_text("[" + SPEC + "," + SPEC + "]")
    with pytest.raises(CommandError, match="duplicated"):
        call_command("apply_metric_collection_specs", file=str(path))
