"""UniFi device selection refuses ambiguous identities (synthetic devices)."""

import pytest

from graphyard import services

TWIN_A = {"name": "Core Switch", "mac": "AA:BB:CC:00:00:01"}
TWIN_B = {"name": "Core Switch", "mac": "aa:bb:cc:00:00:02"}


@pytest.mark.parametrize("devices", [[TWIN_A, TWIN_B], [TWIN_B, TWIN_A]])
def test_duplicate_name_without_mac_is_refused(devices):
    assert services._find_unifi_device(
        devices, device_name="Core Switch", device_mac=""
    ) == (None, services.UNIFI_DEVICE_AMBIGUOUS)


@pytest.mark.parametrize("devices", [[TWIN_A, TWIN_B], [TWIN_B, TWIN_A]])
def test_exact_mac_selects_one_regardless_of_order_and_name(devices):
    for name in ("", "Core Switch", "some other label"):
        assert services._find_unifi_device(
            devices, device_name=name, device_mac=" AA:BB:CC:00:00:02 "
        ) == (TWIN_B, "")


def test_configured_mac_is_authoritative_over_a_matching_name():
    # A stale/duplicate display name must not attribute counters to the
    # device with a different MAC.
    assert services._find_unifi_device(
        [TWIN_A], device_name="Core Switch", device_mac="aa:bb:cc:00:00:02"
    ) == (None, services.UNIFI_DEVICE_NOT_FOUND)


def test_unique_name_still_selects_and_duplicate_mac_is_refused():
    assert services._find_unifi_device(
        [TWIN_A, {"name": "Edge", "mac": "aa:bb:cc:00:00:03"}],
        device_name="Edge",
        device_mac="",
    ) == ({"name": "Edge", "mac": "aa:bb:cc:00:00:03"}, "")
    assert services._find_unifi_device(
        [TWIN_A, {**TWIN_B, "mac": "aa:bb:cc:00:00:01"}],
        device_name="",
        device_mac="aa:bb:cc:00:00:01",
    ) == (None, services.UNIFI_DEVICE_AMBIGUOUS)
