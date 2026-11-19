"""Fixtures for netz_noe_smartmeter tests."""

from __future__ import annotations

from collections.abc import Generator
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME

from custom_components.netz_noe_smartmeter.const import CONF_METER_ID

MOCK_METER_ID = "AT0020000000000000001000000654321"

MOCK_CONFIG_DATA = {
    CONF_USERNAME: "test@example.com",
    CONF_PASSWORD: "test-password",
    CONF_METER_ID: MOCK_METER_ID,
}

MOCK_DAY_RESPONSE = {
    "meteredValues": [0.1, 0.2, 0.3, None],
    "peakDemandTimes": [
        "2025-01-15T00:15:00",
        "2025-01-15T00:30:00",
        "2025-01-15T00:45:00",
        None,
    ],
}

MOCK_WEEK_RESPONSE = {
    "meteredValues": [1.5, 2.0, None, None, None, None, None],
}

MOCK_YEAR_RESPONSE = {
    "values": [10.0, 20.0, 30.0, None, None, None, None, None, None, None, None, None],
}

# Energy community member, daily values from a portal export (week of 9 Nov 2026, Monday to Sunday).
# meteredValues is the sum of the two split values.
MOCK_EEG_WEEK_RESPONSE = {
    "meteredValues": [36.828, 26.577, 27.333, 23.025, 31.438, 33.451, 38.176],
    "gridUsageLeftoverValues": [36.733, 26.481, 26.608, 22.318, 31.219, 23.778, 33.237],
    "selfCoverageValues": [0.095, 0.096, 0.725, 0.707, 0.219, 9.673, 4.939],
}

MOCK_EEG_DAY_RESPONSE = {
    "meteredValues": [10.0, 13.451, 10.0, None],
    "peakDemandTimes": [
        "2026-11-14T00:15:00",
        "2026-11-14T00:30:00",
        "2026-11-14T00:45:00",
        None,
    ],
    "gridUsageLeftoverValues": [10.0, 3.778, 10.0, None],
    "selfCoverageValues": [0.0, 9.673, 0.0, None],
}

MOCK_EEG_YEAR_RESPONSE = {
    "values": [None] * 9 + [700.0, 506.394, None],
    "gridUsageLeftoverValues": [None] * 9 + [650.0, 473.809, None],
    "selfCoverageValues": [None] * 9 + [50.0, 32.585, None],
}

MOCK_EEG_RESPONSES = {
    "get_day": MOCK_EEG_DAY_RESPONSE,
    "get_week": MOCK_EEG_WEEK_RESPONSE,
    "get_year": MOCK_EEG_YEAR_RESPONSE,
}

MOCK_RAW_RESPONSE = [
    {"MeasuredValue": 0.1, "Endtime": "2025-01-15T10:15:00+01:00"},
    {"MeasuredValue": 0.2, "Endtime": "2025-01-15T10:30:00+01:00"},
    {"MeasuredValue": 0.3, "Endtime": "2025-01-15T10:45:00+01:00"},
    {"MeasuredValue": 0.4, "Endtime": "2025-01-15T11:00:00+01:00"},
]


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(
    recorder_mock: None,
    enable_custom_integrations: None,
) -> None:
    """Enable custom integrations and recorder for all tests."""


@pytest.fixture
def mock_setup_entry() -> Generator[AsyncMock]:
    """Prevent the created config entry from being set up against the real API."""
    with patch("custom_components.netz_noe_smartmeter.async_setup_entry", return_value=True) as mock:
        yield mock
