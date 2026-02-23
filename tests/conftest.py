"""Fixtures for netz_noe_smartmeter tests."""

from __future__ import annotations

from collections.abc import Generator
from unittest.mock import AsyncMock, MagicMock, patch

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
def mock_api() -> Generator[MagicMock]:
    """Return a mocked NetzNoeApi."""
    with patch(
        "custom_components.netz_noe_smartmeter.config_flow.NetzNoeApi",
        autospec=True,
    ) as mock_cls:
        api = mock_cls.return_value
        api.authenticate = AsyncMock(return_value=True)
        api.logout = AsyncMock()
        api.get_day = AsyncMock(return_value=MOCK_DAY_RESPONSE)
        api.get_week = AsyncMock(return_value=MOCK_WEEK_RESPONSE)
        api.get_year = AsyncMock(return_value=MOCK_YEAR_RESPONSE)
        api.get_raw = AsyncMock(return_value=MOCK_RAW_RESPONSE)
        api.is_authenticated = True
        api.invalidate_session = lambda: None
        yield api
