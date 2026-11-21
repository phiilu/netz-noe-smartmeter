"""Tests for the diagnostics platform."""

from __future__ import annotations

from unittest.mock import MagicMock

from homeassistant.core import HomeAssistant

from custom_components.netz_noe_smartmeter.const import DOMAIN
from custom_components.netz_noe_smartmeter.coordinator import NetzNoeCoordinator, NetzNoeData
from custom_components.netz_noe_smartmeter.diagnostics import (
    TO_REDACT,
    async_get_config_entry_diagnostics,
)

from .common import MockConfigEntry
from .conftest import MOCK_CONFIG_DATA, MOCK_METER_ID


def _make_entry_with_coordinator(
    hass: HomeAssistant,
    data: NetzNoeData | None,
) -> MockConfigEntry:
    """Create a MockConfigEntry with a coordinator stub as runtime_data."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        title="Smart Meter",
        data=MOCK_CONFIG_DATA,
        unique_id=MOCK_METER_ID,
    )
    entry.add_to_hass(hass)

    coordinator = MagicMock(spec=NetzNoeCoordinator)
    coordinator.data = data
    coordinator.meter_id = MOCK_METER_ID
    coordinator.api = MagicMock(response_shapes={"ConsumptionRecord/Week": [{"MeterID": "str", "ec_id": None}]})
    entry.runtime_data = coordinator
    return entry


async def test_diagnostics_with_data(hass: HomeAssistant) -> None:
    """Test diagnostics returns redacted config and coordinator data."""
    data = NetzNoeData(
        daily_total=1.5,
        weekly_total=10.0,
        yearly_total=100.0,
        last_data_timestamp="2025-01-15T10:00:00",
    )
    entry = _make_entry_with_coordinator(hass, data)

    result = await async_get_config_entry_diagnostics(hass, entry)  # type: ignore[arg-type]

    # Config entry data should be present but credentials redacted
    assert "entry_data" in result
    entry_data = result["entry_data"]
    assert isinstance(entry_data, dict)
    assert entry_data["username"] == "**REDACTED**"
    assert entry_data["password"] == "**REDACTED**"
    assert entry_data["meter_id"] == "**REDACTED**"

    # Coordinator data should be present as a dict
    assert "coordinator_data" in result
    empty_statistic = {"statistic_id": "**REDACTED**", "last_sum": 0.0, "last_end": None}
    assert result["statistics"] == {
        **empty_statistic,
        "energy_community": {"self_coverage": empty_statistic, "grid_leftover": empty_statistic},
    }
    coordinator_data = result["coordinator_data"]
    assert isinstance(coordinator_data, dict)
    assert coordinator_data["daily_total"] == 1.5
    assert coordinator_data["yearly_total"] == 100.0
    assert result["api_responses"] == {"ConsumptionRecord/Week": [{"MeterID": "str", "ec_id": None}]}


async def test_diagnostics_no_coordinator_data(hass: HomeAssistant) -> None:
    """Test diagnostics when coordinator has no data yet."""
    entry = _make_entry_with_coordinator(hass, None)

    result = await async_get_config_entry_diagnostics(hass, entry)  # type: ignore[arg-type]

    assert result["coordinator_data"] is None
    # Credentials should still be redacted
    entry_data = result["entry_data"]
    assert isinstance(entry_data, dict)
    assert entry_data["username"] == "**REDACTED**"


async def test_to_redact_contains_credentials_and_identifiers() -> None:
    """Test that TO_REDACT covers credentials and the customer's meter identifiers."""
    assert {"username", "password", "meter_id", "statistic_id"} == TO_REDACT
