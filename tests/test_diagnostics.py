"""Tests for the diagnostics platform."""

from __future__ import annotations

from unittest.mock import MagicMock

from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.netz_noe_smartmeter.const import DOMAIN
from custom_components.netz_noe_smartmeter.coordinator import NetzNoeCoordinator, NetzNoeData
from custom_components.netz_noe_smartmeter.diagnostics import (
    TO_REDACT,
    async_get_config_entry_diagnostics,
)

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
    assert entry_data["meter_id"] == MOCK_METER_ID

    # Coordinator data should be present as a dict
    assert "coordinator_data" in result
    coordinator_data = result["coordinator_data"]
    assert isinstance(coordinator_data, dict)
    assert coordinator_data["daily_total"] == 1.5
    assert coordinator_data["yearly_total"] == 100.0


async def test_diagnostics_no_coordinator_data(hass: HomeAssistant) -> None:
    """Test diagnostics when coordinator has no data yet."""
    entry = _make_entry_with_coordinator(hass, None)

    result = await async_get_config_entry_diagnostics(hass, entry)  # type: ignore[arg-type]

    assert result["coordinator_data"] is None
    # Credentials should still be redacted
    entry_data = result["entry_data"]
    assert isinstance(entry_data, dict)
    assert entry_data["username"] == "**REDACTED**"


async def test_to_redact_contains_credentials() -> None:
    """Test that TO_REDACT includes username and password."""
    assert "username" in TO_REDACT
    assert "password" in TO_REDACT
