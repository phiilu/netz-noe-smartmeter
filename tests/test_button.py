"""Tests for the button platform."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from homeassistant.core import HomeAssistant

from custom_components.netz_noe_smartmeter.button import NetzNoeRefreshButton
from custom_components.netz_noe_smartmeter.const import DOMAIN
from custom_components.netz_noe_smartmeter.coordinator import NetzNoeCoordinator

from .conftest import MOCK_METER_ID


def _make_button(hass: HomeAssistant) -> tuple[NetzNoeRefreshButton, MagicMock]:
    """Create a refresh button with a mocked coordinator."""
    coordinator = MagicMock(spec=NetzNoeCoordinator)
    coordinator.async_request_refresh = AsyncMock()
    button = NetzNoeRefreshButton(coordinator, MOCK_METER_ID)
    return button, coordinator


async def test_button_press_triggers_refresh(hass: HomeAssistant) -> None:
    """Test pressing the button triggers a coordinator refresh."""
    button, coordinator = _make_button(hass)

    await button.async_press()

    coordinator.async_request_refresh.assert_awaited_once()


def test_button_unique_id(hass: HomeAssistant) -> None:
    """Test button has correct unique_id."""
    button, _ = _make_button(hass)

    assert button.unique_id == f"{MOCK_METER_ID}_refresh"


def test_button_translation_key(hass: HomeAssistant) -> None:
    """Test button uses correct translation key."""
    button, _ = _make_button(hass)

    assert button.translation_key == "refresh"


def test_button_device_info(hass: HomeAssistant) -> None:
    """Test button is attached to the smart meter device."""
    button, _ = _make_button(hass)

    assert button.device_info is not None
    identifiers = button.device_info.get("identifiers")
    assert identifiers is not None
    assert (DOMAIN, MOCK_METER_ID) in identifiers
