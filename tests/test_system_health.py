"""Tests for the system health platform."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from homeassistant.core import HomeAssistant

from custom_components.netz_noe_smartmeter.system_health import (
    PORTAL_URL,
    async_register,
    system_health_info,
)


async def test_system_health_info_returns_reachability(hass: HomeAssistant) -> None:
    """Test system_health_info returns a can_reach_server entry."""
    with patch(
        "custom_components.netz_noe_smartmeter.system_health.system_health.async_check_can_reach_url",
        new=AsyncMock(return_value="ok"),
    ) as mock_check_can_reach_url:
        info = await system_health_info(hass)

    assert "can_reach_server" in info
    assert info["can_reach_server"] == "ok"
    mock_check_can_reach_url.assert_awaited_once_with(hass, PORTAL_URL)


def test_async_register_registers_callback() -> None:
    """Test that async_register calls register.async_register_info."""
    hass = MagicMock()
    registration = MagicMock()

    async_register(hass, registration)

    registration.async_register_info.assert_called_once_with(system_health_info)


def test_portal_url_is_correct() -> None:
    """Test the portal URL constant."""
    assert PORTAL_URL == "https://smartmeter.netz-noe.at"
