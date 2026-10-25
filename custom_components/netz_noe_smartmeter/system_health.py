"""Provide system health information for Netz NÖ Smart Meter."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol, cast

from homeassistant.components import system_health
from homeassistant.core import HomeAssistant, callback

from .const import PORTAL_URL


class _SystemHealthRegistrationProtocol(Protocol):
    """Typed protocol for system health registration."""

    def async_register_info(
        self,
        info_callback: Callable[[HomeAssistant], Awaitable[dict[str, object]]],
        manage_url: str | None = None,
    ) -> None:
        """Register system health callback."""


@callback
def async_register(
    _hass: HomeAssistant,
    register: system_health.SystemHealthRegistration,
) -> None:
    """Register system health callbacks."""
    typed_register = cast(_SystemHealthRegistrationProtocol, register)
    typed_register.async_register_info(system_health_info)


async def system_health_info(hass: HomeAssistant) -> dict[str, object]:
    """Get info for the info page."""
    return {
        "can_reach_server": await system_health.async_check_can_reach_url(hass, PORTAL_URL),
    }
