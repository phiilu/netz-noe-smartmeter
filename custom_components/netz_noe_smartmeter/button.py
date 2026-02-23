"""Button platform for Netz NÖ Smart Meter."""

from __future__ import annotations

from typing import cast, override

from homeassistant.components.button import ButtonEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceEntryType, DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import CONF_METER_ID, DOMAIN, NetzNoeConfigData
from .coordinator import NetzNoeCoordinator


async def async_setup_entry(
    _hass: HomeAssistant,
    config_entry: ConfigEntry[NetzNoeCoordinator],
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Netz NÖ Smart Meter buttons."""
    coordinator = config_entry.runtime_data
    config_data = cast(NetzNoeConfigData, cast(object, config_entry.data))
    meter_id = config_data[CONF_METER_ID]

    async_add_entities([NetzNoeRefreshButton(coordinator, meter_id)])


class NetzNoeRefreshButton(ButtonEntity):
    """Button to trigger a manual data refresh."""

    _attr_has_entity_name: bool = True
    _attr_translation_key: str | None = "refresh"
    _attr_icon: str | None = "mdi:refresh"
    _coordinator: NetzNoeCoordinator
    _attr_unique_id: str | None
    _attr_device_info: DeviceInfo | None

    def __init__(self, coordinator: NetzNoeCoordinator, meter_id: str) -> None:
        """Initialize the button."""
        self._coordinator = coordinator
        self._attr_unique_id = f"{meter_id}_refresh"

        short_id = meter_id[-10:] if len(meter_id) > 10 else meter_id
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, meter_id)},
            name=f"Smart Meter {short_id}",
            manufacturer="Netz NÖ",
            model="Smart Meter",
            entry_type=DeviceEntryType.SERVICE,
            configuration_url="https://smartmeter.netz-noe.at",
        )

    @override
    async def async_press(self) -> None:
        """Handle the button press."""
        await self._coordinator.async_request_refresh()
