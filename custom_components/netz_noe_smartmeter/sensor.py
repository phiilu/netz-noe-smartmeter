"""Sensor platform for Netz NÖ Smart Meter."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from functools import cached_property
from typing import ClassVar, cast, override

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EntityCategory, UnitOfEnergy
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceEntryType, DeviceInfo
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import BaseCoordinatorEntity

from .const import CONF_METER_ID, DOMAIN, NetzNoeConfigData
from .coordinator import NetzNoeCoordinator, NetzNoeData

SIGNAL_BACKFILL_PROGRESS = f"{DOMAIN}_backfill_progress"

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    _hass: HomeAssistant,
    config_entry: ConfigEntry[NetzNoeCoordinator],
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Netz NÖ Smart Meter sensors."""
    coordinator = config_entry.runtime_data
    config_data = cast(NetzNoeConfigData, cast(object, config_entry.data))
    meter_id = config_data[CONF_METER_ID]

    entities: list[SensorEntity] = [
        NetzNoeDailyConsumptionSensor(coordinator, meter_id),
        NetzNoeWeeklyConsumptionSensor(coordinator, meter_id),
        NetzNoeMonthlyConsumptionSensor(coordinator, meter_id),
        NetzNoeYearlyConsumptionSensor(coordinator, meter_id),
        NetzNoeLastUpdatedSensor(coordinator, meter_id),
        NetzNoeBackfillStatusSensor(coordinator, meter_id),
    ]

    async_add_entities(entities)


PARALLEL_UPDATES = 1


class NetzNoeSensorBase(BaseCoordinatorEntity[NetzNoeCoordinator], SensorEntity):
    """Base class for Netz NÖ sensors."""

    _attr_has_entity_name: bool = True
    _meter_id: str
    _attr_unique_id: str | None
    _attr_translation_key: str | None
    _attr_device_info: DeviceInfo | None

    def __init__(
        self,
        coordinator: NetzNoeCoordinator,
        meter_id: str,
        key: str,
    ) -> None:
        """Initialize the sensor."""
        super().__init__(coordinator)
        self._meter_id = meter_id
        self._attr_unique_id = f"{meter_id}_{key}"
        self._attr_translation_key = key

        # Short meter ID for display
        short_id = meter_id[-10:] if len(meter_id) > 10 else meter_id

        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, meter_id)},
            name=f"Smart Meter {short_id}",
            manufacturer="Netz NÖ",
            model="Smart Meter",
            entry_type=DeviceEntryType.SERVICE,
            configuration_url="https://smartmeter.netz-noe.at",
        )

    @cached_property
    @override
    def available(self) -> bool:
        """Return whether data from the coordinator is available."""
        return self.coordinator.last_update_success

    @override
    async def async_update(self) -> None:
        """Update the entity using the coordinator refresh request."""
        if not self.enabled:
            return
        await self.coordinator.async_request_refresh()

    @callback
    @override
    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator."""
        cache = cast(dict[str, object], cast(object, self.__dict__))
        _ = cache.pop("available", None)
        _ = cache.pop("native_value", None)
        _ = cache.pop("extra_state_attributes", None)
        super()._handle_coordinator_update()


class NetzNoeDailyConsumptionSensor(NetzNoeSensorBase):
    """Sensor for yesterday's total consumption."""

    _attr_device_class: SensorDeviceClass | None = SensorDeviceClass.ENERGY
    _attr_state_class: SensorStateClass | str | None = SensorStateClass.MEASUREMENT
    _attr_native_unit_of_measurement: str | None = UnitOfEnergy.KILO_WATT_HOUR
    _attr_icon: str | None = "mdi:flash"
    _attr_suggested_display_precision: int | None = 3

    def __init__(self, coordinator: NetzNoeCoordinator, meter_id: str) -> None:
        """Initialize."""
        super().__init__(coordinator, meter_id, "daily_consumption")

    @cached_property
    @override
    def native_value(self) -> float | None:
        """Return yesterday's total consumption."""
        data = cast(NetzNoeData | None, self.coordinator.data)
        if data is None:
            return None
        return data.daily_total


class NetzNoeWeeklyConsumptionSensor(NetzNoeSensorBase):
    """Sensor for current week's consumption (Mon-Sun)."""

    _attr_device_class: SensorDeviceClass | None = SensorDeviceClass.ENERGY
    _attr_state_class: SensorStateClass | str | None = SensorStateClass.MEASUREMENT
    _attr_native_unit_of_measurement: str | None = UnitOfEnergy.KILO_WATT_HOUR
    _attr_icon: str | None = "mdi:calendar-week"
    _attr_suggested_display_precision: int | None = 3

    def __init__(self, coordinator: NetzNoeCoordinator, meter_id: str) -> None:
        """Initialize."""
        super().__init__(coordinator, meter_id, "weekly_consumption")

    @cached_property
    @override
    def native_value(self) -> float | None:
        """Return current week's consumption."""
        data = cast(NetzNoeData | None, self.coordinator.data)
        if data is None:
            return None
        return data.weekly_total


class NetzNoeMonthlyConsumptionSensor(NetzNoeSensorBase):
    """Sensor for current month's consumption."""

    _attr_device_class: SensorDeviceClass | None = SensorDeviceClass.ENERGY
    _attr_state_class: SensorStateClass | str | None = SensorStateClass.MEASUREMENT
    _attr_native_unit_of_measurement: str | None = UnitOfEnergy.KILO_WATT_HOUR
    _attr_icon: str | None = "mdi:calendar-month"
    _attr_suggested_display_precision: int | None = 3

    def __init__(self, coordinator: NetzNoeCoordinator, meter_id: str) -> None:
        """Initialize."""
        super().__init__(coordinator, meter_id, "monthly_consumption")

    @cached_property
    @override
    def native_value(self) -> float | None:
        """Return current month's consumption."""
        data = cast(NetzNoeData | None, self.coordinator.data)
        if data is None:
            return None
        return data.monthly_total


class NetzNoeYearlyConsumptionSensor(NetzNoeSensorBase):
    """Sensor for year-to-date consumption with monthly breakdown."""

    _attr_device_class: SensorDeviceClass | None = SensorDeviceClass.ENERGY
    _attr_state_class: SensorStateClass | str | None = SensorStateClass.MEASUREMENT
    _attr_native_unit_of_measurement: str | None = UnitOfEnergy.KILO_WATT_HOUR
    _attr_icon: str | None = "mdi:calendar"
    _attr_suggested_display_precision: int | None = 3

    MONTH_NAMES: ClassVar[list[str]] = [
        "jan",
        "feb",
        "mar",
        "apr",
        "mai",
        "jun",
        "jul",
        "aug",
        "sep",
        "okt",
        "nov",
        "dez",
    ]

    def __init__(self, coordinator: NetzNoeCoordinator, meter_id: str) -> None:
        """Initialize."""
        super().__init__(coordinator, meter_id, "yearly_consumption")

    @cached_property
    @override
    def native_value(self) -> float | None:
        """Return year-to-date consumption."""
        data = cast(NetzNoeData | None, self.coordinator.data)
        if data is None:
            return None
        return data.yearly_total

    @cached_property
    @override
    def extra_state_attributes(self) -> dict[str, float | None] | None:
        """Return monthly breakdown as attributes."""
        data = cast(NetzNoeData | None, self.coordinator.data)
        if data is None:
            return None

        attrs: dict[str, float | None] = {}
        monthly = data.monthly_values
        if monthly:
            for i, val in enumerate(monthly[:12]):
                attrs[self.MONTH_NAMES[i]] = round(val, 3) if val is not None else None

        # Previous year data
        prev_monthly = data.prev_year_monthly_values
        if prev_monthly:
            prev_year = data.prev_year
            for i, val in enumerate(prev_monthly[:12]):
                if val is not None:
                    attrs[f"{self.MONTH_NAMES[i]}_{prev_year}"] = round(val, 3)

        return attrs


class NetzNoeLastUpdatedSensor(NetzNoeSensorBase):
    """Sensor showing the timestamp of the newest data point."""

    _attr_device_class: SensorDeviceClass | None = SensorDeviceClass.TIMESTAMP
    _attr_icon: str | None = "mdi:clock-check-outline"

    def __init__(self, coordinator: NetzNoeCoordinator, meter_id: str) -> None:
        """Initialize."""
        super().__init__(coordinator, meter_id, "last_updated")

    @cached_property
    @override
    def native_value(self) -> datetime | None:
        """Return the timestamp of the newest data point."""
        data = cast(NetzNoeData | None, self.coordinator.data)
        if data is None:
            return None

        last_data_timestamp = data.last_data_timestamp
        if last_data_timestamp is None:
            return None
        try:
            dt = datetime.fromisoformat(last_data_timestamp)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt
        except (ValueError, TypeError):
            return None


class NetzNoeBackfillStatusSensor(SensorEntity):
    """Sensor showing the status of the backfill service."""

    _attr_has_entity_name: bool = True
    _attr_translation_key: str | None = "backfill_status"
    _attr_icon: str | None = "mdi:database-import"
    _attr_entity_category: EntityCategory | None = EntityCategory.DIAGNOSTIC
    _coordinator: NetzNoeCoordinator
    _attr_unique_id: str | None
    _attr_device_info: DeviceInfo | None

    def __init__(self, coordinator: NetzNoeCoordinator, meter_id: str) -> None:
        """Initialize the backfill status sensor."""
        self._coordinator = coordinator
        self._attr_unique_id = f"{meter_id}_backfill_status"

        short_id = meter_id[-10:] if len(meter_id) > 10 else meter_id
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, meter_id)},
            name=f"Smart Meter {short_id}",
            manufacturer="Netz NÖ",
            model="Smart Meter",
            entry_type=DeviceEntryType.SERVICE,
            configuration_url="https://smartmeter.netz-noe.at",
        )

    @cached_property
    @override
    def native_value(self) -> str:
        """Return the current backfill state."""
        return self._coordinator.backfill_progress.state

    @cached_property
    @override
    def extra_state_attributes(self) -> dict[str, object]:
        """Return progress details as attributes."""
        progress = self._coordinator.backfill_progress
        attrs: dict[str, object] = {}
        if progress.state in {"running", "clearing"}:
            attrs["current_chunk"] = progress.current_chunk
            if progress.total_chunks:
                attrs["total_chunks"] = progress.total_chunks
            attrs["records_processed"] = progress.records_processed
            if progress.current_date_range:
                attrs["current_date_range"] = progress.current_date_range
        if progress.result_summary:
            attrs["result"] = progress.result_summary
        return attrs

    @override
    async def async_added_to_hass(self) -> None:
        """Register dispatcher listener for live progress updates."""
        self.async_on_remove(async_dispatcher_connect(self.hass, SIGNAL_BACKFILL_PROGRESS, self._on_progress))

    @callback
    def _on_progress(self) -> None:
        """Handle backfill progress update."""
        cache = cast(dict[str, object], cast(object, self.__dict__))
        _ = cache.pop("native_value", None)
        _ = cache.pop("extra_state_attributes", None)
        self.async_write_ha_state()
