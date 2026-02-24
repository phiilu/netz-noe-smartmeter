"""The Netz NÖ Smart Meter integration."""

from __future__ import annotations

import logging
from typing import cast

import aiohttp
import voluptuous as vol
from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.const import Platform
from homeassistant.core import (
    HomeAssistant,
    ServiceCall,
    ServiceResponse,
    SupportsResponse,
)
from homeassistant.exceptions import HomeAssistantError, ServiceValidationError
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.typing import ConfigType

from .api import NetzNoeApi
from .const import DOMAIN
from .coordinator import NetzNoeCoordinator
from .sensor import SIGNAL_BACKFILL_PROGRESS
from .statistics import async_backfill_statistics

_LOGGER = logging.getLogger(__name__)

PLATFORMS = [Platform.BUTTON, Platform.SENSOR]
CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)

SERVICE_BACKFILL = "backfill_statistics"
ATTR_CONFIG_ENTRY_ID = "config_entry_id"

# 5-minute timeout for API requests (the Netz NÖ API can be slow)
API_REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=300)

type NetzNoeConfigEntry = ConfigEntry[NetzNoeCoordinator]


async def async_setup(hass: HomeAssistant, _config: ConfigType) -> bool:
    """Set up the Netz NÖ Smart Meter domain (register services)."""

    async def handle_backfill(call: ServiceCall) -> ServiceResponse:
        """Handle the backfill_statistics service call."""
        call_data = cast(dict[str, object], call.data)

        start_year_raw = call_data.get("start_year")
        if start_year_raw is None:
            start_year = None
        elif isinstance(start_year_raw, int):
            start_year = start_year_raw
        else:
            raise ServiceValidationError("start_year must be an integer")

        loaded_entries: list[NetzNoeConfigEntry] = [
            cast(NetzNoeConfigEntry, entry)
            for entry in hass.config_entries.async_entries(DOMAIN)
            if entry.state is ConfigEntryState.LOADED
        ]
        if not loaded_entries:
            raise ServiceValidationError("No loaded Netz NÖ Smart Meter integration found")

        config_entry_id_raw = call_data.get(ATTR_CONFIG_ENTRY_ID)
        if config_entry_id_raw is None:
            config_entry_id = None
        elif isinstance(config_entry_id_raw, str):
            config_entry_id = config_entry_id_raw
        else:
            raise ServiceValidationError("config_entry_id must be a string")

        if config_entry_id is not None:
            entry = next((e for e in loaded_entries if e.entry_id == config_entry_id), None)
            if entry is None:
                raise ServiceValidationError("Specified config_entry_id is not loaded")
        elif len(loaded_entries) > 1:
            raise ServiceValidationError("Multiple meters loaded; specify config_entry_id")
        else:
            entry = loaded_entries[0]

        coord: NetzNoeCoordinator = entry.runtime_data
        await coord.ensure_authenticated()

        def on_progress(state: str, chunk: int, total: int, records: int, date_range: str) -> None:
            """Update backfill progress on coordinator and notify sensor."""
            coord.backfill_progress.state = state
            coord.backfill_progress.current_chunk = chunk
            coord.backfill_progress.total_chunks = total
            coord.backfill_progress.records_processed = records
            coord.backfill_progress.current_date_range = date_range if date_range else None
            async_dispatcher_send(hass, SIGNAL_BACKFILL_PROGRESS)

        coord.backfill_progress.state = "running"
        coord.backfill_progress.current_chunk = 0
        coord.backfill_progress.total_chunks = None
        coord.backfill_progress.records_processed = 0
        coord.backfill_progress.current_date_range = None
        coord.backfill_progress.result_summary = None
        async_dispatcher_send(hass, SIGNAL_BACKFILL_PROGRESS)

        try:
            result = await async_backfill_statistics(
                hass,
                coord.api,
                coord.meter_id,
                start_year=start_year,
                progress_callback=on_progress,
            )
        except Exception:
            coord.backfill_progress.state = "failed"
            coord.backfill_progress.result_summary = "Unexpected error during backfill"
            async_dispatcher_send(hass, SIGNAL_BACKFILL_PROGRESS)
            raise

        if "error" in result:
            coord.backfill_progress.state = "failed"
            coord.backfill_progress.result_summary = result["error"]
            async_dispatcher_send(hass, SIGNAL_BACKFILL_PROGRESS)
            raise HomeAssistantError(result["error"])

        total_kwh = result.get("total_kwh", 0)
        total_entries = result.get("total_entries", 0)
        coord.backfill_progress.state = "completed"
        coord.backfill_progress.result_summary = f"{total_entries} entries, {total_kwh} kWh"
        async_dispatcher_send(hass, SIGNAL_BACKFILL_PROGRESS)

        return cast(ServiceResponse, cast(object, result))

    hass.services.async_register(
        DOMAIN,
        SERVICE_BACKFILL,
        handle_backfill,
        schema=vol.Schema(
            {
                vol.Optional(ATTR_CONFIG_ENTRY_ID): cv.string,
                vol.Optional("start_year"): vol.Coerce(int),
            }
        ),
        supports_response=SupportsResponse.OPTIONAL,
    )

    return True


async def async_setup_entry(hass: HomeAssistant, entry: NetzNoeConfigEntry) -> bool:
    """Set up Netz NÖ Smart Meter from a config entry."""
    # Create a persistent cookie jar + session for this entry
    jar = aiohttp.CookieJar(unsafe=True)
    session = aiohttp.ClientSession(cookie_jar=jar, timeout=API_REQUEST_TIMEOUT)
    api = NetzNoeApi(session)

    coordinator = NetzNoeCoordinator(hass, entry, api, session)

    try:
        await coordinator.async_config_entry_first_refresh()
    except Exception:
        await session.close()
        raise

    # Store coordinator as runtime_data
    entry.runtime_data = coordinator

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # Schedule a full refresh (including raw data + statistics) now that setup is done
    _ = hass.async_create_task(coordinator.async_request_refresh())

    return True


async def async_unload_entry(hass: HomeAssistant, entry: NetzNoeConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)

    if unload_ok:
        coordinator: NetzNoeCoordinator = entry.runtime_data
        await coordinator.session.close()

    return unload_ok
