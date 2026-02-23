"""Diagnostics support for Netz NÖ Smart Meter."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict
from typing import cast

from homeassistant.components import diagnostics
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant

from . import NetzNoeConfigEntry

TO_REDACT = {CONF_PASSWORD, CONF_USERNAME}

_REDACT_DATA = cast(
    Callable[[Mapping[str, object], Iterable[str]], dict[str, object]],
    diagnostics.async_redact_data,
)


async def async_get_config_entry_diagnostics(
    _hass: HomeAssistant,
    entry: NetzNoeConfigEntry,
) -> dict[str, object]:
    """Return diagnostics for a config entry."""
    coordinator = entry.runtime_data

    return {
        "entry_data": _REDACT_DATA(dict(entry.data), TO_REDACT),
        "coordinator_data": asdict(coordinator.data) if coordinator.data else None,
    }
