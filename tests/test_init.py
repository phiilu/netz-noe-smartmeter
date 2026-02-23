"""Tests for integration setup, unload, and service registration."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Generator
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant, ServiceCall, ServiceResponse
from homeassistant.exceptions import HomeAssistantError, ServiceValidationError
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.netz_noe_smartmeter.const import DOMAIN

from .conftest import MOCK_CONFIG_DATA, MOCK_METER_ID

PATCH_API_CLS = "custom_components.netz_noe_smartmeter.NetzNoeApi"
PATCH_SESSION_CLS = "custom_components.netz_noe_smartmeter.aiohttp.ClientSession"
PATCH_COOKIE_JAR = "custom_components.netz_noe_smartmeter.aiohttp.CookieJar"
PATCH_BACKFILL = "custom_components.netz_noe_smartmeter.async_backfill_statistics"


def _mock_entry(meter_id: str = MOCK_METER_ID) -> MockConfigEntry:
    """Return a MockConfigEntry for the integration."""
    data = {**MOCK_CONFIG_DATA, "meter_id": meter_id}
    return MockConfigEntry(
        domain=DOMAIN,
        title="Smart Meter",
        data=data,
        unique_id=meter_id,
    )


@pytest.fixture
def _patch_aiohttp() -> Generator[AsyncMock]:
    """Patch aiohttp session creation used in async_setup_entry."""
    mock_session = AsyncMock()
    mock_session.close = AsyncMock()
    with (
        patch(PATCH_COOKIE_JAR),
        patch(PATCH_SESSION_CLS, return_value=mock_session) as mock_cls,
    ):
        mock_cls.return_value = mock_session
        yield mock_session


@pytest.fixture
def _patch_api() -> Generator[MagicMock]:
    """Patch the NetzNoeApi to avoid real network calls."""
    with patch(PATCH_API_CLS) as mock_cls:
        api = mock_cls.return_value
        api.authenticate = AsyncMock()
        api.logout = AsyncMock()
        api.get_day = AsyncMock(return_value={"meteredValues": [0.1], "peakDemandTimes": ["2025-01-15T00:15:00"]})
        api.get_week = AsyncMock(return_value={"meteredValues": [1.0]})
        api.get_year = AsyncMock(return_value={"values": [10.0] + [None] * 11})
        api.get_raw = AsyncMock(return_value=[])
        api.is_authenticated = True
        api.invalidate_session = MagicMock()
        yield api


# -- async_setup: service registration --


async def test_async_setup_registers_service(hass: HomeAssistant) -> None:
    """Test that async_setup registers the backfill_statistics service."""
    from custom_components.netz_noe_smartmeter import async_setup

    result = await async_setup(hass, {})

    assert result is True
    assert hass.services.has_service(DOMAIN, "backfill_statistics")


async def test_backfill_service_handler_rejects_non_integer_start_year(hass: HomeAssistant) -> None:
    """Test handler-level guard rejects non-integer start_year values."""
    from custom_components.netz_noe_smartmeter import async_setup

    captured: dict[str, object] = {}

    def _capture_register(
        _domain: str,
        _service: str,
        service_func: Callable[[ServiceCall], Awaitable[ServiceResponse]],
        **_kwargs: object,
    ) -> None:
        captured["handler"] = service_func

    with patch("homeassistant.core.ServiceRegistry.async_register", side_effect=_capture_register):
        result = await async_setup(hass, {})

    assert result is True
    handler = cast(Callable[[ServiceCall], Awaitable[ServiceResponse]], captured["handler"])

    with pytest.raises(ServiceValidationError, match="start_year must be an integer"):
        await handler(
            ServiceCall(
                hass,
                DOMAIN,
                "backfill_statistics",
                {"start_year": "not-an-int"},
                return_response=True,
            )
        )


async def test_backfill_service_handler_rejects_non_string_config_entry_id(hass: HomeAssistant) -> None:
    """Test handler-level guard rejects non-string config_entry_id values."""
    from custom_components.netz_noe_smartmeter import async_setup

    captured: dict[str, object] = {}

    def _capture_register(
        _domain: str,
        _service: str,
        service_func: Callable[[ServiceCall], Awaitable[ServiceResponse]],
        **_kwargs: object,
    ) -> None:
        captured["handler"] = service_func

    with patch("homeassistant.core.ServiceRegistry.async_register", side_effect=_capture_register):
        result = await async_setup(hass, {})

    assert result is True
    handler = cast(Callable[[ServiceCall], Awaitable[ServiceResponse]], captured["handler"])
    loaded_entry = SimpleNamespace(state=ConfigEntryState.LOADED, entry_id="entry-1")

    with (
        patch.object(hass.config_entries, "async_entries", return_value=[loaded_entry]),
        pytest.raises(ServiceValidationError, match="config_entry_id must be a string"),
    ):
        await handler(
            ServiceCall(
                hass,
                DOMAIN,
                "backfill_statistics",
                {"config_entry_id": 123},
                return_response=True,
            )
        )


# -- async_setup_entry / async_unload_entry --


async def test_setup_and_unload_entry(
    hass: HomeAssistant,
    _patch_aiohttp: AsyncMock,
    _patch_api: AsyncMock,
) -> None:
    """Test setting up and unloading a config entry."""
    entry = _mock_entry()
    entry.add_to_hass(hass)

    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.LOADED
    assert entry.runtime_data is not None

    # Unload
    await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.NOT_LOADED
    # Session should be closed
    _patch_aiohttp.close.assert_awaited_once()


async def test_setup_entry_auth_failure(
    hass: HomeAssistant,
    _patch_aiohttp: AsyncMock,
    _patch_api: AsyncMock,
) -> None:
    """Test that setup fails when initial auth raises."""
    from custom_components.netz_noe_smartmeter.api import NetzNoeAuthError

    _patch_api.is_authenticated = False
    _patch_api.authenticate = AsyncMock(side_effect=NetzNoeAuthError("bad creds"))

    entry = _mock_entry()
    entry.add_to_hass(hass)

    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.SETUP_ERROR


async def test_setup_entry_api_partial_failure(
    hass: HomeAssistant,
    _patch_aiohttp: AsyncMock,
    _patch_api: AsyncMock,
) -> None:
    """Test that setup succeeds even when individual API calls fail.

    The coordinator catches NetzNoeApiError per-endpoint and logs warnings
    rather than raising UpdateFailed, so the entry still loads with empty data.
    """
    from custom_components.netz_noe_smartmeter.api import NetzNoeApiError

    _patch_api.get_day = AsyncMock(side_effect=NetzNoeApiError("timeout"))
    _patch_api.get_week = AsyncMock(side_effect=NetzNoeApiError("timeout"))
    _patch_api.get_year = AsyncMock(side_effect=NetzNoeApiError("timeout"))
    _patch_api.get_raw = AsyncMock(side_effect=NetzNoeApiError("timeout"))

    entry = _mock_entry()
    entry.add_to_hass(hass)

    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    # Individual API failures are non-fatal; entry still loads with empty data
    assert entry.state is ConfigEntryState.LOADED


# -- backfill_statistics service --


async def test_backfill_service_no_entries(hass: HomeAssistant) -> None:
    """Test backfill service raises when no integration is configured."""
    from custom_components.netz_noe_smartmeter import async_setup

    await async_setup(hass, {})

    with pytest.raises(ServiceValidationError, match="No loaded Netz NÖ"):
        await hass.services.async_call(
            DOMAIN,
            "backfill_statistics",
            {},
            blocking=True,
            return_response=True,
        )


async def test_backfill_service_success(
    hass: HomeAssistant,
    _patch_aiohttp: AsyncMock,
    _patch_api: AsyncMock,
) -> None:
    """Test backfill service calls async_backfill_statistics successfully."""
    entry = _mock_entry()
    entry.add_to_hass(hass)

    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    with patch(PATCH_BACKFILL, new_callable=AsyncMock) as mock_backfill:
        mock_backfill.return_value = {"imported_count": 42}

        result = await hass.services.async_call(
            DOMAIN,
            "backfill_statistics",
            {"start_year": 2023},
            blocking=True,
            return_response=True,
        )

    assert result == {"imported_count": 42}
    mock_backfill.assert_awaited_once()
    # Verify start_year was passed through
    call_kwargs = mock_backfill.call_args
    assert call_kwargs[1]["start_year"] == 2023


async def test_backfill_service_progress_callback_updates_coordinator_status(
    hass: HomeAssistant,
    _patch_aiohttp: AsyncMock,
    _patch_api: AsyncMock,
) -> None:
    """Test backfill progress callback updates coordinator backfill state."""
    entry = _mock_entry()
    entry.add_to_hass(hass)

    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    async def _backfill_with_progress(*args: object, **kwargs: object) -> dict[str, float | int]:
        progress_callback = cast(Callable[[str, int, int, int, str], None], kwargs["progress_callback"])
        progress_callback("running", 2, 4, 123, "2024-01-01 to 2024-01-31")
        return {"total_entries": 12, "total_kwh": 3.5}

    with patch(PATCH_BACKFILL, new_callable=AsyncMock) as mock_backfill:
        mock_backfill.side_effect = _backfill_with_progress

        result = await hass.services.async_call(
            DOMAIN,
            "backfill_statistics",
            {},
            blocking=True,
            return_response=True,
        )

    assert result == {"total_entries": 12, "total_kwh": 3.5}

    coordinator = entry.runtime_data
    assert coordinator.backfill_progress.state == "completed"
    assert coordinator.backfill_progress.current_chunk == 2
    assert coordinator.backfill_progress.total_chunks == 4
    assert coordinator.backfill_progress.records_processed == 123
    assert coordinator.backfill_progress.current_date_range == "2024-01-01 to 2024-01-31"
    assert coordinator.backfill_progress.result_summary == "12 entries, 3.5 kWh"


async def test_backfill_service_multiple_entries_require_config_entry_id(
    hass: HomeAssistant,
    _patch_aiohttp: AsyncMock,
    _patch_api: AsyncMock,
) -> None:
    """Test service requires config_entry_id when multiple entries are loaded."""
    entry1 = _mock_entry("AT0020000000000000000000100307150")
    entry2 = _mock_entry("AT0020000000000000000000100307151")
    entry1.add_to_hass(hass)
    entry2.add_to_hass(hass)

    await hass.config_entries.async_setup(entry1.entry_id)
    await hass.async_block_till_done()

    with patch(PATCH_BACKFILL, new_callable=AsyncMock) as mock_backfill:
        mock_backfill.return_value = {"imported_count": 1}

        with pytest.raises(ServiceValidationError, match="Multiple meters loaded"):
            await hass.services.async_call(
                DOMAIN,
                "backfill_statistics",
                {},
                blocking=True,
                return_response=True,
            )

        result = await hass.services.async_call(
            DOMAIN,
            "backfill_statistics",
            {"config_entry_id": entry2.entry_id},
            blocking=True,
            return_response=True,
        )

    assert result == {"imported_count": 1}
    mock_backfill.assert_awaited_once()
    assert mock_backfill.call_args.args[2] == "AT0020000000000000000000100307151"


async def test_backfill_service_invalid_config_entry_id(
    hass: HomeAssistant,
    _patch_aiohttp: AsyncMock,
    _patch_api: AsyncMock,
) -> None:
    """Test service raises when config_entry_id does not match loaded entries."""
    entry = _mock_entry()
    entry.add_to_hass(hass)

    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    with pytest.raises(ServiceValidationError, match="config_entry_id is not loaded"):
        await hass.services.async_call(
            DOMAIN,
            "backfill_statistics",
            {"config_entry_id": "not-a-real-entry"},
            blocking=True,
            return_response=True,
        )


async def test_backfill_service_error(
    hass: HomeAssistant,
    _patch_aiohttp: AsyncMock,
    _patch_api: AsyncMock,
) -> None:
    """Test backfill service raises HomeAssistantError when backfill returns error."""
    entry = _mock_entry()
    entry.add_to_hass(hass)

    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    with (
        patch(PATCH_BACKFILL, new_callable=AsyncMock) as mock_backfill,
        pytest.raises(HomeAssistantError, match="Something went wrong"),
    ):
        mock_backfill.return_value = {"error": "Something went wrong"}

        await hass.services.async_call(
            DOMAIN,
            "backfill_statistics",
            {},
            blocking=True,
            return_response=True,
        )


async def test_backfill_service_unexpected_exception_sets_summary(
    hass: HomeAssistant,
    _patch_aiohttp: AsyncMock,
    _patch_api: AsyncMock,
) -> None:
    """Test unexpected backfill exceptions update diagnostics summary."""
    entry = _mock_entry()
    entry.add_to_hass(hass)

    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    with (
        patch(PATCH_BACKFILL, new_callable=AsyncMock, side_effect=RuntimeError("boom")),
        pytest.raises(RuntimeError, match="boom"),
    ):
        await hass.services.async_call(
            DOMAIN,
            "backfill_statistics",
            {},
            blocking=True,
            return_response=True,
        )

    coordinator = entry.runtime_data
    assert coordinator.backfill_progress.state == "failed"
    assert coordinator.backfill_progress.result_summary == "Unexpected error during backfill"
