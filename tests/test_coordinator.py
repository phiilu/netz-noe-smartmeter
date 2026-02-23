"""Tests for the data update coordinator."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import UpdateFailed
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.netz_noe_smartmeter.api import (
    NetzNoeApi,
    NetzNoeApiError,
    NetzNoeAuthError,
    NetzNoeRateLimitError,
)
from custom_components.netz_noe_smartmeter.const import DOMAIN
from custom_components.netz_noe_smartmeter.coordinator import NetzNoeCoordinator

from .conftest import (
    MOCK_CONFIG_DATA,
    MOCK_DAY_RESPONSE,
    MOCK_METER_ID,
    MOCK_RAW_RESPONSE,
    MOCK_WEEK_RESPONSE,
    MOCK_YEAR_RESPONSE,
)


def _make_coordinator(
    hass: HomeAssistant,
    api: NetzNoeApi,
) -> NetzNoeCoordinator:
    """Create a coordinator with a mock config entry and session."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        title="Smart Meter",
        data=MOCK_CONFIG_DATA,
        unique_id=MOCK_METER_ID,
    )
    entry.add_to_hass(hass)
    session = MagicMock()
    coord = NetzNoeCoordinator(hass, entry, api, session)
    coord._first_refresh = False
    return coord


def _mock_api(**overrides: AsyncMock) -> MagicMock:
    """Create a mock NetzNoeApi with default successful responses."""
    api = MagicMock(spec=NetzNoeApi)
    api.authenticate = AsyncMock()
    api.logout = AsyncMock()
    api.is_authenticated = True
    api.invalidate_session = MagicMock()
    api.get_day = AsyncMock(return_value=MOCK_DAY_RESPONSE)
    api.get_week = AsyncMock(return_value=MOCK_WEEK_RESPONSE)
    api.get_year = AsyncMock(return_value=MOCK_YEAR_RESPONSE)
    api.get_raw = AsyncMock(return_value=MOCK_RAW_RESPONSE)
    for key, value in overrides.items():
        setattr(api, key, value)
    return api


# -- ensure_authenticated --


async def test_ensure_authenticated_already_authed(hass: HomeAssistant) -> None:
    """Test ensure_authenticated is a no-op when already authenticated."""
    api = _mock_api()
    coord = _make_coordinator(hass, api)

    await coord.ensure_authenticated()

    api.authenticate.assert_not_awaited()


async def test_ensure_authenticated_re_login(hass: HomeAssistant) -> None:
    """Test ensure_authenticated re-logs in when session expired."""
    api = _mock_api()
    api.is_authenticated = False
    coord = _make_coordinator(hass, api)

    await coord.ensure_authenticated()

    api.authenticate.assert_awaited_once()


async def test_ensure_authenticated_raises_auth_failed(
    hass: HomeAssistant,
) -> None:
    """Test ensure_authenticated raises ConfigEntryAuthFailed on bad creds."""
    api = _mock_api()
    api.is_authenticated = False
    api.authenticate = AsyncMock(side_effect=NetzNoeAuthError("wrong password"))
    coord = _make_coordinator(hass, api)

    with pytest.raises(ConfigEntryAuthFailed):
        await coord.ensure_authenticated()


# -- _async_update_data: happy path --


async def test_update_data_success(hass: HomeAssistant) -> None:
    """Test successful data fetch populates all fields."""
    api = _mock_api()
    coord = _make_coordinator(hass, api)

    with patch(
        "custom_components.netz_noe_smartmeter.coordinator.async_import_raw_statistics",
        new_callable=AsyncMock,
    ):
        data = await coord._async_update_data()

    # Daily
    assert data.daily_values == [0.1, 0.2, 0.3]
    assert data.daily_total == pytest.approx(0.6)
    assert data.last_data_timestamp == "2025-01-15T00:45:00"

    # Weekly
    assert data.weekly_daily_values == [1.5, 2.0, None, None, None, None, None]
    assert data.weekly_total == pytest.approx(3.5)

    # Monthly / Yearly (year response: [10.0, 20.0, 30.0, None*9])
    assert data.yearly_total == pytest.approx(60.0)

    # Raw records
    assert data.raw_records == MOCK_RAW_RESPONSE


# -- _async_update_data: session expiry + retry --


async def test_update_data_session_expiry_retry_success(
    hass: HomeAssistant,
) -> None:
    """Test that session expiry triggers re-auth and successful retry."""
    api = _mock_api()
    # First call to get_day raises auth error (session expired).
    # The coordinator catches this, invalidates, re-auths, then retries
    # _fetch_all_data. On the retry, get_day should succeed.
    call_count = 0

    async def _get_day_with_expiry(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:  # only the very first call fails
            raise NetzNoeAuthError("session expired")
        return MOCK_DAY_RESPONSE

    api.get_day = AsyncMock(side_effect=_get_day_with_expiry)
    coord = _make_coordinator(hass, api)

    with patch(
        "custom_components.netz_noe_smartmeter.coordinator.async_import_raw_statistics",
        new_callable=AsyncMock,
    ):
        data = await coord._async_update_data()

    # Should have invalidated session and re-authenticated
    api.invalidate_session.assert_called_once()
    # Data should still be populated from the retry
    assert data.daily_total is not None


async def test_update_data_session_expiry_retry_fails(
    hass: HomeAssistant,
) -> None:
    """Test that persistent auth failure raises ConfigEntryAuthFailed."""
    api = _mock_api()
    api.get_day = AsyncMock(side_effect=NetzNoeAuthError("expired"))
    api.get_week = AsyncMock(side_effect=NetzNoeAuthError("expired"))
    api.get_year = AsyncMock(side_effect=NetzNoeAuthError("expired"))
    api.get_raw = AsyncMock(side_effect=NetzNoeAuthError("expired"))
    # Re-auth also fails on retry
    api.is_authenticated = False
    api.authenticate = AsyncMock(side_effect=NetzNoeAuthError("bad creds"))

    coord = _make_coordinator(hass, api)

    with pytest.raises(ConfigEntryAuthFailed):
        await coord._async_update_data()


# -- _async_update_data: partial API failures --


async def test_update_data_partial_failure(hass: HomeAssistant) -> None:
    """Test that individual endpoint failures don't block the whole update."""
    api = _mock_api()
    api.get_week = AsyncMock(side_effect=NetzNoeApiError("timeout"))
    api.get_raw = AsyncMock(side_effect=NetzNoeApiError("timeout"))

    coord = _make_coordinator(hass, api)

    with patch(
        "custom_components.netz_noe_smartmeter.coordinator.async_import_raw_statistics",
        new_callable=AsyncMock,
    ):
        data = await coord._async_update_data()

    # Day data should still be present
    assert data.daily_total is not None
    # Week data missing due to failure
    assert data.weekly_daily_values == []
    # Raw records missing
    assert data.raw_records == []


# -- _async_update_data: day data fallback --


async def test_update_data_day_fallback_to_older_date(
    hass: HomeAssistant,
) -> None:
    """Test that when yesterday has no data, it falls back to 2 days ago."""
    api = _mock_api()
    call_count = 0

    async def _get_day_fallback(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # Yesterday: all None
            return {"meteredValues": [None, None], "peakDemandTimes": [None, None]}
        # 2 days ago: has data
        return MOCK_DAY_RESPONSE

    api.get_day = AsyncMock(side_effect=_get_day_fallback)
    coord = _make_coordinator(hass, api)

    with patch(
        "custom_components.netz_noe_smartmeter.coordinator.async_import_raw_statistics",
        new_callable=AsyncMock,
    ):
        data = await coord._async_update_data()

    assert call_count == 2
    assert data.daily_total == pytest.approx(0.6)


async def test_update_data_no_day_data(hass: HomeAssistant) -> None:
    """Test that when no day has data, daily fields stay empty."""
    api = _mock_api()
    api.get_day = AsyncMock(return_value={"meteredValues": [None], "peakDemandTimes": [None]})
    coord = _make_coordinator(hass, api)

    with patch(
        "custom_components.netz_noe_smartmeter.coordinator.async_import_raw_statistics",
        new_callable=AsyncMock,
    ):
        data = await coord._async_update_data()

    assert data.daily_values == []
    assert data.daily_total is None


# -- Statistics import error handling --


async def test_update_data_statistics_import_failure_non_fatal(
    hass: HomeAssistant,
) -> None:
    """Test that a statistics import failure is logged but doesn't fail the update."""
    api = _mock_api()
    coord = _make_coordinator(hass, api)

    with patch(
        "custom_components.netz_noe_smartmeter.coordinator.async_import_raw_statistics",
        new_callable=AsyncMock,
        side_effect=RuntimeError("db error"),
    ):
        # Should not raise despite the import failure
        data = await coord._async_update_data()

    assert data.raw_records == MOCK_RAW_RESPONSE


# -- NetzNoeAuthError re-raises from individual endpoints --


async def test_update_data_week_auth_error_triggers_retry(
    hass: HomeAssistant,
) -> None:
    """Test NetzNoeAuthError from get_week triggers retry path."""
    api = _mock_api()
    call_count = 0

    async def _get_week_with_auth_error(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise NetzNoeAuthError("session expired")
        return MOCK_WEEK_RESPONSE

    api.get_week = AsyncMock(side_effect=_get_week_with_auth_error)
    coord = _make_coordinator(hass, api)

    with patch(
        "custom_components.netz_noe_smartmeter.coordinator.async_import_raw_statistics",
        new_callable=AsyncMock,
    ):
        data = await coord._async_update_data()

    # Retry succeeded — data should be populated
    api.invalidate_session.assert_called_once()
    assert data.weekly_total is not None


async def test_update_data_year_auth_error_triggers_retry(
    hass: HomeAssistant,
) -> None:
    """Test NetzNoeAuthError from get_year triggers retry path."""
    api = _mock_api()
    call_count = 0

    async def _get_year_with_auth_error(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise NetzNoeAuthError("session expired")
        return MOCK_YEAR_RESPONSE

    api.get_year = AsyncMock(side_effect=_get_year_with_auth_error)
    coord = _make_coordinator(hass, api)

    with patch(
        "custom_components.netz_noe_smartmeter.coordinator.async_import_raw_statistics",
        new_callable=AsyncMock,
    ):
        data = await coord._async_update_data()

    api.invalidate_session.assert_called_once()
    assert data.yearly_total is not None


async def test_update_data_raw_auth_error_triggers_retry(
    hass: HomeAssistant,
) -> None:
    """Test NetzNoeAuthError from get_raw triggers retry path."""
    api = _mock_api()
    call_count = 0

    async def _get_raw_with_auth_error(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise NetzNoeAuthError("session expired")
        return MOCK_RAW_RESPONSE

    api.get_raw = AsyncMock(side_effect=_get_raw_with_auth_error)
    coord = _make_coordinator(hass, api)

    with patch(
        "custom_components.netz_noe_smartmeter.coordinator.async_import_raw_statistics",
        new_callable=AsyncMock,
    ):
        data = await coord._async_update_data()

    api.invalidate_session.assert_called_once()
    assert data.raw_records == MOCK_RAW_RESPONSE


async def test_update_data_retry_auth_error_raises_config_entry_auth_failed(
    hass: HomeAssistant,
) -> None:
    """Test persistent auth error on retry raises ConfigEntryAuthFailed."""
    api = _mock_api()
    # get_week always raises auth error — triggers retry, and retry also fails
    api.get_week = AsyncMock(side_effect=NetzNoeAuthError("always expired"))
    # Re-auth succeeds (so we get past ensure_authenticated)
    api.is_authenticated = True
    coord = _make_coordinator(hass, api)

    with pytest.raises(ConfigEntryAuthFailed):
        await coord._async_update_data()


# -- _async_update_data: rate limit handling --


async def test_update_data_rate_limit_raises_update_failed(
    hass: HomeAssistant,
) -> None:
    """Test NetzNoeRateLimitError is wrapped in UpdateFailed with retry_after."""
    api = _mock_api()
    api.get_day = AsyncMock(side_effect=NetzNoeRateLimitError("429 Too Many Requests"))
    api.get_week = AsyncMock(side_effect=NetzNoeRateLimitError("429 Too Many Requests"))
    api.get_year = AsyncMock(side_effect=NetzNoeRateLimitError("429 Too Many Requests"))
    api.get_raw = AsyncMock(side_effect=NetzNoeRateLimitError("429 Too Many Requests"))
    coord = _make_coordinator(hass, api)

    with pytest.raises(UpdateFailed, match="rate limit") as exc_info:
        await coord._async_update_data()

    # Verify retry_after is set to 300 seconds
    assert exc_info.value.retry_after == 300


async def test_update_data_api_error_all_endpoints_fail(
    hass: HomeAssistant,
) -> None:
    """Test all endpoints failing with NetzNoeApiError still returns empty data."""
    api = _mock_api()
    api.get_day = AsyncMock(side_effect=NetzNoeApiError("server error"))
    api.get_week = AsyncMock(side_effect=NetzNoeApiError("server error"))
    api.get_year = AsyncMock(side_effect=NetzNoeApiError("server error"))
    api.get_raw = AsyncMock(side_effect=NetzNoeApiError("server error"))
    coord = _make_coordinator(hass, api)

    # Partial failures are caught per-endpoint, so this should NOT raise
    with patch(
        "custom_components.netz_noe_smartmeter.coordinator.async_import_raw_statistics",
        new_callable=AsyncMock,
    ):
        data = await coord._async_update_data()

    # Data should be empty defaults since all endpoints failed
    assert data.daily_total is None
    assert data.weekly_total is None
    assert data.yearly_total is None


async def test_update_data_api_error_during_auth_raises_update_failed(
    hass: HomeAssistant,
) -> None:
    """Test NetzNoeApiError during authentication raises UpdateFailed."""
    api = _mock_api()
    api.is_authenticated = False
    # authenticate() raises NetzNoeApiError (not auth-specific, e.g. network error)
    api.authenticate = AsyncMock(side_effect=NetzNoeApiError("connection reset"))
    coord = _make_coordinator(hass, api)

    with pytest.raises(UpdateFailed, match="connection reset"):
        await coord._async_update_data()


# -- First refresh: skip raw data fetch --


async def test_first_refresh_skips_raw_data(hass: HomeAssistant) -> None:
    """Test first refresh skips raw data fetch for fast setup."""
    api = _mock_api()
    entry = MockConfigEntry(
        domain=DOMAIN,
        title="Smart Meter",
        data=MOCK_CONFIG_DATA,
        unique_id=MOCK_METER_ID,
    )
    entry.add_to_hass(hass)
    session = MagicMock()
    coord = NetzNoeCoordinator(hass, entry, api, session)

    # First refresh should skip raw data
    assert coord._first_refresh is True
    with patch(
        "custom_components.netz_noe_smartmeter.coordinator.async_import_raw_statistics",
        new_callable=AsyncMock,
    ) as mock_import:
        data = await coord._async_update_data()

    api.get_raw.assert_not_called()
    mock_import.assert_not_called()
    assert coord._first_refresh is False
    assert data.daily_total is not None

    # Second refresh should include raw data
    with patch(
        "custom_components.netz_noe_smartmeter.coordinator.async_import_raw_statistics",
        new_callable=AsyncMock,
    ):
        data = await coord._async_update_data()

    api.get_raw.assert_called_once()
    assert data.raw_records == MOCK_RAW_RESPONSE
