"""Tests for the data update coordinator."""

from __future__ import annotations

from collections.abc import Generator
from datetime import date, timedelta
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from freezegun.api import FrozenDateTimeFactory
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import UpdateFailed

from custom_components.netz_noe_smartmeter.api import (
    NetzNoeApi,
    NetzNoeApiError,
    NetzNoeAuthError,
    NetzNoeLoginError,
    NetzNoeMaintenanceError,
    NetzNoeRateLimitError,
)
from custom_components.netz_noe_smartmeter.const import DOMAIN
from custom_components.netz_noe_smartmeter.coordinator import EnergyCommunitySplit, NetzNoeCoordinator, NetzNoeData
from custom_components.netz_noe_smartmeter.statistics import EnergyCommunityDay

from .common import MockConfigEntry
from .conftest import (
    MOCK_CONFIG_DATA,
    MOCK_DAY_RESPONSE,
    MOCK_EEG_DAY_RESPONSE,
    MOCK_EEG_RESPONSES,
    MOCK_EEG_WEEK_RESPONSE,
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
    api.get_day = AsyncMock(return_value=MOCK_DAY_RESPONSE)
    api.get_week = AsyncMock(return_value=MOCK_WEEK_RESPONSE)
    api.get_year = AsyncMock(return_value=MOCK_YEAR_RESPONSE)
    api.get_raw = AsyncMock(return_value=MOCK_RAW_RESPONSE)
    for key, value in overrides.items():
        setattr(api, key, value)
    return api


# -- _async_update_data: happy path --


async def test_update_data_success(hass: HomeAssistant) -> None:
    """Test successful data fetch populates all fields."""
    api = _mock_api()
    coord = _make_coordinator(hass, api)

    with patch(
        "custom_components.netz_noe_smartmeter.coordinator.async_import_raw_statistics",
        new_callable=AsyncMock,
    ) as mock_import:
        data = await coord._async_update_data()

    # Daily
    assert data.daily_values == [0.1, 0.2, 0.3]
    assert data.daily_timestamps == MOCK_DAY_RESPONSE["peakDemandTimes"]
    assert data.daily_total == pytest.approx(0.6)
    assert data.daily_date == api.get_day.call_args.args[1]
    assert data.last_data_timestamp == "2025-01-15T00:45:00"

    # Weekly
    assert data.weekly_daily_values == [1.5, 2.0, None, None, None, None, None]
    assert data.weekly_total == pytest.approx(3.5)

    # Monthly / Yearly (year response: [10.0, 20.0, 30.0, None*9])
    assert data.monthly_values == MOCK_YEAR_RESPONSE["values"]
    assert data.yearly_total == pytest.approx(60.0)

    # Raw records are imported as statistics
    assert data.raw_records == MOCK_RAW_RESPONSE
    mock_import.assert_awaited_once_with(hass, MOCK_METER_ID, MOCK_RAW_RESPONSE)


async def test_update_data_without_energy_community(hass: HomeAssistant) -> None:
    """Test a meter outside an energy community has no split for any period and costs no extra request."""
    api = _mock_api()
    coord = _make_coordinator(hass, api)

    with patch(
        "custom_components.netz_noe_smartmeter.coordinator.async_import_raw_statistics",
        new_callable=AsyncMock,
    ):
        data = await coord._async_update_data()

    assert not data.has_energy_community
    # No request for the day before: the meter has no split to fall back to
    api.get_day.assert_awaited_once()


async def test_update_data_null_fields(hass: HomeAssistant) -> None:
    """Test fields the portal sends as null count as empty instead of failing the poll."""
    nulls = {"meteredValues": None, "values": None, "selfCoverageValues": None, "gridUsageLeftoverValues": None}
    api = _mock_api(
        get_day=AsyncMock(
            return_value={**MOCK_DAY_RESPONSE, "selfCoverageValues": None, "gridUsageLeftoverValues": None}
        ),
        get_week=AsyncMock(return_value=nulls),
        get_year=AsyncMock(return_value=nulls),
    )
    coord = _make_coordinator(hass, api)

    with patch(
        "custom_components.netz_noe_smartmeter.coordinator.async_import_raw_statistics",
        new_callable=AsyncMock,
    ):
        data = await coord._async_update_data()

    assert data.daily_total == pytest.approx(0.6)
    assert data.weekly_total is None
    assert data.yearly_total is None
    assert not data.has_energy_community


async def test_update_data_monthly_split_needs_month_total(hass: HomeAssistant, freezer: FrozenDateTimeFactory) -> None:
    """Test the monthly split is only reported together with its period start."""
    freezer.move_to("2026-11-15 12:00:00-08:00")
    year = {**MOCK_EEG_RESPONSES["get_year"], "values": [None] * 12}
    api = _mock_api(**{method: AsyncMock(return_value=response) for method, response in MOCK_EEG_RESPONSES.items()})
    api.get_year = AsyncMock(return_value=year)
    coord = _make_coordinator(hass, api)

    with patch(
        "custom_components.netz_noe_smartmeter.coordinator.async_import_raw_statistics",
        new_callable=AsyncMock,
    ):
        data = await coord._async_update_data()

    assert data.month_start is None
    assert data.monthly_split is None
    assert data.yearly_split == EnergyCommunitySplit(82.585, 1123.809, date(2026, 1, 1))


async def test_update_data_energy_community_split(hass: HomeAssistant, freezer: FrozenDateTimeFactory) -> None:
    """Test the community share and the remaining grid consumption are summed per period."""
    freezer.move_to("2026-11-15 12:00:00-08:00")  # a Sunday, the newest day is Saturday 14 Nov
    api = _mock_api(**{method: AsyncMock(return_value=response) for method, response in MOCK_EEG_RESPONSES.items()})
    coord = _make_coordinator(hass, api)

    with patch(
        "custom_components.netz_noe_smartmeter.coordinator.async_import_raw_statistics",
        new_callable=AsyncMock,
    ):
        data = await coord._async_update_data()

    assert data.has_energy_community
    assert data.daily_total == pytest.approx(33.451)
    assert data.daily_split == EnergyCommunitySplit(9.673, 23.778, date(2026, 11, 14))
    assert data.weekly_split == EnergyCommunitySplit(16.454, 200.374, date(2026, 11, 9))
    assert data.monthly_split == EnergyCommunitySplit(32.585, 473.809, date(2026, 11, 1))
    assert data.yearly_split == EnergyCommunitySplit(82.585, 1123.809, date(2026, 1, 1))


def _eeg_api(day_responses: dict[date, object]) -> MagicMock:
    """Return an API answering like the portal for an energy community member, with the given Day responses."""

    async def _get_day(_meter_id: str, day: date, *, record_shape: bool = True) -> object:
        response = day_responses[day]
        if isinstance(response, Exception):
            raise response
        return response

    api = _mock_api(**{method: AsyncMock(return_value=response) for method, response in MOCK_EEG_RESPONSES.items()})
    api.get_day = AsyncMock(side_effect=_get_day)
    return api


_DAY_WITHOUT_SPLIT = {**MOCK_EEG_DAY_RESPONSE, "selfCoverageValues": [None] * 4, "gridUsageLeftoverValues": [None] * 4}
_EC_DAY_13 = EnergyCommunityDay(date(2026, 11, 13), [0.0, 9.673, 0.0, None], [10.0, 3.778, 10.0, None])
_SPLIT_13 = EnergyCommunitySplit(9.673, 23.778, date(2026, 11, 13))
_NO_SPLIT = {"selfCoverageValues": [None] * 12, "gridUsageLeftoverValues": [None] * 12}


@pytest.fixture
def mock_ec_import() -> Generator[AsyncMock]:
    """Patch both statistics imports in a Sunday poll (15 Nov 2026) and yield the energy community import."""
    with (
        patch("custom_components.netz_noe_smartmeter.coordinator.async_import_raw_statistics", new_callable=AsyncMock),
        patch(
            "custom_components.netz_noe_smartmeter.coordinator.async_import_energy_community_statistics",
            new_callable=AsyncMock,
        ) as mock_import,
    ):
        yield mock_import


@pytest.fixture
def sunday(freezer: FrozenDateTimeFactory) -> None:
    """Freeze time on Sunday 15 Nov 2026, so the newest day is Saturday 14 Nov."""
    freezer.move_to("2026-11-15 12:00:00-08:00")


@pytest.mark.usefixtures("sunday")
async def test_update_data_daily_split_falls_back_to_day_before(hass: HomeAssistant, mock_ec_import: AsyncMock) -> None:
    """Test the daily split comes from the day before while the portal has none for the newest day yet."""
    api = _eeg_api({date(2026, 11, 14): _DAY_WITHOUT_SPLIT, date(2026, 11, 13): MOCK_EEG_DAY_RESPONSE})
    coord = _make_coordinator(hass, api)

    data = await coord._async_update_data()

    assert data.daily_date == date(2026, 11, 14)
    assert data.daily_total == pytest.approx(33.451)
    assert data.daily_split == _SPLIT_13
    # Shapes stay those of the newest day, which show the missing split
    api.get_day.assert_awaited_with(MOCK_METER_ID, date(2026, 11, 13), record_shape=False)
    mock_ec_import.assert_awaited_once_with(hass, MOCK_METER_ID, _EC_DAY_13, coord._fetch_energy_community_day)


@pytest.mark.usefixtures("sunday")
async def test_update_data_daily_split_fallback_reuses_previous_poll(
    hass: HomeAssistant, mock_ec_import: AsyncMock
) -> None:
    """Test later polls reuse the day before instead of fetching it again while the newest day has no split."""
    api = _eeg_api({date(2026, 11, 14): _DAY_WITHOUT_SPLIT, date(2026, 11, 13): MOCK_EEG_DAY_RESPONSE})
    coord = _make_coordinator(hass, api)

    coord.data = await coord._async_update_data()
    api.get_day.reset_mock()
    data = await coord._async_update_data()

    assert data.daily_split == _SPLIT_13
    api.get_day.assert_awaited_once_with(MOCK_METER_ID, date(2026, 11, 14))


async def test_update_data_daily_split_fallback_from_previous_poll(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory, mock_ec_import: AsyncMock
) -> None:
    """Test a newest day that starts a week and a month falls back too, as the previous poll had a daily split."""
    freezer.move_to("2026-06-02 12:00:00-07:00")  # the newest day is Monday 1 June
    api = _eeg_api({date(2026, 6, 1): _DAY_WITHOUT_SPLIT, date(2026, 5, 31): MOCK_EEG_DAY_RESPONSE})
    api.get_week = AsyncMock(return_value=MOCK_WEEK_RESPONSE)
    api.get_year = AsyncMock(return_value=MOCK_YEAR_RESPONSE)
    coord = _make_coordinator(hass, api)

    assert (await coord._async_update_data()).daily_split is None
    coord.data = NetzNoeData(daily_split=_SPLIT_13)
    data = await coord._async_update_data()

    assert data.daily_split == EnergyCommunitySplit(9.673, 23.778, date(2026, 5, 31))


@pytest.mark.parametrize(
    ("previous_week", "weekly_split"),
    [
        (MOCK_EEG_WEEK_RESPONSE, EnergyCommunitySplit(16.454, 200.374, date(2026, 11, 9))),
        (NetzNoeApiError("API error 500"), None),
        (NetzNoeRateLimitError("API rate limit exceeded"), UpdateFailed),
    ],
    ids=["previous_week", "request_failed", "poll_fatal_error"],
)
async def test_update_data_split_keeps_completed_week(
    hass: HomeAssistant,
    freezer: FrozenDateTimeFactory,
    mock_ec_import: AsyncMock,
    previous_week: object,
    weekly_split: EnergyCommunitySplit | type[Exception] | None,
) -> None:
    """Test the weekly split stays on the completed week, Sunday included, while Monday has no split yet."""
    freezer.move_to("2026-11-17 12:00:00-08:00")  # the newest day is Monday 16 Nov
    api = _eeg_api({date(2026, 11, 16): _DAY_WITHOUT_SPLIT, date(2026, 11, 15): MOCK_EEG_DAY_RESPONSE})

    async def _get_week(_meter_id: str, start: date, _end: date, *, record_shape: bool = True) -> object:
        if start == date(2026, 11, 16):
            return {**MOCK_EEG_WEEK_RESPONSE, **_NO_SPLIT}
        if isinstance(previous_week, Exception):
            raise previous_week
        return previous_week

    api.get_week = AsyncMock(side_effect=_get_week)
    coord = _make_coordinator(hass, api)

    if isinstance(weekly_split, type):
        with pytest.raises(weekly_split):
            await coord._async_update_data()
        return
    data = await coord._async_update_data()

    assert data.week_start == date(2026, 11, 16)
    assert data.daily_split == EnergyCommunitySplit(9.673, 23.778, date(2026, 11, 15))
    assert data.weekly_split == weekly_split
    assert data.monthly_split == EnergyCommunitySplit(32.585, 473.809, date(2026, 11, 1))
    api.get_week.assert_awaited_with(MOCK_METER_ID, date(2026, 11, 9), date(2026, 11, 16), record_shape=False)


async def test_update_data_split_keeps_completed_month_and_year(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory, mock_ec_import: AsyncMock
) -> None:
    """Test the monthly and yearly splits stay on December while 1 January has no split yet."""
    freezer.move_to("2027-01-02 12:00:00-08:00")  # the newest day is Friday 1 Jan, in the week of 31 Dec
    api = _eeg_api({date(2027, 1, 1): _DAY_WITHOUT_SPLIT, date(2026, 12, 31): MOCK_EEG_DAY_RESPONSE})
    years = {
        2027: {"values": [33.451] + [None] * 11, **_NO_SPLIT},
        2026: {
            "values": [100.0] * 12,
            "selfCoverageValues": [None] * 11 + [50.0],
            "gridUsageLeftoverValues": [None] * 11 + [650.0],
        },
    }

    async def _get_year(_meter_id: str, year: int, *, record_shape: bool = True) -> object:
        return years[year]

    api.get_year = AsyncMock(side_effect=_get_year)
    coord = _make_coordinator(hass, api)

    data = await coord._async_update_data()

    assert (data.month_start, data.year_start) == (date(2027, 1, 1), date(2027, 1, 1))
    assert data.monthly_split == EnergyCommunitySplit(50.0, 650.0, date(2026, 12, 1))
    assert data.yearly_split == EnergyCommunitySplit(50.0, 650.0, date(2026, 1, 1))
    # The day before is in the same week, so no request for another week
    api.get_week.assert_awaited_once()


@pytest.mark.usefixtures("sunday")
async def test_update_data_daily_split_unknown_without_split_for_day_before(
    hass: HomeAssistant, mock_ec_import: AsyncMock
) -> None:
    """Test the daily split stays unknown if the day before has no split either."""
    api = _eeg_api({date(2026, 11, 14): _DAY_WITHOUT_SPLIT, date(2026, 11, 13): _DAY_WITHOUT_SPLIT})
    coord = _make_coordinator(hass, api)

    data = await coord._async_update_data()

    assert data.daily_split is None
    assert data.weekly_split is not None
    mock_ec_import.assert_not_awaited()


@pytest.mark.usefixtures("sunday")
async def test_update_data_daily_split_fallback_error_keeps_poll(
    hass: HomeAssistant, mock_ec_import: AsyncMock, caplog: pytest.LogCaptureFixture
) -> None:
    """Test a failed request for the day before only leaves the daily split unknown."""
    api = _eeg_api({date(2026, 11, 14): _DAY_WITHOUT_SPLIT, date(2026, 11, 13): NetzNoeApiError("API error 500")})
    coord = _make_coordinator(hass, api)

    data = await coord._async_update_data()

    assert data.daily_total == pytest.approx(33.451)
    assert data.daily_split is None
    assert "Failed to fetch energy community data for 2026-11-13" in caplog.text


@pytest.mark.usefixtures("sunday")
async def test_update_data_energy_community_fetch_auth_error(hass: HomeAssistant, mock_ec_import: AsyncMock) -> None:
    """Test rejected credentials in an energy community request start the reauth, like any other request."""
    api = _eeg_api({date(2026, 11, 14): _DAY_WITHOUT_SPLIT, date(2026, 11, 13): NetzNoeAuthError("Invalid")})
    coord = _make_coordinator(hass, api)

    with pytest.raises(ConfigEntryAuthFailed):
        await coord._async_update_data()


@pytest.mark.usefixtures("sunday")
@pytest.mark.parametrize(
    ("side_effect", "raises"),
    [(RuntimeError("db error"), None), (NetzNoeAuthError("Invalid"), ConfigEntryAuthFailed)],
    ids=["import_error_logged", "fetch_auth_error_raised"],
)
async def test_update_data_energy_community_import(
    hass: HomeAssistant, mock_ec_import: AsyncMock, side_effect: Exception, raises: type[Exception] | None
) -> None:
    """Test the newest day's split is imported on a full refresh: import errors are logged, poll-fatal ones raised."""
    api = _eeg_api({date(2026, 11, 14): MOCK_EEG_DAY_RESPONSE})
    coord = _make_coordinator(hass, api)
    mock_ec_import.side_effect = side_effect

    if raises is None:
        data = await coord._async_update_data()
        assert data.daily_split == EnergyCommunitySplit(9.673, 23.778, date(2026, 11, 14))
    else:
        with pytest.raises(raises):
            await coord._async_update_data()

    newest = cast(EnergyCommunityDay, mock_ec_import.await_args_list[0].args[2])
    assert newest.day == date(2026, 11, 14)
    api.get_day.assert_awaited_once()


@pytest.mark.usefixtures("sunday")
async def test_first_refresh_skips_energy_community_requests(hass: HomeAssistant, mock_ec_import: AsyncMock) -> None:
    """Test setup makes no request for the day before and imports nothing; the full refresh after it does."""
    api = _eeg_api({date(2026, 11, 14): _DAY_WITHOUT_SPLIT, date(2026, 11, 13): MOCK_EEG_DAY_RESPONSE})
    coord = _make_coordinator(hass, api)
    coord._first_refresh = True

    data = await coord._async_update_data()

    assert data.daily_split is None
    api.get_day.assert_awaited_once()
    mock_ec_import.assert_not_awaited()


async def test_fetch_energy_community_day(hass: HomeAssistant) -> None:
    """Test the import's fetch function returns the day's split, None without one, and asks for no response shape."""
    api = _eeg_api({date(2026, 11, 13): MOCK_EEG_DAY_RESPONSE, date(2026, 11, 14): _DAY_WITHOUT_SPLIT})
    coord = _make_coordinator(hass, api)

    assert await coord._fetch_energy_community_day(date(2026, 11, 13)) == _EC_DAY_13
    assert await coord._fetch_energy_community_day(date(2026, 11, 14)) is None
    api.get_day.assert_awaited_with(MOCK_METER_ID, date(2026, 11, 14), record_shape=False)


# -- _async_update_data: rejected credentials --


@pytest.mark.parametrize("endpoint", ["get_day", "get_week", "get_year", "get_raw"])
async def test_update_data_auth_error_raises_config_entry_auth_failed(hass: HomeAssistant, endpoint: str) -> None:
    """Test rejected credentials from any endpoint start reauth instead of counting as a missing endpoint."""
    api = _mock_api(**{endpoint: AsyncMock(side_effect=NetzNoeAuthError("Invalid username or password"))})
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
    first, second = (c.args[1] for c in api.get_day.call_args_list)
    assert second == first - timedelta(days=1)
    assert data.daily_total == pytest.approx(0.6)
    assert data.daily_date == second


async def test_fetch_helpers_record_period_starts(hass: HomeAssistant) -> None:
    """Test each fetch helper records the start of the period its total covers."""
    api = _mock_api()
    coord = _make_coordinator(hass, api)
    data = NetzNoeData()
    today = date(2025, 2, 10)  # a Monday: the newest day the portal can have is Sunday
    newest = date(2025, 2, 9)

    await coord._fetch_day(data, today)
    await coord._fetch_week(data, newest)
    api.get_week.assert_called_once_with(MOCK_METER_ID, date(2025, 2, 3), date(2025, 2, 10))
    await coord._fetch_current_year(data, newest, {})

    assert data.daily_date == date(2025, 2, 9)
    assert data.week_start == date(2025, 2, 3)
    assert data.month_start == date(2025, 2, 1)
    assert data.monthly_total == pytest.approx(20.0)  # February entry of MOCK_YEAR_RESPONSE
    assert data.year_start == date(2025, 1, 1)


async def test_update_data_periods_follow_newest_day(hass: HomeAssistant, freezer: FrozenDateTimeFactory) -> None:
    """Test a refresh on New Year's Day still reports last week, December and the old year."""
    freezer.move_to("2025-01-01 12:00:00-08:00")  # a Wednesday, test timezone is US/Pacific
    api = _mock_api()
    api.get_year = AsyncMock(return_value={"values": [10.0] * 12})
    coord = _make_coordinator(hass, api)

    with patch(
        "custom_components.netz_noe_smartmeter.coordinator.async_import_raw_statistics",
        new_callable=AsyncMock,
    ):
        data = await coord._async_update_data()

    api.get_day.assert_any_call(MOCK_METER_ID, date(2024, 12, 31))
    api.get_week.assert_called_once_with(MOCK_METER_ID, date(2024, 12, 30), date(2025, 1, 6))
    assert [c.args for c in api.get_year.call_args_list] == [(MOCK_METER_ID, 2024), (MOCK_METER_ID, 2023)]
    assert data.week_start == date(2024, 12, 30)
    assert data.month_start == date(2024, 12, 1)
    assert data.year_start == date(2024, 1, 1)
    assert data.monthly_total == pytest.approx(10.0)
    assert data.yearly_total == pytest.approx(120.0)
    assert data.prev_year == 2023
    assert data.prev_year_monthly_values == [10.0] * 12


async def test_update_data_periods_follow_fallback_day(hass: HomeAssistant, freezer: FrozenDateTimeFactory) -> None:
    """Test that while Monday is unpublished, the week stays on the completed one instead of going unknown."""
    freezer.move_to("2025-02-11 01:00:00-08:00")  # a Tuesday, test timezone is US/Pacific
    api = _mock_api()
    api.get_day = AsyncMock(
        side_effect=[{"meteredValues": [None], "peakDemandTimes": [None]}, MOCK_DAY_RESPONSE],
    )
    coord = _make_coordinator(hass, api)

    with patch(
        "custom_components.netz_noe_smartmeter.coordinator.async_import_raw_statistics",
        new_callable=AsyncMock,
    ):
        data = await coord._async_update_data()

    assert data.daily_date == date(2025, 2, 9)  # Sunday
    api.get_week.assert_called_once_with(MOCK_METER_ID, date(2025, 2, 3), date(2025, 2, 10))
    assert data.week_start == date(2025, 2, 3)
    assert data.month_start == date(2025, 2, 1)


async def test_update_data_day_error_does_not_fall_back(hass: HomeAssistant) -> None:
    """Test a failed request for yesterday leaves the daily total unknown instead of reporting an older day."""
    api = _mock_api()
    api.get_day = AsyncMock(side_effect=NetzNoeApiError("boom"))
    coord = _make_coordinator(hass, api)

    with patch(
        "custom_components.netz_noe_smartmeter.coordinator.async_import_raw_statistics",
        new_callable=AsyncMock,
    ):
        data = await coord._async_update_data()

    assert api.get_day.call_count == 1
    assert data.daily_total is None
    assert data.daily_date is None


@pytest.mark.parametrize("year_values", [[None] * 12, []])
async def test_update_data_empty_period_totals_are_unknown(hass: HomeAssistant, year_values: list[None]) -> None:
    """Test all-null or empty week and year responses give unknown totals rather than zero."""
    api = _mock_api()
    api.get_week = AsyncMock(return_value={"meteredValues": [None] * 7})
    api.get_year = AsyncMock(return_value={"values": year_values})
    coord = _make_coordinator(hass, api)

    with patch(
        "custom_components.netz_noe_smartmeter.coordinator.async_import_raw_statistics",
        new_callable=AsyncMock,
    ):
        data = await coord._async_update_data()

    assert data.weekly_total is None
    assert data.monthly_total is None
    assert data.yearly_total is None


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
    assert data.daily_date is None


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


# -- _async_update_data: rate limit handling --


async def test_update_data_maintenance_raises_update_failed(
    hass: HomeAssistant,
) -> None:
    """Test NetzNoeMaintenanceError is wrapped in UpdateFailed with a 30 minute retry."""
    api = _mock_api(get_day=AsyncMock(side_effect=NetzNoeMaintenanceError("Portal is under maintenance")))
    coord = _make_coordinator(hass, api)

    with pytest.raises(UpdateFailed, match="under maintenance") as exc_info:
        await coord._async_update_data()

    assert exc_info.value.retry_after == 1800


async def test_update_data_maintenance_during_year_fetch_propagates(
    hass: HomeAssistant,
) -> None:
    """Test maintenance mid-poll aborts the poll instead of being logged as a missing endpoint."""
    api = _mock_api(get_year=AsyncMock(side_effect=NetzNoeMaintenanceError("Portal is under maintenance")))
    coord = _make_coordinator(hass, api)

    with pytest.raises(UpdateFailed, match="under maintenance"):
        await coord._async_update_data()


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


async def test_update_data_login_error_raises_update_failed(
    hass: HomeAssistant,
) -> None:
    """Test a login that fails for another reason than credentials aborts the poll."""
    api = _mock_api(get_day=AsyncMock(side_effect=NetzNoeLoginError("Connection error during login: connection reset")))
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
