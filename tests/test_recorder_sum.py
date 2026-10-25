"""Recorder-backed checks that period totals compile into correct sum statistics."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from freezegun.api import FrozenDateTimeFactory
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import MockConfigEntry
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_wait_recording_done,
    do_adhoc_statistics,
    get_start_time,
    statistics_during_period,
)

from custom_components.netz_noe_smartmeter.const import DOMAIN
from custom_components.netz_noe_smartmeter.coordinator import NetzNoeCoordinator

from .conftest import MOCK_CONFIG_DATA, MOCK_METER_ID

PATCH_API_CLS = "custom_components.netz_noe_smartmeter.NetzNoeApi"
PATCH_SESSION_CLS = "custom_components.netz_noe_smartmeter.aiohttp.ClientSession"
PATCH_COOKIE_JAR = "custom_components.netz_noe_smartmeter.aiohttp.CookieJar"


async def _compile(hass: HomeAssistant) -> None:
    """Commit pending states and compile statistics for the current 5-minute slot."""
    await hass.async_block_till_done()
    await async_wait_recording_done(hass)
    do_adhoc_statistics(hass, start=get_start_time(dt_util.utcnow()))
    await async_wait_recording_done(hass)


async def test_sum_statistics_follow_period_rollovers(
    hass: HomeAssistant,
    freezer: FrozenDateTimeFactory,
) -> None:
    """Daily and weekly totals compile into a sum that adds every period exactly once.

    Polls on Jan 15, 16, 17 and 21 report the days 14, 15, 16 and 20 with totals
    10, 12, 8 and 9. The first value sets the zero point, so the daily sum is 29.
    A correction of the same day (12 -> 12.5) adds only the difference. The
    weekly total grows 1 -> 3 -> 6 inside the week of Jan 13 and restarts at 2 in
    the week of Jan 20, so its sum is 2 + 3 + 2 = 7.
    """
    freezer.move_to("2025-01-15 06:00:00-08:00")  # Wednesday, test timezone is US/Pacific
    api = MagicMock()
    api.authenticate = AsyncMock()
    api.logout = AsyncMock()
    api.get_year = AsyncMock(return_value={"values": [10.0] + [None] * 11})
    api.get_raw = AsyncMock(return_value=[])
    api.is_authenticated = True
    api.invalidate_session = MagicMock()

    def serve(day_total: float, week_total: float) -> None:
        api.get_day = AsyncMock(return_value={"meteredValues": [day_total], "peakDemandTimes": ["x"]})
        api.get_week = AsyncMock(return_value={"meteredValues": [week_total]})

    serve(10.0, 1.0)
    entry = MockConfigEntry(domain=DOMAIN, title="Smart Meter", data=MOCK_CONFIG_DATA, unique_id=MOCK_METER_ID)
    entry.add_to_hass(hass)
    with (
        patch(PATCH_COOKIE_JAR),
        patch(PATCH_SESSION_CLS, return_value=AsyncMock()),
        patch(PATCH_API_CLS, return_value=api),
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
    coordinator: NetzNoeCoordinator = entry.runtime_data
    refresh: Callable[[], Awaitable[None]] = coordinator.async_refresh

    registry = er.async_get(hass)
    daily_id = registry.async_get_entity_id("sensor", DOMAIN, f"{MOCK_METER_ID}_daily_consumption")
    weekly_id = registry.async_get_entity_id("sensor", DOMAIN, f"{MOCK_METER_ID}_weekly_consumption")
    assert daily_id and weekly_id

    def assert_state(entity_id: str, value: float, period_start: date) -> None:
        state = hass.states.get(entity_id)
        assert state is not None
        assert float(state.state) == value
        assert state.attributes["state_class"] == "total"
        assert state.attributes["last_reset"] == dt_util.start_of_local_day(period_start).isoformat()

    assert_state(daily_id, 10.0, date(2025, 1, 14))
    assert_state(weekly_id, 1.0, date(2025, 1, 13))
    start = dt_util.utcnow()
    await _compile(hass)

    for days, day_total, week_total, day_date, week_start in (
        (1, 12.0, 3.0, date(2025, 1, 15), date(2025, 1, 13)),
        (0, 12.5, 3.0, date(2025, 1, 15), date(2025, 1, 13)),  # portal corrected the same day
        (1, 8.0, 6.0, date(2025, 1, 16), date(2025, 1, 13)),
        (4, 9.0, 2.0, date(2025, 1, 20), date(2025, 1, 20)),  # Tuesday: newest day is Monday, new week
    ):
        freezer.tick(timedelta(days=days, minutes=5))
        serve(day_total, week_total)
        await refresh()
        assert_state(daily_id, day_total, day_date)
        assert_state(weekly_id, week_total, week_start)
        await _compile(hass)

    stats = statistics_during_period(
        hass, start - timedelta(hours=1), statistic_ids={daily_id, weekly_id}, period="5minute"
    )
    assert [row["sum"] for row in stats[daily_id]] == pytest.approx([0.0, 12.0, 12.5, 20.5, 29.5])
    assert stats[weekly_id][-1]["sum"] == pytest.approx(7.0)
    assert stats[daily_id][-1]["last_reset"] == dt_util.start_of_local_day(date(2025, 1, 20)).timestamp()
    assert all(row.get("mean") is None for row in stats[daily_id])
