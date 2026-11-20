"""DataUpdateCoordinator for Netz NÖ Smart Meter."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from functools import partial
from typing import cast, override

import aiohttp
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import (
    DataUpdateCoordinator,
    UpdateFailed,
)
from homeassistant.util import dt as dt_util

from .api import (
    EnergyCommunityValues,
    NetzNoeApi,
    NetzNoeApiError,
    NetzNoeAuthError,
    NetzNoeLoginError,
    NetzNoeMaintenanceError,
    NetzNoeRateLimitError,
    RawRecord,
    YearResponse,
    energy_community_values,
)
from .const import CONF_METER_ID, DEFAULT_SCAN_INTERVAL_HOURS, DOMAIN, NetzNoeConfigData
from .statistics import (
    EnergyCommunityDay,
    async_import_energy_community_statistics,
    async_import_raw_statistics,
    energy_community_day,
)

_LOGGER = logging.getLogger(__name__)

_POLL_FATAL_ERRORS = (NetzNoeLoginError, NetzNoeMaintenanceError, NetzNoeRateLimitError)


def _total(values: Sequence[float | None]) -> float | None:
    """Return the rounded sum of the non-null values, or None if there are none."""
    non_null = [v for v in values if v is not None]
    return round(sum(non_null), 3) if non_null else None


def _month_value(values: list[float | None], month: int) -> float | None:
    """Return the rounded value for a month (1-12) from a list of monthly values."""
    value = values[month - 1] if len(values) >= month else None
    return round(value, 3) if value is not None else None


def _week_start(day: date) -> date:
    """Return the Monday of the day's week."""
    return day - timedelta(days=day.weekday())


@dataclass(frozen=True)
class EnergyCommunitySplit:
    """Energy community (EEG) split of a period's consumption.

    self_coverage is the part covered by the community, grid_leftover the rest
    from the supplier, start the first day of the period.
    """

    self_coverage: float | None
    grid_leftover: float | None
    start: date


def _split(
    response: EnergyCommunityValues,
    reduce: Callable[[list[float | None]], float | None],
    start: date,
) -> EnergyCommunitySplit | None:
    """Return the period's energy community split, or None for a meter outside an EEG."""
    self_coverage, grid_leftover = energy_community_values(response)
    return _split_of(self_coverage, grid_leftover, reduce, start)


def _split_of(
    self_coverage: list[float | None],
    grid_leftover: list[float | None],
    reduce: Callable[[list[float | None]], float | None],
    start: date,
) -> EnergyCommunitySplit | None:
    """Return the split of the given values, or None if neither has one."""
    split = EnergyCommunitySplit(reduce(self_coverage), reduce(grid_leftover), start)
    return None if split.self_coverage is None and split.grid_leftover is None else split


@dataclass
class BackfillProgress:
    """Tracks backfill service progress for the UI sensor."""

    state: str = "idle"  # idle, running, clearing, completed, failed
    current_chunk: int = 0
    total_chunks: int | None = None
    records_processed: int = 0
    current_date_range: str | None = None
    result_summary: str | None = None


@dataclass
class NetzNoeData:
    """Data class holding fetched smart meter data."""

    # Yesterday's 15-min consumption values (kWh each)
    daily_values: list[float] = field(default_factory=list)
    # Timestamps for daily values
    daily_timestamps: list[str | None] = field(default_factory=list)
    # Yesterday's total consumption (kWh) and the day it covers
    daily_total: float | None = None
    daily_date: date | None = None
    # Current week's daily totals (Mon-Sun, from Week endpoint)
    weekly_daily_values: list[float | None] = field(default_factory=list)
    # Current week total consumption (kWh) and its Monday
    weekly_total: float | None = None
    week_start: date | None = None
    # Monthly totals from Year endpoint (12 entries, None for future months)
    monthly_values: list[float | None] = field(default_factory=list)
    # Current month consumption (kWh) and its first day
    monthly_total: float | None = None
    month_start: date | None = None
    # Year-to-date consumption (kWh) and January 1st
    yearly_total: float | None = None
    year_start: date | None = None
    # Previous year monthly totals (for invoice comparison)
    prev_year: int | None = None
    prev_year_monthly_values: list[float | None] = field(default_factory=list)
    # Timestamp of the newest data point
    last_data_timestamp: str | None = None
    # Raw records for statistics import
    raw_records: list[RawRecord] = field(default_factory=list)
    # Energy community (EEG) split per period, None for meters outside an EEG.
    # The portal publishes the split later than the consumption, so the splits
    # can be from the day before daily_date, and then from the week, month or
    # year before.
    daily_split: EnergyCommunitySplit | None = None
    weekly_split: EnergyCommunitySplit | None = None
    monthly_split: EnergyCommunitySplit | None = None
    yearly_split: EnergyCommunitySplit | None = None

    @property
    def has_energy_community(self) -> bool:
        """Return whether the portal returned an energy community split for any period."""
        return any((self.daily_split, self.weekly_split, self.monthly_split, self.yearly_split))


class NetzNoeCoordinator(DataUpdateCoordinator[NetzNoeData]):
    """Coordinator to fetch data from Netz NÖ Smart Meter portal."""

    config_entry: ConfigEntry
    api: NetzNoeApi
    session: aiohttp.ClientSession
    meter_id: str
    _first_refresh: bool
    _energy_community_day: EnergyCommunityDay | None
    backfill_progress: BackfillProgress

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: ConfigEntry,
        api: NetzNoeApi,
        session: aiohttp.ClientSession,
    ) -> None:
        """Initialize the coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            config_entry=config_entry,
            update_interval=timedelta(hours=DEFAULT_SCAN_INTERVAL_HOURS),
            always_update=False,
        )
        self.api = api
        self.session = session
        config_data = cast(NetzNoeConfigData, cast(object, config_entry.data))
        self.meter_id = config_data[CONF_METER_ID]
        self._first_refresh = True
        self._energy_community_day = None
        self.backfill_progress = BackfillProgress()

    @override
    async def _async_update_data(self) -> NetzNoeData:
        """Fetch data from the API."""
        try:
            data = await self._fetch_all_data()
        except NetzNoeAuthError as err:
            raise ConfigEntryAuthFailed("Authentication failed. Check credentials.") from err
        except NetzNoeMaintenanceError as err:
            raise UpdateFailed(str(err), retry_after=1800) from err
        except NetzNoeRateLimitError as err:
            raise UpdateFailed("API rate limit exceeded", retry_after=300) from err
        except NetzNoeApiError as err:
            raise UpdateFailed(f"Error communicating with API: {err}") from err
        return data

    async def _fetch_all_data(self) -> NetzNoeData:
        """Fetch all data from the API in parallel.

        The portal publishes a day only after it has ended, and usually a few
        hours later. The week, month and year sensors are anchored on the day
        the day fetch found, so they keep showing the completed period until
        the first day of the new one is published. A failed day fetch falls
        back to yesterday. The energy community sensors do the same with the
        newest day that has a split, which can be the day before.

        Each helper writes to its own fields on the shared NetzNoeData object.
        _POLL_FATAL_ERRORS propagate immediately.
        NetzNoeApiError is caught per-endpoint so partial data is preserved.

        On the first refresh (during setup), raw data + statistics import are
        skipped to keep setup fast. A full refresh is scheduled right after.
        """
        data = NetzNoeData()
        today = dt_util.now().date()
        full_refresh = not self._first_refresh
        self._first_refresh = False

        await self._fetch_day(data, today)
        newest = data.daily_date or today - timedelta(days=1)

        years: dict[int, YearResponse] = {}
        tasks = [
            self._fetch_week(data, newest),
            self._fetch_current_year(data, newest, years),
            self._fetch_prev_year(data, newest, years),
        ]

        if full_refresh:
            tasks.append(self._fetch_raw(data, today))
        else:
            _LOGGER.debug("First refresh — skipping raw data fetch, will schedule full refresh")

        _ = await asyncio.gather(*tasks)

        if not full_refresh:
            return data

        if data.daily_split is None and data.daily_date is not None and self._in_energy_community(data):
            await self._fetch_previous_day_split(data, data.daily_date - timedelta(days=1))
            if data.daily_split is not None:
                await self._use_split_periods_of(data, data.daily_split.start, newest, years)

        # Import raw 15-min data as hourly statistics for Energy Dashboard
        if data.raw_records:
            try:
                await async_import_raw_statistics(self.hass, self.meter_id, data.raw_records)
            except Exception:
                _LOGGER.exception("Failed to import statistics")

        if data.daily_split is not None and self._energy_community_day is not None:
            try:
                await async_import_energy_community_statistics(
                    self.hass, self.meter_id, self._energy_community_day, self._fetch_energy_community_day
                )
            except _POLL_FATAL_ERRORS:
                raise
            except Exception:
                _LOGGER.exception("Failed to import energy community statistics")

        return data

    def _in_energy_community(self, data: NetzNoeData) -> bool:
        """Return whether the meter is in an energy community.

        The week or month has a split, or the previous poll's day had one: on a
        Monday that is also the 1st, the week and month have no split yet either.
        """
        previous = cast(NetzNoeData | None, self.data)
        return (
            data.weekly_split is not None
            or data.monthly_split is not None
            or (previous is not None and previous.daily_split is not None)
        )

    def _use_daily_split(self, data: NetzNoeData, ec_day: EnergyCommunityDay) -> None:
        """Report a day's energy community split as the daily split, and keep the day for the next poll."""
        data.daily_split = _split_of(ec_day.self_coverage, ec_day.grid_leftover, _total, ec_day.day)
        self._energy_community_day = ec_day

    async def _fetch_energy_community_day(self, day: date) -> EnergyCommunityDay | None:
        """Fetch a day's energy community split, None if the portal has none for it or the request failed."""
        try:
            response = await self.api.get_day(self.meter_id, day, record_shape=False)
        except _POLL_FATAL_ERRORS:
            raise
        except NetzNoeApiError as err:
            _LOGGER.warning("Failed to fetch energy community data for %s: %s", day, err)
            return None
        return energy_community_day(day, response)

    async def _fetch_day(self, data: NetzNoeData, today: date) -> None:
        """Fetch the newest day (yesterday, or 2 days ago while yesterday is not published yet).

        A failed request does not fall back: reporting an older day again would
        move last_reset backwards and count that day twice.
        """
        for days_ago in (1, 2):
            fetch_date = today - timedelta(days=days_ago)
            try:
                day_data = await self.api.get_day(self.meter_id, fetch_date)
                values = day_data.get("meteredValues") or []
                timestamps = day_data.get("peakDemandTimes") or []
                non_null = [v for v in values if v is not None]

                if non_null:
                    data.daily_values = non_null
                    data.daily_timestamps = timestamps
                    data.daily_total = _total(non_null)
                    data.daily_date = fetch_date
                    if (ec_day := energy_community_day(fetch_date, day_data)) is not None:
                        self._use_daily_split(data, ec_day)

                    non_null_ts = [t for t in timestamps if t is not None]
                    if non_null_ts:
                        data.last_data_timestamp = non_null_ts[-1]
                    _LOGGER.debug("Day data for %s: %.3f kWh", fetch_date, data.daily_total)
                    return
                _LOGGER.debug("No day data for %s, trying older date", fetch_date)
            except _POLL_FATAL_ERRORS:
                raise
            except NetzNoeApiError as err:
                _LOGGER.warning("Failed to fetch day data for %s: %s", fetch_date, err)
                return

    async def _fetch_previous_day_split(self, data: NetzNoeData, day: date) -> None:
        """Use the energy community split of the day before the newest one, fetched only if no poll had it yet."""
        ec_day = self._energy_community_day
        if ec_day is None or ec_day.day != day:
            ec_day = await self._fetch_energy_community_day(day)
        if ec_day is None:
            _LOGGER.debug("No energy community data for %s either", day)
            return
        self._use_daily_split(data, ec_day)
        _LOGGER.debug("Energy community data from %s, the newest day has none yet", day)

    async def _use_split_periods_of(
        self, data: NetzNoeData, day: date, newest: date, years: dict[int, YearResponse]
    ) -> None:
        """Report the splits of the day's week, month and year where the newest day starts a new one.

        The newest day has no split yet, so the new period has none either. Like
        the consumption sensors, the energy community sensors keep showing the
        completed period, which now includes its last day, until it has one.
        """
        if (year := years.get(day.year)) is not None:
            if (day.year, day.month) != (newest.year, newest.month):
                data.monthly_split = _split(year, partial(_month_value, month=day.month), day.replace(day=1))
            if day.year != newest.year:
                data.yearly_split = _split(year, _total, day.replace(month=1, day=1))

        if (week_start := _week_start(day)) == _week_start(newest):
            return
        try:
            week = await self.api.get_week(
                self.meter_id, week_start, week_start + timedelta(days=7), record_shape=False
            )
        except _POLL_FATAL_ERRORS:
            raise
        except NetzNoeApiError as err:
            _LOGGER.warning("Failed to fetch energy community data for the week of %s: %s", week_start, err)
            return
        data.weekly_split = _split(week, _total, week_start)

    async def _fetch_week(self, data: NetzNoeData, newest: date) -> None:
        """Fetch the week (Mon-Sun) containing the newest day."""
        week_start = _week_start(newest)
        next_week_start = week_start + timedelta(days=7)
        try:
            week_data = await self.api.get_week(self.meter_id, week_start, next_week_start)
            weekly_values = week_data.get("meteredValues") or []
            data.weekly_daily_values = weekly_values
            data.weekly_total = _total(weekly_values)
            data.weekly_split = _split(week_data, _total, week_start)
            data.week_start = week_start
            _LOGGER.debug("Week data from %s: %s kWh", week_start, data.weekly_total)
        except _POLL_FATAL_ERRORS:
            raise
        except NetzNoeApiError as err:
            _LOGGER.warning("Failed to fetch week data: %s", err)

    async def _fetch_current_year(self, data: NetzNoeData, newest: date, years: dict[int, YearResponse]) -> None:
        """Fetch the year containing the newest day for monthly/yearly totals, keeping the response in years."""
        try:
            year_data = await self.api.get_year(self.meter_id, newest.year)
            years[newest.year] = year_data
            monthly = year_data.get("values") or []
            data.monthly_values = monthly

            data.monthly_total = _month_value(monthly, newest.month)
            if data.monthly_total is not None:
                data.month_start = newest.replace(day=1)
                data.monthly_split = _split(year_data, partial(_month_value, month=newest.month), data.month_start)
            data.yearly_total = _total(monthly)
            data.year_start = newest.replace(month=1, day=1)
            data.yearly_split = _split(year_data, _total, data.year_start)
        except _POLL_FATAL_ERRORS:
            raise
        except NetzNoeApiError as err:
            _LOGGER.warning("Failed to fetch year data: %s", err)

    async def _fetch_prev_year(self, data: NetzNoeData, newest: date, years: dict[int, YearResponse]) -> None:
        """Fetch the year before the newest day for invoice comparison, keeping the response in years."""
        prev_year = newest.year - 1
        try:
            prev_data = await self.api.get_year(self.meter_id, prev_year, record_shape=False)
            years[prev_year] = prev_data
            prev_monthly = prev_data.get("values") or []
            data.prev_year = prev_year
            data.prev_year_monthly_values = prev_monthly
            _LOGGER.debug("Previous year %d data fetched", prev_year)
        except _POLL_FATAL_ERRORS:
            raise
        except NetzNoeApiError as err:
            _LOGGER.debug("No previous year data for %d: %s", prev_year, err)

    async def _fetch_raw(self, data: NetzNoeData, today: date) -> None:
        """Fetch raw data for the last 5 days (for statistics import)."""
        raw_start = today - timedelta(days=5)
        try:
            data.raw_records = await self.api.get_raw(self.meter_id, raw_start, today)
        except _POLL_FATAL_ERRORS:
            raise
        except NetzNoeApiError as err:
            _LOGGER.warning("Failed to fetch raw data: %s", err)
