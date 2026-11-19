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
)
from .const import CONF_METER_ID, DEFAULT_SCAN_INTERVAL_HOURS, DOMAIN, NetzNoeConfigData
from .statistics import async_import_raw_statistics

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


@dataclass(frozen=True)
class EnergyCommunitySplit:
    """Energy community (EEG) split of a period's consumption.

    self_coverage is the part covered by the community, grid_leftover the rest from the supplier.
    """

    self_coverage: float | None
    grid_leftover: float | None


def _split(
    response: EnergyCommunityValues,
    reduce: Callable[[list[float | None]], float | None],
) -> EnergyCommunitySplit | None:
    """Return the period's energy community split, or None for a meter outside an EEG."""
    split = EnergyCommunitySplit(
        self_coverage=reduce(response.get("selfCoverageValues") or []),
        grid_leftover=reduce(response.get("gridUsageLeftoverValues") or []),
    )
    return None if split == EnergyCommunitySplit(None, None) else split


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
    # Energy community (EEG) split per period, None for meters outside an EEG
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
        back to yesterday.

        Each helper writes to its own fields on the shared NetzNoeData object.
        _POLL_FATAL_ERRORS propagate immediately.
        NetzNoeApiError is caught per-endpoint so partial data is preserved.

        On the first refresh (during setup), raw data + statistics import are
        skipped to keep setup fast. A full refresh is scheduled right after.
        """
        data = NetzNoeData()
        today = dt_util.now().date()

        await self._fetch_day(data, today)
        newest = data.daily_date or today - timedelta(days=1)

        tasks = [
            self._fetch_week(data, newest),
            self._fetch_current_year(data, newest),
            self._fetch_prev_year(data, newest),
        ]

        if self._first_refresh:
            self._first_refresh = False
            _LOGGER.debug("First refresh — skipping raw data fetch, will schedule full refresh")
        else:
            tasks.append(self._fetch_raw(data, today))

        _ = await asyncio.gather(*tasks)

        # Import raw 15-min data as hourly statistics for Energy Dashboard
        if data.raw_records:
            try:
                await async_import_raw_statistics(self.hass, self.meter_id, data.raw_records)
            except Exception:
                _LOGGER.exception("Failed to import statistics")

        return data

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
                    data.daily_split = _split(day_data, _total)

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

    async def _fetch_week(self, data: NetzNoeData, newest: date) -> None:
        """Fetch the week (Mon-Sun) containing the newest day."""
        week_start = newest - timedelta(days=newest.isoweekday() - 1)
        next_week_start = week_start + timedelta(days=7)
        try:
            week_data = await self.api.get_week(self.meter_id, week_start, next_week_start)
            weekly_values = week_data.get("meteredValues") or []
            data.weekly_daily_values = weekly_values
            data.weekly_total = _total(weekly_values)
            data.weekly_split = _split(week_data, _total)
            data.week_start = week_start
            _LOGGER.debug("Week data from %s: %s kWh", week_start, data.weekly_total)
        except _POLL_FATAL_ERRORS:
            raise
        except NetzNoeApiError as err:
            _LOGGER.warning("Failed to fetch week data: %s", err)

    async def _fetch_current_year(self, data: NetzNoeData, newest: date) -> None:
        """Fetch the year containing the newest day for monthly/yearly totals."""
        try:
            year_data = await self.api.get_year(self.meter_id, newest.year)
            monthly = year_data.get("values") or []
            data.monthly_values = monthly

            data.monthly_total = _month_value(monthly, newest.month)
            if data.monthly_total is not None:
                data.month_start = newest.replace(day=1)
                data.monthly_split = _split(year_data, partial(_month_value, month=newest.month))
            data.yearly_total = _total(monthly)
            data.year_start = newest.replace(month=1, day=1)
            data.yearly_split = _split(year_data, _total)
        except _POLL_FATAL_ERRORS:
            raise
        except NetzNoeApiError as err:
            _LOGGER.warning("Failed to fetch year data: %s", err)

    async def _fetch_prev_year(self, data: NetzNoeData, newest: date) -> None:
        """Fetch the year before the newest day for invoice comparison."""
        prev_year = newest.year - 1
        try:
            prev_data = await self.api.get_year(self.meter_id, prev_year, record_shape=False)
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
