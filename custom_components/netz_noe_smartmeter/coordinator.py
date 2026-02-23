"""DataUpdateCoordinator for Netz NÖ Smart Meter."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import cast, override

import aiohttp
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import (
    DataUpdateCoordinator,
    UpdateFailed,
)

from .api import NetzNoeApi, NetzNoeApiError, NetzNoeAuthError, NetzNoeRateLimitError, RawRecord
from .const import CONF_METER_ID, DEFAULT_SCAN_INTERVAL_HOURS, DOMAIN, NetzNoeConfigData
from .statistics import async_import_raw_statistics

_LOGGER = logging.getLogger(__name__)


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
    # Yesterday's total consumption (kWh)
    daily_total: float | None = None
    # Current week's daily totals (Mon-Sun, from Week endpoint)
    weekly_daily_values: list[float | None] = field(default_factory=list)
    # Current week total consumption (kWh)
    weekly_total: float | None = None
    # Monthly totals from Year endpoint (12 entries, None for future months)
    monthly_values: list[float | None] = field(default_factory=list)
    # Current month consumption (kWh)
    monthly_total: float | None = None
    # Year-to-date consumption (kWh)
    yearly_total: float | None = None
    # Previous year monthly totals (for invoice comparison)
    prev_year: int | None = None
    prev_year_monthly_values: list[float | None] = field(default_factory=list)
    # Timestamp of the newest data point
    last_data_timestamp: str | None = None
    # Raw records for statistics import
    raw_records: list[RawRecord] = field(default_factory=list)


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

    async def ensure_authenticated(self) -> None:
        """Ensure we have a valid session, re-login if needed."""
        if not self.api.is_authenticated:
            try:
                config_data = cast(NetzNoeConfigData, cast(object, self.config_entry.data))
                username = config_data[CONF_USERNAME]
                password = config_data[CONF_PASSWORD]
                _ = await self.api.authenticate(username, password)
            except NetzNoeAuthError as err:
                raise ConfigEntryAuthFailed("Authentication failed. Check credentials.") from err

    @override
    async def _async_update_data(self) -> NetzNoeData:
        """Fetch data from the API."""
        try:
            await self.ensure_authenticated()
            data = await self._fetch_all_data()
        except NetzNoeAuthError:
            # Session expired between polls — re-login once and retry
            _LOGGER.info("Session expired, re-authenticating")
            self.api.invalidate_session()
            try:
                await self.ensure_authenticated()
                data = await self._fetch_all_data()
            except NetzNoeAuthError as err:
                raise ConfigEntryAuthFailed(str(err)) from err
        except NetzNoeRateLimitError as err:
            raise UpdateFailed("API rate limit exceeded", retry_after=300) from err
        except NetzNoeApiError as err:
            raise UpdateFailed(f"Error communicating with API: {err}") from err
        return data

    async def _fetch_all_data(self) -> NetzNoeData:
        """Fetch all data from the API in parallel.

        Each helper writes to its own fields on the shared NetzNoeData object.
        NetzNoeAuthError and NetzNoeRateLimitError propagate immediately.
        NetzNoeApiError is caught per-endpoint so partial data is preserved.

        On the first refresh (during setup), raw data + statistics import are
        skipped to keep setup fast. A full refresh is scheduled right after.
        """
        data = NetzNoeData()
        today = date.today()

        tasks = [
            self._fetch_day(data, today),
            self._fetch_week(data, today),
            self._fetch_current_year(data, today),
            self._fetch_prev_year(data, today),
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
                await async_import_raw_statistics(self.hass, data.raw_records)
            except Exception:
                _LOGGER.exception("Failed to import statistics")

        return data

    async def _fetch_day(self, data: NetzNoeData, today: date) -> None:
        """Fetch most recent day data (try yesterday, fall back to 2 days ago)."""
        for days_ago in (1, 2):
            fetch_date = today - timedelta(days=days_ago)
            try:
                day_data = await self.api.get_day(self.meter_id, fetch_date)
                values = day_data.get("meteredValues", [])
                timestamps = day_data.get("peakDemandTimes", [])
                non_null = [v for v in values if v is not None]

                if non_null:
                    data.daily_values = non_null
                    data.daily_timestamps = timestamps
                    data.daily_total = round(sum(non_null), 3)

                    non_null_ts = [t for t in timestamps if t is not None]
                    if non_null_ts:
                        data.last_data_timestamp = non_null_ts[-1]
                    _LOGGER.debug("Day data for %s: %.3f kWh", fetch_date, data.daily_total)
                    return
                _LOGGER.debug("No day data for %s, trying older date", fetch_date)
            except (NetzNoeAuthError, NetzNoeRateLimitError):
                raise
            except NetzNoeApiError as err:
                _LOGGER.warning("Failed to fetch day data for %s: %s", fetch_date, err)

    async def _fetch_week(self, data: NetzNoeData, today: date) -> None:
        """Fetch current week data (Mon-Sun)."""
        week_start = today - timedelta(days=today.isoweekday() - 1)
        week_end = week_start + timedelta(days=6)
        try:
            week_data = await self.api.get_week(self.meter_id, week_start, week_end)
            weekly_values = week_data.get("meteredValues", [])
            data.weekly_daily_values = weekly_values
            non_null_weekly = [v for v in weekly_values if v is not None]
            data.weekly_total = round(sum(non_null_weekly), 3) if non_null_weekly else 0.0
            _LOGGER.debug(
                "Week data %s to %s: %.3f kWh",
                week_start,
                week_end,
                data.weekly_total,
            )
        except (NetzNoeAuthError, NetzNoeRateLimitError):
            raise
        except NetzNoeApiError as err:
            _LOGGER.warning("Failed to fetch week data: %s", err)

    async def _fetch_current_year(self, data: NetzNoeData, today: date) -> None:
        """Fetch year data for monthly/yearly totals."""
        try:
            year_data = await self.api.get_year(self.meter_id, today.year)
            monthly = year_data.get("values", [])
            data.monthly_values = monthly

            current_month_val = monthly[today.month - 1] if len(monthly) >= today.month else None
            if current_month_val is not None:
                data.monthly_total = round(current_month_val, 3)

            data.yearly_total = round(sum(v for v in monthly if v is not None), 3)
        except (NetzNoeAuthError, NetzNoeRateLimitError):
            raise
        except NetzNoeApiError as err:
            _LOGGER.warning("Failed to fetch year data: %s", err)

    async def _fetch_prev_year(self, data: NetzNoeData, today: date) -> None:
        """Fetch previous year data for invoice comparison."""
        prev_year = today.year - 1
        try:
            prev_data = await self.api.get_year(self.meter_id, prev_year)
            prev_monthly = prev_data.get("values", [])
            data.prev_year = prev_year
            data.prev_year_monthly_values = prev_monthly
            _LOGGER.debug("Previous year %d data fetched", prev_year)
        except (NetzNoeAuthError, NetzNoeRateLimitError):
            raise
        except NetzNoeApiError as err:
            _LOGGER.debug("No previous year data for %d: %s", prev_year, err)

    async def _fetch_raw(self, data: NetzNoeData, today: date) -> None:
        """Fetch raw data for the last 5 days (for statistics import)."""
        raw_start = today - timedelta(days=5)
        try:
            data.raw_records = await self.api.get_raw(self.meter_id, raw_start, today)
        except (NetzNoeAuthError, NetzNoeRateLimitError):
            raise
        except NetzNoeApiError as err:
            _LOGGER.warning("Failed to fetch raw data: %s", err)
