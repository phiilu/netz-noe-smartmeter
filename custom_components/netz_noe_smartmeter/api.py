"""API client for Netz NÖ Smart Meter portal."""

from __future__ import annotations

import logging
from datetime import date
from typing import TypedDict, cast

import aiohttp

from .const import (
    BASE_URL,
    ENDPOINT_DAY,
    ENDPOINT_EXTEND_SESSION,
    ENDPOINT_LOGIN,
    ENDPOINT_LOGOUT,
    ENDPOINT_RAW,
    ENDPOINT_WEEK,
    ENDPOINT_YEAR,
)

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# API response TypedDicts (total=False — we use .get() defensively)
# ---------------------------------------------------------------------------


class RawRecord(TypedDict, total=False):
    """A single 15-min measurement record from the raw endpoint."""

    MeasuredValue: float | None
    Endtime: str | None


class DayResponse(TypedDict, total=False):
    """Day endpoint response: 15-min interval data for one day."""

    meteredValues: list[float | None]
    peakDemandTimes: list[str | None]


class WeekResponse(TypedDict, total=False):
    """Week endpoint response: daily totals for a date range."""

    meteredValues: list[float | None]


class YearResponse(TypedDict, total=False):
    """Year endpoint response: monthly totals for a year."""

    values: list[float | None]


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class NetzNoeApiError(Exception):
    """Generic API error."""


class NetzNoeAuthError(NetzNoeApiError):
    """Authentication error."""


class NetzNoeRateLimitError(NetzNoeApiError):
    """API rate limit exceeded (HTTP 429)."""


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------


class NetzNoeApi:
    """Async API client for smartmeter.netz-noe.at."""

    _session: aiohttp.ClientSession
    _authenticated: bool

    def __init__(self, session: aiohttp.ClientSession) -> None:
        """Initialize the API client.

        Args:
            session: aiohttp session managed externally (with cookie jar).
        """
        self._session = session
        self._authenticated = False

    async def authenticate(self, username: str, password: str) -> bool:
        """Login to the smart meter portal.

        Returns True on success, raises NetzNoeAuthError on failure.
        """
        url = f"{BASE_URL}/{ENDPOINT_LOGIN}"
        payload = {"user": username, "pwd": password}

        try:
            async with self._session.post(
                url,
                json=payload,
                headers={
                    "Accept": "application/json, text/plain, */*",
                    "Origin": "https://smartmeter.netz-noe.at",
                    "Referer": "https://smartmeter.netz-noe.at/",
                },
            ) as resp:
                if resp.status == 200:
                    self._authenticated = True
                    _LOGGER.debug("Login successful")
                    return True
                if resp.status == 401:
                    self._authenticated = False
                    raise NetzNoeAuthError("Invalid username or password")
                body = await resp.text()
                raise NetzNoeApiError(f"Login failed with status {resp.status}: {body}")
        except aiohttp.ClientError as err:
            raise NetzNoeApiError(f"Connection error during login: {err}") from err

    async def logout(self) -> None:
        """Logout from the portal."""
        try:
            url = f"{BASE_URL}/{ENDPOINT_LOGOUT}"
            async with self._session.get(url) as resp:
                _LOGGER.debug("Logout status: %s", resp.status)
        except aiohttp.ClientError:
            pass
        finally:
            self._authenticated = False

    async def extend_session(self) -> None:
        """Extend the session lifetime."""
        url = f"{BASE_URL}/{ENDPOINT_EXTEND_SESSION}"
        try:
            async with self._session.get(url) as resp:
                if resp.status != 200:
                    _LOGGER.debug("Session extend failed: %s", resp.status)
                    self._authenticated = False
        except aiohttp.ClientError:
            self._authenticated = False

    @property
    def is_authenticated(self) -> bool:
        """Return whether we have an active session."""
        return self._authenticated

    def invalidate_session(self) -> None:
        """Mark the session as invalid (e.g. after a 401)."""
        self._authenticated = False

    async def _get(self, endpoint: str, params: dict[str, str]) -> object:
        """Make an authenticated GET request."""
        url = f"{BASE_URL}/{endpoint}"
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://smartmeter.netz-noe.at/",
        }

        try:
            async with self._session.get(url, params=params, headers=headers) as resp:
                if resp.status == 401:
                    self._authenticated = False
                    raise NetzNoeAuthError("Session expired")
                if resp.status == 429:
                    raise NetzNoeRateLimitError("API rate limit exceeded")
                if resp.status != 200:
                    body = await resp.text()
                    raise NetzNoeApiError(f"API error {resp.status} for {endpoint}: {body}")
                return cast(object, await resp.json())
        except aiohttp.ClientError as err:
            raise NetzNoeApiError(f"Connection error for {endpoint}: {err}") from err

    async def get_day(self, meter_id: str, day: date) -> DayResponse:
        """Fetch 15-min interval data for a single day.

        Returns dict with:
          meteredValues: list of 96 float (kWh per 15-min)
          peakDemandTimes: list of 96 ISO timestamps
          meteredPeakDemands: list of 96 float (kW)
        """
        params = {
            "meterId": meter_id,
            "day": day.isoformat(),
        }
        data = await self._get(ENDPOINT_DAY, params)

        # Response may be wrapped in a list — unwrap it
        if isinstance(data, list):
            data_list = cast(list[object], data)
            result: object = data_list[0] if data_list else None
        else:
            result = data
        if not isinstance(result, dict):
            raise NetzNoeApiError(f"Unexpected response format from {ENDPOINT_DAY}")
        return cast(DayResponse, cast(object, result))

    async def get_raw(self, meter_id: str, start_date: date, end_date: date) -> list[RawRecord]:
        """Fetch raw 15-min records for a date range.

        Returns list of dicts, each with:
          MeasuredValue: float (kWh)
          Endtime: str (ISO timestamp)
        """
        params = {
            "meterId": meter_id,
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
        }
        data = await self._get(ENDPOINT_RAW, params)

        # Response is [[{record}, {record}, ...]]
        if isinstance(data, list):
            data_list = cast(list[object], data)
            if data_list and isinstance(data_list[0], list):
                return cast(list[RawRecord], data_list[0])
            return cast(list[RawRecord], data_list)
        return []

    async def get_week(self, meter_id: str, start_date: date, end_date: date) -> WeekResponse:
        """Fetch daily totals for a date range (Week endpoint).

        Returns dict with:
          meteredValues: list of float|None (kWh per day)
          peakDemandTimes: list of ISO timestamps
        """
        params = {
            "meterId": meter_id,
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
        }
        data = await self._get(ENDPOINT_WEEK, params)

        if isinstance(data, list):
            data_list = cast(list[object], data)
            result: object = data_list[0] if data_list else None
        else:
            result = data
        if not isinstance(result, dict):
            raise NetzNoeApiError(f"Unexpected response format from {ENDPOINT_WEEK}")
        return cast(WeekResponse, cast(object, result))

    async def get_year(self, meter_id: str, year: int) -> YearResponse:
        """Fetch monthly totals for a year.

        Returns dict with:
          values: list of 12 float|None (kWh per month)
          peakDemands: list of 12 float|None
        """
        params = {
            "meterId": meter_id,
            "year": str(year),
        }
        data = await self._get(ENDPOINT_YEAR, params)

        if isinstance(data, list):
            data_list = cast(list[object], data)
            result: object = data_list[0] if data_list else None
        else:
            result = data
        if not isinstance(result, dict):
            raise NetzNoeApiError(f"Unexpected response format from {ENDPOINT_YEAR}")
        return cast(YearResponse, cast(object, result))
