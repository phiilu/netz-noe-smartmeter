"""API client for Netz NÖ Smart Meter portal."""

from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import TypedDict, cast

import aiohttp

from .const import (
    BASE_URL,
    ENDPOINT_DAY,
    ENDPOINT_LOGIN,
    ENDPOINT_LOGOUT,
    ENDPOINT_RAW,
    ENDPOINT_WEEK,
    ENDPOINT_YEAR,
    PORTAL_URL,
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


class NetzNoeLoginError(NetzNoeApiError):
    """Login failed, so no request can succeed."""


class NetzNoeAuthError(NetzNoeLoginError):
    """The portal rejected the credentials."""


class NetzNoeMaintenanceError(NetzNoeApiError):
    """Portal is under maintenance (HTTP 999)."""


class NetzNoeRateLimitError(NetzNoeApiError):
    """API rate limit exceeded (HTTP 429)."""


_MAINTENANCE_MESSAGE = f"Portal is under maintenance (HTTP 999), see {PORTAL_URL}"


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------


class _SessionExpiredError(Exception):
    """The portal answered a request with 401."""


class NetzNoeApi:
    """Async API client for smartmeter.netz-noe.at.

    The client owns the portal session. It logs in before the first request and
    again when the portal has dropped the session, so callers never see an
    expired session.
    """

    _session: aiohttp.ClientSession
    _username: str
    _password: str
    _login_lock: asyncio.Lock
    _login_attempts: int
    _login_error: NetzNoeApiError | None

    def __init__(self, session: aiohttp.ClientSession, username: str, password: str) -> None:
        """Initialize the API client.

        Args:
            session: aiohttp session managed externally (with cookie jar).
            username: Portal username.
            password: Portal password.
        """
        self._session = session
        self._username = username
        self._password = password
        self._login_lock = asyncio.Lock()
        self._login_attempts = 0
        self._login_error = None

    async def authenticate(self) -> None:
        """Login to the smart meter portal.

        Raises NetzNoeAuthError if the portal rejects the credentials and
        NetzNoeLoginError if the login fails for another reason.
        """
        url = f"{BASE_URL}/{ENDPOINT_LOGIN}"
        payload = {"user": self._username, "pwd": self._password}

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
                    _LOGGER.debug("Login successful")
                    return
                if resp.status == 401:
                    raise NetzNoeAuthError("Invalid username or password")
                if resp.status == 999:
                    raise NetzNoeMaintenanceError(_MAINTENANCE_MESSAGE)
                body = await resp.text()
                raise NetzNoeLoginError(f"Login failed with status {resp.status}: {body}")
        except aiohttp.ClientError as err:
            raise NetzNoeLoginError(f"Connection error during login: {err}") from err

    async def logout(self) -> None:
        """Logout from the portal."""
        try:
            url = f"{BASE_URL}/{ENDPOINT_LOGOUT}"
            async with self._session.get(url) as resp:
                _LOGGER.debug("Logout status: %s", resp.status)
        except aiohttp.ClientError:
            pass

    async def _login_unless_renewed(self, seen: int) -> None:
        """Log in, unless another request already tried after login attempt number `seen`.

        Requests waiting for the same login share its outcome, so rejected
        credentials are sent once, not once per request.
        """
        async with self._login_lock:
            if self._login_attempts == seen:
                self._login_attempts += 1
                self._login_error = None
                try:
                    await self.authenticate()
                except NetzNoeApiError as err:
                    self._login_error = err
                    raise
            elif self._login_error is not None:
                raise self._login_error

    async def _get(self, endpoint: str, params: dict[str, str]) -> object:
        """Make a GET request, logging in first if needed and again once if the session expired."""
        seen = self._login_attempts
        if seen == 0 or self._login_error is not None:
            await self._login_unless_renewed(seen)
            seen = self._login_attempts
        try:
            return await self._request(endpoint, params)
        except _SessionExpiredError:
            _LOGGER.debug("Session expired, logging in again")
        await self._login_unless_renewed(seen)
        try:
            return await self._request(endpoint, params)
        except _SessionExpiredError as err:
            raise NetzNoeApiError(f"Session rejected right after login for {endpoint}") from err

    async def _request(self, endpoint: str, params: dict[str, str]) -> object:
        """Make a single GET request with the current session."""
        url = f"{BASE_URL}/{endpoint}"
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://smartmeter.netz-noe.at/",
        }

        try:
            async with self._session.get(url, params=params, headers=headers) as resp:
                if resp.status == 401:
                    raise _SessionExpiredError
                if resp.status == 429:
                    raise NetzNoeRateLimitError("API rate limit exceeded")
                if resp.status == 999:
                    raise NetzNoeMaintenanceError(_MAINTENANCE_MESSAGE)
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
        """Fetch daily totals for a date range (Week endpoint), end_date exclusive.

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
