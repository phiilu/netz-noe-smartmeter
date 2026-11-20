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


type ResponseShape = str | dict[str, ResponseShape] | list[ResponseShape] | None


class RawRecord(TypedDict, total=False):
    """A single 15-min measurement record from the raw endpoint."""

    MeasuredValue: float | None
    Endtime: str | None


class EnergyCommunityValues(TypedDict, total=False):
    """Energy community (EEG) split the portal adds to consumption responses, null outside an EEG."""

    gridUsageLeftoverValues: list[float | None]
    selfCoverageValues: list[float | None]


class DayResponse(EnergyCommunityValues, total=False):
    """Day endpoint response: 15-min interval data for one day."""

    meteredValues: list[float | None]
    peakDemandTimes: list[str | None]


class WeekResponse(EnergyCommunityValues, total=False):
    """Week endpoint response: daily totals for a date range."""

    meteredValues: list[float | None]


class YearResponse(EnergyCommunityValues, total=False):
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


def describe_response(data: object) -> ResponseShape:
    """Return the structure of a portal response without identifiers.

    Responses carry the meter id, so scalars become their type name and lists of
    readings their length, how many are set, and their min, max and sum. Keys are
    kept: the portal names fields, never customers.
    """
    if data is None:
        return None
    if isinstance(data, dict):
        return {str(key): describe_response(value) for key, value in cast(dict[object, object], data).items()}
    if isinstance(data, list):
        items = cast(list[object], data)
        if any(isinstance(item, dict | list) for item in items):
            return [describe_response(item) for item in items]
        shape = f"{len(items)} values, {sum(item is not None for item in items)} set"
        numbers = [item for item in items if isinstance(item, int | float) and not isinstance(item, bool)]
        if not numbers:
            return shape
        return f"{shape}, min {min(numbers):.3f}, max {max(numbers):.3f}, sum {sum(numbers):.3f}"
    return type(data).__name__


def energy_community_values(response: EnergyCommunityValues) -> tuple[list[float | None], list[float | None]]:
    """Return the community share and remaining grid consumption values, empty if the portal sent none."""
    return response.get("selfCoverageValues") or [], response.get("gridUsageLeftoverValues") or []


def _total_entry(data: object, endpoint: str) -> dict[str, object]:
    """Return the meter's total from a consumption response.

    The portal wraps the result in a list. For a meter in energy communities the
    list has one entry per community and the total has ec_id null; the order is
    not guaranteed, so the total is looked up and the first entry is the fallback.
    """
    if isinstance(data, dict):
        return cast(dict[str, object], data)
    entries = (
        [cast(dict[str, object], e) for e in cast(list[object], data) if isinstance(e, dict)]
        if isinstance(data, list)
        else []
    )
    if not entries:
        raise NetzNoeApiError(f"Unexpected response format from {endpoint}")
    return next((e for e in entries if e.get("ec_id") is None), entries[0])


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
    response_shapes: dict[str, ResponseShape]

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
        self.response_shapes = {}

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
                raise NetzNoeLoginError(f"Login failed with status {resp.status}")
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
                    raise NetzNoeApiError(f"API error {resp.status} for {endpoint}")
                return cast(object, await resp.json())
        except aiohttp.ClientError as err:
            raise NetzNoeApiError(f"Connection error for {endpoint}: {err}") from err

    async def _get_total(self, endpoint: str, params: dict[str, str], *, record_shape: bool) -> dict[str, object]:
        """Fetch a consumption endpoint and return the meter's total.

        With record_shape the response structure is kept for diagnostics and logged
        at debug level.
        """
        data = await self._get(endpoint, params)
        if record_shape:
            shape = describe_response(data)
            self.response_shapes[endpoint] = shape
            _LOGGER.debug("Response structure of %s: %s", endpoint, shape)
        return _total_entry(data, endpoint)

    async def get_day(self, meter_id: str, day: date, *, record_shape: bool = True) -> DayResponse:
        """Fetch 15-min interval data for a single day, recording the structure for diagnostics unless told otherwise.

        Returns dict with:
          meteredValues: list of 96 float (kWh per 15-min)
          peakDemandTimes: list of 96 ISO timestamps
          meteredPeakDemands: list of 96 float (kW)
        """
        params = {
            "meterId": meter_id,
            "day": day.isoformat(),
        }
        return cast(DayResponse, cast(object, await self._get_total(ENDPOINT_DAY, params, record_shape=record_shape)))

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

    async def get_week(
        self, meter_id: str, start_date: date, end_date: date, *, record_shape: bool = True
    ) -> WeekResponse:
        """Fetch daily totals for a date range (Week endpoint), end_date exclusive.

        Records the structure for diagnostics unless told otherwise.

        Returns dict with:
          meteredValues: list of float|None (kWh per day)
          peakDemandTimes: list of ISO timestamps
        """
        params = {
            "meterId": meter_id,
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
        }
        return cast(WeekResponse, cast(object, await self._get_total(ENDPOINT_WEEK, params, record_shape=record_shape)))

    async def get_year(self, meter_id: str, year: int, *, record_shape: bool = True) -> YearResponse:
        """Fetch monthly totals for a year, recording the structure for diagnostics unless told otherwise.

        Returns dict with:
          values: list of 12 float|None (kWh per month)
          peakDemands: list of 12 float|None
        """
        params = {
            "meterId": meter_id,
            "year": str(year),
        }
        return cast(YearResponse, cast(object, await self._get_total(ENDPOINT_YEAR, params, record_shape=record_shape)))
