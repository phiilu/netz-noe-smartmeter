"""Statistics import for Netz NÖ Smart Meter."""

from __future__ import annotations

import asyncio
import calendar
import logging
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from functools import partial
from typing import TypedDict, cast

from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMeanType,
    StatisticMetaData,
)
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    get_last_statistics,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.recorder import get_instance
from homeassistant.util import dt as dt_util
from homeassistant.util import slugify

from .api import (
    EnergyCommunityValues,
    NetzNoeApi,
    NetzNoeApiError,
    NetzNoeAuthError,
    NetzNoeLoginError,
    NetzNoeMaintenanceError,
    NetzNoeRateLimitError,
    RawRecord,
    energy_community_values,
)
from .const import CONF_STATISTIC_ID, DOMAIN, NetzNoeConfigData

_LOGGER = logging.getLogger(__name__)

# Backfill settings
BACKFILL_CHUNK_DAYS = 30
BACKFILL_CHUNK_MAX_RETRIES = 3
BACKFILL_CLEAR_CONFIRM_TIMEOUT_SECONDS = 15
BACKFILL_DELAY_SECONDS = 3
BACKFILL_MAX_DELAY_SECONDS = 120
# The portal has no data before this year
EARLIEST_DATA_YEAR = 2015

# Days the energy community import looks back for days it has not imported yet
ENERGY_COMMUNITY_CATCH_UP_DAYS = 5


class EnergyCommunityBackfillResult(TypedDict, total=False):
    """Return type of async_backfill_energy_community_statistics."""

    error: str
    total_entries: int
    days_fetched: int
    self_coverage_kwh: float
    grid_leftover_kwh: float
    start: str
    end: str


class BackfillResult(TypedDict, total=False):
    """Return type of async_backfill_statistics."""

    error: str
    total_entries: int
    total_15min_intervals: int
    total_kwh: float
    start: str
    end: str
    chunks_fetched: int


@dataclass(frozen=True)
class EnergyCommunityDay:
    """The 15-min energy community split of one day, in the portal's order from local midnight."""

    day: date
    self_coverage: list[float | None]
    grid_leftover: list[float | None]


@dataclass(frozen=True)
class _EnergyCommunitySeries:
    """One energy community statistic: its ID suffix, name suffix and the values it takes from a day."""

    key: str
    name: str
    values: Callable[[EnergyCommunityDay], list[float | None]]


ENERGY_COMMUNITY_SERIES: tuple[_EnergyCommunitySeries, ...] = (
    _EnergyCommunitySeries("self_coverage", "Eigendeckung", lambda day: day.self_coverage),
    _EnergyCommunitySeries("grid_leftover", "Restnetzbezug", lambda day: day.grid_leftover),
)


def energy_community_day(day: date, response: EnergyCommunityValues) -> EnergyCommunityDay | None:
    """Return the day's energy community split, or None while the portal has none for it."""
    self_coverage, grid_leftover = energy_community_values(response)
    if all(value is None for value in (*self_coverage, *grid_leftover)):
        return None
    return EnergyCommunityDay(day, self_coverage, grid_leftover)


def statistic_id_for(meter_id: str) -> str:
    """Return the external statistic ID for a meter."""
    return f"{DOMAIN}:{slugify(meter_id)}"


def _metadata(statistic_id: str, name: str) -> StatisticMetaData:
    """Return the metadata of an hourly energy statistic."""
    return StatisticMetaData(
        source=DOMAIN,
        statistic_id=statistic_id,
        name=name,
        unit_of_measurement="kWh",
        has_mean=False,
        has_sum=True,
        mean_type=StatisticMeanType.NONE,
        unit_class="energy",
    )


def _entry_title(entry: ConfigEntry | None, meter_id: str) -> str:
    """Return the title of the meter's config entry, the name of its statistics."""
    return entry.title if entry else f"Smart Meter {meter_id}"


def _build_metadata(hass: HomeAssistant, meter_id: str) -> StatisticMetaData:
    """Build the statistics metadata (shared between incremental and backfill).

    The statistic ID is stored in the config entry, since a meter set up before
    0.2.0 keeps the statistic all meters shared back then. The name is the entry
    title, so renamed entries (e.g. "Bezug") show up in the Energy Dashboard.
    """
    entry = hass.config_entries.async_entry_for_domain_unique_id(DOMAIN, meter_id)
    statistic_id = statistic_id_for(meter_id)
    if entry:
        statistic_id = cast(NetzNoeConfigData, cast(object, entry.data)).get(CONF_STATISTIC_ID, statistic_id)
    return _metadata(statistic_id, _entry_title(entry, meter_id))


def _energy_community_metadata(hass: HomeAssistant, meter_id: str) -> list[StatisticMetaData]:
    """Build the metadata of the energy community statistics, in ENERGY_COMMUNITY_SERIES order.

    The IDs come from the meter id, not the stored statistic ID: a meter that kept
    the statistic all meters shared before 0.2.0 must not share these.
    """
    title = _entry_title(hass.config_entries.async_entry_for_domain_unique_id(DOMAIN, meter_id), meter_id)
    return [
        _metadata(f"{statistic_id_for(meter_id)}_{series.key}", f"{title} {series.name}")
        for series in ENERGY_COMMUNITY_SERIES
    ]


def _parse_raw_to_intervals(
    raw_records: list[RawRecord],
    skip_before: datetime | None = None,
) -> dict[datetime, Decimal]:
    """Parse raw 15-min records into interval-start keyed values.

    Endtime is the END of the 15-min interval, so subtract 15 min to get
    the interval start. Each entry is kept at 15-min resolution.
    """
    intervals: dict[datetime, Decimal] = {}
    for record in raw_records:
        value = record.get("MeasuredValue")
        endtime = record.get("Endtime")
        if value is None or endtime is None:
            continue

        ts = dt_util.parse_datetime(endtime)
        if ts is None:
            continue

        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)

        if skip_before and ts <= skip_before:
            continue

        # e.g. Endtime 10:15 -> interval start 10:00
        # e.g. Endtime 11:00 -> interval start 10:45
        interval_start = ts - timedelta(minutes=15)
        intervals[interval_start] = Decimal(str(value))

    return intervals


def _energy_community_intervals(
    days: list[EnergyCommunityDay],
    series: _EnergyCommunitySeries,
    skip_before: datetime | None = None,
) -> dict[datetime, Decimal]:
    """Return a series' 15-min values keyed by interval start (UTC).

    The portal lists a day's values from local midnight, so the position gives
    the interval, also on the days the clocks change.
    """
    intervals: dict[datetime, Decimal] = {}
    for day in days:
        day_start = dt_util.as_utc(dt_util.start_of_local_day(day.day))
        for index, value in enumerate(series.values(day)):
            interval_start = day_start + timedelta(minutes=15 * index)
            if value is not None and (skip_before is None or interval_start >= skip_before):
                intervals[interval_start] = Decimal(str(value))
    return intervals


def _aggregate_to_hourly(
    intervals: dict[datetime, Decimal],
) -> dict[datetime, Decimal]:
    """Aggregate 15-min intervals into hourly sums.

    HA statistics require timestamps at the top of the hour (minute=0, second=0).
    Each hour bucket sums the up-to-4 fifteen-minute intervals within it.
    E.g. 10:00, 10:15, 10:30, 10:45 all map to the 10:00 hour.
    """
    hourly: dict[datetime, Decimal] = {}
    for ts, value in intervals.items():
        hour_start = ts.replace(minute=0, second=0, microsecond=0)
        hourly[hour_start] = hourly.get(hour_start, Decimal(0)) + value
    return hourly


def _cumulative_statistics(
    hourly: dict[datetime, Decimal], running_sum: Decimal
) -> tuple[list[StatisticData], Decimal]:
    """Return the hourly rows in order with their running sum, and the final sum."""
    statistics: list[StatisticData] = []
    for ts in sorted(hourly):
        running_sum += hourly[ts]
        statistics.append(StatisticData(start=ts, state=float(hourly[ts]), sum=float(running_sum)))
    return statistics, running_sum


async def _get_last_sum_and_end(
    hass: HomeAssistant,
    statistic_id: str,
) -> tuple[Decimal, datetime | None]:
    """Query the last imported statistic to get the running sum and end time."""
    last_stats = await get_instance(hass).async_add_executor_job(
        get_last_statistics, hass, 1, statistic_id, True, {"sum", "state"}
    )

    running_sum = Decimal(0)
    last_end: datetime | None = None

    if last_stats and last_stats.get(statistic_id):
        last = last_stats[statistic_id][0]
        sum_val = last.get("sum")
        if sum_val is not None:
            running_sum = Decimal(str(sum_val))
        end_val = cast(object, last.get("end"))
        if isinstance(end_val, int | float):
            last_end = dt_util.utc_from_timestamp(end_val)
        elif isinstance(end_val, datetime):
            last_end = end_val

    return running_sum, last_end


async def async_import_raw_statistics(
    hass: HomeAssistant,
    meter_id: str,
    raw_records: list[RawRecord],
) -> None:
    """Import raw 15-min data as hourly statistics (incremental, called by coordinator)."""
    if not raw_records:
        return

    metadata = _build_metadata(hass, meter_id)
    running_sum, last_end = await _get_last_sum_and_end(hass, metadata["statistic_id"])
    if last_end:
        _LOGGER.debug(
            "Resuming statistics import: last_sum=%.3f, last_end=%s",
            running_sum,
            last_end,
        )

    intervals = _parse_raw_to_intervals(raw_records, skip_before=last_end)
    if not intervals:
        _LOGGER.debug("No new data to import")
        return

    hourly = _aggregate_to_hourly(intervals)
    _LOGGER.debug(
        "Aggregated %d 15-min intervals into %d hourly buckets",
        len(intervals),
        len(hourly),
    )

    statistics, running_sum = _cumulative_statistics(hourly, running_sum)

    _LOGGER.info(
        "Importing %d statistics entries (%s to %s, sum=%.3f kWh)",
        len(statistics),
        statistics[0]["start"],
        statistics[-1]["start"],
        float(running_sum),
    )
    async_add_external_statistics(hass, metadata, statistics)


async def async_import_energy_community_statistics(
    hass: HomeAssistant,
    meter_id: str,
    newest: EnergyCommunityDay,
    fetch_day: Callable[[date], Awaitable[EnergyCommunityDay | None]],
) -> None:
    """Import the energy community split up to the newest day as hourly statistics (incremental).

    The portal publishes the split later than the consumption, so the import
    resumes after the last imported hour and stops at the first day without a
    split; that day is fetched again on the next poll. fetch_day gets the days
    missing before the newest one, at most ENERGY_COMMUNITY_CATCH_UP_DAYS back.
    """
    metadata = _energy_community_metadata(hass, meter_id)
    last_stats = await asyncio.gather(*(_get_last_sum_and_end(hass, meta["statistic_id"]) for meta in metadata))

    days: list[EnergyCommunityDay] = []
    if all(last_end is not None for _, last_end in last_stats):
        resume = dt_util.as_local(min(cast(datetime, last_end) for _, last_end in last_stats)).date()
        current = max(resume, newest.day - timedelta(days=ENERGY_COMMUNITY_CATCH_UP_DAYS - 1))
        while current < newest.day:
            if (day := await fetch_day(current)) is None:
                _LOGGER.debug("No energy community data for %s yet", current)
                return
            days.append(day)
            current += timedelta(days=1)
    days.append(newest)

    for series, meta, (running_sum, last_end) in zip(ENERGY_COMMUNITY_SERIES, metadata, last_stats, strict=True):
        intervals = _energy_community_intervals(days, series, skip_before=last_end)
        if not intervals:
            continue
        statistics, running_sum = _cumulative_statistics(_aggregate_to_hourly(intervals), running_sum)
        _LOGGER.debug(
            "Importing %d %s statistics entries (%s to %s, sum=%.3f kWh)",
            len(statistics),
            series.key,
            statistics[0]["start"],
            statistics[-1]["start"],
            float(running_sum),
        )
        async_add_external_statistics(hass, meta, statistics)


async def async_apply_statistic_names(hass: HomeAssistant, meter_id: str) -> None:
    """Write the metadata now, so a renamed entry shows its new name right away.

    An import without rows only updates the metadata. Otherwise the name would
    change with the next import that has new data, up to a day later. The
    energy community statistics are only written if they exist, so meters
    outside an energy community don't get empty ones.
    """
    async_add_external_statistics(hass, _build_metadata(hass, meter_id), [])
    for meta in _energy_community_metadata(hass, meter_id):
        if await async_statistic_has_data(hass, meta["statistic_id"]):
            async_add_external_statistics(hass, meta, [])


async def async_statistic_has_data(hass: HomeAssistant, statistic_id: str) -> bool:
    """Return whether the statistic has at least one row."""
    _, last_end = await _get_last_sum_and_end(hass, statistic_id)
    return last_end is not None


async def _async_statistic_diagnostics(hass: HomeAssistant, statistic_id: str) -> dict[str, object]:
    """Return the ID, last sum and end of a statistic."""
    last_sum, last_end = await _get_last_sum_and_end(hass, statistic_id)
    return {
        "statistic_id": statistic_id,
        "last_sum": float(last_sum),
        "last_end": last_end.isoformat() if last_end else None,
    }


async def async_get_statistics_diagnostics(hass: HomeAssistant, meter_id: str) -> dict[str, object]:
    """Return the ID, last sum and end of the meter's statistic and its energy community statistics."""
    meter, *energy_community = await asyncio.gather(
        _async_statistic_diagnostics(hass, _build_metadata(hass, meter_id)["statistic_id"]),
        *(
            _async_statistic_diagnostics(hass, meta["statistic_id"])
            for meta in _energy_community_metadata(hass, meter_id)
        ),
    )
    return {
        **meter,
        "energy_community": {
            series.key: diagnostics
            for series, diagnostics in zip(ENERGY_COMMUNITY_SERIES, energy_community, strict=True)
        },
    }


async def _detect_earliest_data(
    api: NetzNoeApi,
    meter_id: str,
) -> tuple[int, int] | None:
    """Auto-detect the earliest year/month with data via the Year endpoint.

    Returns (year, month) or None if no data found.
    """
    delay = BACKFILL_DELAY_SECONDS
    for year in range(EARLIEST_DATA_YEAR, date.today().year + 1):
        try:
            year_data = await api.get_year(meter_id, year, record_shape=False)
            monthly = year_data.get("values") or []
            for month_idx, val in enumerate(monthly):
                if val is not None and val > 0:
                    return year, month_idx + 1
            delay = BACKFILL_DELAY_SECONDS  # reset on success
        except (NetzNoeLoginError, NetzNoeMaintenanceError):
            raise
        except NetzNoeRateLimitError:
            delay = min(delay * 2, BACKFILL_MAX_DELAY_SECONDS)
            _LOGGER.warning("Rate limited during auto-detect, backing off %ds", delay)
        except NetzNoeApiError:
            pass
        await asyncio.sleep(delay)
    return None


async def _fetch_with_retries[T](
    fetch: Callable[[], Awaitable[T]],
    label: str,
    fatal: tuple[type[NetzNoeApiError], ...] = (),
) -> T | None:
    """Call fetch, retrying API errors with a growing delay; None once BACKFILL_CHUNK_MAX_RETRIES attempts failed.

    Errors in fatal are raised, and so are rejected credentials: retrying them could lock the account.
    """
    delay = BACKFILL_DELAY_SECONDS
    for attempt in range(1, BACKFILL_CHUNK_MAX_RETRIES + 1):
        try:
            return await fetch()
        except (NetzNoeAuthError, *fatal):
            raise
        except NetzNoeApiError as err:
            delay = min(delay * 2, BACKFILL_MAX_DELAY_SECONDS)
            _LOGGER.warning(
                "Backfill of %s failed (attempt %d/%d), backing off %ds: %s",
                label,
                attempt,
                BACKFILL_CHUNK_MAX_RETRIES,
                delay,
                err,
            )
        if attempt < BACKFILL_CHUNK_MAX_RETRIES:
            await asyncio.sleep(delay)
    return None


async def _async_clear_statistics(hass: HomeAssistant, statistic_ids: list[str]) -> bool:
    """Clear statistics and wait until the recorder confirms; False if it did not in time."""
    instance = get_instance(hass)
    done_event = asyncio.Event()

    def _clear_done() -> None:
        _ = hass.loop.call_soon_threadsafe(done_event.set)

    instance.async_clear_statistics(statistic_ids, on_done=_clear_done)
    await instance.async_block_till_done()
    try:
        async with asyncio.timeout(BACKFILL_CLEAR_CONFIRM_TIMEOUT_SECONDS):
            _ = await done_event.wait()
    except TimeoutError:
        return False
    return True


async def async_backfill_statistics(
    hass: HomeAssistant,
    api: NetzNoeApi,
    meter_id: str,
    start_year: int | None = None,
    progress_callback: Callable[[str, int, int, int, str], None] | None = None,
) -> BackfillResult:
    """Backfill historical data as 15-min statistics.

    Fetches raw data in 30-day chunks with 3-second delays between
    API calls to avoid rate limits.

    Args:
        start_year: Year to start from. If None, auto-detects the earliest
                    year with data via the Year endpoint.
        progress_callback: Called after each chunk with
            (state, current_chunk, estimated_total, records_so_far, date_range_str).

    Returns a summary dict with total_intervals, total_kwh, date_range.
    """
    _LOGGER.info("Starting historical backfill for meter ending in %s", meter_id[-4:])

    if start_year is not None:
        start_date = date(start_year, 1, 1)
        _LOGGER.info("Using provided start year: %d", start_year)
    else:
        _LOGGER.info("Auto-detecting earliest available data...")
        result = await _detect_earliest_data(api, meter_id)
        if result is None:
            _LOGGER.warning("No historical data found")
            return BackfillResult(error="No historical data found")
        start_date = date(result[0], result[1], 1)

    end_date = date.today()
    total_days = (end_date - start_date).days
    estimated_chunks = max(1, math.ceil(total_days / BACKFILL_CHUNK_DAYS))
    _LOGGER.info("Backfilling from %s to %s", start_date, end_date)

    # Collect all 15-min intervals in chunks
    all_intervals: dict[datetime, Decimal] = {}
    chunk_start = start_date
    chunk_count = 0

    while chunk_start < end_date:
        chunk_end = min(chunk_start + timedelta(days=BACKFILL_CHUNK_DAYS), end_date)
        records = await _fetch_with_retries(
            partial(api.get_raw, meter_id, chunk_start, chunk_end), f"chunk {chunk_start} to {chunk_end}"
        )
        if records is None:
            error = f"Backfill failed for chunk {chunk_start} to {chunk_end} after {BACKFILL_CHUNK_MAX_RETRIES} retries"
            _LOGGER.error("%s", error)
            return BackfillResult(error=error)

        chunk_intervals = _parse_raw_to_intervals(records)
        all_intervals.update(chunk_intervals)
        chunk_count += 1
        _LOGGER.info(
            "Backfill chunk %d: %s to %s — %d records, %d intervals",
            chunk_count,
            chunk_start,
            chunk_end,
            len(records),
            len(chunk_intervals),
        )
        if progress_callback:
            progress_callback(
                "running",
                chunk_count,
                estimated_chunks,
                len(all_intervals),
                f"{chunk_start} to {chunk_end}",
            )

        chunk_start = chunk_end
        await asyncio.sleep(BACKFILL_DELAY_SECONDS)

    if not all_intervals:
        _LOGGER.warning("Backfill produced no data")
        return BackfillResult(error="No raw data returned")

    # Aggregate 15-min intervals into hourly sums (HA requires top-of-hour timestamps)
    hourly = _aggregate_to_hourly(all_intervals)
    _LOGGER.info(
        "Aggregated %d 15-min intervals into %d hourly buckets",
        len(all_intervals),
        len(hourly),
    )

    # Build statistics with running sum (sorted chronologically)
    metadata = _build_metadata(hass, meter_id)
    statistics, running_sum = _cumulative_statistics(hourly, Decimal(0))

    # Clear any existing statistics to avoid sum discontinuities with
    # entries written by the incremental import before backfill ran.
    if progress_callback:
        progress_callback(
            "clearing",
            chunk_count,
            estimated_chunks,
            len(all_intervals),
            "Clearing existing statistics",
        )

    if not await _async_clear_statistics(hass, [metadata["statistic_id"]]):
        error = "Failed to clear existing statistics"
        _LOGGER.error("%s", error)
        return BackfillResult(error=error)

    _LOGGER.info("Cleared existing statistics")

    _LOGGER.info(
        "Backfill complete: importing %d hourly entries (%s to %s, total=%.3f kWh)",
        len(statistics),
        statistics[0]["start"],
        statistics[-1]["start"],
        float(running_sum),
    )
    async_add_external_statistics(hass, metadata, statistics)

    return BackfillResult(
        total_entries=len(statistics),
        total_15min_intervals=len(all_intervals),
        total_kwh=round(float(running_sum), 3),
        start=str(statistics[0]["start"]),
        end=str(statistics[-1]["start"]),
        chunks_fetched=chunk_count,
    )


async def _energy_community_backfill_days(
    api: NetzNoeApi, meter_id: str, today: date, start_year: int | None
) -> list[date]:
    """Return the days up to yesterday of the months with an energy community split, oldest first.

    Walks back the Year endpoint from the current year until a year has no
    split, or to start_year. The current year has none yet in its first days.
    Like the year detection of the consumption backfill, a failed login or
    maintenance aborts; other errors are retried and then raise.
    """
    days: list[date] = []
    for year in range(today.year, max(start_year or EARLIEST_DATA_YEAR, EARLIEST_DATA_YEAR) - 1, -1):
        response = await _fetch_with_retries(
            partial(api.get_year, meter_id, year, record_shape=False),
            f"energy community year {year}",
            fatal=(NetzNoeLoginError, NetzNoeMaintenanceError),
        )
        if response is None:
            raise NetzNoeApiError("Energy community backfill failed to find the months with data")
        values = energy_community_values(response)
        months = [
            month
            for month in range(1, 13)
            if any(value is not None for series in values for value in series[month - 1 : month])
        ]
        if not months and year < today.year:
            break
        days[:0] = [
            day
            for month in months
            for day in (date(year, month, number) for number in range(1, calendar.monthrange(year, month)[1] + 1))
            if day < today
        ]
        await asyncio.sleep(BACKFILL_DELAY_SECONDS)
    return days


async def async_backfill_energy_community_statistics(
    hass: HomeAssistant,
    api: NetzNoeApi,
    meter_id: str,
    start_year: int | None = None,
    progress_callback: Callable[[str, int, int, int, str], None] | None = None,
) -> EnergyCommunityBackfillResult:
    """Backfill the energy community split as hourly statistics.

    Only the Day endpoint has the 15-min split, so this makes one request per
    day of the months the Year endpoint shows a split for, up to yesterday.
    progress_callback gets (state, day number, number of days, 15-min values
    so far, day).
    """
    _LOGGER.info("Starting energy community backfill for meter ending in %s", meter_id[-4:])
    days = await _energy_community_backfill_days(api, meter_id, dt_util.now().date(), start_year)

    fetched: list[EnergyCommunityDay] = []
    values = 0
    for number, day in enumerate(days, start=1):
        response = await _fetch_with_retries(partial(api.get_day, meter_id, day, record_shape=False), str(day))
        if response is None:
            error = f"Energy community backfill failed for {day} after {BACKFILL_CHUNK_MAX_RETRIES} retries"
            _LOGGER.error("%s", error)
            return EnergyCommunityBackfillResult(error=error)
        if (ec_day := energy_community_day(day, response)) is not None:
            fetched.append(ec_day)
            values += sum(value is not None for value in ec_day.self_coverage)
        if progress_callback:
            progress_callback("running", number, len(days), values, str(day))
        await asyncio.sleep(BACKFILL_DELAY_SECONDS)

    metadata = _energy_community_metadata(hass, meter_id)
    imports = {
        series.key: _cumulative_statistics(
            _aggregate_to_hourly(_energy_community_intervals(fetched, series)), Decimal(0)
        )
        for series in ENERGY_COMMUNITY_SERIES
    }
    hours = sorted({row["start"] for statistics, _ in imports.values() for row in statistics})
    if not hours:
        _LOGGER.warning("No energy community data found")
        return EnergyCommunityBackfillResult(error="No energy community data found")

    if progress_callback:
        progress_callback("clearing", len(days), len(days), values, "Clearing existing statistics")
    if not await _async_clear_statistics(hass, [meta["statistic_id"] for meta in metadata]):
        error = "Failed to clear existing energy community statistics"
        _LOGGER.error("%s", error)
        return EnergyCommunityBackfillResult(error=error)

    for series, meta in zip(ENERGY_COMMUNITY_SERIES, metadata, strict=True):
        if statistics := imports[series.key][0]:
            async_add_external_statistics(hass, meta, statistics)
    _LOGGER.info("Energy community backfill complete: %d hours (%s to %s)", len(hours), hours[0], hours[-1])
    return EnergyCommunityBackfillResult(
        total_entries=len(hours),
        days_fetched=len(days),
        self_coverage_kwh=round(float(imports["self_coverage"][1]), 3),
        grid_leftover_kwh=round(float(imports["grid_leftover"][1]), 3),
        start=str(hours[0]),
        end=str(hours[-1]),
    )
