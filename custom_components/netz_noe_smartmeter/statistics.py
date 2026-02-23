"""Statistics import for Netz NÖ Smart Meter."""

from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
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
from homeassistant.core import HomeAssistant
from homeassistant.helpers.recorder import get_instance
from homeassistant.util import dt as dt_util

from .api import NetzNoeApi, NetzNoeApiError, NetzNoeRateLimitError, RawRecord
from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

STATISTIC_ID = f"{DOMAIN}:grid_consumption"
STATISTIC_NAME = "Smart Meter Netzverbrauch"

# Backfill settings
BACKFILL_CHUNK_DAYS = 30
BACKFILL_CHUNK_MAX_RETRIES = 3
BACKFILL_CLEAR_CONFIRM_TIMEOUT_SECONDS = 15
BACKFILL_DELAY_SECONDS = 3
BACKFILL_MAX_DELAY_SECONDS = 120


class BackfillResult(TypedDict, total=False):
    """Return type of async_backfill_statistics."""

    error: str
    total_entries: int
    total_15min_intervals: int
    total_kwh: float
    start: str
    end: str
    chunks_fetched: int


def _build_metadata() -> StatisticMetaData:
    """Build the statistics metadata (shared between incremental and backfill)."""
    return StatisticMetaData(
        source=DOMAIN,
        statistic_id=STATISTIC_ID,
        name=STATISTIC_NAME,
        unit_of_measurement="kWh",
        has_mean=False,
        has_sum=True,
        mean_type=StatisticMeanType.NONE,
        unit_class="energy",
    )


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


async def _get_last_sum_and_end(
    hass: HomeAssistant,
) -> tuple[Decimal, datetime | None]:
    """Query the last imported statistic to get the running sum and end time."""
    last_stats = await get_instance(hass).async_add_executor_job(
        get_last_statistics, hass, 1, STATISTIC_ID, True, {"sum", "state"}
    )

    running_sum = Decimal(0)
    last_end: datetime | None = None

    if last_stats and STATISTIC_ID in last_stats and last_stats[STATISTIC_ID]:
        last = last_stats[STATISTIC_ID][0]
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
    raw_records: list[RawRecord],
) -> None:
    """Import raw 15-min data as hourly statistics (incremental, called by coordinator)."""
    if not raw_records:
        return

    running_sum, last_end = await _get_last_sum_and_end(hass)
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

    metadata = _build_metadata()
    statistics: list[StatisticData] = []
    for ts in sorted(hourly.keys()):
        running_sum += hourly[ts]
        statistics.append(
            StatisticData(
                start=ts,
                state=float(hourly[ts]),
                sum=float(running_sum),
            )
        )

    _LOGGER.info(
        "Importing %d statistics entries (%s to %s, sum=%.3f kWh)",
        len(statistics),
        statistics[0]["start"],
        statistics[-1]["start"],
        float(running_sum),
    )
    async_add_external_statistics(hass, metadata, statistics)


async def _detect_earliest_data(
    api: NetzNoeApi,
    meter_id: str,
) -> tuple[int, int] | None:
    """Auto-detect the earliest year/month with data via the Year endpoint.

    Returns (year, month) or None if no data found.
    """
    delay = BACKFILL_DELAY_SECONDS
    for year in range(2015, date.today().year + 1):
        try:
            year_data = await api.get_year(meter_id, year)
            monthly = year_data.get("values", [])
            for month_idx, val in enumerate(monthly):
                if val is not None and val > 0:
                    return year, month_idx + 1
            delay = BACKFILL_DELAY_SECONDS  # reset on success
        except NetzNoeRateLimitError:
            delay = min(delay * 2, BACKFILL_MAX_DELAY_SECONDS)
            _LOGGER.warning("Rate limited during auto-detect, backing off %ds", delay)
        except NetzNoeApiError:
            pass
        await asyncio.sleep(delay)
    return None


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
    _LOGGER.info("Starting historical backfill for meter %s", meter_id)

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
    delay = BACKFILL_DELAY_SECONDS

    while chunk_start < end_date:
        chunk_end = min(chunk_start + timedelta(days=BACKFILL_CHUNK_DAYS), end_date)
        attempt = 0

        while True:
            try:
                records = await api.get_raw(meter_id, chunk_start, chunk_end)
                chunk_intervals = _parse_raw_to_intervals(records)
                all_intervals.update(chunk_intervals)

                chunk_count += 1
                delay = BACKFILL_DELAY_SECONDS  # reset on success
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
                break
            except NetzNoeRateLimitError:
                attempt += 1
                delay = min(delay * 2, BACKFILL_MAX_DELAY_SECONDS)
                _LOGGER.warning(
                    "Rate limited during backfill chunk %s to %s (attempt %d/%d), backing off %ds",
                    chunk_start,
                    chunk_end,
                    attempt,
                    BACKFILL_CHUNK_MAX_RETRIES,
                    delay,
                )
            except NetzNoeApiError as err:
                attempt += 1
                delay = min(delay * 2, BACKFILL_MAX_DELAY_SECONDS)
                _LOGGER.warning(
                    "Backfill chunk %s to %s failed (attempt %d/%d): %s",
                    chunk_start,
                    chunk_end,
                    attempt,
                    BACKFILL_CHUNK_MAX_RETRIES,
                    err,
                )

            if attempt >= BACKFILL_CHUNK_MAX_RETRIES:
                error = (
                    f"Backfill failed for chunk {chunk_start} to {chunk_end} after {BACKFILL_CHUNK_MAX_RETRIES} retries"
                )
                _LOGGER.error("%s", error)
                return BackfillResult(error=error)

            await asyncio.sleep(delay)

        chunk_start = chunk_end
        await asyncio.sleep(delay)

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
    metadata = _build_metadata()
    statistics: list[StatisticData] = []
    running_sum = Decimal(0)
    for ts in sorted(hourly.keys()):
        running_sum += hourly[ts]
        statistics.append(
            StatisticData(
                start=ts,
                state=float(hourly[ts]),
                sum=float(running_sum),
            )
        )

    # Clear any existing statistics to avoid sum discontinuities with
    # entries written by the incremental import before backfill ran.
    instance = get_instance(hass)
    done_event = asyncio.Event()

    if progress_callback:
        progress_callback(
            "clearing",
            chunk_count,
            estimated_chunks,
            len(all_intervals),
            "Clearing existing statistics",
        )

    def _clear_done() -> None:
        _ = hass.loop.call_soon_threadsafe(done_event.set)

    instance.async_clear_statistics([STATISTIC_ID], on_done=_clear_done)
    await instance.async_block_till_done()
    try:
        async with asyncio.timeout(BACKFILL_CLEAR_CONFIRM_TIMEOUT_SECONDS):
            _ = await done_event.wait()
    except TimeoutError:
        error = f"Failed to clear existing statistics for {STATISTIC_ID}"
        _LOGGER.error("%s", error)
        return BackfillResult(error=error)

    _LOGGER.info("Cleared existing statistics for %s", STATISTIC_ID)

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
