"""Tests for statistics parsing and aggregation logic."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.core import HomeAssistant

from custom_components.netz_noe_smartmeter.api import NetzNoeApiError, NetzNoeRateLimitError, RawRecord
from custom_components.netz_noe_smartmeter.statistics import (
    STATISTIC_ID,
    _aggregate_to_hourly,
    _build_metadata,
    _detect_earliest_data,
    _get_last_sum_and_end,
    _parse_raw_to_intervals,
    async_backfill_statistics,
    async_import_raw_statistics,
)

# ---------------------------------------------------------------------------
# _build_metadata
# ---------------------------------------------------------------------------


def test_build_metadata() -> None:
    """Test that metadata is built correctly."""
    meta = _build_metadata()
    assert meta["source"] == "netz_noe_smartmeter"
    assert meta["statistic_id"] == "netz_noe_smartmeter:grid_consumption"
    assert meta["has_sum"] is True
    assert meta["unit_of_measurement"] == "kWh"


# ---------------------------------------------------------------------------
# _parse_raw_to_intervals
# ---------------------------------------------------------------------------


def test_parse_raw_to_intervals_basic() -> None:
    """Test parsing raw records into 15-min intervals."""
    records: list[RawRecord] = [
        {"MeasuredValue": 0.5, "Endtime": "2025-01-15T10:15:00+01:00"},
        {"MeasuredValue": 0.3, "Endtime": "2025-01-15T10:30:00+01:00"},
    ]
    intervals = _parse_raw_to_intervals(records)

    assert len(intervals) == 2
    # Values are Decimal
    for val in intervals.values():
        assert isinstance(val, Decimal)


def test_parse_raw_skips_none_values() -> None:
    """Test that records with None values are skipped."""
    records: list[RawRecord] = [
        {"MeasuredValue": None, "Endtime": "2025-01-15T10:15:00+01:00"},
        {"MeasuredValue": 0.5, "Endtime": None},
        {"MeasuredValue": 0.3, "Endtime": "2025-01-15T10:30:00+01:00"},
    ]
    intervals = _parse_raw_to_intervals(records)
    assert len(intervals) == 1


def test_parse_raw_skip_before() -> None:
    """Test that records before skip_before are excluded."""
    records: list[RawRecord] = [
        {"MeasuredValue": 0.5, "Endtime": "2025-01-15T10:15:00+00:00"},
        {"MeasuredValue": 0.3, "Endtime": "2025-01-15T10:30:00+00:00"},
        {"MeasuredValue": 0.7, "Endtime": "2025-01-15T11:00:00+00:00"},
    ]
    skip = datetime(2025, 1, 15, 10, 30, tzinfo=UTC)
    intervals = _parse_raw_to_intervals(records, skip_before=skip)

    # Only the 11:00 record should remain (10:15 and 10:30 are <= skip_before)
    assert len(intervals) == 1


def test_parse_raw_naive_timestamps_get_utc() -> None:
    """Test that naive timestamps get UTC timezone."""
    records: list[RawRecord] = [
        {"MeasuredValue": 0.5, "Endtime": "2025-01-15T10:15:00"},
    ]
    intervals = _parse_raw_to_intervals(records)
    assert len(intervals) == 1
    ts = next(iter(intervals.keys()))
    assert ts.tzinfo is not None


def test_parse_raw_unparseable_datetime() -> None:
    """Test that records with unparseable datetime strings are skipped."""
    records: list[RawRecord] = [
        {"MeasuredValue": 0.5, "Endtime": "not-a-datetime"},
        {"MeasuredValue": 0.3, "Endtime": "2025-01-15T10:30:00+01:00"},
    ]
    intervals = _parse_raw_to_intervals(records)
    assert len(intervals) == 1


# ---------------------------------------------------------------------------
# _aggregate_to_hourly
# ---------------------------------------------------------------------------


def test_aggregate_to_hourly() -> None:
    """Test aggregating 15-min intervals into hourly sums."""
    # Four 15-min intervals in the same hour
    base = datetime(2025, 1, 15, 10, 0, tzinfo=UTC)
    intervals = {
        base: Decimal("0.1"),
        base + timedelta(minutes=15): Decimal("0.2"),
        base + timedelta(minutes=30): Decimal("0.3"),
        base + timedelta(minutes=45): Decimal("0.4"),
    }
    hourly = _aggregate_to_hourly(intervals)

    assert len(hourly) == 1
    assert hourly[base] == Decimal("1.0")


def test_aggregate_to_hourly_multiple_hours() -> None:
    """Test aggregation across multiple hours."""
    intervals = {
        datetime(2025, 1, 15, 10, 0, tzinfo=UTC): Decimal("0.5"),
        datetime(2025, 1, 15, 10, 15, tzinfo=UTC): Decimal("0.5"),
        datetime(2025, 1, 15, 11, 0, tzinfo=UTC): Decimal("1.0"),
    }
    hourly = _aggregate_to_hourly(intervals)

    assert len(hourly) == 2
    assert hourly[datetime(2025, 1, 15, 10, 0, tzinfo=UTC)] == Decimal("1.0")
    assert hourly[datetime(2025, 1, 15, 11, 0, tzinfo=UTC)] == Decimal("1.0")


def test_aggregate_empty() -> None:
    """Test aggregation of empty input."""
    assert _aggregate_to_hourly({}) == {}


# ---------------------------------------------------------------------------
# _get_last_sum_and_end
# ---------------------------------------------------------------------------

PATCH_GET_INSTANCE = "custom_components.netz_noe_smartmeter.statistics.get_instance"


async def test_get_last_sum_and_end_no_stats(hass: HomeAssistant) -> None:
    """Test returns (0, None) when no existing statistics."""
    with patch(PATCH_GET_INSTANCE) as mock_inst:
        mock_inst.return_value.async_add_executor_job = AsyncMock(return_value={})
        running_sum, last_end = await _get_last_sum_and_end(hass)

    assert running_sum == Decimal(0)
    assert last_end is None


async def test_get_last_sum_and_end_with_float_end(hass: HomeAssistant) -> None:
    """Test extracts sum and converts float timestamp to datetime."""
    last_stats = {
        STATISTIC_ID: [{"sum": 123.456, "end": 1736899200.0}],  # 2025-01-15 00:00 UTC
    }
    with patch(PATCH_GET_INSTANCE) as mock_inst:
        mock_inst.return_value.async_add_executor_job = AsyncMock(return_value=last_stats)
        running_sum, last_end = await _get_last_sum_and_end(hass)

    assert running_sum == Decimal("123.456")
    assert last_end is not None
    assert isinstance(last_end, datetime)


async def test_get_last_sum_and_end_with_datetime_end(hass: HomeAssistant) -> None:
    """Test handles end value that's already a datetime object."""
    end_dt = datetime(2025, 1, 15, 10, 0, tzinfo=UTC)
    last_stats = {
        STATISTIC_ID: [{"sum": 50.0, "end": end_dt}],
    }
    with patch(PATCH_GET_INSTANCE) as mock_inst:
        mock_inst.return_value.async_add_executor_job = AsyncMock(return_value=last_stats)
        running_sum, last_end = await _get_last_sum_and_end(hass)

    assert running_sum == Decimal("50.0")
    assert last_end is end_dt


async def test_get_last_sum_and_end_no_sum_value(hass: HomeAssistant) -> None:
    """Test handles entry with no sum (sum is None)."""
    last_stats = {
        STATISTIC_ID: [{"sum": None, "end": 1736899200.0}],
    }
    with patch(PATCH_GET_INSTANCE) as mock_inst:
        mock_inst.return_value.async_add_executor_job = AsyncMock(return_value=last_stats)
        running_sum, last_end = await _get_last_sum_and_end(hass)

    assert running_sum == Decimal(0)
    assert last_end is not None


# ---------------------------------------------------------------------------
# async_import_raw_statistics
# ---------------------------------------------------------------------------

PATCH_ADD_STATS = "custom_components.netz_noe_smartmeter.statistics.async_add_external_statistics"


def _mock_clear_statistics_done(mock_get_instance: MagicMock) -> None:
    """Mock recorder clear statistics and invoke on_done immediately."""

    recorder = MagicMock()
    recorder.async_block_till_done = AsyncMock(return_value=None)

    def _async_clear_statistics(_statistic_ids, *, on_done=None) -> None:
        if on_done:
            on_done()

    recorder.async_clear_statistics.side_effect = _async_clear_statistics
    mock_get_instance.return_value = recorder


async def test_import_raw_empty_records(hass: HomeAssistant) -> None:
    """Test early return on empty records."""
    with patch(PATCH_GET_INSTANCE) as mock_inst:
        mock_inst.return_value.async_add_executor_job = AsyncMock(return_value={})
        with patch(PATCH_ADD_STATS) as mock_add:
            await async_import_raw_statistics(hass, [])

    mock_add.assert_not_called()


async def test_import_raw_success(hass: HomeAssistant) -> None:
    """Test successful import builds hourly stats with running sum."""
    records: list[RawRecord] = [
        {"MeasuredValue": 0.1, "Endtime": "2025-01-15T10:15:00+00:00"},
        {"MeasuredValue": 0.2, "Endtime": "2025-01-15T10:30:00+00:00"},
        {"MeasuredValue": 0.3, "Endtime": "2025-01-15T10:45:00+00:00"},
        {"MeasuredValue": 0.4, "Endtime": "2025-01-15T11:00:00+00:00"},
    ]
    # No existing statistics
    with patch(PATCH_GET_INSTANCE) as mock_inst:
        mock_inst.return_value.async_add_executor_job = AsyncMock(return_value={})
        with patch(PATCH_ADD_STATS) as mock_add:
            await async_import_raw_statistics(hass, records)

    mock_add.assert_called_once()
    _hass, _metadata, statistics = mock_add.call_args[0]
    assert len(statistics) == 1  # All 4 intervals in the 10:xx hour
    assert statistics[0]["sum"] == pytest.approx(1.0)


async def test_import_raw_resumes_from_last(hass: HomeAssistant) -> None:
    """Test import skips already-imported data and resumes sum."""
    records: list[RawRecord] = [
        {"MeasuredValue": 0.5, "Endtime": "2025-01-15T10:15:00+00:00"},
        {"MeasuredValue": 0.3, "Endtime": "2025-01-15T10:30:00+00:00"},
        {"MeasuredValue": 0.7, "Endtime": "2025-01-15T11:15:00+00:00"},
    ]
    # Existing stats: sum=100 up to 10:30
    last_stats = {
        STATISTIC_ID: [
            {"sum": 100.0, "end": datetime(2025, 1, 15, 10, 30, tzinfo=UTC)},
        ],
    }
    with patch(PATCH_GET_INSTANCE) as mock_inst:
        mock_inst.return_value.async_add_executor_job = AsyncMock(return_value=last_stats)
        with patch(PATCH_ADD_STATS) as mock_add:
            await async_import_raw_statistics(hass, records)

    mock_add.assert_called_once()
    _hass, _metadata, statistics = mock_add.call_args[0]
    assert len(statistics) == 1  # Only the 11:xx hour
    assert statistics[0]["sum"] == pytest.approx(100.7)


async def test_import_raw_no_new_data(hass: HomeAssistant) -> None:
    """Test no import when all data already imported."""
    records: list[RawRecord] = [
        {"MeasuredValue": 0.5, "Endtime": "2025-01-15T10:15:00+00:00"},
    ]
    # Already imported up to 11:00 (well past all records)
    last_stats = {
        STATISTIC_ID: [
            {"sum": 50.0, "end": datetime(2025, 1, 15, 11, 0, tzinfo=UTC)},
        ],
    }
    with patch(PATCH_GET_INSTANCE) as mock_inst:
        mock_inst.return_value.async_add_executor_job = AsyncMock(return_value=last_stats)
        with patch(PATCH_ADD_STATS) as mock_add:
            await async_import_raw_statistics(hass, records)

    mock_add.assert_not_called()


# ---------------------------------------------------------------------------
# _detect_earliest_data
# ---------------------------------------------------------------------------


async def test_detect_earliest_data_found() -> None:
    """Test auto-detection finds first year/month with data."""
    api = MagicMock()
    # Year 2023: no data; Year 2024: has data in March
    call_count = 0

    async def _get_year(meter_id: str, year: int) -> dict:
        nonlocal call_count
        call_count += 1
        if year < 2024:
            return {"values": [None] * 12}
        return {"values": [None, None, 5.0, 10.0] + [None] * 8}

    api.get_year = AsyncMock(side_effect=_get_year)

    with (
        patch(
            "custom_components.netz_noe_smartmeter.statistics.asyncio.sleep",
            new_callable=AsyncMock,
        ),
        patch(
            "custom_components.netz_noe_smartmeter.statistics.date",
        ) as mock_date,
    ):
        mock_date.today.return_value = date(2025, 6, 1)
        mock_date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)
        result = await _detect_earliest_data(api, "meter1")

    assert result == (2024, 3)


async def test_detect_earliest_data_not_found() -> None:
    """Test auto-detection returns None when no year has data."""
    api = MagicMock()
    api.get_year = AsyncMock(return_value={"values": [None] * 12})

    with (
        patch(
            "custom_components.netz_noe_smartmeter.statistics.asyncio.sleep",
            new_callable=AsyncMock,
        ),
        patch(
            "custom_components.netz_noe_smartmeter.statistics.date",
        ) as mock_date,
    ):
        mock_date.today.return_value = date(2016, 1, 1)
        mock_date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)
        result = await _detect_earliest_data(api, "meter1")

    assert result is None


async def test_detect_earliest_data_rate_limit_backs_off() -> None:
    """Test that 429 rate limit triggers exponential backoff and retries."""
    api = MagicMock()
    call_count = 0

    async def _get_year(meter_id: str, year: int) -> dict:
        nonlocal call_count
        call_count += 1
        if year == 2015:
            raise NetzNoeRateLimitError("429")
        return {"values": [1.0] + [None] * 11}

    api.get_year = AsyncMock(side_effect=_get_year)

    sleep_delays: list[float] = []

    async def _capture_sleep(delay: float) -> None:
        sleep_delays.append(delay)

    with (
        patch(
            "custom_components.netz_noe_smartmeter.statistics.asyncio.sleep",
            side_effect=_capture_sleep,
        ),
        patch(
            "custom_components.netz_noe_smartmeter.statistics.date",
        ) as mock_date,
    ):
        mock_date.today.return_value = date(2017, 6, 1)
        mock_date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)
        result = await _detect_earliest_data(api, "meter1")

    # Should still find data in 2016 after the 2015 rate limit
    assert result == (2016, 1)
    # After rate limit on 2015, delay should double (3 -> 6)
    assert 6 in sleep_delays


async def test_detect_earliest_data_api_error_skips() -> None:
    """Test that API errors for individual years are skipped."""
    api = MagicMock()
    call_count = 0

    async def _get_year(meter_id: str, year: int) -> dict:
        nonlocal call_count
        call_count += 1
        if year == 2015:
            raise NetzNoeApiError("timeout")
        return {"values": [1.0] + [None] * 11}

    api.get_year = AsyncMock(side_effect=_get_year)

    with (
        patch(
            "custom_components.netz_noe_smartmeter.statistics.asyncio.sleep",
            new_callable=AsyncMock,
        ),
        patch(
            "custom_components.netz_noe_smartmeter.statistics.date",
        ) as mock_date,
    ):
        mock_date.today.return_value = date(2017, 6, 1)
        mock_date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)
        result = await _detect_earliest_data(api, "meter1")

    assert result == (2016, 1)


# ---------------------------------------------------------------------------
# async_backfill_statistics
# ---------------------------------------------------------------------------


async def test_backfill_with_start_year(hass: HomeAssistant) -> None:
    """Test backfill with explicit start year fetches chunks and imports."""
    raw_records = [
        {"MeasuredValue": 0.5, "Endtime": "2025-01-01T10:15:00+00:00"},
        {"MeasuredValue": 0.3, "Endtime": "2025-01-01T10:30:00+00:00"},
    ]
    api = MagicMock()
    api.get_raw = AsyncMock(return_value=raw_records)

    with (
        patch(
            "custom_components.netz_noe_smartmeter.statistics.asyncio.sleep",
            new_callable=AsyncMock,
        ),
        patch(PATCH_ADD_STATS) as mock_add,
        patch(PATCH_GET_INSTANCE) as mock_inst,
        patch(
            "custom_components.netz_noe_smartmeter.statistics.date",
        ) as mock_date,
    ):
        _mock_clear_statistics_done(mock_inst)
        mock_date.today.return_value = date(2025, 1, 15)
        mock_date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)
        result = await async_backfill_statistics(hass, api, "meter1", start_year=2025)

    assert "total_entries" in result
    assert result.get("total_kwh") == pytest.approx(0.8)
    mock_add.assert_called_once()


async def test_backfill_auto_detect(hass: HomeAssistant) -> None:
    """Test backfill with auto-detection of start date."""
    raw_records = [
        {"MeasuredValue": 1.0, "Endtime": "2025-01-01T10:15:00+00:00"},
    ]
    api = MagicMock()
    api.get_raw = AsyncMock(return_value=raw_records)
    # Auto-detect finds data starting Jan 2025
    api.get_year = AsyncMock(return_value={"values": [5.0] + [None] * 11})

    with (
        patch(
            "custom_components.netz_noe_smartmeter.statistics.asyncio.sleep",
            new_callable=AsyncMock,
        ),
        patch(PATCH_ADD_STATS),
        patch(PATCH_GET_INSTANCE) as mock_inst,
        patch(
            "custom_components.netz_noe_smartmeter.statistics.date",
        ) as mock_date,
    ):
        _mock_clear_statistics_done(mock_inst)
        mock_date.today.return_value = date(2025, 1, 15)
        mock_date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)
        result = await async_backfill_statistics(hass, api, "meter1")

    assert "total_entries" in result
    assert result.get("total_kwh") == pytest.approx(1.0)


async def test_backfill_auto_detect_no_data(hass: HomeAssistant) -> None:
    """Test backfill returns error when auto-detect finds nothing."""
    api = MagicMock()
    api.get_year = AsyncMock(return_value={"values": [None] * 12})

    with (
        patch(
            "custom_components.netz_noe_smartmeter.statistics.asyncio.sleep",
            new_callable=AsyncMock,
        ),
        patch(
            "custom_components.netz_noe_smartmeter.statistics.date",
        ) as mock_date,
    ):
        mock_date.today.return_value = date(2016, 1, 1)
        mock_date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)
        result = await async_backfill_statistics(hass, api, "meter1")

    assert "error" in result
    assert "No historical data" in result["error"]


async def test_backfill_no_raw_data(hass: HomeAssistant) -> None:
    """Test backfill returns error when API returns no raw records."""
    api = MagicMock()
    api.get_raw = AsyncMock(return_value=[])

    with (
        patch(
            "custom_components.netz_noe_smartmeter.statistics.asyncio.sleep",
            new_callable=AsyncMock,
        ),
        patch(PATCH_ADD_STATS) as mock_add,
        patch(
            "custom_components.netz_noe_smartmeter.statistics.date",
        ) as mock_date,
    ):
        mock_date.today.return_value = date(2025, 1, 15)
        mock_date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)
        result = await async_backfill_statistics(hass, api, "meter1", start_year=2025)

    assert "error" in result
    assert "No raw data" in result["error"]
    mock_add.assert_not_called()


async def test_backfill_progress_callback_reports_running_and_clearing(hass: HomeAssistant) -> None:
    """Test backfill progress callback is emitted during fetch and clear phases."""
    raw_records = [
        {"MeasuredValue": 0.5, "Endtime": "2025-01-01T10:15:00+00:00"},
    ]
    api = MagicMock()
    api.get_raw = AsyncMock(return_value=raw_records)

    progress_events: list[tuple[str, int, int, int, str]] = []

    def _progress_callback(state: str, chunk: int, total: int, records: int, date_range: str) -> None:
        progress_events.append((state, chunk, total, records, date_range))

    with (
        patch(
            "custom_components.netz_noe_smartmeter.statistics.asyncio.sleep",
            new_callable=AsyncMock,
        ),
        patch(PATCH_ADD_STATS),
        patch(PATCH_GET_INSTANCE) as mock_inst,
        patch(
            "custom_components.netz_noe_smartmeter.statistics.date",
        ) as mock_date,
    ):
        _mock_clear_statistics_done(mock_inst)
        mock_date.today.return_value = date(2025, 1, 15)
        mock_date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)
        result = await async_backfill_statistics(
            hass,
            api,
            "meter1",
            start_year=2025,
            progress_callback=_progress_callback,
        )

    assert "total_entries" in result
    assert any(event[0] == "running" for event in progress_events)
    assert any(event[0] == "clearing" for event in progress_events)


async def test_backfill_chunk_error_continues(hass: HomeAssistant) -> None:
    """Test backfill continues when individual chunks fail."""
    call_count = 0

    async def _get_raw(meter_id, start, end):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise NetzNoeApiError("timeout")
        return [{"MeasuredValue": 0.5, "Endtime": "2025-02-01T10:15:00+00:00"}]

    api = MagicMock()
    api.get_raw = AsyncMock(side_effect=_get_raw)

    with (
        patch(
            "custom_components.netz_noe_smartmeter.statistics.asyncio.sleep",
            new_callable=AsyncMock,
        ),
        patch(PATCH_ADD_STATS) as mock_add,
        patch(PATCH_GET_INSTANCE) as mock_inst,
        patch(
            "custom_components.netz_noe_smartmeter.statistics.date",
        ) as mock_date,
    ):
        _mock_clear_statistics_done(mock_inst)
        mock_date.today.return_value = date(2025, 3, 1)
        mock_date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)
        result = await async_backfill_statistics(hass, api, "meter1", start_year=2025)

    # First chunk failed, but later chunk(s) succeeded
    assert "total_entries" in result
    mock_add.assert_called_once()


async def test_backfill_rate_limit_retries_same_chunk(hass: HomeAssistant) -> None:
    """Test backfill retries the same chunk after a 429 rate limit with exponential backoff."""
    call_count = 0

    async def _get_raw(meter_id, start, end):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise NetzNoeRateLimitError("429 Too Many Requests")
        return [{"MeasuredValue": 0.5, "Endtime": "2025-01-05T10:15:00+00:00"}]

    api = MagicMock()
    api.get_raw = AsyncMock(side_effect=_get_raw)

    sleep_delays: list[float] = []

    async def _capture_sleep(delay: float) -> None:
        sleep_delays.append(delay)

    with (
        patch(
            "custom_components.netz_noe_smartmeter.statistics.asyncio.sleep",
            side_effect=_capture_sleep,
        ),
        patch(PATCH_ADD_STATS) as mock_add,
        patch(PATCH_GET_INSTANCE) as mock_inst,
        patch(
            "custom_components.netz_noe_smartmeter.statistics.date",
        ) as mock_date,
    ):
        _mock_clear_statistics_done(mock_inst)
        mock_date.today.return_value = date(2025, 1, 15)
        mock_date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)
        result = await async_backfill_statistics(hass, api, "meter1", start_year=2025)

    # Should have retried and succeeded
    assert "total_entries" in result
    mock_add.assert_called_once()
    # The rate limit should have doubled the delay (3 -> 6)
    assert 6 in sleep_delays
    # The first call failed, so get_raw was called at least 2 times
    assert call_count >= 2


async def test_backfill_chunk_error_after_retries_returns_error(hass: HomeAssistant) -> None:
    """Test backfill returns an error if a chunk keeps failing after retries."""
    api = MagicMock()
    api.get_raw = AsyncMock(side_effect=NetzNoeApiError("timeout"))

    with (
        patch(
            "custom_components.netz_noe_smartmeter.statistics.asyncio.sleep",
            new_callable=AsyncMock,
        ),
        patch(PATCH_ADD_STATS) as mock_add,
        patch(
            "custom_components.netz_noe_smartmeter.statistics.date",
        ) as mock_date,
    ):
        mock_date.today.return_value = date(2025, 1, 15)
        mock_date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)
        result = await async_backfill_statistics(hass, api, "meter1", start_year=2025)

    error = result.get("error")
    assert error is not None
    assert "Backfill failed for chunk" in error
    assert api.get_raw.await_count == 3
    mock_add.assert_not_called()


async def test_backfill_clear_timeout_returns_error(hass: HomeAssistant) -> None:
    """Test backfill returns an error if clear statistics times out."""
    raw_records = [
        {"MeasuredValue": 0.5, "Endtime": "2025-01-01T10:15:00+00:00"},
    ]
    api = MagicMock()
    api.get_raw = AsyncMock(return_value=raw_records)

    with (
        patch(
            "custom_components.netz_noe_smartmeter.statistics.asyncio.sleep",
            new_callable=AsyncMock,
        ),
        patch(PATCH_ADD_STATS) as mock_add,
        patch(PATCH_GET_INSTANCE) as mock_inst,
        patch(
            "custom_components.netz_noe_smartmeter.statistics.BACKFILL_CLEAR_CONFIRM_TIMEOUT_SECONDS",
            0,
        ),
        patch(
            "custom_components.netz_noe_smartmeter.statistics.date",
        ) as mock_date,
    ):
        recorder = MagicMock()
        recorder.async_clear_statistics.return_value = None
        recorder.async_block_till_done = AsyncMock(return_value=None)
        mock_inst.return_value = recorder

        mock_date.today.return_value = date(2025, 1, 15)
        mock_date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)
        result = await async_backfill_statistics(hass, api, "meter1", start_year=2025)

    assert result.get("error") == f"Failed to clear existing statistics for {STATISTIC_ID}"
    mock_add.assert_not_called()
