"""Tests for statistics parsing and aggregation logic."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from functools import partial
from itertools import pairwise
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from freezegun.api import FrozenDateTimeFactory
from homeassistant.components.recorder.models import StatisticData
from homeassistant.components.recorder.statistics import async_add_external_statistics, get_metadata
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import MockConfigEntry
from pytest_homeassistant_custom_component.components.recorder.common import async_wait_recording_done

from custom_components.netz_noe_smartmeter.api import (
    DayResponse,
    EnergyCommunityValues,
    NetzNoeApiError,
    NetzNoeAuthError,
    NetzNoeLoginError,
    NetzNoeMaintenanceError,
    NetzNoeRateLimitError,
    RawRecord,
)
from custom_components.netz_noe_smartmeter.const import CONF_STATISTIC_ID, DOMAIN, LEGACY_STATISTIC_ID
from custom_components.netz_noe_smartmeter.statistics import (
    EnergyCommunityDay,
    _aggregate_to_hourly,
    _build_metadata,
    _detect_earliest_data,
    _get_last_sum_and_end,
    _parse_raw_to_intervals,
    async_apply_statistic_names,
    async_backfill_energy_community_statistics,
    async_backfill_statistics,
    async_get_statistics_diagnostics,
    async_import_energy_community_statistics,
    async_import_raw_statistics,
    async_statistic_has_data,
    energy_community_day,
    statistic_id_for,
)

from .conftest import MOCK_METER_ID

STATISTIC_ID = statistic_id_for("meter1")

# ---------------------------------------------------------------------------
# _build_metadata
# ---------------------------------------------------------------------------


def test_build_metadata(hass: HomeAssistant) -> None:
    """Test that metadata is built correctly."""
    meta = _build_metadata(hass, MOCK_METER_ID)
    assert meta["source"] == "netz_noe_smartmeter"
    assert meta["statistic_id"] == "netz_noe_smartmeter:at0020000000000000001000000654321"
    assert meta["name"] == f"Smart Meter {MOCK_METER_ID}"
    assert meta["has_sum"] is True
    assert meta["unit_of_measurement"] == "kWh"


def test_build_metadata_uses_entry_title(hass: HomeAssistant) -> None:
    """Test that the statistic is named after the (renamable) config entry title."""
    MockConfigEntry(domain=DOMAIN, unique_id=MOCK_METER_ID, title="Einspeisung").add_to_hass(hass)
    assert _build_metadata(hass, MOCK_METER_ID)["name"] == "Einspeisung"


def test_build_metadata_uses_stored_statistic_id(hass: HomeAssistant) -> None:
    """Test a meter set up before 0.2.0 keeps the statistic ID stored in its entry."""
    MockConfigEntry(domain=DOMAIN, unique_id=MOCK_METER_ID, data={CONF_STATISTIC_ID: LEGACY_STATISTIC_ID}).add_to_hass(
        hass
    )
    assert _build_metadata(hass, MOCK_METER_ID)["statistic_id"] == LEGACY_STATISTIC_ID


def test_statistic_id_unique_per_meter() -> None:
    """Test that each meter gets its own statistic ID (issue #4)."""
    assert statistic_id_for("AT0020000000000000001000000000001") != statistic_id_for(
        "AT0020000000000000001000000000002"
    )


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
        running_sum, last_end = await _get_last_sum_and_end(hass, STATISTIC_ID)

    assert running_sum == Decimal(0)
    assert last_end is None


async def test_get_last_sum_and_end_with_float_end(hass: HomeAssistant) -> None:
    """Test extracts sum and converts float timestamp to datetime."""
    last_stats = {
        STATISTIC_ID: [{"sum": 123.456, "end": 1736899200.0}],  # 2025-01-15 00:00 UTC
    }
    with patch(PATCH_GET_INSTANCE) as mock_inst:
        mock_inst.return_value.async_add_executor_job = AsyncMock(return_value=last_stats)
        running_sum, last_end = await _get_last_sum_and_end(hass, STATISTIC_ID)

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
        running_sum, last_end = await _get_last_sum_and_end(hass, STATISTIC_ID)

    assert running_sum == Decimal("50.0")
    assert last_end is end_dt


async def test_get_last_sum_and_end_no_sum_value(hass: HomeAssistant) -> None:
    """Test handles entry with no sum (sum is None)."""
    last_stats = {
        STATISTIC_ID: [{"sum": None, "end": 1736899200.0}],
    }
    with patch(PATCH_GET_INSTANCE) as mock_inst:
        mock_inst.return_value.async_add_executor_job = AsyncMock(return_value=last_stats)
        running_sum, last_end = await _get_last_sum_and_end(hass, STATISTIC_ID)

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
            await async_import_raw_statistics(hass, "meter1", [])

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
            await async_import_raw_statistics(hass, "meter1", records)

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
            await async_import_raw_statistics(hass, "meter1", records)

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
            await async_import_raw_statistics(hass, "meter1", records)

    mock_add.assert_not_called()


# ---------------------------------------------------------------------------
# _detect_earliest_data
# ---------------------------------------------------------------------------


async def test_detect_earliest_data_found() -> None:
    """Test auto-detection finds first year/month with data."""
    api = MagicMock()
    # Year 2023: no data; Year 2024: has data in March
    call_count = 0

    async def _get_year(meter_id: str, year: int, *, record_shape: bool = True) -> dict:
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
    # Probing old years must not replace the poll's response structure in the diagnostics
    assert all(call.kwargs == {"record_shape": False} for call in api.get_year.call_args_list)


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

    async def _get_year(meter_id: str, year: int, *, record_shape: bool = True) -> dict:
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

    async def _get_year(meter_id: str, year: int, *, record_shape: bool = True) -> dict:
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


@pytest.mark.parametrize(
    "error",
    [NetzNoeLoginError("Connection error during login"), NetzNoeMaintenanceError("Portal is under maintenance")],
    ids=["login", "maintenance"],
)
async def test_detect_earliest_data_portal_down_propagates(error: NetzNoeApiError) -> None:
    """Test a failed login or maintenance is not mistaken for a year without data."""
    api = MagicMock()
    api.get_year = AsyncMock(side_effect=error)

    with (
        patch(
            "custom_components.netz_noe_smartmeter.statistics.asyncio.sleep",
            new_callable=AsyncMock,
        ),
        pytest.raises(type(error)),
    ):
        _ = await _detect_earliest_data(api, "meter1")

    assert api.get_year.await_count == 1


# ---------------------------------------------------------------------------
# async_backfill_statistics
# ---------------------------------------------------------------------------


async def test_backfill_with_start_year(hass: HomeAssistant, caplog: pytest.LogCaptureFixture) -> None:
    """Test backfill with explicit start year fetches chunks and imports."""
    caplog.set_level(logging.INFO, logger="custom_components.netz_noe_smartmeter")
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
    assert "meter1" not in caplog.text


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


async def test_backfill_auth_error_is_not_retried(hass: HomeAssistant) -> None:
    """Test rejected credentials stop the backfill instead of retrying the login."""
    api = MagicMock()
    api.get_raw = AsyncMock(side_effect=NetzNoeAuthError("Invalid username or password"))

    with (
        patch(
            "custom_components.netz_noe_smartmeter.statistics.asyncio.sleep",
            new_callable=AsyncMock,
        ),
        patch(PATCH_ADD_STATS) as mock_add,
        pytest.raises(NetzNoeAuthError),
    ):
        _ = await async_backfill_statistics(hass, api, "meter1", start_year=2025)

    assert api.get_raw.await_count == 1
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

    assert result.get("error") == "Failed to clear existing statistics"
    mock_add.assert_not_called()


# ---------------------------------------------------------------------------
# async_statistic_has_data / async_get_statistics_diagnostics (real recorder)
# ---------------------------------------------------------------------------


async def _add_statistics(hass: HomeAssistant) -> None:
    """Write one hourly row to meter1's statistic."""
    start = datetime(2025, 1, 15, 10, tzinfo=UTC)
    async_add_external_statistics(
        hass, _build_metadata(hass, "meter1"), [StatisticData(start=start, state=1.5, sum=1.5)]
    )
    await async_wait_recording_done(hass)


async def test_apply_statistic_names(hass: HomeAssistant) -> None:
    """Test an import without rows writes the entry title into the metadata, without creating EEG statistics."""
    MockConfigEntry(domain=DOMAIN, unique_id=MOCK_METER_ID, title="Einspeisung").add_to_hass(hass)
    statistic_id = statistic_id_for(MOCK_METER_ID)

    await async_apply_statistic_names(hass, MOCK_METER_ID)
    await async_wait_recording_done(hass)

    statistic_ids = {statistic_id, f"{statistic_id}_self_coverage", f"{statistic_id}_grid_leftover"}
    metadata = await hass.async_add_executor_job(partial(get_metadata, hass, statistic_ids=statistic_ids))
    assert set(metadata) == {statistic_id}
    assert metadata[statistic_id][1]["name"] == "Einspeisung"
    assert await async_statistic_has_data(hass, statistic_id) is False


async def test_statistic_has_data(hass: HomeAssistant) -> None:
    """Test a statistic counts as present once it has a row."""
    assert await async_statistic_has_data(hass, STATISTIC_ID) is False
    await _add_statistics(hass)
    assert await async_statistic_has_data(hass, STATISTIC_ID) is True


async def test_statistics_diagnostics(hass: HomeAssistant) -> None:
    """Test diagnostics report the meter's statistic."""
    await _add_statistics(hass)

    assert await async_get_statistics_diagnostics(hass, "meter1") == {
        "statistic_id": STATISTIC_ID,
        "last_sum": 1.5,
        "last_end": "2025-01-15T11:00:00+00:00",
        "energy_community": {
            key: {"statistic_id": f"{STATISTIC_ID}_{key}", "last_sum": 0.0, "last_end": None}
            for key in ("self_coverage", "grid_leftover")
        },
    }


# ---------------------------------------------------------------------------
# Energy community statistics
# ---------------------------------------------------------------------------

SELF_COVERAGE_ID = f"{STATISTIC_ID}_self_coverage"
GRID_LEFTOVER_ID = f"{STATISTIC_ID}_grid_leftover"
PATCH_SLEEP = "custom_components.netz_noe_smartmeter.statistics.asyncio.sleep"


def _ec_response(slots: int = 96, value: float | None = 0.25) -> DayResponse:
    """Return a Day response with the same split in every slot: value and twice the value."""
    return {
        "meteredValues": [None if value is None else 3 * value] * slots,
        "selfCoverageValues": [value] * slots,
        "gridUsageLeftoverValues": [None if value is None else 2 * value] * slots,
    }


def _ec_day(day: date, slots: int = 96) -> EnergyCommunityDay:
    """Return a day with 0.25 kWh community share and 0.5 kWh remaining grid consumption per slot."""
    result = energy_community_day(day, _ec_response(slots))
    assert result is not None
    return result


def _local_midnight_utc(day: date) -> datetime:
    """Return the start of a local day in UTC."""
    return dt_util.as_utc(dt_util.start_of_local_day(day))


def _imports_by_statistic(mock_add: MagicMock) -> dict[str, tuple[str, list[StatisticData]]]:
    """Return the name and rows of each import, keyed by statistic ID."""
    return {call.args[1]["statistic_id"]: (call.args[1]["name"], call.args[2]) for call in mock_add.call_args_list}


async def _run_import(
    hass: HomeAssistant,
    *,
    imported_until: date | None = None,
    fetch_day: AsyncMock | None = None,
    newest: EnergyCommunityDay | None = None,
) -> tuple[MagicMock, AsyncMock]:
    """Import up to 14 Nov 2026 with both statistics imported until the start of a day; return the mocks.

    The sums so far are 10 kWh community share and 20 kWh remaining grid consumption.
    """
    fetch_day = fetch_day or AsyncMock(side_effect=_ec_day)
    last_stats = {}
    if imported_until is not None:
        end = _local_midnight_utc(imported_until)
        last_stats = {SELF_COVERAGE_ID: [{"sum": 10.0, "end": end}], GRID_LEFTOVER_ID: [{"sum": 20.0, "end": end}]}
    with patch(PATCH_GET_INSTANCE) as mock_inst, patch(PATCH_ADD_STATS) as mock_add:
        mock_inst.return_value.async_add_executor_job = AsyncMock(return_value=last_stats)
        await async_import_energy_community_statistics(hass, "meter1", newest or _ec_day(date(2026, 11, 14)), fetch_day)
    return mock_add, fetch_day


@pytest.mark.parametrize(
    "response",
    [
        {},
        {"selfCoverageValues": None, "gridUsageLeftoverValues": None},
        {"selfCoverageValues": [None] * 96, "gridUsageLeftoverValues": [None] * 96},
    ],
    ids=["missing", "null", "not_published"],
)
def test_energy_community_day_without_split(response: EnergyCommunityValues) -> None:
    """Test a day without a split, as for meters outside an EEG or before the portal publishes it, gives None."""
    assert energy_community_day(date(2026, 11, 14), response) is None


async def test_import_energy_community_first_import(hass: HomeAssistant) -> None:
    """Test the first import writes both statistics hourly from local midnight, named after the entry."""
    MockConfigEntry(domain=DOMAIN, unique_id="meter1", title="Bezug").add_to_hass(hass)

    mock_add, fetch_day = await _run_import(hass)

    fetch_day.assert_not_awaited()
    imports = _imports_by_statistic(mock_add)
    assert set(imports) == {SELF_COVERAGE_ID, GRID_LEFTOVER_ID}
    name, rows = imports[SELF_COVERAGE_ID]
    assert name == "Bezug Eigendeckung"
    assert len(rows) == 24
    assert rows[0]["start"] == _local_midnight_utc(date(2026, 11, 14))
    assert rows[0]["state"] == pytest.approx(1.0)
    assert rows[-1]["sum"] == pytest.approx(24.0)
    name, rows = imports[GRID_LEFTOVER_ID]
    assert name == "Bezug Restnetzbezug"
    assert rows[-1]["sum"] == pytest.approx(48.0)


async def test_import_energy_community_catches_up_missing_days(hass: HomeAssistant) -> None:
    """Test days between the last import and the newest day are fetched and imported with the running sum."""
    mock_add, fetch_day = await _run_import(hass, imported_until=date(2026, 11, 12))

    assert [call.args[0] for call in fetch_day.await_args_list] == [date(2026, 11, 12), date(2026, 11, 13)]
    imports = _imports_by_statistic(mock_add)
    _, rows = imports[SELF_COVERAGE_ID]
    assert len(rows) == 72
    assert rows[0]["start"] == _local_midnight_utc(date(2026, 11, 12))
    assert rows[-1]["sum"] == pytest.approx(10.0 + 72)
    assert imports[GRID_LEFTOVER_ID][1][-1]["sum"] == pytest.approx(20.0 + 144)


async def test_import_energy_community_waits_for_late_day(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """Test the import stops at a day without a split yet, so the newest day does not skip it for good."""
    caplog.set_level(logging.DEBUG, logger="custom_components.netz_noe_smartmeter")

    mock_add, fetch_day = await _run_import(
        hass, imported_until=date(2026, 11, 13), fetch_day=AsyncMock(return_value=None)
    )

    fetch_day.assert_awaited_once_with(date(2026, 11, 13))
    mock_add.assert_not_called()
    assert "No energy community data for 2026-11-13 yet" in caplog.text


@pytest.mark.parametrize(
    ("imported_until", "fetched"),
    [
        (date(2026, 11, 2), [date(2026, 11, 10), date(2026, 11, 11), date(2026, 11, 12), date(2026, 11, 13)]),
        (date(2026, 11, 14), []),
        (date(2026, 11, 15), []),
    ],
    ids=["five_days_at_most", "newest_day_next", "up_to_date"],
)
async def test_import_energy_community_fetched_days(
    hass: HomeAssistant, imported_until: date, fetched: list[date]
) -> None:
    """Test which days are fetched: at most five days back, so a day without a split cannot block it forever."""
    mock_add, fetch_day = await _run_import(hass, imported_until=imported_until)

    assert [call.args[0] for call in fetch_day.await_args_list] == fetched
    assert mock_add.called is (imported_until <= date(2026, 11, 14))


async def test_import_energy_community_day_clocks_change(hass: HomeAssistant) -> None:
    """Test a day with 25 hours (100 slots) fills its hours without a gap or overlap."""
    day = date(2026, 11, 1)  # clocks go back in the test time zone (US/Pacific)

    mock_add, _ = await _run_import(hass, newest=_ec_day(day, slots=100))

    _, rows = _imports_by_statistic(mock_add)[SELF_COVERAGE_ID]
    starts = [row["start"] for row in rows]
    assert len(rows) == 25
    assert starts[0] == _local_midnight_utc(day)
    assert starts[-1] == _local_midnight_utc(day + timedelta(days=1)) - timedelta(hours=1)
    assert all(later - earlier == timedelta(hours=1) for earlier, later in pairwise(starts))


async def test_import_energy_community_real_recorder(hass: HomeAssistant) -> None:
    """Test two polls in a row continue the running sum in the recorder, and a rename reaches both statistics."""
    fetch_day = AsyncMock()
    await async_import_energy_community_statistics(hass, "meter1", _ec_day(date(2026, 11, 13)), fetch_day)
    await async_wait_recording_done(hass)
    await async_import_energy_community_statistics(hass, "meter1", _ec_day(date(2026, 11, 14)), fetch_day)
    await async_wait_recording_done(hass)

    fetch_day.assert_not_awaited()
    end = _local_midnight_utc(date(2026, 11, 15)).isoformat()
    diagnostics = await async_get_statistics_diagnostics(hass, "meter1")
    assert diagnostics["energy_community"] == {
        key: {"statistic_id": f"{STATISTIC_ID}_{key}", "last_sum": pytest.approx(sum_), "last_end": end}
        for key, sum_ in (("self_coverage", 48.0), ("grid_leftover", 96.0))
    }

    MockConfigEntry(domain=DOMAIN, unique_id="meter1", title="Bezug").add_to_hass(hass)
    await async_apply_statistic_names(hass, "meter1")
    await async_wait_recording_done(hass)
    metadata = await hass.async_add_executor_job(
        partial(get_metadata, hass, statistic_ids={SELF_COVERAGE_ID, GRID_LEFTOVER_ID})
    )
    assert {meta["name"] for _, meta in metadata.values()} == {"Bezug Eigendeckung", "Bezug Restnetzbezug"}


def _year_response(*months_with_split: int) -> dict[str, list[float | None]]:
    """Return a Year response with a split in the given months."""
    values: list[float | None] = [1.0 if month in months_with_split else None for month in range(1, 13)]
    return {"values": [1.0] * 12, "selfCoverageValues": values, "gridUsageLeftoverValues": values}


def _ec_backfill_api(years: dict[int, dict]) -> MagicMock:
    """Return an API with the given Year responses and a split for every day."""

    async def _get_year(_meter_id: str, year: int, *, record_shape: bool = True) -> dict:
        return years.get(year, _year_response())

    api = MagicMock()
    api.get_year = AsyncMock(side_effect=_get_year)
    api.get_day = AsyncMock(return_value=_ec_response())
    return api


async def test_backfill_energy_community(hass: HomeAssistant, freezer: FrozenDateTimeFactory) -> None:
    """Test the backfill fetches every day of the months with a split up to yesterday and replaces both statistics."""
    freezer.move_to("2026-02-03 12:00:00-08:00")
    api = _ec_backfill_api({2026: _year_response(1, 2), 2025: _year_response(12)})
    progress: list[tuple[str, int, int, int, str]] = []

    def _progress_callback(state: str, chunk: int, total: int, records: int, date_range: str) -> None:
        progress.append((state, chunk, total, records, date_range))

    with (
        patch(PATCH_SLEEP, new_callable=AsyncMock),
        patch(PATCH_ADD_STATS) as mock_add,
        patch(PATCH_GET_INSTANCE) as mock_inst,
    ):
        _mock_clear_statistics_done(mock_inst)
        result = await async_backfill_energy_community_statistics(
            hass, api, "meter1", progress_callback=_progress_callback
        )

    # 2024 has no split, so the walk back stops there
    assert [call.args[1] for call in api.get_year.await_args_list] == [2026, 2025, 2024]
    days = [call.args[1] for call in api.get_day.await_args_list]
    assert days[0] == date(2025, 12, 1)
    assert days[-1] == date(2026, 2, 2)
    assert len(days) == 31 + 31 + 2
    assert all(call.kwargs == {"record_shape": False} for call in api.get_day.await_args_list)
    assert result == {
        "total_entries": 64 * 24,
        "days_fetched": 64,
        "self_coverage_kwh": 64 * 24.0,
        "grid_leftover_kwh": 64 * 48.0,
        "start": str(_local_midnight_utc(date(2025, 12, 1))),
        "end": str(_local_midnight_utc(date(2026, 2, 3)) - timedelta(hours=1)),
    }
    mock_inst.return_value.async_clear_statistics.assert_called_once()
    assert mock_inst.return_value.async_clear_statistics.call_args.args[0] == [SELF_COVERAGE_ID, GRID_LEFTOVER_ID]
    assert set(_imports_by_statistic(mock_add)) == {SELF_COVERAGE_ID, GRID_LEFTOVER_ID}
    assert progress[0] == ("running", 1, 64, 96, "2025-12-01")
    assert progress[-1] == ("clearing", 64, 64, 64 * 96, "Clearing existing statistics")


@pytest.mark.parametrize(
    ("now", "years", "start_year", "years_requested", "days_fetched"),
    [
        ("2026-01-03", {2026: (1,), 2025: (12,)}, 2026, [2026], 2),
        ("2016-01-03", {2016: (1,), 2015: (12,), 2014: (12,)}, None, [2016, 2015], 31 + 2),
        ("2026-01-01", {2025: (12,)}, None, [2026, 2025, 2024], 31),
    ],
    ids=["start_year", "earliest_year", "new_year"],
)
async def test_backfill_energy_community_years(
    hass: HomeAssistant,
    freezer: FrozenDateTimeFactory,
    now: str,
    years: dict[int, tuple[int, ...]],
    start_year: int | None,
    years_requested: list[int],
    days_fetched: int,
) -> None:
    """Test the walk back ends at the start year or 2015, and on 1 January also takes the year before."""
    freezer.move_to(f"{now} 12:00:00-08:00")
    api = _ec_backfill_api({year: _year_response(*months) for year, months in years.items()})

    with patch(PATCH_SLEEP, new_callable=AsyncMock), patch(PATCH_ADD_STATS), patch(PATCH_GET_INSTANCE) as mock_inst:
        _mock_clear_statistics_done(mock_inst)
        result = await async_backfill_energy_community_statistics(hass, api, "meter1", start_year=start_year)

    assert [call.args[1] for call in api.get_year.await_args_list] == years_requested
    assert result.get("days_fetched") == days_fetched


@pytest.mark.parametrize("days_have_split", [False, True], ids=["no_months", "no_days"])
async def test_backfill_energy_community_without_split(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory, days_have_split: bool
) -> None:
    """Test a meter without a split fails the backfill without touching the statistics."""
    freezer.move_to("2026-01-03 12:00:00-08:00")
    api = _ec_backfill_api({2026: _year_response(1)} if days_have_split else {})
    api.get_day = AsyncMock(return_value=_ec_response(value=None))

    with (
        patch(PATCH_SLEEP, new_callable=AsyncMock),
        patch(PATCH_ADD_STATS) as mock_add,
        patch(PATCH_GET_INSTANCE) as mock_inst,
    ):
        result = await async_backfill_energy_community_statistics(hass, api, "meter1")

    assert result == {"error": "No energy community data found"}
    assert api.get_day.await_count == (2 if days_have_split else 0)
    mock_inst.return_value.async_clear_statistics.assert_not_called()
    mock_add.assert_not_called()


@pytest.mark.parametrize(
    ("error", "attempts"),
    [(NetzNoeApiError("API error 500"), 3), (NetzNoeMaintenanceError("maintenance"), 1)],
    ids=["retried", "maintenance"],
)
async def test_backfill_energy_community_year_error(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory, error: NetzNoeApiError, attempts: int
) -> None:
    """Test a failing Year request raises: maintenance at once, like the year detection, other errors after retries."""
    freezer.move_to("2026-01-03 12:00:00-08:00")
    api = _ec_backfill_api({})
    api.get_year = AsyncMock(side_effect=error)

    with patch(PATCH_SLEEP, new_callable=AsyncMock), pytest.raises(NetzNoeApiError):
        _ = await async_backfill_energy_community_statistics(hass, api, "meter1")

    assert api.get_year.await_count == attempts


async def test_backfill_energy_community_day_error(hass: HomeAssistant, freezer: FrozenDateTimeFactory) -> None:
    """Test a day that keeps failing fails the backfill without touching the statistics."""
    freezer.move_to("2026-01-03 12:00:00-08:00")
    api = _ec_backfill_api({2026: _year_response(1)})
    api.get_day = AsyncMock(side_effect=NetzNoeRateLimitError("API rate limit exceeded"))

    with patch(PATCH_SLEEP, new_callable=AsyncMock), patch(PATCH_ADD_STATS) as mock_add:
        result = await async_backfill_energy_community_statistics(hass, api, "meter1")

    assert result == {"error": "Energy community backfill failed for 2026-01-01 after 3 retries"}
    assert api.get_day.await_count == 3
    mock_add.assert_not_called()


async def test_backfill_energy_community_auth_error(hass: HomeAssistant, freezer: FrozenDateTimeFactory) -> None:
    """Test rejected credentials stop the backfill instead of retrying the login."""
    freezer.move_to("2026-01-03 12:00:00-08:00")
    api = _ec_backfill_api({2026: _year_response(1)})
    api.get_day = AsyncMock(side_effect=NetzNoeAuthError("Invalid username or password"))

    with patch(PATCH_SLEEP, new_callable=AsyncMock), pytest.raises(NetzNoeAuthError):
        _ = await async_backfill_energy_community_statistics(hass, api, "meter1")

    assert api.get_day.await_count == 1


async def test_backfill_energy_community_clear_timeout(hass: HomeAssistant, freezer: FrozenDateTimeFactory) -> None:
    """Test the backfill fails if the recorder does not confirm clearing the statistics."""
    freezer.move_to("2026-01-03 12:00:00-08:00")
    api = _ec_backfill_api({2026: _year_response(1)})

    with (
        patch(PATCH_SLEEP, new_callable=AsyncMock),
        patch(PATCH_ADD_STATS) as mock_add,
        patch(PATCH_GET_INSTANCE) as mock_inst,
        patch("custom_components.netz_noe_smartmeter.statistics.BACKFILL_CLEAR_CONFIRM_TIMEOUT_SECONDS", 0),
    ):
        mock_inst.return_value.async_block_till_done = AsyncMock(return_value=None)
        result = await async_backfill_energy_community_statistics(hass, api, "meter1")

    assert result == {"error": "Failed to clear existing energy community statistics"}
    mock_add.assert_not_called()
