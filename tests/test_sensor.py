"""Tests for sensor entities."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from homeassistant.components.sensor import SensorStateClass

from custom_components.netz_noe_smartmeter.coordinator import BackfillProgress, NetzNoeData
from custom_components.netz_noe_smartmeter.sensor import (
    NetzNoeBackfillStatusSensor,
    NetzNoeDailyConsumptionSensor,
    NetzNoeLastUpdatedSensor,
    NetzNoeMonthlyConsumptionSensor,
    NetzNoeSensorBase,
    NetzNoeWeeklyConsumptionSensor,
    NetzNoeYearlyConsumptionSensor,
)


def _make_sensor[SensorT: NetzNoeSensorBase](sensor_cls: type[SensorT], data: NetzNoeData | None) -> SensorT:
    """Create a sensor with a fake coordinator, bypassing __init__."""
    coord = MagicMock()
    coord.data = data
    sensor = object.__new__(sensor_cls)
    sensor.coordinator = coord
    return sensor


class TestSensorValues:
    """Tests for sensor native_value properties."""

    def test_daily_sensor_returns_total(self) -> None:
        """Test daily sensor returns daily_total from coordinator data."""
        sensor = _make_sensor(NetzNoeDailyConsumptionSensor, NetzNoeData(daily_total=12.345))
        assert sensor.native_value == 12.345

    def test_daily_sensor_returns_none_when_no_data(self) -> None:
        """Test daily sensor returns None when coordinator has no data."""
        sensor = _make_sensor(NetzNoeDailyConsumptionSensor, None)
        assert sensor.native_value is None

    def test_weekly_sensor_returns_total(self) -> None:
        """Test weekly sensor returns weekly_total."""
        sensor = _make_sensor(NetzNoeWeeklyConsumptionSensor, NetzNoeData(weekly_total=45.678))
        assert sensor.native_value == 45.678

    def test_monthly_sensor_returns_total(self) -> None:
        """Test monthly sensor returns monthly_total."""
        sensor = _make_sensor(NetzNoeMonthlyConsumptionSensor, NetzNoeData(monthly_total=123.456))
        assert sensor.native_value == 123.456

    def test_yearly_sensor_returns_total(self) -> None:
        """Test yearly sensor returns yearly_total."""
        sensor = _make_sensor(NetzNoeYearlyConsumptionSensor, NetzNoeData(yearly_total=1234.567))
        assert sensor.native_value == 1234.567


class TestSensorStateClass:
    """Tests for sensor state classes."""

    def test_period_sensors_use_measurement_state_class(self) -> None:
        """Test period-total sensors use measurement semantics."""
        coordinator = MagicMock()
        assert NetzNoeDailyConsumptionSensor(coordinator, "meter").state_class is SensorStateClass.MEASUREMENT
        assert NetzNoeWeeklyConsumptionSensor(coordinator, "meter").state_class is SensorStateClass.MEASUREMENT
        assert NetzNoeMonthlyConsumptionSensor(coordinator, "meter").state_class is SensorStateClass.MEASUREMENT
        assert NetzNoeYearlyConsumptionSensor(coordinator, "meter").state_class is SensorStateClass.MEASUREMENT


class TestYearlyAttributes:
    """Tests for yearly sensor extra_state_attributes."""

    def test_monthly_breakdown(self) -> None:
        """Test monthly breakdown in attributes."""
        monthly = [10.0, 20.0, 30.123, None, None, None, None, None, None, None, None, None]
        sensor = _make_sensor(NetzNoeYearlyConsumptionSensor, NetzNoeData(monthly_values=monthly))

        attrs = sensor.extra_state_attributes
        assert attrs is not None
        assert attrs["jan"] == 10.0
        assert attrs["feb"] == 20.0
        assert attrs["mar"] == 30.123
        assert attrs["apr"] is None

    def test_prev_year_attributes(self) -> None:
        """Test previous year data in attributes."""
        prev = [5.0, 6.0, None, None, None, None, None, None, None, None, None, None]
        data = NetzNoeData(
            monthly_values=[],
            prev_year=2024,
            prev_year_monthly_values=prev,
        )
        sensor = _make_sensor(NetzNoeYearlyConsumptionSensor, data)

        attrs = sensor.extra_state_attributes
        assert attrs is not None
        assert attrs["jan_2024"] == 5.0
        assert attrs["feb_2024"] == 6.0
        assert "mar_2024" not in attrs  # None values excluded

    def test_no_data_returns_none(self) -> None:
        """Test attributes return None when no coordinator data."""
        sensor = _make_sensor(NetzNoeYearlyConsumptionSensor, None)
        assert sensor.extra_state_attributes is None


class TestLastUpdatedSensor:
    """Tests for last_updated timestamp sensor."""

    def test_parses_iso_timestamp(self) -> None:
        """Test parsing ISO timestamp."""
        sensor = _make_sensor(
            NetzNoeLastUpdatedSensor,
            NetzNoeData(last_data_timestamp="2025-01-15T10:30:00+01:00"),
        )

        result = sensor.native_value
        assert result is not None
        assert isinstance(result, datetime)
        assert result.tzinfo is not None

    def test_naive_timestamp_gets_utc(self) -> None:
        """Test naive timestamp gets UTC."""
        sensor = _make_sensor(
            NetzNoeLastUpdatedSensor,
            NetzNoeData(last_data_timestamp="2025-01-15T10:30:00"),
        )

        result = sensor.native_value
        assert result is not None
        assert result.tzinfo == UTC

    def test_invalid_timestamp_returns_none(self) -> None:
        """Test invalid timestamp returns None."""
        sensor = _make_sensor(
            NetzNoeLastUpdatedSensor,
            NetzNoeData(last_data_timestamp="not-a-date"),
        )
        assert sensor.native_value is None

    def test_no_timestamp_returns_none(self) -> None:
        """Test None timestamp returns None."""
        sensor = _make_sensor(
            NetzNoeLastUpdatedSensor,
            NetzNoeData(last_data_timestamp=None),
        )
        assert sensor.native_value is None


class TestNullCoordinatorData:
    """Tests for sensors when coordinator.data is None."""

    def test_weekly_sensor_none_data(self) -> None:
        """Test weekly sensor returns None when coordinator data is None."""
        sensor = _make_sensor(NetzNoeWeeklyConsumptionSensor, None)
        assert sensor.native_value is None

    def test_monthly_sensor_none_data(self) -> None:
        """Test monthly sensor returns None when coordinator data is None."""
        sensor = _make_sensor(NetzNoeMonthlyConsumptionSensor, None)
        assert sensor.native_value is None

    def test_yearly_sensor_none_data(self) -> None:
        """Test yearly sensor returns None when coordinator data is None."""
        sensor = _make_sensor(NetzNoeYearlyConsumptionSensor, None)
        assert sensor.native_value is None

    def test_last_updated_none_data(self) -> None:
        """Test last_updated sensor returns None when coordinator data is None."""
        sensor = _make_sensor(NetzNoeLastUpdatedSensor, None)
        assert sensor.native_value is None


class TestBackfillStatusSensor:
    """Tests for the backfill status sensor."""

    def test_idle_state(self) -> None:
        """Test sensor shows idle by default."""
        coord = MagicMock()
        coord.backfill_progress = BackfillProgress()
        sensor = NetzNoeBackfillStatusSensor(coord, "meter123")

        assert sensor.native_value == "idle"
        assert sensor.extra_state_attributes == {}

    def test_running_state_with_attributes(self) -> None:
        """Test sensor shows progress while running."""
        coord = MagicMock()
        coord.backfill_progress = BackfillProgress(
            state="running",
            current_chunk=5,
            total_chunks=38,
            records_processed=14400,
            current_date_range="2023-05-05 to 2023-06-04",
        )
        sensor = NetzNoeBackfillStatusSensor(coord, "meter123")

        assert sensor.native_value == "running"
        attrs = sensor.extra_state_attributes
        assert attrs["current_chunk"] == 5
        assert attrs["total_chunks"] == 38
        assert attrs["records_processed"] == 14400
        assert attrs["current_date_range"] == "2023-05-05 to 2023-06-04"

    def test_clearing_state_with_attributes(self) -> None:
        """Test sensor shows clear phase progress."""
        coord = MagicMock()
        coord.backfill_progress = BackfillProgress(
            state="clearing",
            current_chunk=38,
            total_chunks=38,
            records_processed=99996,
            current_date_range="Clearing existing statistics",
        )
        sensor = NetzNoeBackfillStatusSensor(coord, "meter123")

        assert sensor.native_value == "clearing"
        attrs = sensor.extra_state_attributes
        assert attrs["current_chunk"] == 38
        assert attrs["total_chunks"] == 38
        assert attrs["records_processed"] == 99996
        assert attrs["current_date_range"] == "Clearing existing statistics"

    def test_completed_state_with_summary(self) -> None:
        """Test sensor shows result after completion."""
        coord = MagicMock()
        coord.backfill_progress = BackfillProgress(
            state="completed",
            result_summary="24223 entries, 8379.623 kWh",
        )
        sensor = NetzNoeBackfillStatusSensor(coord, "meter123")

        assert sensor.native_value == "completed"
        assert sensor.extra_state_attributes["result"] == "24223 entries, 8379.623 kWh"

    def test_failed_state(self) -> None:
        """Test sensor shows failure."""
        coord = MagicMock()
        coord.backfill_progress = BackfillProgress(
            state="failed",
            result_summary="No raw data returned",
        )
        sensor = NetzNoeBackfillStatusSensor(coord, "meter123")

        assert sensor.native_value == "failed"
        assert sensor.extra_state_attributes["result"] == "No raw data returned"


class TestCoordinatorUpdateHooks:
    """Tests for async update and coordinator cache invalidation."""

    async def test_async_update_skips_refresh_when_disabled(self) -> None:
        """Test async_update returns early for disabled entities."""
        sensor = _make_sensor(NetzNoeDailyConsumptionSensor, NetzNoeData(daily_total=1.0))
        sensor.coordinator.async_request_refresh = AsyncMock()

        with patch.object(NetzNoeDailyConsumptionSensor, "enabled", new_callable=PropertyMock, return_value=False):
            await sensor.async_update()

        sensor.coordinator.async_request_refresh.assert_not_awaited()

    async def test_async_update_requests_refresh_when_enabled(self) -> None:
        """Test async_update triggers coordinator refresh for enabled entities."""
        sensor = _make_sensor(NetzNoeDailyConsumptionSensor, NetzNoeData(daily_total=1.0))
        sensor.coordinator.async_request_refresh = AsyncMock()

        with patch.object(NetzNoeDailyConsumptionSensor, "enabled", new_callable=PropertyMock, return_value=True):
            await sensor.async_update()

        sensor.coordinator.async_request_refresh.assert_awaited_once()

    def test_handle_coordinator_update_clears_cached_properties(self) -> None:
        """Test coordinator updates invalidate cached state accessors."""
        sensor = _make_sensor(NetzNoeDailyConsumptionSensor, NetzNoeData(daily_total=1.0))
        sensor.coordinator.last_update_success = True

        assert sensor.native_value == 1.0
        assert sensor.available is True

        cache = cast(dict[str, object], cast(object, sensor.__dict__))
        cache["extra_state_attributes"] = {"foo": "bar"}
        sensor.coordinator.data = NetzNoeData(daily_total=2.0)

        with patch.object(sensor, "async_write_ha_state"):
            sensor._handle_coordinator_update()

        assert sensor.native_value == 2.0
        assert "extra_state_attributes" not in sensor.__dict__
