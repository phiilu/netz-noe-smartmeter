"""Home Assistant test helpers from the test package of the installed Home Assistant."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pytest_homeassistant import common
    from pytest_homeassistant.components.recorder import common as recorder_common
else:
    try:
        from pytest_homeassistant import common
        from pytest_homeassistant.components.recorder import common as recorder_common
    except ImportError:  # Home Assistant before 2026.11, run by tests-minimum
        from pytest_homeassistant_custom_component import common
        from pytest_homeassistant_custom_component.components.recorder import common as recorder_common

MockConfigEntry = common.MockConfigEntry
async_wait_recording_done = recorder_common.async_wait_recording_done
do_adhoc_statistics = recorder_common.do_adhoc_statistics
get_start_time = recorder_common.get_start_time
statistics_during_period = recorder_common.statistics_during_period
