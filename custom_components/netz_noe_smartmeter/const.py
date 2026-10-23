"""Constants for the Netz NÖ Smart Meter integration."""

from typing import NotRequired, TypedDict

DOMAIN = "netz_noe_smartmeter"

BASE_URL = "https://smartmeter.netz-noe.at/orchestration"

# API endpoints (relative to BASE_URL)
ENDPOINT_DAY = "ConsumptionRecord/Day"
ENDPOINT_EXTEND_SESSION = "Authentication/ExtendSessionLifetime"
ENDPOINT_LOGIN = "Authentication/Login"
ENDPOINT_LOGOUT = "Authentication/Logout"
ENDPOINT_RAW = "ConsumptionRecord/Raw"
ENDPOINT_WEEK = "ConsumptionRecord/Week"
ENDPOINT_YEAR = "ConsumptionRecord/Year"

# Config keys
CONF_METER_ID = "meter_id"
CONF_STATISTIC_ID = "statistic_id"

# Defaults
DEFAULT_SCAN_INTERVAL_HOURS = 6

# Statistic all meters shared before 0.2.0
LEGACY_STATISTIC_ID = f"{DOMAIN}:grid_consumption"


class NetzNoeConfigData(TypedDict):
    """Typed config entry data for this integration."""

    meter_id: str
    password: str
    statistic_id: NotRequired[str]
    username: str
