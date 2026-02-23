"""Constants for the Netz NÖ Smart Meter integration."""

from typing import TypedDict

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

# Defaults
DEFAULT_SCAN_INTERVAL_HOURS = 6


class NetzNoeConfigData(TypedDict):
    """Typed config entry data for this integration."""

    meter_id: str
    password: str
    username: str
