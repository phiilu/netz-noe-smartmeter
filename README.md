# Netz NÖ Smart Meter

[![HACS](https://img.shields.io/badge/HACS-Custom-41BDF5.svg)](https://www.hacs.xyz/)
[![Release](https://img.shields.io/github/v/release/phiilu/netz-noe-smartmeter)](https://github.com/phiilu/netz-noe-smartmeter/releases)
[![Tests](https://github.com/phiilu/netz-noe-smartmeter/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/phiilu/netz-noe-smartmeter/actions/workflows/tests.yml)
[![Hassfest](https://github.com/phiilu/netz-noe-smartmeter/actions/workflows/hassfest.yml/badge.svg?branch=main)](https://github.com/phiilu/netz-noe-smartmeter/actions/workflows/hassfest.yml)
[![HACS Validate](https://github.com/phiilu/netz-noe-smartmeter/actions/workflows/hacs.yml/badge.svg?branch=main)](https://github.com/phiilu/netz-noe-smartmeter/actions/workflows/hacs.yml)
[![Coverage](https://codecov.io/gh/phiilu/netz-noe-smartmeter/graph/badge.svg?branch=main)](https://codecov.io/gh/phiilu/netz-noe-smartmeter)

**Unofficial** Home Assistant custom integration for reading smart meter data from the [Netz Niederösterreich](https://www.netz-noe.at) (Netz NÖ) customer portal.

> **Disclaimer:** This project is not affiliated with, endorsed by, or connected to EVN AG, Netz Niederösterreich GmbH, or any of their subsidiaries. All product names, trademarks, and registered trademarks are property of their respective owners. Use at your own risk.

## What it does

Reads your electricity consumption from the Netz NÖ smart meter portal and makes it available in Home Assistant — including the **Energy Dashboard**. Data is fetched every 6 hours (the portal updates once per day with ~24h delay).

## Features

- Daily, weekly, monthly, and yearly consumption sensors (kWh)
- Yearly sensor includes monthly breakdown and previous year comparison
- Backfill status diagnostic sensor with live progress details
- Manual refresh button to trigger an on-demand update
- Automatic import of 15-minute interval data as hourly statistics for the Energy Dashboard
- Backfill service to import all historical data back to meter installation
- Config flow with credential validation and reauth support
- Diagnostics for troubleshooting

## Requirements

- A Netz NÖ smart meter with portal access at [smartmeter.netz-noe.at](https://smartmeter.netz-noe.at)
- Your portal username and password from [smartmeter.netz-noe.at login](https://smartmeter.netz-noe.at/#/login?)
- Your Austrian meter ID (Zaehlpunkt), 33 characters starting with `AT` (found on your invoice or in the portal)

## Installation

### HACS (recommended)

1. Add this repository as a custom repository in HACS
2. Search for "Netz NÖ Smart Meter" and install it
3. Restart Home Assistant

[![Install with HACS](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=phiilu&repository=netz-noe-smartmeter&category=integration)

### Manual

1. Copy the `custom_components/netz_noe_smartmeter` folder into your Home Assistant `custom_components` directory
2. Restart Home Assistant

## Configuration

1. Go to **Settings > Devices & Services > Add Integration**
2. Search for "Netz NÖ Smart Meter"
3. Enter your portal credentials and meter ID

## Sensors

| Sensor | Description |
|--------|-------------|
| Daily consumption | Yesterday's total consumption |
| Weekly consumption | Current week's consumption (Mon–Sun) |
| Monthly consumption | Current month's consumption |
| Yearly consumption | Year-to-date consumption (with monthly breakdown as attributes) |
| Last updated | Timestamp of the newest data point |
| Backfill status | Backfill progress/result (diagnostic) |

## Button

| Button | Description |
|--------|-------------|
| Refresh data | Triggers an immediate coordinator refresh |

## Energy Dashboard

The integration automatically imports 15-minute interval data as hourly statistics. This powers the Home Assistant Energy Dashboard without needing a local meter reader. To add it, go to **Settings > Dashboards > Energy** and select the `netz_noe_smartmeter:grid_consumption` statistic under Grid consumption.

## Backfill historical data

To import historical data, call the `netz_noe_smartmeter.backfill_statistics` service from **Developer Tools > Services**:

```yaml
service: netz_noe_smartmeter.backfill_statistics
data:
  start_year: 2023  # optional, auto-detects if omitted
```

This fetches all available raw data in 30-day chunks. It may take a while depending on how many years of data are available.

## Disclaimer

This integration accesses the publicly available smart meter portal at smartmeter.netz-noe.at using your own credentials. It is provided as-is without any warranty. The author is not responsible for any issues arising from its use, including but not limited to account restrictions or data inaccuracies.

## License

[MIT](LICENSE)
