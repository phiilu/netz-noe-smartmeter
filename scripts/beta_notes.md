**Beta for v$final.** Please report findings in the linked issues or open a new one.

## Install

- **Marketplace** (Home Assistant 2026.11 and newer): Settings → Devices & services → Marketplace → Netz NÖ Smart Meter, enable the "Pre-release" switch (it starts disabled) and turn it on. v$version then shows up under Settings → Updates; the Marketplace checks every 6 hours. Install it, then restart Home Assistant.
- **HACS** (older versions): ⋮ → Redownload → "Need a different version?" → pick v$version, then restart Home Assistant.

## What to test

$test_focus

## Rollback

- **Marketplace:** Settings → Marketplace → Netz NÖ Smart Meter, ⋮ → Install another version → $latest_final, then restart. Turn the "Pre-release" switch off again to stay on final releases.
- **HACS:** ⋮ → Redownload → choose $latest_final, then restart.

$changes
