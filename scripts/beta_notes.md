**Beta for v$final.** Please report findings in the linked issues or open a new one.

## Install

- **Marketplace** (Home Assistant 2026.11 and newer): Settings → Marketplace → Netz NÖ Smart Meter, ⋮ → Install another version → v$version, then restart Home Assistant. To get the next betas as updates, turn on ⋮ → Include pre-releases.
- **HACS** (older versions): ⋮ → Redownload → "Need a different version?" → pick v$version, then restart Home Assistant.

## What to test

$test_focus

## Rollback

- **Marketplace:** Settings → Marketplace → Netz NÖ Smart Meter, ⋮ → Install another version → $latest_final, then restart. If you turned on ⋮ → Include pre-releases, turn it off again.
- **HACS:** ⋮ → Redownload → choose $latest_final, then restart.

$changes
