# Changelog

## [0.4.0](https://github.com/phiilu/netz-noe-smartmeter/compare/v0.3.1...v0.4.0) (2026-11-23)


### Features

* add energy community (EEG) sensors ([#49](https://github.com/phiilu/netz-noe-smartmeter/issues/49)) ([966673e](https://github.com/phiilu/netz-noe-smartmeter/commit/966673e4ae3f8ba2e9e70a1a4c3dbc70cae243fc))
* import the energy community split as hourly statistics ([#52](https://github.com/phiilu/netz-noe-smartmeter/issues/52)) ([34304bb](https://github.com/phiilu/netz-noe-smartmeter/commit/34304bbcf39875f84ea2bf3243ea78a3ddd55b3a))


### Bug Fixes

* say which statistics the backfill clears ([#55](https://github.com/phiilu/netz-noe-smartmeter/issues/55)) ([2895120](https://github.com/phiilu/netz-noe-smartmeter/commit/2895120d2fbb2bf5be8c880c7dc946b57508eec5))


### Documentation

* keep the EEG sensors out of the Energy Dashboard ([#51](https://github.com/phiilu/netz-noe-smartmeter/issues/51)) ([ae7da51](https://github.com/phiilu/netz-noe-smartmeter/commit/ae7da5141f186c3577e480166f9d64d5ec5af07b))

## [0.3.1](https://github.com/phiilu/netz-noe-smartmeter/compare/v0.3.0...v0.3.1) (2026-11-02)


### Bug Fixes

* include Sunday in the weekly consumption ([#34](https://github.com/phiilu/netz-noe-smartmeter/issues/34)) ([b863210](https://github.com/phiilu/netz-noe-smartmeter/commit/b863210b44ee946fe411b5d8a07a950bd81008cd))
* keep the completed period until the new one is published ([#36](https://github.com/phiilu/netz-noe-smartmeter/issues/36)) ([d097d9e](https://github.com/phiilu/netz-noe-smartmeter/commit/d097d9ee5f54f4e0972cde353f55c7596ade4b40))
* keep the meter id out of the logs ([#44](https://github.com/phiilu/netz-noe-smartmeter/issues/44)) ([fd02a3d](https://github.com/phiilu/netz-noe-smartmeter/commit/fd02a3ddee277e12e2324f3a04c426c25e73a910))
* log in again when the portal session expired ([#39](https://github.com/phiilu/netz-noe-smartmeter/issues/39)) ([333691b](https://github.com/phiilu/netz-noe-smartmeter/commit/333691bfd802eb135935704657f79ecf23230b77))
* use state class total with last_reset for consumption sensors ([#27](https://github.com/phiilu/netz-noe-smartmeter/issues/27)) ([aac3620](https://github.com/phiilu/netz-noe-smartmeter/commit/aac36206eaa006fc14cf3a41b01c5ee229d4b20b))

## [0.3.0](https://github.com/phiilu/netz-noe-smartmeter/compare/v0.2.1...v0.3.0) (2026-10-25)


### Features

* report portal maintenance instead of a generic login error ([#24](https://github.com/phiilu/netz-noe-smartmeter/issues/24)) ([2de2d62](https://github.com/phiilu/netz-noe-smartmeter/commit/2de2d6273239d239af801b29cc5a35cca29967c0))

## [0.2.1](https://github.com/phiilu/netz-noe-smartmeter/compare/v0.2.0...v0.2.1) (2026-10-25)


### Bug Fixes

* redact meter id and statistic id from diagnostics ([#18](https://github.com/phiilu/netz-noe-smartmeter/issues/18)) ([96ad99b](https://github.com/phiilu/netz-noe-smartmeter/commit/96ad99be69fe5168729dedd2907ce2ede7bf1225))
