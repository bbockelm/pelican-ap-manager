# pelican-ap-manager

`pelican_man` is a lightweight daemon intended to run under `condor_master`. It follows pelican-based HTCondor transfer activity via the golang-htcondor bindings, aggregates recent transfer history by user, endpoint, site, and direction (upload/download), and advertises summaries to the collector. State is periodically serialized so the daemon can resume after restarts.

## Features (phase 1)
- Poll transfer epoch history and queue metadata.
- Track aggregates per user, endpoint, site, and transfer direction.
- Persist progress to disk to survive restarts.
- Advertise summary ClassAds to the collector.
- HTCondor-backed client (using golang-htcondor).

## Building
Requires `golang-htcondor` (and its HTCondor development libraries) on the system. The default build always links against HTCondor.

```bash
cd .
go build ./cmd/pelican_man
```

## Configuration
Configuration is sourced from the active HTCondor configuration (same lookup as `condor_config`).
Prefer setting macros rather than ad-hoc environment variables:

- `PELICAN_MANAGER_POLL_INTERVAL` (default: `30s`)
- `PELICAN_MANAGER_ADVERTISE_INTERVAL` (default: `1m`)
- `PELICAN_MANAGER_EPOCH_LOOKBACK` (default: `24h`)
- `PELICAN_MANAGER_STATS_WINDOW` (default: `1h`; rolling window for in-memory stats)
- `PELICAN_MANAGER_DIRECTOR_CACHE_TTL` (default: `15m`; cache for director lookups)
- `PELICAN_MANAGER_STATE_PATH` (default: `./data/pelican_state.json`)
- `PELICAN_MANAGER_COLLECTOR_HOST` (default: `localhost:9618`; falls back to `COLLECTOR_HOST`)
- `PELICAN_MANAGER_SCHEDD_NAME` (optional: specific schedd to query; falls back to `SCHEDD_NAME`)
- `PELICAN_MANAGER_SITE_ATTRIBUTE` (default: `Site`)

## Runtime notes
- The daemon writes state to the configured path; ensure the parent directory is writable by `condor_master`.
- When built with the `condor` tag, `condor_master` should launch `pelican_man` so it can advertise to the collector.
- Future phases will add machine-learning-derived rate-limiter hints to the advertised ClassAds.
