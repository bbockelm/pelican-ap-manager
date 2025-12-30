# pelican-ap-manager

`pelican_man` is a lightweight daemon designed to run under HTCondor's `condor_master`. It monitors Pelican-based data transfer activity, aggregates transfer statistics by user, endpoint, site, and direction, and publishes summary information to the HTCondor collector. The daemon automatically manages transfer capacity and enforces schedd startup limits to optimize data movement performance.

## Overview

The daemon provides the following capabilities:

- **Transfer Monitoring**: Continuously polls HTCondor transfer epoch history and job queue metadata to track Pelican data transfers
- **Statistics Aggregation**: Maintains rolling statistics per user, endpoint, site, and transfer direction (upload/download)
- **Collector Integration**: Advertises summary ClassAds to the HTCondor collector for centralized monitoring
- **Automatic Control**: Implements a control algorithm that dynamically adjusts transfer capacity and applies job startup limits based on observed performance
- **Persistent State**: Serializes progress to disk, allowing the daemon to resume tracking after restarts without data loss

## Installation

The daemon is intended to run as a service managed by `condor_master`. Add it to your HTCondor configuration:

```
DAEMON_LIST = $(DAEMON_LIST) PELICAN_MAN
PELICAN_MAN = /path/to/pelican_man
PELICAN_MAN_ARGS =
```

Ensure the daemon binary has appropriate permissions and that the state directory (see `PELICAN_MANAGER_STATE_PATH` below) is writable by the HTCondor user.

## Configuration

All configuration is sourced from HTCondor configuration macros (using the same lookup mechanism as `condor_config`). The daemon prefers HTCondor config macros over environment variables.

### Core Settings

- **`PELICAN_MANAGER_POLL_INTERVAL`** (default: `30s`)  
  How frequently to poll for new transfer epoch history

- **`PELICAN_MANAGER_ADVERTISE_INTERVAL`** (default: `1m`)  
  How frequently to advertise summary ClassAds to the collector

- **`PELICAN_MANAGER_EPOCH_LOOKBACK`** (default: `24h`)  
  How far back in time to query transfer history on startup

- **`PELICAN_MANAGER_STATS_WINDOW`** (default: `1h`)  
  Rolling time window for in-memory statistics aggregation

### Storage and Cache

- **`PELICAN_MANAGER_STATE_PATH`** (default: `./data/pelican_state.json`)  
  Path where the daemon persists its state. Ensure the parent directory exists and is writable.

- **`PELICAN_MANAGER_DIRECTOR_CACHE_TTL`** (default: `15m`)  
  Cache duration for Pelican director lookups

### HTCondor Integration

- **`PELICAN_MANAGER_COLLECTOR_HOST`** (default: uses `COLLECTOR_HOST` from HTCondor config, falls back to `localhost:9618`)  
  Collector address for advertising summary ClassAds

- **`PELICAN_MANAGER_SCHEDD_NAME`** (optional; defaults to `SCHEDD_NAME` from HTCondor config)  
  Specific schedd to query for job and transfer information. If not set, queries the local schedd.

- **`PELICAN_MANAGER_SITE_ATTRIBUTE`** (default: `Site`)  
  ClassAd attribute name used for site identification in aggregation

## Control Algorithm

The daemon implements an automatic control algorithm that monitors transfer performance and adjusts system limits accordingly. This includes:

- **Dynamic Capacity Management**: Adjusts transfer capacity based on observed throughput and utilization
- **Schedd Startup Limits**: Enforces limits on new job starts when transfers are heavily utilized, preventing transfer system saturation

For detailed information about control parameters and behavior, see [docs/control-algorithm.md](docs/control-algorithm.md).

The control algorithm includes documentation on:
- Control loop parameters and tuning
- Limit advertisement attributes
- Summary ClassAd attributes published to the collector

## Monitoring

The daemon advertises `PelicanSummary` ClassAds to the collector containing aggregated transfer statistics. You can query these using `condor_status`:

```bash
condor_status -any -constraint 'MyType == "PelicanSummary"'
```

See [docs/pelican-summary-ad-attributes.md](docs/pelican-summary-ad-attributes.md) for details on published attributes.

When schedd limits are active, `PelicanLimit` ClassAds are also advertised:

```bash
condor_status -any -constraint 'MyType == "PelicanLimit"'
```

See [docs/pelican-limit-ad-attributes.md](docs/pelican-limit-ad-attributes.md) for limit ClassAd attributes.

## Operational Notes

- The daemon requires HTCondor development libraries and uses the golang-htcondor bindings to interact with HTCondor
- State persistence ensures transfer tracking continuity across daemon restarts
- The daemon is designed to be lightweight and run continuously alongside other HTCondor daemons
- Log output goes to stdout/stderr and should be captured by `condor_master`'s logging mechanism

## Building and Development

### Building from Source

Requires `golang-htcondor` and HTCondor development libraries installed on the system:

```bash
go build ./cmd/pelican_man
```

### Development Workflow

#### Updating Test Data

Test data is collected from production systems and sanitized to protect user privacy. Sanitized data is excluded from git and must be regenerated:

```bash
# Collect fresh sanitized data from ap40.uw.osg-htc.org
make fetch-ap40-sanitized

# Regenerate golden reference files for tests
make regenerate-golden

# Or do both in one step
make update-testdata
```

The redaction process:
- Only tracks actual usernames from `Owner`, `AcctGroupUser`, and `OsUser` fields
- Preserves project names (e.g., `Georgetown_Joshi`) in `AcctGroup` and `ProjectName` fields
- Replaces usernames in `AccountingGroup` (e.g., `group_opportunistic.Project.user1`)
- Redacts paths while preserving already-redacted usernames (e.g., `/home/user1/...`)
- Maintains a stable `redaction_dict.json` for consistent anonymization across runs

### Running Tests

```bash
go test ./...
```
