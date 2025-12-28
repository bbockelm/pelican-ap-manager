# Control Algorithm Documentation

This document describes the control algorithm parameters used by `pelican_man` to manage data transfer rates and schedd startup limits.

## Overview

The control system uses two complementary mechanisms:

1. **AIMD (Additive Increase Multiplicative Decrease)** for pair-level capacity management
2. **Schedd Startup Limits** to enforce rate limits at the HTCondor scheduler level

## Pair-Level AIMD Controller

The AIMD controller manages capacity for each (source, destination) pair based on error rates and stage-in costs.

### Parameters

#### Error Bands

- **`ErrorGreenThreshold`** (default: `0.005` = 0.5%)
  - Error rate below this threshold indicates healthy operation
  - System will additively increase capacity in green state

- **`ErrorYellowThreshold`** (default: `0.05` = 5%)
  - Error rate above green but below yellow indicates caution
  - System holds capacity steady in yellow state

- **Error RED**: Error rate above `ErrorYellowThreshold`
  - System multiplicatively decreases capacity
  - Schedd startup limit is activated

#### Cost Bands

Stage-in cost is measured as the percentage of job runtime spent transferring input data.

- **`CostGreenThresholdPercent`** (default: `10%`)
  - Cost below this indicates efficient data transfer
  - System will additively increase capacity

- **`CostYellowThresholdPercent`** (default: `30%`)
  - Cost above green but below yellow indicates acceptable overhead
  - System holds capacity steady

- **Cost RED**: Cost above `CostYellowThresholdPercent`
  - System multiplicatively decreases capacity
  - Schedd startup limit is activated

#### Capacity Adjustments

- **`AdditiveIncreaseGBPerMin`** (default: `1000` GB/min)
  - Amount added to capacity when in green state
  - Applied every control cycle (typically every 60s)

- **`MultiplicativeDecrease`** (default: `0.5`)
  - Multiplier applied to capacity when in red state
  - Halves capacity on each red cycle

- **`MinCapacityGBPerMin`** (default: `10` GB/min)
  - Floor on capacity to prevent complete throttling
  - Ensures minimal throughput is always available

#### Job Cost Estimation

- **`DefaultJobCostGB`** (default: `10` GB)
  - Fallback per-job data transfer cost when no metrics available
  - Used to translate capacity (GB/min) to job starts (jobs/min)

- **`MinJobStartPerMinute`** (default: `1` job/min)
  - Floor on job start rate
  - Ensures at least minimal job throughput

## Schedd Startup Limits

Startup limits are enforced at the HTCondor schedd level to control the rate of job starts for specific (source, destination) pairs.

### When Limits Are Applied

Limits are **only created or updated** when a pair enters the **RED state**:
- Error rate > `ErrorYellowThreshold`, OR
- Cost percentage > `CostYellowThresholdPercent`

This ensures limits are only imposed when there's a genuine problem, not preemptively.

### Limit Parameters

#### Rate Window

- **`defaultLimitInterval`** (default: `60` seconds)
  - Time window over which job starts are counted
  - Aligned with HTCondor negotiation cycle (~60s)
  - Jobs are limited to N starts per 60-second window

#### Rate Count Calculation

The number of jobs allowed per interval is calculated from the AIMD capacity:

```
capacityGBPerInterval = capacityGBPerMin * (intervalSeconds / 60)
jobsPerInterval = capacityGBPerInterval / jobCostGB
jobsPerInterval = max(jobsPerInterval, MinJobStartPerMinute * intervalMinutes)
```

Where `jobCostGB` is estimated using:
- Recent transfer rate (bytes/sec) from tracker
- Assumed average job duration (~5 minutes)
- EWMA smoothing with previous estimates

#### EWMA (Exponentially Weighted Moving Average)

- **`ewmaAlpha`** (default: `0.2`)
  - Smoothing factor for job cost estimation
  - Formula: `newEstimate = alpha * currentValue + (1 - alpha) * oldEstimate`
  - Lower alpha = more smoothing (slower response to changes)
  - Effective window ≈ 2/(alpha+1) ≈ 10 intervals

### Limit Lifecycle

#### Creation

Limits are created when:
1. A pair enters RED state for the first time
2. No existing limit exists for that pair

The limit includes:
- **Tag**: Stable hash of (source, destination) pair
- **Name**: Human-readable `pelican_<source>_to_<destination>`
- **Expression**: ClassAd expression matching jobs for this pair
- **RateCount**: Jobs allowed per interval
- **RateWindow**: Interval duration (60s)

#### Updates

Limits are updated when:
1. Pair remains in RED state
2. Rate count changes by more than 20%

Updates use the same UUID to modify the existing limit.

#### Removal

Limits are removed when:
1. Pair exits RED state (error and cost both below red thresholds)
2. Limit has not been hit (no jobs skipped) for `limitExpirationInactivity` (default: `600` seconds = 10 minutes)

This ensures limits don't linger after problems are resolved.

### Limit Monitoring

The system queries active limits every control cycle to:
- Update `lastHit` timestamp when jobs are skipped
- Track limit effectiveness
- Determine when limits can be safely removed

## Control Cycle Timing

### Poll Interval

- **`PELICAN_MANAGER_POLL_INTERVAL`** (default: `30s`)
  - How often to fetch new transfer/job epochs from HTCondor
  - Affects data freshness but not control actions

### Advertise Interval

- **`PELICAN_MANAGER_ADVERTISE_INTERVAL`** (default: `60s`)
  - How often to:
    - Update pair controller states
    - Synchronize schedd startup limits
    - Advertise summary ClassAds
  - This is the effective control cycle period

### Stats Window

- **`PELICAN_MANAGER_STATS_WINDOW`** (default: `1h`)
  - Rolling window for in-memory statistics
  - Used to calculate error rates and costs
  - Longer window = more stable metrics, slower response

## Configuration

All timing and threshold parameters can be overridden via HTCondor configuration macros:

```
# Control algorithm parameters
PELICAN_MANAGER_POLL_INTERVAL = 30s
PELICAN_MANAGER_ADVERTISE_INTERVAL = 60s
PELICAN_MANAGER_STATS_WINDOW = 1h

# AIMD parameters (future: configurable via macros)
# Currently set in code via control.DefaultConfig()
```

## Examples

### Scenario 1: Healthy Operation

- Error rate: 0.2% (green)
- Stage-in cost: 5% (green)
- Action: Capacity increases by 1000 GB/min
- Limit: None (no limit needed)

### Scenario 2: Moderate Errors

- Error rate: 3% (yellow)
- Stage-in cost: 8% (green)
- Action: Capacity held steady
- Limit: None (not in red state)

### Scenario 3: High Error Rate

- Error rate: 8% (red)
- Stage-in cost: 15% (yellow)
- Action: 
  - Capacity reduced by 50%
  - Schedd limit created: 50 jobs/60s
  - Limit enforced until error rate drops below 5%
  - Limit removed after 10 minutes of no hits

### Scenario 4: High Stage-In Cost

- Error rate: 0.5% (green)
- Stage-in cost: 40% (red)
- Action:
  - Capacity reduced by 50%
  - Schedd limit created based on reduced capacity
  - System gradually recovers as stage-in improves

## Monitoring

Key metrics to monitor:

1. **Pair Capacity** (`CapacityGBPerMin` in PelicanPair ads)
2. **Error Rate** (`ErrorRate` in PelicanPair ads)
3. **Stage-In Percentage** (`StageInPercent` in PelicanPair ads)
4. **Active Limits**: Query schedd with `condor_status -limit -schedd`

## Future Enhancements

- Make more parameters configurable via HTCondor macros
- Support per-user spec-error guardrails
- Dynamic adjustment of EWMA alpha based on variance
- Burst capacity for temporarily idle pairs
