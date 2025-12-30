# Schedd Startup Limits Implementation Summary

## Overview

This implementation adds support for golang-htcondor v0.0.6's schedd startup limits API to dynamically control job start rates based on transfer performance metrics.

## Changes Made

### 1. Dependency Update

**File**: `go.mod`
- Updated `github.com/bbockelm/golang-htcondor` from v0.0.5 to v0.0.6
- This new version adds the `CreateStartupLimit`, `QueryStartupLimits` APIs

### 2. Control Algorithm Enhancements

**File**: `internal/control/control.go`
- Exported `Band` type and constants (`BandGreen`, `BandYellow`, `BandRed`)
- Exported `ClassifyBand()` function for use by limit manager
- These allow the limit manager to determine when pairs are in RED state

### 3. Limit Manager Implementation

**File**: `internal/daemon/limit_manager.go` (NEW - 322 lines)

Key features:
- **RED-state-only enforcement**: Limits are only created when error rate or stage-in cost exceeds RED thresholds
- **60-second intervals**: Aligned with HTCondor negotiation cycle
- **EWMA-based rate estimation**: Uses exponentially weighted moving average to estimate job data cost
- **Automatic removal**: Limits removed after 600s of inactivity when pair exits RED state
- **Dynamic updates**: Limits updated when rate changes by >20%

Core components:
- `limitManager`: Tracks active limits and their usage
- `limitState`: Per-limit metadata (UUID, last hit time, rate count)
- `updateLimits()`: Main synchronization method called every ~60s
- `calculateRateCount()`: Converts GB/min capacity to jobs/interval using EWMA
- `refreshLimitStats()`: Queries schedd to update hit times

### 4. Service Integration

**File**: `internal/daemon/service.go`

Changes:
- Added `limitMgr` and `schedd` fields to Service struct
- Added `updateScheddLimits()` method called during advertise cycle
- Added `ensureLimitManager()` for lazy initialization of schedd connection
- Integrated limit updates into the 60-second advertise cycle

### 5. Documentation

**File**: `docs/control-algorithm.md` (NEW - 241 lines)

Comprehensive documentation covering:
- AIMD controller parameters and behavior
- Schedd startup limit lifecycle (creation, updates, removal)
- Rate calculation using EWMA
- Control cycle timing
- Configuration parameters
- Real-world examples
- Monitoring recommendations

**File**: `README.md`
- Updated runtime notes to reference control algorithm documentation
- Removed outdated "future phases" note about rate limiting

## Key Design Decisions

### 1. RED-State-Only Enforcement
Limits are only applied when absolutely needed (error rate > 5% OR stage-in cost > 30%). This prevents unnecessary throttling during normal operation.

### 2. EWMA for Job Cost Estimation
Rather than using a fixed job cost, the system uses recent transfer rates to estimate actual per-job data volume:
- Uses 0.2 alpha for smoothing (effective window ~10 intervals)
- Defaults to 10 GB when no data available
- Scales capacity (GB/min) to job starts (jobs/60s)

### 3. 60-Second Interval
The rate window matches HTCondor's negotiation cycle, ensuring limits are checked at the natural job scheduling frequency.

### 4. 10-Minute Expiration
Limits are removed after 10 minutes of not being hit (and exiting RED state). This balances:
- Quick removal when problems resolve
- Avoiding thrashing for intermittent issues

### 5. 20% Update Threshold
Limits are only updated when rate changes significantly (>20%), reducing unnecessary schedd API calls.

## Control Flow

```
Every 60 seconds (advertise interval):
  1. updatePairControllers() - Run AIMD for all pairs
  2. updateScheddLimits() - Synchronize limits with schedd
     a. Determine which pairs are in RED state
     b. Create/update limits for RED pairs
     c. Remove limits for pairs that exited RED and haven't been hit
     d. Query schedd to refresh hit times
  3. buildSummaryAds() - Generate ClassAds for collector
```

## Testing

All existing tests pass:
- Unit tests in `internal/condor`, `internal/daemon`, `internal/director`, etc.
- No test failures introduced
- Code compiles with and without `condor` build tag

## Future Enhancements

Potential improvements documented in `docs/control-algorithm.md`:
- Make more parameters configurable via HTCondor macros
- Support per-user spec-error guardrails
- Dynamic EWMA alpha adjustment
- Burst capacity support
