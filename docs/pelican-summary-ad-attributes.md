# PelicanSummary ClassAd Attributes

## Overview

PelicanSummary ClassAds track individual file transfer **attempts** for a specific endpoint and direction. Each transfer attempt (including retries) is counted and measured separately. These ads are published by `pelican_man` to provide visibility into transfer performance at the attempt level.

**Key Distinction**: PelicanSummary ads track *attempts*, not *epochs*. A job epoch may have multiple transfer attempts (e.g., due to retries), and each attempt is counted independently.

## Required Identification Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `MyType` | String | Always `"PelicanSummary"` |
| `Name` | String | Unique identifier: `<user>@<site>/<endpoint>/<direction>` |
| `PelicanUser` | String | HTCondor user (Owner attribute) |
| `PelicanSite` | String | Site name (ScheddName attribute) |
| `PelicanEndpoint` | String | Storage endpoint URL (scheme + host) |
| `PelicanDirection` | String | Transfer direction: `"download"` or `"upload"` |

## Federation Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `FederationSourcePrefix` | String | Pelican federation source prefix (only for download) |
| `FederationDestinationPrefix` | String | Pelican federation destination prefix (only for upload) |

## Window Attempt Metrics

All window metrics are computed over a rolling time window (typically 10 minutes).

### Counts

| Attribute | Type | Unit | Description |
|-----------|------|------|-------------|
| `WindowAttemptSuccessCount` | Integer | attempts | Number of successful transfer attempts in window |
| `WindowAttemptFailureCount` | Integer | attempts | Number of failed transfer attempts in window |
| `WindowAttemptCount` | Integer | attempts | Total attempts in window (success + failure) |

### Bytes Transferred

| Attribute | Type | Unit | Description |
|-----------|------|------|-------------|
| `WindowAttemptSuccessBytes` | Integer | bytes | Total bytes transferred in successful attempts |
| `WindowAttemptFailureBytes` | Integer | bytes | Total bytes transferred in failed attempts |
| `WindowAttemptBytes` | Integer | bytes | Total bytes transferred in all attempts |

### Duration

| Attribute | Type | Unit | Description |
|-----------|------|------|-------------|
| `WindowAttemptSuccessDurationSec` | Float | seconds | Sum of durations for successful attempts |
| `WindowAttemptFailureDurationSec` | Float | seconds | Sum of durations for failed attempts |
| `WindowAttemptDurationSec` | Float | seconds | Total duration for all attempts |

### Success Rate

| Attribute | Type | Unit | Description |
|-----------|------|------|-------------|
| `WindowAttemptSuccessRate` | Float | ratio | Success rate: `success_count / (success_count + failure_count)` |

## Total Attempt Metrics

All total metrics are cumulative since the daemon started.

### Counts

| Attribute | Type | Unit | Description |
|-----------|------|------|-------------|
| `TotalAttemptSuccessCount` | Integer | attempts | Total successful attempts since daemon start |
| `TotalAttemptFailureCount` | Integer | attempts | Total failed attempts since daemon start |
| `TotalAttemptCount` | Integer | attempts | Total attempts since daemon start |

### Bytes Transferred

| Attribute | Type | Unit | Description |
|-----------|------|------|-------------|
| `TotalAttemptSuccessBytes` | Integer | bytes | Total bytes in successful attempts |
| `TotalAttemptFailureBytes` | Integer | bytes | Total bytes in failed attempts |
| `TotalAttemptBytes` | Integer | bytes | Total bytes in all attempts |

### Duration

| Attribute | Type | Unit | Description |
|-----------|------|------|-------------|
| `TotalAttemptSuccessDurationSec` | Float | seconds | Sum of durations for all successful attempts |
| `TotalAttemptFailureDurationSec` | Float | seconds | Sum of durations for all failed attempts |
| `TotalAttemptDurationSec` | Float | seconds | Total duration for all attempts |

### Success Rate

| Attribute | Type | Unit | Description |
|-----------|------|------|-------------|
| `TotalAttemptSuccessRate` | Float | ratio | Overall success rate: `success_count / (success_count + failure_count)` |

## Window Rate Statistics

Distribution statistics for transfer rates (bytes/sec) of successful attempts in the rolling window.

| Attribute | Type | Unit | Description |
|-----------|------|------|-------------|
| `WindowRateAvgBytesPerSec` | Float | bytes/sec | Mean transfer rate |
| `WindowRateMedianBytesPerSec` | Float | bytes/sec | Median transfer rate (50th percentile) |
| `WindowRateP10BytesPerSec` | Float | bytes/sec | 10th percentile transfer rate |
| `WindowRateP90BytesPerSec` | Float | bytes/sec | 90th percentile transfer rate |

## Metadata Attributes

| Attribute | Type | Unit | Description |
|-----------|------|------|-------------|
| `LastHeardFrom` | Integer | Unix timestamp | Last time this ad was updated |

## Notes

- **Attempt-Level Focus**: All metrics count individual file transfer attempts. If a job epoch has 3 retries, that counts as 3 separate attempts.
- **No Epoch Metrics**: Unlike PelicanLimit ads, PelicanSummary does not track epoch-level information (ClusterID.ProcID.RunInstanceID).
- **No Wall-Clock Times**: Wall-clock calculations (earliest start to latest end) span multiple attempts and are not included at the attempt level.
- **No Control Metrics**: Control loop decisions (error bands, cost bands, limits) apply at the user+site level and are tracked in PelicanLimit ads.
- **Rate Statistics**: Only computed for successful attempts; failures typically have zero or minimal bytes transferred.
