# Control Loop Design: Rate Limits and Spec Errors

## Goals
- Dynamically cap per-user startup rates when user-controlled “specification errors” spike.
- Dynamically manage (source,destination) startup capacity using AIMD (additive increase, multiplicative decrease) guided by error rates and estimated startup cost.
- Keep knobs configurable (expressions, thresholds, EWMA windows, AIMD factors).

## Loop A: Specification Error Guardrail (per user)
- **Definition**: A specification error is any transfer that matches a user-configurable expression evaluated against the transfer ad (e.g., missing namespace, bad filename pattern, missing credentials). Expression syntax should reuse existing ClassAd/eval machinery.
- **Inputs**:
  - Per-user transfer attempts and their classification (spec-error or not).
  - EWMA of user transfer startup rate over the trailing 1 minute (configurable window, default 60s, alpha ~0.5 suggested).
- **Control law**:
  - Track spec-error rate over a trailing window (default 15 minutes).
  - If spec-error rate > 1%:
    - On first transition into “above threshold,” set `user_max_start_rate` to the current EWMA startup rate.
    - Apply multiplicative decrease by 0.5 (halve) when above threshold.
  - Never drop below floor: 1 job / minute.
  - When rate < threshold for a grace window (e.g., 15 minutes), allow slow additive recovery (e.g., +1 job/minute) up to an upper bound (optional; default unbounded except by config cap).
- **State**:
  - Per-user: current max rate, last-above-threshold flag/timestamp, EWMA of start rate, spec-error counters.
- **Actuation**:
  - Provide `proposed_max_rate_per_user` to the scheduler; scheduler enforces per-user start limit.

## Loop B: AIMD for (Source, Destination) Pairs
- **Configuration**:
  - Expressions to derive `source_key` and `destination_key` from job & machine ads (e.g., site, cache, origin, VO).
  - Expression for per-job cost (bytes or runtime); default to estimated input sandbox size or a fixed default (e.g., 10 GB) when unknown.
  - Thresholds for error-rate bands and cost bands.
  - AIMD parameters: additive increase (default 1000 GB/min), multiplicative decrease (default 0.5).
- **Metrics**:
  - Error rate over trailing 15 minutes for non-spec errors (per (src,dst)). Bands:
    - Green < 0.5%
    - Yellow 0.5–5%
    - Red > 5%
  - Startup cost as % of runtime spent in stage-in (per (src,dst)). Bands:
    - Green < 10%
    - Yellow 10–30%
    - Red > 30%
- **Computation**:
  - Maintain moving windows of successful transfers to estimate:
    - Average input transfer rate (exclude short transfers < 1 min per requirement and any with negligible bytes).
    - Typical sandbox size per user/job (median/percentile, configurable fallback default).
    - Estimated job runtime (if available) to compute stage-in %; otherwise mark as missing (see gaps below).
  - Classify current state: green if both metrics green; yellow if any yellow and none red; red if any red.
- **Control law**:
  - Green: increase capacity by additive increment (GB/min) toward a configurable ceiling.
  - Yellow: hold steady (no increase).
  - Red: multiply current capacity by multiplicative factor (default 0.5), not below a floor (configurable, default 1 job/min equivalent or 10 GB/min computed from average job cost).
  - Capacity is expressed in throughput (GB/min); translate to job start rate by dividing by per-job cost.
- **State**:
  - Per (src,dst): current capacity (GB/min), last state color, recent error counters, recent cost samples.

## Configuration Knobs (defaults)
- Spec-error threshold: 1%.
- Spec-error window: 15 minutes.
- Spec-error EWMA window: 60s (alpha 0.5).
- Spec-error min rate: 1 job/min.
- Spec-error recovery additive step: 1 job/min (configurable); recovery delay: 15m green before increasing.
- AIMD additive: 1000 GB/min (green only).
- AIMD multiplicative: 0.5 (red triggers).
- Error-rate bands: green <0.5%, yellow <5%, red >=5%.
- Cost bands: green <10%, yellow <=30%, red >30%.
- Cost floor ignore: transfers with estimated transfer time <1 minute are excluded from cost percentage.
- Per-job cost default: 10 GB if sandbox unknown.
- Min capacity: 1 job/min or 10 GB/min equivalent.

## Data & Evaluation
- Expressions: use existing ClassAd-style eval for transfer ads (spec errors) and job/machine ads (src/dst keys, cost derivation).
- Counters: reuse transfer history / tracker for windowed rates, bytes, durations; need per (src,dst) bucketing keyed by evaluated expressions.
- EWMA: maintain per-user EWMA of start rate (count of starts / window).
- Windows: implement sliding windows using ring buffers or timestamped bins for 1m, 15m.

## Implementation Notes / Gaps
- **Job runtime estimates**: Not currently available; required to compute stage-in % cost. TODO: source from job ads if provided (e.g., RequestedWallTime) or historical runtime per user.
 - **Job runtime estimates**: Now sourced from HTCondor job epoch history when available; successful job runtime samples are ingested into stats tracking alongside transfer epochs.
- **Sandbox size estimate**: If not in ads, derive from recent successful inputs per user/job; fallback remains default.
- **Expressions library**: Ensure safe evaluation against ads; may need to extend config parsing for source/destination/cost and spec-error predicates.
- **Per-(src,dst) tracking**: Add data structures to tracker/state for error counts, cost samples, capacity state.
- **Actuation path**: Need an interface for scheduler to consume proposed capacities (per-user rate caps and per-(src,dst) GB/min caps).
- **Recovery heuristics**: Define hysteresis to avoid flapping (e.g., require sustained green/yellow periods before changing state).
- **Testing**: Add unit tests for band classification, EWMA, AIMD steps, and spec-error threshold transitions.
