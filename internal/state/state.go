package state

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bbockelm/pelican-ap-manager/internal/control"
)

// Direction indicates whether a transfer is inbound or outbound from the Schedd perspective.
type Direction string

const (
	DirectionUpload   Direction = "upload"
	DirectionDownload Direction = "download"
)

// EpochID uniquely identifies a transfer epoch tuple within the HTCondor history.
type EpochID struct {
	ClusterID     int64 `json:"cluster_id"`
	ProcID        int64 `json:"proc_id"`
	RunInstanceID int64 `json:"run_instance_id"`
}

// Compare reports ordering between two epoch identifiers.
// Returns -1 if e < other, 0 if equal, and 1 if e > other.
func (e EpochID) Compare(other EpochID) int {
	switch {
	case e.ClusterID < other.ClusterID:
		return -1
	case e.ClusterID > other.ClusterID:
		return 1
	case e.ProcID < other.ProcID:
		return -1
	case e.ProcID > other.ProcID:
		return 1
	case e.RunInstanceID < other.RunInstanceID:
		return -1
	case e.RunInstanceID > other.RunInstanceID:
		return 1
	default:
		return 0
	}
}

// After reports whether the epoch occurs after the comparison target.
func (e EpochID) After(other EpochID) bool {
	return e.Compare(other) > 0
}

// IsZero indicates whether the epoch is unset.
func (e EpochID) IsZero() bool {
	return e.ClusterID == 0 && e.ProcID == 0 && e.RunInstanceID == 0
}

// String renders a compact tuple for logging.
func (e EpochID) String() string {
	return fmt.Sprintf("(%d,%d,%d)", e.ClusterID, e.ProcID, e.RunInstanceID)
}

// UnmarshalJSON supports legacy numeric epoch representations as well as structured tuples.
func (e *EpochID) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		*e = EpochID{}
		return nil
	}
	if data[0] != '{' {
		var legacy int64
		if err := json.Unmarshal(data, &legacy); err != nil {
			return err
		}
		*e = EpochID{RunInstanceID: legacy}
		return nil
	}
	var tmp struct {
		ClusterID     int64 `json:"cluster_id"`
		ProcID        int64 `json:"proc_id"`
		RunInstanceID int64 `json:"run_instance_id"`
	}
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	*e = EpochID{ClusterID: tmp.ClusterID, ProcID: tmp.ProcID, RunInstanceID: tmp.RunInstanceID}
	return nil
}

// SummaryKey uniquely identifies a transfer summary bucket.
type SummaryKey struct {
	User      string
	Endpoint  string
	Site      string
	Direction Direction
}

// SummaryStats aggregates outcomes for a summary bucket.
type SummaryStats struct {
	Successes          int                        `json:"successes"`
	Failures           int                        `json:"failures"`
	SuccessBytes       int64                      `json:"success_bytes"`
	FailureBytes       int64                      `json:"failure_bytes"`
	SuccessDurationSec float64                    `json:"success_duration_sec"`
	FailureDurationSec float64                    `json:"failure_duration_sec"`
	LastUpdated        time.Time                  `json:"last_updated"`
	Federations        map[string]FederationStats `json:"federations,omitempty"`
}

// FederationStats tracks outcomes for a specific federation prefix.
type FederationStats struct {
	Successes          int     `json:"successes"`
	Failures           int     `json:"failures"`
	SuccessBytes       int64   `json:"success_bytes"`
	FailureBytes       int64   `json:"failure_bytes"`
	SuccessDurationSec float64 `json:"success_duration_sec"`
	FailureDurationSec float64 `json:"failure_duration_sec"`
}

// EpochCounts aggregates epoch-level metrics for a user+site pair.
type EpochCounts struct {
	SuccessCount        int
	FailureCount        int
	TotalCount          int
	SuccessWallClockSec float64
	FailureWallClockSec float64
}

// State persists progress through the transfer epoch history.
type State struct {
	LastEpoch       EpochID                           `json:"last_epoch"`
	Buckets         map[string]SummaryStats           `json:"buckets"`
	RecentTransfers map[string][]TransferHistoryEntry `json:"recent_transfers"`
	EpochBuckets    map[string][]TransferEpochRef     `json:"epoch_buckets,omitempty"`
	EpochIndex      map[string]string                 `json:"epoch_index,omitempty"`
	JobEpochs       map[string]JobEpochSample         `json:"job_epochs,omitempty"`
	EpochUsers      map[string]string                 `json:"epoch_users,omitempty"`
	BucketRuntimes  map[string][]BucketRuntimeSample  `json:"bucket_runtimes,omitempty"`
	PairStates      map[string]control.PairState      `json:"pair_states,omitempty"`
	LimitStates     map[string]control.PairState      `json:"limit_states,omitempty"`
	mu              sync.Mutex                        `json:"-"`
}

// TransferHistoryEntry stores a recent transfer for persistence across restarts.
type TransferHistoryEntry struct {
	User             string    `json:"user"`
	Endpoint         string    `json:"endpoint,omitempty"`
	Site             string    `json:"site,omitempty"`
	Source           string    `json:"source"`
	Destination      string    `json:"destination"`
	Direction        string    `json:"direction"`
	Bytes            int64     `json:"bytes"`
	DurationSeconds  float64   `json:"duration_seconds"`
	Success          bool      `json:"success"`
	EndedAt          time.Time `json:"ended_at"`
	Cached           bool      `json:"cached"`
	SandboxName      string    `json:"sandbox_name"`
	SandboxSize      int64     `json:"sandbox_size"`
	FederationPrefix string    `json:"federation_prefix,omitempty"`
	SandboxObject    string    `json:"sandbox_object,omitempty"`
	Epoch            EpochID   `json:"epoch,omitempty"`
	JobRuntimeSec    float64   `json:"job_runtime_sec,omitempty"`
}

// TransferEpochRef keeps track of which transfer epochs contributed to a bucket.
type TransferEpochRef struct {
	Epoch                EpochID   `json:"epoch"`
	EndedAt              time.Time `json:"ended_at"`
	DurationSec          float64   `json:"duration_sec"`
	WallClockDurationSec float64   `json:"wall_clock_duration_sec,omitempty"`
	User                 string    `json:"user,omitempty"`
	JobRuntimeSec        float64   `json:"job_runtime_sec,omitempty"`
	Source               string    `json:"source,omitempty"`
	Destination          string    `json:"destination,omitempty"`
}

// JobEpochSample persists a completed job epoch for later lookup.
type JobEpochSample struct {
	Epoch                EpochID   `json:"epoch"`
	User                 string    `json:"user"`
	Site                 string    `json:"site,omitempty"`
	RuntimeSec           float64   `json:"runtime_sec"`                      // Total duration (ActivationDuration)
	ExecutionDurationSec float64   `json:"execution_duration_sec,omitempty"` // Execution only (ActivationExecutionDuration)
	EndedAt              time.Time `json:"ended_at"`
}

// BucketRuntimeSample stores a runtime measurement mapped to a transfer bucket.
type BucketRuntimeSample struct {
	Epoch      EpochID   `json:"epoch"`
	RuntimeSec float64   `json:"runtime_sec"`
	EndedAt    time.Time `json:"ended_at"`
}

// New constructs an empty state with initialized fields.
func New() *State {
	return &State{
		Buckets:         make(map[string]SummaryStats),
		RecentTransfers: make(map[string][]TransferHistoryEntry),
		EpochBuckets:    make(map[string][]TransferEpochRef),
		EpochIndex:      make(map[string]string),
		JobEpochs:       make(map[string]JobEpochSample),
		EpochUsers:      make(map[string]string),
		BucketRuntimes:  make(map[string][]BucketRuntimeSample),
		PairStates:      make(map[string]control.PairState),
		LimitStates:     make(map[string]control.PairState),
	}
}

// KeyString formats a SummaryKey into a stable string for map use.
func KeyString(k SummaryKey) string {
	return fmt.Sprintf("user=%s|endpoint=%s|site=%s|dir=%s", k.User, k.Endpoint, k.Site, k.Direction)
}

// DecodeKey converts the internal map key format back into a SummaryKey structure.
func DecodeKey(raw string) SummaryKey {
	parts := map[string]string{}
	for _, piece := range strings.Split(raw, "|") {
		kv := strings.SplitN(piece, "=", 2)
		if len(kv) != 2 {
			continue
		}
		parts[kv[0]] = kv[1]
	}

	return SummaryKey{
		User:      parts["user"],
		Endpoint:  parts["endpoint"],
		Site:      parts["site"],
		Direction: Direction(parts["dir"]),
	}
}

// Update ingests a transfer record into the aggregate state.
// Update ingests a transfer record with outcome metrics into the aggregate state.
func (s *State) Update(k SummaryKey, success bool, bytes int64, duration time.Duration, when time.Time, federationPrefix string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stats := s.Buckets[KeyString(k)]
	if success {
		stats.Successes++
		stats.SuccessBytes += bytes
		stats.SuccessDurationSec += duration.Seconds()
	} else {
		stats.Failures++
		stats.FailureBytes += bytes
		stats.FailureDurationSec += duration.Seconds()
	}
	if when.After(stats.LastUpdated) {
		stats.LastUpdated = when
	}
	if stats.Federations == nil {
		stats.Federations = make(map[string]FederationStats)
	}

	if federationPrefix != "" {
		entry := stats.Federations[federationPrefix]
		if success {
			entry.Successes++
			entry.SuccessBytes += bytes
			entry.SuccessDurationSec += duration.Seconds()
		} else {
			entry.Failures++
			entry.FailureBytes += bytes
			entry.FailureDurationSec += duration.Seconds()
		}
		stats.Federations[federationPrefix] = entry
	}
	s.Buckets[KeyString(k)] = stats
}

// SetLastEpoch records the latest processed epoch identifier.
func (s *State) SetLastEpoch(epoch EpochID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastEpoch = epoch
}

// Snapshot returns a copy of the current state suitable for reporting.
func (s *State) Snapshot() (EpochID, map[string]SummaryStats) {
	s.mu.Lock()
	defer s.mu.Unlock()

	copyBuckets := make(map[string]SummaryStats, len(s.Buckets))
	for k, v := range s.Buckets {
		copyBuckets[k] = v
	}
	return s.LastEpoch, copyBuckets
}

// CountEpochs returns epoch-level aggregates for a user+site pair.
func (s *State) CountEpochs(user, site string) EpochCounts {
	s.mu.Lock()
	defer s.mu.Unlock()

	counts := EpochCounts{}
	seenEpochs := make(map[EpochID]bool)
	epochSuccess := make(map[EpochID]bool)
	epochWallClock := make(map[EpochID]float64)

	// Iterate through all buckets for this user+site
	for key, refs := range s.EpochBuckets {
		decoded := DecodeKey(key)
		if decoded.User != user || decoded.Site != site {
			continue
		}

		// Track unique epochs and their success status
		for _, ref := range refs {
			if !seenEpochs[ref.Epoch] {
				seenEpochs[ref.Epoch] = true
				counts.TotalCount++
			}

			// Track wall-clock time (use maximum for each epoch)
			if ref.WallClockDurationSec > epochWallClock[ref.Epoch] {
				epochWallClock[ref.Epoch] = ref.WallClockDurationSec
			}
		}
	}

	// Determine success/failure for each epoch by checking JobEpochs
	for epoch := range seenEpochs {
		// Check if this epoch exists in JobEpochs (completed successfully)
		epochKey := epoch.String()
		if jobEpoch, exists := s.JobEpochs[epochKey]; exists && jobEpoch.RuntimeSec > 0 {
			epochSuccess[epoch] = true
			counts.SuccessCount++
			counts.SuccessWallClockSec += epochWallClock[epoch]
		} else {
			counts.FailureCount++
			counts.FailureWallClockSec += epochWallClock[epoch]
		}
	}

	return counts
}

// Load attempts to read state from a JSON file, returning a new State.
func Load(path string) (*State, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return New(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("read state: %w", err)
	}

	st := New()
	if err := json.Unmarshal(data, st); err != nil {
		return nil, fmt.Errorf("unmarshal state: %w", err)
	}
	if st.Buckets == nil {
		st.Buckets = make(map[string]SummaryStats)
	}
	if st.RecentTransfers == nil {
		st.RecentTransfers = make(map[string][]TransferHistoryEntry)
	}
	if st.EpochBuckets == nil {
		st.EpochBuckets = make(map[string][]TransferEpochRef)
	}
	if st.EpochIndex == nil {
		st.EpochIndex = make(map[string]string)
	}
	if st.JobEpochs == nil {
		st.JobEpochs = make(map[string]JobEpochSample)
	}
	if st.EpochUsers == nil {
		st.EpochUsers = make(map[string]string)
	}
	if st.BucketRuntimes == nil {
		st.BucketRuntimes = make(map[string][]BucketRuntimeSample)
	}
	if st.PairStates == nil {
		st.PairStates = make(map[string]control.PairState)
	}
	return st, nil
}

// Save writes the state to disk, ensuring the parent directory exists.
func (s *State) Save(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}

	payload, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}

	if err := os.WriteFile(path, payload, 0o644); err != nil {
		return fmt.Errorf("write state: %w", err)
	}
	return nil
}

// AppendRecent adds recent transfer entries for a user, pruning to the provided window.
func (s *State) AppendRecent(window time.Duration, user string, entries []TransferHistoryEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-window)
	existing := s.RecentTransfers[user]
	pruned := existing[:0]
	for _, e := range existing {
		if e.EndedAt.After(cutoff) {
			pruned = append(pruned, e)
		}
	}
	for _, e := range entries {
		if e.EndedAt.After(cutoff) {
			pruned = append(pruned, e)
		}
	}
	s.RecentTransfers[user] = pruned
}

// ResetRecent clears all cached recent transfers.
func (s *State) ResetRecent() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.RecentTransfers = make(map[string][]TransferHistoryEntry)
}

// SnapshotRecent returns a deep copy of recent transfers.
func (s *State) SnapshotRecent() map[string][]TransferHistoryEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	copyMap := make(map[string][]TransferHistoryEntry, len(s.RecentTransfers))
	for user, entries := range s.RecentTransfers {
		cpy := make([]TransferHistoryEntry, len(entries))
		copy(cpy, entries)
		copyMap[user] = cpy
	}
	return copyMap
}

// AppendEpochBuckets records transfer epochs for a bucket and prunes to the retention window.
func (s *State) AppendEpochBuckets(window time.Duration, bucket string, refs []TransferEpochRef) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.EpochBuckets == nil {
		s.EpochBuckets = make(map[string][]TransferEpochRef)
	}
	if s.EpochIndex == nil {
		s.EpochIndex = make(map[string]string)
	}

	cutoff := time.Now().Add(-window)
	existing := s.EpochBuckets[bucket]
	pruned := existing[:0]
	for _, r := range existing {
		if r.EndedAt.After(cutoff) {
			pruned = append(pruned, r)
		} else {
			delete(s.EpochIndex, epochKey(r.Epoch))
		}
	}

	for _, r := range refs {
		if !r.EndedAt.After(cutoff) {
			continue
		}
		pruned = append(pruned, r)
		s.EpochIndex[epochKey(r.Epoch)] = bucket
	}

	s.EpochBuckets[bucket] = pruned
}

// AppendJobEpoch stores a completed job epoch sample for lookup and pruning.
func (s *State) AppendJobEpoch(window time.Duration, sample JobEpochSample) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.JobEpochs == nil {
		s.JobEpochs = make(map[string]JobEpochSample)
	}
	if s.EpochUsers == nil {
		s.EpochUsers = make(map[string]string)
	}

	cutoff := time.Now().Add(-window)
	for key, val := range s.JobEpochs {
		if val.EndedAt.Before(cutoff) {
			delete(s.JobEpochs, key)
			delete(s.EpochUsers, key)
		}
	}

	key := epochKey(sample.Epoch)
	s.JobEpochs[key] = sample
	if sample.User != "" {
		s.EpochUsers[key] = sample.User
		cpKey := clusterProcKey(sample.Epoch)
		if _, ok := s.EpochUsers[cpKey]; !ok {
			s.EpochUsers[cpKey] = sample.User
		}
	}
}

// AppendBucketRuntime records a runtime measurement for a bucket with pruning.
func (s *State) AppendBucketRuntime(window time.Duration, bucket string, sample BucketRuntimeSample) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.BucketRuntimes == nil {
		s.BucketRuntimes = make(map[string][]BucketRuntimeSample)
	}

	cutoff := time.Now().Add(-window)
	existing := s.BucketRuntimes[bucket]
	pruned := existing[:0]
	for _, r := range existing {
		if r.EndedAt.After(cutoff) {
			pruned = append(pruned, r)
		}
	}
	if sample.EndedAt.After(cutoff) {
		pruned = append(pruned, sample)
	}
	s.BucketRuntimes[bucket] = pruned
}

// LookupBucketForEpoch returns the bucket that observed the given transfer epoch, if any.
func (s *State) LookupBucketForEpoch(epoch EpochID) (string, TransferEpochRef, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	bucket, ok := s.EpochIndex[epochKey(epoch)]
	if !ok {
		return "", TransferEpochRef{}, false
	}
	for _, ref := range s.EpochBuckets[bucket] {
		if ref.Epoch.Compare(epoch) == 0 {
			return bucket, ref, true
		}
	}
	return "", TransferEpochRef{}, false
}

// LookupUserForEpoch tries to resolve a user from prior job epochs by epoch or ClusterID/ProcID.
func (s *State) LookupUserForEpoch(epoch EpochID) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if user, ok := s.EpochUsers[epochKey(epoch)]; ok && user != "" {
		return user, true
	}
	if user, ok := s.EpochUsers[clusterProcKey(epoch)]; ok && user != "" {
		return user, true
	}
	return "", false
}

// BucketRuntimeStats returns count and total runtime seconds for a bucket within the retention window.
func (s *State) BucketRuntimeStats(window time.Duration, bucket string) (int, float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-window)
	samples := s.BucketRuntimes[bucket]
	count := 0
	total := 0.0
	pruned := samples[:0]
	for _, r := range samples {
		if !r.EndedAt.After(cutoff) {
			continue
		}
		count++
		total += r.RuntimeSec
		pruned = append(pruned, r)
	}
	s.BucketRuntimes[bucket] = pruned
	return count, total
}

// BucketStageInPercent estimates stage-in percent for a bucket using matched job runtimes.
func (s *State) BucketStageInPercent(window time.Duration, bucket string) (int, float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-window)
	refs := s.EpochBuckets[bucket]

	// aggregate transfer duration per epoch
	agg := make(map[string]struct {
		dur     float64
		runtime float64
		ended   time.Time
	})
	for _, r := range refs {
		if !r.EndedAt.After(cutoff) {
			continue
		}
		key := epochKey(r.Epoch)
		entry := agg[key]
		entry.dur += r.DurationSec
		if r.EndedAt.After(entry.ended) {
			entry.ended = r.EndedAt
		}
		if entry.runtime <= 0 && r.JobRuntimeSec > 0 {
			entry.runtime = r.JobRuntimeSec
		}
		agg[key] = entry
	}

	for key, entry := range agg {
		if entry.runtime > 0 {
			agg[key] = entry
			continue
		}
		if je, ok := s.JobEpochs[key]; ok && je.RuntimeSec > 0 && je.EndedAt.After(cutoff) {
			entry.runtime = je.RuntimeSec
			agg[key] = entry
		}
	}

	samples := 0
	percent := 0.0
	for key, entry := range agg {
		if entry.runtime <= 0 {
			delete(agg, key)
			continue
		}
		samples++
		percent += (entry.dur / entry.runtime) * 100.0
	}

	if samples == 0 {
		return 0, 0
	}
	return samples, percent / float64(samples)
}

// PairStageInPercent estimates stage-in percent for a (source,destination) pair over the retention window.
func (s *State) PairStageInPercent(window time.Duration, source, destination string) (int, float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-window)

	agg := make(map[string]struct {
		dur     float64
		runtime float64
		ended   time.Time
	})

	for _, refs := range s.EpochBuckets {
		for _, r := range refs {
			if !r.EndedAt.After(cutoff) {
				continue
			}
			if r.Source != source || r.Destination != destination {
				continue
			}
			key := epochKey(r.Epoch)
			entry := agg[key]
			entry.dur += r.DurationSec
			if r.EndedAt.After(entry.ended) {
				entry.ended = r.EndedAt
			}
			if entry.runtime <= 0 && r.JobRuntimeSec > 0 {
				entry.runtime = r.JobRuntimeSec
			}
			agg[key] = entry
		}
	}

	for key, entry := range agg {
		if entry.runtime > 0 {
			agg[key] = entry
			continue
		}
		if je, ok := s.JobEpochs[key]; ok && je.RuntimeSec > 0 && je.EndedAt.After(cutoff) {
			entry.runtime = je.RuntimeSec
			agg[key] = entry
		}
	}

	samples := 0
	percent := 0.0
	for key, entry := range agg {
		if entry.runtime <= 0 {
			delete(agg, key)
			continue
		}
		samples++
		percent += (entry.dur / entry.runtime) * 100.0
	}

	if samples == 0 {
		return 0, 0
	}
	return samples, percent / float64(samples)
}

// StageInStats contains statistics about stage-in percentage calculation for a user+site pair.
type StageInStats struct {
	Samples                int     // Number of epochs with both transfer and job data (used for calculation)
	Percent                float64 // Average stage-in percentage
	JobEpochs              int     // Total job epochs for this user+site in the window
	TransferEpochs         int     // Transfer epochs for this user+site in the window
	EpochsWithBoth         int     // Epochs that have both transfer and job data (same as Samples)
	ActivationTimeCount    int     // Number of job epochs with non-zero activation time (ActivationDuration)
	TotalActivationTimeSec float64 // Sum of total activation time (ActivationDuration) across all job epochs
	ExecutionTimeCount     int     // Number of job epochs with non-zero execution time (ActivationExecutionDuration)
	TotalExecutionTimeSec  float64 // Sum of execution time (ActivationExecutionDuration) across all job epochs
}

// UserSiteStageInPercent estimates stage-in percent for a (user,site) pair over the retention window.
// Returns statistics including counts of job epochs, transfer epochs, and epochs with both.
func (s *State) UserSiteStageInPercent(window time.Duration, user, site string) StageInStats {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-window)

	// Track which epochs have transfer data
	transferEpochSet := make(map[string]bool)

	agg := make(map[string]struct {
		dur          float64
		wallClockDur float64
		runtime      float64
		ended        time.Time
	})

	// Iterate over all bucket keys and filter by user and site
	// For each epoch, we want the maximum wall-clock duration across all buckets (representing the longest parallel transfer)
	// Sum durations across all buckets for backwards compatibility
	for bucketKey, refs := range s.EpochBuckets {
		summaryKey := DecodeKey(bucketKey)
		if summaryKey.User != user || summaryKey.Site != site {
			continue
		}

		for _, r := range refs {
			if !r.EndedAt.After(cutoff) {
				continue
			}
			key := epochKey(r.Epoch)
			transferEpochSet[key] = true
			entry := agg[key]
			entry.dur += r.DurationSec
			// Use the maximum wall-clock duration (for parallel transfers to different endpoints)
			if r.WallClockDurationSec > entry.wallClockDur {
				entry.wallClockDur = r.WallClockDurationSec
			}
			if r.EndedAt.After(entry.ended) {
				entry.ended = r.EndedAt
			}
			if entry.runtime <= 0 && r.JobRuntimeSec > 0 {
				entry.runtime = r.JobRuntimeSec
			}
			agg[key] = entry
		}
	}

	// Count total job epochs for this user+site and compute activation/execution time metrics
	totalJobEpochs := 0
	activationTimeCount := 0
	totalActivationTimeSec := 0.0
	executionTimeCount := 0
	totalExecutionTimeSec := 0.0
	for _, je := range s.JobEpochs {
		if je.User == user && je.Site == site && je.EndedAt.After(cutoff) {
			totalJobEpochs++
			if je.RuntimeSec > 0 {
				activationTimeCount++
				totalActivationTimeSec += je.RuntimeSec
			}
			if je.ExecutionDurationSec > 0 {
				executionTimeCount++
				totalExecutionTimeSec += je.ExecutionDurationSec
			}
		}
	}

	// Try to find job runtime data for transfer epochs
	for key, entry := range agg {
		if entry.runtime > 0 {
			agg[key] = entry
			continue
		}
		if je, ok := s.JobEpochs[key]; ok && je.RuntimeSec > 0 && je.EndedAt.After(cutoff) {
			entry.runtime = je.RuntimeSec
			agg[key] = entry
		}
	}

	samples := 0
	percent := 0.0
	for key, entry := range agg {
		if entry.runtime <= 0 {
			delete(agg, key)
			continue
		}
		samples++
		// Prefer wall-clock duration (accounts for parallel transfers), fall back to summed duration
		transferDur := entry.wallClockDur
		if transferDur == 0 {
			transferDur = entry.dur
		}
		// Stage-in percentage = transfer time / total job time × 100
		percent += (transferDur / entry.runtime) * 100.0
	}

	avgPercent := 0.0
	if samples > 0 {
		avgPercent = percent / float64(samples)
	}

	return StageInStats{
		Samples:                samples,
		Percent:                avgPercent,
		JobEpochs:              totalJobEpochs,
		TransferEpochs:         len(transferEpochSet),
		EpochsWithBoth:         samples,
		ActivationTimeCount:    activationTimeCount,
		TotalActivationTimeSec: totalActivationTimeSec,
		ExecutionTimeCount:     executionTimeCount,
		TotalExecutionTimeSec:  totalExecutionTimeSec,
	}
}

// PairState returns the stored controller state for a (source,destination) pair, if any.
func (s *State) PairState(source, destination string) control.PairState {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.PairStates[pairKey(source, destination)]
}

// SetPairState persists controller state for a (source,destination) pair.
func (s *State) SetPairState(source, destination string, st control.PairState) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.PairStates == nil {
		s.PairStates = make(map[string]control.PairState)
	}
	s.PairStates[pairKey(source, destination)] = st
}

// SnapshotPairStates returns a copy of all stored pair states.
func (s *State) SnapshotPairStates() map[string]control.PairState {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make(map[string]control.PairState, len(s.PairStates))
	for k, v := range s.PairStates {
		out[k] = v
	}
	return out
}

// PairKeys returns all pair keys currently tracked in state.
func (s *State) PairKeys() []control.PairKey {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]control.PairKey, 0, len(s.PairStates))
	for raw := range s.PairStates {
		src, dst := decodePairKey(raw)
		if src == "" || dst == "" {
			continue
		}
		keys = append(keys, control.PairKey{Source: src, Destination: dst})
	}
	return keys
}

func epochKey(e EpochID) string {
	return fmt.Sprintf("%d/%d/%d", e.ClusterID, e.ProcID, e.RunInstanceID)
}

func clusterProcKey(e EpochID) string {
	return fmt.Sprintf("%d/%d", e.ClusterID, e.ProcID)
}

func pairKey(source, destination string) string {
	return fmt.Sprintf("src=%s|dst=%s", source, destination)
}

func decodePairKey(raw string) (string, string) {
	parts := strings.Split(raw, "|")
	if len(parts) != 2 {
		return "", ""
	}
	src := strings.TrimPrefix(parts[0], "src=")
	dst := strings.TrimPrefix(parts[1], "dst=")
	return src, dst
}

// limitKey creates a key for a (user,site) limit.
func limitKey(user, site string) string {
	return fmt.Sprintf("user=%s|site=%s", user, site)
}

// decodeLimitKey decodes a limit key into user and site.
func decodeLimitKey(raw string) (string, string) {
	parts := strings.Split(raw, "|")
	if len(parts) != 2 {
		return "", ""
	}
	user := strings.TrimPrefix(parts[0], "user=")
	site := strings.TrimPrefix(parts[1], "site=")
	return user, site
}

// LimitState returns the stored controller state for a (user,site) limit pair.
func (s *State) LimitState(user, site string) control.PairState {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.LimitStates[limitKey(user, site)]
}

// SetLimitState persists controller state for a (user,site) limit pair.
func (s *State) SetLimitState(user, site string, st control.PairState) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.LimitStates == nil {
		s.LimitStates = make(map[string]control.PairState)
	}
	s.LimitStates[limitKey(user, site)] = st
}

// SnapshotLimitStates returns a copy of all stored limit states.
func (s *State) SnapshotLimitStates() map[string]control.PairState {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make(map[string]control.PairState, len(s.LimitStates))
	for k, v := range s.LimitStates {
		out[k] = v
	}
	return out
}
