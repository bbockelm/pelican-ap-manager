package stats

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bbockelm/pelican-ap-manager/internal/state"
)

// ProcessedTransfer represents a single file transfer after enrichment.
type ProcessedTransfer struct {
	Epoch             state.EpochID
	User              string
	Endpoint          string
	Site              string
	Source            string
	Destination       string
	Direction         state.Direction
	FederationPrefix  string
	SandboxObject     string
	Bytes             int64
	Duration          time.Duration
	WallClockDuration time.Duration
	JobRuntime        time.Duration
	Success           bool
	EndedAt           time.Time
	Cached            bool
	SandboxName       string
	SandboxSize       int64
}

// Tracker maintains rolling, in-memory statistics for transfers over a window.
type Tracker struct {
	window  time.Duration
	mu      sync.Mutex
	perUser map[string][]ProcessedTransfer
	queue   []string
	sand    map[string]sandboxStat
}

type sandboxStat struct {
	Count int
	Size  int64
}

// SandboxFile captures a single transfer belonging to a sandbox.
// SandboxDetail aggregates normalized object paths for a sandbox.
type SandboxDetail struct {
	Name      string   `json:"name"`
	SizeBytes int64    `json:"size_bytes"`
	Paths     []string `json:"paths"`
	Successes int      `json:"successes"`
	Failures  int      `json:"failures"`
}

// NewTracker constructs a Tracker with the provided window duration.
func NewTracker(window time.Duration) *Tracker {
	return &Tracker{
		window:  window,
		perUser: make(map[string][]ProcessedTransfer),
		sand:    make(map[string]sandboxStat),
	}
}

// Load replaces the internal cache with provided data, pruning by the tracker's window.
func (t *Tracker) Load(data map[string][]ProcessedTransfer) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// RuntimeSample captures a completed job runtime for aggregation.
	type RuntimeSample struct {
		User    string
		Site    string
		Runtime time.Duration
		EndedAt time.Time
	}
	cutoff := time.Now().Add(-t.window)
	t.perUser = make(map[string][]ProcessedTransfer, len(data))
	t.queue = nil
	t.sand = make(map[string]sandboxStat)

	for user, entries := range data {
		for _, tr := range entries {
			if !tr.EndedAt.After(cutoff) {
				continue
			}
			t.perUser[user] = append(t.perUser[user], tr)
			if tr.SandboxName != "" {
				t.enqueueSandbox(tr.SandboxName, tr.SandboxSize)
			}
		}
	}
}

// Window returns the current stats window duration.
func (t *Tracker) Window() time.Duration {
	return t.window
}

// Add ingests a batch of processed transfers for a single user.
func (t *Tracker) Add(user string, transfers []ProcessedTransfer) {
	t.mu.Lock()
	defer t.mu.Unlock()

	cutoff := time.Now().Add(-t.window)
	existing := t.perUser[user]
	pruned := existing[:0]
	for _, tr := range existing {
		if tr.EndedAt.After(cutoff) {
			pruned = append(pruned, tr)
		}
	}
	for _, tr := range transfers {
		if tr.EndedAt.After(cutoff) {
			pruned = append(pruned, tr)
			if tr.SandboxName != "" {
				t.enqueueSandbox(tr.SandboxName, tr.SandboxSize)
			}
		}
	}
	t.perUser[user] = pruned
}

// TopSandboxes returns up to limit sandboxes sorted by count (descending).
func (t *Tracker) TopSandboxes(limit int) []SandboxEstimate {
	t.mu.Lock()
	defer t.mu.Unlock()

	if limit <= 0 {
		return nil
	}
	items := make([]SandboxEstimate, 0, len(t.sand))
	for name, st := range t.sand {
		items = append(items, SandboxEstimate{Name: name, Count: st.Count, SizeBytes: st.Size})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Count == items[j].Count {
			return items[i].Name < items[j].Name
		}
		return items[i].Count > items[j].Count
	})
	if len(items) > limit {
		items = items[:limit]
	}
	return items
}

// SandboxDetails returns sandbox entries with their constituent transfers, sorted by file destination then source.
func (t *Tracker) SandboxDetails() []SandboxDetail {
	t.mu.Lock()
	defer t.mu.Unlock()

	bySandbox := make(map[string]*SandboxDetail)

	for _, transfers := range t.perUser {
		for _, tr := range transfers {
			if tr.SandboxName == "" {
				continue
			}
			sd, ok := bySandbox[tr.SandboxName]
			if !ok {
				sd = &SandboxDetail{Name: tr.SandboxName}
				bySandbox[tr.SandboxName] = sd
			}
			if tr.SandboxSize > sd.SizeBytes {
				sd.SizeBytes = tr.SandboxSize
			}
			if tr.SandboxObject != "" {
				sd.Paths = append(sd.Paths, tr.SandboxObject)
			}
			if tr.Success {
				sd.Successes++
			} else {
				sd.Failures++
			}
		}
	}

	details := make([]SandboxDetail, 0, len(bySandbox))
	for _, sd := range bySandbox {
		if len(sd.Paths) > 0 {
			paths := uniqueSorted(sd.Paths)
			sd.Paths = paths
		}
		details = append(details, *sd)
	}

	sort.Slice(details, func(i, j int) bool {
		if len(details[i].Paths) == len(details[j].Paths) {
			return details[i].Name < details[j].Name
		}
		return len(details[i].Paths) > len(details[j].Paths)
	})

	return details
}

// AllTransfers returns a shallow copy of all tracked transfers inside the stats window.
func (t *Tracker) AllTransfers() []ProcessedTransfer {
	t.mu.Lock()
	defer t.mu.Unlock()

	var out []ProcessedTransfer
	for _, transfers := range t.perUser {
		out = append(out, transfers...)
	}
	return out
}

func uniqueSorted(in []string) []string {
	if len(in) == 0 {
		return in
	}
	sort.Strings(in)
	out := in[:0]
	var last string
	for i, v := range in {
		if i == 0 || v != last {
			out = append(out, v)
			last = v
		}
	}
	return out
}

func (t *Tracker) enqueueSandbox(name string, size int64) {
	const maxEntries = 1000

	st := t.sand[name]
	st.Count++
	if size > st.Size {
		st.Size = size
	}
	t.sand[name] = st
	t.queue = append(t.queue, name)

	if len(t.queue) <= maxEntries {
		return
	}

	old := t.queue[0]
	t.queue = t.queue[1:]
	if ost, ok := t.sand[old]; ok {
		ost.Count--
		if ost.Count <= 0 {
			delete(t.sand, old)
		} else {
			t.sand[old] = ost
		}
	}
}

// SandboxEstimate carries sandbox popularity and size.
type SandboxEstimate struct {
	Name      string
	Count     int
	SizeBytes int64
}

// AverageRate returns average bytes/sec for a source->dest pair over window, filtering short/small transfers.
func (t *Tracker) AverageRate(source, dest string) float64 {
	const minDuration = 10 * time.Second
	const minBytes = 500 * 1024 * 1024

	t.mu.Lock()
	defer t.mu.Unlock()

	var totalBytes int64
	var totalDuration time.Duration
	cutoff := time.Now().Add(-t.window)

	for _, transfers := range t.perUser {
		for _, tr := range transfers {
			if !tr.Success {
				continue
			}
			if tr.Source != source || tr.Destination != dest {
				continue
			}
			if tr.EndedAt.Before(cutoff) {
				continue
			}
			if tr.Duration < minDuration && tr.Bytes < minBytes {
				continue
			}
			totalBytes += tr.Bytes
			totalDuration += tr.Duration
		}
	}

	if totalDuration <= 0 {
		return 0
	}
	return float64(totalBytes) / totalDuration.Seconds()
}

// AverageInputSize returns average transfer size for a user (download direction assumed) over window.
func (t *Tracker) AverageInputSize(user string) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	var totalBytes int64
	var count int64
	cutoff := time.Now().Add(-t.window)
	for _, tr := range t.perUser[user] {
		if !tr.Success {
			continue
		}
		if tr.EndedAt.Before(cutoff) {
			continue
		}
		totalBytes += tr.Bytes
		count++
	}
	if count == 0 {
		return 0
	}
	return float64(totalBytes) / float64(count)
}

// AverageRuntime returns average job runtime for a user given per-transfer runtimes.
func (t *Tracker) AverageRuntime(user string) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	var total time.Duration
	var count int64
	cutoff := time.Now().Add(-t.window)
	for _, tr := range t.perUser[user] {
		if !tr.Success {
			continue
		}
		if tr.EndedAt.Before(cutoff) {
			continue
		}
		runtime := tr.JobRuntime
		if runtime <= 0 {
			// fallback: reuse Duration when caller injects runtime via transfer duration
			runtime = tr.Duration
		}
		if runtime <= 0 {
			continue
		}
		total += runtime
		count++
	}
	if count == 0 {
		return 0
	}
	return total.Seconds() / float64(count)
}

// AverageExecutionTime returns average job execution time (runtime minus transfer wall-clock) for a user.
// This represents the actual compute time, excluding I/O overhead.
func (t *Tracker) AverageExecutionTime(user string) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	var totalExecution time.Duration
	var count int64
	cutoff := time.Now().Add(-t.window)

	// Group by epoch to compute execution time per job
	epochData := make(map[string]struct {
		runtime   time.Duration
		wallClock time.Duration
	})

	for _, tr := range t.perUser[user] {
		if !tr.Success {
			continue
		}
		if tr.EndedAt.Before(cutoff) {
			continue
		}
		if tr.JobRuntime <= 0 {
			continue
		}

		epochKey := fmt.Sprintf("%d-%d-%d", tr.Epoch.ClusterID, tr.Epoch.ProcID, tr.Epoch.RunInstanceID)
		data := epochData[epochKey]
		if data.runtime == 0 {
			data.runtime = tr.JobRuntime
		}
		// Track the maximum wall-clock duration for this epoch (parallel transfers)
		if tr.WallClockDuration > data.wallClock {
			data.wallClock = tr.WallClockDuration
		}
		epochData[epochKey] = data
	}

	// Compute execution time for each epoch
	for _, data := range epochData {
		executionTime := data.runtime - data.wallClock
		if executionTime > 0 {
			totalExecution += executionTime
			count++
		}
	}

	if count == 0 {
		return 0
	}
	return totalExecution.Seconds() / float64(count)
}

// ErrorRate returns failure ratio for a source->dest pair over window.
func (t *Tracker) ErrorRate(source, dest string) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	var fail, total int64
	cutoff := time.Now().Add(-t.window)
	for _, transfers := range t.perUser {
		for _, tr := range transfers {
			if tr.Source != source || tr.Destination != dest {
				continue
			}
			if tr.EndedAt.Before(cutoff) {
				continue
			}
			total++
			if !tr.Success {
				fail++
			}
		}
	}
	if total == 0 {
		return 0
	}
	return float64(fail) / float64(total)
}

// DestinationErrorRate returns failure ratio aggregated by destination across users.
func (t *Tracker) DestinationErrorRate(dest string) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	var fail, total int64
	cutoff := time.Now().Add(-t.window)
	for _, transfers := range t.perUser {
		for _, tr := range transfers {
			if tr.Destination != dest {
				continue
			}
			if tr.EndedAt.Before(cutoff) {
				continue
			}
			total++
			if !tr.Success {
				fail++
			}
		}
	}
	if total == 0 {
		return 0
	}
	return float64(fail) / float64(total)
}

// UserSiteErrorRate returns failure ratio for a (user,site) pair over window.
func (t *Tracker) UserSiteErrorRate(user, site string) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	var fail, total int64
	cutoff := time.Now().Add(-t.window)
	for _, tr := range t.perUser[user] {
		if tr.Site != site {
			continue
		}
		if tr.EndedAt.Before(cutoff) {
			continue
		}
		total++
		if !tr.Success {
			fail++
		}
	}
	if total == 0 {
		return 0
	}
	return float64(fail) / float64(total)
}

// UserSiteAverageSandboxSize returns the average sandbox size in GB for a (user,site) pair over the window.
// Returns (averageSizeGB, sampleCount). Returns (0, 0) if fewer than minSamples transfers are available.
func (t *Tracker) UserSiteAverageSandboxSize(user, site string, minSamples int) (float64, int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var totalBytes int64
	var count int
	cutoff := time.Now().Add(-t.window)

	// Track unique sandboxes to avoid double-counting
	seen := make(map[string]bool)

	for _, tr := range t.perUser[user] {
		if tr.Site != site {
			continue
		}
		if tr.EndedAt.Before(cutoff) {
			continue
		}
		if tr.SandboxSize <= 0 {
			continue
		}
		// Only count each sandbox once
		if seen[tr.SandboxName] {
			continue
		}
		seen[tr.SandboxName] = true
		totalBytes += tr.SandboxSize
		count++
	}

	if count < minSamples {
		return 0, count
	}

	// Convert bytes to GB
	avgGB := float64(totalBytes) / float64(count) / 1e9
	return avgGB, count
}

// GuessPrefix returns a namespace-like prefix from a URL path, used when director data is unavailable.
func GuessPrefix(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	segments := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(segments) >= 2 && segments[0] == "ospool" {
		return "/" + strings.Join(segments[:2], "/")
	}
	if len(segments) >= 3 {
		return "/" + strings.Join(segments[:3], "/")
	}
	if len(segments) > 0 {
		return "/" + strings.Join(segments, "/")
	}
	return ""
}
