// Package dbready decides whether htcondordb is current enough to read from
// instead of the schedd.
//
// The mirror is only worth using while it is keeping up. htcondordb advertises
// per-source sync health -- how far behind its tailer is for job_queue.log,
// HISTORY and JOB_EPOCH_HISTORY, and whether it hit a durability gap -- and
// golang-htcondor's dbmirror package already turns that into a routing decision
// for the REST API. This reuses that policy rather than growing a second
// opinion about what "caught up" means, which is how two components end up
// disagreeing about the same mirror.
//
// What this adds is the pelican-man-specific part: which source backs which
// read, a per-source fallback so a gap in one table does not cost the others,
// and reporting that says why a read went to the schedd without repeating
// itself once per poll.
package dbready

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/golang-htcondor/webapi/dbmirror"
)

// Source names a thing pelican-man reads, and therefore a sync source it
// depends on.
type Source string

const (
	// SourceJobs is the live queue: job_queue.log mirrored into "jobs".
	SourceJobs Source = "jobs"
	// SourceHistory is completed jobs: HISTORY mirrored into "history".
	SourceHistory Source = "history"
	// SourceEpoch is per-run-instance records, which is where transfers live:
	// JOB_EPOCH_HISTORY mirrored into "epoch_history".
	SourceEpoch Source = "epoch_history"
)

// Defaults for how far behind a source may be and still be read from.
//
// Both are leeway on "caught up" rather than a tolerance for genuinely stale
// data: a tailer following a busy job_queue.log is a few kilobytes behind most
// of the time, and refusing to read until it reaches exactly EOF would mean
// almost never reading. Ten seconds and ten kilobytes are small next to the
// poll interval and to what an access point writes in that time.
const (
	DefaultMaxLag      = 10 * time.Second
	DefaultMaxLagBytes = 10 << 10
)

// Checker answers whether a source is current enough to read.
//
// The zero value, and a nil Checker, report every source as not ready with a
// reason saying so -- a daemon with no collector configured cannot know, and
// guessing yes would send reads at a mirror that may be hours behind.
type Checker struct {
	locator *dbmirror.Locator

	mu   sync.Mutex
	last map[string]string
}

// Options configures a Checker.
type Options struct {
	// Collector is the pool collector, where htcondordb advertises its sync
	// health. Required: without it there is nothing to read the health from.
	Collector *htcondor.Collector
	// Config supplies the security policy for the collector query.
	Config *config.Config

	// MaxLag and MaxLagBytes are the leeway on caught-up. Zero means the
	// package default.
	MaxLag      time.Duration
	MaxLagBytes int64
}

// New returns a Checker. It is safe to call with a nil collector or config, in
// which case every source reports not ready rather than erroring at startup:
// the mirror is an accelerator, and a daemon that cannot evaluate it should
// keep reading the schedd rather than refuse to run.
//
// The tolerances are dbmirror's process-wide policy, so New sets them for the
// whole process. That is the intended use -- set once at startup -- and this is
// the only component in pelican-man that reads from a mirror.
func New(opts Options) *Checker {
	maxLag := opts.MaxLag
	if maxLag <= 0 {
		maxLag = DefaultMaxLag
	}
	maxBytes := opts.MaxLagBytes
	if maxBytes <= 0 {
		maxBytes = DefaultMaxLagBytes
	}

	secs := int64(maxLag.Seconds())
	if secs < 1 {
		secs = 1
	}
	dbmirror.JobsToleranceSecs = secs
	dbmirror.HistoryToleranceSecs = secs
	dbmirror.EpochToleranceSecs = secs
	dbmirror.CaughtUpLagBytes = maxBytes

	c := &Checker{last: map[string]string{}}
	if opts.Collector != nil && opts.Config != nil {
		c.locator = dbmirror.NewLocator(opts.Collector, opts.Config)
	}
	return c
}

// Ready reports whether src may be read from the mirror, and why not when it
// may not. The reason is prose meant for a log line; callers that need to
// branch should branch on the boolean.
//
// Discovery is cached by the locator, so calling this per poll costs a collector
// query only when the cached advertisement has aged out.
func (c *Checker) Ready(ctx context.Context, src string) (bool, string) {
	if c == nil || c.locator == nil {
		return false, "no collector configured, so htcondordb's sync health cannot be read"
	}

	info, err := c.locator.Discover(ctx)
	if err != nil {
		return false, fmt.Sprintf("cannot find htcondordb in the collector: %v", err)
	}

	var d dbmirror.Decision
	switch Source(src) {
	case SourceJobs:
		d = dbmirror.JobsDecision(info, "")
	case SourceHistory:
		d = dbmirror.HistoryDecision(info, nil)
	case SourceEpoch:
		d = dbmirror.EpochDecision(info)
	default:
		return false, fmt.Sprintf("unknown source %q", src)
	}
	if d.Use {
		return true, ""
	}
	return false, fmt.Sprintf("%s (%s)", d.Note, d.Reason)
}

// Report logs a source's readiness transition, once per distinct reason rather
// than once per poll.
//
// A mirror that is behind is behind for as long as it takes to catch up, which
// at a one-second poll is thousands of identical lines. Reporting the change
// instead keeps the log readable while still saying when reads moved and why --
// the thing an operator actually needs, and the thing a silent fallback denies
// them.
func (c *Checker) Report(src string, ready bool, reason string, logf func(string, ...any)) {
	if c == nil || logf == nil {
		return
	}
	state := reason
	if ready {
		state = ""
	}

	c.mu.Lock()
	prev, seen := c.last[src]
	changed := !seen || prev != state
	c.last[src] = state
	c.mu.Unlock()

	if !changed {
		return
	}
	if ready {
		logf("reading %s from htcondordb", src)
		return
	}
	logf("reading %s from the schedd: %s", src, strings.TrimSpace(reason))
}

// Health exposes the discovered mirror for status reporting. Nil when nothing
// has been discovered.
func (c *Checker) Health() *dbmirror.Health {
	if c == nil || c.locator == nil {
		return nil
	}
	h := c.locator.Health()
	return &h
}
